"""Domain logic: idempotent fragment staging and contiguous shot release.

Every public operation runs inside a single database transaction provided by
the caller. Validation failures are returned as error outcomes *before* any
staging state is mutated, and the idempotency row is updated to the final
result in the same transaction, so receipts and state can never diverge.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from app import config
from app.schemas import FragmentIn

# Sentinel stored while a request is being processed. A committed row always
# carries a real HTTP status, so a concurrent retry either blocks on the
# unique index or observes the finished receipt.
IN_PROGRESS = -1


@dataclass
class Outcome:
    status_code: int
    body: dict[str, Any]


def _error(code: str, message: str, status: int = 409, **extra: Any) -> Outcome:
    detail: dict[str, Any] = {"code": code, "message": message}
    detail.update(extra)
    return Outcome(status, {"error": detail})


def submit_fragment(
    conn: psycopg.Connection, run_id: str, frag: FragmentIn
) -> Outcome:
    """Process one fragment submission atomically.

    Lock order is always ``idempotency`` row then ``runs`` row, which is the
    same for every transaction touching a run and therefore cannot deadlock.
    """
    content_hash = frag.content_hash()
    canonical_channels = sorted(set(frag.channels))

    with conn.cursor() as cur:
        # 1) Idempotency claim / replay.
        #    The unique (run_id, operation_id) index plus READ COMMITTED
        #    blocking makes this a claim: a concurrent insert waits for the
        #    predecessor to finish. If that predecessor rolls back, our
        #    insert proceeds and we own the claim.
        cur.execute(
            """
            INSERT INTO idempotency
                (run_id, operation_id, request_hash, status_code, response)
            VALUES (%s, %s, %s, %s, '{}'::jsonb)
            ON CONFLICT DO NOTHING
            """,
            (run_id, frag.operation_id, content_hash, IN_PROGRESS),
        )
        cur.execute(
            """
            SELECT request_hash, status_code, response
            FROM idempotency
            WHERE run_id = %s AND operation_id = %s
            """,
            (run_id, frag.operation_id),
        )
        receipt = cur.fetchone()
        if receipt is not None and receipt["status_code"] != IN_PROGRESS:
            # A finished receipt already exists. Same content replays the
            # first receipt byte-for-byte; different content reusing the
            # operation id is stably rejected. Either way the original
            # receipt is never modified, not even by the rejection.
            if receipt["request_hash"] != content_hash:
                return _error(
                    "operation_id_reuse",
                    "operation_id has already been used with different "
                    "request content",
                )
            return Outcome(receipt["status_code"], dict(receipt["response"]))

        # We own the claim (either a fresh row or a predecessor that rolled
        # back): process the request and pin the final result below.
        outcome = _process(cur, run_id, frag, canonical_channels)
        return _finalize(cur, run_id, frag, content_hash, outcome)


def _process(
    cur: psycopg.Cursor,
    run_id: str,
    frag: FragmentIn,
    channels: list[str],
) -> Outcome:
    """Validate, stage and release. Run row is locked throughout."""
    # 2) Establish the batch atomically on the first legal request.
    #    Pre-check the water=0 window legality so an illegal first request
    #    (e.g. shot 100) never creates the run.
    cur.execute("SELECT 1 FROM runs WHERE run_id = %s", (run_id,))
    exists = cur.fetchone() is not None
    if not exists and frag.shot > config.WINDOW_AHEAD:
        return _error(
            "out_of_window",
            "shot is beyond the staging window ahead of the water mark",
            status=422,
            water_mark=0,
            window_ahead=config.WINDOW_AHEAD,
            max_accepted_shot=config.WINDOW_AHEAD,
        )

    cur.execute(
        """
        INSERT INTO runs
            (run_id, channels, channel_count, water_mark)
        VALUES (%s, %s, %s, 0)
        ON CONFLICT (run_id) DO NOTHING
        """,
        (run_id, channels, len(channels)),
    )
    cur.execute(
        """
        SELECT channels, water_mark
        FROM runs
        WHERE run_id = %s
        FOR UPDATE
        """,
        (run_id,),
    )
    run = cur.fetchone()
    db_channels = list(run["channels"])
    water_mark = run["water_mark"]

    # The channel set is frozen by the first legal request.
    if set(db_channels) != set(channels):
        return _error(
            "channels_immutable",
            "the channel set of this run was fixed by the first request "
            "and cannot be changed",
            expected=db_channels,
            received=channels,
        )

    # 3) Window checks: no rewriting released shots, no fragments too far
    #    ahead of the water mark.
    if frag.shot <= water_mark:
        return _error(
            "shot_already_committed",
            "shot has already been released and can no longer accept "
            "fragments",
            status=422,
            water_mark=water_mark,
        )
    max_accepted = water_mark + config.WINDOW_AHEAD
    if frag.shot > max_accepted:
        return _error(
            "out_of_window",
            "shot is beyond the staging window ahead of the water mark",
            status=422,
            water_mark=water_mark,
            window_ahead=config.WINDOW_AHEAD,
            max_accepted_shot=max_accepted,
        )

    # 4) Stage the fragment. Same (run, shot, channel) with identical
    #    samples is a harmless duplicate; a different payload is rejected.
    cur.execute(
        """
        INSERT INTO fragments (run_id, shot, channel, samples)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (run_id, shot, channel) DO NOTHING
        RETURNING shot
        """,
        (run_id, frag.shot, frag.channel, frag.samples),
    )
    inserted = cur.fetchone() is not None
    duplicate = False
    if not inserted:
        cur.execute(
            """
            SELECT samples
            FROM fragments
            WHERE run_id = %s AND shot = %s AND channel = %s
            """,
            (run_id, frag.shot, frag.channel),
        )
        stored = list(cur.fetchone()["samples"])
        if stored != list(frag.samples):
            return _error(
                "duplicate_conflict",
                "a fragment with different samples already exists for this "
                "run, shot and channel",
                shot=frag.shot,
                channel=frag.channel,
            )
        duplicate = True

    # 5) Release every fully-staged shot starting exactly at water_mark + 1,
    #    in one contiguous sweep, then advance the water mark. The run row
    #    lock guarantees no other transaction can interleave here.
    cur.execute(
        """
        SELECT shot,
               array_agg(channel ORDER BY channel) AS present,
               jsonb_object_agg(channel, to_jsonb(samples)) AS data
        FROM fragments
        WHERE run_id = %s AND shot > %s
        GROUP BY shot
        ORDER BY shot
        """,
        (run_id, water_mark),
    )
    staged = cur.fetchall()

    new_water = water_mark
    released: list[dict[str, Any]] = []
    for row in staged:
        if row["shot"] != new_water + 1:
            break  # gap: nothing past it may become visible
        if list(row["present"]) != db_channels:
            break  # this shot is still missing channels
        released.append({"shot": row["shot"], "channels": row["data"]})
        new_water = row["shot"]

    if new_water != water_mark:
        cur.execute(
            "UPDATE runs SET water_mark = %s WHERE run_id = %s",
            (new_water, run_id),
        )

    return Outcome(
        200,
        {
            "run_id": run_id,
            "operation_id": frag.operation_id,
            "shot": frag.shot,
            "channel": frag.channel,
            "channels": db_channels,
            "duplicate": duplicate,
            "water_mark_before": water_mark,
            "water_mark": new_water,
            "released_shots": [r["shot"] for r in released],
            "released": released,
        },
    )


def _finalize(
    cur: psycopg.Cursor,
    run_id: str,
    frag: FragmentIn,
    content_hash: str,
    outcome: Outcome,
) -> Outcome:
    """Pin the final result (success or stable rejection) to the receipt."""
    cur.execute(
        """
        UPDATE idempotency
        SET request_hash = %s,
            status_code = %s,
            response = %s
        WHERE run_id = %s AND operation_id = %s
        """,
        (
            content_hash,
            outcome.status_code,
            Jsonb(outcome.body),
            run_id,
            frag.operation_id,
        ),
    )
    return outcome


_GET_RUN_SQL = """
WITH r AS (
    SELECT run_id, channels, water_mark
    FROM runs
    WHERE run_id = %s
),
committed AS (
    SELECT f.shot,
           jsonb_object_agg(f.channel, to_jsonb(f.samples)) AS data
    FROM fragments f
    WHERE f.run_id = %s
      AND f.shot <= (SELECT water_mark FROM r)
    GROUP BY f.shot
),
pending AS (
    SELECT f.shot,
           array_agg(f.channel ORDER BY f.channel) AS present
    FROM fragments f
    WHERE f.run_id = %s
      AND f.shot > (SELECT water_mark FROM r)
    GROUP BY f.shot
)
SELECT
    (SELECT EXISTS (SELECT 1 FROM r))                       AS found,
    (SELECT channels FROM r)                               AS channels,
    (SELECT water_mark FROM r)                             AS water_mark,
    COALESCE((
        SELECT jsonb_agg(
                   jsonb_build_object('shot', shot, 'channels', data)
                   ORDER BY shot
               )
        FROM committed
    ), '[]'::jsonb)                                        AS committed,
    COALESCE((
        SELECT jsonb_agg(
                   jsonb_build_object('shot', shot, 'present', present)
                   ORDER BY shot
               )
        FROM pending
    ), '[]'::jsonb)                                        AS pending
"""


def get_run(conn: psycopg.Connection, run_id: str) -> dict[str, Any] | None:
    """Return the visible state of a run from a single statement snapshot.

    Committed shots (``shot <= water_mark``) carry their channel data; shots
    past a gap only appear in the pending summary with missing channels.
    """
    with conn.cursor() as cur:
        cur.execute(_GET_RUN_SQL, (run_id, run_id, run_id))
        row = cur.fetchone()

    if not row["found"]:
        return None

    channels = list(row["channels"])
    pending: list[dict[str, Any]] = []
    for item in row["pending"]:
        present = list(item["present"])
        missing = [c for c in channels if c not in present]
        pending.append(
            {"shot": item["shot"], "present": present, "missing": missing}
        )

    return {
        "run_id": run_id,
        "channels": channels,
        "water_mark": row["water_mark"],
        "next_shot": row["water_mark"] + 1,
        "committed": row["committed"],
        "pending": pending,
    }
