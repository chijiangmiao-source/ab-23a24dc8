"""Unit tests for request validation and DB-backed integration tests.

Integration tests use DATABASE_URL (see app.config) and are skipped when no
PostgreSQL is reachable, so the pure unit tests still run anywhere.
"""
from __future__ import annotations

import os
import uuid

import pytest
from pydantic import ValidationError

from app import config, db
from app.schemas import FragmentIn
from app.service import get_run, submit_fragment


def frag(**kw) -> FragmentIn:
    base = dict(
        operation_id="op-" + uuid.uuid4().hex,
        channels=["A", "B"],
        channel="A",
        shot=1,
        samples=[1, 2, 3],
    )
    base.update(kw)
    return FragmentIn.model_validate(base)


# ---------------------------------------------------------------- validation

def test_channels_must_be_between_2_and_8():
    with pytest.raises(ValidationError):
        frag(channels=["A"], channel="A")
    with pytest.raises(ValidationError):
        frag(channels=list("ABCDEFGHI"), channel="A")
    frag(channels=list("ABCDEFGH"), channel="A")  # 8 is legal


def test_channel_must_belong_to_set_and_set_is_distinct():
    with pytest.raises(ValidationError):
        frag(channels=["A", "B"], channel="C")
    with pytest.raises(ValidationError):
        frag(channels=["A", "A"], channel="A")


def test_shot_range_and_integer_samples():
    with pytest.raises(ValidationError):
        frag(shot=0)
    with pytest.raises(ValidationError):
        frag(shot=257)
    with pytest.raises(ValidationError):
        frag(samples=[1, "x"])  # type: ignore[list-item]


def test_content_hash_ignores_channel_order():
    a = frag(operation_id="op-x", channels=["A", "B"], shot=3)
    b = frag(operation_id="op-x", channels=["B", "A"], shot=3)
    assert a.content_hash() == b.content_hash()


# ------------------------------------------------------------- integration

@pytest.fixture(scope="session")
def pool():
    try:
        db.init_pool()
        db.init_schema()
    except Exception as exc:  # pragma: no cover - no database available
        pytest.skip(f"PostgreSQL not reachable at {config.DATABASE_URL}: {exc}")
    yield
    db.close_pool()


def _submit(pool, run_id, **kw):
    with db.transaction() as conn:
        return submit_fragment(conn, run_id, frag(**kw))


def _get(pool, run_id):
    with db.transaction() as conn:
        return get_run(conn, run_id)


def test_first_request_creates_batch_and_freezes_channels(pool):
    run_id = "r-" + uuid.uuid4().hex
    ok = _submit(pool, run_id, shot=1, channel="A")
    assert ok.status_code == 200

    bad = _submit(
        pool, run_id,
        channels=["A", "C"], channel="C", shot=1,
    )
    assert bad.status_code == 409
    assert bad.body["error"]["code"] == "channels_immutable"

    state = _get(pool, run_id)
    # The rejected fragment changed nothing: only A is staged for shot 1.
    assert state["channels"] == ["A", "B"]
    assert state["pending"][0]["missing"] == ["B"]


def test_out_of_order_gap_fill_releases_contiguously(pool):
    run_id = "r-" + uuid.uuid4().hex

    # Shot 3 fully staged first: nothing can be released.
    assert _submit(pool, run_id, shot=3, channel="A").status_code == 200
    assert _submit(pool, run_id, shot=3, channel="B").status_code == 200
    state = _get(pool, run_id)
    assert state["water_mark"] == 0
    assert state["committed"] == []

    # Shot 1 complete: releases only 1 (shot 2 is the gap).
    assert _submit(pool, run_id, shot=1, channel="A").status_code == 200
    r = _submit(pool, run_id, shot=1, channel="B")
    assert r.body["released_shots"] == [1]

    # Shot 2 complete: the same transaction releases 2 and the waiting 3.
    assert _submit(pool, run_id, shot=2, channel="A").status_code == 200
    r = _submit(pool, run_id, shot=2, channel="B")
    assert r.body["released_shots"] == [2, 3]
    assert r.body["water_mark"] == 3

    state = _get(pool, run_id)
    assert [c["shot"] for c in state["committed"]] == [1, 2, 3]
    assert state["pending"] == []


def test_window_rejection_does_not_create_run_or_move_water(pool):
    run_id = "r-" + uuid.uuid4().hex
    bad = _submit(pool, run_id, shot=config.WINDOW_AHEAD + 1, channel="A")
    assert bad.status_code == 422
    assert bad.body["error"]["code"] == "out_of_window"
    assert _get(pool, run_id) is None  # batch never established

    # Edge of the window is accepted but cannot release yet.
    edge = _submit(pool, run_id, shot=config.WINDOW_AHEAD, channel="A")
    assert edge.status_code == 200

    too_far = _submit(pool, run_id, shot=config.WINDOW_AHEAD + 1, channel="A")
    assert too_far.status_code == 422
    state = _get(pool, run_id)
    assert state["water_mark"] == 0
    assert [p["shot"] for p in state["pending"]] == [config.WINDOW_AHEAD]


def test_conflicting_retransmit_is_stable_rejection(pool):
    run_id = "r-" + uuid.uuid4().hex
    assert _submit(pool, run_id, shot=1, channel="A",
                   samples=[1, 2]).status_code == 200
    clash = _submit(pool, run_id, shot=1, channel="A", samples=[9, 9])
    assert clash.status_code == 409
    assert clash.body["error"]["code"] == "duplicate_conflict"

    # Repeating the clash is rejected the same way and the original samples
    # are untouched once shot 1 releases.
    assert _submit(pool, run_id, shot=1, channel="A",
                   samples=[9, 9]).status_code == 409
    _submit(pool, run_id, shot=1, channel="B", samples=[3])
    state = _get(pool, run_id)
    assert state["committed"][0]["channels"]["A"] == [1, 2]


def test_idempotent_retry_replays_first_receipt(pool):
    run_id = "r-" + uuid.uuid4().hex
    f1 = frag(operation_id="op-retry-1", shot=1, channel="A", samples=[7])
    with db.transaction() as conn:
        first = submit_fragment(conn, run_id, f1)
    with db.transaction() as conn:
        again = submit_fragment(conn, run_id, f1)
    assert again.status_code == first.status_code
    assert again.body == first.body
    assert again.body["duplicate"] is False

    state = _get(pool, run_id)
    assert len(state["pending"]) == 1


def test_operation_id_reuse_with_different_content_is_rejected(pool):
    run_id = "r-" + uuid.uuid4().hex
    original = frag(operation_id="op-reuse", shot=1, channel="A", samples=[1])
    changed = frag(operation_id="op-reuse", shot=1, channel="A", samples=[2])
    with db.transaction() as conn:
        first = submit_fragment(conn, run_id, original)
    assert first.status_code == 200
    with db.transaction() as conn:
        reused = submit_fragment(conn, run_id, changed)
    assert reused.status_code == 409
    assert reused.body["error"]["code"] == "operation_id_reuse"

    # The first receipt still replays intact.
    with db.transaction() as conn:
        replay = submit_fragment(conn, run_id, original)
    assert replay.body == first.body


def test_committed_shots_reject_new_fragments(pool):
    run_id = "r-" + uuid.uuid4().hex
    _submit(pool, run_id, shot=1, channel="A", samples=[1])
    _submit(pool, run_id, shot=1, channel="B", samples=[2])
    late = _submit(pool, run_id, shot=1, channel="A", samples=[3])
    assert late.status_code == 422
    assert late.body["error"]["code"] == "shot_already_committed"
    state = _get(pool, run_id)
    assert state["committed"][0]["channels"]["A"] == [1]
