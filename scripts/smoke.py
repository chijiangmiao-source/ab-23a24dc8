"""End-to-end HTTP smoke test run by the compose ``verify`` service.

Covers, against a live API:
  1. out-of-order submission and contiguous gap filling (no half shots,
     no jumping over gaps),
  2. concurrent identical retries and concurrent channel completion,
  3. stable rejections (operation-id reuse, conflicting retransmit, window),
  4. restart recovery: state and receipts survive an API container restart.

Exits 0 only if every check passes.
"""
from __future__ import annotations

import concurrent.futures as cf
import os
import sys
import time
import uuid

import httpx

BASE_URL = os.environ.get("BASE_URL", "http://localhost:8000").rstrip("/")
CHANNELS = ["C1", "C2"]
WINDOW = int(os.environ.get("WINDOW_AHEAD", "32"))
API_CONTAINER = os.environ.get("API_CONTAINER_NAME", "pulse-api")
DOCKER_SOCK = "/var/run/docker.sock"

_failures: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    mark = "PASS" if cond else "FAIL"
    print(f"  [{mark}] {name}" + (f" -- {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(f"{name}: {detail}")


def wait_healthy(client: httpx.Client, timeout: float = 60.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            r = client.get("/health")
            if r.status_code == 200:
                return
        except httpx.TransportError:
            pass
        time.sleep(0.5)
    raise RuntimeError("API did not become healthy")


def put_fragment(client: httpx.Client, run_id: str, **fields) -> httpx.Response:
    body = {
        "operation_id": fields.get("operation_id", "op-" + uuid.uuid4().hex),
        "channels": fields.get("channels", CHANNELS),
        "channel": fields["channel"],
        "shot": fields["shot"],
        "samples": fields.get("samples", [1, 2, 3]),
    }
    return client.put(f"/api/runs/{run_id}/fragments", json=body)


def assert_invariants(client: httpx.Client, run_id: str) -> dict:
    state = client.get(f"/api/runs/{run_id}").json()
    wm = state["water_mark"]
    shots = [c["shot"] for c in state["committed"]]
    check(
        "committed shots are exactly 1..water_mark with no gaps",
        shots == list(range(1, wm + 1)),
        f"shots={shots} water_mark={wm}",
    )
    for c in state["committed"]:
        check(
            f"committed shot {c['shot']} has every channel",
            sorted(c["channels"].keys()) == sorted(CHANNELS),
            f"keys={sorted(c['channels'].keys())}",
        )
    # Nothing past a gap may leak into committed.
    check("nothing is committed past a gap", all(s <= wm for s in shots))
    return state


def scenario_gap_fill(client: httpx.Client) -> None:
    print("scenario: out-of-order gap filling")
    run_id = "smoke-gap-" + uuid.uuid4().hex[:12]

    r = put_fragment(client, run_id, shot=3, channel="C1")
    check("first legal request establishes batch", r.status_code == 200, r.text)
    r = put_fragment(client, run_id, shot=3, channel="C2")
    check("shot 3 fully staged but blocked by gap", r.status_code == 200)
    state = assert_invariants(client, run_id)
    check("water mark still 0", state["water_mark"] == 0, str(state["water_mark"]))
    check("shot 3 visible only as pending",
          [p["shot"] for p in state["pending"]] == [3], str(state["pending"]))

    put_fragment(client, run_id, shot=1, channel="C1")
    r = put_fragment(client, run_id, shot=1, channel="C2")
    check("completing shot 1 releases only shot 1",
          r.json()["released_shots"] == [1], r.text)
    assert_invariants(client, run_id)

    put_fragment(client, run_id, shot=2, channel="C1")
    r = put_fragment(client, run_id, shot=2, channel="C2")
    check("completing shot 2 releases 2 and the waiting 3 in one tx",
          r.json()["released_shots"] == [2, 3], r.text)
    state = assert_invariants(client, run_id)
    check("water mark advanced to 3", state["water_mark"] == 3)
    check("staging area drained", state["pending"] == [], str(state["pending"]))

    # Late retransmit of a released shot is rejected.
    r = put_fragment(client, run_id, shot=2, channel="C1", samples=[9])
    check("retransmit into committed shot rejected",
          r.status_code == 422 and
          r.json()["error"]["code"] == "shot_already_committed", r.text)


def scenario_rejections(client: httpx.Client) -> None:
    print("scenario: stable rejections")
    run_id = "smoke-rej-" + uuid.uuid4().hex[:12]
    put_fragment(client, run_id, shot=1, channel="C1")

    # Frozen channel set.
    r = put_fragment(client, run_id, channels=["C1", "C3"], channel="C3", shot=1)
    check("different channel set rejected",
          r.status_code == 409 and
          r.json()["error"]["code"] == "channels_immutable", r.text)

    # Out of window (water=0 -> shot 33 is rejected, run was already made).
    r = put_fragment(client, run_id, shot=WINDOW + 1, channel="C1")
    check("beyond-window fragment rejected",
          r.status_code == 422 and
          r.json()["error"]["code"] == "out_of_window", r.text)
    state = client.get(f"/api/runs/{run_id}").json()
    check("rejection left water mark untouched", state["water_mark"] == 0)

    # Conflicting retransmit.
    r1 = put_fragment(client, run_id, shot=2, channel="C1", samples=[1])
    check("stage shot 2 C1", r1.status_code == 200, r1.text)
    r2 = put_fragment(client, run_id, shot=2, channel="C1", samples=[2])
    check("same shot/channel with different samples rejected",
          r2.status_code == 409 and
          r2.json()["error"]["code"] == "duplicate_conflict", r2.text)
    r3 = put_fragment(client, run_id, shot=2, channel="C1", samples=[2])
    check("conflicting retransmit rejected stably",
          r3.status_code == 409 and r3.json() == r2.json(), r3.text)

    # Operation id reuse with different parameters.
    op = "op-reuse-" + uuid.uuid4().hex[:12]
    a = put_fragment(client, run_id, operation_id=op, shot=3,
                     channel="C1", samples=[1])
    b = put_fragment(client, run_id, operation_id=op, shot=3,
                     channel="C1", samples=[2])
    check("operation-id reuse with other content rejected",
          b.status_code == 409 and
          b.json()["error"]["code"] == "operation_id_reuse", b.text)
    a_again = put_fragment(client, run_id, operation_id=op, shot=3,
                           channel="C1", samples=[1])
    check("first receipt still replays after reuse attempt",
          a_again.status_code == a.status_code and
          a_again.json() == a.json(), f"{a_again.text} vs {a.text}")


def scenario_concurrency(client: httpx.Client) -> None:
    print("scenario: concurrent retries and concurrent completion")
    run_id = "smoke-conc-" + uuid.uuid4().hex[:12]

    # N parallel identical requests with the same operation id.
    op = "op-conc-" + uuid.uuid4().hex[:12]
    body = {
        "operation_id": op,
        "channels": CHANNELS,
        "channel": "C1",
        "shot": 1,
        "samples": [4, 5, 6],
    }
    with cf.ThreadPoolExecutor(max_workers=8) as pool:
        responses = list(pool.map(
            lambda _: client.put(f"/api/runs/{run_id}/fragments", json=body),
            range(8),
        ))
    bodies = [r.json() for r in responses]
    check("all concurrent retries accepted", all(r.status_code == 200 for r in responses),
          str([r.status_code for r in responses]))
    check("every retry returned the identical first receipt",
          all(b == bodies[0] for b in bodies), str(bodies))
    state = client.get(f"/api/runs/{run_id}").json()
    check("exactly one fragment staged despite 8 requests",
          state["pending"] and state["pending"][0]["present"] == ["C1"],
          str(state["pending"]))

    # Two different channels arriving concurrently: release happens once.
    run_id2 = "smoke-conc2-" + uuid.uuid4().hex[:12]
    with cf.ThreadPoolExecutor(max_workers=2) as pool:
        futs = [
            pool.submit(put_fragment, client, run_id2, shot=1, channel="C1"),
            pool.submit(put_fragment, client, run_id2, shot=1, channel="C2"),
        ]
        rs = [f.result() for f in futs]
    check("both concurrent channels accepted",
          all(r.status_code == 200 for r in rs), str([r.text for r in rs]))
    released = sorted(s for r in rs for s in r.json()["released_shots"])
    check("shot released exactly once across both transactions",
          released == [1], str(released))
    state = client.get(f"/api/runs/{run_id2}").json()
    check("water mark is 1", state["water_mark"] == 1, str(state["water_mark"]))


def scenario_restart(client: httpx.Client) -> None:
    print("scenario: restart recovery")
    if not os.path.exists(DOCKER_SOCK):
        _failures.append(
            f"restart recovery: docker socket {DOCKER_SOCK} not mounted")
        return

    run_id = "smoke-restart-" + uuid.uuid4().hex[:12]
    # Leave a real gap: shot 2 is half-staged while shot 1 completes, so the
    # water mark stops at 1 and pending state is non-trivial.
    put_fragment(client, run_id, shot=2, channel="C1", samples=[7])
    op_shot1c1 = "op-" + uuid.uuid4().hex
    # This request does not release yet (C2 still missing); its original
    # receipt must replay identically after the restart.
    r_first = put_fragment(client, run_id, operation_id=op_shot1c1,
                           shot=1, channel="C1", samples=[8])
    put_fragment(client, run_id, shot=1, channel="C2", samples=[9])
    before = client.get(f"/api/runs/{run_id}").json()
    check("shot 1 committed, shot 2 half-staged before restart",
          before["water_mark"] == 1 and
          [p["shot"] for p in before["pending"]] == [2] and
          before["pending"][0]["missing"] == ["C2"], str(before))

    docker = httpx.Client(uds=DOCKER_SOCK, base_url="http://docker", timeout=30)
    rr = docker.post(f"/v1.41/containers/{API_CONTAINER}/restart",
                     params={"t": 5})
    check("docker restart accepted", rr.status_code in (204, 304), rr.text)
    docker.close()

    wait_healthy(client)
    check("health ok after restart", True)

    after = client.get(f"/api/runs/{run_id}").json()
    check("visible state identical after restart", after == before,
          f"before={before} after={after}")

    # Receipts survive the restart too. The shot is committed by now, but
    # idempotent replay short-circuits validation and returns the original
    # receipt rather than a "shot already committed" rejection.
    replay = put_fragment(client, run_id, operation_id=op_shot1c1,
                          shot=1, channel="C1", samples=[8])
    check("idempotent receipt replays after restart",
          replay.status_code == r_first.status_code and
          replay.json() == r_first.json(), f"{replay.text} vs {r_first.text}")

    # Business processing continues normally post-restart: stage shot 3 so it
    # waits behind the half-staged shot 2, then close shot 2 and watch 2+3
    # release together in one transaction.
    put_fragment(client, run_id, shot=3, channel="C1", samples=[11])
    put_fragment(client, run_id, shot=3, channel="C2", samples=[12])
    put_fragment(client, run_id, shot=2, channel="C1", samples=[7])
    r = put_fragment(client, run_id, shot=2, channel="C2", samples=[10])
    check("contiguous release resumes after restart (2 and 3 together)",
          r.status_code == 200 and r.json()["released_shots"] == [2, 3],
          r.text)
    assert_invariants(client, run_id)


def main() -> int:
    print(f"smoke target: {BASE_URL}")
    with httpx.Client(base_url=BASE_URL, timeout=15) as client:
        wait_healthy(client)
        scenario_gap_fill(client)
        scenario_rejections(client)
        scenario_concurrency(client)
        scenario_restart(client)

    if _failures:
        print(f"\nSMOKE FAILED ({len(_failures)} checks):")
        for f in _failures:
            print(f"  - {f}")
        return 1
    print("\nSMOKE OK: all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
