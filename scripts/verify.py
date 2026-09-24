"""Compose verify 服务入口：构建检查 → 代码测试 → HTTP 冒烟（只跑一次）。

冒烟覆盖：
1. 越序补洞：后炮先齐不释放，缺口补齐后同一事务连续释放；
2. 并发重试：同操作标识同内容并发提交只得到同一份首次回执；
3. 稳定拒绝：异参复用、异值重传、越窗、通道集合变更、参数非法；
4. 重启恢复：通过 Docker socket 重启 api 容器，状态与回执保持一致。

以进程退出码报告结果：0 通过，非 0 失败。
"""
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

BASE_URL = os.environ.get("BASE_URL", "http://api:8000")
DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://pulse:pulse@db:5432/pulsedb"
)


def step(name):
    print(f"\n=== {name} ===", flush=True)


def http(method, path, payload=None, expect=None):
    url = BASE_URL + path
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            status, body = resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        status, body = exc.code, json.loads(exc.read())
    if expect is not None:
        assert status == expect, f"{method} {path}: expected {expect}, got {status}: {body}"
    return status, body


def wait_healthy(timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            status, _ = http("GET", "/health")
            if status == 200:
                return
        except Exception:
            pass
        time.sleep(0.5)
    raise RuntimeError("api did not become healthy")


def frag(op_id, channels, channel, shot, samples):
    return {
        "op_id": op_id,
        "channels": channels,
        "channel": channel,
        "shot": shot,
        "samples": samples,
    }


def check_build():
    step("build check: compileall")
    subprocess.run(
        [sys.executable, "-m", "compileall", "-q", "app", "scripts", "tests"],
        check=True,
    )


def check_pytest():
    step("code tests: pytest")
    env = dict(os.environ, DATABASE_URL=DATABASE_URL)
    subprocess.run([sys.executable, "-m", "pytest"], check=True, env=env)


def check_out_of_order_gap_fill(run):
    step("smoke 1/4: out-of-order gap fill")
    chans = ["alpha", "beta"]
    # 第 2 炮先齐
    for ch in chans:
        http("PUT", f"/api/runs/{run}/fragments",
             frag(f"smoke-s2-{ch}", chans, ch, 2, [20, 21]), expect=200)
    # 第 1 炮只到一个通道：不得释放任何炮
    http("PUT", f"/api/runs/{run}/fragments",
         frag("smoke-s1-a", chans, "alpha", 1, [10, 11]), expect=200)
    _, st = http("GET", f"/api/runs/{run}", expect=200)
    assert st["watermark"] == 1 and st["released_shots"] == [], st
    buffered2 = [b for b in st["buffered"] if b["shot"] == 2][0]
    assert buffered2["complete"] is True and buffered2["missing_channels"] == []

    # 补齐缺口：同一请求的事务内连续释放 1、2 炮
    _, receipt = http("PUT", f"/api/runs/{run}/fragments",
                      frag("smoke-s1-b", chans, "beta", 1, [12, 13]), expect=200)
    assert receipt["released_shots"] == [1, 2], receipt
    _, st = http("GET", f"/api/runs/{run}", expect=200)
    assert st["watermark"] == 3 and st["released_shots"] == [1, 2] and st["buffered"] == []


def check_concurrent_retry(run):
    step("smoke 2/4: concurrent identical retries")
    payload = frag("smoke-concurrent", ["alpha", "beta"], "alpha", 5, [1, 2, 3])
    with ThreadPoolExecutor(max_workers=10) as pool:
        results = list(pool.map(
            lambda _: http("PUT", f"/api/runs/{run}/fragments", payload), range(20)))
    assert all(status == 200 for status, _ in results), results
    receipts = {json.dumps(body, sort_keys=True) for _, body in results}
    assert len(receipts) == 1, f"expected one receipt, got {len(receipts)}"


def check_stable_rejections(run):
    step("smoke 3/4: stable rejections")
    chans = ["alpha", "beta"]
    http("PUT", f"/api/runs/{run}/fragments",
         frag("smoke-rj", chans, "alpha", 7, [1]), expect=200)
    # 同操作标识异参复用
    status, body = http("PUT", f"/api/runs/{run}/fragments",
                        frag("smoke-rj", chans, "alpha", 7, [2]))
    assert status == 409 and body["error"]["code"] == "OP_ID_REUSED", body
    # 同通道同炮次异值重传
    status, body = http("PUT", f"/api/runs/{run}/fragments",
                        frag("smoke-rj2", chans, "alpha", 7, [2]))
    assert status == 409 and body["error"]["code"] == "CONFLICTING_FRAGMENT", body
    # 越窗（水位 3，窗口上界 34；炮次 35 拒绝）
    status, body = http("PUT", f"/api/runs/{run}/fragments",
                        frag("smoke-far", chans, "alpha", 35, [1]))
    assert status == 409 and body["error"]["code"] == "SHOT_OUT_OF_WINDOW", body
    # 通道集合变更
    status, body = http("PUT", f"/api/runs/{run}/fragments",
                        frag("smoke-ch", ["alpha", "gamma"], "gamma", 7, [1]))
    assert status == 409 and body["error"]["code"] == "CHANNEL_SET_MISMATCH", body
    # 参数非法（单通道、炮次越界）
    http("PUT", f"/api/runs/{run}/fragments",
         frag("smoke-bad1", ["only"], "only", 1, [1]), expect=422)
    http("PUT", f"/api/runs/{run}/fragments",
         frag("smoke-bad2", chans, "alpha", 999, [1]), expect=422)
    # 失败后水位与暂存区不变
    _, st = http("GET", f"/api/runs/{run}")
    assert st["watermark"] == 3, st


def check_restart_recovery(run):
    step("smoke 4/4: restart recovery")
    import docker

    _, before = http("GET", f"/api/runs/{run}", expect=200)
    _, first_receipt = http(
        "PUT", f"/api/runs/{run}/fragments",
        frag("smoke-s2-alpha", ["alpha", "beta"], "alpha", 2, [20, 21]),
    )  # 第 2 炮此前已释放（幂等返回首次回执）

    client = docker.from_env()
    containers = client.containers.list(
        filters={"label": "com.docker.compose.service=api"}
    )
    assert containers, "api container not found via docker socket"
    print(f"restarting container {containers[0].name} ...", flush=True)
    containers[0].restart(timeout=20)
    wait_healthy()

    _, after = http("GET", f"/api/runs/{run}", expect=200)
    assert after == before, f"state changed across restart:\nbefore={before}\nafter={after}"
    _, retried = http(
        "PUT", f"/api/runs/{run}/fragments",
        frag("smoke-s2-alpha", ["alpha", "beta"], "alpha", 2, [20, 21]),
        expect=200,
    )
    assert retried == first_receipt, "receipt changed across restart"


def main():
    run = "verify-" + str(int(time.time() * 1000))
    wait_healthy()
    check_build()
    check_pytest()
    check_out_of_order_gap_fill(run)
    check_concurrent_retry(run)
    check_stable_rejections(run)
    check_restart_recovery(run)
    print("\nALL CHECKS PASSED", flush=True)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(f"\nVERIFY FAILED: {exc!r}", flush=True)
        sys.exit(1)
