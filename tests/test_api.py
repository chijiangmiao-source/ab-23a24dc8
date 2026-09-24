"""代码测试：在真实 PostgreSQL 上通过进程内 TestClient 验证全部不变量。

运行方式：DATABASE_URL=postgresql://pulse:pulse@localhost:5432/pulsedb pytest
（verify 服务在 Compose 网络内以 db 为主机名运行同一套测试。）
"""
import json
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from app.db import close_pool, init_pool
from app.main import app


@pytest.fixture(scope="session")
def client():
    try:
        with TestClient(app) as c:
            yield c
    except Exception as exc:  # 数据库不可达时整组跳过（本地无库场景）
        pytest.skip(f"database unavailable: {exc}")


def rid() -> str:
    return "t-" + uuid.uuid4().hex[:16]


def body(op_id, channels, channel, shot, samples):
    return {
        "op_id": op_id,
        "channels": channels,
        "channel": channel,
        "shot": shot,
        "samples": samples,
    }


def put(client, run_id, **kw):
    return client.put(f"/api/runs/{run_id}/fragments", json=body(**kw))


def test_out_of_order_gap_fill(client):
    run = rid()
    chans = ["a", "b", "c"]
    # 先交第 2 炮全部通道和第 1 炮的一个通道：水位必须停在 1
    for ch in chans:
        r = put(client, run, op_id=f"op-s2-{ch}", channels=chans, channel=ch, shot=2, samples=[2])
        assert r.status_code == 200
    r = put(client, run, op_id="op-s1-a", channels=chans, channel="a", shot=1, samples=[1])
    assert r.status_code == 200
    assert r.json()["watermark"] == 1

    st = client.get(f"/api/runs/{run}").json()
    assert st["released_shots"] == []
    assert st["watermark"] == 1
    # 第 2 炮已齐备但不得越过第 1 炮的缺口释放
    s2 = [b for b in st["buffered"] if b["shot"] == 2][0]
    assert s2["complete"] is True

    # 补齐第 1 炮后，同一事务内连续释放 1、2 两炮
    put(client, run, op_id="op-s1-b", channels=chans, channel="b", shot=1, samples=[1])
    r = put(client, run, op_id="op-s1-c", channels=chans, channel="c", shot=1, samples=[1])
    assert r.json()["released_shots"] == [1, 2]
    st = client.get(f"/api/runs/{run}").json()
    assert st["watermark"] == 3
    assert st["released_shots"] == [1, 2]
    assert st["buffered"] == []


def test_no_release_across_gap(client):
    run = rid()
    chans = ["a", "b"]
    for shot in (1, 2, 4):  # 第 3 炮留缺口，第 4 炮齐备也不得释放
        for ch in chans:
            put(client, run, op_id=f"op-{shot}-{ch}", channels=chans, channel=ch,
                shot=shot, samples=[shot])
    st = client.get(f"/api/runs/{run}").json()
    assert st["released_shots"] == [1, 2]
    assert st["watermark"] == 3
    s4 = [b for b in st["buffered"] if b["shot"] == 4][0]
    assert s4["complete"] is True


def test_idempotent_retry_returns_first_receipt(client):
    run = rid()
    chans = ["a", "b"]
    r1 = put(client, run, op_id="op-x", channels=chans, channel="a", shot=1, samples=[7, 8])
    r2 = put(client, run, op_id="op-x", channels=chans, channel="a", shot=1, samples=[7, 8])
    assert r1.status_code == r2.status_code == 200
    assert r1.json() == r2.json()  # 首次回执逐字节一致
    st = client.get(f"/api/runs/{run}").json()
    assert st["watermark"] == 1  # 重试不产生任何额外效果


def test_op_id_reuse_with_different_params_rejected(client):
    run = rid()
    chans = ["a", "b"]
    put(client, run, op_id="op-y", channels=chans, channel="a", shot=1, samples=[1])
    r = put(client, run, op_id="op-y", channels=chans, channel="a", shot=1, samples=[2])
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "OP_ID_REUSED"
    r = put(client, run, op_id="op-y", channels=chans, channel="b", shot=1, samples=[1])
    assert r.status_code == 409


def test_conflicting_fragment_rejected_and_state_unchanged(client):
    run = rid()
    chans = ["a", "b"]
    put(client, run, op_id="op-1", channels=chans, channel="a", shot=1, samples=[1, 2])
    before = client.get(f"/api/runs/{run}").json()
    r = put(client, run, op_id="op-2", channels=chans, channel="a", shot=1, samples=[9, 9])
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "CONFLICTING_FRAGMENT"
    after = client.get(f"/api/runs/{run}").json()
    assert before == after  # 失败不改变暂存区与水位
    # 同值重传（不同操作标识）去重接受
    r = put(client, run, op_id="op-3", channels=chans, channel="a", shot=1, samples=[1, 2])
    assert r.status_code == 200
    assert r.json()["duplicate"] is True


def test_channel_set_immutable(client):
    run = rid()
    put(client, run, op_id="op-1", channels=["a", "b"], channel="a", shot=1, samples=[1])
    r = put(client, run, op_id="op-2", channels=["a", "b", "c"], channel="c", shot=1, samples=[1])
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "CHANNEL_SET_MISMATCH"
    # 同一集合不同顺序视为一致
    r = put(client, run, op_id="op-3", channels=["b", "a"], channel="b", shot=1, samples=[1])
    assert r.status_code == 200


def test_window_enforcement(client):
    run = rid()
    chans = ["a", "b"]
    # 水位 1，窗口为炮次 1..32
    r = put(client, run, op_id="op-far", channels=chans, channel="a", shot=33, samples=[1])
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "SHOT_OUT_OF_WINDOW"
    r = put(client, run, op_id="op-edge", channels=chans, channel="a", shot=32, samples=[1])
    assert r.status_code == 200
    # 释放第 1 炮后水位到 2，第 33 炮进入窗口，第 1 炮成为越窗
    for ch in chans:
        put(client, run, op_id=f"op-s1-{ch}", channels=chans, channel=ch, shot=1, samples=[1])
    st = client.get(f"/api/runs/{run}").json()
    assert st["watermark"] == 2
    r = put(client, run, op_id="op-33", channels=chans, channel="a", shot=33, samples=[1])
    assert r.status_code == 200
    r = put(client, run, op_id="op-old", channels=chans, channel="a", shot=1, samples=[1])
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "SHOT_ALREADY_RELEASED"


def test_validation_errors(client):
    run = rid()
    base = dict(op_id="op", channels=["a", "b"], channel="a", shot=1, samples=[1])
    cases = [
        {**base, "channels": ["a"]},                       # 少于 2 个通道
        {**base, "channels": [str(i) for i in range(9)]},  # 多于 8 个通道
        {**base, "channels": ["a", "a"]},                  # 通道重复
        {**base, "channel": "zzz"},                        # 通道不在集合内
        {**base, "shot": 0},
        {**base, "shot": 257},
        {**base, "samples": []},
        {**base, "samples": ["x"]},
        {**base, "op_id": ""},
    ]
    for payload in cases:
        r = client.put(f"/api/runs/{run}/fragments", json=payload)
        assert r.status_code == 422, payload
    assert client.get(f"/api/runs/{run}").status_code == 404  # 非法请求不建批次


def test_get_unknown_run_404(client):
    assert client.get(f"/api/runs/{rid()}").status_code == 404


def test_concurrent_identical_ops_single_receipt(client):
    run = rid()
    payload = body(op_id="op-c", channels=["a", "b"], channel="a", shot=1, samples=[5])
    with ThreadPoolExecutor(max_workers=8) as ex:
        responses = list(ex.map(
            lambda _: client.put(f"/api/runs/{run}/fragments", json=payload), range(16)))
    assert all(r.status_code == 200 for r in responses)
    receipts = {json.dumps(r.json(), sort_keys=True) for r in responses}
    assert len(receipts) == 1  # 并发同内容重试只产生一份回执


def test_concurrent_channels_release_exactly_once(client):
    run = rid()
    chans = ["a", "b", "c", "d"]
    payloads = [body(op_id=f"op-{ch}", channels=chans, channel=ch, shot=1, samples=[1])
                for ch in chans]
    with ThreadPoolExecutor(max_workers=4) as ex:
        responses = list(ex.map(
            lambda p: client.put(f"/api/runs/{run}/fragments", json=p), payloads))
    assert all(r.status_code == 200 for r in responses)
    # 第 1 炮只被释放一次
    winners = [r.json() for r in responses if r.json()["released_shots"] == [1]]
    assert len(winners) == 1
    st = client.get(f"/api/runs/{run}").json()
    assert st["watermark"] == 2
    assert st["released_shots"] == [1]


def test_restart_preserves_state_and_receipts(client):
    run = rid()
    chans = ["a", "b"]
    r1 = put(client, run, op_id="op-r1", channels=chans, channel="a", shot=1, samples=[3])
    put(client, run, op_id="op-r2", channels=chans, channel="b", shot=1, samples=[3])
    put(client, run, op_id="op-r3", channels=chans, channel="a", shot=2, samples=[4])
    before = client.get(f"/api/runs/{run}").json()
    assert before["watermark"] == 2

    # 模拟进程重启：丢弃连接池并重新初始化（状态全部在 PostgreSQL 中）
    close_pool()
    init_pool()

    after = client.get(f"/api/runs/{run}").json()
    assert after == before  # 重启后查询结果一致，无半炮、无重复释放
    r = put(client, run, op_id="op-r1", channels=chans, channel="a", shot=1, samples=[3])
    assert r.status_code == 200
    assert r.json() == r1.json()  # 回执在重启后仍可幂等重放
    # 水位之后的暂存片段仍可继续补洞
    put(client, run, op_id="op-r4", channels=chans, channel="b", shot=2, samples=[4])
    assert client.get(f"/api/runs/{run}").json()["watermark"] == 3
