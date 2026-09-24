"""核心事务逻辑：片段提交、幂等回执、暂存窗口与连续释放。

不变量：
- 每个跑次的所有写路径都由 runs 行锁串行化，水位只前进、不后退；
- 释放只发生在同一事务内：水位下一炮通道齐备才推进，且连续推进，
  因此已释放区间 [1, watermark) 永远连续，不存在半炮或越过缺口的释放；
- 任何校验失败都抛 AppError，连接上下文管理器回滚整个事务，
  暂存区、水位与回执保持不变。
"""
import hashlib
import json

from psycopg.types.json import Jsonb

from .config import MAX_SHOT, WINDOW_SIZE
from .db import get_pool
from .models import FragmentIn


class AppError(Exception):
    def __init__(self, status_code: int, code: str, message: str):
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message


def _samples_hash(samples: list[int]) -> str:
    blob = json.dumps(samples, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def _fingerprint(payload: FragmentIn, data_hash: str) -> dict:
    """操作标识对应的请求指纹，用于区分同内容重试与异参复用。"""
    return {
        "channels": sorted(payload.channels),
        "channel": payload.channel,
        "shot": payload.shot,
        "samples_hash": data_hash,
    }


def submit_fragment(run_id: str, payload: FragmentIn) -> dict:
    data_hash = _samples_hash(payload.samples)
    fingerprint = _fingerprint(payload, data_hash)
    pool = get_pool()

    with pool.connection() as conn:
        with conn.cursor() as cur:
            # 1) 原子建立批次；已存在则锁住跑次行，串行化本跑次的全部写入。
            cur.execute(
                "INSERT INTO runs (run_id, channels, watermark)"
                " VALUES (%s, %s, 1)"
                " ON CONFLICT (run_id) DO NOTHING"
                " RETURNING channels, watermark",
                (run_id, sorted(payload.channels)),
            )
            row = cur.fetchone()
            if row is None:
                cur.execute(
                    "SELECT channels, watermark FROM runs WHERE run_id = %s FOR UPDATE",
                    (run_id,),
                )
                row = cur.fetchone()
            run_channels, watermark = list(row[0]), row[1]

            # 2) 通道集合自首批请求后不可改变（与顺序无关的集合比较）。
            if sorted(payload.channels) != sorted(run_channels):
                raise AppError(
                    409,
                    "CHANNEL_SET_MISMATCH",
                    "channels do not match the run's fixed channel set",
                )

            # 3) 操作标识幂等：同内容重试返回首次回执，异参复用稳定拒绝。
            cur.execute(
                "SELECT receipt FROM receipts WHERE run_id = %s AND op_id = %s",
                (run_id, payload.op_id),
            )
            existing = cur.fetchone()
            if existing is not None:
                stored = existing[0]
                if stored["fingerprint"] == fingerprint:
                    return stored["receipt"]
                raise AppError(
                    409,
                    "OP_ID_REUSED",
                    "op_id was already used with different parameters",
                )

            # 4) 暂存窗口：只接受 [watermark, watermark + WINDOW_SIZE) 内的炮次。
            if payload.shot < watermark:
                raise AppError(
                    409,
                    "SHOT_ALREADY_RELEASED",
                    f"shot {payload.shot} is at or below the committed watermark {watermark}",
                )
            if payload.shot >= watermark + WINDOW_SIZE:
                raise AppError(
                    409,
                    "SHOT_OUT_OF_WINDOW",
                    f"shot {payload.shot} is beyond the staging window"
                    f" [{watermark}, {watermark + WINDOW_SIZE - 1}]",
                )

            # 5) 同通道同炮次重传：同值去重，异值稳定拒绝。
            cur.execute(
                "SELECT data_hash FROM fragments"
                " WHERE run_id = %s AND shot = %s AND channel = %s",
                (run_id, payload.shot, payload.channel),
            )
            frag = cur.fetchone()
            duplicate = False
            if frag is not None:
                if frag[0] != data_hash:
                    raise AppError(
                        409,
                        "CONFLICTING_FRAGMENT",
                        "a different value was already stored for this shot and channel",
                    )
                duplicate = True
            else:
                cur.execute(
                    "INSERT INTO fragments (run_id, shot, channel, op_id, data_hash, samples)"
                    " VALUES (%s, %s, %s, %s, %s, %s)",
                    (
                        run_id,
                        payload.shot,
                        payload.channel,
                        payload.op_id,
                        data_hash,
                        Jsonb(payload.samples),
                    ),
                )

            # 6) 同一事务内连续释放随后已齐备的炮次并推进水位。
            n_channels = len(run_channels)
            new_watermark = watermark
            while new_watermark <= MAX_SHOT:
                cur.execute(
                    "SELECT COUNT(*) FROM fragments WHERE run_id = %s AND shot = %s",
                    (run_id, new_watermark),
                )
                if cur.fetchone()[0] < n_channels:
                    break
                new_watermark += 1
            if new_watermark != watermark:
                cur.execute(
                    "UPDATE runs SET watermark = %s WHERE run_id = %s",
                    (new_watermark, run_id),
                )

            receipt = {
                "run_id": run_id,
                "op_id": payload.op_id,
                "shot": payload.shot,
                "channel": payload.channel,
                "accepted": True,
                "duplicate": duplicate,
                "watermark": new_watermark,
                "released_shots": list(range(watermark, new_watermark)),
            }
            cur.execute(
                "INSERT INTO receipts (run_id, op_id, receipt) VALUES (%s, %s, %s)",
                (run_id, payload.op_id, Jsonb({"fingerprint": fingerprint, "receipt": receipt})),
            )
            return receipt


def get_run_status(run_id: str) -> dict:
    """单条 SQL 汇总：READ COMMITTED 下单语句即单一快照，
    不会看到释放事务提交到一半的半炮状态。"""
    pool = get_pool()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT r.channels, r.watermark,"
                " COALESCE("
                "   (SELECT jsonb_agg(jsonb_build_array(f.shot, f.chans)"
                "                     ORDER BY f.shot)"
                "    FROM (SELECT shot, array_agg(channel ORDER BY channel) AS chans"
                "          FROM fragments WHERE run_id = r.run_id"
                "          GROUP BY shot) AS f),"
                "   '[]'::jsonb)"
                " FROM runs r WHERE r.run_id = %s",
                (run_id,),
            )
            row = cur.fetchone()
    if row is None:
        raise AppError(404, "RUN_NOT_FOUND", f"unknown run_id: {run_id}")
    channels, watermark, frag_rows = list(row[0]), row[1], row[2]
    by_shot = {shot: list(chans) for shot, chans in frag_rows}
    buffered = []
    for shot in sorted(by_shot):
        if shot < watermark:
            continue
        have = by_shot[shot]
        missing = [c for c in channels if c not in have]
        buffered.append(
            {
                "shot": shot,
                "channels": have,
                "missing_channels": missing,
                "complete": not missing,
            }
        )

    return {
        "run_id": run_id,
        "channels": channels,
        "watermark": watermark,
        "released_shots": list(range(1, watermark)),
        "submitted_shots": sorted(by_shot),
        "buffered": buffered,
    }
