-- 脉冲采集片段归集服务的持久化结构。
-- 所有可变状态都在 PostgreSQL 中：提交、释放与水位推进在同一事务里提交，
-- 进程重启后只需读取这些表即可恢复，绝不依赖内存暂存。

CREATE TABLE IF NOT EXISTS runs (
    run_id      TEXT PRIMARY KEY,
    -- 首批合法请求确定的固定通道集合，此后不可改变（按排序后形式存储）
    channels    TEXT[] NOT NULL,
    -- 下一个待释放炮次；[1, watermark) 即为已连续释放的炮次
    watermark   INTEGER NOT NULL CHECK (watermark >= 1 AND watermark <= 257),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS fragments (
    run_id     TEXT NOT NULL REFERENCES runs(run_id),
    shot       INTEGER NOT NULL CHECK (shot BETWEEN 1 AND 256),
    channel    TEXT NOT NULL,
    op_id      TEXT NOT NULL,
    -- 对采样数组做规范化哈希，用于同通道同炮次异值重传的稳定判定
    data_hash  TEXT NOT NULL,
    samples    JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, shot, channel)
);

-- 每个操作标识在同一跑次下只对应一份首次回执
CREATE TABLE IF NOT EXISTS receipts (
    run_id     TEXT NOT NULL REFERENCES runs(run_id),
    op_id      TEXT NOT NULL,
    receipt    JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, op_id)
);

CREATE INDEX IF NOT EXISTS fragments_run_shot_idx
    ON fragments (run_id, shot);
