"""Database connection pool and schema management."""
from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

import psycopg
from psycopg.rows import dict_row

from app import config

_pool: psycopg.ConnectionPool | None = None


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS runs (
    run_id        TEXT PRIMARY KEY,
    channels      TEXT[] NOT NULL,
    channel_count INTEGER NOT NULL,
    water_mark    INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS fragments (
    run_id     TEXT NOT NULL REFERENCES runs(run_id),
    shot       INTEGER NOT NULL,
    channel    TEXT NOT NULL,
    samples    INTEGER[] NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, shot, channel)
);

-- Hot path: list the staged channels of one shot in shot order.
CREATE INDEX IF NOT EXISTS fragments_run_shot_idx
    ON fragments (run_id, shot, channel);

CREATE TABLE IF NOT EXISTS idempotency (
    run_id       TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    status_code  INTEGER NOT NULL,
    response     JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, operation_id)
);
"""


def init_pool() -> None:
    global _pool
    if _pool is None:
        _pool = psycopg.ConnectionPool(
            config.DATABASE_URL,
            min_size=2,
            max_size=16,
            kwargs={"row_factory": dict_row},
            open=True,
        )


def close_pool() -> None:
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None


@contextmanager
def transaction() -> Iterator[psycopg.Connection]:
    """Yield a pooled connection with a transaction open.

    Any exception rolls the transaction back, so a failed request can never
    leave a half-written staging area, water mark or receipt.
    """
    assert _pool is not None, "pool not initialised"
    with _pool.connection() as conn:
        # psycopg opens an implicit transaction; the context manager commits
        # on clean exit and rolls back on exception.
        yield conn


def init_schema() -> None:
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
