"""数据库连接池与建表初始化。"""
from pathlib import Path

from psycopg_pool import ConnectionPool

from .config import database_url

SCHEMA_PATH = Path(__file__).with_name("schema.sql")

_pool: ConnectionPool | None = None


def init_pool() -> ConnectionPool:
    global _pool
    if _pool is None:
        pool = ConnectionPool(
            conninfo=database_url(),
            min_size=1,
            max_size=10,
            timeout=30,
            kwargs={"autocommit": False},
            open=False,
        )
        pool.open(wait=True)
        _ensure_schema(pool)
        _pool = pool
    return _pool


def get_pool() -> ConnectionPool:
    assert _pool is not None, "pool is not initialized"
    return _pool


def close_pool() -> None:
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None


def _ensure_schema(pool: ConnectionPool) -> None:
    ddl = SCHEMA_PATH.read_text(encoding="utf-8")
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
