"""FastAPI 入口：健康探针、就绪门控与业务路由。"""
import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Path, Request
from fastapi.responses import JSONResponse

from .config import MAX_RUN_ID_LEN
from .db import close_pool, get_pool, init_pool
from .models import FragmentIn
from .service import AppError, get_run_status, submit_fragment

logger = logging.getLogger("pulse.api")

_ready = False


@asynccontextmanager
async def lifespan(app: FastAPI):
    """启动时等待数据库就绪并建表；就绪前业务请求一律 503。"""
    global _ready
    deadline = time.monotonic() + 90
    while True:
        try:
            init_pool()
            break
        except Exception:
            if time.monotonic() >= deadline:
                raise
            logger.warning("database not ready, retrying...")
            time.sleep(1)
    _ready = True
    logger.info("service ready")
    try:
        yield
    finally:
        _ready = False
        close_pool()


app = FastAPI(title="pulse-fragment-collector", lifespan=lifespan)


@app.exception_handler(AppError)
async def app_error_handler(_: Request, exc: AppError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": {"code": exc.code, "message": exc.message}},
    )


@app.exception_handler(Exception)
async def unhandled_error_handler(_: Request, exc: Exception) -> JSONResponse:
    logger.exception("unhandled error: %s", exc)
    return JSONResponse(
        status_code=500,
        content={"error": {"code": "INTERNAL", "message": "internal error"}},
    )


def _require_ready() -> None:
    if not _ready:
        raise AppError(503, "NOT_READY", "service is not ready yet")


@app.get("/health")
def health():
    """健康路径：只有数据库可用且服务就绪才返回 200。"""
    if not _ready:
        return JSONResponse(status_code=503, content={"status": "starting"})
    try:
        with get_pool().connection() as conn:
            conn.execute("SELECT 1")
    except Exception:
        return JSONResponse(status_code=503, content={"status": "unavailable"})
    return {"status": "ok"}


RunIdPath = Path(
    min_length=1,
    max_length=MAX_RUN_ID_LEN,
    pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$",
)


@app.put("/api/runs/{run_id}/fragments")
def put_fragment(payload: FragmentIn, run_id: str = RunIdPath):
    _require_ready()
    return submit_fragment(run_id, payload)


@app.get("/api/runs/{run_id}")
def get_run(run_id: str = RunIdPath):
    _require_ready()
    return get_run_status(run_id)
