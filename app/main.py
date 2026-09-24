"""FastAPI application: fragment intake and run visibility."""
from __future__ import annotations

import logging
import time

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app import db
from app.schemas import FragmentIn
from app.service import get_run, submit_fragment

logger = logging.getLogger("pulse")
logging.basicConfig(level=logging.INFO)

app = FastAPI(title="Pulse Run Fragment Collector", version="1.0.0")

# Business endpoints stay gated until the schema exists and the database is
# reachable; /health stays answerable so orchestrators can wait on it.
_ready = False


@app.on_event("startup")
async def _startup() -> None:
    global _ready
    # Compose starts api before Postgres necessarily accepts connections:
    # retry the pool/schema setup instead of crash-looping.
    deadline = time.monotonic() + 120
    last_exc: Exception | None = None
    while time.monotonic() < deadline:
        try:
            db.init_pool()
            db.init_schema()
            _ready = True
            logger.info("startup complete: schema initialised")
            return
        except Exception as exc:  # pragma: no cover - infra retry
            last_exc = exc
            db.close_pool()
            logger.warning("database not ready yet: %s", exc)
            time.sleep(1.0)
    raise RuntimeError(f"database never became ready: {last_exc}")


@app.on_event("shutdown")
async def _shutdown() -> None:
    db.close_pool()


@app.get("/health")
async def health() -> JSONResponse:
    if not _ready:
        return JSONResponse({"status": "starting"}, status_code=503)
    try:
        with db.transaction() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
    except Exception:
        return JSONResponse({"status": "degraded"}, status_code=503)
    return JSONResponse({"status": "ok"})


def _service_unavailable() -> JSONResponse:
    return JSONResponse(
        {"error": {"code": "not_ready",
                   "message": "service is still starting; consult /health"}},
        status_code=503,
    )


@app.put("/api/runs/{run_id}/fragments")
async def put_fragment(run_id: str, request: Request) -> JSONResponse:
    if not _ready:
        return _service_unavailable()
    if not run_id or len(run_id) > 200:
        return JSONResponse(
            {"error": {"code": "invalid_run_id",
                       "message": "run_id must be 1..200 characters"}},
            status_code=422,
        )

    try:
        payload = await request.json()
    except Exception:
        return JSONResponse(
            {"error": {"code": "invalid_json",
                       "message": "request body must be valid JSON"}},
            status_code=400,
        )

    try:
        frag = FragmentIn.model_validate(payload)
    except Exception as exc:
        return JSONResponse(
            {"error": {"code": "validation_error",
                       "message": "request failed schema validation",
                       "detail": str(exc)}},
            status_code=422,
        )

    try:
        with db.transaction() as conn:
            outcome = submit_fragment(conn, run_id, frag)
    except Exception:
        logger.exception("fragment submission failed")
        return JSONResponse(
            {"error": {"code": "internal_error",
                       "message": "internal processing error; no state "
                                  "was changed"}},
            status_code=500,
        )

    return JSONResponse(outcome.body, status_code=outcome.status_code)


@app.get("/api/runs/{run_id}")
async def read_run(run_id: str) -> JSONResponse:
    if not _ready:
        return _service_unavailable()
    try:
        with db.transaction() as conn:
            state = get_run(conn, run_id)
    except Exception:
        logger.exception("run read failed")
        return JSONResponse(
            {"error": {"code": "internal_error",
                       "message": "internal processing error"}},
            status_code=500,
        )
    if state is None:
        return JSONResponse(
            {"error": {"code": "run_not_found",
                       "message": f"no run named {run_id!r} exists yet"}},
            status_code=404,
        )
    return JSONResponse(state)
