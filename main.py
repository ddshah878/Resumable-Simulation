"""FastAPI application — Resumable Simulation Pipeline.

Wires together: Redis, SQLite, JobQueue, WorkerPool, Scheduler,
CircuitBreakerRegistry, and exposes the full REST API.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from contextlib import asynccontextmanager

import redis.asyncio as redis
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from circuit_breaker import CircuitBreakerRegistry
from config import settings
from database import get_db, init_db
from models import (
    CheckpointResponse,
    HealthResponse,
    JobCreate,
    JobDetailResponse,
    JobListResponse,
    JobResponse,
    JobStatus,
)
from job_queue import JobQueue
from scheduler import Scheduler
from worker import WorkerPool

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("main")

# ---------- shared state (initialised in lifespan) ----------
_redis: redis.Redis | None = None
_queue: JobQueue | None = None
_pool: WorkerPool | None = None
_scheduler: Scheduler | None = None
_cb = CircuitBreakerRegistry()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _redis, _queue, _pool, _scheduler

    await init_db()
    logger.info("SQLite initialised at %s", settings.sqlite_db_path)

    _redis = redis.from_url(settings.redis_url, decode_responses=False)
    await _redis.ping()
    logger.info("Redis connected at %s", settings.redis_url)

    _queue = JobQueue(_redis)
    _pool = WorkerPool(_redis, settings.sqlite_db_path, _cb)
    _scheduler = Scheduler(_redis, _queue, _pool, settings.sqlite_db_path)
    await _scheduler.start()

    yield

    logger.info("Shutting down…")
    if _scheduler:
        await _scheduler.stop()
    if _pool:
        await _pool.shutdown()
    if _redis:
        await _redis.aclose()


app = FastAPI(
    title="Resumable Simulation Pipeline",
    description="Distributed job execution engine for scientific simulations",
    version="1.0.0",
    lifespan=lifespan,
)


# ──────────────────────────── helpers ────────────────────────────


def _row_to_response(row) -> JobResponse:
    current = row["current_step"]
    total = row["total_steps"]
    result_raw = row["result"]
    result = json.loads(result_raw) if result_raw else None

    return JobResponse(
        id=row["id"],
        job_type=row["job_type"],
        status=JobStatus(row["status"]),
        priority=row["priority"],
        total_steps=total,
        current_step=current,
        progress=round(current / total, 4) if total > 0 else 0,
        result=result,
        error=row["error"],
        retry_count=row["retry_count"] or 0,
        max_retries=row["max_retries"] or settings.max_retries_default,
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        started_at=row["started_at"],
        completed_at=row["completed_at"],
    )


# ──────────────────────────── routes ────────────────────────────


@app.get("/")
async def root():
    return {"service": "Resumable Simulation Pipeline", "version": "1.0.0"}


@app.post("/jobs", response_model=JobResponse, status_code=201)
async def submit_job(body: JobCreate):
    assert _queue is not None

    queue_depth = await _queue.depth()
    if queue_depth >= settings.queue_overload_threshold:
        return JSONResponse(
            status_code=429,
            content={"detail": "System overloaded, try again later"},
            headers={"Retry-After": "10"},
        )

    if not _cb.allow(body.job_type):
        raise HTTPException(
            status_code=503,
            detail=f"Circuit breaker OPEN for job type '{body.job_type}'",
        )

    job_id = uuid.uuid4().hex
    now = time.time()

    db = await get_db()
    try:
        await db.execute(
            """INSERT INTO jobs
               (id, job_type, status, priority, total_steps, current_step,
                params, retry_count, max_retries, created_at, updated_at)
               VALUES (?,?,?,?,?,0,?,0,?,?,?)""",
            (
                job_id,
                body.job_type,
                JobStatus.PENDING.value,
                body.priority.value,
                body.total_steps,
                json.dumps(body.params),
                body.max_retries,
                now,
                now,
            ),
        )
        await db.commit()

        async with db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)) as cur:
            row = await cur.fetchone()
    finally:
        await db.close()

    await _queue.enqueue(job_id, body.priority.value)
    logger.info("Job %s submitted (type=%s, priority=%d)", job_id, body.job_type, body.priority.value)

    return _row_to_response(row)


@app.get("/jobs", response_model=JobListResponse)
async def list_jobs(
    status: JobStatus | None = None,
    priority: int | None = None,
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
):
    clauses = []
    params: list = []
    if status is not None:
        clauses.append("status=?")
        params.append(status.value)
    if priority is not None:
        clauses.append("priority=?")
        params.append(priority)

    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""

    db = await get_db()
    try:
        async with db.execute(f"SELECT COUNT(*) as cnt FROM jobs{where}", params) as cur:
            total = (await cur.fetchone())["cnt"]

        async with db.execute(
            f"SELECT * FROM jobs{where} ORDER BY created_at DESC LIMIT ? OFFSET ?",
            params + [limit, offset],
        ) as cur:
            rows = await cur.fetchall()
    finally:
        await db.close()

    return JobListResponse(
        jobs=[_row_to_response(r) for r in rows],
        total=total,
    )


@app.get("/jobs/{job_id}", response_model=JobDetailResponse)
async def get_job(job_id: str):
    db = await get_db()
    try:
        async with db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)) as cur:
            row = await cur.fetchone()
        if row is None:
            raise HTTPException(404, "Job not found")

        async with db.execute(
            "SELECT step, state, created_at FROM checkpoints WHERE job_id=? ORDER BY step",
            (job_id,),
        ) as cur:
            cp_rows = await cur.fetchall()
    finally:
        await db.close()

    base = _row_to_response(row)
    params_raw = row["params"]
    return JobDetailResponse(
        **base.model_dump(),
        checkpoints=[
            CheckpointResponse(
                step=cp["step"],
                state=json.loads(cp["state"]),
                created_at=cp["created_at"],
            )
            for cp in cp_rows
        ],
        params=json.loads(params_raw) if params_raw else {},
    )


@app.post("/jobs/{job_id}/pause", response_model=JobResponse)
async def pause_job(job_id: str):
    assert _redis is not None
    db = await get_db()
    try:
        async with db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)) as cur:
            row = await cur.fetchone()
        if row is None:
            raise HTTPException(404, "Job not found")
        if row["status"] != JobStatus.RUNNING.value:
            raise HTTPException(409, f"Cannot pause job in status {row['status']}")

        await _redis.set(f"pause:{job_id}", "1", ex=60)

        for _ in range(20):
            await asyncio.sleep(0.3)
            async with db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)) as cur:
                row = await cur.fetchone()
            if row["status"] == JobStatus.PAUSED.value:
                return _row_to_response(row)

        async with db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)) as cur:
            row = await cur.fetchone()
    finally:
        await db.close()

    return _row_to_response(row)


@app.post("/jobs/{job_id}/resume", response_model=JobResponse)
async def resume_job(job_id: str):
    assert _queue is not None
    db = await get_db()
    try:
        async with db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)) as cur:
            row = await cur.fetchone()
        if row is None:
            raise HTTPException(404, "Job not found")
        if row["status"] != JobStatus.PAUSED.value:
            raise HTTPException(409, f"Cannot resume job in status {row['status']}")

        now = time.time()
        await db.execute(
            "UPDATE jobs SET status=?, updated_at=? WHERE id=?",
            (JobStatus.PENDING.value, now, job_id),
        )
        await db.commit()

        await _queue.enqueue(job_id, row["priority"])

        async with db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)) as cur:
            row = await cur.fetchone()
    finally:
        await db.close()

    return _row_to_response(row)


@app.post("/jobs/{job_id}/cancel", response_model=JobResponse)
async def cancel_job(job_id: str):
    assert _redis is not None and _queue is not None
    db = await get_db()
    try:
        async with db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)) as cur:
            row = await cur.fetchone()
        if row is None:
            raise HTTPException(404, "Job not found")

        status = row["status"]
        if status in (JobStatus.COMPLETED.value, JobStatus.CANCELLED.value):
            raise HTTPException(409, f"Cannot cancel job in status {status}")

        if status == JobStatus.PENDING.value:
            now = time.time()
            await db.execute(
                "UPDATE jobs SET status=?, updated_at=? WHERE id=?",
                (JobStatus.CANCELLED.value, now, job_id),
            )
            await db.commit()
            await _queue.remove(job_id)
        elif status == JobStatus.RUNNING.value:
            await _redis.set(f"cancel:{job_id}", "1", ex=60)
            for _ in range(20):
                await asyncio.sleep(0.3)
                async with db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)) as cur:
                    row = await cur.fetchone()
                if row["status"] == JobStatus.CANCELLED.value:
                    break
        elif status == JobStatus.PAUSED.value:
            now = time.time()
            await db.execute(
                "UPDATE jobs SET status=?, updated_at=? WHERE id=?",
                (JobStatus.CANCELLED.value, now, job_id),
            )
            await db.commit()
            await _queue.remove(job_id)

        async with db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)) as cur:
            row = await cur.fetchone()
    finally:
        await db.close()

    return _row_to_response(row)


@app.post("/jobs/{job_id}/retry", response_model=JobResponse)
async def retry_job(job_id: str):
    assert _queue is not None
    db = await get_db()
    try:
        async with db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)) as cur:
            row = await cur.fetchone()
        if row is None:
            raise HTTPException(404, "Job not found")
        if row["status"] != JobStatus.FAILED.value:
            raise HTTPException(409, f"Cannot retry job in status {row['status']}")

        now = time.time()
        await db.execute(
            "UPDATE jobs SET status=?, error=NULL, retry_count=0, updated_at=? WHERE id=?",
            (JobStatus.PENDING.value, now, job_id),
        )
        await db.commit()

        await _queue.enqueue(job_id, row["priority"])

        async with db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)) as cur:
            row = await cur.fetchone()
    finally:
        await db.close()

    return _row_to_response(row)


@app.get("/health", response_model=HealthResponse)
async def health():
    assert _queue is not None and _pool is not None and _redis is not None

    redis_ok = True
    try:
        await _redis.ping()
    except Exception:
        redis_ok = False

    sqlite_ok = True
    try:
        db = await get_db()
        await db.execute("SELECT 1")
        await db.close()
    except Exception:
        sqlite_ok = False

    q_depth = await _queue.depth()
    active = _pool.active_count
    mx = _pool.max_workers

    overall = "healthy"
    if not redis_ok or not sqlite_ok:
        overall = "degraded"
    if not redis_ok and not sqlite_ok:
        overall = "unhealthy"

    return HealthResponse(
        status=overall,
        queue_depth=q_depth,
        active_workers=active,
        max_workers=mx,
        worker_utilization=round(active / mx, 2) if mx > 0 else 0,
        redis_connected=redis_ok,
        sqlite_connected=sqlite_ok,
        circuit_breakers=_cb.get_states(),
    )


