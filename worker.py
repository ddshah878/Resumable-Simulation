"""Async worker pool with heartbeat, checkpointing, and cooperative cancellation.

Each worker is an asyncio Task that:
  1. Picks a job from the queue
  2. Loads the last checkpoint (if resuming)
  3. Runs simulation steps, checkpointing after each
  4. Checks for pause/cancel signals between steps
  5. Publishes heartbeats to Redis while running
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid

import aiosqlite
import redis.asyncio as redis

from circuit_breaker import CircuitBreakerRegistry
from config import settings
from models import JobStatus
from simulation import run_step

logger = logging.getLogger("worker")


class WorkerPool:
    def __init__(
        self,
        redis_client: redis.Redis,
        db_path: str,
        circuit_breakers: CircuitBreakerRegistry,
    ) -> None:
        self._r = redis_client
        self._db_path = db_path
        self._cb = circuit_breakers
        self._tasks: dict[str, asyncio.Task] = {}
        self._worker_id = uuid.uuid4().hex[:8]
        self._shutting_down = False

    @property
    def active_count(self) -> int:
        self._tasks = {jid: t for jid, t in self._tasks.items() if not t.done()}
        return len(self._tasks)

    @property
    def max_workers(self) -> int:
        return settings.max_workers

    async def execute(self, job_id: str) -> None:
        """Start a worker task for the given job."""
        if self.active_count >= self.max_workers:
            logger.warning("Worker pool full, cannot start job %s", job_id)
            return
        task = asyncio.create_task(self._run_job(job_id))
        self._tasks[job_id] = task

    async def shutdown(self) -> None:
        self._shutting_down = True
        for task in self._tasks.values():
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks.values(), return_exceptions=True)

    async def _run_job(self, job_id: str) -> None:
        db = await aiosqlite.connect(self._db_path)
        db.row_factory = aiosqlite.Row
        try:
            await db.execute("PRAGMA journal_mode=WAL")
            await db.execute("PRAGMA busy_timeout=5000")
            await self._execute_job(db, job_id)
        except asyncio.CancelledError:
            logger.info("Worker for job %s cancelled (shutdown)", job_id)
        except Exception:
            logger.exception("Unhandled error in worker for job %s", job_id)
            await self._mark_failed(db, job_id, "Unhandled worker error")
        finally:
            await self._clear_heartbeat(job_id)
            self._tasks.pop(job_id, None)
            await db.close()

    async def _execute_job(self, db: aiosqlite.Connection, job_id: str) -> None:
        row = await self._fetch_job(db, job_id)
        if row is None:
            logger.error("Job %s not found in DB", job_id)
            return

        job_type = row["job_type"]

        if not self._cb.allow(job_type):
            logger.warning("Circuit breaker OPEN for type '%s', re-queuing %s", job_type, job_id)
            await self._set_status(db, job_id, JobStatus.PENDING)
            return

        total_steps = row["total_steps"]
        current_step = row["current_step"]
        params = json.loads(row["params"]) if row["params"] else {}

        prev_state = await self._load_checkpoint(db, job_id, current_step)

        await self._set_status(db, job_id, JobStatus.RUNNING)

        for step in range(current_step, total_steps):
            if self._shutting_down:
                return

            if await self._is_cancelled(job_id):
                await self._set_status(db, job_id, JobStatus.CANCELLED)
                await self._r.delete(f"cancel:{job_id}")
                logger.info("Job %s cancelled at step %d", job_id, step)
                return

            if await self._is_paused(job_id):
                await self._set_status(db, job_id, JobStatus.PAUSED, current_step=step)
                await self._r.delete(f"pause:{job_id}")
                logger.info("Job %s paused at step %d", job_id, step)
                return

            await self._send_heartbeat(job_id)

            try:
                state = await run_step(job_type, step, total_steps, prev_state, params)
            except Exception as exc:
                await self._handle_step_failure(db, job_id, job_type, step, str(exc))
                return

            await self._save_checkpoint(db, job_id, step + 1, state)
            prev_state = state

        final_state = prev_state
        now = time.time()
        await db.execute(
            "UPDATE jobs SET status=?, current_step=?, result=?, completed_at=?, updated_at=? WHERE id=?",
            (JobStatus.COMPLETED.value, total_steps, json.dumps(final_state), now, now, job_id),
        )
        await db.commit()
        self._cb.record_success(job_type)
        logger.info("Job %s completed", job_id)

    async def _handle_step_failure(
        self, db: aiosqlite.Connection, job_id: str, job_type: str, step: int, error: str
    ) -> None:
        self._cb.record_failure(job_type)
        row = await self._fetch_job(db, job_id)
        retry_count = (row["retry_count"] or 0) + 1
        max_retries = row["max_retries"] or settings.max_retries_default

        if retry_count > max_retries:
            logger.error("Job %s permanently failed after %d retries", job_id, max_retries)
            now = time.time()
            await db.execute(
                "UPDATE jobs SET status=?, error=?, retry_count=?, updated_at=? WHERE id=?",
                (JobStatus.FAILED.value, error, retry_count, now, job_id),
            )
            await db.commit()
            return

        delay = min(
            settings.retry_base_delay_sec * (2 ** (retry_count - 1)),
            settings.retry_max_delay_sec,
        )
        import random
        delay += random.uniform(0, delay * 0.2)
        retry_at = time.time() + delay

        now = time.time()
        await db.execute(
            "UPDATE jobs SET status=?, error=?, retry_count=?, updated_at=? WHERE id=?",
            (JobStatus.PENDING.value, error, retry_count, now, job_id),
        )
        await db.commit()

        await self._r.zadd("retry_queue", {job_id: retry_at})
        logger.info(
            "Job %s failed at step %d, scheduling retry #%d in %.1fs",
            job_id, step, retry_count, delay,
        )

    async def _fetch_job(self, db: aiosqlite.Connection, job_id: str) -> aiosqlite.Row | None:
        async with db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)) as cur:
            return await cur.fetchone()

    async def _set_status(
        self, db: aiosqlite.Connection, job_id: str, status: JobStatus, current_step: int | None = None
    ) -> None:
        now = time.time()
        if current_step is not None:
            await db.execute(
                "UPDATE jobs SET status=?, current_step=?, updated_at=?, started_at=COALESCE(started_at,?) WHERE id=?",
                (status.value, current_step, now, now, job_id),
            )
        else:
            await db.execute(
                "UPDATE jobs SET status=?, updated_at=?, started_at=COALESCE(started_at,?) WHERE id=?",
                (status.value, now, now, job_id),
            )
        await db.commit()

    async def _load_checkpoint(
        self, db: aiosqlite.Connection, job_id: str, current_step: int
    ) -> dict | None:
        if current_step == 0:
            return None
        async with db.execute(
            "SELECT state FROM checkpoints WHERE job_id=? AND step=?",
            (job_id, current_step),
        ) as cur:
            row = await cur.fetchone()
        if row:
            return json.loads(row["state"])
        return None

    async def _save_checkpoint(
        self, db: aiosqlite.Connection, job_id: str, step: int, state: dict
    ) -> None:
        now = time.time()
        await db.execute(
            "INSERT OR REPLACE INTO checkpoints (job_id, step, state, created_at) VALUES (?,?,?,?)",
            (job_id, step, json.dumps(state), now),
        )
        await db.execute(
            "UPDATE jobs SET current_step=?, result=?, updated_at=? WHERE id=?",
            (step, json.dumps(state), now, job_id),
        )
        await db.commit()

    async def _send_heartbeat(self, job_id: str) -> None:
        data = json.dumps({"worker_id": self._worker_id, "timestamp": time.time()})
        await self._r.set(
            f"heartbeat:{job_id}",
            data,
            ex=settings.heartbeat_timeout_sec + 5,
        )

    async def _clear_heartbeat(self, job_id: str) -> None:
        await self._r.delete(f"heartbeat:{job_id}")

    async def _is_cancelled(self, job_id: str) -> bool:
        return bool(await self._r.exists(f"cancel:{job_id}"))

    async def _is_paused(self, job_id: str) -> bool:
        return bool(await self._r.exists(f"pause:{job_id}"))

    async def _mark_failed(self, db: aiosqlite.Connection, job_id: str, error: str) -> None:
        now = time.time()
        await db.execute(
            "UPDATE jobs SET status=?, error=?, updated_at=? WHERE id=?",
            (JobStatus.FAILED.value, error, now, job_id),
        )
        await db.commit()
