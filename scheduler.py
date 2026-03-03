"""Scheduler: orchestrates queue polling, orphan recovery, retry promotion, and score aging.

Runs background loops that:
  1. Poll the queue and dispatch jobs to workers
  2. Promote due retries from the delayed queue
  3. Detect orphaned jobs via stale heartbeats
  4. Periodically refresh queue scores for priority aging
"""

from __future__ import annotations

import asyncio
import json
import logging
import time

import aiosqlite
import redis.asyncio as redis

from config import settings
from models import JobStatus
from job_queue import JobQueue
from worker import WorkerPool

logger = logging.getLogger("scheduler")


class Scheduler:
    def __init__(
        self,
        redis_client: redis.Redis,
        job_queue: JobQueue,
        worker_pool: WorkerPool,
        db_path: str,
    ) -> None:
        self._r = redis_client
        self._q = job_queue
        self._pool = worker_pool
        self._db_path = db_path
        self._tasks: list[asyncio.Task] = []
        self._running = False

    async def start(self) -> None:
        self._running = True
        self._tasks = [
            asyncio.create_task(self._dispatch_loop()),
            asyncio.create_task(self._retry_promoter_loop()),
            asyncio.create_task(self._orphan_detector_loop()),
            asyncio.create_task(self._score_refresh_loop()),
        ]
        logger.info("Scheduler started with %d background loops", len(self._tasks))

    async def stop(self) -> None:
        self._running = False
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        logger.info("Scheduler stopped")

    async def _dispatch_loop(self) -> None:
        while self._running:
            try:
                if self._pool.active_count >= self._pool.max_workers:
                    await asyncio.sleep(0.5)
                    continue

                job_id = await self._q.dequeue()
                if job_id is None:
                    await asyncio.sleep(0.3)
                    continue

                logger.info("Dispatching job %s to worker pool", job_id)
                await self._pool.execute(job_id)

            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception("Error in dispatch loop")
                await asyncio.sleep(1)

    async def _retry_promoter_loop(self) -> None:
        while self._running:
            try:
                promoted = await self._q.promote_due_retries()
                if promoted:
                    db = await self._get_db()
                    try:
                        for job_id in promoted:
                            async with db.execute(
                                "SELECT priority FROM jobs WHERE id=?", (job_id,)
                            ) as cur:
                                row = await cur.fetchone()
                            if row:
                                await self._q.enqueue(job_id, row["priority"])
                                logger.info("Promoted retry for job %s", job_id)
                    finally:
                        await db.close()
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception("Error in retry promoter")
                await asyncio.sleep(2)

    async def _orphan_detector_loop(self) -> None:
        while self._running:
            try:
                db = await self._get_db()
                try:
                    async with db.execute(
                        "SELECT id, job_type, priority FROM jobs WHERE status=?",
                        (JobStatus.RUNNING.value,),
                    ) as cur:
                        running_jobs = await cur.fetchall()
                finally:
                    await db.close()

                for job in running_jobs:
                    job_id = job["id"]
                    if job_id in self._pool._tasks and not self._pool._tasks[job_id].done():
                        continue

                    raw = await self._r.get(f"heartbeat:{job_id}")
                    if raw is None:
                        await self._recover_orphan(job_id, job["priority"])
                        continue

                    heartbeat = json.loads(raw)
                    age = time.time() - heartbeat["timestamp"]
                    if age > settings.heartbeat_timeout_sec:
                        logger.warning(
                            "Job %s heartbeat stale (%.1fs), recovering", job_id, age
                        )
                        await self._recover_orphan(job_id, job["priority"])

                await asyncio.sleep(settings.orphan_check_interval_sec)
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception("Error in orphan detector")
                await asyncio.sleep(5)

    async def _score_refresh_loop(self) -> None:
        while self._running:
            try:
                await self._q.refresh_scores()
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception("Error in score refresh")
                await asyncio.sleep(10)

    async def _recover_orphan(self, job_id: str, priority: int) -> None:
        db = await self._get_db()
        try:
            now = time.time()
            await db.execute(
                "UPDATE jobs SET status=?, updated_at=? WHERE id=? AND status=?",
                (JobStatus.PENDING.value, now, job_id, JobStatus.RUNNING.value),
            )
            await db.commit()
        finally:
            await db.close()

        await self._r.delete(f"heartbeat:{job_id}")
        await self._q.enqueue(job_id, priority)
        logger.info("Recovered orphaned job %s -> PENDING", job_id)

    async def _get_db(self) -> aiosqlite.Connection:
        db = await aiosqlite.connect(self._db_path)
        db.row_factory = aiosqlite.Row
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA busy_timeout=5000")
        return db
