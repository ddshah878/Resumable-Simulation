"""Redis-backed priority queue with aging and delayed retry support.

Uses two Redis sorted sets:
  - job_queue: main priority queue (score = effective priority)
  - retry_queue: delayed retry queue (score = retry-at timestamp)
"""

from __future__ import annotations

import time

import redis.asyncio as redis

from config import settings


_LUA_POPMIN = """
local result = redis.call('ZRANGE', KEYS[1], 0, 0)
if #result == 0 then
    return nil
end
redis.call('ZREM', KEYS[1], result[1])
return result[1]
"""

_LUA_REFRESH_SCORES = """
local queue_key = KEYS[1]
local count = tonumber(ARGV[1])
for i = 1, count do
    local job_id = ARGV[2 + (i - 1) * 2]
    local new_score = tonumber(ARGV[3 + (i - 1) * 2])
    local exists = redis.call('ZSCORE', queue_key, job_id)
    if exists then
        redis.call('ZADD', queue_key, new_score, job_id)
    end
end
return count
"""


class JobQueue:
    def __init__(self, redis_client: redis.Redis) -> None:
        self._r = redis_client
        self._queue_key = "job_queue"
        self._retry_key = "retry_queue"
        self._enqueue_time_key = "enqueue_times"
        self._popmin_sha: str | None = None
        self._refresh_sha: str | None = None

    async def _get_popmin_sha(self) -> str:
        if self._popmin_sha is None:
            self._popmin_sha = await self._r.script_load(_LUA_POPMIN)
        return self._popmin_sha

    async def _get_refresh_sha(self) -> str:
        if self._refresh_sha is None:
            self._refresh_sha = await self._r.script_load(_LUA_REFRESH_SCORES)
        return self._refresh_sha

    async def enqueue(self, job_id: str, priority: int) -> None:
        now = time.time()
        score = self._compute_score(priority, now)
        async with self._r.pipeline(transaction=True) as pipe:
            pipe.zadd(self._queue_key, {job_id: score})
            pipe.hset(self._enqueue_time_key, job_id, str(now))
            await pipe.execute()

    async def dequeue(self) -> str | None:
        """Pop the highest-priority job (lowest score) atomically via Lua."""
        sha = await self._get_popmin_sha()
        result = await self._r.evalsha(sha, 1, self._queue_key)
        if result is None:
            return None
        job_id = result.decode() if isinstance(result, bytes) else result
        await self._r.hdel(self._enqueue_time_key, job_id)
        return job_id

    async def remove(self, job_id: str) -> None:
        async with self._r.pipeline(transaction=True) as pipe:
            pipe.zrem(self._queue_key, job_id)
            pipe.zrem(self._retry_key, job_id)
            pipe.hdel(self._enqueue_time_key, job_id)
            await pipe.execute()

    async def enqueue_retry(self, job_id: str, retry_at: float) -> None:
        await self._r.zadd(self._retry_key, {job_id: retry_at})

    async def promote_due_retries(self) -> list[str]:
        """Move retries whose time has come back to the main queue.

        Returns the list of promoted job IDs.
        """
        now = time.time()
        due = await self._r.zrangebyscore(self._retry_key, "-inf", now)
        if not due:
            return []

        promoted: list[str] = []
        for raw in due:
            job_id = raw.decode() if isinstance(raw, bytes) else raw
            await self._r.zrem(self._retry_key, job_id)
            promoted.append(job_id)
        return promoted

    async def depth(self) -> int:
        return await self._r.zcard(self._queue_key)

    async def retry_depth(self) -> int:
        return await self._r.zcard(self._retry_key)

    async def refresh_scores(self) -> None:
        """Re-score all queued jobs to apply time-based aging.

        Called periodically by the scheduler so that long-waiting
        low-priority jobs gradually bubble up.

        Uses a Lua script so that the ZSCORE check and ZADD happen
        atomically inside Redis — a dequeued job can never be
        accidentally re-added.
        """
        all_entries = await self._r.hgetall(self._enqueue_time_key)
        if not all_entries:
            return

        scores = await self._r.zrange(self._queue_key, 0, -1, withscores=True)
        if not scores:
            return

        current_scores: dict[str, float] = {}
        for raw_id, score in scores:
            job_id = raw_id.decode() if isinstance(raw_id, bytes) else raw_id
            current_scores[job_id] = score

        now = time.time()
        args: list[str | float] = []
        count = 0
        for raw_id, raw_time in all_entries.items():
            job_id = raw_id.decode() if isinstance(raw_id, bytes) else raw_id
            if job_id not in current_scores:
                continue
            enqueue_time = float(raw_time.decode() if isinstance(raw_time, bytes) else raw_time)
            original_priority = int(current_scores[job_id] // 1000) if current_scores[job_id] >= 0 else 0
            new_score = self._compute_score(original_priority, enqueue_time, now)
            args.extend([job_id, new_score])
            count += 1

        if count == 0:
            return

        sha = await self._get_refresh_sha()
        await self._r.evalsha(sha, 1, self._queue_key, count, *args)

    def _compute_score(
        self, priority: int, enqueue_time: float, now: float | None = None
    ) -> float:
        """Lower score = higher effective priority.

        score = priority * 1000 - (now - enqueue_time) * aging_rate
        """
        if now is None:
            now = time.time()
        age = now - enqueue_time
        return priority * 1000 - age * settings.priority_aging_rate
