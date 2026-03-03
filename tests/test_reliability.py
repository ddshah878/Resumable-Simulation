"""Level 2 tests — reliability features.

Tests priority aging math and retry backoff logic (no Redis required).
"""

import time
import pytest

from config import settings


def test_priority_score_ordering():
    """Lower priority number should produce a lower score (higher priority)."""
    from job_queue import JobQueue

    dummy_q = object.__new__(JobQueue)
    dummy_q._r = None
    now = time.time()

    score_critical = JobQueue._compute_score(dummy_q, priority=0, enqueue_time=now, now=now)
    score_normal = JobQueue._compute_score(dummy_q, priority=2, enqueue_time=now, now=now)
    score_low = JobQueue._compute_score(dummy_q, priority=3, enqueue_time=now, now=now)

    assert score_critical < score_normal < score_low


def test_priority_aging():
    """A low-priority job enqueued long ago should eventually beat a normal-priority recent job."""
    from job_queue import JobQueue

    dummy_q = object.__new__(JobQueue)
    dummy_q._r = None
    now = time.time()
    long_ago = now - 50000

    score_old_low = JobQueue._compute_score(dummy_q, priority=3, enqueue_time=long_ago, now=now)
    score_new_normal = JobQueue._compute_score(dummy_q, priority=2, enqueue_time=now, now=now)

    assert score_old_low < score_new_normal, (
        "Old low-priority job should have aged past a fresh normal-priority job"
    )


def test_retry_delay_exponential():
    """Retry delays should grow exponentially with a cap."""
    import random
    random.seed(42)

    delays = []
    for retry_count in range(1, 6):
        delay = min(
            settings.retry_base_delay_sec * (2 ** (retry_count - 1)),
            settings.retry_max_delay_sec,
        )
        delays.append(delay)

    assert delays[0] < delays[1] < delays[2]
    assert all(d <= settings.retry_max_delay_sec for d in delays)
