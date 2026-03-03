"""Level 1 tests — core job lifecycle without Redis.

Tests the simulation module, database layer, and circuit breaker
in isolation (no Redis required).
"""

import json
import os
import time

import aiosqlite
import pytest
import pytest_asyncio

from config import settings
from circuit_breaker import CircuitBreakerRegistry
from simulation import run_step

DB_PATH = os.environ.get("SQLITE_DB_PATH", "./test_simulation.db")


@pytest_asyncio.fixture(autouse=True)
async def setup_db():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    settings.sqlite_db_path = DB_PATH
    from database import init_db
    await init_db()
    yield
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)


# ── Simulation tests ──


async def test_simulation_step_basic():
    state = await run_step("simulation", 0, 10, None, {"target": 100.0})
    assert "value" in state
    assert "residual" in state
    assert isinstance(state["value"], float)


async def test_simulation_convergence():
    prev = None
    for step in range(10):
        prev = await run_step("simulation", step, 10, prev, {"target": 50.0})
    assert prev["residual"] < 10.0


async def test_monte_carlo_step():
    state = await run_step("montecarlo", 0, 1, None, {"samples_per_step": 500})
    assert "pi_estimate" in state
    assert 2.0 < state["pi_estimate"] < 4.0


async def test_matrix_step():
    state = await run_step("matrix", 0, 1, None, {"decay_rate": 0.5})
    assert "norm" in state
    assert state["norm"] < 1000.0


async def test_simulation_failure_injection():
    with pytest.raises(RuntimeError, match="Injected failure"):
        await run_step("simulation", 3, 10, None, {"fail_at_step": 3})


# ── Database tests ──


async def test_job_insert_and_read():
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    try:
        now = time.time()
        await db.execute(
            """INSERT INTO jobs
               (id, job_type, status, priority, total_steps, current_step,
                params, retry_count, max_retries, created_at, updated_at)
               VALUES (?,?,?,?,?,0,?,0,3,?,?)""",
            ("test-1", "simulation", "PENDING", 2, 10, "{}", now, now),
        )
        await db.commit()

        async with db.execute("SELECT * FROM jobs WHERE id=?", ("test-1",)) as cur:
            row = await cur.fetchone()

        assert row["id"] == "test-1"
        assert row["status"] == "PENDING"
        assert row["total_steps"] == 10
    finally:
        await db.close()


async def test_checkpoint_insert_and_resume():
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    try:
        now = time.time()
        await db.execute(
            """INSERT INTO jobs
               (id, job_type, status, priority, total_steps, current_step,
                params, retry_count, max_retries, created_at, updated_at)
               VALUES (?,?,?,?,?,0,?,0,3,?,?)""",
            ("test-cp", "simulation", "RUNNING", 2, 5, "{}", now, now),
        )
        state = {"value": 42.0, "residual": 10.0, "converged": False}
        await db.execute(
            "INSERT INTO checkpoints (job_id, step, state, created_at) VALUES (?,?,?,?)",
            ("test-cp", 3, json.dumps(state), now),
        )
        await db.execute(
            "UPDATE jobs SET current_step=3 WHERE id=?", ("test-cp",)
        )
        await db.commit()

        async with db.execute(
            "SELECT state FROM checkpoints WHERE job_id=? AND step=?",
            ("test-cp", 3),
        ) as cur:
            row = await cur.fetchone()

        loaded = json.loads(row["state"])
        assert loaded["value"] == 42.0
    finally:
        await db.close()


# ── Circuit breaker tests ──


def test_circuit_breaker_lifecycle():
    cb = CircuitBreakerRegistry(failure_threshold=3, recovery_sec=60)

    assert cb.allow("test_type") is True

    cb.record_failure("test_type")
    cb.record_failure("test_type")
    assert cb.allow("test_type") is True

    cb.record_failure("test_type")
    assert cb.allow("test_type") is False

    states = cb.get_states()
    assert states["test_type"] == "OPEN"


def test_circuit_breaker_half_open_success():
    cb = CircuitBreakerRegistry(failure_threshold=2, recovery_sec=0)

    cb.record_failure("fast")
    cb.record_failure("fast")
    # recovery_sec=0, so immediately transitions to HALF_OPEN on next allow()
    assert cb.allow("fast") is True  # HALF_OPEN probe
    cb.record_success("fast")
    assert cb.get_states()["fast"] == "CLOSED"


def test_circuit_breaker_half_open_failure():
    cb = CircuitBreakerRegistry(failure_threshold=2, recovery_sec=0)

    cb.record_failure("flaky")
    cb.record_failure("flaky")
    assert cb.allow("flaky") is True  # HALF_OPEN probe
    cb.record_failure("flaky")  # probe failed
    assert cb.get_states()["flaky"] == "OPEN"


def test_circuit_breaker_half_open_blocks_second_probe():
    cb = CircuitBreakerRegistry(failure_threshold=2, recovery_sec=0)

    cb.record_failure("guarded")
    cb.record_failure("guarded")
    assert cb.allow("guarded") is True   # first probe allowed
    assert cb.allow("guarded") is False  # second probe blocked while first is in-flight
    assert cb.allow("guarded") is False  # still blocked

    cb.record_success("guarded")
    assert cb.allow("guarded") is True   # breaker closed, normal traffic resumes
