"""Simulated scientific computation tasks.

Each simulation type models a multi-step iterative solver.  Every step
produces an intermediate result that can be checkpointed and resumed.
"""

from __future__ import annotations

import asyncio
import math
import random
from typing import Any


async def run_step(
    job_type: str,
    step: int,
    total_steps: int,
    previous_state: dict[str, Any] | None,
    params: dict[str, Any],
) -> dict[str, Any]:
    """Execute a single simulation step and return the new state."""
    runners = {
        "simulation": _iterative_solver_step,
        "montecarlo": _monte_carlo_step,
        "matrix": _matrix_computation_step,
    }
    runner = runners.get(job_type, _iterative_solver_step)
    return await runner(step, total_steps, previous_state, params)


async def _iterative_solver_step(
    step: int,
    total_steps: int,
    prev: dict[str, Any] | None,
    params: dict[str, Any],
) -> dict[str, Any]:
    """Simulates convergence of an iterative numerical solver."""
    await asyncio.sleep(random.uniform(0.5, 2.0))

    value = prev["value"] if prev else 0.0
    target = params.get("target", 100.0)
    convergence = value + (target - value) * (1 / (total_steps - step + 1))
    residual = abs(target - convergence)

    if params.get("fail_at_step") == step:
        raise RuntimeError(f"Injected failure at step {step}")

    return {
        "value": round(convergence, 6),
        "residual": round(residual, 6),
        "converged": residual < 0.01,
    }


async def _monte_carlo_step(
    step: int,
    total_steps: int,
    prev: dict[str, Any] | None,
    params: dict[str, Any],
) -> dict[str, Any]:
    """Estimates pi via Monte Carlo sampling."""
    await asyncio.sleep(random.uniform(0.3, 1.5))

    samples_per_step = params.get("samples_per_step", 1000)
    inside = prev["inside"] if prev else 0
    total = prev["total"] if prev else 0

    for _ in range(samples_per_step):
        x, y = random.random(), random.random()
        if x * x + y * y <= 1.0:
            inside += 1
        total += 1

    estimate = 4.0 * inside / total
    error = abs(estimate - math.pi)

    return {
        "inside": inside,
        "total": total,
        "pi_estimate": round(estimate, 8),
        "error": round(error, 8),
    }


async def _matrix_computation_step(
    step: int,
    total_steps: int,
    prev: dict[str, Any] | None,
    params: dict[str, Any],
) -> dict[str, Any]:
    """Simulates iterative matrix decomposition progress."""
    await asyncio.sleep(random.uniform(0.5, 1.8))

    norm = prev["norm"] if prev else 1000.0
    decay = params.get("decay_rate", 0.7)
    norm *= decay + random.uniform(-0.05, 0.05)

    return {
        "norm": round(norm, 6),
        "iteration": step,
        "stable": norm < 1.0,
    }
