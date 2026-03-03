"""Per-job-type circuit breaker.

Tracks failure rates over a sliding window and trips open when a
threshold is exceeded, preventing new jobs of that type from running
until a recovery period elapses.

States:
  CLOSED    -> normal operation
  OPEN      -> failures exceeded threshold; reject new work
  HALF_OPEN -> recovery period elapsed; allow one probe job
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum

from config import settings


class CBState(str, Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


@dataclass
class _BreakerData:
    state: CBState = CBState.CLOSED
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: float = 0.0
    last_transition_time: float = field(default_factory=time.time)
    probe_in_flight: bool = False


class CircuitBreakerRegistry:
    """In-process registry of per-job-type circuit breakers."""

    def __init__(
        self,
        failure_threshold: int = settings.circuit_breaker_failure_threshold,
        recovery_sec: int = settings.circuit_breaker_recovery_sec,
    ) -> None:
        self._threshold = failure_threshold
        self._recovery = recovery_sec
        self._breakers: dict[str, _BreakerData] = {}

    def _get(self, job_type: str) -> _BreakerData:
        if job_type not in self._breakers:
            self._breakers[job_type] = _BreakerData()
        return self._breakers[job_type]

    def allow(self, job_type: str) -> bool:
        b = self._get(job_type)
        if b.state == CBState.CLOSED:
            return True
        if b.state == CBState.OPEN:
            if time.time() - b.last_transition_time >= self._recovery:
                b.state = CBState.HALF_OPEN
                b.probe_in_flight = True
                b.last_transition_time = time.time()
                return True
            return False
        if b.probe_in_flight:
            return False
        return True

    def record_success(self, job_type: str) -> None:
        b = self._get(job_type)
        if b.state == CBState.HALF_OPEN:
            b.state = CBState.CLOSED
            b.failure_count = 0
            b.success_count = 0
            b.probe_in_flight = False
            b.last_transition_time = time.time()
        b.success_count += 1

    def record_failure(self, job_type: str) -> None:
        b = self._get(job_type)
        b.failure_count += 1
        b.last_failure_time = time.time()

        if b.state == CBState.HALF_OPEN:
            b.state = CBState.OPEN
            b.probe_in_flight = False
            b.last_transition_time = time.time()
            return

        if b.failure_count >= self._threshold:
            b.state = CBState.OPEN
            b.last_transition_time = time.time()

    def get_states(self) -> dict[str, str]:
        return {jt: b.state.value for jt, b in self._breakers.items()}
