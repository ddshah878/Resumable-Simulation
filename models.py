from __future__ import annotations

import enum
from typing import Any

from pydantic import BaseModel, Field


class JobStatus(str, enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    PAUSED = "PAUSED"
    CANCELLED = "CANCELLED"


class Priority(int, enum.Enum):
    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3


class JobCreate(BaseModel):
    job_type: str = "simulation"
    total_steps: int = Field(default=10, ge=1, le=10000)
    priority: Priority = Priority.NORMAL
    max_retries: int = Field(default=3, ge=0, le=100)
    params: dict[str, Any] = Field(default_factory=dict)


class JobResponse(BaseModel):
    id: str
    job_type: str
    status: JobStatus
    priority: int
    total_steps: int
    current_step: int
    progress: float
    result: Any | None = None
    error: str | None = None
    retry_count: int
    max_retries: int
    created_at: float
    updated_at: float
    started_at: float | None = None
    completed_at: float | None = None


class JobListResponse(BaseModel):
    jobs: list[JobResponse]
    total: int


class CheckpointResponse(BaseModel):
    step: int
    state: Any
    created_at: float


class JobDetailResponse(JobResponse):
    checkpoints: list[CheckpointResponse] = []
    params: dict[str, Any] = Field(default_factory=dict)


class HealthResponse(BaseModel):
    status: str
    queue_depth: int
    active_workers: int
    max_workers: int
    worker_utilization: float
    redis_connected: bool
    sqlite_connected: bool
    circuit_breakers: dict[str, str] = Field(default_factory=dict)
