from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    redis_url: str = "redis://localhost:6379/0"
    sqlite_db_path: str = "./simulation.db"

    max_workers: int = 4
    queue_overload_threshold: int = 100

    heartbeat_interval_sec: int = 2
    heartbeat_timeout_sec: int = 10
    orphan_check_interval_sec: int = 5

    retry_base_delay_sec: float = 2.0
    retry_max_delay_sec: float = 60.0
    max_retries_default: int = 3

    priority_aging_rate: float = 0.1

    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_recovery_sec: int = 30

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
