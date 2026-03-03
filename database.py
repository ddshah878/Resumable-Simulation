import aiosqlite

from config import settings

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    job_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING',
    priority INTEGER NOT NULL DEFAULT 2,
    total_steps INTEGER NOT NULL,
    current_step INTEGER NOT NULL DEFAULT 0,
    result TEXT,
    error TEXT,
    params TEXT NOT NULL DEFAULT '{}',
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    started_at REAL,
    completed_at REAL
);

CREATE TABLE IF NOT EXISTS checkpoints (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL REFERENCES jobs(id),
    step INTEGER NOT NULL,
    state TEXT NOT NULL,
    created_at REAL NOT NULL,
    UNIQUE(job_id, step)
);

CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_priority ON jobs(priority);
CREATE INDEX IF NOT EXISTS idx_checkpoints_job_id ON checkpoints(job_id);
"""


async def get_db() -> aiosqlite.Connection:
    db = await aiosqlite.connect(settings.sqlite_db_path)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA busy_timeout=5000")
    return db


async def init_db() -> None:
    db = await get_db()
    try:
        await db.executescript(_SCHEMA)
        await db.commit()
    finally:
        await db.close()
