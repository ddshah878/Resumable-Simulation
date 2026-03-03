# Resumable Simulation Pipeline

A job execution engine for scientific simulations built with FastAPI, Redis, and SQLite. You can submit jobs, pause/resume them, cancel mid-run, and everything picks up from where it left off thanks to step-level checkpointing.

## Setup & Running

**Prerequisites:**
- Python 3.12+
- Redis server on localhost:6379

If you don't have Redis installed, the easiest way is Docker:
```bash
docker run -d -p 6379:6379 --name redis redis
```

**Clone and install:**
```bash
git clone https://github.com/ddshah878/Resumable-Simulation
cd Resumable-Simulation
python -m venv venv
```

Activate the virtual environment:
```bash
# Windows
.\venv\Scripts\Activate.ps1

# Mac/Linux
source venv/bin/activate
```

Install dependencies and set up config:
```bash
pip install -r requirements.txt
```
```bash
copy .env.example .env    # Windows
cp .env.example .env      # Mac/Linux
```

**Start the server:**
```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

Swagger docs come up at http://localhost:8000/docs once the server's running.

All config lives in `.env` — things like Redis URL, max workers, retry delays, circuit breaker thresholds, etc. Check `.env.example` for the full list with defaults.

**Run tests** (no Redis required):
```bash
python -m pytest tests/ -v
```

## What I Implemented

**Core (Level 1):**
- Async job queue — submit jobs that run as concurrent asyncio tasks
- Persistent state in SQLite — jobs and checkpoints survive restarts
- Full status API with filtering and pagination
- Clean cancellation — workers check a Redis flag between steps, never killed mid-computation
- Checkpoint-based resume — paused or crashed jobs pick up from the last committed step

**Level 2 — Reliability:**
- Priority queue with anti-starvation aging (Redis sorted sets, low-priority jobs gradually bubble up over time)
- Retry with exponential backoff + jitter
- Overload protection — returns 429 when queue gets too deep
- Per-job-type circuit breaker (CLOSED → OPEN → HALF_OPEN → CLOSED)
- Heartbeat-based orphan recovery for crashed workers

**Known limitations:**
- Single-node only — SQLite doesn't support multi-node, and the circuit breaker state is in-process memory. Would need Postgres + Redis-backed breaker state for horizontal scaling.
- Level 3 Intelligence is a design write-up only — not implemented. Dashboard UI and WebSocket live updates are also not built yet.
- The simulation tasks are I/O-bound fakes (`asyncio.sleep`). Real CPU-bound workloads would need `ProcessPoolExecutor` instead — the worker interface is designed so that swap would be straightforward though.

## Architectural Decisions

**SQLite over Postgres:** I went with SQLite in WAL mode because it's zero-config, ACID-compliant, and totally fine for a single-node demo. It handles concurrent reads while writing. The schema is plain SQL so moving to Postgres later wouldn't require rewriting anything meaningful — just swapping the connection layer.

**Redis for the queue instead of Celery:** Redis sorted sets give O(log N) priority insertion and pop-min, which maps perfectly to a priority queue with aging. I wrote a small Lua script for atomic dequeue (`ZRANGE` + `ZREM` in one call) so it's compatible with Redis 3.0+ including the Windows port — didn't want to depend on `ZPOPMIN` which needs Redis 5.0+. Celery would've hidden the scheduling logic behind its own abstractions, and the whole point here was to own those decisions.

**Asyncio workers over multiprocessing:** Since the simulations are I/O-bound (sleep-based), asyncio made more sense. Cooperative cancellation is simple — just check a flag between steps. No inter-process signaling headaches. For real CPU-bound work, you'd swap in `ProcessPoolExecutor` but the worker interface stays the same.

**Correctness guarantee:** After each simulation step, the worker writes the checkpoint and updates job progress in a single SQLite transaction. If a worker dies between step 5 and 6, the job resumes from step 5. Worst case you re-execute one step, but each step is idempotent (checkpoint just overwrites), so that's fine.

## Level 3 — Intelligence (Not Implemented)

I didn't get to this one, but I've thought through how I'd approach it.

The idea is a `POST /plan` endpoint that takes a plain-English instruction, sends it to an LLM with structured output, and gets back a DAG of sub-jobs — each with an id, type, params, and dependencies. I'd feed the LLM the list of available job types so it only plans things the system can actually run. Before executing anything, validate the graph: check for cycles, make sure dependency references are valid, confirm the job types exist.

For execution, it maps pretty cleanly onto the existing queue. Start by enqueuing everything with zero dependencies. As each sub-job finishes, check if it unblocks any dependents and enqueue those. Independent branches run in parallel through the same worker pool. If something fails, block the downstream subtree and let the user retry just the failed piece. Sub-jobs would pass results forward through a JSON artifacts column — the executor injects parent outputs into child params before enqueuing.

The harder question is output quality. I'd layer a few things: schema validation on each job's output to catch structural issues, an optional LLM-as-judge pass at the end to score whether the final result actually matches the original intent, and a human approval gate for anything high-stakes (the pause/resume infrastructure already supports this). None of these are bulletproof on their own, but together they cover most failure modes.

## What I'd Do Differently With More Time

I'd add a WebSocket-based live progress feed. Right now you have to poll `GET /jobs/{id}` to watch a job run, which is clunky. A WebSocket connection per job (or a single multiplexed one) that streams checkpoint updates as they happen would make the whole thing feel much more responsive. I'd also build a small React dashboard on top of that — being able to see jobs running, pause one with a click, watch the progress bars fill up in real time. The backend is already structured to support it (the checkpoint writes are the natural event source), I just ran out of time to wire it up.
