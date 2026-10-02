# PROJECT.md — Distributed Job Scheduler

> Context file for me and for any AI assistant working in this repo.
> Read this fully before doing anything. Update "Current milestone" as we progress.

## 1. What we are building

A distributed cron system. Users define jobs with a cron expression. One elected
scheduler decides when runs are due and publishes them to Kafka. A pool of workers
executes them. The system must never lose a run and must avoid running one twice.

Goal of the project: **learn distributed-systems fundamentals** (leader election,
at-least-once delivery, idempotency, locks, retries, backpressure) and be able to
explain them in interviews. Correctness and understanding matter more than speed.

## 2. Stack (hybrid)

| Part | Language / tool | Responsibility |
|---|---|---|
| `scheduler` | Go | Leader election, find due jobs, create runs, publish to Kafka, retry sweeper, reaper |
| `worker` | Go | Consume runs, Redis lock, execute handler, retry/backoff, report results |
| `api` | TypeScript, Node.js, Express | CRUD for jobs, read runs and logs, health endpoints |
| `dashboard` | TypeScript, React + Vite (later, step 7) | Jobs list, run history, logs |
| Data | PostgreSQL | Source of truth: jobs, runs, workers, logs |
| Messaging | Kafka (KRaft mode, no ZooKeeper) | Durable buffer between scheduler and workers |
| Locks / heartbeats | Redis | Run locks (`SET NX PX`), worker heartbeats |
| Coordination | etcd | Leader election lease |
| Packaging | Docker + Docker Compose | Everything runs locally with one command |

Boundary rule: **Go services and the TS API never call each other directly.**
They communicate only through PostgreSQL (shared schema) and Kafka (message contract).
The shared contract lives in `contracts/`.

## 3. Hard rules (do not violate)

1. Delivery is **at-least-once**. Every job handler **must be idempotent**.
2. All timestamps are `timestamptz`, stored and handled in **UTC**. Cron expressions
   are interpreted in the job's `timezone`, but computed times are converted to UTC.
3. Scheduling decisions use **database time** (`now()` in Postgres), not app clocks.
4. `UNIQUE (job_id, scheduled_for)` on `job_runs` is the final guard against
   double scheduling. Never remove it.
5. Commit a Kafka offset only **after** the DB state for that message is saved.
6. Releasing a Redis lock must be compare-and-delete (only the owner releases).
7. Every worker DB update on a run is conditional on
   `status = RUNNING AND worker_id = me AND attempt = n` (fencing against zombie workers).
8. Schema changes only through numbered migration files. Never edit an applied migration.
9. No secrets in git. Config comes from environment variables (`.env.example` is committed).
10. Prefer small, tested steps. Explain failure cases before writing code.

## 4. Repository layout (target)

```
job-scheduler/
├── PROJECT.md
├── README.md
├── Makefile
├── docker-compose.yml
├── .env.example
├── .gitignore
├── contracts/               # Kafka message schema, API OpenAPI spec (shared truth)
├── migrations/              # numbered SQL migrations (000001_init.up.sql / .down.sql)
├── go/                      # single Go module
│   ├── go.mod
│   ├── cmd/
│   │   ├── scheduler/       # main.go
│   │   └── worker/          # main.go
│   └── internal/
│       ├── config/          # env loading
│       ├── db/              # pgx pool, queries
│       ├── kafka/
│       ├── lock/            # Redis lock helpers
│       ├── election/        # etcd leader election
│       └── cron/            # cron parsing, next-run computation
├── api/                     # Node + TypeScript + Express
│   ├── package.json
│   ├── tsconfig.json
│   └── src/
├── dashboard/               # added in step 7
└── deploy/
    ├── go.Dockerfile        # one Dockerfile, build arg picks scheduler or worker
    └── api.Dockerfile
```

Step 0 only creates the skeleton, infra and migrations. Service folders may contain
a minimal "hello + health check" entry point and nothing more.

## 5. Data model (created by migration 000001 in step 0)

### `jobs`
| Column | Type | Notes |
|---|---|---|
| id | uuid PK | default `gen_random_uuid()` |
| name | text | unique among non-deleted jobs |
| cron_expr | text | validated before insert |
| timezone | text | IANA name, default `UTC` |
| payload | jsonb | passed to the handler |
| priority | smallint | default 5; lower number = higher priority |
| max_retries | int | default 3 |
| timeout_seconds | int | default 60 |
| enabled | boolean | default true |
| next_run_at | timestamptz | the only field the scheduler polls |
| created_at, updated_at | timestamptz | default `now()` |
| deleted_at | timestamptz null | soft delete |

Index: `(enabled, next_run_at)` where `deleted_at IS NULL`.

### `job_runs`
| Column | Type | Notes |
|---|---|---|
| id | uuid PK | also the idempotency key |
| job_id | uuid FK -> jobs | |
| scheduled_for | timestamptz | the cron tick this run represents |
| status | text / enum | `PENDING, QUEUED, RUNNING, SUCCEEDED, FAILED, RETRYING, DEAD` |
| attempt | int | starts at 0 |
| worker_id | text null | |
| next_retry_at | timestamptz null | set when `RETRYING` |
| queued_at, started_at, finished_at | timestamptz null | |
| error | text null | last error |
| created_at | timestamptz | default `now()` |

Constraints and indexes:
- `UNIQUE (job_id, scheduled_for)`
- `(status, next_retry_at)` for the retry sweep
- `(job_id, created_at DESC)` for the dashboard

### `workers`
`id text PK`, `host text`, `status text` (`ACTIVE`/`DEAD`), `started_at`, `last_heartbeat`.

### `run_logs`
`id bigserial PK`, `run_id uuid FK`, `attempt int`, `ts timestamptz`, `level text`, `message text`.
Index: `(run_id, ts)`.

### Non-Postgres state
- Redis `lock:run:{run_id}` = worker id, TTL renewed while running.
- Redis `hb:worker:{worker_id}` TTL ~15s, refreshed every ~5s.
- etcd `/scheduler/leader` election key under a lease.
- Kafka topics: `job_runs` (6 partitions, key = `job_id`), `job_runs.dlq` (1 partition).

## 6. Local infrastructure (Docker Compose)

| Service | Purpose | Host port |
|---|---|---|
| postgres | database | 5432 |
| redis | locks, heartbeats | 6379 |
| kafka | broker, KRaft single node | 9092 (host), 29092 (inside Docker network) |
| kafka-init | one-shot job that creates the topics, then exits | none |
| etcd | leader election | 2379 |
| kafka-ui | inspect topics and consumer lag | 8080 |

Notes for whoever writes the compose file:
- Pin image versions explicitly (no `latest`). Check the current stable tags first.
- Kafka needs **two listeners**: one for containers (`kafka:29092`) and one for the host
  (`localhost:9092`). Getting this wrong is the most common local Kafka failure.
- Every stateful service gets a named volume and a healthcheck.
- App services (api, scheduler, worker) are added to compose **later**. In step 0, only
  infra runs in Docker; app code runs on the host.
- A separate compose profile for app services is welcome but optional in step 0.

## 7. Environment variables (`.env.example`)

```
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=scheduler
POSTGRES_PASSWORD=change_me
POSTGRES_DB=scheduler
REDIS_ADDR=localhost:6379
KAFKA_BROKERS=localhost:9092
KAFKA_TOPIC_RUNS=job_runs
KAFKA_TOPIC_DLQ=job_runs.dlq
ETCD_ENDPOINTS=localhost:2379
API_PORT=3000
LOG_LEVEL=info
```

## 8. Current milestone: STEP 0 — Project setup

### Tasks
- [ ] Create the repo, `.gitignore`, `README.md`, and the folder layout from section 4
- [ ] `docker-compose.yml` with postgres, redis, kafka (KRaft), kafka-init, etcd, kafka-ui
- [ ] `.env.example` from section 7 (real `.env` is git-ignored)
- [ ] Migration `000001_init` creating all four tables, enum/status check, constraints, indexes
- [ ] A migration command (via `golang-migrate` CLI or a Makefile target using its Docker image)
- [ ] `Makefile` targets: `up`, `down`, `reset` (down + delete volumes), `logs`, `migrate-up`,
      `migrate-down`, `psql`, `topics`
- [ ] Go module initialised; `cmd/scheduler` and `cmd/worker` each start, load config,
      connect to Postgres, Redis, Kafka and etcd, print one log line per connection, and exit cleanly
- [ ] TS API initialised (Express + TypeScript, strict mode); `GET /health` checks Postgres
      and returns JSON
- [ ] `README.md` explains how to start from a clean clone in under 5 commands

### Definition of done
1. From a fresh clone: `cp .env.example .env && make up && make migrate-up` succeeds.
2. `make psql` then `\dt` shows `jobs`, `job_runs`, `workers`, `run_logs`.
3. Inserting two `job_runs` rows with the same `(job_id, scheduled_for)` fails with a
   unique violation (verify manually and note it in the README).
4. `make topics` lists `job_runs` (6 partitions) and `job_runs.dlq`.
5. Kafka UI at `localhost:8080` loads and shows both topics.
6. Both Go binaries log successful connections to all four dependencies, then exit 0.
7. `curl localhost:3000/health` returns 200 with `{ "status": "ok", "db": "up" }`.
8. `make reset && make up && make migrate-up` works again from nothing.

### Out of scope for step 0
Cron parsing, scheduling loops, producing or consuming messages, locks, leader election,
CRUD endpoints, the dashboard, metrics. Do not build ahead.

## 9. Roadmap

- [ ] **0. Setup** (current)
- [ ] 1. Single process: poll `jobs`, compute next run, run dummy handler, write `job_runs`
- [ ] 2. API: create / pause / resume / delete jobs
- [ ] 3. Split scheduler and worker through Kafka
- [ ] 4. Retries with exponential backoff + jitter, dead letter topic
- [ ] 5. Redis locks, lock renewal, idempotency checks
- [ ] 6. Leader election with etcd, run two schedulers and kill one
- [ ] 7. Dashboard: run history and logs
- [ ] 8. Chaos test: kill random workers and schedulers, verify exactly one `SUCCEEDED` per tick

Add basic metrics (queue lag, run latency, retries, leader changes) starting at step 3.

## 10. How the AI should work in this repo

1. Start every task by restating the goal, listing assumptions and **failure cases**. No code
   until I say "go".
2. Work one milestone at a time. Do not implement later steps early.
3. Show the plan for any file or schema change before applying it.
4. Every behaviour change comes with a test (Go: table-driven tests; TS: Vitest or Jest).
5. Explain *why* a design choice is made, in a couple of sentences, so I can repeat it in an interview.
6. When something is uncertain (library API, image tag, config flag), say so and tell me to verify
   in the official docs instead of guessing.
7. Keep functions small, log structured JSON, return errors with context. Handle graceful shutdown
   (SIGTERM) in every long-running process.

## 11. Glossary

- **At-least-once:** a message may be delivered more than once, never zero times.
- **Idempotent:** running the same operation twice has the same effect as running it once.
- **Lease:** a time-limited claim (etcd) that must be renewed or it expires.
- **Fencing:** rejecting actions from a stale owner, here via `worker_id` + `attempt` checks.
- **DLQ:** dead letter queue; where runs go after exhausting retries.
- **Outbox / sweeper:** a DB-backed safety net that republishes runs that were saved but never sent.