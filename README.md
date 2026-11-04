# ⏱️ Distributed Job Scheduler

A fault-tolerant, horizontally scalable distributed cron and background job execution engine built with **Go**, **TypeScript/Node.js**, **Apache Kafka (KRaft)**, **PostgreSQL**, **Redis**, and **etcd**.

Designed from first principles to explore and solve distributed systems fundamentals: **at-least-once message delivery**, **exactly-once execution semantics via database-backed idempotency**, **distributed consensus & leader election**, **full-jitter exponential backoff**, and **dead-letter queuing (DLQ)**.

---

## 🏛️ System Architecture

```
                                  +-----------------------------+
                                  |   Client / Admin / Webhook  |
                                  +--------------+--------------+
                                                 |
                                                 | HTTP (JSON REST)
                                                 v
                                  +-----------------------------+
                                  |   Control Plane (Node/TS)   |
                                  |   - Express MVC             |
                                  |   - Cron & Timezone parser  |
                                  |   - Job Lifecycle (CRUD)    |
                                  +--------------+--------------+
                                                 |
                                                 | PostgreSQL ACID
                                                 v
  +---------------------------------------------------------------------------------------+
  |                                   PostgreSQL 16 (SSOT)                                |
  |  - jobs (cron rules, payload, next_run_at)     - workers (heartbeats & liveness)      |
  |  - job_runs (UNIQUE(job_id, scheduled_for))    - run_logs (structured execution logs) |
  +---------------------------------------------------------------------------------------+
            ^                                                                ^
            | Polls due jobs (DB time)                                       | DB state updates
            | & retry sweep (SKIP LOCKED)                                    | (Conditional fencing)
            v                                                                |
+--------------------------+                                      +----------+------------+
|  Scheduler Engine (Go)   |                                      |   Worker Engine (Go)  |
|  - etcd Leader Election  |                                      |   - Consumer Group    |
|  - Bootstrap & Clock Sync|                                      |   - Distributed Lock  |
|  - Retry Sweeper Loop    |                                      |     (Redis SET NX PX) |
+-------------+------------+                                      |   - Jitter Backoff    |
              |                                                   +----------+------------+
              | Publishes due runs (Key: job_id)                             ^
              v                                                              |
+----------------------------------------------------------------------------+------------+
|                               Apache Kafka (KRaft Broker)                               |
|   - Topic `job_runs` (6 Partitions, keyed by job_id for FIFO order per job)             |
|   - Topic `job_runs.dlq` (1 Partition, poisoned / exhausted messages)                   |
+-----------------------------------------------------------------------------------------+
```

---

## 🚀 Key Architectural Guarantees & Features

### 1. Exactly-Once Execution Semantics (via Idempotency Guards)
* Distributed networks are inherently **at-least-once**. To prevent duplicate execution caused by network retries or consumer group rebalances, the database enforces a strict `UNIQUE (job_id, scheduled_for)` composite constraint on `job_runs`.
* If a scheduler process crashes after updating `next_run_at` but before receiving confirmation, subsequent election or tick attempts hit `ON CONFLICT DO NOTHING`, safely skipping re-execution.
* Kafka consumer offsets are committed **strictly after** the database state transition is persisted.

### 2. Full-Jitter Exponential Backoff (Thundering Herd Protection)
* Failures don't re-execute in lockstep. Retries use full-jitter exponential backoff:
  $$\text{Delay} = \text{random}(0,\, \min(M, B \times 2^{\text{attempt}}))$$
* Prevents downstream cascading outages and database stampedes when shared external dependencies fail simultaneously.

### 3. Non-Blocking Concurrency via PostgreSQL `SKIP LOCKED`
* The retry engine implements an **outbox sweeper** polling for due runs (`status = 'RETRYING' AND next_retry_at <= now()`).
* Queries utilize `FOR UPDATE OF r SKIP LOCKED`, enabling multiple scheduler instances or threads to process retry queues concurrently without lock contention or thread starvation.

### 4. Dead Letter Queue (DLQ) & Poison-Pill Isolation
* Transient errors retry with backoff up to `max_retries`. Permanent failures transition to `DEAD` and are automatically published as JSON envelopes (`DeadLetterMessage`) to the dedicated `job_runs.dlq` Kafka topic for operator inspection, root-cause analysis, and offline replaying.

### 5. High Availability & Distributed Coordination
* **etcd Lease Leader Election**: Guarantees a single active scheduler instance is driving ticks, eliminating double-ticking split-brain scenarios while supporting instant failover upon lease expiry.
* **Redis Distributed Locks**: Workers lock runs during execution (`SET NX PX`) with atomic Lua compare-and-delete unlock scripts to prevent zombie worker overrides.

---

## 🛠️ Tech Stack & Distributed Components

| Layer | Technology | Primary Responsibility |
|---|---|---|
| **Scheduler Engine** | **Go (Golang 1.22)** | Due-job polling, cron evaluation, Kafka publisher, retry sweeper |
| **Worker Engine** | **Go (Golang 1.22)** | Kafka consumer group, execution runner, backoff retry scheduler, DLQ emitter |
| **Control Plane API** | **Node.js, TypeScript, Express** | Production MVC REST API: Job validation, lifecycle control (Pause/Resume/Delete) |
| **Primary Storage** | **PostgreSQL 16** | System of record: ACID guarantees, temporal queries, run statuses |
| **Event Stream / Queue** | **Apache Kafka 3.7 (KRaft)** | Low-latency, high-throughput partitioned messaging buffer |
| **Distributed Locks / Cache**| **Redis 7 (Alpine)** | Distributed run locking (`SET NX PX`), heartbeats, lease management |
| **Consensus / Coordination** | **etcd v3.5** | Distributed leader election lease mechanism |
| **Containerization** | **Docker & Docker Compose** | Multi-container reproducible local and production infrastructure |

---

## 📂 Repository Structure

```
.
├── api/                        # Control Plane API (TypeScript + Node.js)
│   ├── src/
│   │   ├── controllers/        # Express HTTP controllers (MVC pattern)
│   │   ├── routes/             # REST endpoints (/health, /jobs)
│   │   ├── services/           # Business logic, cron parsing, IANA timezones
│   │   ├── db/                 # pg connection pool
│   │   └── app.ts              # Express application configuration
│   └── tests/                  # Unit and integration test suites
│
├── go-scheduler/               # Execution Plane Services (Go)
│   ├── cmd/
│   │   ├── scheduler/          # Scheduler binary entrypoint
│   │   └── worker/             # Worker binary entrypoint
│   ├── db/                     # sqlc generated queries & type-safe models
│   │   └── query/              # Raw SQL queries (jobs.sql, runs.sql)
│   └── internal/
│       ├── config/             # Environment configuration parser
│       ├── cronutil/           # Cron parsing & next-run calculation
│       ├── db/                 # pgx connection pool
│       ├── election/           # etcd lease-based leader election
│       ├── kafka/              # Kafka consumer/writer abstractions & message schemas
│       ├── redis/              # Redis connection & locking helpers
│       ├── retry/              # Full-jitter backoff math & algorithms
│       ├── scheduler/          # Core scheduler tick loop & retry sweeper
│       └── worker/             # Worker consumer loop, execution sandbox, & DLQ
│
├── migrations/                 # Versioned SQL migrations (golang-migrate)
│   ├── 000001_init.up.sql      # Schema definitions, enum types, indexes, foreign keys
│   └── 000001_init.down.sql    # Teardown script
│
├── docker-compose.yml          # Infrastructure topology (Postgres, Kafka, Redis, etcd)
├── Makefile                    # Unified developer task runner
└── PROJECT.md                  # System specification & engineering decisions
```

---

## 🚦 Quick Start Guide

### Prerequisites
- Docker & Docker Compose
- Go 1.22+
- Node.js 20+ & npm

### 1. Clone & Bootstrap Infrastructure
```bash
# 1. Clone the repository
git clone https://github.com/Prashantbhati7/job-scheduler.git
cd job-scheduler

# 2. Configure environment
cp .env.example .env

# 3. Spin up all distributed infrastructure (Postgres, Kafka, Redis, etcd, Kafka UI)
make up

# 4. Run database migrations
make migrate-up
```

Verify running containers:
```bash
docker compose ps
```
Services available:
- **Postgres**: `localhost:5432`
- **Redis**: `localhost:6379`
- **Kafka**: `localhost:9092`
- **etcd**: `localhost:2379`
- **Kafka UI**: [http://localhost:8080](http://localhost:8080) (Inspect topics, consumer lag, and partitions)

---

### 2. Run the Services

Open three terminal tabs:

#### Tab 1: Control Plane API (TypeScript)
```bash
make api-dev
# Server listening on http://localhost:3000
```

#### Tab 2: Scheduler Engine (Go)
```bash
make scheduler-test
# Polls due jobs, publishes runs to Kafka, and sweeps retrying runs
```

#### Tab 3: Worker Engine (Go)
```bash
make worker-test
# Consumes from Kafka 'job_runs', acquires run lock, executes job, handles retries
```

---

## 📡 API Reference & Usage

### 1. Create a Scheduled Job
```bash
curl -X POST http://localhost:3000/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "name": "data-warehouse-sync",
    "cron_expr": "*/2 * * * *",
    "timezone": "UTC",
    "payload": {
      "table": "orders",
      "batch_size": 500
    },
    "priority": 1,
    "max_retries": 3,
    "timeout_seconds": 30
  }'
```

**Response (201 Created):**
```json
{
  "id": "e8df5e8c-843b-4ce3-a226-5be9ff3b26cb",
  "name": "data-warehouse-sync",
  "cron_expr": "*/2 * * * *",
  "timezone": "UTC",
  "payload": { "table": "orders", "batch_size": 500 },
  "priority": 1,
  "max_retries": 3,
  "timeout_seconds": 30,
  "enabled": true,
  "next_run_at": "2026-10-28T02:00:00.000Z",
  "created_at": "2026-10-28T01:58:12.431Z",
  "updated_at": "2026-10-28T01:58:12.431Z",
  "deleted_at": null
}
```

### 2. Pause a Job
```bash
curl -X PATCH http://localhost:3000/jobs/e8df5e8c-843b-4ce3-a226-5be9ff3b26cb/pause
```

### 3. Resume a Job
```bash
curl -X PATCH http://localhost:3000/jobs/e8df5e8c-843b-4ce3-a226-5be9ff3b26cb/resume
```

### 4. Delete a Job (Soft Delete)
```bash
curl -X DELETE http://localhost:3000/jobs/e8df5e8c-843b-4ce3-a226-5be9ff3b26cb
```

---

## 🧪 Testing & Verification

Run the entire comprehensive test suite across both Go engines and TypeScript API:

```bash
# Run all tests
make test

# Or individually:
cd go-scheduler && go test -v -count=1 ./...
cd api && npm test
```

### Verification Checks:
1. **Idempotency Check**:
   Attempting to insert two runs for the same `(job_id, scheduled_for)` results in a unique constraint violation (`uq_job_runs_job_scheduled`), confirming zero duplicate triggers.
2. **Crash Recovery & Offset Safety**:
   Worker crashes mid-execution do not lose messages; uncommitted offsets are re-consumed by surviving consumer group members.
3. **Partition Order Preservation**:
   Partition key hashing on `job_id` ensures that all ticks for a specific job execute strictly in scheduled FIFO order.

