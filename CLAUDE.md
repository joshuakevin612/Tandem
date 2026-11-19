# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`taskq`: a distributed task execution platform (a mini Celery). Clients submit jobs over HTTP, workers lease and run them from a Postgres-backed queue, and a scheduler process reclaims dead workers' jobs and fires cron schedules. The headline guarantee is **at-least-once execution: an accepted job (HTTP 202) is never lost or left stuck**, even when workers are SIGKILLed. Exactly-once is not promised; fencing and idempotency keys make duplicates safe.

## Commands

On Windows Git Bash, Go may not be on PATH in a fresh shell: `export PATH="/c/Program Files/Go/bin:$PATH"`.

```bash
go build ./... && go vet ./...
go test ./... -count=1                              # needs Docker running (testcontainers starts postgres:17)
go test ./internal/store -run TestLeaseContention -count=1 -v   # single test
docker compose -f deploy/docker-compose.yml up -d --build        # postgres, redis, 2 api replicas behind nginx :8080, 3 workers, 2 schedulers

# Main compose runs AUTH_MODE=apikey; mint a key (printed once), then send it as `Authorization: Bearer <key>` or `X-API-Key`
docker compose -f deploy/docker-compose.yml exec api taskq-admin create-key -tenant acme -name demo
docker compose -f deploy/docker-compose.yml exec api taskq-admin list-keys -tenant acme   # also: revoke-key -id <uuid>
```

Chaos test (proves the guarantee; see `chaos/README.md` for reading the report):

```bash
docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.chaos.yml up -d --build
go run ./cmd/chaos -jobs 20000 -kill-every 1500ms
```

Notes:
- Almost every package has real-Postgres tests via testcontainers; without Docker they fail at container start, not at assertions.
- A full `go test ./...` once failed `internal/scheduler` with a testcontainers "get provider" error while other packages were starting containers; it passed when run alone. If that recurs, run the package on its own (or try `-p 1`).
- `-race` does not work here (`CGO_ENABLED` is off, no C toolchain). Concurrency in `internal/worker` was reviewed by inspection; be careful with its mutex-guarded state.
- The API migrates the DB on startup; workers and schedulers do not, so they may log lease errors for a moment on a fresh stack.

## Architecture

One Go module, one Docker image, three binaries chosen by the compose `command`: `cmd/api`, `cmd/worker`, `cmd/scheduler` (plus the `cmd/chaos` tool). All configuration is env vars with defaults in `internal/config/config.go`.

**Correctness lives in SQL in `internal/store`.** Every state transition is a single statement (data-modifying CTEs), and the other packages depend on the guarantees those statements give. Read the doc comments in `store.go`, `failure.go`, `schedule.go` and `cancel.go` before changing any of them. The key mechanisms:

- **Leasing:** `LeaseJobs` uses `FOR UPDATE SKIP LOCKED`, sets `status='running'`, a fresh per-job `lease_token`, `locked_until`, and opens a `job_attempts` row, all in one statement.
- **Fencing:** `CompleteJob`, `FailJob` and `ExtendLeases` match on `lease_token AND status='running'`. Anything that takes a job away from its worker (reaper, cancel) clears `lease_token`; that is how the old owner gets locked out. Don't add a transition that leaves a stale token on a job someone else may now own.
- **Failure path:** `FailJob(retryIn)` either requeues with `run_at = now()+retryIn` or moves to `dead` when attempts are exhausted or `retryIn == store.NoRetry`. `dead` is the dead-letter queue; `failed` is a legacy status nothing writes anymore.
- **Reaper:** `ReapExpired` reclaims `running` jobs past `locked_until`, also with `SKIP LOCKED`, so every scheduler replica runs it with no leader. It deliberately leaves `run_at` unchanged so reclaimed jobs keep their place at the front of the queue.
- **Cancel:** `CancelJob` clears the lease token; the worker notices on its next heartbeat. The worker has no cancel-specific code.
- **Schedules:** `FireSchedule` is a compare-and-swap on `next_fire_at`, plus a job idempotency key `sched:<id>:<fire unix seconds>`. Double-firing is impossible even if two schedulers both run the cron loop. The leader lock (`TryLeaderLock`, a Postgres session advisory lock on a dedicated connection) is only an optimisation; never rely on it for correctness.
- **Idempotent submit:** unique `(tenant_id, idempotency_key)` with `ON CONFLICT DO NOTHING` then select; a replay returns the original job.

**Worker (`internal/worker/pool.go`):** one lease loop that never holds more than `Concurrency` jobs, plus one heartbeat goroutine per pool that extends all in-flight leases in a single `ExtendLeases` call every `Lease/3`. A token missing from the heartbeat result means the lease was lost (reaped or cancelled): the handler's context is cancelled and **its result is never reported**. Results are written with a context detached from shutdown so they land during a graceful drain. Retry delay comes from `internal/queue.Backoff` (exponential, full jitter); `worker.Permanent(err)` skips retries. Handlers are registered by job `type`; built-ins (`sleep`, `flaky`, `http`) are in `internal/worker/handlers`.

**Scheduler (`cmd/scheduler`):** the reaper runs on every replica; the cron runner (`internal/scheduler/cron.go`) runs only inside the `Elector` (`leader.go`). After an outage, missed fires collapse into one catch-up fire, not a backfill.

**API (`internal/api`):** chi router with `/v1/jobs` (submit, get, list, cancel, requeue) and `/v1/schedules` (CRUD, pause, resume). Tenant comes from the `X-Tenant-ID` header (default `default`) in header auth mode, or from the API key in apikey mode. Cross-tenant access returns 404, not 403, so ids can't be probed. Handlers depend on a small `JobStore` interface, so unit tests use a fake and integration tests use real Postgres. Job listing pages with an opaque `cursor` over `(created_at, id)`.

**Redis (optional, `REDIS_URL`; unset = off):** never on the correctness path. Each feature fails open or falls back, and Postgres alone must always give correct behaviour. Rate limiting (`internal/ratelimit`, token bucket in Lua, per tenant, `POST /v1/jobs` only; `RATELIMIT_ENABLED`, `RATELIMIT_RATE_PER_SEC=50`, `RATELIMIT_BURST=100`) allows the request if Redis errors. The status cache (`internal/statuscache`, `STATUS_CACHE_TTL_MS=10000`, 0 disables) falls back to Postgres on any error. Wake-ups (`internal/wake`, `WAKE_ENABLED`) publish on enqueue so idle workers lease sooner; workers still poll, so a lost wake only costs latency.

**Auth (`internal/api/auth.go`):** `AUTH_MODE=header` (default, dev/chaos) trusts `X-Tenant-ID`. `AUTH_MODE=apikey` resolves the tenant from a key (`Authorization: Bearer` or `X-API-Key`; only a hash is stored in `api_keys`), returns 401 otherwise, and ignores `X-Tenant-ID`. Keys are managed with `cmd/taskq-admin`. A revoked key may keep working for up to ~30s (auth cache).

**Edge (`deploy/nginx.conf`):** nginx on host `:8080` round-robins over the 2 `api` replicas (no host port on `api`; Docker DNS re-resolved every 5s). It retries only on connect error/timeout and never retries non-idempotent methods, so a POST is not replayed. The distroless api image has no curl, so its healthcheck is `api -healthcheck` (GET `/readyz`); workers and schedulers wait for a healthy api. The main compose sets `AUTH_MODE=apikey`; the chaos override sets `AUTH_MODE=header` and `RATELIMIT_ENABLED=false`.

## Conventions

- Migrations are embedded from `internal/store/migrations` and numbered (`0001_`, `0002_`, ...). Add a new pair (`.up.sql`/`.down.sql`); never edit an applied one.
- Packages define the small store interface they need rather than depending on `*store.Store`; keep fakes in tests in sync when adding methods.
- Tests coordinate with channels and injected clocks or hooks, not sleeps.
- Planned work not yet built: Prometheus/Grafana, k6 load tests.
