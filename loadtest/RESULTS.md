# Load test and dequeue analysis results

## Environment (read this first)

Everything ran on one Windows laptop under Docker Desktop (WSL2 VM): Postgres,
Redis, 2 API replicas, nginx, 3 workers (concurrency 10 each), 2 schedulers,
Prometheus, Grafana **and k6 itself** shared the same CPUs and disk. The numbers
show the system behaving sanely on this machine; they are not capacity claims
and are not comparable to dedicated hardware. Runs were short (30-55 s), single
samples, no repeats. Jobs were `sleep` handlers (20-50 ms), so handler cost is
tiny and the queue/DB path dominates. Rate limiting was off for the throughput
runs (`deploy/docker-compose.loadtest.yml`).

## k6 results

| Run | Load | Result |
| --- | --- | --- |
| `submit.js`, 200/s target | 10 s ramp, 40 s hold, 5 s down; 9,549 requests | 0 errors, all 202; submit p95 **3.4 ms** (avg 2.6, max 24.7); all thresholds passed |
| `submit.js`, 1000/s target | same shape (30 s hold); 37,389 requests | 0 errors; **~830 req/s average over the whole run** (including ramps; k6 dropped 160 iterations for lack of free VUs, so the full target was not sustained); submit p95 **42 ms** (avg 9.1, p90 21, max 201); passed |
| `e2e.js`, 50/s | 50 s, 2,500 jobs | 0 failures, 0 poll timeouts; **submit-to-done p50 58 ms, p95 109 ms**, max 112 ms |
| `submit.js`, 150/s, limiter **on** (default 50/s, burst 100) | 15 s hold | 1,925 of 3,424 requests got 429 (`rate_limited_429`), none counted as errors; p95 3.2 ms |

Notes on the numbers:
- The e2e floor is the handler (50 ms) plus the 50 ms poll interval, so 58-109 ms
  means queue wait is close to zero: enqueue wake-ups (Redis) are reaching idle
  workers promptly. This was not compared against wake-ups off.
- Throughput is bounded by what k6 and the stack could do together on this laptop. I did
  not find the real ceiling; 1000/s was the highest target tried.
- The status cache showed only misses in the e2e run (426 misses, 0 hits): polling
  stops at the first terminal read, so nothing hits. The hit-ratio panel is only
  meaningful with repeated reads of finished jobs.
- Not exercised: retries, dead jobs, lease loss, reaping (all 0 in this workload).

## What Grafana / Prometheus showed

I queried the same Prometheus series the dashboard panels use (peaks over the
runs); I did not visually inspect the rendered panels. Grafana provisioning was
verified through its API (`/api/search` lists the `taskq` dashboard; it has 11 panels).

- Submit rate peaked at ~1,000/s (15 s window); worker `succeeded` attempts/s peaked at
  ~1,010/s, so workers kept pace with submits.
- Queue depth (`max by (queue,status)(taskq_jobs{status="queued"})`) peaked at 258
  and oldest queued job age at 0.25 s: no meaningful backlog even at 1000/s.
- In-flight jobs per worker reached the concurrency cap of 10.
- Lease latency p95 peaked at ~4.7 ms; attempt duration p95 for `sleep` ~98 ms
  (histogram bucket resolution; the handler sleeps 20-50 ms).
- API `POST /v1/jobs` p95 peaked at ~10 ms in Prometheus (bucket estimate) versus
  42 ms in k6, which also includes nginx and the client side.
- 429 rate peaked at ~100/s in the limiter-on run.

## Dequeue query analysis

Seed: truncated `jobs`, inserted 500,000 rows with `generate_series`, `ANALYZE`d.
Queues `default` (60%), `emails`, `reports` (20% each); 90% succeeded, 5% dead,
5% queued (~25k queued; 20% of those have `run_at` 1-11 h in the future);
priority 0 (70%), 5 (25%), 10 (5%). Heap 71 MB, `jobs_queued_idx` 1.2 MB.
Workers and schedulers were stopped so the data stayed fixed. Queries ran in
`psql` inside rolled-back transactions (the index was dropped inside a
transaction that was rolled back; it is still present afterwards).

Query shape is the `picked` CTE from `store.LeaseJobs` (`internal/store/store.go`),
`LIMIT 10` (a worker with 10 free slots):

```sql
SELECT id FROM jobs
WHERE status = 'queued' AND queue = 'default' AND run_at <= now()
ORDER BY priority DESC, run_at LIMIT 10 FOR UPDATE SKIP LOCKED;
```

**With `jobs_queued_idx (queue, priority DESC, run_at) WHERE status='queued'`**
(third run, warm): `Limit -> LockRows -> Index Scan using jobs_queued_idx`,
`Index Cond: (queue = 'default' AND run_at <= now())`, no Sort node, 22 shared
buffers, **Execution Time 0.024 ms** (first run 0.128 ms, cold). The whole
`LeaseJobs` statement (select + update 10 rows + insert 10 `job_attempts` rows,
including the FK trigger) took **1.57 ms** under EXPLAIN ANALYZE.

**Without the index**: `Limit -> LockRows -> Sort (quicksort, 1 MB, 12,022 rows)
-> Seq Scan on jobs`, `Rows Removed by Filter: 487,978`, 9,092 buffers,
**Execution Time 44.9-52.3 ms**, roughly 1,000x slower, and growing with table
size, whereas the indexed plan touches about 20 pages regardless of how many
finished jobs exist.

**Does the index order fully serve the query?** Yes for the `ORDER BY`: with
`queue` pinned by equality, the index is already ordered by `priority DESC, run_at`,
so no sort is needed and `LIMIT` stops the scan after 10 rows. `status='queued'`
is implied by the partial predicate, which is why finished jobs cost nothing.
The one imperfection: `run_at <= now()` is an index qual but not a range bound
(it follows `priority` in the key), so the scan walks entries in priority order
and skips future-dated ones inside the index. A test with 8,000 future-dated
priority-20 rows ahead of the due rows still cost only 32 buffers / 0.072 ms (I
did not investigate why it was that small; the supportable claim is "not a
problem at this scale"). Cost should grow with the number of future-dated rows
ranked ahead of due ones in a queue.

**Recommendation: no index change.** The plan is the ideal shape and latency is
negligible. If a deployment ever holds very large numbers of future-dated
high-priority jobs (millions), revisit then, for example by keeping far-future
jobs out of the hot index. That is a hypothesis with no evidence here; no
migration was added.

## Batch leasing

No change needed. `LeaseJobs(ctx, queue, workerID, limit, lease)` leases up to
`limit` jobs in one statement (`LIMIT $2 ... FOR UPDATE SKIP LOCKED` in the
`picked` CTE, then one `UPDATE ... FROM picked` and one `INSERT INTO job_attempts
SELECT ... FROM leased`). The worker passes its free slots:
`free := cap(p.sem) - len(p.sem)` then `p.Store.LeaseJobs(ctx, queue, p.WorkerID, free, p.Lease)`
(`internal/worker/pool.go`). The measured lease p95 of ~5 ms at ~1,000 jobs/s
shows the lease path was not the bottleneck on this setup.
