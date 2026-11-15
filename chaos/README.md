# taskq chaos test

Proves at-least-once execution: with workers (and the reaper) SIGKILLed every few
seconds while 20k jobs flow through, no job is lost, duplicated at submit, or left
stuck; every job ends `succeeded` or `dead` with a consistent attempt history.

Run (from repo root, Docker running):

    docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.chaos.yml up -d --build
    go run ./cmd/chaos -jobs 20000     # flags: -api -dsn -kill-every -timeout -seed

Reading the output: `ok`/`FAIL` lines are the 8 invariants (a FAIL prints the
count and sample job ids; exit code 1). The report shows end-to-end throughput,
kills, per-kind status counts (`flaky100` must be all dead), `lease_expired`
attempts (work reclaimed from killed workers), re-executions (extra runs of
sleep jobs = at-least-once duplicates), and p50/p95/max time-to-detect (lease
start to reap) and time-to-re-lease (reap to next attempt start).
