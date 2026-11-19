# Two-minute demo script

Goal: show that SIGKILLing workers mid-run loses nothing, and let the viewer watch the recovery on the Grafana dashboard. Commands below were run against this repo while writing the script; the panel behavior is described from the code and from a rehearsal run, so do one dry run before recording.

Use Git Bash (the commands use `$(...)`). Needs Docker and Go.

## Before you hit record

```bash
cd "/d/Projects 2.0"
export PATH="/c/Program Files/Go/bin:$PATH"
export C="-f deploy/docker-compose.yml -f deploy/docker-compose.chaos.yml"

# Chaos override: 5 s leases, 4 workers x 20 slots, reaper every 500 ms, rate limit off, header auth.
docker compose $C up -d --build
docker compose $C ps          # wait until api-1 and api-2 are healthy
go vet ./cmd/chaos            # optional: warms the Go build cache so `go run` starts fast on camera
```

Open two windows side by side: a terminal (large font) and a browser at **http://localhost:3000/d/taskq**. Set the dashboard time range to "Last 5 minutes"; it refreshes every 5 s on its own. Grafana is anonymous Viewer, so no login screen.

Starting from a fresh stack (`down -v`, then `up`) gives clean graphs, because Prometheus keeps its data only in the container.

## Script

| Time | You do | Say |
| --- | --- | --- |
| 0:00 | Show the dashboard, idle. Run `docker compose $C ps`. | "This is Tandem, a job queue on Postgres. Two API replicas behind nginx, four workers, two schedulers, Prometheus and Grafana. The promise: once the API returns 202, the job is never lost, even if workers are killed with SIGKILL." |
| 0:15 | Run the load and the killer in one command: `go run ./cmd/chaos -jobs 40000 -kill-every 4s` | "This submits forty thousand jobs, with idempotency keys, and a chaos loop that SIGKILLs a random worker every four seconds and sometimes a scheduler. Some jobs are designed to fail, so retries and dead letters are exercised too." |
| 0:25 | Point at **Submit rate** and **Queue depth by status**. | "Submits land, the queued count jumps, and workers drain it. Each worker holds at most its concurrency, twenty here: see **In-flight jobs per worker** sitting at the cap." |
| 0:35 | In a second terminal, kill every worker at once: `docker compose $C kill -s SIGKILL worker` | "Now I will do something nastier than the script does: SIGKILL all four workers at the same moment. No graceful shutdown, no chance to report anything." |
| 0:40 | Point at **In-flight jobs per worker** (lines stop), **Attempts/sec by outcome** (succeeded falls to zero), **Oldest queued job age** (climbs), **Queue depth** (queued stops falling). | "In-flight drops to zero and nothing drains. The jobs those workers held are still marked running in Postgres, under leases that will expire." |
| 0:45 | Wait about five seconds. Point at **Reaped jobs rate**. | "The lease is five seconds in this configuration. When it lapses, the reaper on the scheduler replicas finds those jobs and puts them back in the queue. Reaped jobs rising is that happening. No operator action, no leader needed." |
| 0:50 | Restart them: `docker compose $C start worker` | "Bring the workers back." |
| 0:55 | Point at **In-flight jobs per worker** (back at the cap), **Queue depth** (draining again), **Oldest queued job age** (falling), **Attempts/sec** (succeeded and retried resume). | "In-flight returns to the cap, the backlog drains, the oldest-job age falls. The reclaimed jobs kept their place at the front of the queue." |
| 1:05 | Optional. Freeze one worker past its lease with the pause commands below (about 9 seconds, then unpause). | "A different failure: a worker that does not die but stalls. Its lease expires, the job is re-run elsewhere, and when it wakes up its late result is rejected by the fencing token." |
| 1:15 | Point at **Attempts/sec by outcome**, the `lease_lost` series. | "That blip is lease_lost: a worker that finished work after losing the lease, and the database refused its result." Skip this row and the next if the run has already finished. |
| 1:20 | Back in the chaos terminal, wait for the report. Point at `ok 1` through `ok 8` and `RESULT: PASS - all invariants held`. | "When the queue drains it checks eight invariants directly in Postgres: every job is there exactly once, every job ended succeeded or dead, none is stuck, attempt counts add up, no finished job still holds a lease. All pass." |
| 1:35 | Point at the report: `kills`, `lease_expired attempts`, `re-executions`, `time-to-detect`, `time-to-re-lease`. | "And here is the honest part: the re-executions line. Those are jobs that ran more than once because a worker died after doing the work and before reporting. That is at-least-once. Exactly-once is not promised; the design doc explains why, and how fencing and idempotency keys keep duplicates safe." |
| 1:50 | Show the repo README on screen. | "Postgres with SKIP LOCKED for leasing, fencing tokens, a reaper, cron with compare-and-swap, optional Redis that is never on the correctness path. The chaos test and the benchmark numbers are in the README. Thanks." |

The pause step, run from the second terminal:

```bash
W=$(docker compose $C ps -q worker | head -1)
docker pause $W
# wait about 9 seconds (longer than the 5 s lease), then:
docker unpause $W
```

Timing note: in a rehearsal, 40,000 jobs took 42 s end to end including my manual kill and pause, so the run should finish around 1:00-1:15 on a similar machine. If yours is much faster, raise `-jobs` (for example 60000) so the load outlasts the kills; if the run ends before the pause step, skip it.

## What each panel shows

| Panel | Metric | In this demo |
| --- | --- | --- |
| Queue depth by status | `taskq_jobs{queue,status}` | Queued rises with submits and falls as workers drain; stalls while workers are down. `succeeded` and `dead` climb too. |
| Oldest queued job age | `taskq_oldest_queued_job_age_seconds` | Climbs while no worker is leasing, falls after restart. |
| Submit rate and 429 rate | `taskq_jobs_submitted_total`, `taskq_rate_limited_total` | The chaos override turns the limiter off, so 429 stays at zero. |
| In-flight jobs per worker | `taskq_in_flight_jobs` | One line per worker at the concurrency cap (20 here); lines stop when a worker is killed. |
| Attempts/sec by outcome | `taskq_attempt_duration_seconds_count` by outcome | `succeeded`, `retried` (flaky jobs), `dead`, `lease_lost` (the pause step). A SIGKILLed worker reports nothing, so the kill itself appears as a drop, not as a series. |
| Reaped jobs rate | `taskq_reaped_jobs_total` | Spikes about one lease (5 s) after the kill, when expired leases are reclaimed. |
| Lease latency p95, Attempt duration p95, API latency | histograms | Context. Lease latency stays small unless the database is saturated. |
| Status cache hit ratio | `taskq_status_cache_requests_total` | Empty in this demo; nothing reads job status. |
| Wake-ups rate | `taskq_wakeups_total` | Idle workers cut short by Redis wake-ups. |

`taskq_heartbeat_lost_leases_total` (leases a live worker found it had lost) is exported but has no dashboard panel; the `lease_lost` outcome in Attempts/sec is the visible signal.

## After recording

```bash
docker compose $C down -v
```

If a take goes wrong mid-run, jobs from the aborted run stay in the database under their old tenant id; `down -v` plus `up` is the clean reset.
