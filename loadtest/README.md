# Load tests (k6)

Two scripts, run with the `grafana/k6` Docker image **on the compose network**
(so they hit nginx at `http://nginx:80` exactly like a client would). This
avoids Docker Desktop host-networking quirks on Windows; no k6 install needed.

```bash
# 1. stack with the submit rate limit off (apikey auth stays on)
docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.loadtest.yml up -d --build

# 2. mint a key (printed once)
docker compose -f deploy/docker-compose.yml exec api taskq-admin create-key -tenant load -name k6
export API_KEY=tq_...

# 3. run (network name is <compose project>_default, i.e. taskq_default)
docker run --rm -i --network taskq_default -e API_KEY=$API_KEY -e RATE=200 -e DURATION=40 \
  grafana/k6 run - < loadtest/submit.js
docker run --rm -i --network taskq_default -e API_KEY=$API_KEY -e RATE=50 -e DURATION=50 \
  grafana/k6 run - < loadtest/e2e.js
```

Dashboards while it runs: Grafana http://localhost:3000 (anonymous viewer, dev only),
Prometheus http://localhost:9091.

| Script | What it does | Env |
| --- | --- | --- |
| `submit.js` | Ramping arrival rate of 20 ms `sleep` jobs; thresholds on p95 latency (<250 ms), failure rate and checks. | `RATE` (default 200/s), `DURATION` (steady seconds, 40) |
| `e2e.js` | Submits a 50 ms `sleep` job, polls `GET /v1/jobs/{id}` every 50 ms until `succeeded`; custom Trend `submit_to_done_ms`. | `RATE` (20/s), `DURATION` (50) |

Env for both: `BASE_URL` (default `http://nginx:80`), `API_KEY` (required).

## 429 handling

The default limit is 50 req/s per tenant (burst 100), so one tenant cannot load
test past it. `submit.js` treats 429 as an expected response (not an error) and
counts it in `rate_limited_429`, so it can also be run against the default
limiter to exercise it. For throughput numbers use the
`docker-compose.loadtest.yml` override, which sets `RATELIMIT_ENABLED=false`.
