// Command chaos proves tandem's at-least-once guarantee under worker/scheduler
// SIGKILLs. See chaos/README.md.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"text/tabwriter"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	kindSleep  = "sleep"
	kindFlaky  = "flaky50"
	kindDead   = "flaky100"
	project    = "tandem"
	maxAttempt = 5
	clients    = 50
)

var (
	apiURL    = flag.String("api", "http://localhost:8080", "API base URL")
	dsn       = flag.String("dsn", "postgres://tandem:tandem@localhost:5432/tandem?sslmode=disable", "Postgres DSN")
	numJobs   = flag.Int("jobs", 20000, "jobs to submit")
	killEvery = flag.Duration("kill-every", 3*time.Second, "interval between kill rounds")
	timeout   = flag.Duration("timeout", 5*time.Minute, "overall timeout (submit + drain)")
	seed      = flag.Int64("seed", time.Now().UnixNano(), "random seed")
)

// kindOf is deterministic by index: 80% sleep, 15% flaky 0.5, 5% flaky 1.0.
func kindOf(i int) string {
	switch m := i % 20; {
	case m < 16:
		return kindSleep
	case m < 19:
		return kindFlaky
	default:
		return kindDead
	}
}

type counters struct{ workerKills, schedKills atomic.Int64 }

func main() {
	flag.Parse()
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "chaos:", err)
		os.Exit(1)
	}
}

func run() error {
	runID := fmt.Sprintf("%x", time.Now().UnixNano()/1e6)
	tenant := "chaos-" + runID
	fmt.Printf("run %s tenant %s seed %d jobs %d\n", runID, tenant, *seed, *numJobs)

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	pool, err := pgxpool.New(ctx, *dsn)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("postgres: %w", err)
	}

	var cnt counters
	chaosCtx, stopChaos := context.WithCancel(context.Background())
	var chaosWG sync.WaitGroup
	start := time.Now()
	chaosWG.Add(1)
	go func() { defer chaosWG.Done(); chaosLoop(chaosCtx, &cnt) }()

	err = submitAll(ctx, tenant, runID)
	if err == nil {
		fmt.Printf("submitted %d jobs in %s\n", *numJobs, time.Since(start).Round(time.Millisecond))
		err = waitDrain(ctx, pool, tenant)
	}
	elapsed := time.Since(start)
	stopChaos()
	chaosWG.Wait() // ensures every container is restarted
	if err != nil {
		return err
	}

	ok, rep := check(context.Background(), pool, tenant, &cnt)
	fmt.Print("\n", rep)
	report(context.Background(), pool, tenant, elapsed, &cnt)
	if !ok {
		fmt.Println("RESULT: FAIL")
		os.Exit(1)
	}
	fmt.Println("RESULT: PASS - all invariants held")
	return nil
}

// ---------- submit ----------

func submitAll(ctx context.Context, tenant, runID string) error {
	idx := make(chan int)
	errc := make(chan error, clients)
	hc := &http.Client{Timeout: 15 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: clients}}
	var wg sync.WaitGroup
	var retries atomic.Int64
	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range idx {
				if err := submitOne(ctx, hc, tenant, runID, i, &retries); err != nil {
					errc <- err
					return
				}
			}
		}()
	}
	go func() {
		defer close(idx)
		for i := 0; i < *numJobs; i++ {
			select {
			case idx <- i:
			case <-ctx.Done():
				return
			}
		}
	}()
	wg.Wait()
	select {
	case err := <-errc:
		return err
	default:
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	fmt.Printf("submit HTTP retries: %d\n", retries.Load())
	return nil
}

func submitOne(ctx context.Context, hc *http.Client, tenant, runID string, i int, retries *atomic.Int64) error {
	rng := rand.New(rand.NewSource(*seed + int64(i)))
	req := map[string]any{"type": "sleep", "max_attempts": maxAttempt}
	switch k := kindOf(i); k {
	case kindSleep:
		req["payload"] = map[string]any{"ms": 10 + rng.Intn(91), "kind": k}
	case kindFlaky:
		req["type"] = "flaky"
		req["payload"] = map[string]any{"fail_rate": 0.5, "kind": k}
	default:
		req["type"] = "flaky"
		req["payload"] = map[string]any{"fail_rate": 1.0, "kind": k}
	}
	body, _ := json.Marshal(req)
	key := fmt.Sprintf("%s-%d", runID, i)
	backoff := 50 * time.Millisecond
	for {
		r, _ := http.NewRequestWithContext(ctx, http.MethodPost, *apiURL+"/v1/jobs", bytes.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-Tenant-ID", tenant)
		r.Header.Set("Idempotency-Key", key)
		resp, err := hc.Do(r)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusAccepted {
				return nil
			}
			err = fmt.Errorf("status %d", resp.StatusCode)
		}
		if ctx.Err() != nil {
			return fmt.Errorf("submit %s: %w (last: %v)", key, ctx.Err(), err)
		}
		if n := retries.Add(1); n <= 5 {
			fmt.Printf("submit retry %s: %v\n", key, err)
		}
		select {
		case <-time.After(backoff + time.Duration(rng.Int63n(int64(backoff)))):
		case <-ctx.Done():
		}
		if backoff < 2*time.Second {
			backoff *= 2
		}
	}
}

// ---------- chaos ----------

func dockerOut(args ...string) (string, error) {
	out, err := exec.Command("docker", args...).CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

func containers(service string) []string {
	out, err := dockerOut("ps", "-q",
		"--filter", "label=com.docker.compose.project="+project,
		"--filter", "label=com.docker.compose.service="+service,
		"--filter", "status=running")
	if err != nil || out == "" {
		return nil
	}
	return strings.Fields(out)
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-time.After(d):
		return true
	case <-ctx.Done():
		return false
	}
}

// killRestart SIGKILLs one container of service and restarts it 1-2s later.
// The restart always happens, even when ctx is cancelled meanwhile.
func killRestart(ctx context.Context, rng *rand.Rand, service string, kills *atomic.Int64) {
	ids := containers(service)
	if len(ids) == 0 {
		fmt.Printf("chaos: no running %s container\n", service)
		return
	}
	id := ids[rng.Intn(len(ids))]
	if out, err := dockerOut("kill", "--signal", "KILL", id); err != nil {
		fmt.Printf("chaos: kill %s: %v %s\n", id[:12], err, out)
		return
	}
	n := kills.Add(1)
	fmt.Printf("chaos: killed %s %s (#%d)\n", service, id[:12], n)
	sleepCtx(ctx, time.Second+time.Duration(rng.Int63n(int64(time.Second))))
	if out, err := dockerOut("start", id); err != nil {
		fmt.Printf("chaos: start %s: %v %s\n", id[:12], err, out)
	}
}

func chaosLoop(ctx context.Context, cnt *counters) {
	rng := rand.New(rand.NewSource(*seed))
	for round := 1; ; round++ {
		if !sleepCtx(ctx, *killEvery) {
			return
		}
		killRestart(ctx, rng, "worker", &cnt.workerKills)
		if round%4 == 0 {
			killRestart(ctx, rng, "scheduler", &cnt.schedKills)
		}
	}
}

// ---------- wait ----------

func waitDrain(ctx context.Context, pool *pgxpool.Pool, tenant string) error {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	last := time.Now()
	for {
		var q, r, total int
		err := pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE status='queued'),
			count(*) FILTER (WHERE status='running'), count(*) FROM jobs WHERE tenant_id=$1`, tenant).Scan(&q, &r, &total)
		if err == nil {
			if q+r == 0 {
				return nil
			}
			if time.Since(last) >= 5*time.Second {
				last = time.Now()
				fmt.Printf("waiting: queued=%d running=%d total=%d\n", q, r, total)
			}
		} else if ctx.Err() != nil {
			return fmt.Errorf("timeout waiting for drain: %w", ctx.Err())
		}
		select {
		case <-t.C:
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for drain (queued=%d running=%d): %w", q, r, ctx.Err())
		}
	}
}

// ---------- checker ----------

func check(ctx context.Context, pool *pgxpool.Pool, tenant string, cnt *counters) (bool, string) {
	var sb strings.Builder
	ok := true
	fail := func(name string, n int, samples []string) {
		ok = false
		fmt.Fprintf(&sb, "FAIL %s: %d offending; samples: %v\n", name, n, samples)
	}
	pass := func(name string) { fmt.Fprintf(&sb, "ok   %s\n", name) }

	var total int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE tenant_id=$1`, tenant).Scan(&total); err != nil {
		fail("1 job count (query error "+err.Error()+")", 1, nil)
	} else if total != *numJobs {
		fail(fmt.Sprintf("1 job count == %d (got %d)", *numJobs, total), total, nil)
	} else {
		pass("1 job count == jobs")
	}

	// Each query returns offending job ids.
	checks := []struct{ name, sql string }{
		{"2 every job succeeded or dead",
			`SELECT id::text FROM jobs WHERE tenant_id=$1 AND status NOT IN ('succeeded','dead')`},
		{"3 attempts <= max_attempts; dead => attempts = max_attempts",
			`SELECT id::text FROM jobs WHERE tenant_id=$1 AND (attempts > max_attempts OR (status='dead' AND attempts <> max_attempts))`},
		{"4 every fail_rate 1.0 job is dead",
			`SELECT id::text FROM jobs WHERE tenant_id=$1 AND payload->>'kind'='` + kindDead + `' AND status <> 'dead'`},
		{"5 attempt rows == attempts, none open",
			`SELECT j.id::text FROM jobs j
			 LEFT JOIN (SELECT job_id, count(*) n, count(*) FILTER (WHERE finished_at IS NULL) open
			            FROM job_attempts WHERE job_id IN (SELECT id FROM jobs WHERE tenant_id=$1) GROUP BY job_id) a ON a.job_id=j.id
			 WHERE j.tenant_id=$1 AND (coalesce(a.n,0) <> j.attempts OR coalesce(a.open,0) > 0)`},
		{"6 succeeded => exactly one succeeded attempt row, and it is the last attempt",
			`SELECT j.id::text FROM jobs j
			 LEFT JOIN (SELECT job_id, count(*) FILTER (WHERE outcome='succeeded') ns,
			                   max(attempt) maxa, max(attempt) FILTER (WHERE outcome='succeeded') sa
			            FROM job_attempts WHERE job_id IN (SELECT id FROM jobs WHERE tenant_id=$1) GROUP BY job_id) a ON a.job_id=j.id
			 WHERE j.tenant_id=$1 AND j.status='succeeded'
			   AND (coalesce(a.ns,0) <> 1 OR a.sa IS DISTINCT FROM a.maxa OR a.sa IS DISTINCT FROM j.attempts)`},
		{"7 no lease fields on terminal jobs",
			`SELECT id::text FROM jobs WHERE tenant_id=$1 AND status IN ('succeeded','dead','failed','cancelled')
			 AND (lease_token IS NOT NULL OR locked_until IS NOT NULL)`},
	}
	for _, c := range checks {
		rows, err := pool.Query(ctx, c.sql, tenant)
		if err != nil {
			fail(c.name+" (query error: "+err.Error()+")", 1, nil)
			continue
		}
		var ids []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err == nil {
				ids = append(ids, id)
			}
		}
		rows.Close()
		if len(ids) > 0 {
			s := ids
			if len(s) > 5 {
				s = s[:5]
			}
			fail(c.name, len(ids), s)
		} else {
			pass(c.name)
		}
	}

	var expired int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM job_attempts a JOIN jobs j ON j.id=a.job_id
		WHERE j.tenant_id=$1 AND a.outcome='lease_expired'`, tenant).Scan(&expired)
	kills := cnt.workerKills.Load() + cnt.schedKills.Load()
	if kills == 0 || expired == 0 {
		fail(fmt.Sprintf("8 chaos happened (kills=%d, lease_expired attempts=%d)", kills, expired), 1, nil)
	} else {
		pass(fmt.Sprintf("8 chaos happened (kills=%d, lease_expired attempts=%d)", kills, expired))
	}
	return ok, sb.String()
}

// ---------- report ----------

func report(ctx context.Context, pool *pgxpool.Pool, tenant string, elapsed time.Duration, cnt *counters) {
	tw := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
	defer tw.Flush()
	row := func(k string, v ...any) { fmt.Fprintf(tw, "%s\t%s\n", k, fmt.Sprint(v...)) }
	fmt.Fprintln(tw, "\n=== CHAOS REPORT ===")
	row("jobs", *numJobs)
	row("elapsed", elapsed.Round(time.Millisecond))
	row("jobs/sec (end to end)", fmt.Sprintf("%.1f", float64(*numJobs)/elapsed.Seconds()))
	row("kills (worker/scheduler)", fmt.Sprintf("%d / %d", cnt.workerKills.Load(), cnt.schedKills.Load()))

	rows, err := pool.Query(ctx, `SELECT payload->>'kind', status, count(*) FROM jobs WHERE tenant_id=$1
		GROUP BY 1,2 ORDER BY 1,2`, tenant)
	if err == nil {
		for rows.Next() {
			var k, s string
			var n int
			_ = rows.Scan(&k, &s, &n)
			row("  status "+k+"/"+s, n)
		}
		rows.Close()
	}
	var expired, reexec int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM job_attempts a JOIN jobs j ON j.id=a.job_id
		WHERE j.tenant_id=$1 AND a.outcome='lease_expired'`, tenant).Scan(&expired)
	_ = pool.QueryRow(ctx, `SELECT coalesce(sum(attempts),0)-count(*) FROM jobs
		WHERE tenant_id=$1 AND payload->>'kind'='`+kindSleep+`'`, tenant).Scan(&reexec)
	row("lease_expired attempts", expired)
	row("re-executions (sleep: sum(attempts)-jobs)", reexec)

	pct := func(label, expr string) {
		var n int
		var p50, p95, mx *float64
		err := pool.QueryRow(ctx, `WITH r AS (
			SELECT a.finished_at, a.started_at, n.started_at AS next_start
			FROM job_attempts a JOIN jobs j ON j.id=a.job_id
			LEFT JOIN job_attempts n ON n.job_id=a.job_id AND n.attempt=a.attempt+1
			WHERE j.tenant_id=$1 AND a.outcome='lease_expired' AND a.finished_at IS NOT NULL), v AS (
			SELECT EXTRACT(EPOCH FROM (`+expr+`))::float8 AS s FROM r)
			SELECT count(s), percentile_cont(0.5) WITHIN GROUP (ORDER BY s),
			  percentile_cont(0.95) WITHIN GROUP (ORDER BY s), max(s) FROM v WHERE s IS NOT NULL`, tenant).Scan(&n, &p50, &p95, &mx)
		if err != nil || n == 0 || p50 == nil {
			row(label, fmt.Sprintf("n/a (%v)", err))
			return
		}
		row(label, fmt.Sprintf("n=%d p50=%.2fs p95=%.2fs max=%.2fs", n, *p50, *p95, *mx))
	}
	pct("time-to-detect (finished-started)", "finished_at - started_at")
	pct("time-to-re-lease (next start-finished)", "next_start - finished_at")
}
