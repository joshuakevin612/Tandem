package metrics

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"taskq/internal/store"
)

// The Postgres container starts lazily so tests that need no DB run without Docker.
var (
	pgOnce sync.Once
	pgC    *postgres.PostgresContainer
	pgDSN  string
	pgErr  error
)

func TestMain(m *testing.M) {
	code := m.Run()
	if pgC != nil {
		_ = testcontainers.TerminateContainer(pgC)
	}
	os.Exit(code)
}

func testStore(t *testing.T) *store.Store {
	t.Helper()
	pgOnce.Do(func() {
		ctx := context.Background()
		pgC, pgErr = postgres.Run(ctx, "postgres:17",
			postgres.WithDatabase("taskq"), postgres.WithUsername("taskq"), postgres.WithPassword("taskq"), postgres.BasicWaitStrategies())
		if pgErr != nil {
			return
		}
		if pgDSN, pgErr = pgC.ConnectionString(ctx, "sslmode=disable"); pgErr != nil {
			return
		}
		pgErr = store.Migrate(pgDSN)
	})
	if pgErr != nil {
		t.Fatalf("postgres unavailable: %v", pgErr)
	}
	st, err := store.Open(context.Background(), pgDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	return st
}

type countingSource struct {
	DepthSource
	calls atomic.Int64
}

func (c *countingSource) QueueStats(ctx context.Context) ([]store.QueueStat, error) {
	c.calls.Add(1)
	return c.DepthSource.QueueStats(ctx)
}

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func TestQueueDepthAgainstPostgres(t *testing.T) {
	ctx := context.Background()
	st := testStore(t)
	const q = "depth-q" // unique to this test; other tests share the database

	past := time.Now().Add(-90 * time.Second)
	future := time.Now().Add(time.Hour)
	for range 3 {
		if _, err := st.CreateJob(ctx, store.NewJob{Queue: q, Type: "x", RunAt: &past}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := st.CreateJob(ctx, store.NewJob{Queue: q, Type: "x", RunAt: &future}); err != nil {
		t.Fatal(err)
	}
	// Lease one of the runnable jobs so there is a running row; it is the oldest by
	// priority/run_at tie-break, so 2 runnable queued jobs remain.
	leased, err := st.LeaseJobs(ctx, q, "w", 1, time.Minute)
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease: %v, %d", err, len(leased))
	}

	src := &countingSource{DepthSource: st}
	clk := &clock{t: time.Unix(1_000_000, 0)}
	c := NewQueueDepth(src, nil)
	c.Now = clk.now
	reg := prometheus.NewRegistry()
	reg.MustRegister(c)

	gauge := func(status string) float64 {
		t.Helper()
		mfs, err := reg.Gather()
		if err != nil {
			t.Fatal(err)
		}
		for _, mf := range mfs {
			if mf.GetName() != "taskq_jobs" {
				continue
			}
			for _, m := range mf.GetMetric() {
				var queue, st string
				for _, l := range m.GetLabel() {
					switch l.GetName() {
					case "queue":
						queue = l.GetValue()
					case "status":
						st = l.GetValue()
					}
				}
				if queue == q && st == status {
					return m.GetGauge().GetValue()
				}
			}
		}
		t.Fatalf("no taskq_jobs series for %s/%s", q, status)
		return 0
	}

	if got := gauge("queued"); got != 3 { // 2 runnable + 1 scheduled for later
		t.Errorf("queued = %v, want 3", got)
	}
	if got := gauge("running"); got != 1 {
		t.Errorf("running = %v, want 1", got)
	}
	if got := gauge("dead"); got != 0 {
		t.Errorf("dead = %v, want 0 (zero-filled)", got)
	}
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var age float64 = -1
	for _, mf := range mfs {
		if mf.GetName() == "taskq_oldest_queued_job_age_seconds" {
			for _, m := range mf.GetMetric() {
				if m.GetLabel()[0].GetValue() == q {
					age = m.GetGauge().GetValue()
				}
			}
		}
	}
	if age < 85 || age > 600 { // ~90s old; the future job must not count
		t.Errorf("oldest age = %v, want about 90", age)
	}

	// Two gathers inside the window ran the (single) query once per Gather
	// batch: calls so far must be exactly 1.
	if n := src.calls.Load(); n != 1 {
		t.Fatalf("queries within window = %d, want 1", n)
	}
	if n := testutil.CollectAndCount(c); n == 0 {
		t.Fatal("no series")
	}
	if n := src.calls.Load(); n != 1 {
		t.Errorf("extra collect inside window queried again: %d", n)
	}

	// New data is invisible until the window passes, then visible.
	if _, err := st.CreateJob(ctx, store.NewJob{Queue: q, Type: "x"}); err != nil {
		t.Fatal(err)
	}
	clk.advance(4 * time.Second)
	if got := gauge("queued"); got != 3 {
		t.Errorf("queued inside window = %v, want cached 3", got)
	}
	clk.advance(2 * time.Second)
	if got := gauge("queued"); got != 4 {
		t.Errorf("queued after window = %v, want 4", got)
	}
	if n := src.calls.Load(); n != 2 {
		t.Errorf("total queries = %d, want 2", n)
	}
}

type failingSource struct{ err error }

func (f failingSource) QueueStats(context.Context) ([]store.QueueStat, error) { return nil, f.err }

func TestQueueDepthSourceErrorEmitsNothing(t *testing.T) {
	c := NewQueueDepth(failingSource{errors.New("db down")}, nil)
	if n := testutil.CollectAndCount(c); n != 0 {
		t.Errorf("series = %d, want 0", n)
	}
}
