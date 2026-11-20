package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"tandem/internal/queue"
	"tandem/internal/scheduler"
	"tandem/internal/store"
)

type pgEnv struct {
	ctx context.Context
	raw *pgxpool.Pool
	st  *store.Store
}

// newPG starts a fresh Postgres container with the schema applied, plus a
// reaper running every 100ms for the life of the test.
func newPG(t *testing.T) *pgEnv {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	pg, err := postgres.Run(ctx, "postgres:17",
		postgres.WithDatabase("tandem"), postgres.WithUsername("tandem"), postgres.WithPassword("tandem"), postgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(pg) })
	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Migrate(dsn); err != nil {
		t.Fatal(err)
	}
	raw, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(raw.Close)
	st, err := store.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)

	reaperCtx, stopReaper := context.WithCancel(ctx)
	reaperDone := make(chan struct{})
	r := &scheduler.Reaper{Store: st, Interval: 100 * time.Millisecond, BatchSize: 100, Logger: quiet}
	go func() { defer close(reaperDone); _ = r.Run(reaperCtx) }()
	t.Cleanup(func() { stopReaper(); <-reaperDone })
	return &pgEnv{ctx: ctx, raw: raw, st: st}
}

func (e *pgEnv) create(t *testing.T, typ string, maxAttempts int) uuid.UUID {
	t.Helper()
	j, err := e.st.CreateJob(e.ctx, store.NewJob{Type: typ, MaxAttempts: maxAttempts})
	if err != nil {
		t.Fatal(err)
	}
	return j.ID
}

func (e *pgEnv) waitStatus(t *testing.T, id uuid.UUID, want string) store.Job {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		j, err := e.st.GetJob(e.ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if j.Status == want {
			return j
		}
		if time.Now().After(deadline) {
			t.Fatalf("job stuck in %q (attempts=%d, err=%v), want %q", j.Status, j.Attempts, j.LastError, want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (e *pgEnv) outcomes(t *testing.T, id uuid.UUID) []string {
	t.Helper()
	rows, err := e.raw.Query(e.ctx, `SELECT outcome FROM job_attempts WHERE job_id = $1 ORDER BY attempt`, id)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var o *string
		if err := rows.Scan(&o); err != nil {
			t.Fatal(err)
		}
		if o == nil {
			out = append(out, "<open>")
		} else {
			out = append(out, *o)
		}
	}
	return out
}

func (e *pgEnv) runPool(t *testing.T, cfg Config) (stop func()) {
	t.Helper()
	cfg.Store = e.st
	cfg.Logger = quiet
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 10 * time.Millisecond
	}
	if cfg.Concurrency == 0 {
		cfg.Concurrency = 4
	}
	p := NewPool(cfg)
	ctx, cancel := context.WithCancel(e.ctx)
	done := make(chan struct{})
	go func() { defer close(done); _ = p.Run(ctx) }()
	var once sync.Once
	stop = func() { once.Do(func() { cancel(); <-done }) }
	t.Cleanup(stop)
	return stop
}

func eqStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

var fastRetry = queue.Backoff{Base: 5 * time.Millisecond, Max: 20 * time.Millisecond}

func TestE2EHeartbeatKeepsLongJobAlive(t *testing.T) {
	e := newPG(t)
	var runs atomic.Int32
	reg := NewRegistry()
	reg.Register("long", func(ctx context.Context, _ store.Job) error {
		runs.Add(1)
		select {
		case <-time.After(3 * time.Second): // 3x the lease
			return nil
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	})
	id := e.create(t, "long", 3)
	e.runPool(t, Config{Registry: reg, Lease: time.Second, Backoff: fastRetry})
	j := e.waitStatus(t, id, "succeeded")
	if j.Attempts != 1 || runs.Load() != 1 {
		t.Fatalf("attempts=%d runs=%d, want 1 and 1", j.Attempts, runs.Load())
	}
	if got := e.outcomes(t, id); !eqStrings(got, []string{"succeeded"}) {
		t.Fatalf("attempt rows = %v", got)
	}
}

func TestE2ERetryThenSucceed(t *testing.T) {
	e := newPG(t)
	reg := NewRegistry()
	reg.Register("flaky", func(_ context.Context, j store.Job) error {
		if j.Attempts < 3 {
			return errors.New("transient")
		}
		return nil
	})
	id := e.create(t, "flaky", 5)
	e.runPool(t, Config{Registry: reg, Lease: time.Second, Backoff: fastRetry})
	j := e.waitStatus(t, id, "succeeded")
	if j.Attempts != 3 {
		t.Fatalf("attempts = %d, want 3", j.Attempts)
	}
	if got := e.outcomes(t, id); !eqStrings(got, []string{"failed", "failed", "succeeded"}) {
		t.Fatalf("attempt rows = %v", got)
	}
}

func TestE2EAlwaysFailingGoesDead(t *testing.T) {
	e := newPG(t)
	reg := NewRegistry()
	reg.Register("bad", func(context.Context, store.Job) error { return errors.New("always") })
	id := e.create(t, "bad", 4)
	e.runPool(t, Config{Registry: reg, Lease: time.Second, Backoff: fastRetry})
	j := e.waitStatus(t, id, "dead")
	if j.Attempts != j.MaxAttempts || j.Attempts != 4 {
		t.Fatalf("attempts = %d, max = %d, want 4", j.Attempts, j.MaxAttempts)
	}
	if got := e.outcomes(t, id); !eqStrings(got, []string{"failed", "failed", "failed", "failed"}) {
		t.Fatalf("attempt rows = %v", got)
	}
}

func TestE2EPermanentErrorDiesImmediately(t *testing.T) {
	e := newPG(t)
	reg := NewRegistry()
	reg.Register("perm", func(context.Context, store.Job) error { return Permanent(errors.New("bad input")) })
	id := e.create(t, "perm", 5)
	e.runPool(t, Config{Registry: reg, Lease: time.Second, Backoff: fastRetry})
	if j := e.waitStatus(t, id, "dead"); j.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1", j.Attempts)
	}
}

func TestE2ECrashRecovery(t *testing.T) {
	e := newPG(t)
	started := make(chan struct{})
	var startOnce sync.Once
	var runs atomic.Int32
	reg := NewRegistry()
	reg.Register("crashy", func(ctx context.Context, j store.Job) error {
		runs.Add(1)
		if j.Attempts == 1 {
			startOnce.Do(func() { close(started) })
			<-ctx.Done() // blocked until pool A is hard-stopped
			return ctx.Err()
		}
		return nil
	})
	id := e.create(t, "crashy", 3)

	stopA := e.runPool(t, Config{Registry: reg, Lease: time.Second, Backoff: fastRetry,
		ShutdownTimeout: 10 * time.Millisecond, WorkerID: "pool-a"})
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("handler never started on pool A")
	}
	stopA() // heartbeats stop, handler cancelled, nothing reported

	j, err := e.st.GetJob(e.ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if j.Status != "running" {
		t.Fatalf("after hard stop job is %q, want running (abandoned, not reported)", j.Status)
	}

	e.runPool(t, Config{Registry: reg, Lease: time.Second, Backoff: fastRetry, WorkerID: "pool-b"})
	j = e.waitStatus(t, id, "succeeded")
	if j.Attempts != 2 || runs.Load() != 2 {
		t.Fatalf("attempts=%d runs=%d, want 2 and 2", j.Attempts, runs.Load())
	}
	if got := e.outcomes(t, id); !eqStrings(got, []string{"lease_expired", "succeeded"}) {
		t.Fatalf("attempt rows = %v", got)
	}
}
