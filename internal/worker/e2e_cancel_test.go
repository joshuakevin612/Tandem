package worker

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"taskq/internal/store"
)

// startPool runs a pool with a short lease and heartbeat and returns a stop
// function that drains it.
func startPool(t *testing.T, e *pgEnv, reg *Registry, heartbeat time.Duration) (stop func()) {
	t.Helper()
	runCtx, cancel := context.WithCancel(e.ctx)
	p := NewPool(Config{
		Store: e.st, Registry: reg, Concurrency: 2, Lease: 600 * time.Millisecond, Heartbeat: heartbeat,
		PollInterval: 20 * time.Millisecond, Logger: quiet,
	})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := p.Run(runCtx); err != nil {
			t.Errorf("Run: %v", err)
		}
	}()
	var once sync.Once
	stop = func() { once.Do(func() { cancel(); wg.Wait() }) }
	t.Cleanup(stop)
	return stop
}

func TestEndToEndCancelRunningJob(t *testing.T) {
	const heartbeat = 100 * time.Millisecond
	e := newPG(t)

	started := make(chan struct{})
	stopped := make(chan time.Time, 1)
	var runs atomic.Int64
	reg := NewRegistry()
	reg.Register("block", func(ctx context.Context, _ store.Job) error {
		runs.Add(1)
		close(started)
		<-ctx.Done()
		stopped <- time.Now()
		return ctx.Err()
	})
	id := e.create(t, "block", 5)
	stop := startPool(t, e, reg, heartbeat)

	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("handler never started")
	}
	cancelledAt := time.Now()
	j, err := e.st.CancelJob(e.ctx, "default", id)
	if err != nil || j.Status != "cancelled" {
		t.Fatalf("CancelJob: %+v %v", j, err)
	}

	select {
	case at := <-stopped:
		// The next heartbeat finds the lease gone; allow a couple of beats plus
		// scheduling slack.
		if d := at.Sub(cancelledAt); d > 5*heartbeat {
			t.Errorf("handler context cancelled %v after CancelJob, want within a few heartbeats (%v)", d, heartbeat)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler context never cancelled after CancelJob")
	}

	// Drain the pool: the result write (if the worker wrongly attempted one)
	// happens before Run returns. Also let the reaper run several cycles.
	time.Sleep(time.Second)
	stop()

	got, err := e.st.GetJob(e.ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "cancelled" || got.Attempts != 1 || got.LeaseToken != nil {
		t.Errorf("final job: status=%q attempts=%d lease=%v err=%v, want cancelled/1/nil", got.Status, got.Attempts, got.LeaseToken, got.LastError)
	}
	if n := runs.Load(); n != 1 {
		t.Errorf("handler ran %d times, want 1", n)
	}
	var rows int
	var outcome string
	err = e.raw.QueryRow(e.ctx, `SELECT count(*), coalesce(max(outcome), '') FROM job_attempts WHERE job_id=$1`, id).Scan(&rows, &outcome)
	if err != nil {
		t.Fatal(err)
	}
	if rows != 1 || outcome != "cancelled" {
		t.Errorf("attempt rows=%d outcome=%q, want 1 / cancelled", rows, outcome)
	}
	var open int
	if err := e.raw.QueryRow(e.ctx, `SELECT count(*) FROM job_attempts WHERE job_id=$1 AND finished_at IS NULL`, id).Scan(&open); err != nil || open != 0 {
		t.Errorf("open attempts = %d (%v)", open, err)
	}
}

func TestEndToEndCancelQueuedJobNeverRuns(t *testing.T) {
	e := newPG(t)

	var runs atomic.Int64
	reg := NewRegistry()
	reg.Register("never", func(context.Context, store.Job) error {
		runs.Add(1)
		return nil
	})
	id := e.create(t, "never", 5)
	j, err := e.st.CancelJob(e.ctx, "default", id)
	if err != nil || j.Status != "cancelled" {
		t.Fatalf("CancelJob: %+v %v", j, err)
	}

	// A sentinel job proves the pool was polling and leasing while the
	// cancelled one sat there.
	var sentinelRan atomic.Int64
	reg.Register("sentinel", func(context.Context, store.Job) error {
		sentinelRan.Add(1)
		return nil
	})
	sentinel := e.create(t, "sentinel", 5)
	stop := startPool(t, e, reg, 100*time.Millisecond)
	e.waitStatus(t, sentinel, "succeeded")
	time.Sleep(300 * time.Millisecond)
	stop()

	if runs.Load() != 0 {
		t.Errorf("cancelled queued job ran %d times", runs.Load())
	}
	got, err := e.st.GetJob(e.ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "cancelled" || got.Attempts != 0 {
		t.Errorf("job: status=%q attempts=%d, want cancelled/0", got.Status, got.Attempts)
	}
	var rows int
	if err := e.raw.QueryRow(e.ctx, `SELECT count(*) FROM job_attempts WHERE job_id=$1`, id).Scan(&rows); err != nil || rows != 0 {
		t.Errorf("attempt rows = %d (%v), want 0", rows, err)
	}
}
