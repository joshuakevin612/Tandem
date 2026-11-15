package scheduler

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

type result struct {
	requeued, dead int
	err            error
}

// fakeStore returns results in order, then (0, 0, nil).
type fakeStore struct {
	mu      sync.Mutex
	limits  []int
	results []result
}

func (f *fakeStore) ReapExpired(_ context.Context, limit int) (int, int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.limits = append(f.limits, limit)
	var r result
	if len(f.results) > 0 {
		r, f.results = f.results[0], f.results[1:]
	}
	return r.requeued, r.dead, r.err
}

func (f *fakeStore) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.limits)
}

func startReaper(t *testing.T, f *fakeStore, interval time.Duration) (stop func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	r := &Reaper{Store: f, Interval: interval, BatchSize: 10, Logger: quiet}
	go func() { done <- r.Run(ctx) }()
	return func() error {
		cancel()
		select {
		case err := <-done:
			return err
		case <-time.After(5 * time.Second):
			t.Fatal("reaper did not stop on cancel")
			return nil
		}
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestReaperDrainsFullBatches(t *testing.T) {
	// An hour-long interval proves the extra calls come from draining, not ticking.
	f := &fakeStore{results: []result{{requeued: 7, dead: 3}, {requeued: 10}, {requeued: 2}}}
	stop := startReaper(t, f, time.Hour)
	waitFor(t, func() bool { return f.calls() >= 3 })
	time.Sleep(20 * time.Millisecond)
	if got := f.calls(); got != 3 {
		t.Fatalf("calls = %d, want 3 (two full batches then a partial one)", got)
	}
	if f.limits[0] != 10 {
		t.Fatalf("batch limit = %d", f.limits[0])
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}
}

func TestReaperSurvivesError(t *testing.T) {
	f := &fakeStore{results: []result{{err: errors.New("db down")}, {requeued: 1}}}
	stop := startReaper(t, f, 2*time.Millisecond)
	waitFor(t, func() bool { return f.calls() >= 3 }) // kept polling after the error
	if err := stop(); err != nil {
		t.Fatal(err)
	}
}

func TestReaperStopsOnCancel(t *testing.T) {
	f := &fakeStore{}
	stop := startReaper(t, f, time.Hour)
	waitFor(t, func() bool { return f.calls() >= 1 })
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	n := f.calls()
	time.Sleep(20 * time.Millisecond)
	if f.calls() != n {
		t.Fatal("reaper kept running after cancel")
	}
}
