package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"tandem/internal/metrics"
	"tandem/internal/store"
)

// histCount sums the observation count of every series of the named histogram
// whose labels include want.
func histCount(t *testing.T, reg *prometheus.Registry, name string, want map[string]string) uint64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var n uint64
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			have := map[string]string{}
			for _, l := range m.GetLabel() {
				have[l.GetName()] = l.GetValue()
			}
			match := true
			for k, v := range want {
				if have[k] != v {
					match = false
				}
			}
			if match {
				n += m.GetHistogram().GetSampleCount()
			}
		}
	}
	return n
}

func TestMetricsRecordedForCompletedAndFailedJobs(t *testing.T) {
	f, reg := newFake(3, "x"), NewRegistry()
	reg.Register("x", func(_ context.Context, j store.Job) error {
		if j.Attempts == 1 && j.MaxAttempts == 3 {
			return errors.New("boom")
		}
		return nil
	})
	// One job that fails with attempts remaining: the worker can tell it is retried.
	tok := uuid.New()
	f.mu.Lock()
	f.queue = append(f.queue, store.Job{ID: uuid.New(), Type: "x", Attempts: 1, MaxAttempts: 3, LeaseToken: &tok})
	f.mu.Unlock()
	// Unregistered type: must be bucketed as "unknown", not leak the raw type.
	f.add("nope")

	mreg := prometheus.NewRegistry()
	m := metrics.NewWorker(mreg)
	p := NewPool(Config{
		Store: f, Registry: reg, Concurrency: 2, Lease: time.Minute,
		PollInterval: time.Millisecond, ShutdownTimeout: 5 * time.Second, Logger: quiet, Metrics: m,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	for range 3 {
		recv(t, f.completed)
	}
	for range 2 {
		recv(t, f.failed)
	}
	cancel()
	if err := <-done; err != nil { // Run returns only after every handler and report finished
		t.Fatal(err)
	}

	for _, tc := range []struct {
		labels map[string]string
		want   uint64
	}{
		{map[string]string{"type": "x", "outcome": "succeeded"}, 3},
		{map[string]string{"type": "x", "outcome": "retried"}, 1},
		{map[string]string{"type": "unknown"}, 1},
	} {
		if got := histCount(t, mreg, "tandem_attempt_duration_seconds", tc.labels); got != tc.want {
			t.Errorf("attempt count %v = %d, want %d", tc.labels, got, tc.want)
		}
	}
	if got := gaugeValue(t, mreg, "tandem_in_flight_jobs"); got != 0 {
		t.Errorf("in_flight_jobs = %v, want 0", got)
	}
	if got := histCount(t, mreg, "tandem_lease_duration_seconds", map[string]string{"queue": "default"}); got == 0 {
		t.Error("lease duration not observed")
	}
}

// gaugeValue reads the single series of the named gauge.
func gaugeValue(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() == name {
			return mf.GetMetric()[0].GetGauge().GetValue()
		}
	}
	t.Fatalf("%s not registered", name)
	return 0
}

func TestNilMetricsPoolStillRuns(t *testing.T) {
	f, reg := newFake(2, "x"), NewRegistry()
	reg.Register("x", func(context.Context, store.Job) error { return nil })
	start(t, f, reg, 2) // Metrics unset
	for range 2 {
		recv(t, f.completed)
	}
}
