// Package scheduler runs background maintenance loops against the store.
package scheduler

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"time"

	"taskq/internal/metrics"
)

// ReaperStore is the subset of store.Store the reaper uses.
type ReaperStore interface {
	ReapExpired(ctx context.Context, limit int) (requeued, dead int, err error)
}

// Reaper periodically recovers jobs whose lease expired. Any number of
// reapers may run at once; the store hands each a disjoint batch.
type Reaper struct {
	Store     ReaperStore
	Interval  time.Duration // default 1s
	BatchSize int           // default 500
	Logger    *slog.Logger
	Metrics   *metrics.Scheduler // optional
}

// Run blocks until ctx is cancelled, then returns nil.
func (r *Reaper) Run(ctx context.Context) error {
	interval, batch, log := r.Interval, r.BatchSize, r.Logger
	if interval <= 0 {
		interval = time.Second
	}
	if batch <= 0 {
		batch = 500
	}
	if log == nil {
		log = slog.Default()
	}
	for ctx.Err() == nil {
		// Drain: keep going while batches come back full.
		for ctx.Err() == nil {
			requeued, dead, err := r.Store.ReapExpired(ctx, batch)
			if err != nil {
				if ctx.Err() == nil {
					log.Error("reap failed", "err", err)
				}
				break
			}
			r.Metrics.Reaped(requeued + dead)
			if requeued+dead > 0 {
				log.Info("reaped expired leases", "requeued", requeued, "dead", dead)
			}
			if requeued+dead < batch {
				break
			}
		}
		t := time.NewTimer(interval/2 + rand.N(interval)) // jitter: [0.5i, 1.5i)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
		}
	}
	return nil
}
