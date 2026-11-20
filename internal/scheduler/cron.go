package scheduler

import (
	"context"
	"log/slog"
	"time"

	"tandem/internal/cronexpr"
	"tandem/internal/metrics"
	"tandem/internal/store"
)

// CronStore is the subset of store.Store the cron runner uses.
type CronStore interface {
	DueSchedules(ctx context.Context, limit int) ([]store.Schedule, error)
	FireSchedule(ctx context.Context, s store.Schedule, next time.Time) (bool, error)
}

// CronRunner fires due schedules. It is safe to run on several replicas at once
// because FireSchedule arbitrates; the Elector only avoids wasted work.
type CronRunner struct {
	Store     CronStore
	Interval  time.Duration // default 1s
	BatchSize int           // default 100
	Logger    *slog.Logger
	Now       func() time.Time   // default time.Now
	Metrics   *metrics.Scheduler // optional
}

// Run blocks until ctx is cancelled, then returns nil.
func (c *CronRunner) Run(ctx context.Context) error {
	interval, batch, log, now := c.Interval, c.BatchSize, c.Logger, c.Now
	if interval <= 0 {
		interval = time.Second
	}
	if batch <= 0 {
		batch = 100
	}
	if log == nil {
		log = slog.Default()
	}
	if now == nil {
		now = time.Now
	}
	for ctx.Err() == nil {
		for ctx.Err() == nil {
			due, err := c.Store.DueSchedules(ctx, batch)
			if err != nil {
				if ctx.Err() == nil {
					log.Error("list due schedules failed", "err", err)
				}
				break
			}
			handled := 0
			for _, s := range due {
				if ctx.Err() != nil {
					break
				}
				if c.fire(ctx, s, now(), log) {
					handled++
				}
			}
			// Drain while batches are full, but only if we made progress: a
			// batch of unfireable schedules would otherwise spin forever.
			if len(due) < batch || handled == 0 {
				break
			}
		}
		sleep(ctx, interval)
	}
	return nil
}

// fire reports whether the schedule was dealt with (fired or lost the race).
func (c *CronRunner) fire(ctx context.Context, s store.Schedule, now time.Time, log *slog.Logger) bool {
	l := log.With("schedule_id", s.ID, "name", s.Name)
	expr, err := cronexpr.Parse(s.Cron, s.Timezone)
	if err != nil {
		l.Error("schedule cron does not parse; skipping", "cron", s.Cron, "tz", s.Timezone, "err", err)
		return false
	}
	// Next is the first fire strictly after now, so a long outage collapses
	// into ONE catch-up fire instead of a backfill storm. The max() keeps
	// next after s.NextFireAt if this host's clock is behind the database's.
	base := now
	if s.NextFireAt.After(base) {
		base = s.NextFireAt
	}
	next := expr.Next(base)
	if next.IsZero() {
		l.Error("schedule never fires again; skipping", "cron", s.Cron)
		return false
	}
	fired, err := c.Store.FireSchedule(ctx, s, next)
	if err != nil {
		if ctx.Err() == nil {
			l.Error("fire schedule failed", "err", err)
		}
		return false
	}
	if fired {
		c.Metrics.ScheduleFired()
		l.Info("fired schedule", "fire_at", s.NextFireAt, "next", next)
	} else {
		l.Debug("schedule already fired by another scheduler", "fire_at", s.NextFireAt)
	}
	return true
}
