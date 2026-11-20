package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"tandem/internal/config"
	"tandem/internal/metrics"
	"tandem/internal/scheduler"
	"tandem/internal/store"
)

// schedulerLeaderKey is the advisory-lock key for the cron leader ("TQCRON").
const schedulerLeaderKey int64 = 0x54514352_4f4e

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("svc", "scheduler")
	cfg := config.Load()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := store.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("open store", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	// The scheduler is the only binary that exports queue depth: one source of
	// truth, no duplicate series from N workers or API replicas.
	mreg := metrics.NewRegistry()
	mreg.MustRegister(metrics.NewQueueDepth(st, log))
	sm := metrics.NewScheduler(mreg)
	mctx, stopMetrics := context.WithCancel(context.Background())
	waitMetrics := metrics.Serve(mctx, cfg.MetricsAddr, mreg, log)
	defer func() { stopMetrics(); waitMetrics() }()

	reaper := &scheduler.Reaper{Store: st, Interval: cfg.ReapInterval(), BatchSize: cfg.ReapBatch, Logger: log, Metrics: sm}
	cron := &scheduler.CronRunner{Store: st, Interval: cfg.CronInterval(), Logger: log, Metrics: sm}
	elector := &scheduler.Elector{
		Store: scheduler.PGLocker{S: st}, Key: schedulerLeaderKey,
		RetryInterval: cfg.LeaderRetry(), CheckInterval: cfg.LeaderCheck(), Logger: log,
	}
	log.Info("started", "reap_interval", cfg.ReapInterval().String(), "reap_batch", cfg.ReapBatch,
		"cron_interval", cfg.CronInterval().String(), "leader_retry", cfg.LeaderRetry().String(), "leader_check", cfg.LeaderCheck().String())

	var wg sync.WaitGroup
	failed := make(chan struct{}, 2)
	run := func(name string, f func(context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := f(ctx); err != nil {
				log.Error(name, "err", err)
				failed <- struct{}{}
				stop()
			}
		}()
	}
	// The reaper is safe concurrently, so it runs on every replica; the cron
	// runner only runs while this replica holds the leader lock.
	run("reaper", reaper.Run)
	run("elector", func(ctx context.Context) error { return elector.Run(ctx, cron.Run) })
	wg.Wait() // returns after ctx is cancelled and the lock has been released
	log.Info("shutting down")
	if len(failed) > 0 {
		os.Exit(1)
	}
}
