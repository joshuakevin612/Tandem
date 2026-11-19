package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"taskq/internal/config"
	"taskq/internal/metrics"
	"taskq/internal/queue"
	"taskq/internal/redisx"
	"taskq/internal/store"
	"taskq/internal/wake"
	"taskq/internal/worker"
	"taskq/internal/worker/handlers"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("svc", "worker")
	cfg := config.Load()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := store.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("open store", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	// Redis is optional and only used for wake-up hints; nil means pure polling.
	rdb, err := redisx.New(cfg.RedisURL)
	if err != nil {
		log.Error("redis url", "err", err)
		os.Exit(1)
	}
	var wakeCh <-chan struct{}
	if rdb != nil {
		defer rdb.Close()
		if cfg.WakeEnabled {
			wakeCh = wake.Subscribe(ctx, rdb, log)
		}
	}

	// The metrics server outlives the pool drain so in-flight work stays visible.
	mreg := metrics.NewRegistry()
	mctx, stopMetrics := context.WithCancel(context.Background())
	waitMetrics := metrics.Serve(mctx, cfg.MetricsAddr, mreg, log)
	defer func() { stopMetrics(); waitMetrics() }()

	reg := worker.NewRegistry()
	handlers.RegisterBuiltins(reg)
	pool := worker.NewPool(worker.Config{
		Store:       st,
		Registry:    reg,
		Queues:      cfg.WorkerQueues,
		Concurrency: cfg.WorkerConcurrency,
		Lease:       cfg.Lease(),
		JobTimeout:  cfg.JobTimeout(),
		Backoff:     queue.Backoff{Base: cfg.RetryBase(), Max: cfg.RetryMax()},
		Wake:        wakeCh,
		Logger:      log,
		Metrics:     metrics.NewWorker(mreg),
	})

	log.Info("started", "queues", cfg.WorkerQueues, "concurrency", cfg.WorkerConcurrency)
	if err := pool.Run(ctx); err != nil {
		log.Error("pool", "err", err)
		os.Exit(1)
	}
	log.Info("shut down")
}
