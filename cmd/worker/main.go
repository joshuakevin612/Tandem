package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"taskq/internal/config"
	"taskq/internal/queue"
	"taskq/internal/store"
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
		Logger:      log,
	})

	log.Info("started", "queues", cfg.WorkerQueues, "concurrency", cfg.WorkerConcurrency)
	if err := pool.Run(ctx); err != nil {
		log.Error("pool", "err", err)
		os.Exit(1)
	}
	log.Info("shut down")
}
