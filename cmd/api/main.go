package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"taskq/internal/api"
	"taskq/internal/config"
	"taskq/internal/ratelimit"
	"taskq/internal/redisx"
	"taskq/internal/statuscache"
	"taskq/internal/store"
	"taskq/internal/wake"
)

// healthcheck probes /readyz on the local listener for the distroless image,
// which has no curl. The return value is the process exit code.
func healthcheck(addr string) int {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return 1
	}
	c := http.Client{Timeout: 2 * time.Second}
	resp, err := c.Get("http://127.0.0.1:" + port + "/readyz")
	if err != nil {
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}

func main() {
	hc := flag.Bool("healthcheck", false, "probe /readyz and exit 0/1")
	flag.Parse()
	if *hc {
		os.Exit(healthcheck(config.Load().HTTPAddr))
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("svc", "api")
	cfg := config.Load()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := store.Migrate(cfg.DatabaseURL); err != nil {
		log.Error("migrate", "err", err)
		os.Exit(1)
	}
	st, err := store.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("open store", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	// Redis is optional and never on the correctness path; nil means off.
	rdb, err := redisx.New(cfg.RedisURL)
	if err != nil {
		log.Error("redis url", "err", err)
		os.Exit(1)
	}
	if rdb != nil {
		defer rdb.Close()
	}

	var deps api.Deps
	if rdb != nil {
		if cfg.RateLimitEnabled {
			deps.Limiter = ratelimit.New(rdb, cfg.RateLimitRate, cfg.RateLimitBurst)
		}
		if cfg.WakeEnabled {
			deps.Notifier = wake.NewPublisher(rdb, log)
		}
		if cfg.StatusCacheTTLMS > 0 {
			deps.Cache = statuscache.New(rdb, time.Duration(cfg.StatusCacheTTLMS)*time.Millisecond, log)
		}
	}
	switch cfg.AuthMode {
	case api.AuthModeHeader:
		log.Warn("AUTH_MODE=header: tenant is taken from the unauthenticated X-Tenant-ID header; anyone can act as any tenant. Use AUTH_MODE=apikey outside development.")
	case api.AuthModeAPIKey:
		deps.Auth = api.APIKeyAuth(st)
	default:
		log.Error("invalid AUTH_MODE", "value", cfg.AuthMode)
		os.Exit(1)
	}

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.NewServer(st, log, api.WithDeps(deps)),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	log.Info("started", "addr", cfg.HTTPAddr)

	select {
	case err := <-errc:
		log.Error("serve", "err", err)
		st.Close()
		os.Exit(1)
	case <-ctx.Done():
	}

	log.Info("shutting down")
	shutCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("shutdown", "err", err)
	}
	// st.Close runs via defer after the drain completes.
}
