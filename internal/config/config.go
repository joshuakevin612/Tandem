// Package config loads process configuration from the environment.
package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	DatabaseURL       string
	HTTPAddr          string
	WorkerQueues      []string
	WorkerConcurrency int
	LeaseSeconds      int
	ReapIntervalMS    int
	ReapBatch         int
	RetryBaseMS       int
	RetryMaxMS        int
	JobTimeoutSeconds int
	CronIntervalMS    int
	LeaderRetryMS     int
	LeaderCheckMS     int
}

func Load() Config {
	return Config{
		DatabaseURL:       str("DATABASE_URL", "postgres://taskq:taskq@localhost:5432/taskq?sslmode=disable"),
		HTTPAddr:          str("HTTP_ADDR", ":8080"),
		WorkerQueues:      list("WORKER_QUEUES", "default"),
		WorkerConcurrency: num("WORKER_CONCURRENCY", 10),
		LeaseSeconds:      num("LEASE_SECONDS", 30),
		ReapIntervalMS:    num("REAP_INTERVAL_MS", 1000),
		ReapBatch:         num("REAP_BATCH", 500),
		RetryBaseMS:       num("RETRY_BASE_MS", 1000),
		RetryMaxMS:        num("RETRY_MAX_MS", 300000),
		JobTimeoutSeconds: num("JOB_TIMEOUT_SECONDS", 900),
		CronIntervalMS:    num("CRON_INTERVAL_MS", 1000),
		LeaderRetryMS:     num("LEADER_RETRY_MS", 2000),
		LeaderCheckMS:     num("LEADER_CHECK_MS", 1000),
	}
}

func (c Config) Lease() time.Duration { return time.Duration(c.LeaseSeconds) * time.Second }

func (c Config) ReapInterval() time.Duration {
	return time.Duration(c.ReapIntervalMS) * time.Millisecond
}

func str(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func num(key string, def int) int {
	if n, err := strconv.Atoi(os.Getenv(key)); err == nil && n > 0 {
		return n
	}
	return def
}

func list(key, def string) []string {
	var out []string
	for _, p := range strings.Split(str(key, def), ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// RetryBase is the base of the exponential retry backoff.
func (c Config) RetryBase() time.Duration { return time.Duration(c.RetryBaseMS) * time.Millisecond }

// RetryMax caps the retry backoff.
func (c Config) RetryMax() time.Duration { return time.Duration(c.RetryMaxMS) * time.Millisecond }

// JobTimeout is the per-attempt handler deadline.
func (c Config) JobTimeout() time.Duration { return time.Duration(c.JobTimeoutSeconds) * time.Second }

// CronInterval is how often the leader looks for due schedules.
func (c Config) CronInterval() time.Duration {
	return time.Duration(c.CronIntervalMS) * time.Millisecond
}

// LeaderRetry is how often a follower tries to take the leader lock.
func (c Config) LeaderRetry() time.Duration { return time.Duration(c.LeaderRetryMS) * time.Millisecond }

// LeaderCheck is how often the leader verifies it still holds the lock.
func (c Config) LeaderCheck() time.Duration { return time.Duration(c.LeaderCheckMS) * time.Millisecond }
