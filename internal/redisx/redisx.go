// Package redisx builds the optional Redis client. Redis is never on the
// correctness path: callers must treat a nil client as "feature off" and
// every Redis error as a cache miss / no-op.
package redisx

import (
	"time"

	"github.com/redis/go-redis/v9"
)

// New returns a client for url, or nil, nil when url is empty (Redis off).
// Timeouts are tight and retries off so a sick Redis fails fast instead of
// slowing requests down.
func New(url string) (*redis.Client, error) {
	if url == "" {
		return nil, nil
	}
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	opt.DialTimeout = 200 * time.Millisecond
	opt.ReadTimeout = 100 * time.Millisecond
	opt.WriteTimeout = 100 * time.Millisecond
	// go-redis quirks: MaxRetries 0 means "default (3)", -1 means none, and
	// dials are retried DialerRetries times (<=0 means default 5). Left at the
	// defaults, a stopped Redis cost ~6s per call.
	opt.MaxRetries = -1
	opt.DialerRetries = 1
	opt.PoolTimeout = 200 * time.Millisecond
	// Honour caller deadlines (e.g. the wake publisher's 100ms budget).
	opt.ContextTimeoutEnabled = true
	return redis.NewClient(opt), nil
}
