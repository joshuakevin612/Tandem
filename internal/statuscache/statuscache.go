// Package statuscache caches terminal job rows in Redis for fast status reads.
//
// Why this is safe: only terminal statuses (succeeded, dead, cancelled) are
// ever stored, and Set enforces that itself. The only transition out of a
// terminal state is an API-side requeue, which deletes the key before it
// responds. The residual race is a GET that read the terminal row from
// Postgres before the requeue and calls Set after the requeue's Del; that
// stale entry is bounded to one TTL. Redis is never on the correctness path:
// every Redis error is a cache miss or a no-op.
package statuscache

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"tandem/internal/store"
)

// Cache implements api.StatusCache over Redis.
type Cache struct {
	rdb redis.Cmdable
	ttl time.Duration
	log *slog.Logger
}

// New returns a Cache with the given TTL. log may be nil.
func New(rdb redis.Cmdable, ttl time.Duration, log *slog.Logger) *Cache {
	return &Cache{rdb: rdb, ttl: ttl, log: log}
}

func key(id uuid.UUID) string { return "job:" + id.String() }

func terminal(status string) bool {
	return status == "succeeded" || status == "dead" || status == "cancelled"
}

func (c *Cache) warn(msg string, err error) {
	if c.log != nil {
		c.log.Warn(msg, "err", err)
	}
}

// Get returns the cached job; any error is a miss.
func (c *Cache) Get(ctx context.Context, id uuid.UUID) (store.Job, bool) {
	b, err := c.rdb.Get(ctx, key(id)).Bytes()
	if err != nil {
		if !errors.Is(err, redis.Nil) {
			c.warn("status cache get", err)
		}
		return store.Job{}, false
	}
	var j store.Job
	if err := json.Unmarshal(b, &j); err != nil {
		c.warn("status cache decode", err)
		return store.Job{}, false
	}
	return j, true
}

// Set stores j only if its status is terminal; otherwise it is a no-op.
func (c *Cache) Set(ctx context.Context, j store.Job) {
	if !terminal(j.Status) || c.ttl <= 0 {
		return
	}
	b, err := json.Marshal(j)
	if err != nil {
		c.warn("status cache encode", err)
		return
	}
	if err := c.rdb.Set(ctx, key(j.ID), b, c.ttl).Err(); err != nil {
		c.warn("status cache set", err)
	}
}

// Del removes id from the cache.
func (c *Cache) Del(ctx context.Context, id uuid.UUID) {
	if err := c.rdb.Del(ctx, key(id)).Err(); err != nil {
		c.warn("status cache del", err)
	}
}
