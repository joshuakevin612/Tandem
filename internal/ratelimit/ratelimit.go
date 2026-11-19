// Package ratelimit is a Redis-backed per-tenant token bucket.
//
// Redis is never on the correctness path: Allow returns an error when Redis
// is unreachable and callers must treat that as "allow".
package ratelimit

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed bucket.lua
var bucketSrc string

var script = redis.NewScript(bucketSrc)

// Limiter is a token bucket per tenant. All time comes from Redis TIME inside
// the script so API replicas agree regardless of client clock skew.
type Limiter struct {
	rdb   redis.Scripter
	rate  float64 // tokens per second
	burst int
}

// New returns a Limiter allowing rate requests/second with bursts up to burst.
func New(rdb redis.Scripter, rate float64, burst int) *Limiter {
	return &Limiter{rdb: rdb, rate: rate, burst: burst}
}

// Allow takes one token for tenant. When denied it reports how long until a
// token is available.
func (l *Limiter) Allow(ctx context.Context, tenant string) (bool, time.Duration, error) {
	res, err := script.Run(ctx, l.rdb, []string{"rl:{" + tenant + "}"}, l.rate, l.burst).Slice()
	if err != nil {
		return false, 0, err
	}
	if len(res) != 2 {
		return false, 0, errors.New("ratelimit: unexpected script reply")
	}
	allowed, ok1 := res[0].(int64)
	retry, ok2 := res[1].(int64)
	if !ok1 || !ok2 {
		return false, 0, fmt.Errorf("ratelimit: unexpected script reply types %T %T", res[0], res[1])
	}
	return allowed == 1, time.Duration(retry) * time.Millisecond, nil
}
