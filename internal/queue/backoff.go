// Package queue holds queue-policy helpers shared by workers.
package queue

import (
	"math"
	"math/rand/v2"
	"time"
)

// Backoff computes retry delays: exponential growth with full jitter.
type Backoff struct{ Base, Max time.Duration }

// Delay returns a uniform random duration in [0, min(Max, Base*2^(attempt-1))].
// attempt is 1-based (values < 1 are treated as 1). A Max <= 0 means no cap
// beyond what time.Duration can represent. It never overflows.
func (b Backoff) Delay(attempt int) time.Duration {
	if b.Base <= 0 {
		return 0
	}
	return jitter(b.ceiling(attempt))
}

// ceiling is the upper bound of the jitter window for an attempt.
func (b Backoff) ceiling(attempt int) time.Duration {
	limit := time.Duration(math.MaxInt64)
	if b.Max > 0 {
		limit = b.Max
	}
	d := b.Base
	for i := 1; i < attempt; i++ {
		if d >= limit/2 { // doubling would reach or pass the cap
			return limit
		}
		d *= 2
	}
	return min(d, limit)
}

func jitter(ceil time.Duration) time.Duration {
	if ceil >= math.MaxInt64 {
		return time.Duration(rand.Int64N(math.MaxInt64))
	}
	return time.Duration(rand.Int64N(int64(ceil) + 1))
}
