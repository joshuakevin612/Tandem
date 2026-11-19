// Package wake carries best-effort "a job was enqueued" hints from the API to
// idle workers over Redis pub/sub, so an idle worker need not wait out its
// poll interval.
//
// Redis is never on the correctness path: polling stays in place as the
// fallback, so a lost, late or failed wake-up costs at most one PollInterval.
package wake

import (
	"context"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

// Channel is the pub/sub channel; the payload is the queue name.
const Channel = "taskq:enqueue"

const publishTimeout = 100 * time.Millisecond

// Publisher implements api.Notifier.
type Publisher struct {
	rdb redis.Cmdable
	log *slog.Logger
}

// NewPublisher returns a Publisher. log may be nil.
func NewPublisher(rdb redis.Cmdable, log *slog.Logger) *Publisher {
	return &Publisher{rdb: rdb, log: log}
}

// Notify publishes a wake-up for queue. It blocks for at most 100ms and
// ignores errors; no goroutines are spawned.
func (p *Publisher) Notify(ctx context.Context, queue string) {
	// Detached from the request so a cancelled request still publishes, but bounded.
	pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), publishTimeout)
	defer cancel()
	if err := p.rdb.Publish(pctx, Channel, queue).Err(); err != nil && p.log != nil {
		p.log.Debug("wake publish failed", "err", err)
	}
}

const (
	minBackoff = 100 * time.Millisecond
	maxBackoff = 5 * time.Second
)

// Subscribe returns a channel that receives a value whenever a wake-up
// arrives. The channel has capacity 1 and sends never block, so bursts
// coalesce into one pending signal. If the subscription drops it is retried
// with capped exponential backoff. The channel is never closed (receivers
// can select on it safely; stop the goroutine by cancelling ctx). A nil rdb
// returns a nil channel, which blocks forever in a select: pure polling.
func Subscribe(ctx context.Context, rdb *redis.Client, log *slog.Logger) <-chan struct{} {
	if rdb == nil {
		return nil
	}
	out := make(chan struct{}, 1)
	go func() {
		backoff := minBackoff
		for ctx.Err() == nil {
			start := time.Now()
			err := consume(ctx, rdb, out)
			if ctx.Err() != nil {
				return
			}
			if time.Since(start) > maxBackoff {
				backoff = minBackoff // it was healthy for a while
			}
			if log != nil {
				log.Debug("wake subscription dropped", "err", err, "retry_in", backoff)
			}
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return
			}
			backoff = min(backoff*2, maxBackoff)
		}
	}()
	return out
}

// consume runs one subscription until it drops or ctx is done.
func consume(ctx context.Context, rdb *redis.Client, out chan<- struct{}) error {
	sub := rdb.Subscribe(ctx, Channel)
	defer sub.Close()
	// Wait for the server's confirmation so failures surface here.
	if _, err := sub.Receive(ctx); err != nil {
		return err
	}
	ch := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case _, ok := <-ch:
			if !ok {
				return context.Canceled
			}
			select {
			case out <- struct{}{}:
			default:
			}
		}
	}
}
