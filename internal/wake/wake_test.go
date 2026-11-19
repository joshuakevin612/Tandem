package wake

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"taskq/internal/redisx"
	"taskq/internal/redisx/redistest"
)

func TestPublishSubscribe(t *testing.T) {
	rdb := redistest.Start(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := Subscribe(ctx, rdb, nil)
	pub := NewPublisher(rdb, nil)

	// The subscription is established asynchronously; publish until seen.
	deadline := time.After(10 * time.Second)
	for {
		pub.Notify(ctx, "default")
		select {
		case <-ch:
			return
		case <-time.After(50 * time.Millisecond):
		case <-deadline:
			t.Fatal("no wake-up received")
		}
	}
}

func TestBurstCoalesces(t *testing.T) {
	rdb := redistest.Start(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := Subscribe(ctx, rdb, nil)
	pub := NewPublisher(rdb, nil)
	// Make sure the subscription is live first.
	for ready := false; !ready; {
		pub.Notify(ctx, "q")
		select {
		case <-ch:
			ready = true
		case <-time.After(50 * time.Millisecond):
		}
	}
	for range 500 { // nobody is receiving: sends must not block
		pub.Notify(ctx, "q")
	}
	<-ch // at least one pending
	// Drain; capacity is 1, so at most one more can be buffered.
	n := 0
	for {
		select {
		case <-ch:
			n++
			continue
		case <-time.After(300 * time.Millisecond):
		}
		break
	}
	if n > 1 {
		t.Fatalf("got %d extra signals, want <= 1", n)
	}
}

func TestNilClientNilChannel(t *testing.T) {
	if ch := Subscribe(context.Background(), nil, nil); ch != nil {
		t.Fatal("want nil channel")
	}
}

func TestPublisherUnreachableRedis(t *testing.T) {
	rdb, err := redisx.New("redis://127.0.0.1:1/0")
	if err != nil {
		t.Fatal(err)
	}
	defer rdb.Close()
	pub := NewPublisher(rdb, nil)
	start := time.Now()
	pub.Notify(context.Background(), "default")
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("Notify took %s", d)
	}
}

var _ redis.Cmdable = (*redis.Client)(nil)
