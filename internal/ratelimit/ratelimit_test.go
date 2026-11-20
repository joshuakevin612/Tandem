package ratelimit

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"tandem/internal/redisx/redistest"
)

func TestBurstThenDenied(t *testing.T) {
	l := New(redistest.Start(t), 1, 3) // slow refill: 1/s
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		ok, _, err := l.Allow(ctx, "a")
		if err != nil || !ok {
			t.Fatalf("call %d: ok=%v err=%v", i, ok, err)
		}
	}
	ok, retry, err := l.Allow(ctx, "a")
	if err != nil || ok {
		t.Fatalf("want denied, ok=%v err=%v", ok, err)
	}
	if retry <= 0 || retry > 1100*time.Millisecond {
		t.Fatalf("retry = %v", retry)
	}
	if ok, _, _ := l.Allow(ctx, "b"); !ok {
		t.Fatal("tenant b should be allowed")
	}
}

func TestRefill(t *testing.T) {
	l := New(redistest.Start(t), 1000, 2)
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if ok, _, err := l.Allow(ctx, "a"); err != nil || !ok {
			t.Fatalf("burst call %d: ok=%v err=%v", i, ok, err)
		}
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		ok, _, err := l.Allow(ctx, "a")
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("bucket never refilled")
		}
	}
}

func TestConcurrentNeverExceedsBurst(t *testing.T) {
	l := New(redistest.Start(t), 0.001, 10) // negligible refill
	var allowed atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, _, err := l.Allow(context.Background(), "a")
			if err != nil {
				t.Error(err)
			}
			if ok {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := allowed.Load(); got != 10 {
		t.Fatalf("allowed = %d, want 10", got)
	}
}

func TestClosedPortReturnsError(t *testing.T) {
	c := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 100 * time.Millisecond, MaxRetries: -1})
	defer c.Close()
	_, _, err := New(c, 10, 10).Allow(context.Background(), "a")
	if err == nil {
		t.Fatal("want error so callers fail open")
	}
}
