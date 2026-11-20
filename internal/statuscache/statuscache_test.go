package statuscache

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"tandem/internal/redisx/redistest"
	"tandem/internal/store"
)

func sample(status string) store.Job {
	now := time.Now().UTC().Truncate(time.Microsecond)
	tok, wid, ik, le := uuid.New(), "w1", "k", "boom"
	return store.Job{
		ID: uuid.New(), TenantID: "acme", Queue: "q", Type: "t",
		Payload: json.RawMessage(`{"a":1}`), Status: status, Priority: 5,
		RunAt: now, Attempts: 2, MaxAttempts: 3, LockedUntil: &now,
		LeaseToken: &tok, WorkerID: &wid, IdempotencyKey: &ik, LastError: &le,
		CreatedAt: now, UpdatedAt: now, FinishedAt: &now,
	}
}

func TestNonTerminalNotCached(t *testing.T) {
	rdb := redistest.Start(t)
	c := New(rdb, time.Minute, nil)
	ctx := context.Background()
	for _, s := range []string{"queued", "running", "failed"} {
		j := sample(s)
		c.Set(ctx, j)
		if _, ok := c.Get(ctx, j.ID); ok {
			t.Fatalf("%s was cached", s)
		}
		if n, _ := rdb.Exists(ctx, key(j.ID)).Result(); n != 0 {
			t.Fatalf("%s key exists", s)
		}
	}
}

func TestTerminalRoundTripTTLAndDel(t *testing.T) {
	rdb := redistest.Start(t)
	c := New(rdb, 5*time.Second, nil)
	ctx := context.Background()
	for _, s := range []string{"succeeded", "dead", "cancelled"} {
		j := sample(s)
		c.Set(ctx, j)
		got, ok := c.Get(ctx, j.ID)
		if !ok || !reflect.DeepEqual(got, j) {
			t.Fatalf("%s: ok=%v\n got %+v\nwant %+v", s, ok, got, j)
		}
		ttl, err := rdb.PTTL(ctx, key(j.ID)).Result()
		if err != nil || ttl <= 0 || ttl > 5*time.Second {
			t.Fatalf("ttl = %v err=%v", ttl, err)
		}
		c.Del(ctx, j.ID)
		if _, ok := c.Get(ctx, j.ID); ok {
			t.Fatal("still cached after Del")
		}
	}
}

func TestClosedPortIsMiss(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 100 * time.Millisecond, MaxRetries: -1})
	defer rdb.Close()
	c := New(rdb, time.Minute, nil)
	ctx := context.Background()
	j := sample("succeeded")
	c.Set(ctx, j)
	c.Del(ctx, j.ID)
	if _, ok := c.Get(ctx, j.ID); ok {
		t.Fatal("want miss")
	}
}
