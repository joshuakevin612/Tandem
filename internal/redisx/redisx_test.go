package redisx

import (
	"context"
	"testing"
	"time"

	"taskq/internal/redisx/redistest"
)

func TestNewEmptyIsNil(t *testing.T) {
	c, err := New("")
	if c != nil || err != nil {
		t.Fatalf("got %v, %v", c, err)
	}
}

func TestNewBadURL(t *testing.T) {
	if _, err := New("http://nope"); err == nil {
		t.Fatal("want error")
	}
}

func TestStartPing(t *testing.T) {
	c := redistest.Start(t)
	if err := c.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
}

// A dead Redis must fail fast. With go-redis defaults (3 command retries x 5
// dial retries) a blackholed host stalled each call for ~6s.
func TestDeadRedisFailsFast(t *testing.T) {
	c, err := New("redis://10.255.255.1:6379/0") // non-routable: dials time out
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	start := time.Now()
	if err := c.Ping(context.Background()).Err(); err == nil {
		t.Fatal("want error")
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("dead redis took %v, want < 1s", d)
	}
}
