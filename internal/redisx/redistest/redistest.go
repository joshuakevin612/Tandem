// Package redistest starts a throwaway Redis for tests.
package redistest

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

// Start runs redis:7-alpine and returns a client connected to it.
func Start(t *testing.T) *redis.Client {
	t.Helper()
	ctx := context.Background()
	c, err := tcredis.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatalf("start redis: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(c) })
	url, err := c.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}
	opt, err := redis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	cl := redis.NewClient(opt)
	t.Cleanup(func() { _ = cl.Close() })
	return cl
}
