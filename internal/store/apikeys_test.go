package store

import (
	"context"
	"crypto/sha256"
	"errors"
	"strconv"
	"sync"
	"testing"

	"github.com/google/uuid"
)

func TestAPIKeyLifecycle(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	if _, err := s.pool.Exec(ctx, `TRUNCATE api_keys`); err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256([]byte("td_secret"))

	k, err := s.CreateAPIKey(ctx, "acme", "ci", "td_secre", h[:])
	if err != nil {
		t.Fatal(err)
	}
	if got, err := s.TenantForKeyHash(ctx, h[:]); err != nil || got != "acme" {
		t.Fatalf("got %q, %v", got, err)
	}
	other := sha256.Sum256([]byte("nope"))
	if _, err := s.TenantForKeyHash(ctx, other[:]); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("unknown: %v", err)
	}
	if _, err := s.CreateAPIKey(ctx, "acme", "dup", "td_secre", h[:]); err == nil {
		t.Fatal("duplicate hash should fail")
	}
	keys, err := s.ListAPIKeys(ctx, "acme")
	if err != nil || len(keys) != 1 || keys[0].ID != k.ID {
		t.Fatalf("list: %v %v", keys, err)
	}
	if err := s.RevokeAPIKey(ctx, k.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.TenantForKeyHash(ctx, h[:]); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("revoked: %v", err)
	}
	if err := s.RevokeAPIKey(ctx, k.ID); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("double revoke: %v", err)
	}
	if err := s.RevokeAPIKey(ctx, uuid.New()); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("missing revoke: %v", err)
	}
}

// Two API replicas starting together both run Migrate against a fresh
// database; both must succeed. Uses its own empty database so it does not
// disturb the shared one.
func TestMigrateConcurrent(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	if _, err := s.pool.Exec(ctx, `DROP DATABASE IF EXISTS migrate_race`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, `CREATE DATABASE migrate_race`); err != nil {
		t.Fatal(err)
	}
	cfg := s.pool.Config().ConnConfig
	dsn := "postgres://" + cfg.User + ":" + cfg.Password + "@" + cfg.Host + ":" + strconv.Itoa(int(cfg.Port)) + "/migrate_race?sslmode=disable"

	var wg sync.WaitGroup
	errs := make([]error, 2)
	start := make(chan struct{})
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs[i] = Migrate(dsn)
		}()
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("migrate %d: %v", i, err)
		}
	}
}
