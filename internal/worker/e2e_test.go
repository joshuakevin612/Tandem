package worker

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"taskq/internal/store"
)

func TestEndToEndExactlyOnce(t *testing.T) {
	const (
		total  = 10000
		pools  = 10
		perCon = 10
	)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pg, err := postgres.Run(ctx, "postgres:17",
		postgres.WithDatabase("taskq"), postgres.WithUsername("taskq"), postgres.WithPassword("taskq"), postgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(pg) })
	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Migrate(dsn); err != nil {
		t.Fatal(err)
	}
	raw, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()

	ids := make([]uuid.UUID, total)
	rows := make([][]any, total)
	for i := range rows {
		ids[i] = uuid.New()
		rows[i] = []any{ids[i], "count", []byte(`{}`)}
	}
	if _, err := raw.CopyFrom(ctx, pgx.Identifier{"jobs"}, []string{"id", "type", "payload"}, pgx.CopyFromRows(rows)); err != nil {
		t.Fatalf("bulk insert: %v", err)
	}

	st, err := store.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	var mu sync.Mutex
	runs := make(map[uuid.UUID]int, total)
	var executed atomic.Int64
	allDone := make(chan struct{})
	reg := NewRegistry()
	reg.Register("count", func(_ context.Context, j store.Job) error {
		mu.Lock()
		runs[j.ID]++
		mu.Unlock()
		if executed.Add(1) == total {
			close(allDone)
		}
		return nil
	})

	runCtx, stopPools := context.WithCancel(ctx)
	var wg sync.WaitGroup
	start := time.Now()
	for range pools {
		p := NewPool(Config{
			Store: st, Registry: reg, Concurrency: perCon, Lease: time.Minute,
			PollInterval: 20 * time.Millisecond, Logger: quiet,
		})
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := p.Run(runCtx); err != nil {
				t.Errorf("Run: %v", err)
			}
		}()
	}
	select {
	case <-allDone:
	case <-ctx.Done():
		t.Fatalf("timed out: %d/%d executed", executed.Load(), total)
	}
	stopPools()
	wg.Wait() // drains in-flight result writes
	elapsed := time.Since(start)
	t.Logf("%d jobs in %v: %.0f jobs/sec", total, elapsed, float64(total)/elapsed.Seconds())

	for _, id := range ids {
		if n := runs[id]; n != 1 {
			t.Fatalf("job %s ran %d times", id, n)
		}
	}
	var succeeded, attempts1 int
	err = raw.QueryRow(ctx, `SELECT count(*) FILTER (WHERE status='succeeded'), count(*) FILTER (WHERE attempts=1) FROM jobs`).Scan(&succeeded, &attempts1)
	if err != nil {
		t.Fatal(err)
	}
	if succeeded != total || attempts1 != total {
		t.Fatalf("succeeded=%d attempts=1:%d, want %d each", succeeded, attempts1, total)
	}
	var attemptRows int
	if err := raw.QueryRow(ctx, `SELECT count(*) FROM job_attempts WHERE outcome='succeeded'`).Scan(&attemptRows); err != nil {
		t.Fatal(err)
	}
	if attemptRows != total {
		t.Fatalf("job_attempts succeeded = %d, want %d", attemptRows, total)
	}
}
