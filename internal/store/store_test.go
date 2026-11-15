package store

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

var testDSN string

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	ctx := context.Background()
	pg, err := postgres.Run(ctx, "postgres:17",
		postgres.WithDatabase("taskq"), postgres.WithUsername("taskq"), postgres.WithPassword("taskq"), postgres.BasicWaitStrategies(),
	)
	if err != nil {
		log.Printf("start postgres: %v", err)
		return 1
	}
	defer func() { _ = testcontainers.TerminateContainer(pg) }()
	testDSN, err = pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		log.Printf("dsn: %v", err)
		return 1
	}
	if err := Migrate(testDSN); err != nil {
		log.Printf("migrate: %v", err)
		return 1
	}
	return m.Run()
}

// newStore returns a Store on an empty jobs table.
func newStore(t *testing.T) *Store {
	t.Helper()
	ctx := context.Background()
	s, err := Open(ctx, testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if _, err := s.pool.Exec(ctx, `TRUNCATE jobs, schedules CASCADE`); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestMigrateIdempotent(t *testing.T) {
	if err := Migrate(testDSN); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
}

func TestCreateGet(t *testing.T) {
	s, ctx := newStore(t), context.Background()
	if err := s.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	j, err := s.CreateJob(ctx, NewJob{Type: "email", Payload: []byte(`{"to":"a"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if j.Queue != "default" || j.TenantID != "default" || j.MaxAttempts != 5 || j.Status != "queued" {
		t.Fatalf("defaults not applied: %+v", j)
	}
	got, err := s.GetJob(ctx, j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != j.ID || got.Type != "email" || string(got.Payload) != `{"to": "a"}` {
		t.Fatalf("round trip mismatch: %+v", got)
	}
	if _, err := s.GetJob(ctx, uuid.New()); err != ErrNotFound {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestLeaseOrdering(t *testing.T) {
	s, ctx := newStore(t), context.Background()
	mk := func(typ string, prio int16, runAt *time.Time) {
		if _, err := s.CreateJob(ctx, NewJob{Type: typ, Priority: prio, RunAt: runAt}); err != nil {
			t.Fatal(err)
		}
	}
	past, future := time.Now().Add(-time.Hour), time.Now().Add(time.Hour)
	mk("low", 0, &past)
	mk("high", 10, &past)
	mk("mid", 5, &past)
	mk("future", 100, &future)

	jobs, err := s.LeaseJobs(ctx, "default", "w1", 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var order []string
	for _, j := range jobs {
		order = append(order, j.Type)
		if j.Status != "running" || j.Attempts != 1 || j.LeaseToken == nil || j.LockedUntil == nil || *j.WorkerID != "w1" {
			t.Fatalf("bad leased job: %+v", j)
		}
	}
	if fmt.Sprint(order) != "[high mid low]" {
		t.Fatalf("order = %v", order)
	}
	var n int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM job_attempts WHERE finished_at IS NULL`).Scan(&n); err != nil || n != 3 {
		t.Fatalf("open attempts = %d, err %v", n, err)
	}
	if other, _ := s.LeaseJobs(ctx, "other", "w1", 10, time.Minute); len(other) != 0 {
		t.Fatal("leased from wrong queue")
	}
}

func TestLeaseContention(t *testing.T) {
	s, ctx := newStore(t), context.Background()
	const total, workers = 2000, 20
	if _, err := s.pool.Exec(ctx, `INSERT INTO jobs (id, type) SELECT gen_random_uuid(), 'x' FROM generate_series(1, $1)`, total); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	seen := map[uuid.UUID]int{}
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				jobs, err := s.LeaseJobs(ctx, "default", fmt.Sprintf("w%d", w), 10, time.Minute)
				if err != nil {
					errs <- err
					return
				}
				if len(jobs) == 0 {
					return
				}
				mu.Lock()
				for _, j := range jobs {
					seen[j.ID]++
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if len(seen) != total {
		t.Fatalf("leased %d distinct jobs, want %d", len(seen), total)
	}
	for id, c := range seen {
		if c != 1 {
			t.Fatalf("job %s leased %d times", id, c)
		}
	}
	var n int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM job_attempts`).Scan(&n); err != nil || n != total {
		t.Fatalf("attempt rows = %d, err %v", n, err)
	}
}

func TestFencing(t *testing.T) {
	s, ctx := newStore(t), context.Background()
	if _, err := s.CreateJob(ctx, NewJob{Type: "x"}); err != nil {
		t.Fatal(err)
	}
	jobs, err := s.LeaseJobs(ctx, "default", "w1", 1, time.Minute)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("lease: %v %d", err, len(jobs))
	}
	j, bad := jobs[0], uuid.New()

	if err := s.CompleteJob(ctx, j.ID, bad); err != ErrLeaseLost {
		t.Fatalf("complete wrong token: %v", err)
	}
	if err := s.FailJob(ctx, j.ID, bad, "boom", time.Second); err != ErrLeaseLost {
		t.Fatalf("fail wrong token: %v", err)
	}
	got, _ := s.GetJob(ctx, j.ID)
	if got.Status != "running" || got.LeaseToken == nil || *got.LeaseToken != *j.LeaseToken || got.LastError != nil {
		t.Fatalf("job modified by fenced call: %+v", got)
	}

	if err := s.CompleteJob(ctx, j.ID, *j.LeaseToken); err != nil {
		t.Fatal(err)
	}
	got, _ = s.GetJob(ctx, j.ID)
	if got.Status != "succeeded" || got.FinishedAt == nil || got.LeaseToken != nil || got.LockedUntil != nil {
		t.Fatalf("after complete: %+v", got)
	}
	var outcome string
	if err := s.pool.QueryRow(ctx, `SELECT outcome FROM job_attempts WHERE job_id=$1`, j.ID).Scan(&outcome); err != nil || outcome != "succeeded" {
		t.Fatalf("attempt outcome %q err %v", outcome, err)
	}
	// Token was cleared, so a second completion is also rejected.
	if err := s.CompleteJob(ctx, j.ID, *j.LeaseToken); err != ErrLeaseLost {
		t.Fatalf("double complete: %v", err)
	}
}

func TestIdempotency(t *testing.T) {
	s, ctx := newStore(t), context.Background()
	key := "k1"
	a, err := s.CreateJob(ctx, NewJob{Type: "x", IdempotencyKey: &key})
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.CreateJob(ctx, NewJob{Type: "x", IdempotencyKey: &key})
	if err != nil || b.ID != a.ID {
		t.Fatalf("second create: %v id %s want %s", err, b.ID, a.ID)
	}

	key2 := "race"
	ids := make(chan uuid.UUID, 20)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			j, err := s.CreateJob(ctx, NewJob{Type: "x", IdempotencyKey: &key2})
			if err != nil {
				t.Error(err)
				return
			}
			ids <- j.ID
		}()
	}
	wg.Wait()
	close(ids)
	distinct := map[uuid.UUID]bool{}
	for id := range ids {
		distinct[id] = true
	}
	if len(distinct) != 1 {
		t.Fatalf("got %d distinct ids", len(distinct))
	}
	var n int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE idempotency_key=$1`, key2).Scan(&n); err != nil || n != 1 {
		t.Fatalf("rows = %d err %v", n, err)
	}
}
