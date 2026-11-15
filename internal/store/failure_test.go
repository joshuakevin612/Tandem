package store

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

// leaseOne leases a single job from the default queue.
func leaseOne(t *testing.T, s *Store, worker string, lease time.Duration) Job {
	t.Helper()
	jobs, err := s.LeaseJobs(context.Background(), "default", worker, 1, lease)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("lease: %v, %d jobs", err, len(jobs))
	}
	return jobs[0]
}

// expire makes a running job's lease already expired.
func expire(t *testing.T, s *Store, id uuid.UUID) {
	t.Helper()
	if _, err := s.pool.Exec(context.Background(), `UPDATE jobs SET locked_until = now() - interval '1 second' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
}

func mustCreate(t *testing.T, s *Store, n NewJob) Job {
	t.Helper()
	j, err := s.CreateJob(context.Background(), n)
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func TestFailJobRetry(t *testing.T) {
	s, ctx := newStore(t), context.Background()
	mustCreate(t, s, NewJob{Type: "x"})
	j := leaseOne(t, s, "w1", time.Minute)
	before := time.Now()
	if err := s.FailJob(ctx, j.ID, *j.LeaseToken, "boom", time.Hour); err != nil {
		t.Fatal(err)
	}
	got, _ := s.GetJob(ctx, j.ID)
	if got.Status != "queued" || got.Attempts != 1 || got.FinishedAt != nil || got.LeaseToken != nil || got.LockedUntil != nil ||
		got.LastError == nil || *got.LastError != "boom" {
		t.Fatalf("after retryable fail: %+v", got)
	}
	if d := got.RunAt.Sub(before); d < 59*time.Minute || d > 61*time.Minute {
		t.Fatalf("run_at is %v ahead, want ~1h", d)
	}
	if jobs, _ := s.LeaseJobs(ctx, "default", "w2", 1, time.Minute); len(jobs) != 0 {
		t.Fatal("job leasable before retry time")
	}
	var outcome, msg string
	if err := s.pool.QueryRow(ctx, `SELECT outcome, error FROM job_attempts WHERE job_id=$1`, j.ID).Scan(&outcome, &msg); err != nil || outcome != "failed" || msg != "boom" {
		t.Fatalf("attempt %q %q err %v", outcome, msg, err)
	}
	// Once due, it is leasable again and attempts keep counting.
	if _, err := s.pool.Exec(ctx, `UPDATE jobs SET run_at = now() WHERE id=$1`, j.ID); err != nil {
		t.Fatal(err)
	}
	if again := leaseOne(t, s, "w2", time.Minute); again.ID != j.ID || again.Attempts != 2 {
		t.Fatalf("re-lease: %+v", again)
	}
}

func TestFailJobExhaustion(t *testing.T) {
	s, ctx := newStore(t), context.Background()
	mustCreate(t, s, NewJob{Type: "x", MaxAttempts: 3})
	for i := 1; i <= 3; i++ {
		j := leaseOne(t, s, "w1", time.Minute)
		if j.Attempts != i {
			t.Fatalf("attempt = %d, want %d", j.Attempts, i)
		}
		if err := s.FailJob(ctx, j.ID, *j.LeaseToken, fmt.Sprint("err", i), 0); err != nil {
			t.Fatal(err)
		}
		got, _ := s.GetJob(ctx, j.ID)
		if i < 3 && (got.Status != "queued" || got.FinishedAt != nil) {
			t.Fatalf("attempt %d: %+v", i, got)
		}
		if i == 3 && (got.Status != "dead" || got.FinishedAt == nil || got.LastError == nil || *got.LastError != "err3") {
			t.Fatalf("attempt %d: %+v", i, got)
		}
	}
	if jobs, _ := s.LeaseJobs(ctx, "default", "w1", 1, time.Minute); len(jobs) != 0 {
		t.Fatal("dead job was leased")
	}
}

func TestFailJobNoRetry(t *testing.T) {
	s, ctx := newStore(t), context.Background()
	mustCreate(t, s, NewJob{Type: "x"})
	j := leaseOne(t, s, "w1", time.Minute)
	if err := s.FailJob(ctx, j.ID, uuid.New(), "x", time.Second); err != ErrLeaseLost {
		t.Fatalf("wrong token: %v", err)
	}
	if err := s.FailJob(ctx, j.ID, *j.LeaseToken, "permanent", NoRetry); err != nil {
		t.Fatal(err)
	}
	got, _ := s.GetJob(ctx, j.ID)
	if got.Status != "dead" || got.Attempts != 1 || got.FinishedAt == nil || *got.LastError != "permanent" {
		t.Fatalf("after NoRetry: %+v", got)
	}
}

func TestExtendLeases(t *testing.T) {
	s, ctx := newStore(t), context.Background()
	if alive, err := s.ExtendLeases(ctx, nil, time.Minute); err != nil || len(alive) != 0 {
		t.Fatalf("empty: %v %v", alive, err)
	}
	for i := 0; i < 4; i++ {
		mustCreate(t, s, NewJob{Type: "x"})
	}
	jobs, err := s.LeaseJobs(ctx, "default", "w1", 4, time.Minute)
	if err != nil || len(jobs) != 4 {
		t.Fatal(err, len(jobs))
	}
	if err := s.CompleteJob(ctx, jobs[0].ID, *jobs[0].LeaseToken); err != nil {
		t.Fatal(err)
	}
	expire(t, s, jobs[1].ID)
	if n, _, err := s.ReapExpired(ctx, 10); err != nil || n != 1 {
		t.Fatalf("reap: %d %v", n, err)
	}
	tokens := []uuid.UUID{*jobs[0].LeaseToken, *jobs[1].LeaseToken, *jobs[2].LeaseToken, *jobs[3].LeaseToken, uuid.New()}
	alive, err := s.ExtendLeases(ctx, tokens, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(alive) != 2 {
		t.Fatalf("alive = %v", alive)
	}
	got := map[uuid.UUID]bool{alive[0]: true, alive[1]: true}
	if !got[*jobs[2].LeaseToken] || !got[*jobs[3].LeaseToken] {
		t.Fatalf("alive = %v", alive)
	}
	j, _ := s.GetJob(ctx, jobs[2].ID)
	if d := time.Until(*j.LockedUntil); d < 59*time.Minute {
		t.Fatalf("lease extended only to %v from now", d)
	}
}

func TestReapExpired(t *testing.T) {
	s, ctx := newStore(t), context.Background()
	mustCreate(t, s, NewJob{Type: "x"})
	old := leaseOne(t, s, "w1", time.Minute)

	// Unexpired: untouched.
	if r, d, err := s.ReapExpired(ctx, 10); err != nil || r != 0 || d != 0 {
		t.Fatalf("reap unexpired: %d %d %v", r, d, err)
	}
	if got, _ := s.GetJob(ctx, old.ID); got.Status != "running" {
		t.Fatalf("unexpired job touched: %+v", got)
	}

	expire(t, s, old.ID)
	if r, d, err := s.ReapExpired(ctx, 10); err != nil || r != 1 || d != 0 {
		t.Fatalf("reap: %d %d %v", r, d, err)
	}
	got, _ := s.GetJob(ctx, old.ID)
	if got.Status != "queued" || got.LeaseToken != nil || got.LockedUntil != nil || got.FinishedAt != nil || got.Attempts != 1 {
		t.Fatalf("after reap: %+v", got)
	}
	var outcome string
	if err := s.pool.QueryRow(ctx, `SELECT outcome FROM job_attempts WHERE job_id=$1 AND attempt=1`, old.ID).Scan(&outcome); err != nil || outcome != "lease_expired" {
		t.Fatalf("attempt outcome %q err %v", outcome, err)
	}

	// Another worker takes it with a new token; the zombie is fenced out.
	fresh := leaseOne(t, s, "w2", time.Minute)
	if fresh.ID != old.ID || *fresh.LeaseToken == *old.LeaseToken || fresh.Attempts != 2 {
		t.Fatalf("re-lease: %+v", fresh)
	}
	if err := s.CompleteJob(ctx, old.ID, *old.LeaseToken); err != ErrLeaseLost {
		t.Fatalf("zombie complete: %v", err)
	}
	if err := s.FailJob(ctx, old.ID, *old.LeaseToken, "late", time.Second); err != ErrLeaseLost {
		t.Fatalf("zombie fail: %v", err)
	}
	if alive, err := s.ExtendLeases(ctx, []uuid.UUID{*old.LeaseToken}, time.Minute); err != nil || len(alive) != 0 {
		t.Fatalf("zombie extend: %v %v", alive, err)
	}
	if err := s.CompleteJob(ctx, fresh.ID, *fresh.LeaseToken); err != nil {
		t.Fatalf("new owner complete: %v", err)
	}
}

func TestReapExpiredDead(t *testing.T) {
	s, ctx := newStore(t), context.Background()
	mustCreate(t, s, NewJob{Type: "x", MaxAttempts: 1})
	j := leaseOne(t, s, "w1", time.Minute)
	expire(t, s, j.ID)
	if r, d, err := s.ReapExpired(ctx, 10); err != nil || r != 0 || d != 1 {
		t.Fatalf("reap: %d %d %v", r, d, err)
	}
	got, _ := s.GetJob(ctx, j.ID)
	if got.Status != "dead" || got.FinishedAt == nil || got.LastError == nil || *got.LastError != "lease expired" || got.LeaseToken != nil {
		t.Fatalf("after reap: %+v", got)
	}
}

func TestReapExpiredConcurrent(t *testing.T) {
	s, ctx := newStore(t), context.Background()
	const total, reapers = 1000, 4
	// Half have attempts left, half are exhausted, so both branches run.
	if _, err := s.pool.Exec(ctx, `INSERT INTO jobs (id, type, status, attempts, max_attempts, locked_until, lease_token)
		SELECT gen_random_uuid(), 'x', 'running', 1, CASE WHEN i % 2 = 0 THEN 1 ELSE 5 END, now() - interval '1 minute', gen_random_uuid()
		FROM generate_series(1, $1) i`, total); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var sumQ, sumD int
	var wg sync.WaitGroup
	errs := make(chan error, reapers)
	for i := 0; i < reapers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				r, d, err := s.ReapExpired(ctx, 50)
				if err != nil {
					errs <- err
					return
				}
				mu.Lock()
				sumQ, sumD = sumQ+r, sumD+d
				mu.Unlock()
				if r+d == 0 {
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if sumQ+sumD != total || sumQ != total/2 || sumD != total/2 {
		t.Fatalf("requeued %d dead %d, want %d each", sumQ, sumD, total/2)
	}
	var n int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE status='running'`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("still running: %d %v", n, err)
	}
}

func TestListJobs(t *testing.T) {
	s, ctx := newStore(t), context.Background()
	base := time.Now().Add(-time.Hour).Truncate(time.Microsecond)
	var ids []uuid.UUID // oldest first, tenant "a", queue alternates
	for i := 0; i < 5; i++ {
		q := "q1"
		if i%2 == 1 {
			q = "q2"
		}
		j := mustCreate(t, s, NewJob{TenantID: "a", Queue: q, Type: "x"})
		if _, err := s.pool.Exec(ctx, `UPDATE jobs SET created_at=$2 WHERE id=$1`, j.ID, base.Add(time.Duration(i)*time.Minute)); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, j.ID)
	}
	mustCreate(t, s, NewJob{TenantID: "b", Type: "x"})
	if _, err := s.pool.Exec(ctx, `UPDATE jobs SET status='dead' WHERE id=$1`, ids[1]); err != nil {
		t.Fatal(err)
	}

	all, err := s.ListJobs(ctx, ListFilter{TenantID: "a"})
	if err != nil || len(all) != 5 {
		t.Fatalf("all: %v %d", err, len(all))
	}
	for i, j := range all {
		if j.ID != ids[4-i] {
			t.Fatalf("order wrong at %d", i)
		}
	}
	if got, _ := s.ListJobs(ctx, ListFilter{TenantID: "a", Status: "dead"}); len(got) != 1 || got[0].ID != ids[1] {
		t.Fatalf("status filter: %+v", got)
	}
	if got, _ := s.ListJobs(ctx, ListFilter{TenantID: "a", Queue: "q1"}); len(got) != 3 {
		t.Fatalf("queue filter: %d", len(got))
	}
	if got, _ := s.ListJobs(ctx, ListFilter{TenantID: "a", Status: "queued", Queue: "q2"}); len(got) != 1 || got[0].ID != ids[3] {
		t.Fatalf("combined filter: %+v", got)
	}
	if got, _ := s.ListJobs(ctx, ListFilter{TenantID: "nobody"}); len(got) != 0 {
		t.Fatalf("tenant leak: %d", len(got))
	}
	// Cursor paging, 2 at a time.
	var paged []uuid.UUID
	var before *time.Time
	for {
		page, err := s.ListJobs(ctx, ListFilter{TenantID: "a", Limit: 2, Before: before})
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		for _, j := range page {
			paged = append(paged, j.ID)
		}
		before = &page[len(page)-1].CreatedAt
	}
	if len(paged) != 5 {
		t.Fatalf("paged %d jobs", len(paged))
	}
	for i, id := range paged {
		if id != ids[4-i] {
			t.Fatalf("page order wrong at %d", i)
		}
	}
	if got, _ := s.ListJobs(ctx, ListFilter{TenantID: "a", Limit: 1000}); len(got) != 5 {
		t.Fatal("max limit clamp broke listing")
	}
}

func TestRequeueJob(t *testing.T) {
	s, ctx := newStore(t), context.Background()
	mustCreate(t, s, NewJob{TenantID: "a", Type: "x", MaxAttempts: 1})
	jobs, err := s.LeaseJobs(ctx, "default", "w1", 1, time.Minute)
	if err != nil || len(jobs) != 1 {
		t.Fatal(err, len(jobs))
	}
	j := jobs[0]
	if _, err := s.RequeueJob(ctx, "a", j.ID); err != ErrInvalidState {
		t.Fatalf("running job: %v", err)
	}
	if err := s.FailJob(ctx, j.ID, *j.LeaseToken, "boom", time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RequeueJob(ctx, "other", j.ID); err != ErrNotFound {
		t.Fatalf("wrong tenant: %v", err)
	}
	if _, err := s.RequeueJob(ctx, "a", uuid.New()); err != ErrNotFound {
		t.Fatalf("unknown id: %v", err)
	}
	got, err := s.RequeueJob(ctx, "a", j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "queued" || got.Attempts != 0 || got.FinishedAt != nil || got.LastError == nil || *got.LastError != "boom" || got.RunAt.After(time.Now()) {
		t.Fatalf("after requeue: %+v", got)
	}
	if _, err := s.RequeueJob(ctx, "a", j.ID); err != ErrInvalidState {
		t.Fatalf("queued job: %v", err)
	}
	if again := leaseOne(t, s, "w1", time.Minute); again.ID != j.ID || again.Attempts != 1 {
		t.Fatalf("lease after requeue: %+v", again)
	}
}
