package store

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func mkSchedule(t *testing.T, s *Store, tenant, name string, next time.Time) Schedule {
	t.Helper()
	sc, err := s.CreateSchedule(context.Background(), NewSchedule{
		TenantID: tenant, Name: name, Cron: "* * * * *", Type: "report",
		Payload: []byte(`{"a":1}`), Priority: 3, MaxAttempts: 7, Queue: "q1", NextFireAt: next,
	})
	if err != nil {
		t.Fatal(err)
	}
	return sc
}

func TestScheduleCRUD(t *testing.T) {
	s, ctx := newStore(t), context.Background()
	next := time.Now().Add(time.Hour).Truncate(time.Microsecond)
	sc := mkSchedule(t, s, "t1", "nightly", next)
	if !sc.Enabled || sc.Timezone != "UTC" || sc.Queue != "q1" || sc.MaxAttempts != 7 || sc.Priority != 3 ||
		sc.LastFireAt != nil || !sc.NextFireAt.Equal(next) || string(sc.Payload) != `{"a": 1}` {
		t.Fatalf("created: %+v", sc)
	}

	if _, err := s.CreateSchedule(ctx, NewSchedule{TenantID: "t1", Name: "nightly", Cron: "@daily", Type: "x", NextFireAt: next}); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate: %v", err)
	}
	// Same name under another tenant is fine.
	other := mkSchedule(t, s, "t2", "nightly", next)

	got, err := s.GetSchedule(ctx, "t1", sc.ID)
	if err != nil || got.ID != sc.ID {
		t.Fatalf("get: %v %+v", err, got)
	}
	// Tenant isolation.
	if _, err := s.GetSchedule(ctx, "t1", other.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-tenant get: %v", err)
	}
	if _, err := s.SetScheduleEnabled(ctx, "t1", other.ID, false, next); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-tenant enable: %v", err)
	}
	if err := s.DeleteSchedule(ctx, "t1", other.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-tenant delete: %v", err)
	}
	mkSchedule(t, s, "t1", "alpha", next)
	list, err := s.ListSchedules(ctx, "t1")
	if err != nil || len(list) != 2 || list[0].Name != "alpha" || list[1].Name != "nightly" {
		t.Fatalf("list: %v %+v", err, list)
	}
	if l, _ := s.ListSchedules(ctx, "nobody"); len(l) != 0 {
		t.Fatalf("list other tenant: %+v", l)
	}

	later := next.Add(time.Hour)
	upd, err := s.SetScheduleEnabled(ctx, "t1", sc.ID, false, later)
	if err != nil || upd.Enabled || !upd.NextFireAt.Equal(later) {
		t.Fatalf("disable: %v %+v", err, upd)
	}

	if err := s.DeleteSchedule(ctx, "t1", sc.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteSchedule(ctx, "t1", sc.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second delete: %v", err)
	}
}

func TestDueSchedules(t *testing.T) {
	s, ctx := newStore(t), context.Background()
	now := time.Now()
	old := mkSchedule(t, s, "t", "old", now.Add(-2*time.Hour))
	recent := mkSchedule(t, s, "t", "recent", now.Add(-time.Minute))
	mkSchedule(t, s, "t", "future", now.Add(time.Hour))
	off := mkSchedule(t, s, "t", "off", now.Add(-time.Hour))
	if _, err := s.SetScheduleEnabled(ctx, "t", off.ID, false, now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}

	due, err := s.DueSchedules(ctx, 10)
	if err != nil || len(due) != 2 || due[0].ID != old.ID || due[1].ID != recent.ID {
		t.Fatalf("due: %v %+v", err, due)
	}
	if due, _ = s.DueSchedules(ctx, 1); len(due) != 1 || due[0].ID != old.ID {
		t.Fatalf("limit: %+v", due)
	}
}

func jobCountForSchedule(t *testing.T, s *Store, id uuid.UUID) int {
	t.Helper()
	var n int
	if err := s.pool.QueryRow(context.Background(), `SELECT count(*) FROM jobs WHERE schedule_id=$1`, id).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestFireSchedule(t *testing.T) {
	s, ctx := newStore(t), context.Background()
	due := time.Now().Add(-time.Minute).Truncate(time.Second)
	sc := mkSchedule(t, s, "t", "s", due)
	sc, _ = s.GetSchedule(ctx, "t", sc.ID)

	next := time.Now().Add(time.Hour)
	fired, err := s.FireSchedule(ctx, sc, next)
	if err != nil || !fired {
		t.Fatalf("fire: %v %v", fired, err)
	}
	got, _ := s.GetSchedule(ctx, "t", sc.ID)
	if !got.NextFireAt.Equal(next.Truncate(time.Microsecond)) || got.LastFireAt == nil || !got.LastFireAt.Equal(due) {
		t.Fatalf("after fire: %+v", got)
	}
	var j Job
	jobs, err := s.ListJobs(ctx, ListFilter{TenantID: "t"})
	if err != nil || len(jobs) != 1 {
		t.Fatalf("jobs: %v %d", err, len(jobs))
	}
	j = jobs[0]
	wantKey := "sched:" + sc.ID.String() + ":" + strconv.FormatInt(due.Unix(), 10)
	if j.Type != "report" || j.Queue != "q1" || j.Priority != 3 || j.MaxAttempts != 7 || string(j.Payload) != `{"a": 1}` ||
		j.IdempotencyKey == nil || *j.IdempotencyKey != wantKey || j.Status != "queued" {
		t.Fatalf("job: %+v", j)
	}
	if jobCountForSchedule(t, s, sc.ID) != 1 {
		t.Fatal("schedule_id not set")
	}

	// Stale copy: CAS fails, nothing happens.
	if fired, err := s.FireSchedule(ctx, sc, next.Add(time.Hour)); err != nil || fired {
		t.Fatalf("stale fire: %v %v", fired, err)
	}
	// next must move forward.
	if _, err := s.FireSchedule(ctx, got, got.NextFireAt); err == nil {
		t.Fatal("non-advancing next accepted")
	}

	// Re-armed to the same instant: CAS succeeds but the idempotency key blocks
	// a second job; the schedule still advances.
	if _, err := s.pool.Exec(ctx, `UPDATE schedules SET next_fire_at=$2 WHERE id=$1`, sc.ID, due); err != nil {
		t.Fatal(err)
	}
	if fired, err := s.FireSchedule(ctx, sc, next.Add(2*time.Hour)); err != nil || fired {
		t.Fatalf("re-armed fire: %v %v", fired, err)
	}
	if jobCountForSchedule(t, s, sc.ID) != 1 {
		t.Fatal("duplicate job created")
	}
	got, _ = s.GetSchedule(ctx, "t", sc.ID)
	if !got.NextFireAt.Equal(next.Add(2 * time.Hour).Truncate(time.Microsecond)) {
		t.Fatalf("not advanced: %+v", got)
	}

	// Deleting the schedule keeps the job, detached.
	if err := s.DeleteSchedule(ctx, "t", sc.ID); err != nil {
		t.Fatal(err)
	}
	if after, err := s.GetJob(ctx, j.ID); err != nil || after.Status != "queued" {
		t.Fatalf("job after schedule delete: %v %+v", err, after)
	}
}

func itoa(n int64) string { return time.Unix(n, 0).UTC().Format("") + fmtInt(n) }

func fmtInt(n int64) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for ; n > 0; n /= 10 {
		b = append([]byte{byte('0' + n%10)}, b...)
	}
	return string(b)
}

func TestFireScheduleConcurrent(t *testing.T) {
	s, ctx := newStore(t), context.Background()
	sc := mkSchedule(t, s, "t", "race", time.Now().Add(-time.Minute))
	sc, _ = s.GetSchedule(ctx, "t", sc.ID)
	next := time.Now().Add(time.Hour)

	const n = 20
	var wg sync.WaitGroup
	var mu sync.Mutex
	fired := 0
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := s.FireSchedule(ctx, sc, next)
			if err != nil {
				t.Error(err)
				return
			}
			if ok {
				mu.Lock()
				fired++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if fired != 1 {
		t.Fatalf("fired = %d, want 1", fired)
	}
	if c := jobCountForSchedule(t, s, sc.ID); c != 1 {
		t.Fatalf("job rows = %d, want 1", c)
	}
	got, _ := s.GetSchedule(ctx, "t", sc.ID)
	if !got.NextFireAt.Equal(next.Truncate(time.Microsecond)) {
		t.Fatalf("next_fire_at = %v, want %v", got.NextFireAt, next)
	}
}

func TestFireDisabledSchedule(t *testing.T) {
	s, ctx := newStore(t), context.Background()
	due := time.Now().Add(-time.Minute)
	sc := mkSchedule(t, s, "t", "off", due)
	off, err := s.SetScheduleEnabled(ctx, "t", sc.ID, false, due)
	if err != nil {
		t.Fatal(err)
	}
	fired, err := s.FireSchedule(ctx, off, time.Now().Add(time.Hour))
	if err != nil || fired {
		t.Fatalf("disabled fire: %v %v", fired, err)
	}
	if jobCountForSchedule(t, s, sc.ID) != 0 {
		t.Fatal("job created for disabled schedule")
	}
	got, _ := s.GetSchedule(ctx, "t", sc.ID)
	if !got.NextFireAt.Equal(off.NextFireAt) || got.LastFireAt != nil {
		t.Fatalf("disabled schedule advanced: %+v", got)
	}
}

func TestCancelJobQueued(t *testing.T) {
	s, ctx := newStore(t), context.Background()
	j := mustCreate(t, s, NewJob{TenantID: "t", Type: "x"})
	got, err := s.CancelJob(ctx, "t", j.ID)
	if err != nil || got.Status != "cancelled" || got.FinishedAt == nil || got.LeaseToken != nil {
		t.Fatalf("cancel queued: %v %+v", err, got)
	}
	if jobs, _ := s.LeaseJobs(ctx, "default", "w", 5, time.Minute); len(jobs) != 0 {
		t.Fatal("cancelled job leased")
	}
	if _, err := s.CancelJob(ctx, "t", j.ID); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("double cancel: %v", err)
	}
}

func TestCancelJobRunning(t *testing.T) {
	s, ctx := newStore(t), context.Background()
	mustCreate(t, s, NewJob{TenantID: "t", Type: "x"})
	j := leaseOne(t, s, "w1", time.Minute)
	tok := *j.LeaseToken

	got, err := s.CancelJob(ctx, "t", j.ID)
	if err != nil || got.Status != "cancelled" || got.FinishedAt == nil || got.LeaseToken != nil || got.LockedUntil != nil {
		t.Fatalf("cancel running: %v %+v", err, got)
	}
	var outcome string
	var finished *time.Time
	if err := s.pool.QueryRow(ctx, `SELECT outcome, finished_at FROM job_attempts WHERE job_id=$1`, j.ID).Scan(&outcome, &finished); err != nil || outcome != "cancelled" || finished == nil {
		t.Fatalf("attempt: %q %v err %v", outcome, finished, err)
	}

	// The old owner is fenced out everywhere.
	if err := s.CompleteJob(ctx, j.ID, tok); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("complete: %v", err)
	}
	if err := s.FailJob(ctx, j.ID, tok, "boom", time.Second); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("fail: %v", err)
	}
	if owned, err := s.ExtendLeases(ctx, []uuid.UUID{tok}, time.Minute); err != nil || len(owned) != 0 {
		t.Fatalf("extend: %v %v", err, owned)
	}
	// Even with a stale lease timestamp the reaper ignores it.
	if _, err := s.pool.Exec(ctx, `UPDATE jobs SET locked_until = now() - interval '1 hour' WHERE id=$1`, j.ID); err != nil {
		t.Fatal(err)
	}
	if rq, dead, err := s.ReapExpired(ctx, 10); err != nil || rq != 0 || dead != 0 {
		t.Fatalf("reap: %d %d %v", rq, dead, err)
	}
	after, _ := s.GetJob(ctx, j.ID)
	if after.Status != "cancelled" {
		t.Fatalf("status = %s", after.Status)
	}
}

func TestCancelJobTerminalAndTenant(t *testing.T) {
	s, ctx := newStore(t), context.Background()
	mustCreate(t, s, NewJob{TenantID: "t", Type: "x"})
	j := leaseOne(t, s, "w1", time.Minute)
	if _, err := s.CancelJob(ctx, "other", j.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("wrong tenant: %v", err)
	}
	if _, err := s.CancelJob(ctx, "t", uuid.New()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown id: %v", err)
	}
	if err := s.CompleteJob(ctx, j.ID, *j.LeaseToken); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CancelJob(ctx, "t", j.ID); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("terminal: %v", err)
	}
	if got, _ := s.GetJob(ctx, j.ID); got.Status != "succeeded" {
		t.Fatalf("status changed: %s", got.Status)
	}
}

func TestLeaderLock(t *testing.T) {
	s, ctx := newStore(t), context.Background()
	for _, key := range []int64{42, -7, 1 << 40} {
		a, ok, err := s.TryLeaderLock(ctx, key)
		if err != nil || !ok {
			t.Fatalf("first lock %d: %v %v", key, ok, err)
		}
		if err := a.Check(ctx); err != nil {
			t.Fatalf("check %d: %v", key, err)
		}
		if b, ok, err := s.TryLeaderLock(ctx, key); err != nil || ok {
			if b != nil {
				b.Release()
			}
			t.Fatalf("second lock %d while held: %v %v", key, ok, err)
		}
		// A different key is independent.
		if c, ok, err := s.TryLeaderLock(ctx, key+1); err != nil || !ok {
			t.Fatalf("other key: %v %v", ok, err)
		} else {
			c.Release()
		}
		a.Release()
		a.Release() // safe twice
		if err := a.Check(ctx); err == nil {
			t.Fatal("check after release should fail")
		}
		b, ok, err := s.TryLeaderLock(ctx, key)
		if err != nil || !ok {
			t.Fatalf("lock after release %d: %v %v", key, ok, err)
		}
		b.Release()
	}
}

func TestLeaderLockBackendKilled(t *testing.T) {
	s, ctx := newStore(t), context.Background()
	const key = 777
	a, ok, err := s.TryLeaderLock(ctx, key)
	if err != nil || !ok {
		t.Fatalf("lock: %v %v", ok, err)
	}
	defer a.Release()
	var pid int32
	if err := a.conn.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
		t.Fatal(err)
	}
	var terminated bool
	if err := s.pool.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, pid).Scan(&terminated); err != nil || !terminated {
		t.Fatalf("terminate: %v %v", terminated, err)
	}
	if err := a.Check(ctx); err == nil {
		t.Fatal("Check should fail after backend killed")
	}
	// Postgres frees the lock when the backend exits; allow it a moment.
	deadline := time.Now().Add(5 * time.Second)
	for {
		b, ok, err := s.TryLeaderLock(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			b.Release()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("lock not freed after backend killed")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestListJobsCursorSameCreatedAt(t *testing.T) {
	s, ctx := newStore(t), context.Background()
	const total = 25
	for i := 0; i < total; i++ {
		mustCreate(t, s, NewJob{TenantID: "t", Type: "x"})
	}
	if _, err := s.pool.Exec(ctx, `UPDATE jobs SET created_at = '2025-01-01T00:00:00Z'`); err != nil {
		t.Fatal(err)
	}
	seen := map[uuid.UUID]int{}
	f := ListFilter{TenantID: "t", Limit: 10}
	pages := 0
	for {
		page, err := s.ListJobs(ctx, f)
		if err != nil {
			t.Fatal(err)
		}
		for _, j := range page {
			seen[j.ID]++
		}
		pages++
		if len(page) < f.Limit {
			break
		}
		last := page[len(page)-1]
		f.Before, f.BeforeID = &last.CreatedAt, &last.ID
		if pages > 10 {
			t.Fatal("paging does not terminate")
		}
	}
	if len(seen) != total || pages != 3 {
		t.Fatalf("saw %d distinct over %d pages, want %d over 3", len(seen), pages, total)
	}
	for id, c := range seen {
		if c != 1 {
			t.Fatalf("job %s seen %d times", id, c)
		}
	}
}
