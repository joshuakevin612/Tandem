package scheduler

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"taskq/internal/store"
)

type fire struct {
	id   uuid.UUID
	next time.Time
}

type fakeCronStore struct {
	mu       sync.Mutex
	due      []store.Schedule // returned until fired
	dueErrs  []error
	fires    []fire
	dueCalls int
	notify   chan struct{} // one token per DueSchedules call
}

func newFakeCronStore(due ...store.Schedule) *fakeCronStore {
	return &fakeCronStore{due: due, notify: make(chan struct{}, 1000)}
}

func (f *fakeCronStore) DueSchedules(_ context.Context, limit int) ([]store.Schedule, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dueCalls++
	defer func() { f.notify <- struct{}{} }()
	if len(f.dueErrs) > 0 {
		err := f.dueErrs[0]
		f.dueErrs = f.dueErrs[1:]
		return nil, err
	}
	out := append([]store.Schedule(nil), f.due...)
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (f *fakeCronStore) FireSchedule(_ context.Context, s store.Schedule, next time.Time) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fires = append(f.fires, fire{s.ID, next})
	for i, d := range f.due {
		if d.ID == s.ID {
			f.due = append(f.due[:i], f.due[i+1:]...)
			break
		}
	}
	return true, nil
}

func (f *fakeCronStore) snapshot() []fire {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fire(nil), f.fires...)
}

func sched(cron string, nextFire time.Time) store.Schedule {
	return store.Schedule{ID: uuid.New(), Name: cron, Cron: cron, Timezone: "UTC", NextFireAt: nextFire, Enabled: true}
}

// runCron starts a runner; waitCalls blocks until DueSchedules has been
// called n times in total.
func runCron(t *testing.T, f *fakeCronStore, interval time.Duration, now time.Time, batch int) (waitCalls func(n int), stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	c := &CronRunner{Store: f, Interval: interval, BatchSize: batch, Logger: quiet, Now: func() time.Time { return now }}
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	seen := 0
	return func(n int) {
			for seen < n {
				recv(t, f.notify, "DueSchedules call")
				seen++
			}
		}, func() {
			cancel()
			if err := recv(t, done, "runner to stop"); err != nil {
				t.Fatal(err)
			}
		}
}

func TestCronFiresDueSchedulesWithNext(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 30, 500, time.UTC)
	a := sched("*/5 * * * *", time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	b := sched("@hourly", time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	f := newFakeCronStore(a, b)
	wait, stop := runCron(t, f, time.Millisecond, now, 10)
	wait(2)
	stop()
	got := f.snapshot()
	if len(got) != 2 {
		t.Fatalf("fires = %v", got)
	}
	want := map[uuid.UUID]time.Time{
		a.ID: time.Date(2026, 1, 1, 12, 5, 0, 0, time.UTC),
		b.ID: time.Date(2026, 1, 1, 13, 0, 0, 0, time.UTC),
	}
	for _, g := range got {
		if !g.next.Equal(want[g.id]) {
			t.Errorf("schedule %v next = %v, want %v", g.id, g.next, want[g.id])
		}
	}
}

func TestCronLongOutageCollapsesToOneFire(t *testing.T) {
	now := time.Date(2026, 1, 3, 12, 0, 0, 0, time.UTC)
	s := sched("* * * * *", now.Add(-48*time.Hour)) // two days of missed minutes
	// The fake keeps returning the schedule until fired, so a backfill would
	// show up as many fires; a collapse shows exactly one.
	f := newFakeCronStore(s)
	wait, stop := runCron(t, f, time.Millisecond, now, 10)
	wait(4)
	stop()
	got := f.snapshot()
	if len(got) != 1 {
		t.Fatalf("fires = %d, want exactly 1 catch-up", len(got))
	}
	if !got[0].next.After(now) || got[0].next.Sub(now) > time.Minute {
		t.Fatalf("next = %v, want the first minute after %v", got[0].next, now)
	}
}

func TestCronClockBehindStillAdvances(t *testing.T) {
	// This host's clock is behind the DB's: next must still follow NextFireAt.
	s := sched("@every 1s", time.Date(2026, 1, 1, 12, 0, 10, 0, time.UTC))
	f := newFakeCronStore(s)
	wait, stop := runCron(t, f, time.Millisecond, s.NextFireAt.Add(-time.Minute), 10)
	wait(1)
	stop()
	if g := f.snapshot(); len(g) != 1 || !g[0].next.After(s.NextFireAt) {
		t.Fatalf("fires = %v", g)
	}
}

func TestCronBadScheduleSkippedOthersFire(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	past := now.Add(-time.Hour)
	bad := sched("not a cron", past)
	badTZ := sched("@hourly", past)
	badTZ.Timezone = "Mars/Olympus"
	never := sched("0 0 30 2 *", past) // Feb 30: never fires
	good1, good2 := sched("@hourly", past), sched("@daily", past)
	f := newFakeCronStore(bad, good1, badTZ, never, good2)
	wait, stop := runCron(t, f, time.Millisecond, now, 10)
	wait(3)
	stop()
	got := f.snapshot()
	if len(got) != 2 {
		t.Fatalf("fires = %v, want only the two good schedules", got)
	}
	ids := map[uuid.UUID]bool{got[0].id: true, got[1].id: true}
	if !ids[good1.ID] || !ids[good2.ID] {
		t.Fatalf("wrong schedules fired: %v", got)
	}
}

func TestCronStoreErrorsDoNotStopLoop(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	s := sched("@hourly", now.Add(-time.Hour))
	f := newFakeCronStore(s)
	f.dueErrs = []error{errors.New("db down"), errors.New("db still down")}
	wait, stop := runCron(t, f, time.Millisecond, now, 10)
	wait(4)
	stop()
	if len(f.snapshot()) != 1 {
		t.Fatal("did not fire after store errors cleared")
	}
}

func TestCronDrainsFullBatches(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	var due []store.Schedule
	for i := 0; i < 5; i++ {
		due = append(due, sched("@hourly", now.Add(-time.Hour)))
	}
	f := newFakeCronStore(due...)
	// Batch 2 with an hour-long interval: only draining can fire all 5, in
	// polls of 2, 2, 1.
	wait, stop := runCron(t, f, time.Hour, now, 2)
	wait(3)
	stop()
	if n := len(f.snapshot()); n != 5 {
		t.Fatalf("fires = %d, want 5 via draining", n)
	}
}

func TestCronUnfireableFullBatchDoesNotSpin(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	past := now.Add(-time.Hour)
	f := newFakeCronStore(sched("garbage", past), sched("garbage", past))
	wait, stop := runCron(t, f, time.Hour, now, 2)
	wait(1)
	time.Sleep(50 * time.Millisecond) // a spin would rack up polls here
	stop()
	f.mu.Lock()
	calls := f.dueCalls
	f.mu.Unlock()
	if calls != 1 {
		t.Fatalf("DueSchedules calls = %d, want 1", calls)
	}
}
