package scheduler

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeLockStore is one lock shared by any number of electors.
type fakeLockStore struct {
	mu     sync.Mutex
	holder *fakeLock
	tries  int
}

type fakeLock struct {
	s        *fakeLockStore
	mu       sync.Mutex
	broken   bool
	released bool
	releases chan struct{} // closed on first Release
	once     sync.Once
}

func newFakeLockStore() *fakeLockStore { return &fakeLockStore{} }

func (s *fakeLockStore) TryLock(context.Context, int64) (Lock, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tries++
	if s.holder != nil {
		return nil, false, nil
	}
	l := &fakeLock{s: s, releases: make(chan struct{})}
	s.holder = l
	return l, true, nil
}

func (l *fakeLock) Check(context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.broken || l.released {
		return errors.New("lock lost")
	}
	return nil
}

func (l *fakeLock) breakIt() { l.mu.Lock(); l.broken = true; l.mu.Unlock() }

func (l *fakeLock) isReleased() bool { l.mu.Lock(); defer l.mu.Unlock(); return l.released }

func (l *fakeLock) Release() {
	l.once.Do(func() {
		l.mu.Lock()
		l.released = true
		l.mu.Unlock()
		l.s.mu.Lock()
		if l.s.holder == l {
			l.s.holder = nil
		}
		l.s.mu.Unlock()
		close(l.releases)
	})
}

func (s *fakeLockStore) current() *fakeLock {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.holder
}

func newElector(s LockStore) *Elector {
	return &Elector{Store: s, Key: 1, RetryInterval: time.Millisecond, CheckInterval: time.Millisecond, Logger: quiet}
}

func recv[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		panic("unreachable")
	}
}

func runElector(e *Elector, ctx context.Context, lead func(context.Context) error) <-chan error {
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx, lead) }()
	return done
}

func TestElectorAcquiresAndRunsLead(t *testing.T) {
	s := newFakeLockStore()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{}, 1)
	done := runElector(newElector(s), ctx, func(ctx context.Context) error {
		started <- struct{}{}
		<-ctx.Done()
		return nil
	})
	recv(t, started, "lead start")
	if s.current() == nil {
		t.Fatal("lock not held while leading")
	}
	cancel()
	if err := recv(t, done, "Run to return"); err != nil {
		t.Fatal(err)
	}
}

func TestElectorCheckFailureCancelsLeadAndRecampaigns(t *testing.T) {
	s := newFakeLockStore()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	starts := make(chan context.Context, 4)
	stopped := make(chan struct{}, 4)
	done := runElector(newElector(s), ctx, func(ctx context.Context) error {
		starts <- ctx
		<-ctx.Done()
		stopped <- struct{}{}
		return ctx.Err()
	})
	first := recv(t, starts, "first lead")
	l1 := s.current()
	l1.breakIt()
	recv(t, stopped, "lead to be cancelled")
	if first.Err() == nil {
		t.Fatal("lead ctx not cancelled")
	}
	recv(t, l1.releases, "release of lost lock")
	recv(t, starts, "second lead after re-campaign")
	if s.current() == nil || s.current() == l1 {
		t.Fatal("expected a fresh lock after re-campaign")
	}
	cancel()
	recv(t, done, "Run to return")
}

func TestElectorCancelReleases(t *testing.T) {
	s := newFakeLockStore()
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{}, 1)
	done := runElector(newElector(s), ctx, func(ctx context.Context) error {
		started <- struct{}{}
		<-ctx.Done()
		return nil
	})
	recv(t, started, "lead")
	l := s.current()
	cancel()
	recv(t, done, "Run to return")
	select {
	case <-l.releases:
	default:
		t.Fatal("lock not released when Run returned")
	}
	if s.current() != nil {
		t.Fatal("lock still held")
	}
}

func TestElectorLeadReturningReleasesAndRecampaigns(t *testing.T) {
	s := newFakeLockStore()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	starts := make(chan struct{}, 4)
	var n atomic.Int32
	done := runElector(newElector(s), ctx, func(ctx context.Context) error {
		starts <- struct{}{}
		if n.Add(1) == 1 {
			return errors.New("boom") // first run fails on its own
		}
		<-ctx.Done()
		return nil
	})
	recv(t, starts, "first lead")
	recv(t, starts, "second lead")
	cancel()
	recv(t, done, "Run to return")
}

func TestElectorFollowerTakesOver(t *testing.T) {
	s := newFakeLockStore()
	ctxA, cancelA := context.WithCancel(context.Background())
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	aLead, bLead := make(chan struct{}, 4), make(chan struct{}, 4)
	mk := func(c chan struct{}) func(context.Context) error {
		return func(ctx context.Context) error { c <- struct{}{}; <-ctx.Done(); return nil }
	}
	doneA := runElector(newElector(s), ctxA, mk(aLead))
	recv(t, aLead, "A leads")
	doneB := runElector(newElector(s), ctxB, mk(bLead))
	// B must be campaigning (and failing) while A leads.
	deadline := time.Now().Add(5 * time.Second)
	for {
		s.mu.Lock()
		tries := s.tries
		s.mu.Unlock()
		if tries >= 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("follower never campaigned")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-bLead:
		t.Fatal("follower led while leader alive")
	default:
	}
	cancelA()
	recv(t, doneA, "A to stop")
	recv(t, bLead, "B takes over")
	cancelB()
	recv(t, doneB, "B to stop")
}

func TestElectorsNeverOverlap(t *testing.T) {
	s := newFakeLockStore()
	ctx, cancel := context.WithCancel(context.Background())
	var running, maxRunning, starts atomic.Int32
	lead := func(ctx context.Context) error {
		n := running.Add(1)
		for {
			m := maxRunning.Load()
			if n <= m || maxRunning.CompareAndSwap(m, n) {
				break
			}
		}
		starts.Add(1)
		<-ctx.Done()
		running.Add(-1)
		return nil
	}
	d1 := runElector(newElector(s), ctx, lead)
	d2 := runElector(newElector(s), ctx, lead)
	// Repeatedly depose whoever leads; each deposition forces a hand-over.
	for i := 0; i < 30; i++ {
		deadline := time.Now().Add(5 * time.Second)
		var l *fakeLock
		for l = s.current(); l == nil || l.isReleased(); l = s.current() {
			if time.Now().After(deadline) {
				t.Fatal("no leader emerged")
			}
			time.Sleep(100 * time.Microsecond)
		}
		before := starts.Load()
		l.breakIt()
		for starts.Load() == before {
			if time.Now().After(deadline) {
				t.Fatal("no new leader after deposition")
			}
			time.Sleep(100 * time.Microsecond)
		}
	}
	cancel()
	recv(t, d1, "elector 1")
	recv(t, d2, "elector 2")
	if m := maxRunning.Load(); m != 1 {
		t.Fatalf("max concurrent lead functions = %d, want 1", m)
	}
}
