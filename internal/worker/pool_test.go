package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"taskq/internal/store"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

type failure struct {
	id      uuid.UUID
	msg     string
	retryIn time.Duration
}

// fakeStore hands out queued jobs and reports completions/failures on channels.
type fakeStore struct {
	mu          sync.Mutex
	queue       []store.Job
	leaseErrs   int // number of initial LeaseJobs calls that fail
	leaseCalls  int
	maxLimit    int
	completeErr error
	extendFn    func(tokens []uuid.UUID) ([]uuid.UUID, error) // nil: all alive
	extended    chan []uuid.UUID                              // every ExtendLeases call

	completed chan uuid.UUID
	failed    chan failure
}

func newFake(n int, jobType string) *fakeStore {
	f := &fakeStore{completed: make(chan uuid.UUID, n+8), failed: make(chan failure, n+8), extended: make(chan []uuid.UUID, 4096)}
	for range n {
		f.add(jobType)
	}
	return f
}

func (f *fakeStore) add(jobType string) {
	tok := uuid.New()
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queue = append(f.queue, store.Job{ID: uuid.New(), Type: jobType, LeaseToken: &tok})
}

func (f *fakeStore) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.leaseCalls
}

func (f *fakeStore) LeaseJobs(_ context.Context, _, _ string, limit int, _ time.Duration) ([]store.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.leaseCalls++
	f.maxLimit = max(f.maxLimit, limit)
	if f.leaseErrs > 0 {
		f.leaseErrs--
		return nil, errors.New("db down")
	}
	n := min(limit, len(f.queue))
	out := f.queue[:n:n]
	f.queue = f.queue[n:]
	return out, nil
}

func (f *fakeStore) CompleteJob(_ context.Context, id, _ uuid.UUID) error {
	f.completed <- id
	return f.completeErr
}

func (f *fakeStore) FailJob(_ context.Context, id, _ uuid.UUID, msg string, retryIn time.Duration) error {
	f.failed <- failure{id, msg, retryIn}
	return nil
}

// start runs a pool in the background; the returned func cancels it and waits.
func start(t *testing.T, st Store, reg *Registry, conc int) (cancel func()) {
	t.Helper()
	p := NewPool(Config{
		Store: st, Registry: reg, Concurrency: conc, Lease: time.Minute,
		PollInterval: time.Millisecond, ShutdownTimeout: 5 * time.Second, Logger: quiet,
	})
	ctx, cancelCtx := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancelCtx()
			if err := <-done; err != nil {
				t.Errorf("Run: %v", err)
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

func recv[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for result")
		panic("unreachable")
	}
}

func TestHandlerErrorFailsJob(t *testing.T) {
	f, reg := newFake(1, "x"), NewRegistry()
	reg.Register("x", func(context.Context, store.Job) error { return errors.New("boom") })
	start(t, f, reg, 2)
	if got := recv(t, f.failed); got.msg != "boom" {
		t.Fatalf("msg = %q", got.msg)
	}
}

func TestSuccessCompletes(t *testing.T) {
	f, reg := newFake(3, "x"), NewRegistry()
	reg.Register("x", func(context.Context, store.Job) error { return nil })
	start(t, f, reg, 2)
	for range 3 {
		recv(t, f.completed)
	}
}

func TestUnknownType(t *testing.T) {
	f := newFake(1, "mystery")
	start(t, f, NewRegistry(), 1)
	if got := recv(t, f.failed); got.msg != "no handler registered for type mystery" {
		t.Fatalf("msg = %q", got.msg)
	}
}

func TestPanicRecovered(t *testing.T) {
	f, reg := newFake(1, "panic"), NewRegistry()
	reg.Register("panic", func(context.Context, store.Job) error { panic("kaboom") })
	reg.Register("ok", func(context.Context, store.Job) error { return nil })
	start(t, f, reg, 1)
	if got := recv(t, f.failed); got.msg != "panic: kaboom" {
		t.Fatalf("msg = %q", got.msg)
	}
	f.add("ok") // the pool must still be alive and leasing
	recv(t, f.completed)
}

func TestConcurrencyCap(t *testing.T) {
	const conc, total = 5, 200
	f, reg := newFake(total, "x"), NewRegistry()
	var inflight, peak atomic.Int32
	gate := make(chan struct{})
	var once sync.Once
	reg.Register("x", func(context.Context, store.Job) error {
		n := inflight.Add(1)
		defer inflight.Add(-1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		if n == conc {
			once.Do(func() { close(gate) }) // saturation reached
		}
		<-gate
		return nil
	})
	start(t, f, reg, conc)
	for range total {
		recv(t, f.completed)
	}
	if got := peak.Load(); got != conc {
		t.Fatalf("peak in-flight = %d, want %d", got, conc)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.maxLimit > conc {
		t.Fatalf("leased with limit %d > concurrency %d", f.maxLimit, conc)
	}
}

func TestLeaseLostTolerated(t *testing.T) {
	f, reg := newFake(2, "x"), NewRegistry()
	f.completeErr = store.ErrLeaseLost
	reg.Register("x", func(context.Context, store.Job) error { return nil })
	start(t, f, reg, 1)
	recv(t, f.completed)
	recv(t, f.completed) // second job still processed
}

func TestLeaseErrorRetried(t *testing.T) {
	f, reg := newFake(1, "x"), NewRegistry()
	f.leaseErrs = 2
	reg.Register("x", func(context.Context, store.Job) error { return nil })
	start(t, f, reg, 1)
	recv(t, f.completed)
}

func TestGracefulShutdown(t *testing.T) {
	f, reg := newFake(1, "x"), NewRegistry()
	started, release := make(chan struct{}), make(chan struct{})
	reg.Register("x", func(context.Context, store.Job) error {
		close(started)
		<-release
		return nil
	})
	stop := start(t, f, reg, 1)
	<-started

	stopped := make(chan struct{})
	go func() { stop(); close(stopped) }()

	select {
	case <-stopped:
		t.Fatal("Run returned while a handler was still in flight")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	<-stopped

	// The in-flight job's result was written despite the cancelled run context.
	recv(t, f.completed)

	// No more leasing after shutdown.
	before := f.calls()
	f.add("x")
	time.Sleep(20 * time.Millisecond)
	if after := f.calls(); after != before {
		t.Fatalf("leased %d times after shutdown", after-before)
	}
}

func TestShutdownTimeoutCancelsHandlers(t *testing.T) {
	f, reg := newFake(1, "x"), NewRegistry()
	started := make(chan struct{})
	reg.Register("x", func(ctx context.Context, _ store.Job) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})
	p := NewPool(Config{Store: f, Registry: reg, Concurrency: 1, Lease: time.Minute,
		PollInterval: time.Millisecond, ShutdownTimeout: 50 * time.Millisecond, Logger: quiet})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	<-started
	cancel()
	if err := recv(t, done); err != nil {
		t.Fatal(err)
	}
	// An abandoned job must not be reported; its lease expiry handles it.
	if len(f.failed) != 0 || len(f.completed) != 0 {
		t.Fatal("abandoned job was reported")
	}
}

func (f *fakeStore) ExtendLeases(_ context.Context, tokens []uuid.UUID, _ time.Duration) ([]uuid.UUID, error) {
	select {
	case f.extended <- append([]uuid.UUID(nil), tokens...):
	default:
	}
	f.mu.Lock()
	fn := f.extendFn
	f.mu.Unlock()
	if fn == nil {
		return tokens, nil
	}
	return fn(tokens)
}

func (f *fakeStore) setExtend(fn func([]uuid.UUID) ([]uuid.UUID, error)) {
	f.mu.Lock()
	f.extendFn = fn
	f.mu.Unlock()
}

// With a huge PollInterval, only a Wake can get a job picked up promptly.
func TestWakeCutsPollSleepShort(t *testing.T) {
	fs := newFake(0, "t")
	reg := NewRegistry()
	reg.Register("t", func(context.Context, store.Job) error { return nil })
	wake := make(chan struct{}, 1)
	p := NewPool(Config{
		Store: fs, Registry: reg, Concurrency: 1, Lease: time.Minute,
		PollInterval: time.Hour, ShutdownTimeout: 5 * time.Second, Logger: quiet, Wake: wake,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	defer func() { cancel(); <-done }()

	// Wait until the loop has polled the empty queue and is sleeping.
	for fs.calls() == 0 {
		runtime.Gosched()
	}
	fs.add("t")
	wake <- struct{}{}
	recv(t, fs.completed)
}

// A nil Wake must leave the polling path untouched.
func TestNilWakeStillPolls(t *testing.T) {
	fs := newFake(0, "t")
	reg := NewRegistry()
	reg.Register("t", func(context.Context, store.Job) error { return nil })
	start(t, fs, reg, 1) // Wake nil, PollInterval 1ms
	fs.add("t")
	recv(t, fs.completed)
}
