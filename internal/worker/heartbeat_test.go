package worker

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"taskq/internal/queue"
	"taskq/internal/store"
)

// startCfg runs a pool built from cfg (with test defaults filled in).
func startCfg(t *testing.T, cfg Config) (cancel func()) {
	t.Helper()
	if cfg.Concurrency == 0 {
		cfg.Concurrency = 1
	}
	if cfg.Lease == 0 {
		cfg.Lease = time.Minute
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = time.Millisecond
	}
	if cfg.ShutdownTimeout == 0 {
		cfg.ShutdownTimeout = 5 * time.Second
	}
	cfg.Logger = quiet
	p := NewPool(cfg)
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

func (f *fakeStore) addAttempt(jobType string, attempts int) {
	tok := uuid.New()
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queue = append(f.queue, store.Job{ID: uuid.New(), Type: jobType, Attempts: attempts, LeaseToken: &tok})
}

func sorted(ids []uuid.UUID) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = id.String()
	}
	slices.Sort(out)
	return out
}

func TestHeartbeatSendsAllInFlightTokens(t *testing.T) {
	f, reg := newFake(3, "x"), NewRegistry()
	var mu sync.Mutex
	var tokens []uuid.UUID
	started, release := make(chan struct{}, 3), make(chan struct{})
	reg.Register("x", func(_ context.Context, j store.Job) error {
		mu.Lock()
		tokens = append(tokens, *j.LeaseToken)
		mu.Unlock()
		started <- struct{}{}
		<-release
		return nil
	})
	startCfg(t, Config{Store: f, Registry: reg, Concurrency: 3, Heartbeat: 2 * time.Millisecond})
	for range 3 {
		recv(t, started)
	}
	mu.Lock()
	want := sorted(tokens)
	mu.Unlock()
	for {
		got := recv(t, f.extended)
		if len(got) == 3 {
			if !slices.Equal(sorted(got), want) {
				t.Fatalf("heartbeat tokens %v, want %v", sorted(got), want)
			}
			break
		}
	}
	close(release)
	for range 3 {
		recv(t, f.completed)
	}
}

func TestLostTokenCancelsOnlyThatHandler(t *testing.T) {
	f, reg := newFake(3, "x"), NewRegistry()
	var mu sync.Mutex
	byID := map[uuid.UUID]uuid.UUID{} // job id -> token
	var lostID uuid.UUID
	var lostToken uuid.UUID
	started, release := make(chan store.Job, 3), make(chan struct{})
	type outcome struct {
		id    uuid.UUID
		cause error
	}
	returned := make(chan outcome, 3)
	reg.Register("x", func(ctx context.Context, j store.Job) error {
		mu.Lock()
		byID[j.ID] = *j.LeaseToken
		mu.Unlock()
		started <- j
		select {
		case <-ctx.Done():
			returned <- outcome{j.ID, context.Cause(ctx)}
			return ctx.Err()
		case <-release:
			return nil
		}
	})
	startCfg(t, Config{Store: f, Registry: reg, Concurrency: 3, Heartbeat: 2 * time.Millisecond})
	var jobs []store.Job
	for range 3 {
		jobs = append(jobs, recv(t, started))
	}
	lostID, lostToken = jobs[1].ID, *jobs[1].LeaseToken
	f.setExtend(func(tokens []uuid.UUID) ([]uuid.UUID, error) {
		var alive []uuid.UUID
		for _, tok := range tokens {
			if tok != lostToken {
				alive = append(alive, tok)
			}
		}
		return alive, nil
	})

	got := recv(t, returned)
	if got.id != lostID || !errors.Is(got.cause, ErrLeaseLost) {
		t.Fatalf("cancelled %v cause %v, want %v / ErrLeaseLost", got.id, got.cause, lostID)
	}
	select {
	case extra := <-returned:
		t.Fatalf("second handler cancelled: %v", extra.id)
	case <-time.After(30 * time.Millisecond):
	}

	close(release)
	done := map[uuid.UUID]bool{}
	for range 2 {
		done[recv(t, f.completed)] = true
	}
	if done[lostID] || len(done) != 2 {
		t.Fatalf("completed %v; lost job must not be among them", done)
	}
	// Run to completion, then nothing else may have been reported for the lost job.
	time.Sleep(20 * time.Millisecond)
	if len(f.completed) != 0 || len(f.failed) != 0 {
		t.Fatalf("lost job was reported: completed=%d failed=%d", len(f.completed), len(f.failed))
	}
}

func TestSustainedHeartbeatFailureCancelsAll(t *testing.T) {
	f, reg := newFake(2, "x"), NewRegistry()
	f.setExtend(func([]uuid.UUID) ([]uuid.UUID, error) { return nil, errors.New("db down") })
	causes := make(chan error, 2)
	started := make(chan struct{}, 2)
	reg.Register("x", func(ctx context.Context, _ store.Job) error {
		started <- struct{}{}
		<-ctx.Done()
		causes <- context.Cause(ctx)
		return ctx.Err()
	})
	startCfg(t, Config{Store: f, Registry: reg, Concurrency: 2, Lease: 60 * time.Millisecond, Heartbeat: 5 * time.Millisecond})
	for range 2 {
		if c := recv(t, causes); !errors.Is(c, ErrLeaseUncertain) {
			t.Fatalf("cause = %v", c)
		}
	}
	time.Sleep(20 * time.Millisecond)
	if len(f.completed) != 0 || len(f.failed) != 0 {
		t.Fatal("cancelled-for-uncertainty job was reported")
	}
}

func TestTransientHeartbeatErrorsTolerated(t *testing.T) {
	f, reg := newFake(1, "x"), NewRegistry()
	var mu sync.Mutex
	calls := 0
	f.setExtend(func(tokens []uuid.UUID) ([]uuid.UUID, error) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls <= 3 {
			return nil, errors.New("blip")
		}
		return tokens, nil
	})
	started, release, result := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	reg.Register("x", func(ctx context.Context, _ store.Job) error {
		close(started)
		<-release
		result <- ctx.Err()
		return nil
	})
	startCfg(t, Config{Store: f, Registry: reg, Lease: time.Minute, Heartbeat: 2 * time.Millisecond})
	<-started
	for range 6 { // three failures, then successes
		recv(t, f.extended)
	}
	close(release)
	if err := recv(t, result); err != nil {
		t.Fatalf("handler cancelled by transient errors: %v", err)
	}
	recv(t, f.completed)
}

func TestHeartbeatRunsDuringDrainAndStopsAfterRun(t *testing.T) {
	f, reg := newFake(1, "x"), NewRegistry()
	started, release := make(chan struct{}), make(chan struct{})
	reg.Register("x", func(context.Context, store.Job) error {
		close(started)
		<-release
		return nil
	})
	stop := startCfg(t, Config{Store: f, Registry: reg, Heartbeat: 2 * time.Millisecond})
	<-started
	recv(t, f.extended)

	stopped := make(chan struct{})
	go func() { stop(); close(stopped) }()
	// Run is draining (handler still blocked); heartbeats must continue.
	for range 3 {
		recv(t, f.extended)
	}
	select {
	case <-stopped:
		t.Fatal("Run returned during drain")
	default:
	}
	close(release)
	<-stopped
	recv(t, f.completed)

	for len(f.extended) > 0 {
		<-f.extended
	}
	time.Sleep(30 * time.Millisecond) // 15 heartbeat periods
	if n := len(f.extended); n != 0 {
		t.Fatalf("%d heartbeats after Run returned", n)
	}
}

func TestAbandonedJobStopsHeartbeating(t *testing.T) {
	f, reg := newFake(1, "x"), NewRegistry()
	started := make(chan struct{})
	reg.Register("x", func(ctx context.Context, _ store.Job) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})
	stop := startCfg(t, Config{Store: f, Registry: reg, Heartbeat: 2 * time.Millisecond, ShutdownTimeout: 20 * time.Millisecond})
	<-started
	recv(t, f.extended)
	stop()
	for len(f.extended) > 0 {
		<-f.extended
	}
	time.Sleep(20 * time.Millisecond)
	if n := len(f.extended); n != 0 {
		t.Fatalf("%d heartbeats after abandon", n)
	}
	if len(f.failed) != 0 || len(f.completed) != 0 {
		t.Fatal("abandoned job reported")
	}
}

func TestRetryDelayRespectsBackoff(t *testing.T) {
	const n = 300
	b := queue.Backoff{Base: 10 * time.Millisecond, Max: time.Second}
	f, reg := newFake(0, "x"), NewRegistry()
	f.completed, f.failed = make(chan uuid.UUID, 1), make(chan failure, 2*n+8)
	reg.Register("x", func(context.Context, store.Job) error { return errors.New("nope") })
	attemptOf := map[uuid.UUID]int{}
	for _, a := range []int{1, 6, 50} {
		for range n {
			f.addAttempt("x", a)
		}
	}
	f.mu.Lock()
	for _, j := range f.queue {
		attemptOf[j.ID] = j.Attempts
	}
	f.mu.Unlock()
	f.failed = make(chan failure, 3*n)
	startCfg(t, Config{Store: f, Registry: reg, Concurrency: 8, Backoff: b})

	sum := map[int]time.Duration{}
	for range 3 * n {
		got := recv(t, f.failed)
		a := attemptOf[got.id]
		ceil := min(b.Max, b.Base<<(a-1))
		if a >= 8 {
			ceil = b.Max
		}
		if got.retryIn < 0 || got.retryIn > ceil {
			t.Fatalf("attempt %d: retryIn %v outside [0,%v]", a, got.retryIn, ceil)
		}
		sum[a] += got.retryIn
	}
	if !(sum[1] < sum[6] && sum[6] < sum[50]) {
		t.Fatalf("delay does not grow with attempt: sums %v", sum)
	}
}

func TestPermanentErrorIsNoRetry(t *testing.T) {
	var calls atomic.Int32
	f, reg := newFake(2, "x"), NewRegistry()
	reg.Register("x", func(_ context.Context, j store.Job) error {
		base := errors.New("bad payload")
		if calls.Add(1) == 1 {
			return Permanent(base)
		}
		return fmt.Errorf("ctx: %w", Permanent(base))
	})
	startCfg(t, Config{Store: f, Registry: reg})
	for range 2 {
		got := recv(t, f.failed)
		if got.retryIn != store.NoRetry {
			t.Fatalf("retryIn = %v, want NoRetry", got.retryIn)
		}
	}
}

func TestPermanentWrapping(t *testing.T) {
	base := errors.New("boom")
	p := Permanent(base)
	if !errors.Is(p, ErrPermanent) || !errors.Is(p, base) || !IsPermanent(fmt.Errorf("w: %w", p)) {
		t.Fatal("Permanent not detectable via errors.Is")
	}
	var pe *permanentError
	if !errors.As(p, &pe) || p.Error() != "boom" {
		t.Fatal("Permanent not detectable via errors.As / message changed")
	}
	if Permanent(nil) != nil || IsPermanent(base) {
		t.Fatal("nil / plain error mishandled")
	}
}

func TestPanicTimeoutAndUnknownAreRetryable(t *testing.T) {
	f, reg := newFake(0, "x"), NewRegistry()
	reg.Register("panic", func(context.Context, store.Job) error { panic("kaboom") })
	reg.Register("slow", func(ctx context.Context, _ store.Job) error { <-ctx.Done(); return ctx.Err() })
	for _, ty := range []string{"panic", "slow", "mystery"} {
		f.addAttempt(ty, 1)
	}
	startCfg(t, Config{Store: f, Registry: reg, Concurrency: 3, JobTimeout: 20 * time.Millisecond,
		Backoff: queue.Backoff{Base: time.Second, Max: time.Minute}})
	for range 3 {
		got := recv(t, f.failed)
		if got.retryIn < 0 || got.retryIn > time.Second {
			t.Fatalf("%q: retryIn = %v, want retryable within [0,1s]", got.msg, got.retryIn)
		}
	}
}

func TestTimeoutMessageAndIndependenceFromLease(t *testing.T) {
	f, reg := newFake(1, "slow"), NewRegistry()
	reg.Register("slow", func(ctx context.Context, _ store.Job) error { <-ctx.Done(); return ctx.Err() })
	// Lease is far shorter than JobTimeout: only JobTimeout may end the handler.
	startCfg(t, Config{Store: f, Registry: reg, Lease: 30 * time.Millisecond, Heartbeat: 5 * time.Millisecond,
		JobTimeout: 100 * time.Millisecond})
	start := time.Now()
	got := recv(t, f.failed)
	if time.Since(start) < 90*time.Millisecond {
		t.Fatalf("handler cut off after %v; deadline must follow JobTimeout, not Lease", time.Since(start))
	}
	if want := "job timed out after 100ms: context deadline exceeded"; got.msg != want {
		t.Fatalf("msg = %q, want %q", got.msg, want)
	}
}
