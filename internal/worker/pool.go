package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"runtime/debug"
	"sync"
	"time"

	"github.com/google/uuid"

	"taskq/internal/queue"
	"taskq/internal/store"
)

// Store is the subset of store.Store the pool uses.
type Store interface {
	LeaseJobs(ctx context.Context, queue, workerID string, limit int, lease time.Duration) ([]store.Job, error)
	CompleteJob(ctx context.Context, id, leaseToken uuid.UUID) error
	FailJob(ctx context.Context, id, leaseToken uuid.UUID, errMsg string, retryIn time.Duration) error
	ExtendLeases(ctx context.Context, tokens []uuid.UUID, lease time.Duration) ([]uuid.UUID, error)
}

type Config struct {
	Store           Store
	Registry        *Registry
	Queues          []string
	Concurrency     int
	Lease           time.Duration
	WorkerID        string        // default hostname-pid-shortuuid
	PollInterval    time.Duration // default 500ms
	ShutdownTimeout time.Duration // default 20s
	JobTimeout      time.Duration // per-attempt handler deadline; default 15m
	Backoff         queue.Backoff // retry delays; default 1s base, 5m max
	Heartbeat       time.Duration // heartbeat period; default Lease/3
	Logger          *slog.Logger
}

const (
	reportTimeout = 10 * time.Second
	killGrace     = 2 * time.Second // wait for handlers to notice a forced cancel
	maxBackoff    = 5 * time.Second
)

type Pool struct {
	Config
	sem      chan struct{} // one token per in-flight job
	released chan struct{} // wakes the lease loop when a slot frees up

	mu       sync.Mutex            // guards inflight and the flights in it
	inflight map[uuid.UUID]*flight // lease token -> running job

}

func NewPool(cfg Config) *Pool {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if len(cfg.Queues) == 0 {
		cfg.Queues = []string{"default"}
	}
	if cfg.WorkerID == "" {
		host, _ := os.Hostname()
		cfg.WorkerID = fmt.Sprintf("%s-%d-%s", host, os.Getpid(), uuid.NewString()[:8])
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 500 * time.Millisecond
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 20 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Lease <= 0 {
		cfg.Lease = 30 * time.Second
	}
	if cfg.JobTimeout <= 0 {
		cfg.JobTimeout = 15 * time.Minute
	}
	if cfg.Backoff == (queue.Backoff{}) {
		cfg.Backoff = queue.Backoff{Base: time.Second, Max: 5 * time.Minute}
	}
	if cfg.Heartbeat <= 0 {
		cfg.Heartbeat = cfg.Lease / 3
	}
	cfg.Logger = cfg.Logger.With("worker_id", cfg.WorkerID)
	return &Pool{Config: cfg, sem: make(chan struct{}, cfg.Concurrency), released: make(chan struct{}, 1), inflight: map[uuid.UUID]*flight{}}
}

// flight is one in-flight job. Fields after token are guarded by Pool.mu.
type flight struct {
	ctx       context.Context
	cancel    context.CancelCauseFunc
	token     uuid.UUID
	confirmed time.Time // last moment the lease was known to be ours
	lost      bool      // ownership lost: handler cancelled, result must not be reported
}

// Run leases and executes jobs until ctx is cancelled, then drains in-flight
// handlers for up to ShutdownTimeout. Heartbeats run for the whole of Run,
// including the drain, and stop when it returns.
func (p *Pool) Run(ctx context.Context) error {
	// Handlers derive from jobCtx, not ctx, so cancelling ctx lets them finish.
	jobCtx, killJobs := context.WithCancel(context.WithoutCancel(ctx))
	defer killJobs()
	var wg sync.WaitGroup

	hbCtx, stopHB := context.WithCancel(context.WithoutCancel(ctx))
	hbDone := make(chan struct{})
	go func() { defer close(hbDone); p.heartbeatLoop(hbCtx) }()
	defer func() { stopHB(); <-hbDone }()

	p.leaseLoop(ctx, jobCtx, &wg)

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	t := time.NewTimer(p.ShutdownTimeout)
	defer t.Stop()
	select {
	case <-done:
	case <-t.C:
		p.Logger.Warn("shutdown timeout, cancelling in-flight jobs", "in_flight", len(p.sem))
		killJobs()
		select {
		case <-done:
		case <-time.After(killGrace):
		}
	}
	return nil
}

func (p *Pool) leaseLoop(ctx, jobCtx context.Context, wg *sync.WaitGroup) {
	var next, idle, failures int
	for ctx.Err() == nil {
		free := cap(p.sem) - len(p.sem) // only this goroutine acquires, so free can only grow
		if free == 0 {
			select {
			case <-p.released:
			case <-ctx.Done():
			}
			continue
		}
		queue := p.Queues[next%len(p.Queues)]
		next++

		// The lease clock starts server-side during this call, so measure from before it.
		leasedAt := time.Now()
		jobs, err := p.Store.LeaseJobs(ctx, queue, p.WorkerID, free, p.Lease)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			failures++
			d := backoff(failures)
			p.Logger.Error("lease failed", "queue", queue, "err", err, "retry_in", d)
			sleep(ctx, d)
			continue
		}
		failures = 0

		for _, j := range jobs {
			p.sem <- struct{}{}
			// Registered before the goroutine starts so the next heartbeat covers it.
			f := p.track(jobCtx, j, leasedAt)
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() {
					<-p.sem
					select {
					case p.released <- struct{}{}:
					default:
					}
				}()
				p.execute(f, j)
			}()
		}

		// A non-empty batch loops again immediately; sleep only once every
		// queue has come up empty in a row.
		if len(jobs) > 0 {
			idle = 0
		} else if idle++; idle >= len(p.Queues) {
			idle = 0
			sleep(ctx, jitter(p.PollInterval))
		}
	}
}

// track registers a leased job for heartbeating. A job without a token cannot
// be heartbeated or reported; execute logs and drops it.
func (p *Pool) track(parent context.Context, j store.Job, leasedAt time.Time) *flight {
	ctx, cancel := context.WithCancelCause(parent)
	f := &flight{ctx: ctx, cancel: cancel, confirmed: leasedAt}
	if j.LeaseToken == nil {
		return f
	}
	f.token = *j.LeaseToken
	p.mu.Lock()
	p.inflight[f.token] = f
	p.mu.Unlock()
	return f
}

func (p *Pool) untrack(f *flight) {
	p.mu.Lock()
	if p.inflight[f.token] == f {
		delete(p.inflight, f.token)
	}
	p.mu.Unlock()
	f.cancel(nil)
}

// markLost flags a still-tracked job as no longer ours and cancels its handler.
func (p *Pool) markLost(f *flight, cause error) {
	p.mu.Lock()
	if p.inflight[f.token] != f || f.lost {
		p.mu.Unlock()
		return
	}
	f.lost = true
	p.mu.Unlock()
	f.cancel(cause)
}

func (p *Pool) isLost(f *flight) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return f.lost
}

func (p *Pool) heartbeatLoop(ctx context.Context) {
	t := time.NewTicker(p.Heartbeat)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.heartbeat(ctx)
		}
	}
}

// heartbeat extends the leases of every in-flight job in one call.
func (p *Pool) heartbeat(ctx context.Context) {
	started := time.Now()
	p.mu.Lock()
	snap := make([]*flight, 0, len(p.inflight))
	tokens := make([]uuid.UUID, 0, len(p.inflight))
	for tok, f := range p.inflight {
		if !f.lost {
			snap = append(snap, f)
			tokens = append(tokens, tok)
		}
	}
	p.mu.Unlock()
	if len(tokens) == 0 {
		return
	}

	rctx, cancel := context.WithTimeout(ctx, max(p.Lease/2, time.Millisecond))
	alive, err := p.Store.ExtendLeases(rctx, tokens, p.Lease)
	cancel()
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		p.Logger.Warn("heartbeat failed", "err", err, "jobs", len(tokens))
		// Transient until a full lease has passed without confirmation; after
		// that the lease may have expired and been reaped, so stop assuming it.
		now := time.Now()
		for _, f := range snap {
			p.mu.Lock()
			stale := now.Sub(f.confirmed) >= p.Lease
			p.mu.Unlock()
			if stale {
				p.Logger.Error("heartbeat failing for a full lease, cancelling job", "token", f.token)
				p.markLost(f, ErrLeaseUncertain)
			}
		}
		return
	}

	ok := make(map[uuid.UUID]struct{}, len(alive))
	for _, tok := range alive {
		ok[tok] = struct{}{}
	}
	for _, f := range snap {
		if _, yes := ok[f.token]; yes {
			p.mu.Lock()
			f.confirmed = started
			p.mu.Unlock()
		} else {
			p.Logger.Warn("lease lost, cancelling job", "token", f.token)
			p.markLost(f, ErrLeaseLost)
		}
	}
}

func (p *Pool) execute(f *flight, j store.Job) {
	log := p.Logger.With("job_id", j.ID, "type", j.Type, "attempt", j.Attempts)
	if j.LeaseToken == nil {
		log.Error("leased job has no lease token")
		f.cancel(nil)
		return
	}
	token := f.token
	defer p.untrack(f) // after the report, so heartbeats cover the write too
	ctx := f.ctx

	var err error
	if h, ok := p.Registry.Lookup(j.Type); !ok {
		// Retryable on purpose: during a rolling deploy another worker may have this handler.
		err = fmt.Errorf("no handler registered for type %s", j.Type)
	} else {
		hctx, cancel := context.WithTimeout(ctx, p.JobTimeout)
		err = runSafely(hctx, h, j, log)
		timedOut := errors.Is(hctx.Err(), context.DeadlineExceeded) && ctx.Err() == nil
		cancel()
		if err != nil && timedOut {
			err = fmt.Errorf("job timed out after %s: %w", p.JobTimeout, err)
		}
	}

	if p.isLost(f) {
		// Someone else owns the job now (or may); reporting could only confuse things.
		log.Warn("lease lost during execution; result discarded", "cause", context.Cause(ctx), "err", err)
		return
	}
	if err != nil && ctx.Err() != nil {
		// Forced shutdown: leave the job running; it is re-leased once its lease expires.
		log.Warn("job abandoned at shutdown", "err", err)
		return
	}

	// Detached from the run context so results are written during shutdown.
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), reportTimeout)
	defer cancel()
	if err == nil {
		err = p.Store.CompleteJob(rctx, j.ID, token)
	} else {
		retryIn := p.Backoff.Delay(j.Attempts)
		if IsPermanent(err) {
			retryIn = store.NoRetry
		}
		log.Info("job failed", "err", err, "retry_in", retryIn)
		err = p.Store.FailJob(rctx, j.ID, token, err.Error(), retryIn)
	}
	switch {
	case errors.Is(err, store.ErrLeaseLost):
		log.Warn("lease lost; another worker owns the job")
	case err != nil:
		log.Error("report result failed", "err", err)
	}
}

func runSafely(ctx context.Context, h Handler, j store.Job, log *slog.Logger) (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("handler panic", "panic", r, "stack", string(debug.Stack()))
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return h(ctx, j)
}

func backoff(failures int) time.Duration {
	d := 100 * time.Millisecond << min(failures-1, 6)
	return jitter(min(d, maxBackoff))
}

// jitter returns d scaled to [0.5d, 1.5d).
func jitter(d time.Duration) time.Duration {
	return d/2 + rand.N(d)
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}
