package scheduler

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"time"

	"taskq/internal/store"
)

// Lock is a held leadership lock.
type Lock interface {
	// Check returns an error once the lock is no longer held.
	Check(ctx context.Context) error
	// Release frees the lock. It must be safe to call more than once.
	Release()
}

// LockStore can try to take the leadership lock without blocking.
type LockStore interface {
	TryLock(ctx context.Context, key int64) (Lock, bool, error)
}

// PGLocker adapts *store.Store to LockStore.
type PGLocker struct{ S *store.Store }

func (p PGLocker) TryLock(ctx context.Context, key int64) (Lock, bool, error) {
	l, ok, err := p.S.TryLeaderLock(ctx, key)
	if err != nil || !ok {
		return nil, false, err
	}
	return l, true, nil
}

// Elector campaigns for a lock and runs a singleton function while it holds it.
// Leadership is an optimisation only (see store.FireSchedule): two leaders at
// once must never break correctness.
type Elector struct {
	Store         LockStore
	Key           int64
	RetryInterval time.Duration // default 2s
	CheckInterval time.Duration // default 1s
	Logger        *slog.Logger
}

// Run campaigns until ctx is cancelled, then returns nil. While leader it runs
// lead with a child context that is cancelled when leadership is lost, when
// ctx ends, or never otherwise. If lead returns by itself the lock is released
// and the elector campaigns again after a pause.
func (e *Elector) Run(ctx context.Context, lead func(ctx context.Context) error) error {
	retry, check, log := e.RetryInterval, e.CheckInterval, e.Logger
	if retry <= 0 {
		retry = 2 * time.Second
	}
	if check <= 0 {
		check = time.Second
	}
	if log == nil {
		log = slog.Default()
	}
	for ctx.Err() == nil {
		lock, ok, err := e.Store.TryLock(ctx, e.Key)
		if err != nil && ctx.Err() == nil {
			log.Error("leader lock attempt failed", "err", err)
		}
		if err == nil && ok {
			log.Info("became leader", "key", e.Key)
			e.lead(ctx, lock, lead, check, log)
			lock.Release()
		}
		// Jittered pause; also stops a failed leader from instantly re-grabbing.
		sleep(ctx, retry/2+rand.N(retry))
	}
	return nil
}

func (e *Elector) lead(ctx context.Context, lock Lock, lead func(context.Context) error, check time.Duration, log *slog.Logger) {
	lctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- lead(lctx) }()

	t := time.NewTicker(check)
	defer t.Stop()
	for {
		select {
		case err := <-done:
			if err != nil && ctx.Err() == nil {
				log.Error("leader function exited", "err", err)
			}
			return
		case <-ctx.Done():
			cancel()
			<-done
			return
		case <-t.C:
			cctx, ccancel := context.WithTimeout(ctx, 2*check)
			err := lock.Check(cctx)
			ccancel()
			if err != nil && ctx.Err() == nil {
				cancel()
				<-done // never run two leaders: wait for lead to stop first
				log.Warn("lost leadership", "err", err)
				return
			}
		}
	}
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}
