package store

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CancelJob moves a queued or running job to cancelled. lease_token and
// locked_until are cleared on purpose: a running job's owner no longer finds
// its token on the next heartbeat, cancels the handler and discards the
// result, and CompleteJob/FailJob/ReapExpired can no longer touch the row. Any
// open attempt is closed with outcome 'cancelled'. A job in a terminal state
// returns ErrInvalidState; an unknown id or another tenant's job, ErrNotFound.
func (s *Store) CancelJob(ctx context.Context, tenantID string, id uuid.UUID) (Job, error) {
	const q = `
WITH j AS (
	UPDATE jobs SET status = 'cancelled', finished_at = now(), locked_until = NULL,
		lease_token = NULL, updated_at = now()
	WHERE tenant_id = $1 AND id = $2 AND status IN ('queued', 'running')
	RETURNING ` + jobCols + `
), a AS (
	UPDATE job_attempts a SET finished_at = now(), outcome = 'cancelled'
	FROM j WHERE a.job_id = j.id AND a.finished_at IS NULL
)
SELECT ` + jobCols + ` FROM j`
	j, err := s.one(ctx, q, tenantID, id)
	if !errors.Is(err, ErrNotFound) {
		return j, err
	}
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM jobs WHERE tenant_id = $1 AND id = $2)`, tenantID, id).Scan(&exists); err != nil {
		return Job{}, err
	}
	if exists {
		return Job{}, ErrInvalidState
	}
	return Job{}, ErrNotFound
}

// LeaderLock is a session-level Postgres advisory lock bound to a dedicated
// connection. If the process or connection dies Postgres releases the lock
// automatically. It is an optimisation for singleton work, not a correctness
// mechanism: see FireSchedule.
type LeaderLock struct {
	mu   sync.Mutex
	conn *pgxpool.Conn // nil once released
	key  int64
}

// TryLeaderLock attempts pg_try_advisory_lock(key) on a connection taken from
// the pool and held until Release. ok is false if another session holds it.
func (s *Store) TryLeaderLock(ctx context.Context, key int64) (*LeaderLock, bool, error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, false, err
	}
	var got bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, key).Scan(&got); err != nil {
		closeConn(conn)
		return nil, false, err
	}
	if !got {
		conn.Release()
		return nil, false, nil
	}
	return &LeaderLock{conn: conn, key: key}, true, nil
}

// Check runs a cheap query on the lock's own connection and confirms the
// advisory lock is still held by that session. An error means leadership is
// lost and the caller must stop leading.
func (l *LeaderLock) Check(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.conn == nil {
		return errors.New("store: leader lock released")
	}
	var held bool
	err := l.conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory'
		AND pid = pg_backend_pid() AND granted AND ((classid::bigint << 32) | objid::bigint) = $1)`, l.key).Scan(&held)
	if err != nil {
		return err
	}
	if !held {
		return errors.New("store: leader lock not held")
	}
	return nil
}

// Release unlocks and closes the dedicated connection. Safe to call twice.
func (l *LeaderLock) Release() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.conn == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Unlock synchronously so the key is free as soon as Release returns, then
	// discard the connection rather than returning a once-locked one to the pool.
	_, _ = l.conn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, l.key)
	closeConn(l.conn)
	l.conn = nil
}

func closeConn(c *pgxpool.Conn) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = c.Hijack().Close(ctx)
}
