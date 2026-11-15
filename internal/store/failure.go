package store

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// NoRetry, passed as FailJob's retryIn, marks a failure as permanent: the job
// goes straight to dead regardless of remaining attempts.
const NoRetry time.Duration = -1

// FailJob records a failed attempt. In one statement the job is either
// requeued with run_at = now()+retryIn (attempts remain and retryIn >= 0) or
// moved to dead. Fenced by lease_token like CompleteJob.
func (s *Store) FailJob(ctx context.Context, id, leaseToken uuid.UUID, errMsg string, retryIn time.Duration) error {
	const q = `
WITH j AS (
	UPDATE jobs SET
		status = CASE WHEN attempts < max_attempts AND $4 THEN 'queued' ELSE 'dead' END,
		run_at = CASE WHEN attempts < max_attempts AND $4 THEN now() + $5::interval ELSE run_at END,
		finished_at = CASE WHEN attempts < max_attempts AND $4 THEN NULL ELSE now() END,
		last_error = $3,
		locked_until = NULL,
		lease_token = NULL,
		updated_at = now()
	WHERE id = $1 AND lease_token = $2 AND status = 'running'
	RETURNING id, attempts
), a AS (
	UPDATE job_attempts a SET finished_at = now(), outcome = 'failed', error = $3
	FROM j WHERE a.job_id = j.id AND a.attempt = j.attempts
)
SELECT count(*) FROM j`
	var n int
	if err := s.pool.QueryRow(ctx, q, id, leaseToken, errMsg, retryIn >= 0, max(retryIn, 0)).Scan(&n); err != nil {
		return err
	}
	if n == 0 {
		return ErrLeaseLost
	}
	return nil
}

// ExtendLeases is a batched heartbeat: it pushes locked_until out for every
// running job whose lease_token is in tokens and returns the tokens still
// owned. Tokens missing from the result were completed, reaped or re-leased.
func (s *Store) ExtendLeases(ctx context.Context, tokens []uuid.UUID, lease time.Duration) ([]uuid.UUID, error) {
	if len(tokens) == 0 {
		return nil, nil
	}
	const q = `
UPDATE jobs SET locked_until = now() + $2::interval, updated_at = now()
WHERE lease_token = ANY($1) AND status = 'running'
RETURNING lease_token`
	rows, err := s.pool.Query(ctx, q, tokens, lease)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[uuid.UUID])
}

// ReapExpired recovers jobs whose lease expired (the worker died or stalled):
// requeued while attempts remain, dead otherwise. FOR UPDATE SKIP LOCKED lets
// any number of reapers run concurrently, each taking a disjoint batch.
// Clearing lease_token fences out the old owner. run_at is left alone so a
// reclaimed job keeps its place at the front of the queue rather than waiting
// behind everything submitted since.
func (s *Store) ReapExpired(ctx context.Context, limit int) (requeued, dead int, err error) {
	const q = `
WITH picked AS (
	SELECT id FROM jobs
	WHERE status = 'running' AND locked_until < now()
	ORDER BY locked_until
	LIMIT $1
	FOR UPDATE SKIP LOCKED
), reaped AS (
	UPDATE jobs j SET
		status = CASE WHEN j.attempts >= j.max_attempts THEN 'dead' ELSE 'queued' END,
		finished_at = CASE WHEN j.attempts >= j.max_attempts THEN now() ELSE NULL END,
		last_error = CASE WHEN j.attempts >= j.max_attempts THEN 'lease expired' ELSE j.last_error END,
		locked_until = NULL,
		lease_token = NULL,
		updated_at = now()
	FROM picked WHERE j.id = picked.id
	RETURNING j.id, j.attempts, j.status
), a AS (
	UPDATE job_attempts a SET finished_at = now(), outcome = 'lease_expired', error = 'lease expired'
	FROM reaped WHERE a.job_id = reaped.id AND a.attempt = reaped.attempts AND a.finished_at IS NULL
)
SELECT count(*) FILTER (WHERE status = 'queued'), count(*) FILTER (WHERE status = 'dead') FROM reaped`
	err = s.pool.QueryRow(ctx, q, limit).Scan(&requeued, &dead)
	return requeued, dead, err
}

type ListFilter struct {
	TenantID string
	Status   string     // optional
	Queue    string     // optional
	Limit    int        // default 50, max 200
	Before   *time.Time // created_at cursor: only jobs created strictly before
	// BeforeID is the id half of the cursor. With Before it selects rows
	// strictly before (created_at, id) in the list order, so jobs sharing a
	// created_at are neither skipped nor repeated across pages.
	BeforeID *uuid.UUID
}

// ListJobs returns a tenant's jobs, newest first.
func (s *Store) ListJobs(ctx context.Context, f ListFilter) ([]Job, error) {
	if f.Limit <= 0 {
		f.Limit = 50
	}
	f.Limit = min(f.Limit, 200)
	const q = `SELECT ` + jobCols + ` FROM jobs
WHERE tenant_id = $1
	AND ($2::text = '' OR status = $2)
	AND ($3::text = '' OR queue = $3)
	AND ($4::timestamptz IS NULL OR CASE WHEN $6::uuid IS NULL THEN created_at < $4 ELSE (created_at, id) < ($4, $6) END)
ORDER BY created_at DESC, id DESC
LIMIT $5`
	rows, err := s.pool.Query(ctx, q, f.TenantID, f.Status, f.Queue, f.Before, f.Limit, f.BeforeID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByName[Job])
}

// RequeueJob gives a dead (or legacy failed) job a fresh set of attempts.
// last_error is kept for context.
func (s *Store) RequeueJob(ctx context.Context, tenantID string, id uuid.UUID) (Job, error) {
	const q = `
UPDATE jobs SET status = 'queued', attempts = 0, run_at = now(), finished_at = NULL,
	locked_until = NULL, lease_token = NULL, updated_at = now()
WHERE tenant_id = $1 AND id = $2 AND status IN ('dead', 'failed')
RETURNING ` + jobCols
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
