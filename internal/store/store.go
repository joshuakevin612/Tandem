// Package store is the Postgres-backed job queue.
package store

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

var (
	ErrNotFound     = errors.New("store: job not found")
	ErrLeaseLost    = errors.New("store: lease lost")
	ErrInvalidState = errors.New("store: job in invalid state")
)

type Job struct {
	ID             uuid.UUID       `json:"id" db:"id"`
	TenantID       string          `json:"tenant_id" db:"tenant_id"`
	Queue          string          `json:"queue" db:"queue"`
	Type           string          `json:"type" db:"type"`
	Payload        json.RawMessage `json:"payload" db:"payload"`
	Status         string          `json:"status" db:"status"`
	Priority       int16           `json:"priority" db:"priority"`
	RunAt          time.Time       `json:"run_at" db:"run_at"`
	Attempts       int             `json:"attempts" db:"attempts"`
	MaxAttempts    int             `json:"max_attempts" db:"max_attempts"`
	LockedUntil    *time.Time      `json:"locked_until" db:"locked_until"`
	LeaseToken     *uuid.UUID      `json:"lease_token" db:"lease_token"`
	WorkerID       *string         `json:"worker_id" db:"worker_id"`
	IdempotencyKey *string         `json:"idempotency_key" db:"idempotency_key"`
	LastError      *string         `json:"last_error" db:"last_error"`
	CreatedAt      time.Time       `json:"created_at" db:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at" db:"updated_at"`
	FinishedAt     *time.Time      `json:"finished_at" db:"finished_at"`
}

type NewJob struct {
	TenantID       string
	Queue          string
	Type           string
	Payload        json.RawMessage
	Priority       int16
	RunAt          *time.Time
	MaxAttempts    int
	IdempotencyKey *string
}

type Store struct{ pool *pgxpool.Pool }

const jobCols = `id, tenant_id, queue, type, payload, status, priority, run_at, attempts, max_attempts,
	locked_until, lease_token, worker_id, idempotency_key, last_error, created_at, updated_at, finished_at`

func Open(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// Migrate applies all pending migrations. golang-migrate's pgx5 driver
// registers under the "pgx5" scheme, so the DSN scheme is rewritten.
func Migrate(dsn string) error {
	src, err := iofs.New(migrationsFS, "migrations")
	if err != nil {
		return err
	}
	if rest, ok := strings.CutPrefix(dsn, "postgresql://"); ok {
		dsn = "pgx5://" + rest
	} else if rest, ok := strings.CutPrefix(dsn, "postgres://"); ok {
		dsn = "pgx5://" + rest
	}
	m, err := migrate.NewWithSourceInstance("iofs", src, dsn)
	if err != nil {
		return err
	}
	defer m.Close()
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return err
	}
	return nil
}

func (s *Store) CreateJob(ctx context.Context, n NewJob) (Job, error) {
	if n.TenantID == "" {
		n.TenantID = "default"
	}
	if n.Queue == "" {
		n.Queue = "default"
	}
	if n.MaxAttempts <= 0 {
		n.MaxAttempts = 5
	}
	if len(n.Payload) == 0 {
		n.Payload = json.RawMessage(`{}`)
	}
	const ins = `INSERT INTO jobs (id, tenant_id, queue, type, payload, priority, run_at, max_attempts, idempotency_key)
		VALUES ($1, $2, $3, $4, $5, $6, COALESCE($7, now()), $8, $9)
		ON CONFLICT (tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
		RETURNING ` + jobCols
	// A concurrent insert with the same key blocks until the winner commits, so
	// the follow-up select normally finds the row; the loop covers the row
	// vanishing between the two statements.
	for {
		j, err := s.one(ctx, ins, uuid.New(), n.TenantID, n.Queue, n.Type, []byte(n.Payload), n.Priority, n.RunAt, n.MaxAttempts, n.IdempotencyKey)
		if !errors.Is(err, ErrNotFound) || n.IdempotencyKey == nil {
			return j, err
		}
		j, err = s.one(ctx, `SELECT `+jobCols+` FROM jobs WHERE tenant_id=$1 AND idempotency_key=$2`, n.TenantID, *n.IdempotencyKey)
		if !errors.Is(err, ErrNotFound) {
			return j, err
		}
	}
}

func (s *Store) GetJob(ctx context.Context, id uuid.UUID) (Job, error) {
	return s.one(ctx, `SELECT `+jobCols+` FROM jobs WHERE id=$1`, id)
}

// LeaseJobs claims up to limit due jobs in a single statement. FOR UPDATE SKIP
// LOCKED lets concurrent workers pass over rows another worker is claiming
// instead of blocking on them, so no job is handed out twice and workers do
// not serialize. Each job gets its own lease_token and an open job_attempts row.
func (s *Store) LeaseJobs(ctx context.Context, queue, workerID string, limit int, lease time.Duration) ([]Job, error) {
	const q = `
WITH picked AS (
	SELECT id FROM jobs
	WHERE status = 'queued' AND queue = $1 AND run_at <= now()
	ORDER BY priority DESC, run_at
	LIMIT $2
	FOR UPDATE SKIP LOCKED
), leased AS (
	UPDATE jobs j SET
		status = 'running',
		attempts = j.attempts + 1,
		locked_until = now() + $4::interval,
		lease_token = gen_random_uuid(),
		worker_id = $3,
		updated_at = now()
	FROM picked WHERE j.id = picked.id
	RETURNING j.*
), attempt_rows AS (
	INSERT INTO job_attempts (job_id, attempt, worker_id, lease_token)
	SELECT id, attempts, worker_id, lease_token FROM leased
)
SELECT ` + jobCols + ` FROM leased ORDER BY priority DESC, run_at`
	rows, err := s.pool.Query(ctx, q, queue, limit, workerID, lease)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByName[Job])
}

func (s *Store) CompleteJob(ctx context.Context, id, leaseToken uuid.UUID) error {
	return s.finish(ctx, id, leaseToken, "succeeded", "")
}

// finish moves a running job to a terminal status (status doubles as the
// attempt outcome). The lease_token + status fence means a worker whose lease
// expired, and whose job may have been re-leased, cannot overwrite the newer
// owner's state.
func (s *Store) finish(ctx context.Context, id, token uuid.UUID, status, errMsg string) error {
	const q = `
WITH j AS (
	UPDATE jobs SET
		status = $3,
		last_error = CASE WHEN $4 = '' THEN last_error ELSE $4 END,
		locked_until = NULL,
		lease_token = NULL,
		finished_at = now(),
		updated_at = now()
	WHERE id = $1 AND lease_token = $2 AND status = 'running'
	RETURNING id, attempts
), a AS (
	UPDATE job_attempts a SET finished_at = now(), outcome = $3, error = NULLIF($4, '')
	FROM j WHERE a.job_id = j.id AND a.attempt = j.attempts
)
SELECT count(*) FROM j`
	var n int
	if err := s.pool.QueryRow(ctx, q, id, token, status, errMsg).Scan(&n); err != nil {
		return err
	}
	if n == 0 {
		return ErrLeaseLost
	}
	return nil
}

func (s *Store) one(ctx context.Context, q string, args ...any) (Job, error) {
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return Job{}, err
	}
	j, err := pgx.CollectOneRow(rows, pgx.RowToStructByName[Job])
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	if err != nil {
		return Job{}, fmt.Errorf("store: %w", err)
	}
	return j, nil
}
