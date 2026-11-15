package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// ErrConflict means a unique constraint was violated: a schedule with the same
// (tenant, name) already exists.
var ErrConflict = errors.New("store: conflict")

// Schedule is a cron schedule plus the job template it fires.
type Schedule struct {
	ID          uuid.UUID       `json:"id" db:"id"`
	TenantID    string          `json:"tenant_id" db:"tenant_id"`
	Name        string          `json:"name" db:"name"`
	Cron        string          `json:"cron" db:"cron"`
	Timezone    string          `json:"timezone" db:"timezone"`
	Queue       string          `json:"queue" db:"queue"`
	Type        string          `json:"type" db:"type"`
	Payload     json.RawMessage `json:"payload" db:"payload"`
	Priority    int16           `json:"priority" db:"priority"`
	MaxAttempts int             `json:"max_attempts" db:"max_attempts"`
	Enabled     bool            `json:"enabled" db:"enabled"`
	NextFireAt  time.Time       `json:"next_fire_at" db:"next_fire_at"`
	LastFireAt  *time.Time      `json:"last_fire_at" db:"last_fire_at"`
	CreatedAt   time.Time       `json:"created_at" db:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at" db:"updated_at"`
}

// NewSchedule is the input to CreateSchedule. Cron expressions are validated by
// the caller (see internal/cronexpr); the store only persists them.
type NewSchedule struct {
	TenantID    string
	Name        string
	Cron        string
	Timezone    string
	Queue       string
	Type        string
	Payload     json.RawMessage
	Priority    int16
	MaxAttempts int
	NextFireAt  time.Time
}

const scheduleCols = `id, tenant_id, name, cron, timezone, queue, type, payload, priority, max_attempts,
	enabled, next_fire_at, last_fire_at, created_at, updated_at`

func (s *Store) scheduleOne(ctx context.Context, q string, args ...any) (Schedule, error) {
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return Schedule{}, err
	}
	sc, err := pgx.CollectOneRow(rows, pgx.RowToStructByName[Schedule])
	if errors.Is(err, pgx.ErrNoRows) {
		return Schedule{}, ErrNotFound
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return Schedule{}, ErrConflict
	}
	if err != nil {
		return Schedule{}, fmt.Errorf("store: %w", err)
	}
	return sc, nil
}

// CreateSchedule inserts an enabled schedule. A duplicate (tenant, name)
// returns ErrConflict.
func (s *Store) CreateSchedule(ctx context.Context, n NewSchedule) (Schedule, error) {
	if n.TenantID == "" {
		n.TenantID = "default"
	}
	if n.Queue == "" {
		n.Queue = "default"
	}
	if n.Timezone == "" {
		n.Timezone = "UTC"
	}
	if n.MaxAttempts <= 0 {
		n.MaxAttempts = 5
	}
	if len(n.Payload) == 0 {
		n.Payload = json.RawMessage(`{}`)
	}
	const q = `INSERT INTO schedules (id, tenant_id, name, cron, timezone, queue, type, payload, priority, max_attempts, next_fire_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		RETURNING ` + scheduleCols
	return s.scheduleOne(ctx, q, uuid.New(), n.TenantID, n.Name, n.Cron, n.Timezone, n.Queue, n.Type, []byte(n.Payload), n.Priority, n.MaxAttempts, n.NextFireAt)
}

func (s *Store) GetSchedule(ctx context.Context, tenantID string, id uuid.UUID) (Schedule, error) {
	return s.scheduleOne(ctx, `SELECT `+scheduleCols+` FROM schedules WHERE tenant_id=$1 AND id=$2`, tenantID, id)
}

// ListSchedules returns a tenant's schedules ordered by name.
func (s *Store) ListSchedules(ctx context.Context, tenantID string) ([]Schedule, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+scheduleCols+` FROM schedules WHERE tenant_id=$1 ORDER BY name, id`, tenantID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByName[Schedule])
}

// DeleteSchedule removes a schedule. Jobs it already created are kept with
// schedule_id set to NULL.
func (s *Store) DeleteSchedule(ctx context.Context, tenantID string, id uuid.UUID) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM schedules WHERE tenant_id=$1 AND id=$2`, tenantID, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// SetScheduleEnabled pauses or resumes a schedule and always sets next_fire_at,
// so a resumed schedule does not replay the fires missed while it was disabled;
// the caller computes nextFireAt from the cron expression.
func (s *Store) SetScheduleEnabled(ctx context.Context, tenantID string, id uuid.UUID, enabled bool, nextFireAt time.Time) (Schedule, error) {
	const q = `UPDATE schedules SET enabled=$3, next_fire_at=$4, updated_at=now()
		WHERE tenant_id=$1 AND id=$2 RETURNING ` + scheduleCols
	return s.scheduleOne(ctx, q, tenantID, id, enabled, nextFireAt)
}

// DueSchedules returns enabled schedules whose next_fire_at has passed, oldest
// first. It takes no locks: FireSchedule's compare-and-swap arbitrates between
// concurrent schedulers.
func (s *Store) DueSchedules(ctx context.Context, limit int) ([]Schedule, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `SELECT `+scheduleCols+` FROM schedules
		WHERE enabled AND next_fire_at <= now() ORDER BY next_fire_at, id LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByName[Schedule])
}

// FireSchedule enqueues the job for the occurrence sc.NextFireAt and advances
// the schedule to next, in a single statement.
//
// Correctness does not depend on leader election; LeaderLock is only an
// optimisation to avoid wasted work. Double firing is made impossible by two
// independent mechanisms, so it holds even if two schedulers both believe they
// are leader:
//
//  1. a compare-and-swap on the schedule row (next_fire_at must still equal
//     sc.NextFireAt and the schedule must be enabled): of any number of
//     concurrent callers with the same sc, exactly one advances the row, and
//     only that caller inserts a job;
//  2. the job's idempotency key 'sched:<id>:<unix seconds of sc.NextFireAt>'
//     with ON CONFLICT DO NOTHING, which holds even if the schedule row were
//     re-armed to the same instant by another path.
//
// fired is true only if a job row was actually inserted. sc must come from the
// database (DueSchedules/GetSchedule) so NextFireAt matches exactly; next must
// be after sc.NextFireAt. A lost CAS or an existing job returns (false, nil).
func (s *Store) FireSchedule(ctx context.Context, sc Schedule, next time.Time) (fired bool, err error) {
	if !next.After(sc.NextFireAt) {
		return false, fmt.Errorf("store: next fire %v not after current %v", next, sc.NextFireAt)
	}
	const q = `
WITH adv AS (
	UPDATE schedules SET last_fire_at = $2, next_fire_at = $3, updated_at = now()
	WHERE id = $1 AND next_fire_at = $2 AND enabled
	RETURNING id, tenant_id, queue, type, payload, priority, max_attempts
), ins AS (
	INSERT INTO jobs (id, tenant_id, queue, type, payload, priority, max_attempts, schedule_id, idempotency_key)
	SELECT gen_random_uuid(), tenant_id, queue, type, payload, priority, max_attempts, id,
		'sched:' || id::text || ':' || $4::bigint::text
	FROM adv
	ON CONFLICT (tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
	RETURNING id
)
SELECT count(*) FROM ins`
	var n int
	if err := s.pool.QueryRow(ctx, q, sc.ID, sc.NextFireAt, next, sc.NextFireAt.Unix()).Scan(&n); err != nil {
		return false, err
	}
	return n > 0, nil
}
