CREATE TABLE schedules (
    id           uuid PRIMARY KEY,
    tenant_id    text        NOT NULL DEFAULT 'default',
    name         text        NOT NULL,
    cron         text        NOT NULL,
    timezone     text        NOT NULL DEFAULT 'UTC',
    queue        text        NOT NULL DEFAULT 'default',
    type         text        NOT NULL,
    payload      jsonb       NOT NULL DEFAULT '{}',
    priority     smallint    NOT NULL DEFAULT 0,
    max_attempts int         NOT NULL DEFAULT 5,
    enabled      boolean     NOT NULL DEFAULT true,
    next_fire_at timestamptz NOT NULL,
    last_fire_at timestamptz,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, name)
);

CREATE INDEX schedules_due_idx ON schedules (next_fire_at) WHERE enabled;

ALTER TABLE job_attempts DROP CONSTRAINT job_attempts_outcome_check;
ALTER TABLE job_attempts ADD CONSTRAINT job_attempts_outcome_check
    CHECK (outcome IN ('succeeded','failed','lease_expired','cancelled'));

ALTER TABLE jobs ADD COLUMN schedule_id uuid REFERENCES schedules(id) ON DELETE SET NULL;
CREATE INDEX jobs_schedule_idx ON jobs (schedule_id) WHERE schedule_id IS NOT NULL;
