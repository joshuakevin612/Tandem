CREATE TABLE jobs (
    id              uuid PRIMARY KEY,
    tenant_id       text        NOT NULL DEFAULT 'default',
    queue           text        NOT NULL DEFAULT 'default',
    type            text        NOT NULL,
    payload         jsonb       NOT NULL DEFAULT '{}',
    status          text        NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued','running','succeeded','failed','dead','cancelled')),
    priority        smallint    NOT NULL DEFAULT 0,
    run_at          timestamptz NOT NULL DEFAULT now(),
    attempts        int         NOT NULL DEFAULT 0,
    max_attempts    int         NOT NULL DEFAULT 5,
    locked_until    timestamptz,
    lease_token     uuid,
    worker_id       text,
    idempotency_key text,
    last_error      text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    finished_at     timestamptz
);

CREATE TABLE job_attempts (
    id          bigserial PRIMARY KEY,
    job_id      uuid        NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt     int         NOT NULL,
    worker_id   text,
    lease_token uuid,
    started_at  timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    outcome     text CHECK (outcome IN ('succeeded','failed','lease_expired')),
    error       text
);

CREATE INDEX jobs_queued_idx ON jobs (queue, priority DESC, run_at) WHERE status = 'queued';
CREATE INDEX jobs_running_idx ON jobs (locked_until) WHERE status = 'running';
CREATE UNIQUE INDEX jobs_idempotency_idx ON jobs (tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX job_attempts_job_idx ON job_attempts (job_id);
