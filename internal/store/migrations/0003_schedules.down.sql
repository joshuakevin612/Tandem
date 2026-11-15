DROP INDEX IF EXISTS jobs_schedule_idx;
ALTER TABLE jobs DROP COLUMN IF EXISTS schedule_id;

UPDATE job_attempts SET outcome = 'failed' WHERE outcome = 'cancelled';
ALTER TABLE job_attempts DROP CONSTRAINT job_attempts_outcome_check;
ALTER TABLE job_attempts ADD CONSTRAINT job_attempts_outcome_check
    CHECK (outcome IN ('succeeded','failed','lease_expired'));

DROP TABLE IF EXISTS schedules;
