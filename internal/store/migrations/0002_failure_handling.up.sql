CREATE INDEX jobs_tenant_status_created_idx ON jobs (tenant_id, status, created_at DESC);
CREATE INDEX jobs_tenant_created_idx ON jobs (tenant_id, created_at DESC);
