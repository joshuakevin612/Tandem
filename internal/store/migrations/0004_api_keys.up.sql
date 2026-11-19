CREATE TABLE api_keys (
    id          uuid PRIMARY KEY,
    tenant_id   text NOT NULL,
    key_hash    bytea NOT NULL UNIQUE,
    prefix      text NOT NULL,
    name        text,
    created_at  timestamptz NOT NULL DEFAULT now(),
    revoked_at  timestamptz
);

CREATE INDEX api_keys_tenant_idx ON api_keys (tenant_id);
