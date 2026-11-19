package store

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ErrKeyNotFound means no active (non-revoked) API key matched.
var ErrKeyNotFound = errors.New("store: api key not found")

// APIKey is key metadata; the secret itself is never stored, only its hash.
type APIKey struct {
	ID        uuid.UUID  `json:"id"`
	TenantID  string     `json:"tenant_id"`
	Prefix    string     `json:"prefix"`
	Name      string     `json:"name"`
	CreatedAt time.Time  `json:"created_at"`
	RevokedAt *time.Time `json:"revoked_at"`
}

// CreateAPIKey stores the sha256 hash of a key for a tenant.
func (s *Store) CreateAPIKey(ctx context.Context, tenantID, name, prefix string, keyHash []byte) (APIKey, error) {
	k := APIKey{ID: uuid.New(), TenantID: tenantID, Prefix: prefix, Name: name}
	err := s.pool.QueryRow(ctx,
		`INSERT INTO api_keys (id, tenant_id, key_hash, prefix, name) VALUES ($1,$2,$3,$4,$5) RETURNING created_at`,
		k.ID, tenantID, keyHash, prefix, name).Scan(&k.CreatedAt)
	return k, err
}

// TenantForKeyHash resolves a key hash to its tenant, ignoring revoked keys.
func (s *Store) TenantForKeyHash(ctx context.Context, keyHash []byte) (string, error) {
	var tenant string
	err := s.pool.QueryRow(ctx,
		`SELECT tenant_id FROM api_keys WHERE key_hash = $1 AND revoked_at IS NULL`, keyHash).Scan(&tenant)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrKeyNotFound
	}
	return tenant, err
}

// RevokeAPIKey revokes an active key; ErrKeyNotFound if missing or already revoked.
func (s *Store) RevokeAPIKey(ctx context.Context, id uuid.UUID) error {
	tag, err := s.pool.Exec(ctx, `UPDATE api_keys SET revoked_at = now() WHERE id = $1 AND revoked_at IS NULL`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrKeyNotFound
	}
	return nil
}

// ListAPIKeys returns a tenant's keys (including revoked), newest first.
func (s *Store) ListAPIKeys(ctx context.Context, tenantID string) ([]APIKey, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, tenant_id, prefix, coalesce(name,''), created_at, revoked_at
		 FROM api_keys WHERE tenant_id = $1 ORDER BY created_at DESC, id`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []APIKey
	for rows.Next() {
		var k APIKey
		if err := rows.Scan(&k.ID, &k.TenantID, &k.Prefix, &k.Name, &k.CreatedAt, &k.RevokedAt); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}
