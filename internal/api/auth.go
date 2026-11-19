package api

import (
	"context"
	"crypto/sha256"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"taskq/internal/store"
)

// Auth modes selected by AUTH_MODE.
const (
	AuthModeHeader = "header"
	AuthModeAPIKey = "apikey"
)

const (
	apiKeyHeader     = "X-API-Key"
	apiKeyPrefix     = "tq_"
	defaultAuthCache = 30 * time.Second
)

type tenantCtxKey struct{}

// KeyResolver maps the sha256 of an API key to its tenant. It must return
// store.ErrKeyNotFound for unknown or revoked keys.
type KeyResolver interface {
	TenantForKeyHash(ctx context.Context, keyHash []byte) (string, error)
}

// HeaderAuth is the default (insecure, development) mode: the tenant is
// whatever X-Tenant-ID says, or "default".
func HeaderAuth() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t := r.Header.Get(tenantHeader)
			if t == "" {
				t = defaultTenant
			}
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), tenantCtxKey{}, t)))
		})
	}
}

// AuthOption configures APIKeyAuth.
type AuthOption func(*apiKeyAuth)

// WithAuthClock injects the clock used for cache expiry.
func WithAuthClock(now func() time.Time) AuthOption { return func(a *apiKeyAuth) { a.now = now } }

// WithAuthCacheTTL overrides the positive-cache TTL (0 disables the cache).
func WithAuthCacheTTL(d time.Duration) AuthOption { return func(a *apiKeyAuth) { a.ttl = d } }

type cacheEntry struct {
	tenant  string
	expires time.Time
}

type apiKeyAuth struct {
	res KeyResolver
	now func() time.Time
	ttl time.Duration

	mu    sync.Mutex
	cache map[[sha256.Size]byte]cacheEntry
}

// APIKeyAuth authenticates requests by `Authorization: Bearer tq_...` or
// `X-API-Key`. The tenant comes only from the key; X-Tenant-ID is ignored.
//
// Only successful lookups are cached (hash -> tenant, ~30s TTL), so a revoked
// key keeps working on an API replica for up to the TTL after revocation.
// Failures are never cached, so a newly created key works immediately.
func APIKeyAuth(res KeyResolver, opts ...AuthOption) func(http.Handler) http.Handler {
	a := &apiKeyAuth{res: res, now: time.Now, ttl: defaultAuthCache, cache: map[[sha256.Size]byte]cacheEntry{}}
	for _, o := range opts {
		o(a)
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := extractKey(r)
			if key == "" {
				writeError(w, http.StatusUnauthorized, "missing API key")
				return
			}
			tenant, err := a.resolve(r.Context(), key)
			switch {
			case errors.Is(err, store.ErrKeyNotFound):
				writeError(w, http.StatusUnauthorized, "invalid API key")
			case err != nil:
				writeError(w, http.StatusInternalServerError, "internal error")
			default:
				next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), tenantCtxKey{}, tenant)))
			}
		})
	}
}

func extractKey(r *http.Request) string {
	if h := r.Header.Get("Authorization"); h != "" {
		if scheme, tok, ok := strings.Cut(h, " "); ok && strings.EqualFold(scheme, "Bearer") {
			if tok = strings.TrimSpace(tok); strings.HasPrefix(tok, apiKeyPrefix) {
				return tok
			}
		}
	}
	return strings.TrimSpace(r.Header.Get(apiKeyHeader))
}

func (a *apiKeyAuth) resolve(ctx context.Context, key string) (string, error) {
	h := sha256.Sum256([]byte(key))
	now := a.now()
	if a.ttl > 0 {
		a.mu.Lock()
		e, ok := a.cache[h]
		a.mu.Unlock()
		if ok && now.Before(e.expires) {
			return e.tenant, nil
		}
	}
	tenant, err := a.res.TenantForKeyHash(ctx, h[:])
	if err != nil {
		return "", err
	}
	if a.ttl > 0 {
		a.mu.Lock()
		if len(a.cache) > 10000 { // bound memory against junk-key floods
			clear(a.cache)
		}
		a.cache[h] = cacheEntry{tenant: tenant, expires: now.Add(a.ttl)}
		a.mu.Unlock()
	}
	return tenant, nil
}
