package api

import (
	"context"
	"crypto/sha256"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"tandem/internal/store"
)

// fakeKeys is a KeyResolver backed by a map of sha256(key) -> tenant.
type fakeKeys struct {
	mu    sync.Mutex
	keys  map[[sha256.Size]byte]string
	calls int
}

func newFakeKeys() *fakeKeys { return &fakeKeys{keys: map[[sha256.Size]byte]string{}} }

func (f *fakeKeys) add(key, tenant string) { f.keys[sha256.Sum256([]byte(key))] = tenant }
func (f *fakeKeys) revoke(key string)      { delete(f.keys, sha256.Sum256([]byte(key))) }

func (f *fakeKeys) TenantForKeyHash(_ context.Context, h []byte) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	var k [sha256.Size]byte
	copy(k[:], h)
	if t, ok := f.keys[k]; ok {
		return t, nil
	}
	return "", store.ErrKeyNotFound
}

func apiKeyServer(fs *fakeStore, kr KeyResolver, opts ...AuthOption) http.Handler {
	return NewServer(fs, quiet(), WithDeps(Deps{Auth: APIKeyAuth(kr, opts...)}))
}

func TestAPIKeyAuth(t *testing.T) {
	kr := newFakeKeys()
	kr.add("td_good", "acme")
	h := apiKeyServer(&fakeStore{}, kr)

	tests := []struct {
		name string
		hdr  map[string]string
		want int
	}{
		{"missing", nil, 401},
		{"bad key", map[string]string{"Authorization": "Bearer td_bad"}, 401},
		{"bad x-api-key", map[string]string{"X-API-Key": "td_bad"}, 401},
		{"non-bearer scheme", map[string]string{"Authorization": "Basic td_good"}, 401},
		{"bearer ok", map[string]string{"Authorization": "Bearer td_good"}, 202},
		{"x-api-key ok", map[string]string{"X-API-Key": "td_good"}, 202},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := do(h, "POST", "/v1/jobs", `{"type":"t"}`, tc.hdr)
			if rec.Code != tc.want {
				t.Fatalf("got %d: %s", rec.Code, rec.Body)
			}
			if tc.want == 401 && rec.Header().Get("Content-Type") != "application/json" {
				t.Fatalf("401 not JSON: %q", rec.Header().Get("Content-Type"))
			}
		})
	}
}

func TestAPIKeyRevokedIs401(t *testing.T) {
	kr := newFakeKeys()
	kr.add("td_k", "acme")
	h := apiKeyServer(&fakeStore{}, kr, WithAuthCacheTTL(0))
	hdr := map[string]string{"X-API-Key": "td_k"}
	if rec := do(h, "GET", "/v1/jobs", "", hdr); rec.Code != 200 {
		t.Fatalf("got %d", rec.Code)
	}
	kr.revoke("td_k")
	if rec := do(h, "GET", "/v1/jobs", "", hdr); rec.Code != 401 {
		t.Fatalf("revoked: got %d", rec.Code)
	}
}

func TestAPIKeyResolvesTenantAndIgnoresHeader(t *testing.T) {
	kr := newFakeKeys()
	kr.add("td_k", "acme")
	fs := &fakeStore{}
	h := apiKeyServer(fs, kr)
	rec := do(h, "POST", "/v1/jobs", `{"type":"t"}`,
		map[string]string{"X-API-Key": "td_k", "X-Tenant-ID": "victim"})
	if rec.Code != 202 || fs.got.TenantID != "acme" {
		t.Fatalf("code %d tenant %q", rec.Code, fs.got.TenantID)
	}
	// The spoofed header alone must not authenticate either.
	if rec := do(h, "GET", "/v1/jobs", "", map[string]string{"X-Tenant-ID": "acme"}); rec.Code != 401 {
		t.Fatalf("spoof only: got %d", rec.Code)
	}
}

func TestAPIKeyCrossTenant404(t *testing.T) {
	kr := newFakeKeys()
	kr.add("td_a", "a")
	fs := &fakeStore{get: func(id uuid.UUID) (store.Job, error) { return store.Job{ID: id, TenantID: "b"}, nil }}
	rec := do(apiKeyServer(fs, kr), "GET", "/v1/jobs/"+uuid.NewString(), "", map[string]string{"X-API-Key": "td_a"})
	if rec.Code != 404 {
		t.Fatalf("got %d", rec.Code)
	}
}

func TestAPIKeyHealthExempt(t *testing.T) {
	h := apiKeyServer(&fakeStore{}, newFakeKeys())
	for _, p := range []string{"/healthz", "/readyz"} {
		if rec := do(h, "GET", p, "", nil); rec.Code != 200 {
			t.Fatalf("%s: got %d", p, rec.Code)
		}
	}
}

func TestHeaderModeUnchanged(t *testing.T) {
	fs := &fakeStore{}
	h := NewServer(fs, quiet())
	if rec := do(h, "POST", "/v1/jobs", `{"type":"t"}`, map[string]string{"X-Tenant-ID": "acme"}); rec.Code != 202 || fs.got.TenantID != "acme" {
		t.Fatalf("code %d tenant %q", rec.Code, fs.got.TenantID)
	}
	if rec := do(h, "POST", "/v1/jobs", `{"type":"t"}`, nil); rec.Code != 202 || fs.got.TenantID != "default" {
		t.Fatalf("code %d tenant %q", rec.Code, fs.got.TenantID)
	}
}

func TestAPIKeyCacheTTL(t *testing.T) {
	kr := newFakeKeys()
	kr.add("td_k", "acme")
	now := time.Unix(1000, 0)
	h := apiKeyServer(&fakeStore{}, kr, WithAuthClock(func() time.Time { return now }), WithAuthCacheTTL(30*time.Second))
	hdr := map[string]string{"X-API-Key": "td_k"}
	get := func() int { return do(h, "GET", "/v1/jobs", "", hdr).Code }

	if get() != 200 || get() != 200 || kr.calls != 1 {
		t.Fatalf("second call should hit cache; resolver calls=%d", kr.calls)
	}
	kr.revoke("td_k")
	now = now.Add(29 * time.Second)
	if get() != 200 {
		t.Fatal("revocation lag: still cached within TTL")
	}
	now = now.Add(2 * time.Second)
	if get() != 401 {
		t.Fatal("after TTL the revocation must take effect")
	}
}

func TestAPIKeyResolverErrorIs500(t *testing.T) {
	h := NewServer(&fakeStore{}, quiet(), WithDeps(Deps{Auth: APIKeyAuth(errResolver{})}))
	if rec := do(h, "GET", "/v1/jobs", "", map[string]string{"X-API-Key": "td_k"}); rec.Code != 500 {
		t.Fatalf("got %d", rec.Code)
	}
}

type errResolver struct{}

func (errResolver) TenantForKeyHash(context.Context, []byte) (string, error) {
	return "", context.DeadlineExceeded
}
