package api

import (
	"net/http"
	"testing"
)

func TestUIIsPublic(t *testing.T) {
	// Auth that rejects everything: the UI must still be served.
	deny := func(http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusUnauthorized) })
	}
	h := NewServer(&fakeStore{}, quiet(), WithDeps(Deps{Auth: deny}))

	rec := do(h, "GET", "/", "", nil)
	if rec.Code != http.StatusFound || rec.Header().Get("Location") != "/ui/" {
		t.Fatalf("GET /: %d %q", rec.Code, rec.Header().Get("Location"))
	}
	rec = do(h, "GET", "/ui", "", nil)
	if rec.Code != http.StatusMovedPermanently || rec.Header().Get("Location") != "/ui/" {
		t.Fatalf("GET /ui: %d %q", rec.Code, rec.Header().Get("Location"))
	}
	if rec = do(h, "GET", "/ui/", "", nil); rec.Code != 200 {
		t.Fatalf("GET /ui/: %d", rec.Code)
	}
	if rec = do(h, "GET", "/v1/jobs", "", nil); rec.Code != http.StatusUnauthorized {
		t.Fatalf("GET /v1/jobs should still be auth-gated, got %d", rec.Code)
	}
}
