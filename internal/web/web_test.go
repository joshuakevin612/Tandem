package web

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func get(path string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func TestIndex(t *testing.T) {
	rec := get("/ui/")
	if rec.Code != 200 {
		t.Fatalf("code = %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Fatalf("content-type = %q", ct)
	}
	if rec.Header().Get("Cache-Control") != "no-cache" {
		t.Fatalf("cache-control = %q", rec.Header().Get("Cache-Control"))
	}
	if rec.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Fatal("missing nosniff")
	}
}

func TestAssetsNotCachedStale(t *testing.T) {
	for _, p := range []string{"/ui/css/app.css", "/ui/js/main.js"} {
		rec := get(p)
		if rec.Code != 200 || rec.Header().Get("Cache-Control") != "no-cache" {
			t.Fatalf("%s: code=%d cache-control=%q", p, rec.Code, rec.Header().Get("Cache-Control"))
		}
	}
}

func TestSPAFallback(t *testing.T) {
	want := get("/ui/").Body.String()
	rec := get("/ui/jobs/123")
	if rec.Code != 200 || rec.Body.String() != want {
		t.Fatalf("fallback code=%d", rec.Code)
	}
	if !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/html") {
		t.Fatalf("content-type = %q", rec.Header().Get("Content-Type"))
	}
}

func TestMissingFileWithExtension404(t *testing.T) {
	for _, p := range []string{"/ui/nope.js", "/ui/assets/nope.png"} {
		if rec := get(p); rec.Code != 404 {
			t.Fatalf("%s: code = %d", p, rec.Code)
		}
	}
}

func TestCSPHeader(t *testing.T) {
	csp := get("/ui/").Header().Get("Content-Security-Policy")
	for _, part := range []string{"default-src 'self'", "script-src 'self'", "frame-ancestors 'none'", "connect-src 'self'"} {
		if !strings.Contains(csp, part) {
			t.Fatalf("CSP %q missing %q", csp, part)
		}
	}
}
