package handlers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"tandem/internal/store"
)

func job(payload string) store.Job { return store.Job{Payload: []byte(payload)} }

func TestSleepHonoursCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	res := make(chan error, 1)
	go func() { res <- Sleep(ctx, job(`{"ms": 3600000}`)) }()
	cancel()
	if err := <-res; !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestSleepCompletes(t *testing.T) {
	if err := Sleep(context.Background(), job(`{"ms": 1}`)); err != nil {
		t.Fatal(err)
	}
}

func TestFlakyExtremes(t *testing.T) {
	if err := Flaky(context.Background(), job(`{"fail_rate": 1}`)); err == nil {
		t.Fatal("fail_rate 1 should fail")
	}
	if err := Flaky(context.Background(), job(`{"fail_rate": 0}`)); err != nil {
		t.Fatal(err)
	}
}

func TestHTTP(t *testing.T) {
	type seen struct{ method, body string }
	got := make(chan seen, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/bad" {
			http.Error(w, "nope", http.StatusInternalServerError)
			return
		}
		b, _ := io.ReadAll(r.Body)
		got <- seen{r.Method, string(b)}
	}))
	defer srv.Close()

	ctx := context.Background()
	if err := HTTP(ctx, job(fmt.Sprintf(`{"url":%q,"body":"hi"}`, srv.URL+"/ok"))); err != nil {
		t.Fatal(err)
	}
	if s := <-got; s.method != "POST" || s.body != "hi" {
		t.Fatalf("server saw %+v", s)
	}
	if err := HTTP(ctx, job(fmt.Sprintf(`{"url":%q}`, srv.URL+"/bad"))); err == nil || !strings.Contains(err.Error(), "500") {
		t.Fatalf("non-2xx err = %v", err)
	}
	for _, u := range []string{"file:///etc/passwd", "ftp://x/y", "gopher://x", "/relative", ""} {
		if err := HTTP(ctx, job(fmt.Sprintf(`{"url":%q}`, u))); err == nil {
			t.Fatalf("url %q accepted", u)
		}
	}
}

func TestHTTPTimeout(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-release }))
	defer srv.Close()
	defer close(release)
	if err := HTTP(context.Background(), job(fmt.Sprintf(`{"url":%q,"timeout_ms":50}`, srv.URL))); err == nil {
		t.Fatal("expected timeout error")
	}
}
