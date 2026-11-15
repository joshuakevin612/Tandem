package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"taskq/internal/store"
)

type fakeStore struct {
	create  func(store.NewJob) (store.Job, error)
	get     func(uuid.UUID) (store.Job, error)
	list    func(store.ListFilter) ([]store.Job, error)
	requeue func(string, uuid.UUID) (store.Job, error)
	cancel  func(string, uuid.UUID) (store.Job, error)
	gotList store.ListFilter

	schedCreate func(store.NewSchedule) (store.Schedule, error)
	schedGet    func(string, uuid.UUID) (store.Schedule, error)
	schedList   func(string) ([]store.Schedule, error)
	schedDelete func(string, uuid.UUID) error
	schedSet    func(tenant string, id uuid.UUID, enabled bool, next time.Time) (store.Schedule, error)
	gotNewSched store.NewSchedule
	gotEnabled  *bool
	gotNext     time.Time
	gotTen      string
	pingErr     error
	got         store.NewJob
}

func (f *fakeStore) CreateJob(_ context.Context, n store.NewJob) (store.Job, error) {
	f.got = n
	if f.create != nil {
		return f.create(n)
	}
	return store.Job{ID: uuid.New(), TenantID: n.TenantID, Type: n.Type, Status: "queued"}, nil
}

func (f *fakeStore) GetJob(_ context.Context, id uuid.UUID) (store.Job, error) {
	if f.get != nil {
		return f.get(id)
	}
	return store.Job{ID: id, TenantID: "default"}, nil
}

func (f *fakeStore) ListJobs(_ context.Context, lf store.ListFilter) ([]store.Job, error) {
	f.gotList = lf
	if f.list != nil {
		return f.list(lf)
	}
	return nil, nil
}

func (f *fakeStore) RequeueJob(_ context.Context, tenantID string, id uuid.UUID) (store.Job, error) {
	f.gotTen = tenantID
	if f.requeue != nil {
		return f.requeue(tenantID, id)
	}
	return store.Job{ID: id, TenantID: tenantID, Status: "queued"}, nil
}

func (f *fakeStore) CancelJob(_ context.Context, tenantID string, id uuid.UUID) (store.Job, error) {
	f.gotTen = tenantID
	if f.cancel != nil {
		return f.cancel(tenantID, id)
	}
	return store.Job{ID: id, TenantID: tenantID, Status: "cancelled"}, nil
}

func (f *fakeStore) Ping(context.Context) error { return f.pingErr }

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func do(h http.Handler, method, path, body string, hdr map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestCreateJob(t *testing.T) {
	big := `{"type":"x","payload":"` + strings.Repeat("a", maxBodyBytes) + `"}`
	tests := []struct {
		name    string
		body    string
		hdr     map[string]string
		store   func(store.NewJob) (store.Job, error)
		want    int
		errPart string
	}{
		{name: "minimal", body: `{"type":"email"}`, want: 202},
		{name: "full", body: `{"type":"e","queue":"q","payload":{"a":1},"priority":-100,"run_at":"2030-01-02T03:04:05Z","max_attempts":25}`, want: 202},
		{name: "missing type", body: `{}`, want: 400, errPart: "type is required"},
		{name: "unknown field", body: `{"type":"x","bogus":1}`, want: 400},
		{name: "malformed", body: `{"type":`, want: 400},
		{name: "trailing data", body: `{"type":"x"}{}`, want: 400},
		{name: "empty body", body: ``, want: 400},
		{name: "too large", body: big, want: 400, errPart: "too large"},
		{name: "priority high", body: `{"type":"x","priority":101}`, want: 400, errPart: "priority"},
		{name: "priority low", body: `{"type":"x","priority":-101}`, want: 400, errPart: "priority"},
		{name: "attempts zero", body: `{"type":"x","max_attempts":0}`, want: 400, errPart: "max_attempts"},
		{name: "attempts high", body: `{"type":"x","max_attempts":26}`, want: 400, errPart: "max_attempts"},
		{name: "bad run_at", body: `{"type":"x","run_at":"tomorrow"}`, want: 400},
		{name: "long key", body: `{"type":"x"}`, hdr: map[string]string{"Idempotency-Key": strings.Repeat("k", 256)}, want: 400, errPart: "Idempotency-Key"},
		{name: "store error", body: `{"type":"x"}`, want: 500, errPart: "internal error",
			store: func(store.NewJob) (store.Job, error) { return store.Job{}, errors.New("pq: secret detail") }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs := &fakeStore{create: tc.store}
			rec := do(NewServer(fs, quiet()), "POST", "/v1/jobs", tc.body, tc.hdr)
			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d: %s", rec.Code, tc.want, rec.Body)
			}
			if tc.want == 202 {
				var j store.Job
				if err := json.Unmarshal(rec.Body.Bytes(), &j); err != nil {
					t.Fatal(err)
				}
				if loc := rec.Header().Get("Location"); loc != "/v1/jobs/"+j.ID.String() {
					t.Errorf("Location = %q", loc)
				}
				return
			}
			var e struct{ Error string }
			if err := json.Unmarshal(rec.Body.Bytes(), &e); err != nil || e.Error == "" {
				t.Fatalf("want JSON error body, got %q", rec.Body)
			}
			if !strings.Contains(e.Error, tc.errPart) {
				t.Errorf("error %q lacks %q", e.Error, tc.errPart)
			}
			if strings.Contains(e.Error, "secret") {
				t.Errorf("internal error leaked: %q", e.Error)
			}
		})
	}
}

func TestCreateJobPassThrough(t *testing.T) {
	fs := &fakeStore{}
	body := `{"type":"t","queue":"q","payload":{"a":1},"priority":7,"max_attempts":3}`
	rec := do(NewServer(fs, quiet()), "POST", "/v1/jobs", body,
		map[string]string{"Idempotency-Key": "abc", "X-Tenant-ID": "acme"})
	if rec.Code != 202 {
		t.Fatal(rec.Code, rec.Body)
	}
	n := fs.got
	if n.IdempotencyKey == nil || *n.IdempotencyKey != "abc" || n.TenantID != "acme" ||
		n.Queue != "q" || n.Priority != 7 || n.MaxAttempts != 3 || string(n.Payload) != `{"a":1}` {
		t.Errorf("unexpected NewJob: %+v", n)
	}

	fs = &fakeStore{}
	do(NewServer(fs, quiet()), "POST", "/v1/jobs", `{"type":"t","payload":null}`, nil)
	if fs.got.TenantID != "default" || fs.got.IdempotencyKey != nil || len(fs.got.Payload) != 0 {
		t.Errorf("defaults not applied: %+v", fs.got)
	}
}

func TestGetJob(t *testing.T) {
	id := uuid.New()
	tests := []struct {
		name string
		path string
		get  func(uuid.UUID) (store.Job, error)
		want int
	}{
		{"ok", "/v1/jobs/" + id.String(), nil, 200},
		{"bad id", "/v1/jobs/nope", nil, 400},
		{"not found", "/v1/jobs/" + id.String(), func(uuid.UUID) (store.Job, error) { return store.Job{}, store.ErrNotFound }, 404},
		{"store error", "/v1/jobs/" + id.String(), func(uuid.UUID) (store.Job, error) { return store.Job{}, errors.New("boom") }, 500},
		{"other tenant", "/v1/jobs/" + id.String(), func(uuid.UUID) (store.Job, error) { return store.Job{ID: id, TenantID: "acme"}, nil }, 404},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := do(NewServer(&fakeStore{get: tc.get}, quiet()), "GET", tc.path, "", nil)
			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d", rec.Code, tc.want)
			}
			if tc.want != 200 && !strings.Contains(rec.Body.String(), `"error"`) {
				t.Errorf("no JSON error: %s", rec.Body)
			}
		})
	}
}

func TestGetJobTenantHeader(t *testing.T) {
	id := uuid.New()
	fs := &fakeStore{get: func(uuid.UUID) (store.Job, error) { return store.Job{ID: id, TenantID: "acme"}, nil }}
	h := NewServer(fs, quiet())
	if rec := do(h, "GET", "/v1/jobs/"+id.String(), "", map[string]string{"X-Tenant-ID": "acme"}); rec.Code != 200 {
		t.Errorf("own tenant = %d", rec.Code)
	}
	if rec := do(h, "GET", "/v1/jobs/"+id.String(), "", map[string]string{"X-Tenant-ID": "other"}); rec.Code != 404 {
		t.Errorf("foreign tenant = %d", rec.Code)
	}
	long := map[string]string{"X-Tenant-ID": strings.Repeat("t", 256)}
	if rec := do(h, "GET", "/v1/jobs/"+id.String(), "", long); rec.Code != 400 {
		t.Errorf("long tenant = %d", rec.Code)
	}
}

func cursorFor(t time.Time, id uuid.UUID) string {
	return base64.RawURLEncoding.EncodeToString([]byte(t.Format(time.RFC3339Nano) + "|" + id.String()))
}

func TestListJobs(t *testing.T) {
	t0 := time.Date(2030, 1, 2, 3, 4, 5, 123456000, time.UTC)
	id0 := uuid.New()
	mk := func(n int) []store.Job {
		js := make([]store.Job, n)
		for i := range js {
			js[i] = store.Job{ID: uuid.New(), CreatedAt: t0.Add(-time.Duration(i) * time.Second)}
		}
		return js
	}
	tests := []struct {
		name     string
		query    string
		hdr      map[string]string
		list     func(store.ListFilter) ([]store.Job, error)
		want     int
		errPart  string
		wantJobs int
		wantNext bool
		check    func(*testing.T, store.ListFilter)
	}{
		{name: "empty", want: 200, wantJobs: 0},
		{name: "default limit not full", want: 200, list: func(store.ListFilter) ([]store.Job, error) { return mk(3), nil }, wantJobs: 3,
			check: func(t *testing.T, f store.ListFilter) {
				if f.Limit != 50 || f.TenantID != "default" {
					t.Errorf("filter = %+v", f)
				}
			}},
		{name: "full page", query: "?limit=2", want: 200, list: func(store.ListFilter) ([]store.Job, error) { return mk(2), nil }, wantJobs: 2, wantNext: true},
		{name: "filters pass through", query: "?status=dead&queue=q1&limit=200&cursor=" + cursorFor(t0, id0), hdr: map[string]string{"X-Tenant-ID": "acme"}, want: 200,
			check: func(t *testing.T, f store.ListFilter) {
				if f.Status != "dead" || f.Queue != "q1" || f.Limit != 200 || f.TenantID != "acme" ||
					f.Before == nil || !f.Before.Equal(t0) || f.BeforeID == nil || *f.BeforeID != id0 {
					t.Errorf("filter = %+v", f)
				}
			}},
		{name: "bad status", query: "?status=bogus", want: 400, errPart: "status"},
		{name: "limit zero", query: "?limit=0", want: 400, errPart: "limit"},
		{name: "limit too high", query: "?limit=201", want: 400, errPart: "limit"},
		{name: "limit not number", query: "?limit=abc", want: 400, errPart: "limit"},
		{name: "cursor not base64", query: "?cursor=!!!", want: 400, errPart: "cursor"},
		{name: "cursor no separator", query: "?cursor=" + base64.RawURLEncoding.EncodeToString([]byte("nope")), want: 400, errPart: "cursor"},
		{name: "cursor bad time", query: "?cursor=" + base64.RawURLEncoding.EncodeToString([]byte("yesterday|"+id0.String())), want: 400, errPart: "cursor"},
		{name: "cursor bad id", query: "?cursor=" + base64.RawURLEncoding.EncodeToString([]byte("2030-01-01T00:00:00Z|x")), want: 400, errPart: "cursor"},
		{name: "old before param not honoured", query: "?before=2030-01-01T00:00:00Z", want: 200, check: func(t *testing.T, f store.ListFilter) {
			if f.Before != nil {
				t.Errorf("before must no longer be honoured: %+v", f)
			}
		}},
		{name: "long tenant", hdr: map[string]string{"X-Tenant-ID": strings.Repeat("t", 256)}, want: 400, errPart: "X-Tenant-ID"},
		{name: "store error", want: 500, errPart: "internal error",
			list: func(store.ListFilter) ([]store.Job, error) { return nil, errors.New("pq: secret detail") }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs := &fakeStore{list: tc.list}
			rec := do(NewServer(fs, quiet()), "GET", "/v1/jobs"+tc.query, "", tc.hdr)
			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d: %s", rec.Code, tc.want, rec.Body)
			}
			if tc.check != nil {
				tc.check(t, fs.gotList)
			}
			if tc.want == 200 {
				var raw map[string]json.RawMessage
				if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
					t.Fatal(err)
				}
				if raw["jobs"] == nil || string(raw["jobs"]) == "null" {
					t.Fatalf("jobs must be an array: %s", rec.Body)
				}
				if _, ok := raw["next_cursor"]; !ok {
					t.Error("next_cursor key missing")
				}
				if _, ok := raw["next_before"]; ok {
					t.Error("next_before must be gone")
				}
				var resp struct {
					Jobs       []store.Job
					NextCursor *string `json:"next_cursor"`
				}
				if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
					t.Fatal(err)
				}
				if len(resp.Jobs) != tc.wantJobs {
					t.Errorf("jobs = %d, want %d", len(resp.Jobs), tc.wantJobs)
				}
				if (resp.NextCursor != nil) != tc.wantNext {
					t.Errorf("next_cursor = %v, want present=%v", resp.NextCursor, tc.wantNext)
				}
				if tc.wantNext {
					last := resp.Jobs[len(resp.Jobs)-1]
					if *resp.NextCursor != cursorFor(last.CreatedAt, last.ID) {
						t.Errorf("next_cursor %q does not encode last item", *resp.NextCursor)
					}
					ts, id, err := decodeCursor(*resp.NextCursor)
					if err != nil || !ts.Equal(last.CreatedAt) || id != last.ID {
						t.Errorf("cursor round trip: %v %v %v", ts, id, err)
					}
				}
				return
			}
			var e struct{ Error string }
			if err := json.Unmarshal(rec.Body.Bytes(), &e); err != nil || e.Error == "" {
				t.Fatalf("want JSON error body, got %q", rec.Body)
			}
			if !strings.Contains(e.Error, tc.errPart) || strings.Contains(e.Error, "secret") {
				t.Errorf("error %q (want part %q)", e.Error, tc.errPart)
			}
		})
	}
}

func TestRequeueJob(t *testing.T) {
	id := uuid.New()
	path := "/v1/jobs/" + id.String() + "/requeue"
	fail := func(err error) func(string, uuid.UUID) (store.Job, error) {
		return func(string, uuid.UUID) (store.Job, error) { return store.Job{}, err }
	}
	tests := []struct {
		name    string
		path    string
		hdr     map[string]string
		requeue func(string, uuid.UUID) (store.Job, error)
		want    int
		errPart string
		tenant  string
	}{
		{name: "ok", path: path, want: 200, tenant: "default"},
		{name: "ok tenant", path: path, hdr: map[string]string{"X-Tenant-ID": "acme"}, want: 200, tenant: "acme"},
		{name: "bad id", path: "/v1/jobs/nope/requeue", want: 400, errPart: "UUID"},
		{name: "not found", path: path, requeue: fail(store.ErrNotFound), want: 404, errPart: "not found"},
		{name: "invalid state", path: path, requeue: fail(store.ErrInvalidState), want: 409, errPart: "cannot be requeued"},
		{name: "long tenant", path: path, hdr: map[string]string{"X-Tenant-ID": strings.Repeat("t", 256)}, want: 400, errPart: "X-Tenant-ID"},
		{name: "store error", path: path, requeue: fail(errors.New("pq: secret detail")), want: 500, errPart: "internal error"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs := &fakeStore{requeue: tc.requeue}
			rec := do(NewServer(fs, quiet()), "POST", tc.path, "", tc.hdr)
			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d: %s", rec.Code, tc.want, rec.Body)
			}
			if tc.want == 200 {
				var j store.Job
				if err := json.Unmarshal(rec.Body.Bytes(), &j); err != nil || j.ID != id || fs.gotTen != tc.tenant {
					t.Errorf("job %+v tenant %q err %v", j, fs.gotTen, err)
				}
				return
			}
			var e struct{ Error string }
			if err := json.Unmarshal(rec.Body.Bytes(), &e); err != nil || e.Error == "" {
				t.Fatalf("want JSON error body, got %q", rec.Body)
			}
			if !strings.Contains(e.Error, tc.errPart) || strings.Contains(e.Error, "secret") {
				t.Errorf("error %q (want part %q)", e.Error, tc.errPart)
			}
		})
	}
}

func TestCancelJob(t *testing.T) {
	id := uuid.New()
	path := "/v1/jobs/" + id.String() + "/cancel"
	fail := func(err error) func(string, uuid.UUID) (store.Job, error) {
		return func(string, uuid.UUID) (store.Job, error) { return store.Job{}, err }
	}
	tests := []struct {
		name    string
		path    string
		hdr     map[string]string
		cancel  func(string, uuid.UUID) (store.Job, error)
		want    int
		errPart string
		tenant  string
	}{
		{name: "ok", path: path, want: 200, tenant: "default"},
		{name: "ok tenant", path: path, hdr: map[string]string{"X-Tenant-ID": "acme"}, want: 200, tenant: "acme"},
		{name: "bad id", path: "/v1/jobs/nope/cancel", want: 400, errPart: "UUID"},
		{name: "not found", path: path, cancel: fail(store.ErrNotFound), want: 404, errPart: "not found"},
		{name: "terminal", path: path, cancel: fail(store.ErrInvalidState), want: 409, errPart: "cannot be cancelled"},
		{name: "long tenant", path: path, hdr: map[string]string{"X-Tenant-ID": strings.Repeat("t", 256)}, want: 400, errPart: "X-Tenant-ID"},
		{name: "store error", path: path, cancel: fail(errors.New("pq: secret detail")), want: 500, errPart: "internal error"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs := &fakeStore{cancel: tc.cancel}
			rec := do(NewServer(fs, quiet()), "POST", tc.path, "", tc.hdr)
			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d: %s", rec.Code, tc.want, rec.Body)
			}
			if tc.want == 200 {
				var j store.Job
				if err := json.Unmarshal(rec.Body.Bytes(), &j); err != nil || j.ID != id || j.Status != "cancelled" || fs.gotTen != tc.tenant {
					t.Errorf("job %+v tenant %q err %v", j, fs.gotTen, err)
				}
				return
			}
			var e struct{ Error string }
			if err := json.Unmarshal(rec.Body.Bytes(), &e); err != nil || e.Error == "" {
				t.Fatalf("want JSON error body, got %q", rec.Body)
			}
			if !strings.Contains(e.Error, tc.errPart) || strings.Contains(e.Error, "secret") {
				t.Errorf("error %q (want part %q)", e.Error, tc.errPart)
			}
		})
	}
}

func TestHealth(t *testing.T) {
	down := &fakeStore{pingErr: errors.New("down")}
	if rec := do(NewServer(down, quiet()), "GET", "/healthz", "", nil); rec.Code != 200 {
		t.Errorf("healthz = %d", rec.Code)
	}
	if rec := do(NewServer(&fakeStore{}, quiet()), "GET", "/readyz", "", nil); rec.Code != 200 {
		t.Errorf("readyz ok = %d", rec.Code)
	}
	if rec := do(NewServer(down, quiet()), "GET", "/readyz", "", nil); rec.Code != 503 {
		t.Errorf("readyz down = %d", rec.Code)
	}
}

func TestRequestID(t *testing.T) {
	h := NewServer(&fakeStore{}, quiet())
	if got := do(h, "GET", "/healthz", "", map[string]string{"X-Request-ID": "req-1"}).Header().Get("X-Request-ID"); got != "req-1" {
		t.Errorf("echo = %q", got)
	}
	if got := do(h, "GET", "/healthz", "", nil).Header().Get("X-Request-ID"); got == "" {
		t.Error("no generated request id")
	}
}

func TestPanicRecovery(t *testing.T) {
	fs := &fakeStore{get: func(uuid.UUID) (store.Job, error) { panic("kaboom") }}
	rec := do(NewServer(fs, quiet()), "GET", "/v1/jobs/"+uuid.NewString(), "", nil)
	if rec.Code != 500 || !strings.Contains(rec.Body.String(), `"error"`) || strings.Contains(rec.Body.String(), "kaboom") {
		t.Errorf("got %d %s", rec.Code, rec.Body)
	}
	if rec.Header().Get("X-Request-ID") == "" {
		t.Error("request id lost on panic")
	}
}
