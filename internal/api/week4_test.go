package api

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"

	"taskq/internal/store"
)

type fakeLimiter struct {
	ok    bool
	retry time.Duration
	err   error
	calls int
}

func (f *fakeLimiter) Allow(context.Context, string) (bool, time.Duration, error) {
	f.calls++
	return f.ok, f.retry, f.err
}

func TestRateLimit(t *testing.T) {
	tests := []struct {
		name      string
		l         *fakeLimiter
		body      string
		want      int
		retry     string
		wantStore bool
	}{
		{"allow", &fakeLimiter{ok: true}, `{"type":"x"}`, 202, "", true},
		{"deny rounds up", &fakeLimiter{retry: 1200 * time.Millisecond}, `{"type":"x"}`, 429, "2", false},
		{"deny min 1s", &fakeLimiter{retry: 5 * time.Millisecond}, `{"type":"x"}`, 429, "1", false},
		{"deny before body decode", &fakeLimiter{}, `{bad`, 429, "1", false},
		{"error fails open", &fakeLimiter{err: errors.New("redis down")}, `{"type":"x"}`, 202, "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			created := false
			fs := &fakeStore{create: func(n store.NewJob) (store.Job, error) {
				created = true
				return store.Job{ID: uuid.New(), TenantID: n.TenantID, Status: "queued"}, nil
			}}
			h := NewServer(fs, quiet(), WithDeps(Deps{Limiter: tc.l}))
			rec := do(h, "POST", "/v1/jobs", tc.body, nil)
			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d: %s", rec.Code, tc.want, rec.Body)
			}
			if got := rec.Header().Get("Retry-After"); got != tc.retry {
				t.Errorf("Retry-After = %q, want %q", got, tc.retry)
			}
			if created != tc.wantStore {
				t.Errorf("store called = %v", created)
			}
			if tc.want == 429 && rec.Body.String() != "{\"error\":\"rate limit exceeded\"}\n" {
				t.Errorf("body = %q", rec.Body)
			}
		})
	}
}

func TestRateLimitOnlyOnCreate(t *testing.T) {
	l := &fakeLimiter{}
	h := NewServer(&fakeStore{}, quiet(), WithDeps(Deps{Limiter: l}))
	for _, p := range []string{"/v1/jobs", "/v1/jobs/" + uuid.NewString()} {
		if rec := do(h, "GET", p, "", nil); rec.Code != 200 {
			t.Fatalf("%s = %d", p, rec.Code)
		}
	}
	if l.calls != 0 {
		t.Fatalf("limiter called %d times on GETs", l.calls)
	}
}

type fakeCache struct {
	jobs map[uuid.UUID]store.Job
	sets []store.Job
	dels []uuid.UUID
}

func (f *fakeCache) Get(_ context.Context, id uuid.UUID) (store.Job, bool) {
	j, ok := f.jobs[id]
	return j, ok
}
func (f *fakeCache) Set(_ context.Context, j store.Job) { f.sets = append(f.sets, j) }
func (f *fakeCache) Del(_ context.Context, id uuid.UUID) {
	f.dels = append(f.dels, id)
	delete(f.jobs, id)
}

func cacheServer(fs *fakeStore, c *fakeCache) http.Handler {
	return NewServer(fs, quiet(), WithDeps(Deps{Cache: c}))
}

func TestGetJobCacheHit(t *testing.T) {
	id := uuid.New()
	c := &fakeCache{jobs: map[uuid.UUID]store.Job{id: {ID: id, TenantID: "default", Status: "succeeded"}}}
	fs := &fakeStore{get: func(uuid.UUID) (store.Job, error) { t.Error("store read on cache hit"); return store.Job{}, nil }}
	rec := do(cacheServer(fs, c), "GET", "/v1/jobs/"+id.String(), "", nil)
	if rec.Code != 200 {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestGetJobCacheHitCrossTenant(t *testing.T) {
	id := uuid.New()
	c := &fakeCache{jobs: map[uuid.UUID]store.Job{id: {ID: id, TenantID: "other", Status: "succeeded"}}}
	rec := do(cacheServer(&fakeStore{}, c), "GET", "/v1/jobs/"+id.String(), "", nil)
	if rec.Code != 404 {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestGetJobMissPopulates(t *testing.T) {
	id := uuid.New()
	c := &fakeCache{}
	fs := &fakeStore{get: func(id uuid.UUID) (store.Job, error) {
		return store.Job{ID: id, TenantID: "default", Status: "dead"}, nil
	}}
	if rec := do(cacheServer(fs, c), "GET", "/v1/jobs/"+id.String(), "", nil); rec.Code != 200 {
		t.Fatalf("status = %d", rec.Code)
	}
	if len(c.sets) != 1 || c.sets[0].ID != id {
		t.Fatalf("sets = %+v", c.sets)
	}
}

func TestGetJobCrossTenantMissNotCached(t *testing.T) {
	id := uuid.New()
	c := &fakeCache{}
	fs := &fakeStore{get: func(id uuid.UUID) (store.Job, error) {
		return store.Job{ID: id, TenantID: "other", Status: "dead"}, nil
	}}
	if rec := do(cacheServer(fs, c), "GET", "/v1/jobs/"+id.String(), "", nil); rec.Code != 404 {
		t.Fatalf("status = %d", rec.Code)
	}
	if len(c.sets) != 0 {
		t.Fatalf("cross-tenant job was cached: %+v", c.sets)
	}
}

func TestRequeueAndCancelDelCache(t *testing.T) {
	for _, op := range []string{"requeue", "cancel"} {
		t.Run(op, func(t *testing.T) {
			id := uuid.New()
			c := &fakeCache{jobs: map[uuid.UUID]store.Job{id: {ID: id, TenantID: "default", Status: "dead"}}}
			rec := do(cacheServer(&fakeStore{}, c), "POST", "/v1/jobs/"+id.String()+"/"+op, "", nil)
			if rec.Code != 200 {
				t.Fatalf("status = %d: %s", rec.Code, rec.Body)
			}
			if len(c.dels) != 1 || c.dels[0] != id {
				t.Fatalf("dels = %v", c.dels)
			}
		})
	}
}

func TestRequeueFailureDoesNotDel(t *testing.T) {
	id := uuid.New()
	c := &fakeCache{}
	fs := &fakeStore{requeue: func(string, uuid.UUID) (store.Job, error) { return store.Job{}, store.ErrInvalidState }}
	if rec := do(cacheServer(fs, c), "POST", "/v1/jobs/"+id.String()+"/requeue", "", nil); rec.Code != 409 {
		t.Fatalf("status = %d", rec.Code)
	}
	if len(c.dels) != 0 {
		t.Fatalf("dels = %v", c.dels)
	}
}

type fakeNotifier struct{ queues []string }

func (f *fakeNotifier) Notify(_ context.Context, q string) { f.queues = append(f.queues, q) }

func TestCreateJobNotifies(t *testing.T) {
	future := time.Now().Add(time.Hour)
	tests := []struct {
		name string
		job  store.Job
		want int
	}{
		{"runnable now", store.Job{ID: uuid.New(), Queue: "emails", RunAt: time.Now().Add(-time.Second)}, 1},
		{"future run_at", store.Job{ID: uuid.New(), Queue: "emails", RunAt: future}, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			n := &fakeNotifier{}
			fs := &fakeStore{create: func(store.NewJob) (store.Job, error) { return tc.job, nil }}
			h := NewServer(fs, quiet(), WithDeps(Deps{Notifier: n}))
			rec := do(h, "POST", "/v1/jobs", `{"type":"x"}`, nil)
			if rec.Code != 202 {
				t.Fatalf("status = %d", rec.Code)
			}
			if len(n.queues) != tc.want || (tc.want == 1 && n.queues[0] != "emails") {
				t.Fatalf("notified %v, want %d", n.queues, tc.want)
			}
		})
	}
}
