package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"taskq/internal/store"
)

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("needs docker")
	}
	ctx := context.Background()
	pg, err := postgres.Run(ctx, "postgres:17",
		postgres.WithDatabase("taskq"), postgres.WithUsername("taskq"), postgres.WithPassword("taskq"), postgres.BasicWaitStrategies())
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(pg) })
	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Migrate(dsn); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	srv := httptest.NewServer(NewServer(st, quiet()))
	t.Cleanup(srv.Close)

	submit := func(key string) store.Job {
		t.Helper()
		req, _ := http.NewRequest("POST", srv.URL+"/v1/jobs", strings.NewReader(`{"type":"email","payload":{"to":"a@b.c"},"priority":5}`))
		req.Header.Set("Idempotency-Key", key)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		var j store.Job
		if err := json.NewDecoder(resp.Body).Decode(&j); err != nil {
			t.Fatal(err)
		}
		return j
	}

	first := submit("k1")
	resp, err := http.Get(srv.URL + "/v1/jobs/" + first.ID.String())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var got store.Job
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	var payload map[string]string
	if err := json.Unmarshal(got.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 || got.ID != first.ID || got.Type != "email" || got.Status != "queued" ||
		got.Priority != 5 || got.TenantID != "default" || payload["to"] != "a@b.c" {
		t.Errorf("round trip mismatch: %d %+v", resp.StatusCode, got)
	}

	if again := submit("k1"); again.ID != first.ID {
		t.Errorf("replay id %v != %v", again.ID, first.ID)
	}
	if other := submit("k2"); other.ID == first.ID {
		t.Error("different key returned same job")
	}
}

func TestIntegrationListRequeueTenants(t *testing.T) {
	if testing.Short() {
		t.Skip("needs docker")
	}
	ctx := context.Background()
	pg, err := postgres.Run(ctx, "postgres:17",
		postgres.WithDatabase("taskq"), postgres.WithUsername("taskq"), postgres.WithPassword("taskq"), postgres.BasicWaitStrategies())
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(pg) })
	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Migrate(dsn); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	srv := httptest.NewServer(NewServer(st, quiet()))
	t.Cleanup(srv.Close)

	call := func(method, path, tenant string, body string, dst any) int {
		t.Helper()
		req, _ := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
		req.Header.Set("X-Tenant-ID", tenant)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if dst != nil {
			if err := json.NewDecoder(resp.Body).Decode(dst); err != nil {
				t.Fatalf("%s %s: decode: %v", method, path, err)
			}
		}
		return resp.StatusCode
	}
	submit := func(tenant, queue string) store.Job {
		t.Helper()
		var j store.Job
		if code := call("POST", "/v1/jobs", tenant, `{"type":"email","queue":"`+queue+`"}`, &j); code != http.StatusAccepted {
			t.Fatalf("submit status = %d", code)
		}
		return j
	}
	type page struct {
		Jobs       []store.Job `json:"jobs"`
		NextCursor *string     `json:"next_cursor"`
	}
	list := func(tenant, query string) page {
		t.Helper()
		var p page
		if code := call("GET", "/v1/jobs"+query, tenant, "", &p); code != 200 {
			t.Fatalf("list %q status = %d", query, code)
		}
		return p
	}

	// Tenant A: 4 jobs on queue "a" plus one on "dlq"; tenant B: one job.
	want := map[uuid.UUID]bool{}
	var queued store.Job
	for range 4 {
		queued = submit("A", "a")
		want[queued.ID] = true
	}
	dlq := submit("A", "dlq")
	want[dlq.ID] = true
	other := submit("B", "a")

	// Paging: 5 jobs, pages of 3 then 2.
	p1 := list("A", "?limit=3")
	if len(p1.Jobs) != 3 || p1.NextCursor == nil {
		t.Fatalf("page 1: %d jobs, next_cursor=%v", len(p1.Jobs), p1.NextCursor)
	}
	p2 := list("A", "?limit=3&cursor="+url.QueryEscape(*p1.NextCursor))
	if len(p2.Jobs) != 2 || p2.NextCursor != nil {
		t.Fatalf("page 2: %d jobs, next_cursor=%v", len(p2.Jobs), p2.NextCursor)
	}
	seen := map[uuid.UUID]bool{}
	for _, j := range append(p1.Jobs, p2.Jobs...) {
		if j.TenantID != "A" || !want[j.ID] || seen[j.ID] {
			t.Errorf("unexpected or duplicate job on pages: %+v", j)
		}
		seen[j.ID] = true
	}
	if len(seen) != 5 {
		t.Errorf("paged %d distinct jobs, want 5", len(seen))
	}
	if p := list("A", "?queue=dlq"); len(p.Jobs) != 1 || p.Jobs[0].ID != dlq.ID || p.NextCursor != nil {
		t.Errorf("queue filter: %+v", p)
	}
	if p := list("A", "?status=queued"); len(p.Jobs) != 5 {
		t.Errorf("status=queued: %d jobs", len(p.Jobs))
	}
	if p := list("A", "?status=dead"); p.Jobs == nil || len(p.Jobs) != 0 {
		t.Errorf("status=dead before failure should be an empty array: %+v", p)
	}
	if p := list("B", ""); len(p.Jobs) != 1 || p.Jobs[0].ID != other.ID {
		t.Errorf("tenant B sees %+v", p.Jobs)
	}

	// Drive the dlq job to dead via lease + permanent failure.
	leased, err := st.LeaseJobs(ctx, "dlq", "w1", 1, time.Minute)
	if err != nil || len(leased) != 1 || leased[0].ID != dlq.ID {
		t.Fatalf("lease: %v %+v", err, leased)
	}
	if err := st.FailJob(ctx, dlq.ID, *leased[0].LeaseToken, "boom", store.NoRetry); err != nil {
		t.Fatal(err)
	}
	if p := list("A", "?status=dead"); len(p.Jobs) != 1 || p.Jobs[0].ID != dlq.ID || p.Jobs[0].Attempts != 1 {
		t.Fatalf("dead list: %+v", p.Jobs)
	}

	// Cross-tenant access is 404.
	if code := call("GET", "/v1/jobs/"+dlq.ID.String(), "B", "", nil); code != 404 {
		t.Errorf("cross-tenant get = %d", code)
	}
	if code := call("POST", "/v1/jobs/"+dlq.ID.String()+"/requeue", "B", "", nil); code != 404 {
		t.Errorf("cross-tenant requeue = %d", code)
	}
	if code := call("GET", "/v1/jobs/"+dlq.ID.String(), "A", "", nil); code != 200 {
		t.Errorf("own get = %d", code)
	}
	if code := call("POST", "/v1/jobs/"+uuid.NewString()+"/requeue", "A", "", nil); code != 404 {
		t.Errorf("unknown requeue = %d", code)
	}

	// Requeue the dead job; requeueing a queued job conflicts.
	var rq store.Job
	if code := call("POST", "/v1/jobs/"+dlq.ID.String()+"/requeue", "A", "", &rq); code != 200 {
		t.Fatalf("requeue = %d", code)
	}
	if rq.ID != dlq.ID || rq.Status != "queued" || rq.Attempts != 0 {
		t.Errorf("requeued job: %+v", rq)
	}
	var e struct{ Error string }
	if code := call("POST", "/v1/jobs/"+dlq.ID.String()+"/requeue", "A", "", &e); code != 409 || e.Error == "" {
		t.Errorf("requeue of queued (just requeued) = %d %q", code, e.Error)
	}
	if code := call("POST", "/v1/jobs/"+queued.ID.String()+"/requeue", "A", "", nil); code != 409 {
		t.Errorf("requeue of queued = %d", code)
	}
}
