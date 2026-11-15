package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"taskq/internal/store"
)

// apiEnv is a real-Postgres store behind a running API server.
type apiEnv struct {
	ctx context.Context
	st  *store.Store
	raw *pgxpool.Pool
	srv *httptest.Server
}

func newAPIEnv(t *testing.T) *apiEnv {
	t.Helper()
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
	raw, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(raw.Close)
	srv := httptest.NewServer(NewServer(st, quiet()))
	t.Cleanup(srv.Close)
	return &apiEnv{ctx: ctx, st: st, raw: raw, srv: srv}
}

func (e *apiEnv) call(t *testing.T, method, path, tenant, body string, dst any) (int, http.Header) {
	t.Helper()
	req, _ := http.NewRequest(method, e.srv.URL+path, strings.NewReader(body))
	req.Header.Set("X-Tenant-ID", tenant)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if dst != nil && resp.StatusCode != http.StatusNoContent {
		if err := json.NewDecoder(resp.Body).Decode(dst); err != nil {
			t.Fatalf("%s %s: decode: %v", method, path, err)
		}
	}
	return resp.StatusCode, resp.Header
}

func TestIntegrationSchedules(t *testing.T) {
	e := newAPIEnv(t)
	type errResp struct{ Error string }

	// Create.
	var a store.Schedule
	code, hdr := e.call(t, "POST", "/v1/schedules", "A",
		`{"name":"nightly","cron":"0 3 * * *","timezone":"Europe/Berlin","type":"report","queue":"q","payload":{"x":1},"priority":3,"max_attempts":7}`, &a)
	if code != 201 || hdr.Get("Location") != "/v1/schedules/"+a.ID.String() {
		t.Fatalf("create = %d, Location %q", code, hdr.Get("Location"))
	}
	if a.TenantID != "A" || a.Name != "nightly" || a.Timezone != "Europe/Berlin" || a.Queue != "q" || a.Type != "report" ||
		a.Priority != 3 || a.MaxAttempts != 7 || !a.Enabled || !jsonEqual(a.Payload, `{"x":1}`) {
		t.Errorf("created schedule: %+v", a)
	}
	if !a.NextFireAt.After(time.Now()) || a.NextFireAt.After(time.Now().Add(25*time.Hour)) {
		t.Errorf("next_fire_at = %v", a.NextFireAt)
	}
	if h := a.NextFireAt.In(mustLoc(t, "Europe/Berlin")).Hour(); h != 3 {
		t.Errorf("next_fire_at is at %d:00 Berlin, want 03:00", h)
	}

	// Defaults.
	var d store.Schedule
	if code, _ := e.call(t, "POST", "/v1/schedules", "A", `{"name":"min","cron":"@hourly","type":"t"}`, &d); code != 201 {
		t.Fatalf("create minimal = %d", code)
	}
	if d.Timezone != "UTC" || d.Queue != "default" || d.MaxAttempts != 5 {
		t.Errorf("defaults: %+v", d)
	}

	// Duplicate name 409 (same tenant), allowed for another tenant.
	var er errResp
	if code, _ := e.call(t, "POST", "/v1/schedules", "A", `{"name":"nightly","cron":"* * * * *","type":"t"}`, &er); code != 409 || er.Error == "" {
		t.Errorf("duplicate = %d %q", code, er.Error)
	}
	if code, _ := e.call(t, "POST", "/v1/schedules", "B", `{"name":"nightly","cron":"* * * * *","type":"t"}`, nil); code != 201 {
		t.Errorf("same name other tenant = %d", code)
	}

	// Validation errors.
	er = errResp{}
	if code, _ := e.call(t, "POST", "/v1/schedules", "A", `{"name":"x","cron":"not a cron","type":"t"}`, &er); code != 400 || er.Error == "" {
		t.Errorf("bad cron = %d %q", code, er.Error)
	}
	er = errResp{}
	if code, _ := e.call(t, "POST", "/v1/schedules", "A", `{"name":"x","cron":"* * * * *","type":"t","timezone":"Nowhere/Land"}`, &er); code != 400 || !strings.Contains(er.Error, "time zone") {
		t.Errorf("bad timezone = %d %q", code, er.Error)
	}

	// List: tenant scoped, ordered by name.
	var l struct {
		Schedules []store.Schedule `json:"schedules"`
	}
	if code, _ := e.call(t, "GET", "/v1/schedules", "A", "", &l); code != 200 || len(l.Schedules) != 2 ||
		l.Schedules[0].Name != "min" || l.Schedules[1].Name != "nightly" {
		t.Errorf("list A = %d %+v", code, l.Schedules)
	}
	l.Schedules = nil
	if code, _ := e.call(t, "GET", "/v1/schedules", "empty", "", &l); code != 200 || l.Schedules == nil || len(l.Schedules) != 0 {
		t.Errorf("list empty tenant = %d %+v", code, l.Schedules)
	}

	// Get.
	var got store.Schedule
	if code, _ := e.call(t, "GET", "/v1/schedules/"+a.ID.String(), "A", "", &got); code != 200 || got.ID != a.ID {
		t.Errorf("get = %d %+v", code, got)
	}

	// Cross-tenant 404 everywhere.
	for _, c := range []struct{ method, suffix string }{
		{"GET", ""}, {"DELETE", ""}, {"POST", "/pause"}, {"POST", "/resume"},
	} {
		if code, _ := e.call(t, c.method, "/v1/schedules/"+a.ID.String()+c.suffix, "intruder", "", nil); code != 404 {
			t.Errorf("cross-tenant %s %s = %d", c.method, c.suffix, code)
		}
	}
	if code, _ := e.call(t, "GET", "/v1/schedules/"+uuid.NewString(), "A", "", nil); code != 404 {
		t.Errorf("unknown get = %d", code)
	}
	// ... and the schedule is untouched.
	if code, _ := e.call(t, "GET", "/v1/schedules/"+a.ID.String(), "A", "", &got); code != 200 || !got.Enabled {
		t.Errorf("schedule changed by intruder: %d %+v", code, got)
	}

	// Pause keeps next_fire_at.
	var p store.Schedule
	if code, _ := e.call(t, "POST", "/v1/schedules/"+a.ID.String()+"/pause", "A", "", &p); code != 200 || p.Enabled || !p.NextFireAt.Equal(a.NextFireAt) {
		t.Errorf("pause = %d %+v (next was %v)", code, p, a.NextFireAt)
	}

	// Make next_fire_at stale, then resume must recompute it into the future.
	stale := time.Now().Add(-72 * time.Hour)
	if _, err := e.raw.Exec(e.ctx, `UPDATE schedules SET next_fire_at=$2 WHERE id=$1`, a.ID, stale); err != nil {
		t.Fatal(err)
	}
	var r store.Schedule
	if code, _ := e.call(t, "POST", "/v1/schedules/"+a.ID.String()+"/resume", "A", "", &r); code != 200 || !r.Enabled {
		t.Fatalf("resume = %d %+v", code, r)
	}
	if !r.NextFireAt.After(time.Now()) || r.NextFireAt.After(time.Now().Add(25*time.Hour)) || r.NextFireAt.In(mustLoc(t, "Europe/Berlin")).Hour() != 3 {
		t.Errorf("resume next_fire_at = %v, want upcoming 03:00 Berlin", r.NextFireAt)
	}
	if code, _ := e.call(t, "GET", "/v1/schedules/"+a.ID.String(), "A", "", &got); code != 200 || !got.Enabled || !got.NextFireAt.Equal(r.NextFireAt) {
		t.Errorf("persisted after resume: %d %+v", code, got)
	}

	// Delete.
	if code, _ := e.call(t, "DELETE", "/v1/schedules/"+a.ID.String(), "A", "", nil); code != 204 {
		t.Errorf("delete = %d", code)
	}
	if code, _ := e.call(t, "GET", "/v1/schedules/"+a.ID.String(), "A", "", nil); code != 404 {
		t.Errorf("get after delete = %d", code)
	}
	if code, _ := e.call(t, "DELETE", "/v1/schedules/"+a.ID.String(), "A", "", nil); code != 404 {
		t.Errorf("second delete = %d", code)
	}
	if code, _ := e.call(t, "POST", "/v1/schedules/"+a.ID.String()+"/pause", "A", "", nil); code != 404 {
		t.Errorf("pause after delete = %d", code)
	}
}

func mustLoc(t *testing.T, name string) *time.Location {
	t.Helper()
	l, err := time.LoadLocation(name)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func TestIntegrationCancel(t *testing.T) {
	e := newAPIEnv(t)
	var j store.Job
	if code, _ := e.call(t, "POST", "/v1/jobs", "A", `{"type":"email"}`, &j); code != http.StatusAccepted {
		t.Fatalf("submit = %d", code)
	}
	path := "/v1/jobs/" + j.ID.String() + "/cancel"

	if code, _ := e.call(t, "POST", path, "intruder", "", nil); code != 404 {
		t.Errorf("cross-tenant cancel = %d", code)
	}
	if code, _ := e.call(t, "POST", "/v1/jobs/"+uuid.NewString()+"/cancel", "A", "", nil); code != 404 {
		t.Errorf("unknown cancel = %d", code)
	}
	if code, _ := e.call(t, "POST", "/v1/jobs/nope/cancel", "A", "", nil); code != 400 {
		t.Errorf("bad id cancel = %d", code)
	}

	var c store.Job
	if code, _ := e.call(t, "POST", path, "A", "", &c); code != 200 || c.ID != j.ID || c.Status != "cancelled" || c.FinishedAt == nil {
		t.Fatalf("cancel = %d %+v", code, c)
	}
	var er struct{ Error string }
	if code, _ := e.call(t, "POST", path, "A", "", &er); code != 409 || er.Error == "" {
		t.Errorf("second cancel = %d %q", code, er.Error)
	}
	var got store.Job
	if code, _ := e.call(t, "GET", "/v1/jobs/"+j.ID.String(), "A", "", &got); code != 200 || got.Status != "cancelled" {
		t.Errorf("get after cancel = %d %+v", code, got)
	}
	// A cancelled job is never leased.
	if leased, err := e.st.LeaseJobs(e.ctx, "default", "w", 10, time.Minute); err != nil || len(leased) != 0 {
		t.Errorf("cancelled job leased: %v %v", leased, err)
	}
}

// TestIntegrationPagingSharedCreatedAt pages over jobs whose created_at is
// identical: a timestamp-only cursor would skip or repeat them.
func TestIntegrationPagingSharedCreatedAt(t *testing.T) {
	e := newAPIEnv(t)
	const total = 25
	for range total {
		var j store.Job
		if code, _ := e.call(t, "POST", "/v1/jobs", "A", `{"type":"t"}`, &j); code != http.StatusAccepted {
			t.Fatalf("submit = %d", code)
		}
	}
	if _, err := e.raw.Exec(e.ctx, `UPDATE jobs SET created_at = '2030-01-02 03:04:05.123456+00' WHERE tenant_id = 'A'`); err != nil {
		t.Fatal(err)
	}
	var tail store.Job
	if code, _ := e.call(t, "POST", "/v1/jobs", "B", `{"type":"t"}`, &tail); code != http.StatusAccepted {
		t.Fatal(code)
	}

	type page struct {
		Jobs       []store.Job `json:"jobs"`
		NextCursor *string     `json:"next_cursor"`
	}
	seen := map[uuid.UUID]bool{}
	query := "?limit=10"
	pages := 0
	for {
		var p page
		if code, _ := e.call(t, "GET", "/v1/jobs"+query, "A", "", &p); code != 200 {
			t.Fatalf("list %q = %d", query, code)
		}
		pages++
		for _, j := range p.Jobs {
			if seen[j.ID] {
				t.Fatalf("job %s seen twice", j.ID)
			}
			seen[j.ID] = true
		}
		if p.NextCursor == nil {
			if len(p.Jobs) == 10 {
				t.Fatal("full last page must carry a cursor")
			}
			break
		}
		if pages > 5 {
			t.Fatal("too many pages")
		}
		query = "?limit=10&cursor=" + url.QueryEscape(*p.NextCursor)
	}
	if len(seen) != total || pages != 3 {
		t.Errorf("saw %d jobs over %d pages, want %d over 3", len(seen), pages, total)
	}

	// Malformed cursors are rejected.
	for _, c := range []string{"!!!", "bm9wZQ", "2030-01-02T03:04:05Z"} {
		var er struct{ Error string }
		if code, _ := e.call(t, "GET", "/v1/jobs?cursor="+url.QueryEscape(c), "A", "", &er); code != 400 || er.Error == "" {
			t.Errorf("cursor %q = %d %q", c, code, er.Error)
		}
	}
}

func jsonEqual(raw json.RawMessage, want string) bool {
	var a, b any
	return json.Unmarshal(raw, &a) == nil && json.Unmarshal([]byte(want), &b) == nil && reflect.DeepEqual(a, b)
}
