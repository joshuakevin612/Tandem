package api

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"tandem/internal/store"
)

func (f *fakeStore) CreateSchedule(_ context.Context, n store.NewSchedule) (store.Schedule, error) {
	f.gotNewSched = n
	if f.schedCreate != nil {
		return f.schedCreate(n)
	}
	return store.Schedule{ID: uuid.New(), TenantID: n.TenantID, Name: n.Name, Cron: n.Cron, Timezone: n.Timezone,
		Type: n.Type, Enabled: true, NextFireAt: n.NextFireAt}, nil
}

func (f *fakeStore) GetSchedule(_ context.Context, tenantID string, id uuid.UUID) (store.Schedule, error) {
	f.gotTen = tenantID
	if f.schedGet != nil {
		return f.schedGet(tenantID, id)
	}
	return store.Schedule{ID: id, TenantID: tenantID, Cron: "0 9 * * *", Timezone: "UTC", Enabled: true,
		NextFireAt: time.Date(2020, 1, 1, 9, 0, 0, 0, time.UTC)}, nil
}

func (f *fakeStore) ListSchedules(_ context.Context, tenantID string) ([]store.Schedule, error) {
	f.gotTen = tenantID
	if f.schedList != nil {
		return f.schedList(tenantID)
	}
	return nil, nil
}

func (f *fakeStore) DeleteSchedule(_ context.Context, tenantID string, id uuid.UUID) error {
	f.gotTen = tenantID
	if f.schedDelete != nil {
		return f.schedDelete(tenantID, id)
	}
	return nil
}

func (f *fakeStore) SetScheduleEnabled(_ context.Context, tenantID string, id uuid.UUID, enabled bool, next time.Time) (store.Schedule, error) {
	f.gotTen, f.gotEnabled, f.gotNext = tenantID, &enabled, next
	if f.schedSet != nil {
		return f.schedSet(tenantID, id, enabled, next)
	}
	return store.Schedule{ID: id, TenantID: tenantID, Enabled: enabled, NextFireAt: next}, nil
}

func errBody(t *testing.T, body string, wantPart string) {
	t.Helper()
	var e struct{ Error string }
	if err := json.Unmarshal([]byte(body), &e); err != nil || e.Error == "" {
		t.Fatalf("want JSON error body, got %q", body)
	}
	if !strings.Contains(e.Error, wantPart) || strings.Contains(e.Error, "secret") {
		t.Errorf("error %q (want part %q)", e.Error, wantPart)
	}
}

func TestCreateSchedule(t *testing.T) {
	long := strings.Repeat("a", 256)
	tests := []struct {
		name    string
		body    string
		hdr     map[string]string
		store   func(store.NewSchedule) (store.Schedule, error)
		want    int
		errPart string
	}{
		{name: "minimal", body: `{"name":"n","cron":"*/5 * * * *","type":"t"}`, want: 201},
		{name: "full", body: `{"name":"n","cron":"0 9 * * 1-5","timezone":"America/New_York","type":"t","queue":"q","payload":{"a":1},"priority":-100,"max_attempts":25}`, want: 201},
		{name: "descriptor", body: `{"name":"n","cron":"@every 10s","type":"t"}`, want: 201},
		{name: "missing name", body: `{"cron":"* * * * *","type":"t"}`, want: 400, errPart: "name is required"},
		{name: "missing cron", body: `{"name":"n","type":"t"}`, want: 400, errPart: "cron is required"},
		{name: "missing type", body: `{"name":"n","cron":"* * * * *"}`, want: 400, errPart: "type is required"},
		{name: "long name", body: `{"name":"` + long + `","cron":"* * * * *","type":"t"}`, want: 400, errPart: "255"},
		{name: "long queue", body: `{"name":"n","cron":"* * * * *","type":"t","queue":"` + long + `"}`, want: 400, errPart: "255"},
		{name: "bad cron", body: `{"name":"n","cron":"nope","type":"t"}`, want: 400, errPart: "cronexpr"},
		{name: "bad timezone", body: `{"name":"n","cron":"* * * * *","type":"t","timezone":"Mars/Base"}`, want: 400, errPart: "time zone"},
		{name: "embedded tz", body: `{"name":"n","cron":"TZ=UTC * * * * *","type":"t"}`, want: 400, errPart: "time zone"},
		{name: "never fires", body: `{"name":"n","cron":"0 0 30 2 *","type":"t"}`, want: 400, errPart: "never fires"},
		{name: "priority high", body: `{"name":"n","cron":"* * * * *","type":"t","priority":101}`, want: 400, errPart: "priority"},
		{name: "attempts zero", body: `{"name":"n","cron":"* * * * *","type":"t","max_attempts":0}`, want: 400, errPart: "max_attempts"},
		{name: "attempts high", body: `{"name":"n","cron":"* * * * *","type":"t","max_attempts":26}`, want: 400, errPart: "max_attempts"},
		{name: "unknown field", body: `{"name":"n","cron":"* * * * *","type":"t","bogus":1}`, want: 400},
		{name: "malformed", body: `{"name":`, want: 400},
		{name: "empty body", body: ``, want: 400},
		{name: "long tenant", body: `{"name":"n","cron":"* * * * *","type":"t"}`, hdr: map[string]string{"X-Tenant-ID": strings.Repeat("t", 256)}, want: 400, errPart: "X-Tenant-ID"},
		{name: "duplicate", body: `{"name":"n","cron":"* * * * *","type":"t"}`, want: 409, errPart: "already exists",
			store: func(store.NewSchedule) (store.Schedule, error) { return store.Schedule{}, store.ErrConflict }},
		{name: "store error", body: `{"name":"n","cron":"* * * * *","type":"t"}`, want: 500, errPart: "internal error",
			store: func(store.NewSchedule) (store.Schedule, error) {
				return store.Schedule{}, errors.New("pq: secret detail")
			}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs := &fakeStore{schedCreate: tc.store}
			rec := do(NewServer(fs, quiet()), "POST", "/v1/schedules", tc.body, tc.hdr)
			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d: %s", rec.Code, tc.want, rec.Body)
			}
			if tc.want == 201 {
				var sc store.Schedule
				if err := json.Unmarshal(rec.Body.Bytes(), &sc); err != nil {
					t.Fatal(err)
				}
				if loc := rec.Header().Get("Location"); loc != "/v1/schedules/"+sc.ID.String() {
					t.Errorf("Location = %q", loc)
				}
				return
			}
			errBody(t, rec.Body.String(), tc.errPart)
		})
	}
}

func TestCreateScheduleFields(t *testing.T) {
	fs := &fakeStore{}
	h := NewServer(fs, quiet())
	before := time.Now()
	body := `{"name":"n","cron":"*/5 * * * *","type":"t","queue":"q","payload":{"a":1},"priority":7,"max_attempts":3}`
	if rec := do(h, "POST", "/v1/schedules", body, map[string]string{"X-Tenant-ID": "acme"}); rec.Code != 201 {
		t.Fatal(rec.Code, rec.Body)
	}
	n := fs.gotNewSched
	if n.TenantID != "acme" || n.Name != "n" || n.Cron != "*/5 * * * *" || n.Timezone != "UTC" || n.Type != "t" || n.Queue != "q" ||
		n.Priority != 7 || n.MaxAttempts != 3 || string(n.Payload) != `{"a":1}` {
		t.Errorf("unexpected NewSchedule: %+v", n)
	}
	if !n.NextFireAt.After(before) || n.NextFireAt.After(before.Add(5*time.Minute+time.Second)) || n.NextFireAt.Minute()%5 != 0 || n.NextFireAt.Second() != 0 {
		t.Errorf("next_fire_at = %v (created at %v)", n.NextFireAt, before)
	}

	fs = &fakeStore{}
	do(NewServer(fs, quiet()), "POST", "/v1/schedules", `{"name":"n","cron":"* * * * *","type":"t","payload":null}`, nil)
	if fs.gotNewSched.TenantID != "default" || len(fs.gotNewSched.Payload) != 0 || fs.gotNewSched.MaxAttempts != 0 {
		t.Errorf("defaults not applied: %+v", fs.gotNewSched)
	}
}

func TestListSchedules(t *testing.T) {
	tests := []struct {
		name string
		list func(string) ([]store.Schedule, error)
		hdr  map[string]string
		want int
		n    int
	}{
		{name: "empty is array", want: 200},
		{name: "two", want: 200, n: 2, list: func(tn string) ([]store.Schedule, error) {
			return []store.Schedule{{ID: uuid.New(), TenantID: tn}, {ID: uuid.New(), TenantID: tn}}, nil
		}},
		{name: "long tenant", hdr: map[string]string{"X-Tenant-ID": strings.Repeat("t", 256)}, want: 400},
		{name: "store error", want: 500, list: func(string) ([]store.Schedule, error) { return nil, errors.New("pq: secret detail") }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := do(NewServer(&fakeStore{schedList: tc.list}, quiet()), "GET", "/v1/schedules", "", tc.hdr)
			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d: %s", rec.Code, tc.want, rec.Body)
			}
			if tc.want != 200 {
				errBody(t, rec.Body.String(), "")
				return
			}
			var raw map[string]json.RawMessage
			if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
				t.Fatal(err)
			}
			if string(raw["schedules"]) == "null" || raw["schedules"] == nil {
				t.Fatalf("schedules must be an array: %s", rec.Body)
			}
			var arr []store.Schedule
			if err := json.Unmarshal(raw["schedules"], &arr); err != nil || len(arr) != tc.n {
				t.Errorf("schedules = %d, want %d (%v)", len(arr), tc.n, err)
			}
		})
	}
}

func TestScheduleByID(t *testing.T) {
	id := uuid.New()
	base := "/v1/schedules/" + id.String()
	notFound := func(string, uuid.UUID) (store.Schedule, error) { return store.Schedule{}, store.ErrNotFound }
	boom := func(string, uuid.UUID) (store.Schedule, error) {
		return store.Schedule{}, errors.New("pq: secret detail")
	}
	setNotFound := func(string, uuid.UUID, bool, time.Time) (store.Schedule, error) {
		return store.Schedule{}, store.ErrNotFound
	}
	tests := []struct {
		name    string
		method  string
		path    string
		hdr     map[string]string
		get     func(string, uuid.UUID) (store.Schedule, error)
		del     func(string, uuid.UUID) error
		set     func(string, uuid.UUID, bool, time.Time) (store.Schedule, error)
		want    int
		errPart string
	}{
		{name: "get ok", method: "GET", path: base, want: 200},
		{name: "get bad id", method: "GET", path: "/v1/schedules/nope", want: 400, errPart: "UUID"},
		{name: "get not found", method: "GET", path: base, get: notFound, want: 404, errPart: "not found"},
		{name: "get error", method: "GET", path: base, get: boom, want: 500, errPart: "internal error"},
		{name: "delete ok", method: "DELETE", path: base, want: 204},
		{name: "delete bad id", method: "DELETE", path: "/v1/schedules/nope", want: 400, errPart: "UUID"},
		{name: "delete not found", method: "DELETE", path: base, want: 404, errPart: "not found",
			del: func(string, uuid.UUID) error { return store.ErrNotFound }},
		{name: "delete error", method: "DELETE", path: base, want: 500, errPart: "internal error",
			del: func(string, uuid.UUID) error { return errors.New("pq: secret detail") }},
		{name: "pause ok", method: "POST", path: base + "/pause", want: 200},
		{name: "pause bad id", method: "POST", path: "/v1/schedules/nope/pause", want: 400, errPart: "UUID"},
		{name: "pause not found", method: "POST", path: base + "/pause", get: notFound, want: 404, errPart: "not found"},
		{name: "pause get error", method: "POST", path: base + "/pause", get: boom, want: 500, errPart: "internal error"},
		{name: "pause set not found", method: "POST", path: base + "/pause", want: 404, errPart: "not found", set: setNotFound},
		{name: "pause set error", method: "POST", path: base + "/pause", want: 500, errPart: "internal error",
			set: func(string, uuid.UUID, bool, time.Time) (store.Schedule, error) {
				return store.Schedule{}, errors.New("pq: secret detail")
			}},
		{name: "resume ok", method: "POST", path: base + "/resume", want: 200},
		{name: "resume bad id", method: "POST", path: "/v1/schedules/nope/resume", want: 400, errPart: "UUID"},
		{name: "resume not found", method: "POST", path: base + "/resume", get: notFound, want: 404, errPart: "not found"},
		{name: "resume get error", method: "POST", path: base + "/resume", get: boom, want: 500, errPart: "internal error"},
		{name: "resume stored cron unparsable", method: "POST", path: base + "/resume", want: 500, errPart: "internal error",
			get: func(tn string, id uuid.UUID) (store.Schedule, error) {
				return store.Schedule{ID: id, TenantID: tn, Cron: "garbage", Timezone: "UTC"}, nil
			}},
		{name: "resume set not found", method: "POST", path: base + "/resume", want: 404, errPart: "not found", set: setNotFound},
		{name: "long tenant", method: "GET", path: base, hdr: map[string]string{"X-Tenant-ID": strings.Repeat("t", 256)}, want: 400, errPart: "X-Tenant-ID"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs := &fakeStore{schedGet: tc.get, schedDelete: tc.del, schedSet: tc.set}
			rec := do(NewServer(fs, quiet()), tc.method, tc.path, "", tc.hdr)
			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d: %s", rec.Code, tc.want, rec.Body)
			}
			if tc.want == 200 {
				var sc store.Schedule
				if err := json.Unmarshal(rec.Body.Bytes(), &sc); err != nil || sc.ID != id {
					t.Errorf("schedule %+v err %v", sc, err)
				}
			}
			if tc.want == 204 && rec.Body.Len() != 0 {
				t.Errorf("204 with body %q", rec.Body)
			}
			if tc.want >= 400 {
				errBody(t, rec.Body.String(), tc.errPart)
			}
		})
	}
}

func TestPauseResumeNextFire(t *testing.T) {
	id := uuid.New()
	stale := time.Date(2020, 1, 1, 9, 0, 0, 0, time.UTC)
	get := func(tn string, id uuid.UUID) (store.Schedule, error) {
		return store.Schedule{ID: id, TenantID: tn, Cron: "0 9 * * *", Timezone: "UTC", NextFireAt: stale}, nil
	}

	fs := &fakeStore{schedGet: get}
	if rec := do(NewServer(fs, quiet()), "POST", "/v1/schedules/"+id.String()+"/pause", "", map[string]string{"X-Tenant-ID": "acme"}); rec.Code != 200 {
		t.Fatal(rec.Code, rec.Body)
	}
	if fs.gotEnabled == nil || *fs.gotEnabled || !fs.gotNext.Equal(stale) || fs.gotTen != "acme" {
		t.Errorf("pause: enabled=%v next=%v tenant=%q", fs.gotEnabled, fs.gotNext, fs.gotTen)
	}

	fs = &fakeStore{schedGet: get}
	before := time.Now()
	rec := do(NewServer(fs, quiet()), "POST", "/v1/schedules/"+id.String()+"/resume", "", nil)
	if rec.Code != 200 {
		t.Fatal(rec.Code, rec.Body)
	}
	if fs.gotEnabled == nil || !*fs.gotEnabled {
		t.Fatal("resume must enable")
	}
	if !fs.gotNext.After(before) || fs.gotNext.After(before.Add(24*time.Hour+time.Second)) || fs.gotNext.Hour() != 9 || fs.gotNext.Minute() != 0 {
		t.Errorf("resume next_fire_at = %v, want next 09:00 UTC after %v", fs.gotNext, before)
	}
	var sc store.Schedule
	if err := json.Unmarshal(rec.Body.Bytes(), &sc); err != nil || !sc.Enabled || !sc.NextFireAt.Equal(fs.gotNext) {
		t.Errorf("response %+v err %v", sc, err)
	}
}
