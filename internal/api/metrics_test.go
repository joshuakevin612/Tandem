package api

import (
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"taskq/internal/metrics"
)

// counterValue sums the named counter's series whose labels include want.
func counterValue(t *testing.T, reg *prometheus.Registry, name string, want map[string]string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var sum float64
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			have := map[string]string{}
			for _, l := range m.GetLabel() {
				have[l.GetName()] = l.GetValue()
			}
			ok := true
			for k, v := range want {
				ok = ok && have[k] == v
			}
			if ok {
				sum += m.GetCounter().GetValue()
			}
		}
	}
	return sum
}

func seriesCount(t *testing.T, reg *prometheus.Registry, name string) int {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() == name {
			return len(mf.GetMetric())
		}
	}
	return 0
}

func TestRequestMetricsUseRoutePattern(t *testing.T) {
	reg := prometheus.NewRegistry()
	h := NewServer(&fakeStore{}, quiet(), WithDeps(Deps{Metrics: metrics.NewAPI(reg)}))

	for range 3 { // three different ids must collapse into one series
		do(h, "GET", "/v1/jobs/"+uuid.NewString(), "", nil)
	}
	do(h, "POST", "/v1/jobs", `{"type":"x","queue":"q1"}`, nil)
	do(h, "GET", "/no/such/path/"+uuid.NewString(), "", nil)

	if got := counterValue(t, reg, "taskq_http_requests_total", map[string]string{"route": "/v1/jobs/{id}", "method": "GET", "code": "200"}); got != 3 {
		t.Errorf("GET /v1/jobs/{id} = %v, want 3", got)
	}
	if got := counterValue(t, reg, "taskq_http_requests_total", map[string]string{"route": "/v1/jobs", "method": "POST", "code": "202"}); got != 1 {
		t.Errorf("POST /v1/jobs = %v, want 1", got)
	}
	if got := counterValue(t, reg, "taskq_http_requests_total", map[string]string{"route": "unmatched", "code": "404"}); got != 1 {
		t.Errorf("unmatched = %v, want 1", got)
	}
	if got := counterValue(t, reg, "taskq_jobs_submitted_total", map[string]string{"queue": ""}); got != 1 {
		// the fake store echoes no queue, so the label is empty
		t.Errorf("jobs_submitted = %v, want 1", got)
	}
	// 3 GETs + 1 POST + 1 unmatched = exactly 3 request series, no raw paths.
	if n := seriesCount(t, reg, "taskq_http_requests_total"); n != 3 {
		t.Errorf("request series = %d, want 3", n)
	}
	if n := seriesCount(t, reg, "taskq_http_request_duration_seconds"); n != 3 {
		t.Errorf("duration series = %d, want 3", n)
	}
}

func TestRateLimitedAndCacheMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	h := NewServer(&fakeStore{}, quiet(), WithDeps(Deps{
		Metrics: metrics.NewAPI(reg),
		Limiter: &fakeLimiter{retry: 1},
		Cache:   &fakeCache{},
	}))
	do(h, "POST", "/v1/jobs", `{"type":"x"}`, nil)
	do(h, "GET", "/v1/jobs/"+uuid.NewString(), "", nil)
	if got := counterValue(t, reg, "taskq_rate_limited_total", nil); got != 1 {
		t.Errorf("rate_limited = %v, want 1", got)
	}
	if got := counterValue(t, reg, "taskq_status_cache_requests_total", map[string]string{"result": "miss"}); got != 1 {
		t.Errorf("cache miss = %v, want 1", got)
	}
}

func TestMetricsEndpointOutsideAuth(t *testing.T) {
	reg := prometheus.NewRegistry()
	deny := func(http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusUnauthorized) })
	}
	h := NewServer(&fakeStore{}, quiet(), WithDeps(Deps{Auth: deny, Metrics: metrics.NewAPI(reg), MetricsHandler: metrics.Handler(reg)}))
	if rec := do(h, "GET", "/v1/jobs", "", nil); rec.Code != http.StatusUnauthorized {
		t.Fatalf("auth not enforced: %d", rec.Code)
	}
	rec := do(h, "GET", "/metrics", "", nil)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "taskq_http_requests_total") {
		t.Fatalf("/metrics = %d: %.200s", rec.Code, rec.Body)
	}
}
