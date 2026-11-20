// Package metrics defines tandem's Prometheus metrics.
//
// Each binary builds its own *prometheus.Registry (never the global default,
// so tests can construct many without re-registration collisions) and the
// handle structs below. Every method is safe on a nil receiver, so packages
// can take an optional *API / *Worker / *Scheduler and tests can omit it.
package metrics

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// NewRegistry returns a registry with the Go runtime and process collectors.
func NewRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return reg
}

// Handler serves reg in the Prometheus text format.
func Handler(reg *prometheus.Registry) http.Handler {
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
}

// Serve runs a /metrics HTTP server on addr until ctx is cancelled, then shuts
// it down gracefully. The returned func blocks until the server has stopped.
// A listen failure is logged, not fatal: metrics must never take a worker down.
func Serve(ctx context.Context, addr string, reg *prometheus.Registry, log *slog.Logger) (wait func()) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", Handler(reg))
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	done := make(chan struct{})
	go func() {
		defer close(done)
		errc := make(chan error, 1)
		go func() { errc <- srv.ListenAndServe() }()
		select {
		case err := <-errc:
			if !errors.Is(err, http.ErrServerClosed) {
				log.Error("metrics server", "addr", addr, "err", err)
			}
		case <-ctx.Done():
			shut, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := srv.Shutdown(shut); err != nil {
				log.Error("metrics shutdown", "err", err)
			}
		}
	}()
	return func() { <-done }
}

// API holds the HTTP API metrics.
type API struct {
	requests      *prometheus.CounterVec
	duration      *prometheus.HistogramVec
	submitted     *prometheus.CounterVec
	rateLimited   prometheus.Counter
	statusCacheRq *prometheus.CounterVec
}

func NewAPI(reg prometheus.Registerer) *API {
	f := promauto.With(reg)
	return &API{
		requests:      f.NewCounterVec(prometheus.CounterOpts{Name: "tandem_http_requests_total", Help: "HTTP requests by route pattern, method and status code."}, []string{"route", "method", "code"}),
		duration:      f.NewHistogramVec(prometheus.HistogramOpts{Name: "tandem_http_request_duration_seconds", Help: "HTTP request latency by route pattern and method.", Buckets: prometheus.DefBuckets}, []string{"route", "method"}),
		submitted:     f.NewCounterVec(prometheus.CounterOpts{Name: "tandem_jobs_submitted_total", Help: "Jobs accepted by POST /v1/jobs (idempotent replays included)."}, []string{"queue"}),
		rateLimited:   f.NewCounter(prometheus.CounterOpts{Name: "tandem_rate_limited_total", Help: "Submits rejected with 429."}),
		statusCacheRq: f.NewCounterVec(prometheus.CounterOpts{Name: "tandem_status_cache_requests_total", Help: "Job status cache lookups."}, []string{"result"}),
	}
}

// ObserveRequest records one finished request. route must be a route pattern.
func (m *API) ObserveRequest(route, method string, code int, d time.Duration) {
	if m == nil {
		return
	}
	m.requests.WithLabelValues(route, method, strconv.Itoa(code)).Inc()
	m.duration.WithLabelValues(route, method).Observe(d.Seconds())
}

func (m *API) JobSubmitted(queue string) {
	if m != nil {
		m.submitted.WithLabelValues(queue).Inc()
	}
}

func (m *API) RateLimited() {
	if m != nil {
		m.rateLimited.Inc()
	}
}

func (m *API) StatusCache(hit bool) {
	if m == nil {
		return
	}
	r := "miss"
	if hit {
		r = "hit"
	}
	m.statusCacheRq.WithLabelValues(r).Inc()
}

// Attempt outcomes the worker can observe.
const (
	OutcomeSucceeded   = "succeeded"
	OutcomeRetried     = "retried"
	OutcomeDead        = "dead"
	OutcomeLeaseLost   = "lease_lost"
	OutcomeAbandoned   = "abandoned"    // forced shutdown; left for the reaper
	OutcomeReportError = "report_error" // result could not be written
)

// Worker holds the worker pool metrics.
type Worker struct {
	leaseDuration *prometheus.HistogramVec
	leased        *prometheus.CounterVec
	attempt       *prometheus.HistogramVec
	inFlight      prometheus.Gauge
	lostLeases    prometheus.Counter
	wakeups       prometheus.Counter
}

func NewWorker(reg prometheus.Registerer) *Worker {
	f := promauto.With(reg)
	return &Worker{
		leaseDuration: f.NewHistogramVec(prometheus.HistogramOpts{Name: "tandem_lease_duration_seconds", Help: "Duration of the LeaseJobs call.", Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5}}, []string{"queue"}),
		leased:        f.NewCounterVec(prometheus.CounterOpts{Name: "tandem_jobs_leased_total", Help: "Jobs leased by this worker."}, []string{"queue"}),
		attempt:       f.NewHistogramVec(prometheus.HistogramOpts{Name: "tandem_attempt_duration_seconds", Help: "Handler run time per attempt by type and outcome.", Buckets: []float64{.01, .05, .1, .5, 1, 5, 15, 60, 300, 900}}, []string{"type", "outcome"}),
		inFlight:      f.NewGauge(prometheus.GaugeOpts{Name: "tandem_in_flight_jobs", Help: "Jobs currently held by this worker."}),
		lostLeases:    f.NewCounter(prometheus.CounterOpts{Name: "tandem_heartbeat_lost_leases_total", Help: "Leases found lost (reaped, cancelled or unconfirmable) by the heartbeat."}),
		wakeups:       f.NewCounter(prometheus.CounterOpts{Name: "tandem_wakeups_total", Help: "Idle polls cut short by a wake-up hint."}),
	}
}

func (m *Worker) ObserveLease(queue string, d time.Duration, leased int) {
	if m == nil {
		return
	}
	m.leaseDuration.WithLabelValues(queue).Observe(d.Seconds())
	if leased > 0 {
		m.leased.WithLabelValues(queue).Add(float64(leased))
	}
}

func (m *Worker) ObserveAttempt(jobType, outcome string, d time.Duration) {
	if m != nil {
		m.attempt.WithLabelValues(jobType, outcome).Observe(d.Seconds())
	}
}

func (m *Worker) InFlightInc() {
	if m != nil {
		m.inFlight.Inc()
	}
}

func (m *Worker) InFlightDec() {
	if m != nil {
		m.inFlight.Dec()
	}
}

func (m *Worker) LostLease() {
	if m != nil {
		m.lostLeases.Inc()
	}
}

func (m *Worker) Wakeup() {
	if m != nil {
		m.wakeups.Inc()
	}
}

// Scheduler holds reaper and cron metrics.
type Scheduler struct {
	reaped prometheus.Counter
	fires  prometheus.Counter
}

func NewScheduler(reg prometheus.Registerer) *Scheduler {
	f := promauto.With(reg)
	return &Scheduler{
		reaped: f.NewCounter(prometheus.CounterOpts{Name: "tandem_reaped_jobs_total", Help: "Jobs reclaimed from expired leases (requeued or dead)."}),
		fires:  f.NewCounter(prometheus.CounterOpts{Name: "tandem_schedule_fires_total", Help: "Schedule fires that created a job on this replica."}),
	}
}

func (m *Scheduler) Reaped(n int) {
	if m != nil && n > 0 {
		m.reaped.Add(float64(n))
	}
}

func (m *Scheduler) ScheduleFired() {
	if m != nil {
		m.fires.Inc()
	}
}
