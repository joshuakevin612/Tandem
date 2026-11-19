// Package api is the HTTP front end for submitting and inspecting jobs.
package api

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"taskq/internal/metrics"
	"taskq/internal/store"
)

const (
	maxBodyBytes   = 1 << 20
	maxKeyLen      = 255
	defaultTenant  = "default"
	tenantHeader   = "X-Tenant-ID"
	idempotencyKey = "Idempotency-Key"
)

// JobStore is the subset of store.Store the API uses.
type JobStore interface {
	CreateJob(ctx context.Context, n store.NewJob) (store.Job, error)
	GetJob(ctx context.Context, id uuid.UUID) (store.Job, error)
	ListJobs(ctx context.Context, f store.ListFilter) ([]store.Job, error)
	RequeueJob(ctx context.Context, tenantID string, id uuid.UUID) (store.Job, error)
	CancelJob(ctx context.Context, tenantID string, id uuid.UUID) (store.Job, error)
	CreateSchedule(ctx context.Context, n store.NewSchedule) (store.Schedule, error)
	GetSchedule(ctx context.Context, tenantID string, id uuid.UUID) (store.Schedule, error)
	ListSchedules(ctx context.Context, tenantID string) ([]store.Schedule, error)
	DeleteSchedule(ctx context.Context, tenantID string, id uuid.UUID) error
	SetScheduleEnabled(ctx context.Context, tenantID string, id uuid.UUID, enabled bool, nextFireAt time.Time) (store.Schedule, error)
	Ping(ctx context.Context) error
}

type server struct {
	store JobStore
	log   *slog.Logger
	now   func() time.Time
	deps  Deps
}

// Limiter is a per-tenant rate limiter. A nil Limiter allows everything.
// Errors (e.g. Redis down) must be treated as "allow" by callers.
type Limiter interface {
	Allow(ctx context.Context, tenant string) (ok bool, retryAfter time.Duration, err error)
}

// StatusCache caches job rows for fast status reads. A nil StatusCache is a no-op.
type StatusCache interface {
	Get(ctx context.Context, id uuid.UUID) (store.Job, bool)
	Set(ctx context.Context, j store.Job)
	Del(ctx context.Context, id uuid.UUID)
}

// Notifier wakes idle workers after an enqueue. A nil Notifier is a no-op.
type Notifier interface {
	Notify(ctx context.Context, queue string)
}

// Deps are optional collaborators. Any nil field means the feature is off.
type Deps struct {
	// Auth is the authentication middleware; nil means HeaderAuth().
	Auth     func(http.Handler) http.Handler
	Limiter  Limiter
	Cache    StatusCache
	Notifier Notifier
	// Metrics records request and business metrics; nil records nothing.
	Metrics *metrics.API
	// MetricsHandler, if set, is served at /metrics OUTSIDE auth. In production
	// restrict it (network policy / separate listener); nginx does not proxy it.
	MetricsHandler http.Handler
}

// Option customises NewServer.
type Option func(*Deps)

// WithDeps sets the optional collaborators.
func WithDeps(d Deps) Option { return func(dst *Deps) { *dst = d } }

func NewServer(st JobStore, logger *slog.Logger, opts ...Option) http.Handler {
	var deps Deps
	for _, o := range opts {
		o(&deps)
	}
	if deps.Auth == nil {
		deps.Auth = HeaderAuth()
	}
	s := &server{store: st, log: logger, now: time.Now, deps: deps}
	r := chi.NewRouter()
	r.Use(withRequestID, accessLog(logger), instrument(deps.Metrics), recoverer(logger), timeout(requestTimeout))
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	r.Get("/readyz", s.ready)
	if deps.MetricsHandler != nil {
		r.Method(http.MethodGet, "/metrics", deps.MetricsHandler)
	}
	r.Group(func(r chi.Router) {
		r.Use(deps.Auth)
		r.Post("/v1/jobs", s.createJob)
		r.Get("/v1/jobs", s.listJobs)
		r.Get("/v1/jobs/{id}", s.getJob)
		r.Post("/v1/jobs/{id}/requeue", s.requeueJob)
		r.Post("/v1/jobs/{id}/cancel", s.cancelJob)
		r.Post("/v1/schedules", s.createSchedule)
		r.Get("/v1/schedules", s.listSchedules)
		r.Get("/v1/schedules/{id}", s.getSchedule)
		r.Delete("/v1/schedules/{id}", s.deleteSchedule)
		r.Post("/v1/schedules/{id}/pause", s.pauseSchedule)
		r.Post("/v1/schedules/{id}/resume", s.resumeSchedule)
	})
	return r
}

type createRequest struct {
	Type        string          `json:"type"`
	Queue       string          `json:"queue"`
	Payload     json.RawMessage `json:"payload"`
	Priority    int             `json:"priority"`
	RunAt       *time.Time      `json:"run_at"`
	MaxAttempts *int            `json:"max_attempts"`
}

func (s *server) createJob(w http.ResponseWriter, r *http.Request) {
	tenant, ok := requestTenant(w, r)
	if !ok {
		return
	}
	// Rate limit after auth, before body decoding or any DB work. Redis
	// errors fail open.
	if l := s.deps.Limiter; l != nil {
		allowed, retryAfter, err := l.Allow(r.Context(), tenant)
		switch {
		case err != nil:
			s.log.WarnContext(r.Context(), "rate limiter unavailable, allowing", "err", err, "request_id", requestID(r))
		case !allowed:
			secs := int(math.Ceil(retryAfter.Seconds()))
			if secs < 1 {
				secs = 1
			}
			s.deps.Metrics.RateLimited()
			w.Header().Set("Retry-After", strconv.Itoa(secs))
			writeError(w, http.StatusTooManyRequests, "rate limit exceeded")
			return
		}
	}
	var req createRequest
	if err := decodeBody(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	n, err := req.validate()
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	n.TenantID = tenant
	if key := r.Header.Get(idempotencyKey); key != "" {
		if len(key) > maxKeyLen {
			writeError(w, http.StatusBadRequest, idempotencyKey+" must be at most 255 characters")
			return
		}
		n.IdempotencyKey = &key
	}

	job, err := s.store.CreateJob(r.Context(), n)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	s.deps.Metrics.JobSubmitted(job.Queue)
	// Wake idle workers if the job is runnable now. The store does not say
	// whether this was an idempotent replay, so a replay may notify too; that
	// only costs a spurious empty poll. Notify is bounded and ignores errors.
	if n := s.deps.Notifier; n != nil && !job.RunAt.After(time.Now()) {
		n.Notify(r.Context(), job.Queue)
	}
	w.Header().Set("Location", "/v1/jobs/"+job.ID.String())
	writeJSON(w, http.StatusAccepted, job)
}

func (req createRequest) validate() (store.NewJob, error) {
	n := store.NewJob{Type: req.Type, Queue: req.Queue, RunAt: req.RunAt}
	switch {
	case req.Type == "":
		return n, errors.New("type is required")
	case len(req.Type) > maxKeyLen || len(req.Queue) > maxKeyLen:
		return n, errors.New("type and queue must be at most 255 characters")
	case req.Priority < -100 || req.Priority > 100:
		return n, errors.New("priority must be between -100 and 100")
	}
	n.Priority = int16(req.Priority)
	if req.MaxAttempts != nil {
		if *req.MaxAttempts < 1 || *req.MaxAttempts > 25 {
			return n, errors.New("max_attempts must be between 1 and 25")
		}
		n.MaxAttempts = *req.MaxAttempts
	}
	if p := bytes.TrimSpace(req.Payload); len(p) > 0 && string(p) != "null" {
		n.Payload = req.Payload
	}
	return n, nil
}

func (s *server) getJob(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "id must be a UUID")
		return
	}
	tenant, ok := requestTenant(w, r)
	if !ok {
		return
	}
	if c := s.deps.Cache; c != nil {
		job, hit := c.Get(r.Context(), id)
		s.deps.Metrics.StatusCache(hit)
		if hit {
			// Tenant is enforced on hits exactly as on misses.
			if job.TenantID != tenant {
				writeError(w, http.StatusNotFound, "job not found")
				return
			}
			writeJSON(w, http.StatusOK, job)
			return
		}
	}
	job, err := s.store.GetJob(r.Context(), id)
	if err == nil && job.TenantID == tenant && s.deps.Cache != nil {
		s.deps.Cache.Set(r.Context(), job) // ignores non-terminal statuses
	}
	switch {
	case errors.Is(err, store.ErrNotFound), err == nil && job.TenantID != tenant:
		// 404 rather than 403 for another tenant's job so ids cannot be probed.
		writeError(w, http.StatusNotFound, "job not found")
	case err != nil:
		s.internalError(w, r, err)
	default:
		writeJSON(w, http.StatusOK, job)
	}
}

var jobStatuses = map[string]bool{
	"queued": true, "running": true, "succeeded": true, "failed": true, "dead": true, "cancelled": true,
}

const (
	defaultListLimit = 50
	maxListLimit     = 200
)

type listResponse struct {
	Jobs       []store.Job `json:"jobs"`
	NextCursor *string     `json:"next_cursor"`
}

// encodeCursor packs the keyset position (created_at, id) of a job into an
// opaque token.
func encodeCursor(j store.Job) string {
	raw := j.CreatedAt.UTC().Format(time.RFC3339Nano) + "|" + j.ID.String()
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodeCursor(c string) (time.Time, uuid.UUID, error) {
	b, err := base64.RawURLEncoding.DecodeString(c)
	if err != nil {
		return time.Time{}, uuid.Nil, err
	}
	ts, idStr, ok := strings.Cut(string(b), "|")
	if !ok {
		return time.Time{}, uuid.Nil, errors.New("malformed cursor")
	}
	t, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		return time.Time{}, uuid.Nil, err
	}
	id, err := uuid.Parse(idStr)
	if err != nil {
		return time.Time{}, uuid.Nil, err
	}
	return t, id, nil
}

func (s *server) listJobs(w http.ResponseWriter, r *http.Request) {
	tenant, ok := requestTenant(w, r)
	if !ok {
		return
	}
	q := r.URL.Query()
	f := store.ListFilter{TenantID: tenant, Status: q.Get("status"), Queue: q.Get("queue"), Limit: defaultListLimit}
	if f.Status != "" && !jobStatuses[f.Status] {
		writeError(w, http.StatusBadRequest, "status must be one of queued, running, succeeded, failed, dead, cancelled")
		return
	}
	if len(f.Queue) > maxKeyLen {
		writeError(w, http.StatusBadRequest, "queue must be at most 255 characters")
		return
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > maxListLimit {
			writeError(w, http.StatusBadRequest, "limit must be an integer between 1 and 200")
			return
		}
		f.Limit = n
	}
	if v := q.Get("cursor"); v != "" {
		t, id, err := decodeCursor(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "cursor is invalid")
			return
		}
		f.Before, f.BeforeID = &t, &id
	}
	jobs, err := s.store.ListJobs(r.Context(), f)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	if jobs == nil {
		jobs = []store.Job{}
	}
	resp := listResponse{Jobs: jobs}
	if len(jobs) == f.Limit {
		next := encodeCursor(jobs[len(jobs)-1])
		resp.NextCursor = &next
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *server) requeueJob(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "id must be a UUID")
		return
	}
	tenant, ok := requestTenant(w, r)
	if !ok {
		return
	}
	job, err := s.store.RequeueJob(r.Context(), tenant, id)
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "job not found")
	case errors.Is(err, store.ErrInvalidState):
		writeError(w, http.StatusConflict, "job cannot be requeued: only dead or failed jobs can be requeued")
	case err != nil:
		s.internalError(w, r, err)
	default:
		// Invalidate before responding so a caller never sees a stale terminal
		// entry after a successful requeue.
		if s.deps.Cache != nil {
			s.deps.Cache.Del(r.Context(), id)
		}
		writeJSON(w, http.StatusOK, job)
	}
}

func (s *server) cancelJob(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "id must be a UUID")
		return
	}
	tenant, ok := requestTenant(w, r)
	if !ok {
		return
	}
	job, err := s.store.CancelJob(r.Context(), tenant, id)
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "job not found")
	case errors.Is(err, store.ErrInvalidState):
		writeError(w, http.StatusConflict, "job cannot be cancelled: only queued or running jobs can be cancelled")
	case err != nil:
		s.internalError(w, r, err)
	default:
		if s.deps.Cache != nil {
			s.deps.Cache.Del(r.Context(), id) // defensive
		}
		writeJSON(w, http.StatusOK, job)
	}
}

// requestTenant returns the request's tenant, writing a 400 if it is invalid.
func requestTenant(w http.ResponseWriter, r *http.Request) (string, bool) {
	// Set by the auth middleware (header or API key).
	t, _ := r.Context().Value(tenantCtxKey{}).(string)
	if t == "" {
		t = defaultTenant
	}
	if len(t) > maxKeyLen {
		writeError(w, http.StatusBadRequest, tenantHeader+" too long")
		return "", false
	}
	return t, true
}

func (s *server) ready(w http.ResponseWriter, r *http.Request) {
	if err := s.store.Ping(r.Context()); err != nil {
		s.log.WarnContext(r.Context(), "readiness check failed", "err", err, "request_id", requestID(r))
		writeError(w, http.StatusServiceUnavailable, "not ready")
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *server) internalError(w http.ResponseWriter, r *http.Request, err error) {
	s.log.ErrorContext(r.Context(), "request failed", "err", err, "request_id", requestID(r))
	writeError(w, http.StatusInternalServerError, "internal error")
}

// decodeBody strictly decodes a single JSON value of at most maxBodyBytes.
func decodeBody(w http.ResponseWriter, r *http.Request, dst any) error {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			return errors.New("request body too large")
		}
		return fmt.Errorf("invalid JSON body: %w", err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return errors.New("invalid JSON body: unexpected trailing data")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
