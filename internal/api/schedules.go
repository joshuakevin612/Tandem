package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"taskq/internal/cronexpr"
	"taskq/internal/store"
)

type scheduleRequest struct {
	Name        string          `json:"name"`
	Cron        string          `json:"cron"`
	Timezone    string          `json:"timezone"`
	Type        string          `json:"type"`
	Queue       string          `json:"queue"`
	Payload     json.RawMessage `json:"payload"`
	Priority    int             `json:"priority"`
	MaxAttempts *int            `json:"max_attempts"`
}

type schedulesResponse struct {
	Schedules []store.Schedule `json:"schedules"`
}

// validate checks the request and returns the NewSchedule (without tenant and
// next_fire_at) plus the parsed expression.
func (req scheduleRequest) validate() (store.NewSchedule, cronexpr.Expr, error) {
	n := store.NewSchedule{Name: req.Name, Cron: req.Cron, Timezone: req.Timezone, Type: req.Type, Queue: req.Queue}
	switch {
	case req.Name == "":
		return n, cronexpr.Expr{}, errors.New("name is required")
	case req.Cron == "":
		return n, cronexpr.Expr{}, errors.New("cron is required")
	case req.Type == "":
		return n, cronexpr.Expr{}, errors.New("type is required")
	case len(req.Name) > maxKeyLen || len(req.Type) > maxKeyLen || len(req.Queue) > maxKeyLen:
		return n, cronexpr.Expr{}, errors.New("name, type and queue must be at most 255 characters")
	case req.Priority < -100 || req.Priority > 100:
		return n, cronexpr.Expr{}, errors.New("priority must be between -100 and 100")
	}
	n.Priority = int16(req.Priority)
	if req.MaxAttempts != nil {
		if *req.MaxAttempts < 1 || *req.MaxAttempts > 25 {
			return n, cronexpr.Expr{}, errors.New("max_attempts must be between 1 and 25")
		}
		n.MaxAttempts = *req.MaxAttempts
	}
	if p := bytes.TrimSpace(req.Payload); len(p) > 0 && string(p) != "null" {
		n.Payload = req.Payload
	}
	if n.Timezone == "" {
		n.Timezone = "UTC"
	}
	expr, err := cronexpr.Parse(req.Cron, n.Timezone)
	if err != nil {
		return n, cronexpr.Expr{}, err
	}
	return n, expr, nil
}

func (s *server) createSchedule(w http.ResponseWriter, r *http.Request) {
	var req scheduleRequest
	if err := decodeBody(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	n, expr, err := req.validate()
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	tenant, ok := requestTenant(w, r)
	if !ok {
		return
	}
	n.TenantID = tenant
	n.NextFireAt = expr.Next(s.now())
	if n.NextFireAt.IsZero() {
		writeError(w, http.StatusBadRequest, "cron expression never fires")
		return
	}
	sc, err := s.store.CreateSchedule(r.Context(), n)
	switch {
	case errors.Is(err, store.ErrConflict):
		writeError(w, http.StatusConflict, "a schedule with this name already exists")
	case err != nil:
		s.internalError(w, r, err)
	default:
		w.Header().Set("Location", "/v1/schedules/"+sc.ID.String())
		writeJSON(w, http.StatusCreated, sc)
	}
}

func (s *server) listSchedules(w http.ResponseWriter, r *http.Request) {
	tenant, ok := requestTenant(w, r)
	if !ok {
		return
	}
	list, err := s.store.ListSchedules(r.Context(), tenant)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	if list == nil {
		list = []store.Schedule{}
	}
	writeJSON(w, http.StatusOK, schedulesResponse{Schedules: list})
}

// idAndTenant parses the {id} path parameter and the tenant, writing a 400 on
// failure.
func idAndTenant(w http.ResponseWriter, r *http.Request) (uuid.UUID, string, bool) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "id must be a UUID")
		return uuid.Nil, "", false
	}
	tenant, ok := requestTenant(w, r)
	if !ok {
		return uuid.Nil, "", false
	}
	return id, tenant, true
}

// scheduleResult writes sc as 200, or the error mapped to a status.
func (s *server) scheduleResult(w http.ResponseWriter, r *http.Request, sc store.Schedule, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "schedule not found")
	case err != nil:
		s.internalError(w, r, err)
	default:
		writeJSON(w, http.StatusOK, sc)
	}
}

func (s *server) getSchedule(w http.ResponseWriter, r *http.Request) {
	id, tenant, ok := idAndTenant(w, r)
	if !ok {
		return
	}
	sc, err := s.store.GetSchedule(r.Context(), tenant, id)
	s.scheduleResult(w, r, sc, err)
}

func (s *server) deleteSchedule(w http.ResponseWriter, r *http.Request) {
	id, tenant, ok := idAndTenant(w, r)
	if !ok {
		return
	}
	err := s.store.DeleteSchedule(r.Context(), tenant, id)
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "schedule not found")
	case err != nil:
		s.internalError(w, r, err)
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}

// pauseSchedule disables the schedule and keeps its current next_fire_at.
func (s *server) pauseSchedule(w http.ResponseWriter, r *http.Request) {
	id, tenant, ok := idAndTenant(w, r)
	if !ok {
		return
	}
	cur, err := s.store.GetSchedule(r.Context(), tenant, id)
	if err != nil {
		s.scheduleResult(w, r, cur, err)
		return
	}
	sc, err := s.store.SetScheduleEnabled(r.Context(), tenant, id, false, cur.NextFireAt)
	s.scheduleResult(w, r, sc, err)
}

// resumeSchedule enables the schedule and recomputes next_fire_at from now, so
// the fires missed while paused are skipped rather than replayed.
func (s *server) resumeSchedule(w http.ResponseWriter, r *http.Request) {
	id, tenant, ok := idAndTenant(w, r)
	if !ok {
		return
	}
	cur, err := s.store.GetSchedule(r.Context(), tenant, id)
	if err != nil {
		s.scheduleResult(w, r, cur, err)
		return
	}
	expr, err := cronexpr.Parse(cur.Cron, cur.Timezone)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	next := expr.Next(s.now())
	if next.IsZero() {
		writeError(w, http.StatusConflict, "schedule has no future fire time")
		return
	}
	sc, err := s.store.SetScheduleEnabled(r.Context(), tenant, id, true, next)
	s.scheduleResult(w, r, sc, err)
}
