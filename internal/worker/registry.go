// Package worker leases jobs from the store and runs registered handlers.
package worker

import (
	"context"
	"sync"

	"taskq/internal/store"
)

// Handler runs one job. Returning nil completes it; an error fails it.
type Handler func(ctx context.Context, job store.Job) error

// Registry maps job types to handlers. It is safe for concurrent use.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

func NewRegistry() *Registry { return &Registry{handlers: map[string]Handler{}} }

func (r *Registry) Register(jobType string, h Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[jobType] = h
}

func (r *Registry) Lookup(jobType string) (Handler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handlers[jobType]
	return h, ok
}
