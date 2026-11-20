// Package handlers holds the built-in job handlers.
package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strings"
	"time"

	"tandem/internal/store"
	"tandem/internal/worker"
)

const maxDrain = 1 << 20 // response bytes read before the connection is dropped

func RegisterBuiltins(r *worker.Registry) {
	r.Register("sleep", Sleep)
	r.Register("flaky", Flaky)
	r.Register("http", HTTP)
}

// Sleep waits payload.ms milliseconds or until ctx is done.
func Sleep(ctx context.Context, j store.Job) error {
	var p struct {
		MS int `json:"ms"`
	}
	if err := json.Unmarshal(j.Payload, &p); err != nil {
		return fmt.Errorf("bad payload: %w", err)
	}
	t := time.NewTimer(time.Duration(p.MS) * time.Millisecond)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Flaky fails with probability payload.fail_rate.
func Flaky(_ context.Context, j store.Job) error {
	var p struct {
		FailRate float64 `json:"fail_rate"`
	}
	if err := json.Unmarshal(j.Payload, &p); err != nil {
		return fmt.Errorf("bad payload: %w", err)
	}
	if rand.Float64() < p.FailRate {
		return errors.New("flaky: random failure")
	}
	return nil
}

// HTTP sends a request and treats any non-2xx response as an error.
func HTTP(ctx context.Context, j store.Job) error {
	p := struct {
		URL       string `json:"url"`
		Method    string `json:"method"`
		Body      string `json:"body"`
		TimeoutMS int    `json:"timeout_ms"`
	}{Method: http.MethodPost, TimeoutMS: 10000}
	if err := json.Unmarshal(j.Payload, &p); err != nil {
		return fmt.Errorf("bad payload: %w", err)
	}
	u, err := url.Parse(p.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("url must be an absolute http or https URL: %q", p.URL)
	}
	if p.TimeoutMS <= 0 {
		p.TimeoutMS = 10000
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(p.TimeoutMS)*time.Millisecond)
	defer cancel()

	var body io.Reader
	if p.Body != "" {
		body = strings.NewReader(p.Body)
	}
	req, err := http.NewRequestWithContext(ctx, p.Method, p.URL, body)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxDrain))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("http %s %s: status %d", p.Method, u.Host, resp.StatusCode)
	}
	return nil
}
