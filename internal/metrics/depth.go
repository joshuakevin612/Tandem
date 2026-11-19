package metrics

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"taskq/internal/store"
)

// DepthSource is the subset of store.Store the depth collector uses.
type DepthSource interface {
	QueueStats(ctx context.Context) ([]store.QueueStat, error)
}

// jobStatuses are zero-filled for every queue seen so a drained status reads 0
// instead of vanishing.
var jobStatuses = []string{"queued", "running", "succeeded", "failed", "dead", "cancelled"}

// QueueDepth exposes taskq_jobs{queue,status} and
// taskq_oldest_queued_job_age_seconds{queue} from the database at scrape time.
//
// Register it in exactly one binary (the scheduler): every replica would
// otherwise export identical series. Results are cached for TTL and the query
// is serialised, so concurrent scrapes cost at most one query per window.
type QueueDepth struct {
	Source  DepthSource
	TTL     time.Duration    // default 5s
	Timeout time.Duration    // per-query, default 2s
	Now     func() time.Time // default time.Now; injected in tests
	Logger  *slog.Logger

	jobs *prometheus.Desc
	age  *prometheus.Desc

	mu      sync.Mutex
	fetched time.Time
	stats   []store.QueueStat
}

func NewQueueDepth(src DepthSource, log *slog.Logger) *QueueDepth {
	return &QueueDepth{
		Source: src, Logger: log,
		jobs: prometheus.NewDesc("taskq_jobs", "Jobs per queue and status.", []string{"queue", "status"}, nil),
		age:  prometheus.NewDesc("taskq_oldest_queued_job_age_seconds", "Age of the oldest queued job that is already runnable.", []string{"queue"}, nil),
	}
}

func (c *QueueDepth) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.jobs
	ch <- c.age
}

func (c *QueueDepth) snapshot() []store.QueueStat {
	ttl, timeout, now := c.TTL, c.Timeout, c.Now
	if ttl <= 0 {
		ttl = 5 * time.Second
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	if now == nil {
		now = time.Now
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.fetched.IsZero() && now().Sub(c.fetched) < ttl {
		return c.stats
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	stats, err := c.Source.QueueStats(ctx)
	if err != nil {
		if c.Logger != nil {
			c.Logger.Warn("queue depth scrape failed", "err", err)
		}
		// Serve the last good snapshot rather than gaps; a failed fetch is not cached.
		return c.stats
	}
	c.fetched, c.stats = now(), stats
	return stats
}

func (c *QueueDepth) Collect(ch chan<- prometheus.Metric) {
	counts := map[string]map[string]int64{}
	ages := map[string]float64{}
	for _, s := range c.snapshot() {
		if counts[s.Queue] == nil {
			counts[s.Queue] = map[string]int64{}
		}
		counts[s.Queue][s.Status] = s.Count
		if s.Status == "queued" {
			ages[s.Queue] = s.OldestRunnableAge
		}
	}
	for q, byStatus := range counts {
		for _, st := range jobStatuses {
			ch <- prometheus.MustNewConstMetric(c.jobs, prometheus.GaugeValue, float64(byStatus[st]), q, st)
		}
		for st, n := range byStatus { // statuses outside the known set still show up
			if !knownStatus(st) {
				ch <- prometheus.MustNewConstMetric(c.jobs, prometheus.GaugeValue, float64(n), q, st)
			}
		}
		ch <- prometheus.MustNewConstMetric(c.age, prometheus.GaugeValue, ages[q], q)
	}
}

func knownStatus(s string) bool {
	for _, k := range jobStatuses {
		if s == k {
			return true
		}
	}
	return false
}
