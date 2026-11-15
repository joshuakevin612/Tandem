// Package cronexpr parses cron expressions and computes fire times. It wraps
// robfig/cron's parser only; its runner is never used.
package cronexpr

import (
	"fmt"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
)

// MinEvery is the smallest accepted "@every" interval.
const MinEvery = time.Second

var parser = cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

// Expr is a parsed schedule bound to a time zone.
type Expr struct {
	sched cron.Schedule
}

// Parse accepts a standard 5-field cron expression, an optional leading
// seconds field (6 fields), or a descriptor (@hourly, @daily, @every 10s...).
// tz is an IANA zone name; empty means UTC. Embedded TZ=/CRON_TZ= prefixes are
// rejected: the zone comes from tz only.
func Parse(expr, tz string) (Expr, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return Expr{}, fmt.Errorf("cronexpr: empty expression")
	}
	if strings.HasPrefix(expr, "TZ=") || strings.HasPrefix(expr, "CRON_TZ=") {
		return Expr{}, fmt.Errorf("cronexpr: embedded time zone not allowed; use the timezone field")
	}
	loc := time.UTC
	if tz != "" {
		l, err := time.LoadLocation(tz)
		if err != nil {
			return Expr{}, fmt.Errorf("cronexpr: unknown time zone %q", tz)
		}
		loc = l
	}
	if rest, ok := strings.CutPrefix(expr, "@every "); ok {
		d, err := time.ParseDuration(strings.TrimSpace(rest))
		if err != nil {
			return Expr{}, fmt.Errorf("cronexpr: %w", err)
		}
		if d < MinEvery {
			return Expr{}, fmt.Errorf("cronexpr: @every interval %v is under %v", d, MinEvery)
		}
	}
	sched, err := parser.Parse(expr)
	if err != nil {
		return Expr{}, fmt.Errorf("cronexpr: %w", err)
	}
	if spec, ok := sched.(*cron.SpecSchedule); ok {
		spec.Location = loc
	}
	return Expr{sched: sched}, nil
}

// Next returns the first fire time strictly after the given instant, in UTC.
// Fire times are computed on the wall clock of the schedule's zone, so a daily
// "0 9 * * *" stays at 09:00 local across DST changes. The result is always
// after `after`, so repeatedly feeding Next its own output cannot loop. It
// returns the zero time if the expression never fires again.
func (e Expr) Next(after time.Time) time.Time {
	if e.sched == nil {
		return time.Time{}
	}
	n := e.sched.Next(after)
	if n.IsZero() {
		return n
	}
	return n.UTC()
}
