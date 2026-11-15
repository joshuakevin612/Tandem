package cronexpr

import (
	"testing"
	"time"
)

func mustParse(t *testing.T, expr, tz string) Expr {
	t.Helper()
	e, err := Parse(expr, tz)
	if err != nil {
		t.Fatalf("Parse(%q, %q): %v", expr, tz, err)
	}
	return e
}

func utc(y int, m time.Month, d, h, mi, s int) time.Time {
	return time.Date(y, m, d, h, mi, s, 0, time.UTC)
}

func TestParseForms(t *testing.T) {
	base := utc(2025, 1, 1, 12, 0, 0)
	cases := []struct {
		expr string
		want time.Time
	}{
		{"*/5 * * * *", utc(2025, 1, 1, 12, 5, 0)},
		{"30 * * * * *", utc(2025, 1, 1, 12, 0, 30)}, // leading seconds
		{"@hourly", utc(2025, 1, 1, 13, 0, 0)},
		{"@daily", utc(2025, 1, 2, 0, 0, 0)},
		{"@every 10s", utc(2025, 1, 1, 12, 0, 10)},
	}
	for _, c := range cases {
		if got := mustParse(t, c.expr, "").Next(base); !got.Equal(c.want) {
			t.Errorf("%q: Next = %v, want %v", c.expr, got, c.want)
		}
	}
}

func TestParseRejects(t *testing.T) {
	bad := []struct{ expr, tz string }{
		{"", ""},
		{"not a cron", ""},
		{"* * * *", ""},
		{"61 * * * *", ""},
		{"@every 500ms", ""},
		{"@every 0s", ""},
		{"@every nonsense", ""},
		{"CRON_TZ=UTC * * * * *", ""},
		{"* * * * *", "Mars/Olympus"},
	}
	for _, c := range bad {
		if _, err := Parse(c.expr, c.tz); err == nil {
			t.Errorf("Parse(%q, %q): want error", c.expr, c.tz)
		}
	}
	if _, err := Parse("@every 1s", ""); err != nil {
		t.Errorf("@every 1s: %v", err)
	}
}

func TestNextStrictlyAfter(t *testing.T) {
	e := mustParse(t, "0 * * * *", "")
	at := utc(2025, 1, 1, 12, 0, 0)
	if got := e.Next(at); !got.Equal(utc(2025, 1, 1, 13, 0, 0)) {
		t.Fatalf("Next on boundary = %v", got)
	}
}

func TestTimezone(t *testing.T) {
	// 09:00 in New York is 14:00 UTC in January (EST) and 13:00 UTC in July (EDT).
	e := mustParse(t, "0 9 * * *", "America/New_York")
	if got := e.Next(utc(2025, 1, 15, 0, 0, 0)); !got.Equal(utc(2025, 1, 15, 14, 0, 0)) {
		t.Errorf("winter: %v", got)
	}
	if got := e.Next(utc(2025, 7, 15, 0, 0, 0)); !got.Equal(utc(2025, 7, 15, 13, 0, 0)) {
		t.Errorf("summer: %v", got)
	}
	if got := e.Next(utc(2025, 7, 15, 0, 0, 0)); got.Location() != time.UTC {
		t.Errorf("result not UTC: %v", got.Location())
	}
}

func TestDSTSafe(t *testing.T) {
	ny, _ := time.LoadLocation("America/New_York")

	// Spring forward 2025-03-09: 02:30 local does not exist. Next must still
	// return a later instant (never loop or go backwards) and the daily
	// cadence must resume the next day at 02:30 local.
	e := mustParse(t, "30 2 * * *", "America/New_York")
	prev := utc(2025, 3, 8, 12, 0, 0)
	for i := 0; i < 5; i++ {
		n := e.Next(prev)
		if !n.After(prev) {
			t.Fatalf("Next(%v) = %v, not after", prev, n)
		}
		prev = n
	}
	if got := e.Next(utc(2025, 3, 9, 12, 0, 0)).In(ny); got.Hour() != 2 || got.Minute() != 30 || got.Day() != 10 {
		t.Errorf("after gap day: %v", got)
	}

	// Fall back 2025-11-02: 01:30 local happens twice. Next must always advance;
	// robfig fires at both occurrences (two distinct instants), which is
	// accepted and never more than twice.
	e = mustParse(t, "30 1 * * *", "America/New_York")
	prev = utc(2025, 11, 1, 12, 0, 0)
	fires := 0
	for i := 0; i < 4; i++ {
		n := e.Next(prev)
		if !n.After(prev) {
			t.Fatalf("Next(%v) = %v, not after", prev, n)
		}
		if n.In(ny).Day() == 2 {
			fires++
		}
		prev = n
	}
	if fires < 1 || fires > 2 {
		t.Errorf("fires on repeated-hour day = %d, want 1 or 2", fires)
	}

	// Hourly in a zone keeps stepping through the transition.
	h := mustParse(t, "0 * * * *", "America/New_York")
	prev = utc(2025, 3, 9, 4, 0, 0)
	for i := 0; i < 6; i++ {
		n := h.Next(prev)
		if !n.After(prev) || n.Sub(prev) > 2*time.Hour {
			t.Fatalf("hourly step %v -> %v", prev, n)
		}
		prev = n
	}
}
