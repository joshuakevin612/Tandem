package queue

import (
	"testing"
	"time"
)

func TestCeilingGrowsAndCaps(t *testing.T) {
	b := Backoff{Base: time.Second, Max: 30 * time.Second}
	want := []time.Duration{1, 2, 4, 8, 16, 30, 30}
	for i, w := range want {
		if got := b.ceiling(i + 1); got != w*time.Second {
			t.Fatalf("attempt %d: ceiling %v, want %v", i+1, got, w*time.Second)
		}
	}
}

func TestDelayWithinBounds(t *testing.T) {
	b := Backoff{Base: 10 * time.Millisecond, Max: time.Second}
	for attempt := 1; attempt <= 12; attempt++ {
		ceil := b.ceiling(attempt)
		var sawHigh, sawLow bool
		for range 2000 {
			d := b.Delay(attempt)
			if d < 0 || d > ceil {
				t.Fatalf("attempt %d: delay %v outside [0,%v]", attempt, d, ceil)
			}
			sawHigh = sawHigh || d > ceil/2
			sawLow = sawLow || d <= ceil/2
		}
		if !sawHigh || !sawLow {
			t.Fatalf("attempt %d: no jitter spread (high=%v low=%v)", attempt, sawHigh, sawLow)
		}
	}
}

func TestDelayNeverExceedsMax(t *testing.T) {
	b := Backoff{Base: time.Second, Max: 5 * time.Second}
	for range 5000 {
		if d := b.Delay(100); d < 0 || d > b.Max {
			t.Fatalf("delay %v exceeds max", d)
		}
	}
}

func TestNoOverflow(t *testing.T) {
	for _, b := range []Backoff{
		{Base: time.Second, Max: 5 * time.Minute},
		{Base: time.Hour, Max: 0}, // uncapped
		{Base: time.Duration(1 << 62), Max: time.Duration(1<<63 - 1)},
	} {
		for _, attempt := range []int{1, 2, 63, 64, 65, 1000, 1 << 30} {
			for range 100 {
				if d := b.Delay(attempt); d < 0 {
					t.Fatalf("%+v attempt %d: negative delay %v", b, attempt, d)
				}
			}
		}
	}
}

func TestEdgeCases(t *testing.T) {
	if d := (Backoff{}).Delay(3); d != 0 {
		t.Fatalf("zero backoff delay = %v", d)
	}
	b := Backoff{Base: time.Second, Max: time.Minute}
	if got := b.ceiling(0); got != time.Second {
		t.Fatalf("attempt 0 ceiling = %v", got)
	}
}
