package alerts

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"
)

// sentinelHarness builds a one-rule sentinel over a mutable counter with an
// injectable clock, reusing the monitor tests' captureServer.
func sentinelHarness(t *testing.T, threshold uint64) (*Sentinel, *uint64, *time.Time, func() []Event) {
	t.Helper()
	cs := newCaptureServer(t)
	client := New(cs.srv.URL, "s", slog.New(slog.NewTextHandler(io.Discard, nil)))
	value := new(uint64)
	now := new(time.Time)
	*now = time.Unix(1_700_000_000, 0)
	s := &Sentinel{Client: client, Logger: client.Logger, Interval: time.Minute, Window: 3}
	s.applyDefaults()
	s.now = func() time.Time { return *now }
	s.rules = []sentinelRule{{
		name: "test_rule", severity: SevError, threshold: threshold,
		read: func() uint64 { return *value },
		describe: func(d uint64, w time.Duration) string {
			return fmt.Sprintf("%d in %s", d, w)
		},
	}}
	return s, value, now, cs.all
}

func TestSentinel_WarmupSuppressesLifetimeTotals(t *testing.T) {
	s, value, _, events := sentinelHarness(t, 3)
	ctx := context.Background()
	// A process that restarts with large lifetime totals must not read them
	// as a burst: the first Window+1 samples only build history.
	*value = 900
	for i := 0; i < s.Window+1; i++ {
		s.sample(ctx)
	}
	if got := events(); len(got) != 0 {
		t.Fatalf("warm-up must stay silent, got %v", got)
	}
}

func TestSentinel_BurstTripsOnceThenDampens(t *testing.T) {
	s, value, now, events := sentinelHarness(t, 3)
	ctx := context.Background()
	for i := 0; i < s.Window+1; i++ {
		s.sample(ctx) // warm up at 0
	}
	*value = 5 // +5 within one window
	s.sample(ctx)
	if got := events(); len(got) != 1 {
		t.Fatalf("expected 1 alert on burst, got %d", len(got))
	}
	*value = 10 // still bursting
	s.sample(ctx)
	if got := events(); len(got) != 1 {
		t.Fatalf("dampening must hold within Realert, got %d", len(events()))
	}
	// Past the dampening window and still elevated: re-alert.
	*now = now.Add(31 * time.Minute)
	*value = 15
	s.sample(ctx)
	if got := events(); len(got) != 2 {
		t.Fatalf("expected re-alert after dampening window, got %d", len(got))
	}
}

func TestSentinel_QuietCountersStaySilent(t *testing.T) {
	// +1 every other sample = at most +2 per 3-sample window: below threshold.
	s, value, _, events := sentinelHarness(t, 3)
	ctx := context.Background()
	for i := 0; i < s.Window*4; i++ {
		if i%2 == 0 {
			*value++
		}
		s.sample(ctx)
	}
	if got := events(); len(got) != 0 {
		t.Fatalf("sub-threshold drift must stay silent, got %d", len(got))
	}
}
