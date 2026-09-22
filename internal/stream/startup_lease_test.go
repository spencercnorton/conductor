package stream

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

// leaseReader delivers a chunk of n bytes every interval until stopped —
// a byte-flowing-but-never-proven source.
type leaseReader struct {
	chunk    int
	interval time.Duration
	stopAt   time.Time
	ctx      context.Context
}

func (r *leaseReader) Read(p []byte) (int, error) {
	if r.ctx.Err() != nil {
		return 0, r.ctx.Err()
	}
	if !r.stopAt.IsZero() && time.Now().After(r.stopAt) {
		select {
		case <-r.ctx.Done():
			return 0, r.ctx.Err()
		}
	}
	time.Sleep(r.interval)
	n := min(r.chunk, len(p))
	return n, nil
}

func leaseAttempt(t *testing.T, timeout, wall, window time.Duration, floor int64) *upstreamAttempt {
	t.Helper()
	a := newUpstreamAttempt(context.Background(), "http://example.invalid/x.ts", "lease-test",
		slog.New(slog.NewTextHandler(io.Discard, nil)), timeout)
	t.Cleanup(a.Close)
	a.allowStartupProgressExtension(wall)
	a.armStartupProgressLease()
	a.leaseWindow = window
	a.leaseFloor = floor
	return a
}

func TestStartupLease_SilentSourceDiesOnInitialBudget(t *testing.T) {
	a := leaseAttempt(t, 150*time.Millisecond, 2*time.Second, 150*time.Millisecond, 64)
	select {
	case <-a.ctx.Done():
	case <-time.After(600 * time.Millisecond):
		t.Fatal("silent attempt outlived its initial budget despite armed leasing")
	}
}

func TestStartupLease_FlowingSourceOutlivesInitialBudgetAndHitsWall(t *testing.T) {
	a := leaseAttempt(t, 150*time.Millisecond, 900*time.Millisecond, 150*time.Millisecond, 64)
	a.reader = &leaseReader{chunk: 128, interval: 30 * time.Millisecond, ctx: a.ctx}
	start := time.Now()
	buf := make([]byte, 4096)
	for {
		if _, err := a.Read(buf); err != nil {
			break
		}
	}
	lived := time.Since(start)
	if lived < 500*time.Millisecond {
		t.Fatalf("flowing attempt died at %s — leases did not extend past the 150ms budget", lived)
	}
	if lived > 2500*time.Millisecond {
		t.Fatalf("flowing attempt lived %s — the wall did not bound extensions", lived)
	}
}

func TestStartupLease_FlowStoppingDiesOneWindowLater(t *testing.T) {
	a := leaseAttempt(t, 150*time.Millisecond, 5*time.Second, 200*time.Millisecond, 64)
	a.reader = &leaseReader{chunk: 128, interval: 30 * time.Millisecond,
		stopAt: time.Now().Add(400 * time.Millisecond), ctx: a.ctx}
	start := time.Now()
	buf := make([]byte, 4096)
	for {
		if _, err := a.Read(buf); err != nil {
			break
		}
	}
	lived := time.Since(start)
	if lived < 500*time.Millisecond || lived > 1800*time.Millisecond {
		t.Fatalf("attempt lived %s; want death roughly one lease window after flow stopped (~600ms)", lived)
	}
}

func TestStartupLease_MediaReadyStopsLeasing(t *testing.T) {
	a := leaseAttempt(t, time.Second, 5*time.Second, time.Second, 1)
	a.noteStartupProgress(64)
	if err := a.markMediaReady(); err != nil {
		t.Fatalf("markMediaReady: %v", err)
	}
	// Renewals after readiness must be inert; the stopped timer stays stopped.
	a.noteStartupProgress(4096)
	a.timerMu.Lock()
	stopped := a.timerStopped
	a.timerMu.Unlock()
	if !stopped {
		t.Fatal("timer must remain stopped after readiness")
	}
}

func TestStartupExtensionWall_InitialTuneClampedByDeadline(t *testing.T) {
	if w := startupExtensionWall(time.Time{}, true); w != maxStartupAttemptWall {
		t.Fatalf("reconnect wall = %s", w)
	}
	if w := startupExtensionWall(time.Now().Add(3*time.Second), false); w > 3*time.Second+50*time.Millisecond {
		t.Fatalf("initial-tune wall %s exceeds the client deadline", w)
	}
	if w := startupExtensionWall(time.Now().Add(-time.Second), false); w != 0 {
		t.Fatalf("expired deadline must disable extensions, got %s", w)
	}
}

func TestStartupLease_UnarmedAttemptNeverRenews(t *testing.T) {
	// The finite-preflight download reads through a.Read before open()
	// settles on the streaming path; those bytes must not renew leases or
	// the classifier-capacity/finite-download bounds slide.
	a := newUpstreamAttempt(context.Background(), "http://example.invalid/x.ts", "lease-unarmed",
		slog.New(slog.NewTextHandler(io.Discard, nil)), 150*time.Millisecond)
	t.Cleanup(a.Close)
	a.allowStartupProgressExtension(5 * time.Second)
	a.leaseFloor = 1
	a.noteStartupProgress(1 << 20)
	if a.leaseRenewals != 0 {
		t.Fatalf("unarmed attempt renewed %d leases", a.leaseRenewals)
	}
	select {
	case <-a.ctx.Done():
	case <-time.After(600 * time.Millisecond):
		t.Fatal("unarmed attempt outlived its budget")
	}
}
