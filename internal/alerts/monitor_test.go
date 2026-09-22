package alerts

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// captureServer collects every Event POSTed to it.
type captureServer struct {
	mu     sync.Mutex
	events []Event
	srv    *httptest.Server
}

func newCaptureServer(t *testing.T) *captureServer {
	t.Helper()
	cs := &captureServer{}
	cs.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var ev Event
		if err := json.Unmarshal(body, &ev); err != nil {
			t.Errorf("bad event body: %v", err)
		}
		cs.mu.Lock()
		cs.events = append(cs.events, ev)
		cs.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(cs.srv.Close)
	return cs
}

func (cs *captureServer) all() []Event {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return append([]Event(nil), cs.events...)
}

func testMonitor(t *testing.T, stallAfter time.Duration) (*WorkerMonitor, *captureServer, *time.Time) {
	t.Helper()
	cs := newCaptureServer(t)
	client := New(cs.srv.URL, "test-secret", slog.New(slog.NewTextHandler(io.Discard, nil)))
	m := NewWorkerMonitor("testworker", stallAfter, client)
	clock := time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC)
	m.now = func() time.Time { return clock }
	return m, cs, &clock
}

func TestMonitor_SuccessOnly_NoAlerts(t *testing.T) {
	m, cs, _ := testMonitor(t, time.Hour)
	m.Start()
	m.Success(context.Background())
	m.Success(context.Background())
	if got := cs.all(); len(got) != 0 {
		t.Fatalf("expected no alerts, got %d", len(got))
	}
	snap := m.Snapshot()
	if snap.Stalled || snap.LastError != "" {
		t.Fatalf("unexpected snapshot: %+v", snap)
	}
}

func TestMonitor_FailureBeforeDeadline_Quiet(t *testing.T) {
	m, cs, clock := testMonitor(t, time.Hour)
	m.Start()
	*clock = clock.Add(30 * time.Minute)
	m.Failure(context.Background(), "boom")
	if got := cs.all(); len(got) != 0 {
		t.Fatalf("expected no alerts before deadline, got %d", len(got))
	}
	if snap := m.Snapshot(); snap.Stalled || snap.LastError != "boom" {
		t.Fatalf("unexpected snapshot: %+v", snap)
	}
}

func TestMonitor_FailurePastDeadline_AlertsOnce(t *testing.T) {
	m, cs, clock := testMonitor(t, time.Hour)
	m.Start()
	*clock = clock.Add(2 * time.Hour)
	m.Failure(context.Background(), "boom 1")
	*clock = clock.Add(time.Hour)
	m.Failure(context.Background(), "boom 2")
	got := cs.all()
	if len(got) != 1 {
		t.Fatalf("expected exactly 1 stall alert, got %d", len(got))
	}
	if got[0].Severity != SevError || got[0].Source != "conductor.testworker" {
		t.Fatalf("unexpected event: %+v", got[0])
	}
	if !m.Snapshot().Stalled {
		t.Fatal("expected stalled snapshot")
	}
}

func TestMonitor_RecoveryAfterStall_AlertsRecovered(t *testing.T) {
	m, cs, clock := testMonitor(t, time.Hour)
	m.Start()
	*clock = clock.Add(2 * time.Hour)
	m.Failure(context.Background(), "boom")
	*clock = clock.Add(time.Hour)
	m.Success(context.Background())
	m.Success(context.Background()) // second success must not re-alert

	got := cs.all()
	if len(got) != 2 {
		t.Fatalf("expected stall + recovery, got %d events", len(got))
	}
	if got[1].Severity != SevInfo {
		t.Fatalf("recovery should be info: %+v", got[1])
	}
	snap := m.Snapshot()
	if snap.Stalled || snap.LastError != "" {
		t.Fatalf("unexpected snapshot after recovery: %+v", snap)
	}
}

func TestMonitor_InitFailed_AlertsImmediately(t *testing.T) {
	m, cs, _ := testMonitor(t, time.Hour)
	m.Start()
	m.InitFailed(context.Background(), "pg unreachable")
	got := cs.all()
	if len(got) != 1 {
		t.Fatalf("expected immediate init-failure alert, got %d", len(got))
	}
	snap := m.Snapshot()
	if !snap.Stalled || !snap.InitFailed {
		t.Fatalf("unexpected snapshot: %+v", snap)
	}
}

func TestMonitor_NilReceiver_NoPanic(t *testing.T) {
	var m *WorkerMonitor
	m.Start()
	m.Success(context.Background())
	m.Failure(context.Background(), "x")
	m.InitFailed(context.Background(), "y")
	if snap := m.Snapshot(); snap.Name != "" {
		t.Fatalf("nil snapshot should be zero: %+v", snap)
	}
}

func TestMonitor_ZeroStallAfter_NeverAlerts(t *testing.T) {
	m, cs, clock := testMonitor(t, 0)
	m.Start()
	*clock = clock.Add(100 * time.Hour)
	m.Failure(context.Background(), "boom")
	if got := cs.all(); len(got) != 0 {
		t.Fatalf("StallAfter=0 must disable stall alerts, got %d", len(got))
	}
}
