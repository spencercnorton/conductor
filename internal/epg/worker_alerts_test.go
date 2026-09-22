package epg

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/spencercnorton/conductor/internal/alerts"
)

// alertCapture spins an httptest endpoint and an alerts.Client pointed at it.
func alertCapture(t *testing.T) (*alerts.Client, func() []map[string]any) {
	t.Helper()
	var mu sync.Mutex
	var events []map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var ev map[string]any
		_ = json.Unmarshal(body, &ev)
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)
	client := alerts.New(srv.URL, "s", slog.New(slog.NewTextHandler(io.Discard, nil)))
	get := func() []map[string]any {
		mu.Lock()
		defer mu.Unlock()
		return append([]map[string]any(nil), events...)
	}
	return client, get
}

func TestAlertSourceTransition_FiresOnceAtThresholdCrossing(t *testing.T) {
	client, got := alertCapture(t)
	w := &Worker{Alerts: client, FailureAlertThreshold: 3}
	ctx := context.Background()
	ferr := errors.New("HTTP 502")

	w.alertSourceTransition(ctx, "iboost", 0, 1, ferr) // streak 1: quiet
	w.alertSourceTransition(ctx, "iboost", 1, 2, ferr) // streak 2: quiet
	w.alertSourceTransition(ctx, "iboost", 2, 3, ferr) // crosses 3: alert
	w.alertSourceTransition(ctx, "iboost", 3, 4, ferr) // deepening: quiet
	w.alertSourceTransition(ctx, "iboost", 4, 5, ferr) // deepening: quiet

	events := got()
	if len(events) != 1 {
		t.Fatalf("expected exactly 1 failing alert, got %d: %v", len(events), events)
	}
	if events[0]["severity"] != "error" {
		t.Fatalf("expected error severity, got %v", events[0]["severity"])
	}
}

func TestAlertSourceTransition_RefiresAfterRestartMidStreak(t *testing.T) {
	client, got := alertCapture(t)
	// Fresh worker = restarted container: no in-memory stamp, but the DB
	// already carries a deep failure streak. The first failing pass must
	// alert — the old crossing-only rule stayed silent forever here.
	w := &Worker{Alerts: client, FailureAlertThreshold: 3}
	ctx := context.Background()
	ferr := errors.New("XMLTV channel declaration has an empty id")

	w.alertSourceTransition(ctx, "iboost", 14, 15, ferr) // already past threshold: alert
	w.alertSourceTransition(ctx, "iboost", 15, 16, ferr) // dampened: quiet

	if events := got(); len(events) != 1 {
		t.Fatalf("expected exactly 1 failing alert after restart mid-streak, got %d: %v", len(events), events)
	}
}

func TestAlertSourceTransition_RealertsAfterDampeningWindow(t *testing.T) {
	client, got := alertCapture(t)
	w := &Worker{Alerts: client, FailureAlertThreshold: 3}
	ctx := context.Background()
	ferr := errors.New("HTTP 502")

	w.alertSourceTransition(ctx, "iboost", 2, 3, ferr) // crossing: alert
	w.alertSourceTransition(ctx, "iboost", 3, 4, ferr) // dampened: quiet

	// Age the stamp past the re-alert window: still failing → alert again.
	w.sourceAlertMu.Lock()
	w.lastSourceAlert["iboost"] = time.Now().Add(-sourceRealertEvery - time.Minute)
	w.sourceAlertMu.Unlock()
	w.alertSourceTransition(ctx, "iboost", 4, 5, ferr)

	if events := got(); len(events) != 2 {
		t.Fatalf("expected crossing + re-alert = 2 events, got %d: %v", len(events), events)
	}
}

func TestAlertSourceTransition_RecoveryClearsDampeningStamp(t *testing.T) {
	client, got := alertCapture(t)
	w := &Worker{Alerts: client, FailureAlertThreshold: 3}
	ctx := context.Background()
	ferr := errors.New("HTTP 502")

	w.alertSourceTransition(ctx, "iboost", 2, 3, ferr) // crossing: alert
	w.alertSourceTransition(ctx, "iboost", 3, 0, nil)  // recovery: alert + stamp cleared
	w.alertSourceTransition(ctx, "iboost", 2, 3, ferr) // new streak crossing: alert

	if events := got(); len(events) != 3 {
		t.Fatalf("expected fail+recover+fail = 3 events, got %d: %v", len(events), events)
	}
}

func TestAlertSourceTransition_RecoveryAfterThreshold(t *testing.T) {
	client, got := alertCapture(t)
	w := &Worker{Alerts: client, FailureAlertThreshold: 3}
	ctx := context.Background()

	w.alertSourceTransition(ctx, "iboost", 5, 0, nil) // recovered from deep streak

	events := got()
	if len(events) != 1 {
		t.Fatalf("expected 1 recovery alert, got %d", len(events))
	}
	if events[0]["severity"] != "info" {
		t.Fatalf("expected info severity, got %v", events[0]["severity"])
	}
}

func TestAlertSourceTransition_ShortBlipRecovery_Quiet(t *testing.T) {
	client, got := alertCapture(t)
	w := &Worker{Alerts: client, FailureAlertThreshold: 3}
	ctx := context.Background()

	// One failure then success — never reached the threshold, so neither
	// direction alerts.
	w.alertSourceTransition(ctx, "iboost", 0, 1, errors.New("blip"))
	w.alertSourceTransition(ctx, "iboost", 1, 0, nil)

	if events := got(); len(events) != 0 {
		t.Fatalf("expected silence for sub-threshold blip, got %v", events)
	}
}

func TestAlertSourceTransition_NilClientOrDisabledThreshold_NoPanic(t *testing.T) {
	ctx := context.Background()
	(&Worker{}).alertSourceTransition(ctx, "x", 2, 3, errors.New("e"))

	client, got := alertCapture(t)
	w := &Worker{Alerts: client, FailureAlertThreshold: 0}
	w.alertSourceTransition(ctx, "x", 2, 3, errors.New("e"))
	if events := got(); len(events) != 0 {
		t.Fatalf("threshold 0 must disable alerts, got %v", events)
	}
}
