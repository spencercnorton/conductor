package alerts

import (
	"context"
	"sync"
	"time"
)

// WorkerMonitor tracks per-worker pass outcomes and emits a single
// WorkerStalled alert when a worker has gone StallAfter without a successful
// pass, plus a WorkerRecovered alert when it next succeeds. Used by the
// ppvsync and sports workers — both previously only logged failures, so a
// permanently-failed worker was invisible (2026-06-09 live-TV audit, E5).
//
// All methods are safe for concurrent use; a nil *WorkerMonitor no-ops, so
// workers can call it unconditionally.
type WorkerMonitor struct {
	Name       string
	StallAfter time.Duration // how long without success before alerting
	Client     *Client       // may be nil (alerting disabled, tracking still works)

	now func() time.Time // test seam; defaults to time.Now

	mu          sync.Mutex
	started     time.Time
	lastSuccess time.Time
	lastErr     string
	stalled     bool
	initFailed  bool
}

func NewWorkerMonitor(name string, stallAfter time.Duration, client *Client) *WorkerMonitor {
	return &WorkerMonitor{Name: name, StallAfter: stallAfter, Client: client, now: time.Now}
}

// clock tolerates literal construction (nil now func).
func (m *WorkerMonitor) clock() time.Time {
	if m.now == nil {
		return time.Now()
	}
	return m.now()
}

// Start stamps the baseline: "no success yet" is measured from here, not
// from the zero time, so a worker that is broken from boot alerts after
// StallAfter rather than instantly (transient startup races shouldn't page).
func (m *WorkerMonitor) Start() {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	t := m.clock()
	m.started = t
	m.lastSuccess = t
}

// Success records a successful pass and sends a recovery alert if the
// worker had previously been reported stalled.
func (m *WorkerMonitor) Success(ctx context.Context) {
	if m == nil {
		return
	}
	m.mu.Lock()
	wasStalled := m.stalled
	downFor := m.clock().Sub(m.lastSuccess)
	m.lastSuccess = m.clock()
	m.lastErr = ""
	m.stalled = false
	m.initFailed = false
	m.mu.Unlock()

	if wasStalled && m.Client != nil {
		m.Client.Send(ctx, WorkerRecovered(m.Name, downFor))
	}
}

// Failure records a failed pass. When the worker has gone StallAfter
// without a success it sends one WorkerStalled alert; further failures stay
// quiet until the worker recovers (Success resets the latch).
func (m *WorkerMonitor) Failure(ctx context.Context, detail string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.lastErr = detail
	since := m.clock().Sub(m.lastSuccess)
	shouldAlert := !m.stalled && m.StallAfter > 0 && since >= m.StallAfter
	if shouldAlert {
		m.stalled = true
	}
	m.mu.Unlock()

	if shouldAlert && m.Client != nil {
		m.Client.Send(ctx, WorkerStalled(m.Name, since, detail))
	}
}

// InitFailed marks the worker as permanently failed at startup (e.g.
// ppvsync's dispatcharr connection backoff gave up) and alerts immediately —
// there is no pass loop running that would ever trip the stall deadline.
func (m *WorkerMonitor) InitFailed(ctx context.Context, detail string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.lastErr = detail
	m.initFailed = true
	alreadyStalled := m.stalled
	m.stalled = true
	m.mu.Unlock()

	if !alreadyStalled && m.Client != nil {
		m.Client.Send(ctx, WorkerStalled(m.Name, 0, "init permanently failed: "+detail))
	}
}

// MonitorSnapshot is the /admin/epg/health view of one worker.
type MonitorSnapshot struct {
	Name        string     `json:"name"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	LastSuccess *time.Time `json:"last_success,omitempty"`
	LastError   string     `json:"last_error,omitempty"`
	Stalled     bool       `json:"stalled"`
	InitFailed  bool       `json:"init_failed"`
}

// Snapshot returns the current state. Safe on a nil monitor (returns a
// zero-value snapshot with Name "").
func (m *WorkerMonitor) Snapshot() MonitorSnapshot {
	if m == nil {
		return MonitorSnapshot{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	snap := MonitorSnapshot{
		Name:       m.Name,
		LastError:  m.lastErr,
		Stalled:    m.stalled,
		InitFailed: m.initFailed,
	}
	if !m.started.IsZero() {
		t := m.started
		snap.StartedAt = &t
		// lastSuccess == started means "no real success yet" — report it
		// anyway; consumers can compare against started_at.
		s := m.lastSuccess
		snap.LastSuccess = &s
	}
	return snap
}
