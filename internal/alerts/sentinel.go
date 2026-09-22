package alerts

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/spencercnorton/conductor/internal/metrics"
)

// Sentinel is the continuous stream-health canary (2026-08-29 stability
// program, goal 1 layer C): it samples conductor's own counters on a fixed
// cadence and pages the ops bot when teardown-class activity spikes. Every
// regression this month was found by a human noticing Plex misbehave and
// reconstructing from logs afterwards; the counters already told the story
// in real time — nothing was watching them.
//
// Deliberately threshold-on-deltas over a short sliding window rather than
// a metrics stack: no Prometheus deployment exists for this estate, and the
// ops-bot webhook is already the alert path for EPG source failures.
type Sentinel struct {
	Client   *Client // nil-safe; Send no-ops without a webhook URL
	Logger   *slog.Logger
	Interval time.Duration // sample cadence; default 1 min
	Window   int           // samples per evaluation window; default 10
	Realert  time.Duration // per-rule dampening; default 30 min

	rules []sentinelRule
	now   func() time.Time // test seam
}

type sentinelRule struct {
	name      string
	severity  Severity
	threshold uint64 // delta over the window that trips the rule
	read      func() uint64
	describe  func(delta uint64, window time.Duration) string

	ring      []uint64 // previous samples (len Window+1)
	filled    int
	lastAlert time.Time
}

// NewSentinel wires the default rule set against the live metrics counters.
func NewSentinel(client *Client, logger *slog.Logger) *Sentinel {
	s := &Sentinel{Client: client, Logger: logger}
	s.applyDefaults()
	starvation := func() uint64 {
		return metrics.InputAudioStarvationEvents.Value() +
			metrics.InputVideoStarvationEvents.Value() +
			metrics.InputAVClockStarvationEvents.Value() +
			metrics.OutputAudioStarvationEvents.Value() +
			metrics.OutputVideoStarvationEvents.Value() +
			metrics.OutputAVClockStarvationEvents.Value()
	}
	s.rules = []sentinelRule{
		{
			name: "failover_burst", severity: SevError, threshold: 3,
			read: metrics.Failovers.Value,
			describe: func(d uint64, w time.Duration) string {
				return fmt.Sprintf("%d mid-stream source failovers in the last %s — viewers are seeing reconnects", d, w)
			},
		},
		{
			name: "slate_hold_burst", severity: SevError, threshold: 3,
			read: metrics.SlateHolds.Value,
			describe: func(d uint64, w time.Duration) string {
				return fmt.Sprintf("%d source-exhausted slate holds in the last %s — channels are failing to load", d, w)
			},
		},
		{
			name: "ffmpeg_restart_burst", severity: SevWarn, threshold: 3,
			read: metrics.FFmpegRestarts.Value,
			describe: func(d uint64, w time.Duration) string {
				return fmt.Sprintf("%d transcode pipeline restarts in the last %s", d, w)
			},
		},
		{
			name: "starvation", severity: SevError, threshold: 1,
			read: starvation,
			describe: func(d uint64, w time.Duration) string {
				return fmt.Sprintf("%d A/V starvation faults in the last %s", d, w)
			},
		},
	}
	return s
}

func (s *Sentinel) applyDefaults() {
	if s.Interval <= 0 {
		s.Interval = time.Minute
	}
	if s.Window <= 0 {
		s.Window = 10
	}
	if s.Realert <= 0 {
		s.Realert = 30 * time.Minute
	}
	if s.now == nil {
		s.now = time.Now
	}
}

// Run blocks until ctx is cancelled, sampling every Interval.
func (s *Sentinel) Run(ctx context.Context) {
	s.applyDefaults()
	s.Logger.Info("stream-health sentinel starting",
		"interval", s.Interval, "window_samples", s.Window, "rules", len(s.rules))
	t := time.NewTicker(s.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			s.Logger.Info("stream-health sentinel stopped")
			return
		case <-t.C:
			s.sample(ctx)
		}
	}
}

// sample records one reading per rule and alerts on any rule whose delta
// across the full window meets its threshold, dampened per rule.
func (s *Sentinel) sample(ctx context.Context) {
	window := time.Duration(s.Window) * s.Interval
	now := s.now()
	for i := range s.rules {
		r := &s.rules[i]
		if r.ring == nil {
			r.ring = make([]uint64, s.Window+1)
		}
		copy(r.ring, r.ring[1:])
		r.ring[len(r.ring)-1] = r.read()
		if r.filled < len(r.ring) {
			r.filled++
			if r.filled < len(r.ring) {
				// Not enough history for a full-window delta yet; comparing
				// against a zero-initialized slot would read process-lifetime
				// totals as a fresh burst on every start. Once this sample
				// completes the window, ring[0] is the first REAL sample and
				// evaluation proceeds (review follow-up: the old form
				// waited one extra sample).
				continue
			}
		}
		delta := r.ring[len(r.ring)-1] - r.ring[0]
		if delta < r.threshold {
			continue
		}
		if now.Sub(r.lastAlert) < s.Realert {
			continue
		}
		r.lastAlert = now
		s.Client.Send(ctx, Event{
			Severity: r.severity,
			Source:   "conductor.sentinel",
			Title:    "Stream health: " + r.describe(delta, window),
			Tags:     map[string]any{"rule": r.name, "delta": delta, "window": window.String()},
		})
		s.Logger.Warn("stream-health sentinel tripped",
			"rule", r.name, "delta", delta, "window", window)
	}
}
