// Package metrics is Conductor's Prometheus exposition (audit Q1, the
// minimal Phase 6 cut): process-lifetime counters for the streaming
// resilience layer plus scrape-time gauges backed by cheap DB queries.
//
// Deliberately hand-rolled instead of importing client_golang: the repo
// keeps direct dependencies lean (four today), we only need counters and
// gauges, and the text exposition format for those is a stable dozen lines
// of formatting. If histograms ever become necessary, switch to the real
// client library rather than growing this.
//
// Hot-path discipline (AGENTS.md): the shared-pump fanout does no metric
// allocation or logging. Isolated subscriber workers update atomic delivery
// counters per chunk; upstream byte counts still flush on the existing
// 3-second heartbeat cadence.
package metrics

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Counter is a monotonically increasing metric. Safe for concurrent use.
type Counter struct {
	name string
	help string
	v    atomic.Uint64
}

func (c *Counter) Inc()          { c.v.Add(1) }
func (c *Counter) Add(n uint64)  { c.v.Add(n) }
func (c *Counter) Value() uint64 { return c.v.Load() }

// Sample is one labeled gauge reading. Labels is the pre-rendered inner
// label set (e.g. `source="iboost-epg"`), empty for an unlabeled sample.
type Sample struct {
	Labels string
	Value  float64
}

type gauge struct {
	name string
	help string
	fn   func() []Sample
}

var (
	mu       sync.Mutex
	counters []*Counter
	gauges   []gauge
)

// NewCounter creates and registers a counter. Call from package init /
// var blocks only — names must be unique.
func NewCounter(name, help string) *Counter {
	c := &Counter{name: name, help: help}
	mu.Lock()
	counters = append(counters, c)
	mu.Unlock()
	return c
}

// RegisterGauge registers a single-sample gauge evaluated at scrape time.
// fn must be safe for concurrent calls and bound its own latency (DB-backed
// gauges use a short context internally and return no sample on error).
func RegisterGauge(name, help string, fn func() (float64, bool)) {
	RegisterGaugeVec(name, help, func() []Sample {
		if v, ok := fn(); ok {
			return []Sample{{Value: v}}
		}
		return nil
	})
}

// RegisterGaugeVec registers a labeled gauge evaluated at scrape time.
func RegisterGaugeVec(name, help string, fn func() []Sample) {
	mu.Lock()
	gauges = append(gauges, gauge{name: name, help: help, fn: fn})
	mu.Unlock()
}

// Stream-path counters registered at package load. Shared-pump event counters
// are rare; isolated delivery workers update the subscriber counters per
// chunk with allocation-free atomics.
var (
	StreamsStarted                  = NewCounter("conductor_streams_started_total", "Upstream pump goroutines launched.")
	StreamsFailed                   = NewCounter("conductor_streams_failed_total", "Pumps that exited with an error (after exhausting reconnects).")
	ReconnectAttempts               = NewCounter("conductor_stream_reconnects_total", "Mid-stream reconnect attempts after an upstream failure.")
	Failovers                       = NewCounter("conductor_stream_failovers_total", "Reconnects that relocated to a different channel source.")
	FFmpegRestarts                  = NewCounter("conductor_ffmpeg_restarts_total", "Transcode ffmpeg respawns while the upstream stayed healthy.")
	TranscodeOutputStalls           = NewCounter("conductor_transcode_output_stalls_total", "Validated transcode attempts restarted after FFmpeg stdout stopped while the provider attempt remained open.")
	SubscriberStalls                = NewCounter("conductor_subscriber_stalls_total", "Subscriber sink deliveries that blocked and recovered within the end-to-end queue-age budget.")
	SubscriberDrops                 = NewCounter("conductor_subscriber_drops_total", "Subscribers disconnected for exceeding the fanout stall budget.")
	SubscriberStallMilliseconds     = NewCounter("conductor_subscriber_stall_milliseconds_total", "Wall milliseconds spent waiting on isolated subscriber sinks before recovery or disconnect.")
	SubscriberDeliveries            = NewCounter("conductor_subscriber_deliveries_total", "Media chunks delivered from an isolated subscriber queue to its sink.")
	SubscriberDeliveryMillis        = NewCounter("conductor_subscriber_delivery_milliseconds_total", "Queue-age milliseconds accumulated across delivered subscriber chunks.")
	SlateBytes                      = NewCounter("conductor_slate_bytes_total", "Source Unavailable placeholder bytes fed to subscribers during outages.")
	SlateHolds                      = NewCounter("conductor_slate_holds_total", "Times a channel exhausted every source in a gap and held viewers on the slate.")
	UpstreamBytes                   = NewCounter("conductor_upstream_bytes_total", "Bytes read from upstream providers (flushed on the heartbeat cadence).")
	UpstreamReadGaps                = NewCounter("conductor_upstream_read_gaps_total", "Provider input gaps of at least one second observed between media reads.")
	UpstreamReadGapMilliseconds     = NewCounter("conductor_upstream_read_gap_milliseconds_total", "Wall milliseconds accumulated across provider input gaps of at least one second.")
	RelayOutputGaps                 = NewCounter("conductor_relay_output_gaps_total", "Passthrough or ffmpeg output gaps of at least one second before fanout.")
	RelayOutputGapMilliseconds      = NewCounter("conductor_relay_output_gap_milliseconds_total", "Wall milliseconds accumulated across relay output gaps of at least one second.")
	RelayPacingMilliseconds         = NewCounter("conductor_relay_pacing_milliseconds_total", "Wall milliseconds spent pacing buffered MPEG-TS output against its PCR clock.")
	RelayPacingLatencyCaps          = NewCounter("conductor_relay_pacing_latency_caps_total", "Live MPEG-TS chunks whose PCR target exceeded the bounded latency allowed after source arrival.")
	RelayPacingCappedMillis         = NewCounter("conductor_relay_pacing_capped_milliseconds_total", "PCR-scheduled milliseconds discarded by the live source-arrival latency cap.")
	RelayClockResets                = NewCounter("conductor_relay_clock_resets_total", "PCR pacing epochs reset after transport discontinuity, underflow, or implausible clock movement.")
	RelayTransportResyncs           = NewCounter("conductor_relay_transport_resyncs_total", "Bounded mid-stream TS sync-loss realignments that continued the attempt instead of tearing it down.")
	RelaySkipAheadEpisodes          = NewCounter("conductor_relay_skip_ahead_episodes_total", "Live-edge skip episodes: stale backlog discarded with declared discontinuities instead of failing the attempt.")
	RelaySkipAheadChunks            = NewCounter("conductor_relay_skip_ahead_chunks_total", "Media chunks discarded by live-edge skip episodes.")
	RelayRecoveryEpochs             = NewCounter("conductor_relay_recovery_epochs_total", "Live MPEG-TS pacing epochs accelerated to drain a validated prefix or a shared-arrival batch.")
	RelayUnderflowReanchors         = NewCounter("conductor_relay_underflow_reanchors_total", "Live pacing schedules re-anchored after a source pause outlived the queued lead; the returning media is kept as lead, not raced to the live edge.")
	RelayLeadTrimEpochs             = NewCounter("conductor_relay_lead_trim_epochs_total", "Bounded 2x pacing epochs that trimmed a retained lead back below the reservoir high-water mark.")
	RelayRecoveryAcceleratedMS      = NewCounter("conductor_relay_recovery_accelerated_milliseconds_total", "PCR-scheduled milliseconds compressed by bounded live recovery pacing.")
	RelayRecoveryBacklogFailures    = NewCounter("conductor_relay_recovery_backlog_failures_total", "Live MPEG-TS pacing rejected when its latency bound would require more than the maximum recovery rate.")
	RelayBacklogFailures            = NewCounter("conductor_relay_backlog_failures_total", "Buffered MPEG-TS tails rejected before a shared arrival deadline would force zero-interval fanout.")
	RelayStaleChunkFailures         = NewCounter("conductor_relay_stale_chunk_failures_total", "Live MPEG-TS chunks rejected after exceeding the source-arrival latency bound before fanout.")
	RelayStaleChunkMilliseconds     = NewCounter("conductor_relay_stale_chunk_milliseconds_total", "Source-arrival latency milliseconds beyond the live bound accumulated across rejected MPEG-TS chunks.")
	InputAVDriftEvents              = NewCounter("conductor_input_av_drift_events_total", "Upstream MPEG-TS attempts ending with at least 250 ms of audio-versus-video PTS span drift.")
	InputAVDriftMilliseconds        = NewCounter("conductor_input_av_drift_milliseconds_total", "Absolute upstream audio-versus-video PTS drift milliseconds accumulated across drift events.")
	OutputAVDriftEvents             = NewCounter("conductor_output_av_drift_events_total", "Relay MPEG-TS attempts ending with at least 250 ms of audio-versus-video PTS span drift.")
	OutputAVDriftMilliseconds       = NewCounter("conductor_output_av_drift_milliseconds_total", "Absolute relay audio-versus-video PTS drift milliseconds accumulated across drift events.")
	InputAudioStarvationEvents      = NewCounter("conductor_input_audio_starvation_total", "Upstream attempts relocated after video PTS activity continued while established audio PTS activity stopped.")
	InputVideoStarvationEvents      = NewCounter("conductor_input_video_starvation_total", "Upstream attempts relocated after audio PTS activity continued while established video PTS activity stopped.")
	InputAVClockStarvationEvents    = NewCounter("conductor_input_av_clock_starvation_total", "Upstream attempts relocated after transport bytes continued without either established A/V PES clock.")
	InputAVContinuityDriftEvents    = NewCounter("conductor_input_av_continuity_drift_total", "Upstream passthrough attempts relocated after sustained audio/video PTS span divergence.")
	InputAVContinuitySkewEvents     = NewCounter("conductor_input_av_continuity_skew_total", "Upstream passthrough attempts relocated after sustained pathological absolute audio/video PTS skew.")
	InputTransportCorruptionEvents  = NewCounter("conductor_input_transport_corruption_total", "Upstream attempts relocated after sustained selected-program TS continuity, TEI, or scrambling faults.")
	OutputAudioStarvationEvents     = NewCounter("conductor_output_audio_starvation_total", "Local relay attempts restarted after video PTS activity continued while established audio PTS activity stopped.")
	OutputVideoStarvationEvents     = NewCounter("conductor_output_video_starvation_total", "Local relay attempts restarted after audio PTS activity continued while established video PTS activity stopped.")
	OutputAVClockStarvationEvents   = NewCounter("conductor_output_av_clock_starvation_total", "Local relay attempts restarted after transport bytes continued without either established A/V PES clock.")
	OutputAVContinuityDriftEvents   = NewCounter("conductor_output_av_continuity_drift_total", "Local relay attempts restarted after sustained audio/video PTS span divergence.")
	OutputAVContinuitySkewEvents    = NewCounter("conductor_output_av_continuity_skew_total", "Local relay attempts restarted after sustained pathological absolute audio/video PTS skew.")
	OutputTransportCorruptionEvents = NewCounter("conductor_output_transport_corruption_total", "Local relay attempts restarted after sustained selected-program TS continuity, TEI, or scrambling faults.")
)

var DVRArtifactStartupInspectionDetachments = NewCounter(
	"conductor_dvr_artifact_startup_inspection_detachments_total",
	"Startup DVR artifact inspection workers not observed finished when the aggregate deadline detached startup from them.")

var DVRAdmissionPostCommitObservationFailures = NewCounter(
	"conductor_dvr_admission_postcommit_observation_failures_total",
	"Committed DVR intents returned from transaction evidence after their advisory forecast refresh or reload failed.")

// Handler serves the Prometheus text exposition format (v0.0.4).
func Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		cs := make([]*Counter, len(counters))
		copy(cs, counters)
		gs := make([]gauge, len(gauges))
		copy(gs, gauges)
		mu.Unlock()

		var b strings.Builder
		for _, c := range cs {
			writeHeader(&b, c.name, c.help, "counter")
			fmt.Fprintf(&b, "%s %d\n", c.name, c.Value())
		}
		// Stable output order for gauges registered from init paths.
		sort.SliceStable(gs, func(i, j int) bool { return gs[i].name < gs[j].name })
		for _, g := range gs {
			samples := g.fn()
			if len(samples) == 0 {
				continue
			}
			writeHeader(&b, g.name, g.help, "gauge")
			for _, s := range samples {
				if s.Labels == "" {
					fmt.Fprintf(&b, "%s %s\n", g.name, formatFloat(s.Value))
				} else {
					fmt.Fprintf(&b, "%s{%s} %s\n", g.name, s.Labels, formatFloat(s.Value))
				}
			}
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = w.Write([]byte(b.String()))
	})
}

func writeHeader(b *strings.Builder, name, help, typ string) {
	help = strings.ReplaceAll(help, "\\", `\\`)
	help = strings.ReplaceAll(help, "\n", `\n`)
	fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, typ)
}

func formatFloat(v float64) string {
	return strconv.FormatFloat(v, 'g', -1, 64)
}

// EscapeLabelValue renders a string safe for use inside a label value.
func EscapeLabelValue(s string) string {
	s = strings.ReplaceAll(s, "\\", `\\`)
	s = strings.ReplaceAll(s, "\n", `\n`)
	return strings.ReplaceAll(s, `"`, `\"`)
}

// Label renders one key="value" pair with correct escaping. Use this for
// Sample.Labels instead of fmt.Sprintf with %q — %q on an already-escaped
// value double-escapes backslashes (review finding).
func Label(key, value string) string {
	return key + `="` + EscapeLabelValue(value) + `"`
}
