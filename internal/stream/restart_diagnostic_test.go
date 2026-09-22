package stream

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/spencercnorton/conductor/internal/metrics"
)

// Exercise the logger/counter branch actually used by Run. Guard refusals still
// respawn a pipeline and must retain the existing burst-alert signal, but do not
// prove that FFmpeg died. Wrapping mirrors the production attempt diagnostic.
func TestLocalPipelineRestartNamesGuardWithoutLosingCounter(t *testing.T) {
	cases := []struct {
		name, stage, reason, want string
		cause                     error
		count                     uint64
	}{
		{"output clock", "output_clock", "output_clock_boundary", "output clock join refused; restarting local pipeline", errOutputClockBoundary, 1},
		{"publication AV", "output_av_publication", "timeline_skew", "output A/V publication guard refused media; restarting local pipeline", errors.New("test timeline skew"), 1},
		{"output stall", "stream", "transcode_output_idle", "transcode output stalled; restarting local pipeline", errTranscodeOutputIdle, 1},
		{"process exit", "stream", "local_failure", "ffmpeg exited mid-stream; respawning", errors.New("exit status 1"), 1},
		{"bounded relay queue", "transport_pace", "relay_read_ahead_full", "relay output buffer exhausted; restarting local pipeline", errRelayReadAheadFull, 0},
		{"pacing boundary", "transport_pace", "transport_attempt_boundary", "relay pacing backlog exceeded live bound; restarting local pipeline", errTransportAttemptBoundary, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			s := &TranscodeStreamer{id: "diagnostic-test", logger: slog.New(slog.NewJSONHandler(&out, nil))}
			cause := &sourceAttemptError{stage: tc.stage, reason: tc.reason, host: "provider.test", cause: localTranscodeFailure(tc.cause)}
			before := metrics.FFmpegRestarts.Value()
			s.logLocalPipelineRestart(fmt.Errorf("wrapped attempt: %w", cause))
			if got := metrics.FFmpegRestarts.Value() - before; got != tc.count {
				t.Fatalf("restart aggregate=%d want%d", got, tc.count)
			}
			var row map[string]any
			if err := json.Unmarshal(bytes.TrimSpace(out.Bytes()), &row); err != nil {
				t.Fatal(err)
			}
			if row["msg"] != tc.want || row["level"] != "WARN" {
				t.Fatalf("misleading restart log: %s", out.String())
			}
			if !strings.Contains(row["err"].(string), tc.stage) {
				t.Fatalf("lost safe attempt stage: %s", out.String())
			}
		})
	}
}
