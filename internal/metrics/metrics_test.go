package metrics

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"
)

func scrape(t *testing.T) string {
	t.Helper()
	rec := httptest.NewRecorder()
	Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body, _ := io.ReadAll(rec.Result().Body)
	return string(body)
}

func TestExpositionFormat(t *testing.T) {
	c := NewCounter("test_widgets_total", "Widgets made.")
	c.Add(41)
	c.Inc()

	RegisterGauge("test_pressure", "Pascals.", func() (float64, bool) { return 3.5, true })
	RegisterGauge("test_absent", "Never present.", func() (float64, bool) { return 0, false })
	RegisterGaugeVec("test_age_seconds", "Per-thing age.", func() []Sample {
		return []Sample{
			{Labels: `thing="a"`, Value: 1},
			{Labels: `thing="b"`, Value: -1},
		}
	})

	out := scrape(t)

	for _, want := range []string{
		"# HELP test_widgets_total Widgets made.\n# TYPE test_widgets_total counter\ntest_widgets_total 42\n",
		"# TYPE test_pressure gauge\ntest_pressure 3.5\n",
		"test_age_seconds{thing=\"a\"} 1\n",
		"test_age_seconds{thing=\"b\"} -1\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("exposition missing %q\n--- got:\n%s", want, out)
		}
	}
	if strings.Contains(out, "test_absent") {
		t.Error("gauge that returned ok=false must be omitted entirely")
	}
	// The built-in stream counters must always be present (zero-valued).
	if !strings.Contains(out, "conductor_streams_started_total") {
		t.Error("built-in counters missing from exposition")
	}
}

func TestEscapeLabelValue(t *testing.T) {
	if got := EscapeLabelValue(`a"b\c` + "\n"); got != `a\"b\\c\n` {
		t.Fatalf("escape = %q", got)
	}
}

func TestLabelNoDoubleEscape(t *testing.T) {
	// Regression for the review finding: %q over an already-escaped
	// value emitted \\" instead of \". Label must escape exactly once.
	if got := Label("source", `say "hi" \ bye`); got != `source="say \"hi\" \\ bye"` {
		t.Fatalf("Label = %q", got)
	}
	if got := Label("source", "plain name"); got != `source="plain name"` {
		t.Fatalf("Label plain = %q", got)
	}
	// And end-to-end through the exposition.
	RegisterGaugeVec("test_label_escape", "x", func() []Sample {
		return []Sample{{Labels: Label("name", `q"v`), Value: 1}}
	})
	if out := scrape(t); !strings.Contains(out, `test_label_escape{name="q\"v"} 1`) {
		t.Fatalf("exposition has wrong escaping:\n%s", out)
	}
}

func TestGapMetricsUseOneUnlabeledSeriesEach(t *testing.T) {
	out := scrape(t)
	for _, name := range []string{
		"conductor_upstream_read_gaps_total",
		"conductor_upstream_read_gap_milliseconds_total",
		"conductor_relay_output_gaps_total",
		"conductor_relay_output_gap_milliseconds_total",
		"conductor_subscriber_deliveries_total",
		"conductor_subscriber_delivery_milliseconds_total",
		"conductor_relay_pacing_milliseconds_total",
		"conductor_relay_pacing_latency_caps_total",
		"conductor_relay_pacing_capped_milliseconds_total",
		"conductor_relay_clock_resets_total",
		"conductor_relay_recovery_epochs_total",
		"conductor_relay_recovery_accelerated_milliseconds_total",
		"conductor_relay_recovery_backlog_failures_total",
		"conductor_relay_backlog_failures_total",
		"conductor_relay_stale_chunk_failures_total",
		"conductor_relay_stale_chunk_milliseconds_total",
		"conductor_input_av_drift_events_total",
		"conductor_input_av_drift_milliseconds_total",
		"conductor_output_av_drift_events_total",
		"conductor_output_av_drift_milliseconds_total",
		"conductor_input_audio_starvation_total",
		"conductor_input_video_starvation_total",
		"conductor_input_av_clock_starvation_total",
		"conductor_input_av_continuity_drift_total",
		"conductor_input_av_continuity_skew_total",
		"conductor_input_transport_corruption_total",
		"conductor_output_audio_starvation_total",
		"conductor_output_video_starvation_total",
		"conductor_output_av_clock_starvation_total",
		"conductor_output_av_continuity_drift_total",
		"conductor_output_av_continuity_skew_total",
		"conductor_output_transport_corruption_total",
		"conductor_transcode_output_stalls_total",
	} {
		var samples int
		for _, line := range strings.Split(out, "\n") {
			if strings.HasPrefix(line, name+"{") {
				t.Errorf("%s unexpectedly carries labels: %q", name, line)
			}
			if strings.HasPrefix(line, name+" ") {
				samples++
			}
		}
		if samples != 1 {
			t.Errorf("%s sample count = %d, want exactly one process-wide series", name, samples)
		}
	}
}

func TestDVRArtifactStartupInspectionDetachmentsMetric(t *testing.T) {
	const name = "conductor_dvr_artifact_startup_inspection_detachments_total"
	out := scrape(t)
	if !strings.Contains(out, "# TYPE "+name+" counter\n") {
		t.Fatalf("detachment counter type missing from exposition:\n%s", out)
	}
	var samples int
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, name+"{") {
			t.Fatalf("detachment counter unexpectedly carries labels: %q", line)
		}
		if strings.HasPrefix(line, name+" ") {
			samples++
		}
	}
	if samples != 1 {
		t.Fatalf("detachment counter samples=%d, want exactly one", samples)
	}
}

func TestDVRAdmissionPostCommitObservationFailuresMetric(t *testing.T) {
	const name = "conductor_dvr_admission_postcommit_observation_failures_total"
	out := scrape(t)
	if !strings.Contains(out, "# TYPE "+name+" counter\n") {
		t.Fatalf("post-commit observation counter type missing from exposition:\n%s", out)
	}
	var samples int
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, name+"{") {
			t.Fatalf("post-commit observation counter unexpectedly carries labels: %q", line)
		}
		if strings.HasPrefix(line, name+" ") {
			samples++
		}
	}
	if samples != 1 {
		t.Fatalf("post-commit observation counter samples=%d, want exactly one", samples)
	}
}
