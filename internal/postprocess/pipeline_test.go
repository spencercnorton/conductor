package postprocess_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/spencercnorton/conductor/internal/postprocess"
)

type fakeStage struct {
	name    string
	success bool
	detail  string
	delay   time.Duration
	calls   int
}

func (f *fakeStage) Name() string { return f.name }
func (f *fakeStage) Run(ctx context.Context, _ *postprocess.Input) postprocess.StageResult {
	f.calls++
	if f.delay > 0 {
		time.Sleep(f.delay)
	}
	return postprocess.StageResult{Stage: f.name, Success: f.success, Detail: f.detail}
}

func newLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

func TestPipelineRunsAllStages(t *testing.T) {
	a := &fakeStage{name: "a", success: true, detail: "edl=foo.edl"}
	b := &fakeStage{name: "b", success: true, detail: "srt=foo.srt"}
	p := &postprocess.Pipeline{Logger: newLogger(), Stages: []postprocess.Stage{a, b}}

	results, allOK := p.Run(context.Background(), &postprocess.Input{
		RecordingID: "r1", FilePath: "/tmp/foo.ts",
	})
	if !allOK {
		t.Error("expected allOK=true")
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results; got %d", len(results))
	}
	if a.calls != 1 || b.calls != 1 {
		t.Errorf("each stage should run exactly once; got a=%d b=%d", a.calls, b.calls)
	}
}

func TestPipelineContinuesPastFailure(t *testing.T) {
	// Earlier failure must NOT short-circuit the pipeline. commskip-auto
	// failing shouldn't suppress a later "notify Sonarr" stage, etc.
	a := &fakeStage{name: "a", success: false, detail: "boom"}
	b := &fakeStage{name: "b", success: true}
	p := &postprocess.Pipeline{Logger: newLogger(), Stages: []postprocess.Stage{a, b}}

	results, allOK := p.Run(context.Background(), &postprocess.Input{})
	if allOK {
		t.Error("allOK should be false when any stage fails")
	}
	if b.calls != 1 {
		t.Errorf("stage b should still run after a failed; got b.calls=%d", b.calls)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 results; got %d", len(results))
	}
}

func TestPipelineRespectsContextCancellation(t *testing.T) {
	a := &fakeStage{name: "a", success: true}
	b := &fakeStage{name: "b", success: true}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already done

	p := &postprocess.Pipeline{Logger: newLogger(), Stages: []postprocess.Stage{a, b}}
	results, allOK := p.Run(ctx, &postprocess.Input{})
	if allOK {
		t.Error("allOK should be false when ctx already done")
	}
	if a.calls != 0 || b.calls != 0 {
		t.Errorf("stages should not run when ctx already cancelled; a=%d b=%d", a.calls, b.calls)
	}
	for _, r := range results {
		if r.Detail != "cancelled" {
			t.Errorf("expected detail=cancelled; got %q", r.Detail)
		}
	}
}

func TestSummaryLogFormat(t *testing.T) {
	results := []postprocess.StageResult{
		{Stage: "a", Success: true, Duration: 50 * time.Millisecond, Detail: "edl=x"},
		{Stage: "b", Success: false, Duration: 100 * time.Millisecond, Detail: "boom"},
	}
	got := postprocess.SummaryLog(results)
	for _, want := range []string{
		"a=ok(50ms)",
		"b=fail(100ms: boom)",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("summary missing %q\n  full: %s", want, got)
		}
	}
}

// _ keeps errors imported (mirrors pattern in pipeline.go).
var _ = errors.New
