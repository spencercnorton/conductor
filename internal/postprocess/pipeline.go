// Package postprocess runs after a DVR recording completes. Stages execute
// in order; one stage's failure does NOT abort the next stage (best-effort
// completion is more useful than fail-fast for this pipeline — even if
// commskip fails, the recording is still importable).
//
// Built-in stages:
//
//   commskip-auto    POSTs the file to the external commskip-auto service,
//                    which runs Comskip + Whisper + LLM as a hybrid
//                    pipeline. Produces .edl and .srt sidecars next to
//                    the recording.
//
//   webhook          Generic POST hook. Body = JSON {file_path, bytes,
//                    title, …}. Useful for plugging in additional tools
//                    (custom transcoders, integrity checkers, etc.)
//                    without writing Go.
//
// The pipeline is invoked by Recorder.Run when a DVR recording finishes
// (state=completed). When state=failed the pipeline is skipped — there's
// no usable file to process.
package postprocess

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// Stage is one unit of work in the pipeline.
type Stage interface {
	Name() string
	Run(ctx context.Context, in *Input) StageResult
}

// Input is what every stage receives. The set of fields was chosen to be
// what stages actually need; expand as new stages are added.
type Input struct {
	RecordingID    string
	FilePath       string         // absolute path to the recorded .ts
	Title          string
	SubTitle       string
	EpisodeOnscreen string        // "S03E07" or ""
	IsMovie        bool
	BytesWritten   int64
}

// StageResult is what every stage returns.
type StageResult struct {
	Stage    string
	Success  bool
	Duration time.Duration
	Detail   string // success: short note; failure: error message
}

// Pipeline is an ordered list of stages.
type Pipeline struct {
	Logger *slog.Logger
	Stages []Stage
}

// Run executes the pipeline. Always runs every stage even if earlier ones
// fail. Returns aggregate stats; per-stage results are in the slice.
func (p *Pipeline) Run(ctx context.Context, in *Input) (results []StageResult, allOK bool) {
	allOK = true
	for _, s := range p.Stages {
		select {
		case <-ctx.Done():
			results = append(results, StageResult{
				Stage: s.Name(), Success: false, Detail: "cancelled",
			})
			allOK = false
			continue
		default:
		}

		t0 := time.Now()
		r := s.Run(ctx, in)
		r.Duration = time.Since(t0)
		if r.Stage == "" {
			r.Stage = s.Name()
		}

		if r.Success {
			p.Logger.Info("postprocess stage ok",
				"recording", in.RecordingID, "stage", r.Stage,
				"dur_ms", r.Duration.Milliseconds(), "detail", r.Detail)
		} else {
			p.Logger.Warn("postprocess stage failed",
				"recording", in.RecordingID, "stage", r.Stage,
				"dur_ms", r.Duration.Milliseconds(), "detail", r.Detail)
			allOK = false
		}
		results = append(results, r)
	}
	return results, allOK
}

// SummaryLog joins per-stage results into a single line for the
// postprocess_run.log column. Format: "stage=ok(123ms) stage2=fail(456ms: msg)".
func SummaryLog(results []StageResult) string {
	var b strings.Builder
	for i, r := range results {
		if i > 0 {
			b.WriteString(" ")
		}
		fmt.Fprintf(&b, "%s=", r.Stage)
		if r.Success {
			fmt.Fprintf(&b, "ok(%dms)", r.Duration.Milliseconds())
		} else {
			fmt.Fprintf(&b, "fail(%dms: %s)", r.Duration.Milliseconds(), r.Detail)
		}
	}
	return b.String()
}

// failResult is a small ctor for stages.
func failResult(name, msg string) StageResult { return StageResult{Stage: name, Success: false, Detail: msg} }
func okResult(name, msg string) StageResult   { return StageResult{Stage: name, Success: true, Detail: msg} }

// keep errors imported for downstream files in this package.
var _ = errors.New
