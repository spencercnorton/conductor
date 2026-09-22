package dvr

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/store"
	"github.com/spencercnorton/conductor/internal/stream"
)

// TestDecideOutcome pins the salvage rule: an upstream failure AFTER the
// program's scheduled window has elapsed must keep the capture, not bin it.
// Two real recordings (Body Cam S11E02, President Curtis S01E01) were marked
// 'failed' and abandoned as .partial despite holding the complete program.
func TestDecideOutcome(t *testing.T) {
	upstreamDead := errors.New("transcode pipeline down for 1m33s, giving up: unexpected EOF")

	tests := []struct {
		name    string
		err     error
		bytes   int64
		covered bool
		want    outcome
	}{
		{"clean eof", nil, 1 << 30, true, outcomeComplete},
		{"clean eof after late admission", nil, 1 << 30, false, outcomeFailed},
		{"deadline hit", context.DeadlineExceeded, 1 << 30, true, outcomeComplete},
		{"deadline after late admission", context.DeadlineExceeded, 1 << 30, false, outcomeFailed},
		{"operator cancel", context.Canceled, 1 << 30, false, outcomeCancelled},

		// The regression: 2.1 min past scheduled_end, 1.9 GB on disk.
		{"upstream died in post-roll padding", upstreamDead, 1939824044, true, outcomeSalvage},

		// Still genuinely failed: died mid-program, or produced nothing.
		{"upstream died mid-program", upstreamDead, 1 << 30, false, outcomeFailed},
		{"no slot, zero bytes", errors.New("all sources exhausted"), 0, false, outcomeFailed},
		{"zero bytes after end is not salvageable", upstreamDead, 0, true, outcomeFailed},

		// Cancel must win over salvage — an operator stopping a recording in
		// its padding should not be recorded as a success.
		{"cancel after end still cancels", context.Canceled, 1 << 30, true, outcomeCancelled},

		// A local write failure is never salvageable, however complete the
		// coverage looks: the bytes on disk are not what we think they are.
		{"disk write failed with full coverage",
			fmt.Errorf("%w: %w", stream.ErrSinkWrite, errors.New("no space left on device")),
			1 << 30, true, outcomeFailed},
		{"disk write failed mid-program", fmt.Errorf("%w: %w", stream.ErrSinkWrite,
			errors.New("input/output error")), 1 << 30, false, outcomeFailed},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := decideOutcome(tc.err, tc.bytes, tc.covered); got != tc.want {
				t.Fatalf("decideOutcome = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestValidateDVRRealMediaCoverageRejectsMidProgramOutage(t *testing.T) {
	gapStart := time.Date(2026, 8, 23, 10, 25, 0, 0, time.UTC)
	err := validateDVRRealMediaCoverage(stream.DVRMediaCoverage{
		MaxRealGap:  time.Minute,
		MaxGapStart: gapStart,
		MaxGapEnd:   gapStart.Add(time.Minute),
	})
	if !errors.Is(err, ErrRecordingRealMediaGap) ||
		!strings.Contains(err.Error(), "gap=1m0s") {
		t.Fatalf("mid-program coverage err=%v", err)
	}
	if err := validateDVRRealMediaCoverage(stream.DVRMediaCoverage{
		MaxRealGap: maxDVRRealMediaGap,
	}); err != nil {
		t.Fatalf("bounded chunk jitter rejected: %v", err)
	}
}

// A successful COMMIT whose response is lost must be retried with the same
// durable operation identity on a fresh context. Trusting the provisional
// result from the failed call or generating a new identity would make the
// recorder either guess or mistake its own completed row for a foreign one.
func TestMarkCompletedReconcilesAmbiguousCommitWithFreshContext(t *testing.T) {
	recorder := &Recorder{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	recordingID := uuid.New()
	simulated := errors.New("simulated lost COMMIT response")
	var operationID uuid.UUID
	var firstCtx context.Context
	calls := 0

	state, err := recorder.markCompletedWith(recordingID, 4321, func(
		ctx context.Context,
		gotRecordingID, gotOperationID uuid.UUID,
		gotBytes int64,
	) (store.DVRCompletionResult, error) {
		calls++
		if gotRecordingID != recordingID || gotBytes != 4321 {
			t.Fatalf("attempt %d recording=%s bytes=%d", calls, gotRecordingID, gotBytes)
		}
		if _, ok := ctx.Deadline(); !ok || ctx.Err() != nil {
			t.Fatalf("attempt %d did not receive a live bounded context", calls)
		}
		if calls == 1 {
			operationID = gotOperationID
			firstCtx = ctx
			return store.DVRCompletionResult{
				State: "completed", OperationOwned: true,
			}, simulated
		}
		if gotOperationID == uuid.Nil || gotOperationID != operationID {
			t.Fatalf("retry operation=%s, want stable non-zero %s", gotOperationID, operationID)
		}
		if firstCtx.Err() != context.Canceled {
			t.Fatalf("first attempt context err=%v, want cancelled before retry", firstCtx.Err())
		}
		return store.DVRCompletionResult{
			State: "completed", OperationOwned: true,
		}, nil
	})
	if err != nil || state != "completed" || calls != 2 {
		t.Fatalf("mark completed state=%q calls=%d err=%v", state, calls, err)
	}
}

func TestPromoteArtifactReconcilesAmbiguousCommitWithStableEvidence(t *testing.T) {
	recorder := &Recorder{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	recordingID := uuid.New()
	report := MediaReport{
		Duration: 30 * time.Minute, ArtifactBytes: 123456,
		MaxGap: 40 * time.Millisecond, AVStartDelta: 30 * time.Millisecond,
		AVEndDelta: 60 * time.Millisecond, AVDrift: 30 * time.Millisecond,
	}
	sha := make([]byte, 32)
	sha[0] = 0x47
	simulated := errors.New("simulated lost artifact COMMIT response")
	var operationID uuid.UUID
	calls := 0

	state, err := recorder.promoteArtifactWith(
		recordingID, report, sha, "dev:op476-test",
		"/recordings/episode.ts", "/recordings/episode.backup",
		func(ctx context.Context, gotID, gotOperation uuid.UUID, gotBytes int64,
			gotReport store.DVRMediaReport, gotSHA []byte,
			filesystemID, validator, canonical, backup string,
		) (store.DVRCompletionResult, error) {
			calls++
			if gotID != recordingID || gotBytes != report.ArtifactBytes ||
				gotReport.DurationMS != report.Duration.Milliseconds() ||
				gotReport.AVDriftMS != report.AVDrift.Milliseconds() ||
				!bytes.Equal(gotSHA, sha) || filesystemID != "dev:op476-test" ||
				validator != dvrArtifactValidatorVersion ||
				canonical != "/recordings/episode.ts" || backup != "/recordings/episode.backup" {
				t.Fatalf("attempt %d changed promotion evidence", calls)
			}
			if calls == 1 {
				operationID = gotOperation
				return store.DVRCompletionResult{State: "completed", OperationOwned: true},
					errors.Join(store.ErrDVRArtifactCommitOutcomeUnknown, simulated)
			}
			if gotOperation == uuid.Nil || gotOperation != operationID {
				t.Fatalf("retry operation=%s, want stable %s", gotOperation, operationID)
			}
			return store.DVRCompletionResult{State: "completed", OperationOwned: true}, nil
		})
	if err != nil || state != "completed" || calls != 2 {
		t.Fatalf("promote state=%q calls=%d err=%v", state, calls, err)
	}
}

func TestPromoteArtifactReturnsDeterministicConflictWithoutAmbiguity(t *testing.T) {
	recorder := &Recorder{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	report := MediaReport{Duration: time.Hour, ArtifactBytes: 1234}
	calls := 0
	state, err := recorder.promoteArtifactWith(
		uuid.New(), report, make([]byte, 32), "dev:op476-test",
		"/recordings/episode.ts", "",
		func(context.Context, uuid.UUID, uuid.UUID, int64, store.DVRMediaReport,
			[]byte, string, string, string, string,
		) (store.DVRCompletionResult, error) {
			calls++
			return store.DVRCompletionResult{}, store.ErrDVRArtifactPromotionConflict
		})
	if state != "" || calls != 1 ||
		!errors.Is(err, store.ErrDVRArtifactPromotionConflict) ||
		!errors.Is(err, ErrRecordingTerminalState) ||
		errors.Is(err, ErrRecordingCompletionUnresolved) {
		t.Fatalf("state=%q calls=%d err=%v, want one authoritative conflict", state, calls, err)
	}
}

func TestRejectedReplacementRestoresPredecessorAndClearsPublishStage(t *testing.T) {
	recorder := &Recorder{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	dir := t.TempDir()
	canonical := dir + "/episode.ts"
	normalized := dir + "/episode.normalized.partial"
	backup := dir + "/episode.superseded"
	prior := []byte("prior validated recording")
	candidate := []byte("new candidate that lost database ownership")
	if err := os.WriteFile(canonical, prior, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(normalized, candidate, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := publishRecordingCandidate(canonical, normalized, backup); err != nil {
		t.Fatal(err)
	}

	rec := store.DVRRecording{ID: uuid.New(), OutputPath: canonical}
	finishCalls := 0
	conflict := store.ErrDVRArtifactPromotionConflict
	err := recorder.rollbackRejectedArtifact(
		rec, normalized, backup, "", conflict,
		func(ctx context.Context, gotID uuid.UUID, reason, path string) error {
			finishCalls++
			if ctx.Err() != nil || gotID != rec.ID || path != normalized ||
				!strings.Contains(reason, conflict.Error()) {
				t.Fatalf("finish recovery id=%s reason=%q path=%q ctx=%v",
					gotID, reason, path, ctx.Err())
			}
			return nil
		})
	if !errors.Is(err, conflict) || errors.Is(err, ErrRecordingCompletionUnresolved) {
		t.Fatalf("rollback err=%v, want deterministic conflict only", err)
	}
	if finishCalls != 1 {
		t.Fatalf("finish recovery calls=%d, want 1", finishCalls)
	}
	if got, readErr := os.ReadFile(canonical); readErr != nil || !bytes.Equal(got, prior) {
		t.Fatalf("canonical=%q err=%v, want prior=%q", got, readErr, prior)
	}
	if got, readErr := os.ReadFile(normalized); readErr != nil || !bytes.Equal(got, candidate) {
		t.Fatalf("candidate=%q err=%v, want retained=%q", got, readErr, candidate)
	}
	if _, statErr := os.Stat(backup); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("predecessor backup remains after rollback: %v", statErr)
	}
}

func TestMarkCompletedRequiresExactOperationOwnership(t *testing.T) {
	recorder := &Recorder{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	tests := []struct {
		name      string
		result    store.DVRCompletionResult
		wantState string
		wantErr   bool
	}{
		{name: "own completion", result: store.DVRCompletionResult{
			State: "completed", OperationOwned: true,
		}, wantState: "completed"},
		{name: "foreign or legacy completion", result: store.DVRCompletionResult{
			State: "completed", OperationOwned: false,
		}, wantState: "completed", wantErr: true},
		{name: "operator cancellation", result: store.DVRCompletionResult{
			State: "cancelled", OperationOwned: false,
		}, wantState: "cancelled"},
		{name: "failed terminal state", result: store.DVRCompletionResult{
			State: "failed", OperationOwned: false,
		}, wantState: "failed", wantErr: true},
		{name: "still recording is invalid", result: store.DVRCompletionResult{
			State: "recording", OperationOwned: false,
		}, wantState: "recording", wantErr: true},
		{name: "ownership token on cancelled is invalid", result: store.DVRCompletionResult{
			State: "cancelled", OperationOwned: true,
		}, wantState: "cancelled", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			state, err := recorder.markCompletedWith(uuid.New(), 1, func(
				context.Context, uuid.UUID, uuid.UUID, int64,
			) (store.DVRCompletionResult, error) {
				calls++
				return tc.result, nil
			})
			if state != tc.wantState || (err != nil) != tc.wantErr || calls != 1 {
				t.Fatalf("state=%q calls=%d err=%v, want state=%q calls=1 err=%v",
					state, calls, err, tc.wantState, tc.wantErr)
			}
			if tc.wantErr && !errors.Is(err, ErrRecordingTerminalState) {
				t.Fatalf("err=%v, want ErrRecordingTerminalState", err)
			}
		})
	}
}

func TestFinalizeOwnedCompletionPublishesWithoutReplacement(t *testing.T) {
	recorder := &Recorder{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	dir := t.TempDir()
	partial := dir + "/episode.ts.partial"
	output := dir + "/episode.ts"
	want := []byte("new exactly-owned capture")
	if err := os.WriteFile(partial, want, 0o644); err != nil {
		t.Fatal(err)
	}
	rec := store.DVRRecording{ID: uuid.New(), OutputPath: output}
	markCalls := 0
	postProcessCalls := 0

	err := recorder.finalizeWithActions(
		rec, partial, int64(len(want)),
		func(gotID uuid.UUID, gotBytes int64) (string, error) {
			markCalls++
			if gotID != rec.ID || gotBytes != int64(len(want)) {
				t.Fatalf("completion id=%s bytes=%d", gotID, gotBytes)
			}
			partialInfo, err := os.Stat(partial)
			if err != nil {
				t.Fatalf("partial missing before durable completion: %v", err)
			}
			outputInfo, err := os.Stat(output)
			if err != nil {
				t.Fatalf("final missing before durable completion: %v", err)
			}
			if !os.SameFile(partialInfo, outputInfo) {
				t.Fatal("final path does not alias partial before durable completion")
			}
			return "completed", nil
		},
		func(uuid.UUID, error, string, int64) error {
			t.Fatal("exact owned completion attempted a failure transition")
			return nil
		},
		func(gotRec store.DVRRecording, gotBytes int64) {
			postProcessCalls++
			if gotRec.ID != rec.ID || gotBytes != int64(len(want)) {
				t.Fatalf("postprocess recording=%s bytes=%d", gotRec.ID, gotBytes)
			}
		},
	)
	if err != nil || markCalls != 1 || postProcessCalls != 1 {
		t.Fatalf("finalize err=%v mark_calls=%d postprocess_calls=%d",
			err, markCalls, postProcessCalls)
	}
	got, err := os.ReadFile(output)
	if err != nil || string(got) != string(want) {
		t.Fatalf("final media=%q err=%v, want %q", got, err, want)
	}
	if _, err := os.Stat(partial); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("owned completion retained partial alias: %v", err)
	}
}

func TestFinalizeRefusesToReplaceExistingFinal(t *testing.T) {
	recorder := &Recorder{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	dir := t.TempDir()
	partial := dir + "/episode.ts.partial"
	output := dir + "/episode.ts"
	oldMedia := []byte("known-good prior recording")
	newMedia := []byte("new re-recording")
	if err := os.WriteFile(output, oldMedia, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(partial, newMedia, 0o644); err != nil {
		t.Fatal(err)
	}
	rec := store.DVRRecording{ID: uuid.New(), OutputPath: output}
	markCalled := false
	failureCalled := false
	postProcessCalled := false

	err := recorder.finalizeWithActions(
		rec, partial, int64(len(newMedia)),
		func(uuid.UUID, int64) (string, error) {
			markCalled = true
			return "completed", nil
		},
		func(gotID uuid.UUID, primary error, reason string, gotBytes int64) error {
			failureCalled = true
			if gotID != rec.ID || gotBytes != int64(len(newMedia)) ||
				!errors.Is(primary, os.ErrExist) || reason == "" {
				t.Fatalf("failure id=%s bytes=%d primary=%v reason=%q",
					gotID, gotBytes, primary, reason)
			}
			return primary
		},
		func(store.DVRRecording, int64) { postProcessCalled = true },
	)
	if !errors.Is(err, os.ErrExist) {
		t.Fatalf("finalize err=%v, want existing-path error", err)
	}
	if markCalled || !failureCalled || postProcessCalled {
		t.Fatalf("mark=%v failure=%v postprocess=%v", markCalled, failureCalled, postProcessCalled)
	}
	if got, err := os.ReadFile(output); err != nil || string(got) != string(oldMedia) {
		t.Fatalf("prior final=%q err=%v, want %q", got, err, oldMedia)
	}
	if got, err := os.ReadFile(partial); err != nil || string(got) != string(newMedia) {
		t.Fatalf("new partial=%q err=%v, want %q", got, err, newMedia)
	}
}

func TestFinalizeTerminalWinnerUnpublishesOnlyNewAlias(t *testing.T) {
	tests := []struct {
		name      string
		state     string
		stateErr  error
		wantError bool
	}{
		{name: "operator cancellation", state: "cancelled"},
		{name: "recording failure", state: "failed",
			stateErr: fmt.Errorf("%w: failure won", ErrRecordingTerminalState), wantError: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			recorder := &Recorder{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
			dir := t.TempDir()
			partial := dir + "/episode.ts.partial"
			output := dir + "/episode.ts"
			want := []byte("salvageable losing capture")
			if err := os.WriteFile(partial, want, 0o644); err != nil {
				t.Fatal(err)
			}
			rec := store.DVRRecording{ID: uuid.New(), OutputPath: output}

			err := recorder.finalizeWith(rec, partial, int64(len(want)), func(
				uuid.UUID, int64,
			) (string, error) {
				return tc.state, tc.stateErr
			})
			if (err != nil) != tc.wantError {
				t.Fatalf("finalize err=%v, want error=%v", err, tc.wantError)
			}
			if _, err := os.Stat(output); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("losing completion retained final alias: %v", err)
			}
			if got, err := os.ReadFile(partial); err != nil || string(got) != string(want) {
				t.Fatalf("partial=%q err=%v, want %q", got, err, want)
			}
		})
	}
}

// If every exact-operation reconciliation read fails, moving the final file
// back to .partial would guess that COMMIT rolled back. The opposite outcome
// is possible: PostgreSQL may already advertise this exact path as completed.
// Preserve media at the first durable handoff and return failed quiescence.
func TestFinalizePreservesFinalMediaWhileCompletionIsUnresolved(t *testing.T) {
	recorder := &Recorder{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	dir := t.TempDir()
	partial := dir + "/episode.ts.partial"
	output := dir + "/episode.ts"
	want := []byte("durable captured media")
	if err := os.WriteFile(partial, want, 0o644); err != nil {
		t.Fatal(err)
	}
	rec := store.DVRRecording{ID: uuid.New(), OutputPath: output}
	unresolved := errors.Join(ErrRecordingTerminalState, ErrRecordingCompletionUnresolved)

	err := recorder.finalizeWith(rec, partial, int64(len(want)), func(
		uuid.UUID, int64,
	) (string, error) {
		return "", unresolved
	})
	if !errors.Is(err, ErrRecordingTerminalState) ||
		!errors.Is(err, ErrRecordingCompletionUnresolved) {
		t.Fatalf("finalize err=%v, want terminal+unresolved markers", err)
	}
	got, err := os.ReadFile(output)
	if err != nil {
		t.Fatalf("final media missing after ambiguous completion: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("final media=%q, want %q", got, want)
	}
	gotPartial, err := os.ReadFile(partial)
	if err != nil || string(gotPartial) != string(want) {
		t.Fatalf("partial media=%q err=%v, want preserved %q", gotPartial, err, want)
	}
}

func TestFinalizePreservesFinalMediaForForeignCompletedOperation(t *testing.T) {
	recorder := &Recorder{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	dir := t.TempDir()
	partial := dir + "/episode.ts.partial"
	output := dir + "/episode.ts"
	want := []byte("complete media with foreign durable owner")
	if err := os.WriteFile(partial, want, 0o644); err != nil {
		t.Fatal(err)
	}
	rec := store.DVRRecording{ID: uuid.New(), OutputPath: output}
	foreign := fmt.Errorf("%w: completed by another operation", ErrRecordingTerminalState)

	err := recorder.finalizeWith(rec, partial, int64(len(want)), func(
		uuid.UUID, int64,
	) (string, error) {
		return "completed", foreign
	})
	if !errors.Is(err, ErrRecordingTerminalState) ||
		errors.Is(err, ErrRecordingCompletionUnresolved) {
		t.Fatalf("finalize err=%v, want owned-elsewhere terminal error", err)
	}
	got, err := os.ReadFile(output)
	if err != nil || string(got) != string(want) {
		t.Fatalf("final media=%q err=%v, want preserved %q", got, err, want)
	}
	gotPartial, err := os.ReadFile(partial)
	if err != nil || string(gotPartial) != string(want) {
		t.Fatalf("partial media=%q err=%v, want preserved %q", gotPartial, err, want)
	}
}

func TestFinalizeOwnedCompletionSkipsPostProcessWhenPartialCleanupFails(t *testing.T) {
	recorder := &Recorder{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	dir := t.TempDir()
	partial := dir + "/episode.ts.partial"
	output := dir + "/episode.ts"
	want := []byte("durably completed media")
	if err := os.WriteFile(partial, want, 0o644); err != nil {
		t.Fatal(err)
	}
	rec := store.DVRRecording{ID: uuid.New(), OutputPath: output}
	postProcessCalled := false

	err := recorder.finalizeWithActions(
		rec, partial, int64(len(want)),
		func(uuid.UUID, int64) (string, error) {
			// Replace the partial alias with a non-empty directory after publication
			// so the cleanup unlink fails deterministically even under root CI.
			if err := os.Remove(partial); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(partial, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(partial+"/blocker", []byte("x"), 0o644); err != nil {
				t.Fatal(err)
			}
			return "completed", nil
		},
		func(uuid.UUID, error, string, int64) error {
			t.Fatal("durable completion attempted a failure transition")
			return nil
		},
		func(store.DVRRecording, int64) { postProcessCalled = true },
	)
	if err == nil || errors.Is(err, ErrRecordingTerminalState) {
		t.Fatalf("cleanup err=%v, want non-terminal cleanup failure", err)
	}
	if postProcessCalled {
		t.Fatal("postprocess ran after partial alias cleanup failed")
	}
	if got, readErr := os.ReadFile(output); readErr != nil || string(got) != string(want) {
		t.Fatalf("durable final=%q err=%v, want %q", got, readErr, want)
	}
}

// TestCoveredWholeProgram is the guard that keeps a stale row from being
// salvaged. ListPendingDVRRecordings has no lower bound on scheduled_start, so
// a row left 'scheduled' while Conductor was down gets picked up long after
// its program aired and still receives a one-minute window from Run's sanity
// clamp. "now >= scheduledEnd" alone is trivially true for such a row.
func TestCoveredWholeProgram(t *testing.T) {
	start := time.Date(2026, 8, 10, 7, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)

	tests := []struct {
		name          string
		firstWrite    time.Time
		lastRealChunk time.Time
		want          bool
	}{
		{"normal run, real content past the end", start.Add(-30 * time.Second),
			end.Add(2 * time.Minute), true},
		{"started exactly on time", start, end, true},
		{"last chunk just inside the tolerance", start.Add(-30 * time.Second),
			end.Add(-coverageTolerance), true},
		{"first byte just inside the tolerance", start.Add(coverageTolerance),
			end.Add(time.Minute), true},

		// code review' high finding. Entering Run during pre-roll proves nothing:
		// mkdir + file open + the started-at DB write + slot acquisition + up
		// to 15s of header wait sit before the first byte. If that crosses
		// scheduled_start the recording is missing its beginning.
		{"first byte landed after the program started", start.Add(20 * time.Second),
			end.Add(2 * time.Minute), false},

		// The pump retries a dead upstream for ~90s, so an outage starting
		// before scheduled_end surfaces as an error after it. Real content
		// stopped a minute early — we lost the last minute of the program.
		{"upstream went silent before the end", start.Add(-30 * time.Second),
			end.Add(-time.Minute), false},

		// The case that sent this change back for review. During the gap the file kept
		// growing (slate + a finite black placeholder from upstream), but no
		// real content arrived after this point. File growth would have said
		// "covered"; lastRealChunk correctly says it was not.
		{"placeholder flooded the file but real content had stopped",
			start.Add(-30 * time.Second), end.Add(-10 * time.Minute), false},

		// Stale row: picked up hours late, ran for the one-minute sanity clamp.
		{"stale row picked up after the program aired", end.Add(3 * time.Hour),
			end.Add(3*time.Hour + time.Minute), false},
		// One second late is deliberately inside coverageTolerance — chunk
		// granularity and scheduling jitter, not lost content.
		{"one second late is within tolerance", start.Add(time.Second),
			end.Add(time.Minute), true},
		{"just outside the start tolerance",
			start.Add(coverageTolerance + time.Second), end.Add(time.Minute), false},

		// Upstream never delivered anything real: zero value must not pass.
		{"no real content ever arrived", start.Add(-30 * time.Second), time.Time{}, false},
		// Nothing was ever written at all.
		{"no bytes ever written", time.Time{}, end.Add(time.Minute), false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := coveredWholeProgram(tc.firstWrite, tc.lastRealChunk, start, end); got != tc.want {
				t.Fatalf("coveredWholeProgram = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestFirstWriteAt pins that the sink wrapper stamps the FIRST byte and never
// moves afterwards — that timestamp is the salvage rule's start-coverage
// evidence, so a later write must not overwrite it.
func TestFirstWriteAt(t *testing.T) {
	fw := &firstWriteAt{w: io.Discard}
	if !fw.at.IsZero() {
		t.Fatal("firstWriteAt.at set before any write")
	}

	if _, err := fw.Write([]byte("first")); err != nil {
		t.Fatalf("write: %v", err)
	}
	first := fw.at
	if first.IsZero() {
		t.Fatal("firstWriteAt.at still zero after a write")
	}

	time.Sleep(2 * time.Millisecond)
	if _, err := fw.Write([]byte("second")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if !fw.at.Equal(first) {
		t.Fatalf("a later write moved the first-write stamp: %v -> %v", first, fw.at)
	}

	// A zero-length write must not stamp anything.
	fw2 := &firstWriteAt{w: io.Discard}
	if _, err := fw2.Write(nil); err != nil {
		t.Fatalf("write: %v", err)
	}
	if !fw2.at.IsZero() {
		t.Fatal("zero-length write stamped a first-write time")
	}
}

// TestDefaultPaddingBudget guards the slot-contention fix: StartSlack+
// MaxOverrun is exactly how long two adjacent recordings overlap, and every
// recording holds one of only two provider stream slots for its padded
// window. The original 30s+5m overlap starved 4 of 9 observed failures.
func TestDefaultPaddingBudget(t *testing.T) {
	if got := DefaultStartSlack + DefaultMaxOverrun; got > 2*time.Minute {
		t.Fatalf("default padding budget %s exceeds 2m; adjacent recordings will "+
			"fight for provider stream slots that long", got)
	}
}
