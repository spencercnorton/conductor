package dvr

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/store"
)

func TestRecordingReapActionRequiresMissingOwnerBeforeAbsoluteCeiling(t *testing.T) {
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	maxOverrun := 90 * time.Second
	hardGrace := 32 * time.Minute
	heartbeatTimeout := 30 * time.Second
	staleHeartbeat := now.Add(-time.Minute)
	freshHeartbeat := now.Add(-5 * time.Second)
	ordinaryEnd := now.Add(-2 * time.Minute)
	hardEnd := now.Add(-maxOverrun - hardGrace - time.Second)

	tests := []struct {
		name         string
		rec          store.DVRRecording
		locallyOwned bool
		want         recordingReapAction
	}{
		{
			name: "capture window still open",
			rec: store.DVRRecording{
				ScheduledEnd:         ordinaryEnd.Add(2 * time.Minute),
				RecordingHeartbeatAt: &staleHeartbeat,
			},
			want: recordingReapNone,
		},
		{
			name: "ownerless stale heartbeat after ordinary deadline",
			rec: store.DVRRecording{
				ScheduledEnd:         ordinaryEnd,
				RecordingHeartbeatAt: &staleHeartbeat,
			},
			want: recordingReapOwnerless,
		},
		{
			name: "ownerless fresh heartbeat",
			rec: store.DVRRecording{
				ScheduledEnd:         ordinaryEnd,
				RecordingHeartbeatAt: &freshHeartbeat,
			},
			want: recordingReapNone,
		},
		{
			name: "local owner survives stale DB heartbeat",
			rec: store.DVRRecording{
				ScheduledEnd:         ordinaryEnd,
				RecordingHeartbeatAt: &staleHeartbeat,
			},
			locallyOwned: true,
			want:         recordingReapNone,
		},
		{
			name: "local owner exceeds absolute ceiling",
			rec: store.DVRRecording{
				ScheduledEnd:         hardEnd,
				RecordingHeartbeatAt: &freshHeartbeat,
			},
			locallyOwned: true,
			want:         recordingReapAbsolute,
		},
		{
			name: "ownerless row exceeds absolute ceiling regardless of heartbeat",
			rec: store.DVRRecording{
				ScheduledEnd:         hardEnd,
				RecordingHeartbeatAt: &freshHeartbeat,
			},
			want: recordingReapAbsolute,
		},
		{
			name: "nil heartbeat falls back to stale started at",
			rec: store.DVRRecording{
				ScheduledEnd: ordinaryEnd,
				StartedAt:    &staleHeartbeat,
			},
			want: recordingReapOwnerless,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := recordingReapActionAt(
				tt.rec, now, maxOverrun, hardGrace, heartbeatTimeout, tt.locallyOwned)
			if got != tt.want {
				t.Fatalf("recordingReapActionAt=%v, want %v", got, tt.want)
			}
		})
	}
}

func TestSchedulerAbsoluteCeilingPersistsFailureBeforeDistinctCancel(t *testing.T) {
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	recordingID := uuid.New()
	workerCtx, cancelWorker := context.WithCancelCause(context.Background())
	defer cancelWorker(context.Canceled)
	var marks atomic.Int32
	var persistedReason string

	scheduler := &Scheduler{
		Logger:                      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Recorder:                    &Recorder{MaxOverrun: 90 * time.Second},
		RecordingHeartbeatTimeout:   30 * time.Second,
		RecordingFinalizationMargin: 2 * time.Minute,
		inFlight:                    make(map[uuid.UUID]context.CancelCauseFunc),
		now:                         func() time.Time { return now },
	}
	scheduler.inFlight[recordingID] = cancelWorker
	scheduler.listStaleRecordings = func(
		_ context.Context,
		maxOverrun, hardGrace, heartbeatTimeout time.Duration,
	) ([]store.DVRRecording, error) {
		if maxOverrun != scheduler.Recorder.MaxOverrun {
			t.Fatalf("max overrun=%s", maxOverrun)
		}
		if hardGrace != mediaProcessTimeout+scheduler.RecordingFinalizationMargin {
			t.Fatalf("hard grace=%s, want media timeout + margin %s",
				hardGrace, mediaProcessTimeout+scheduler.RecordingFinalizationMargin)
		}
		if heartbeatTimeout != scheduler.RecordingHeartbeatTimeout {
			t.Fatalf("heartbeat timeout=%s", heartbeatTimeout)
		}
		return []store.DVRRecording{{
			ID: recordingID,
			ScheduledEnd: now.Add(-scheduler.Recorder.MaxOverrun).
				Add(-mediaProcessTimeout).
				Add(-scheduler.RecordingFinalizationMargin).
				Add(-time.Second),
			BytesWritten: 42,
		}}, nil
	}
	scheduler.markRecordingFailed = func(
		_ context.Context,
		id uuid.UUID,
		reason string,
		bytes int64,
	) (bool, error) {
		marks.Add(1)
		if id != recordingID || bytes != 42 {
			t.Fatalf("mark failed id=%s bytes=%d", id, bytes)
		}
		if cause := context.Cause(workerCtx); cause != nil {
			t.Fatalf("worker cancelled before failure persisted: %v", cause)
		}
		persistedReason = reason
		return true, nil
	}

	scheduler.reconcileRecordingZombies(context.Background())
	if marks.Load() != 1 {
		t.Fatalf("failure marks=%d, want 1", marks.Load())
	}
	if !strings.Contains(persistedReason, errRecordingAbsoluteRuntimeCeiling.Error()) ||
		!strings.Contains(persistedReason, "media_process_timeout=30m0s") ||
		!strings.Contains(persistedReason, "margin=2m0s") {
		t.Fatalf("absolute failure reason lacks distinct deadline evidence: %q", persistedReason)
	}
	if cause := context.Cause(workerCtx); !errors.Is(cause, errRecordingAbsoluteRuntimeCeiling) {
		t.Fatalf("worker cancellation cause=%v, want absolute ceiling", cause)
	}
}

func TestSchedulerOwnerlessStaleRecordingFailsButLiveOwnerDoesNot(t *testing.T) {
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	stale := now.Add(-time.Minute)
	ownerlessID, locallyOwnedID := uuid.New(), uuid.New()
	ownedCtx, cancelOwned := context.WithCancelCause(context.Background())
	defer cancelOwned(context.Canceled)
	var marked []uuid.UUID
	scheduler := &Scheduler{
		Logger:                      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Recorder:                    &Recorder{MaxOverrun: 30 * time.Second},
		RecordingHeartbeatTimeout:   30 * time.Second,
		RecordingFinalizationMargin: time.Minute,
		inFlight:                    make(map[uuid.UUID]context.CancelCauseFunc),
		now:                         func() time.Time { return now },
	}
	scheduler.inFlight[locallyOwnedID] = cancelOwned
	scheduler.listStaleRecordings = func(
		context.Context,
		time.Duration,
		time.Duration,
		time.Duration,
	) ([]store.DVRRecording, error) {
		return []store.DVRRecording{
			{
				ID: ownerlessID, ScheduledEnd: now.Add(-2 * time.Minute),
				RecordingHeartbeatAt: &stale,
			},
			{
				ID: locallyOwnedID, ScheduledEnd: now.Add(-2 * time.Minute),
				RecordingHeartbeatAt: &stale,
			},
		}, nil
	}
	scheduler.markRecordingFailed = func(
		_ context.Context,
		id uuid.UUID,
		reason string,
		_ int64,
	) (bool, error) {
		if !strings.Contains(reason, "worker missing") {
			t.Fatalf("ownerless failure reason=%q", reason)
		}
		marked = append(marked, id)
		return true, nil
	}

	scheduler.reconcileRecordingZombies(context.Background())
	if len(marked) != 1 || marked[0] != ownerlessID {
		t.Fatalf("marked recordings=%v, want only ownerless %s", marked, ownerlessID)
	}
	if cause := context.Cause(ownedCtx); cause != nil {
		t.Fatalf("live owner cancelled only for stale DB heartbeat: %v", cause)
	}
}

func TestSchedulerAbsoluteCASLossDoesNotCancelLocalOwner(t *testing.T) {
	now := time.Now()
	id := uuid.New()
	workerCtx, cancelWorker := context.WithCancelCause(context.Background())
	defer cancelWorker(context.Canceled)
	scheduler := &Scheduler{
		Logger:                      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Recorder:                    &Recorder{},
		RecordingFinalizationMargin: time.Minute,
		inFlight:                    map[uuid.UUID]context.CancelCauseFunc{id: cancelWorker},
		markRecordingFailed: func(context.Context, uuid.UUID, string, int64) (bool, error) {
			return false, nil
		},
	}
	hardGrace := scheduler.recordingAbsoluteGrace()
	scheduler.reconcileRecordingZombie(context.Background(), store.DVRRecording{
		ID: id, ScheduledEnd: now.Add(-hardGrace - time.Second),
	}, now, hardGrace)
	if cause := context.Cause(workerCtx); cause != nil {
		t.Fatalf("lost terminal CAS cancelled local owner: %v", cause)
	}
}

func TestSchedulerCancelRetainsOwnershipUntilRecorderUnwinds(t *testing.T) {
	id := uuid.New()
	workerCtx, cancelWorker := context.WithCancelCause(context.Background())
	scheduler := &Scheduler{
		inFlight: map[uuid.UUID]context.CancelCauseFunc{id: cancelWorker},
	}
	if !scheduler.Cancel(id) {
		t.Fatal("Cancel did not find local owner")
	}
	if !errors.Is(context.Cause(workerCtx), context.Canceled) {
		t.Fatalf("operator cancellation cause=%v", context.Cause(workerCtx))
	}
	if _, stillOwned := scheduler.inFlight[id]; !stillOwned {
		t.Fatal("Cancel removed ownership before recorder unwind")
	}
}
