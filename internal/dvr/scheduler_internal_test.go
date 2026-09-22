package dvr

import (
	"testing"
	"time"

	"github.com/spencercnorton/conductor/internal/store"
)

func TestSchedulerQueryLookaheadCoversConfiguredStartSlack(t *testing.T) {
	tests := []struct {
		name        string
		windowAhead time.Duration
		startSlack  time.Duration
		want        time.Duration
	}{
		{
			name:        "window smaller than start slack",
			windowAhead: time.Minute,
			startSlack:  2 * time.Minute,
			want:        2 * time.Minute,
		},
		{
			name:        "window equal to start slack",
			windowAhead: 2 * time.Minute,
			startSlack:  2 * time.Minute,
			want:        2 * time.Minute,
		},
		{
			name:        "window larger than start slack",
			windowAhead: 3 * time.Minute,
			startSlack:  2 * time.Minute,
			want:        3 * time.Minute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Scheduler{
				WindowAhead: tt.windowAhead,
				Recorder:    &Recorder{StartSlack: tt.startSlack},
			}
			if got := s.queryLookahead(); got != tt.want {
				t.Fatalf("queryLookahead()=%s, want %s", got, tt.want)
			}
		})
	}
}

// The retry budget begins at padded start, not nominal start. Adjacent DVR
// windows overlap for StartSlack+MaxOverrun, so starting the budget later
// would silently grant extra retries exactly where padding consumes capacity.
func TestCapacityRetryDeadlineStartsAtPaddedWindowAndCapsAtProgramEnd(t *testing.T) {
	start := time.Date(2026, 8, 21, 18, 0, 0, 0, time.UTC)
	s := &Scheduler{
		Recorder:            &Recorder{StartSlack: 30 * time.Second},
		CapacityRetryWindow: 5 * time.Minute,
	}

	long := store.DVRRecording{ScheduledStart: start, ScheduledEnd: start.Add(time.Hour)}
	want := start.Add(-30 * time.Second).Add(5 * time.Minute)
	if got := s.capacityRetryDeadline(long); !got.Equal(want) {
		t.Fatalf("long-program deadline=%s, want padded-start+window %s", got, want)
	}

	short := store.DVRRecording{ScheduledStart: start, ScheduledEnd: start.Add(2 * time.Minute)}
	if got := s.capacityRetryDeadline(short); !got.Equal(short.ScheduledEnd) {
		t.Fatalf("short-program deadline=%s, want scheduled_end cap %s", got, short.ScheduledEnd)
	}
}

func TestAdmissionAttemptTimeoutIsIndependentAndRetryCapped(t *testing.T) {
	s := &Scheduler{CapacityRetryInterval: 5 * time.Second}
	if got := s.admissionAttemptTimeout(); got != DefaultAdmissionAttemptTimeout {
		t.Fatalf("default admission attempt timeout=%s, want %s",
			got, DefaultAdmissionAttemptTimeout)
	}

	s.AdmissionAttemptTimeout = 750 * time.Millisecond
	if got := s.admissionAttemptTimeout(); got != 750*time.Millisecond {
		t.Fatalf("configured admission attempt timeout=%s, want 750ms", got)
	}

	s.CapacityRetryInterval = 100 * time.Millisecond
	if got := s.admissionAttemptTimeout(); got != 100*time.Millisecond {
		t.Fatalf("retry-capped admission attempt timeout=%s, want 100ms", got)
	}
}
