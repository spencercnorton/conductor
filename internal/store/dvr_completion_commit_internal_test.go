package store

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func seedClaimedCommitAmbiguityRecording(t *testing.T, db *DB, title string) DVRRecording {
	t.Helper()
	ctx := context.Background()
	channel, _ := seedCommitAmbiguityChannel(t, db)
	start := time.Now().Add(time.Hour)
	recording, err := db.CreateDVRRecording(ctx, DVRRecording{
		ChannelID: channel.ID, Title: title + "-" + uuid.NewString(),
		ScheduledStart: start, ScheduledEnd: start.Add(time.Hour),
		Priority: 100, RequestedBy: "commit-ambiguity-test",
		OutputPath: t.TempDir() + "/recording.ts",
	})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := db.TryMarkDVRRecordingStarted(ctx, recording.ID, recording.OutputPath)
	if err != nil || !claimed {
		t.Fatalf("claim recording: claimed=%v err=%v", claimed, err)
	}
	return recording
}

// This exercises the real PostgreSQL transaction and then replaces only its
// successful client-visible result. The first call has durably committed, but
// its cancelled context and returned error reveal no trustworthy state. A new
// transaction using the same token must prove ownership from durable storage;
// a different operation must observe completed without claiming it.
func TestIntegrationDVRCompletionCommitAmbiguityReplaysExactOperation(t *testing.T) {
	db := openCommitAmbiguityDB(t)
	recording := seedClaimedCommitAmbiguityRecording(t, db, "ambiguous completion")
	operationID := uuid.New()
	simulated := errors.New("simulated lost DVR completion COMMIT response")
	ctx, cancel := context.WithCancel(context.Background())
	db.dvrCompletionCommitResultHook = func() error {
		db.dvrCompletionCommitResultHook = nil
		cancel()
		return simulated
	}

	provisional, err := db.MarkDVRRecordingCompleted(ctx, recording.ID, operationID, 4321)
	if !errors.Is(err, simulated) {
		t.Fatalf("first completion err=%v, want simulated ambiguity", err)
	}
	if provisional != (DVRCompletionResult{}) {
		t.Fatalf("ambiguous call leaked provisional result %+v", provisional)
	}
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("first context err=%v, want cancelled", ctx.Err())
	}

	var state string
	var bytes int64
	var persistedOperationID *uuid.UUID
	if err := db.Pool.QueryRow(context.Background(), `
		SELECT state::text, bytes_written, completion_operation_id
		  FROM dvr_recording WHERE id = $1`, recording.ID).
		Scan(&state, &bytes, &persistedOperationID); err != nil {
		t.Fatal(err)
	}
	if state != "completed" || bytes != 4321 || persistedOperationID == nil ||
		*persistedOperationID != operationID {
		t.Fatalf("durable row state=%s bytes=%d operation=%v, want completed/4321/%s",
			state, bytes, persistedOperationID, operationID)
	}

	replayed, err := db.MarkDVRRecordingCompleted(
		context.Background(), recording.ID, operationID, 9999)
	if err != nil || replayed.State != "completed" || !replayed.OperationOwned {
		t.Fatalf("exact replay result=%+v err=%v", replayed, err)
	}
	foreign, err := db.MarkDVRRecordingCompleted(
		context.Background(), recording.ID, uuid.New(), 8888)
	if err != nil || foreign.State != "completed" || foreign.OperationOwned {
		t.Fatalf("foreign replay result=%+v err=%v", foreign, err)
	}
	got, err := db.GetDVRRecording(context.Background(), recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != "completed" || got.BytesWritten != 4321 {
		t.Fatalf("replay changed durable completion: %+v", got)
	}
}

func TestIntegrationDVRCompletionPreservesNonOwnedTerminalStates(t *testing.T) {
	db := openCommitAmbiguityDB(t)
	ctx := context.Background()

	t.Run("cancelled", func(t *testing.T) {
		recording := seedClaimedCommitAmbiguityRecording(t, db, "cancelled completion")
		if err := db.MarkDVRRecordingCancelled(ctx, recording.ID); err != nil {
			t.Fatal(err)
		}
		result, err := db.MarkDVRRecordingCompleted(ctx, recording.ID, uuid.New(), 111)
		if err != nil || result.State != "cancelled" || result.OperationOwned {
			t.Fatalf("cancelled result=%+v err=%v", result, err)
		}
	})

	t.Run("failed", func(t *testing.T) {
		recording := seedClaimedCommitAmbiguityRecording(t, db, "failed completion")
		updated, err := db.MarkDVRRecordingFailed(ctx, recording.ID, "prior failure", 222)
		if err != nil || !updated {
			t.Fatalf("mark failed updated=%v err=%v", updated, err)
		}
		result, err := db.MarkDVRRecordingCompleted(ctx, recording.ID, uuid.New(), 333)
		if err != nil || result.State != "failed" || result.OperationOwned {
			t.Fatalf("failed result=%+v err=%v", result, err)
		}
	})

	t.Run("legacy completed without token", func(t *testing.T) {
		recording := seedClaimedCommitAmbiguityRecording(t, db, "legacy completion")
		if _, err := db.Pool.Exec(ctx, `
			UPDATE dvr_recording
			   SET state = 'completed', completed_at = now(), bytes_written = 444
			 WHERE id = $1`, recording.ID); err != nil {
			t.Fatal(err)
		}
		result, err := db.MarkDVRRecordingCompleted(ctx, recording.ID, uuid.New(), 555)
		if err != nil || result.State != "completed" || result.OperationOwned {
			t.Fatalf("legacy completed result=%+v err=%v", result, err)
		}
		got, err := db.GetDVRRecording(ctx, recording.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.BytesWritten != 444 {
			t.Fatalf("legacy completed bytes=%d, want unchanged 444", got.BytesWritten)
		}
	})
}

func TestIntegrationDVRCompletionConcurrentOperationsHaveOneExactOwner(t *testing.T) {
	db := openCommitAmbiguityDB(t)
	recording := seedClaimedCommitAmbiguityRecording(t, db, "concurrent completion")
	type callResult struct {
		operationID uuid.UUID
		bytes       int64
		result      DVRCompletionResult
		err         error
	}
	calls := []callResult{
		{operationID: uuid.New(), bytes: 6101},
		{operationID: uuid.New(), bytes: 6202},
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range calls {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			calls[i].result, calls[i].err = db.MarkDVRRecordingCompleted(
				ctx, recording.ID, calls[i].operationID, calls[i].bytes)
		}(i)
	}
	close(start)
	wg.Wait()

	owned := -1
	for i, call := range calls {
		if call.err != nil || call.result.State != "completed" {
			t.Fatalf("call %d result=%+v err=%v", i, call.result, call.err)
		}
		if call.result.OperationOwned {
			if owned >= 0 {
				t.Fatalf("calls %d and %d both claimed completion ownership", owned, i)
			}
			owned = i
		}
	}
	if owned < 0 {
		t.Fatal("neither concurrent operation owned completion")
	}

	var persistedOperationID uuid.UUID
	var bytes int64
	if err := db.Pool.QueryRow(context.Background(), `
		SELECT completion_operation_id, bytes_written
		  FROM dvr_recording WHERE id = $1`, recording.ID).
		Scan(&persistedOperationID, &bytes); err != nil {
		t.Fatal(err)
	}
	if persistedOperationID != calls[owned].operationID || bytes != calls[owned].bytes {
		t.Fatalf("durable operation=%s bytes=%d, owned call operation=%s bytes=%d",
			persistedOperationID, bytes, calls[owned].operationID, calls[owned].bytes)
	}
}

// Cancellation/failure do not participate in the completion advisory-lock
// namespace. If either already owns the row, completion must wait at a locking
// read and return its durable terminal state. A single UPDATE CTE plus fallback
// SELECT instead keeps the statement snapshot from before the wait and can
// incorrectly report "recording" after the terminal transaction commits.
func TestIntegrationDVRCompletionWaitsForConcurrentTerminalState(t *testing.T) {
	db := openCommitAmbiguityDB(t)
	tests := []struct {
		name  string
		state string
	}{
		{name: "operator cancellation", state: "cancelled"},
		{name: "recording failure", state: "failed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			recording := seedClaimedCommitAmbiguityRecording(t, db, tc.name)
			terminalTx, err := db.Pool.Begin(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = terminalTx.Rollback(context.Background()) })
			if _, err := terminalTx.Exec(context.Background(), `
				UPDATE dvr_recording
				   SET state = $2::dvr_recording_state, completed_at = clock_timestamp()
				 WHERE id = $1 AND state = 'recording'`, recording.ID, tc.state); err != nil {
				t.Fatal(err)
			}

			// PostgreSQL truncates application_name to 63 bytes; keep the
			// identifier short enough for an exact pg_stat_activity barrier.
			applicationName := "dvr-terminal-" + uuid.NewString()
			retryConfig := db.Pool.Config()
			retryConfig.ConnConfig.RuntimeParams["application_name"] = applicationName
			retryPool, err := pgxpool.NewWithConfig(context.Background(), retryConfig)
			if err != nil {
				t.Fatal(err)
			}
			// Release the row lock before closing a pool whose completion query
			// may still be waiting on it. This keeps a failed barrier assertion
			// from hanging test cleanup and obscuring the real failure.
			t.Cleanup(func() {
				_ = terminalTx.Rollback(context.Background())
				retryPool.Close()
			})
			retryDB := &DB{Pool: retryPool}
			type completionCall struct {
				result DVRCompletionResult
				err    error
			}
			completed := make(chan completionCall, 1)
			go func() {
				result, completionErr := retryDB.MarkDVRRecordingCompleted(
					context.Background(), recording.ID, uuid.New(), 777)
				completed <- completionCall{result: result, err: completionErr}
			}()

			deadline := time.Now().Add(5 * time.Second)
			for {
				var blocked bool
				if err := db.Pool.QueryRow(context.Background(), `
					SELECT EXISTS (
						SELECT 1
						  FROM pg_stat_activity
						 WHERE application_name = $1
						   AND state = 'active'
						   AND wait_event_type = 'Lock'
					)`, applicationName).Scan(&blocked); err != nil {
					t.Fatal(err)
				}
				if blocked {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("completion did not overlap the terminal transaction at its row lock")
				}
				time.Sleep(10 * time.Millisecond)
			}

			if err := terminalTx.Commit(context.Background()); err != nil {
				t.Fatal(err)
			}
			select {
			case call := <-completed:
				if call.err != nil || call.result.State != tc.state || call.result.OperationOwned {
					t.Fatalf("completion result=%+v err=%v, want %s/non-owned/nil",
						call.result, call.err, tc.state)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("completion did not finish after terminal transaction committed")
			}

			var state string
			var operationID *uuid.UUID
			if err := db.Pool.QueryRow(context.Background(), `
				SELECT state::text, completion_operation_id
				  FROM dvr_recording WHERE id = $1`, recording.ID).
				Scan(&state, &operationID); err != nil {
				t.Fatal(err)
			}
			if state != tc.state || operationID != nil {
				t.Fatalf("durable state=%s operation=%v, want %s/nil",
					state, operationID, tc.state)
			}
		})
	}
}
