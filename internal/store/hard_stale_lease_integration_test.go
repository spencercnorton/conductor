package store_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/store"
)

func TestIntegrationReconcileStartupPreservesOnlyPublishingArtifactRecovery(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	providerID, _ := seedProviderWithCredentials(t, db, 1, 8)
	channel := seedLeaseChannel(t, db, providerID, 9486.0, 1)
	oldHeartbeat := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)

	type stagedRecording struct {
		id                uuid.UUID
		stage             string
		wantStage         string
		wantArtifactState string
		wantArtifactError bool
		wantFilesystemID  string
	}
	staged := make([]stagedRecording, 0, 3)
	for i, stage := range []string{"capturing", "normalizing", "publishing"} {
		start := time.Now().Add(time.Duration(i) * 2 * time.Hour)
		output := t.TempDir() + "/" + stage + ".ts"
		rec, err := db.CreateDVRRecording(ctx, store.DVRRecording{
			ChannelID:      channel.ID,
			Title:          "restart " + stage,
			ScheduledStart: start,
			ScheduledEnd:   start.Add(time.Hour),
			RequestedBy:    "test",
			OutputPath:     output,
		})
		if err != nil {
			t.Fatal(err)
		}
		claimed, err := db.TryMarkDVRRecordingStarted(ctx, rec.ID, output)
		if err != nil || !claimed {
			t.Fatalf("claim %s recording: claimed=%v err=%v", stage, claimed, err)
		}
		// Publishing normally follows normalization; seed that transition so the
		// artifact state is validating and must remain intact for filesystem
		// rollback rather than being prematurely rejected at startup.
		if stage == "publishing" {
			if err := db.MarkDVRArtifactStage(
				ctx, rec.ID, "normalizing", output+".normalized"); err != nil {
				t.Fatal(err)
			}
		}
		if err := db.MarkDVRArtifactStage(ctx, rec.ID, stage, output+"."+stage); err != nil {
			t.Fatal(err)
		}
		seedFilesystemID := "dev:restart-" + stage
		if _, err := db.Pool.Exec(ctx, `
			UPDATE dvr_recording
			   SET recording_heartbeat_at=$2, artifact_filesystem_id=$3
			 WHERE id=$1`, rec.ID, oldHeartbeat, seedFilesystemID); err != nil {
			t.Fatal(err)
		}
		wantStage := "none"
		wantArtifactState := "rejected"
		wantArtifactError := true
		wantFilesystemID := ""
		if stage == "publishing" {
			wantStage = "publishing"
			wantArtifactState = "validating"
			wantArtifactError = false
			wantFilesystemID = seedFilesystemID
		}
		staged = append(staged, stagedRecording{
			id: rec.ID, stage: stage,
			wantStage: wantStage, wantArtifactState: wantArtifactState,
			wantArtifactError: wantArtifactError, wantFilesystemID: wantFilesystemID,
		})
	}

	recovered, err := db.ReconcileStartup(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.DVRRecordings != int64(len(staged)) {
		t.Fatalf("startup recovered DVR rows=%d, want %d",
			recovered.DVRRecordings, len(staged))
	}

	for _, want := range staged {
		got, err := db.GetDVRRecording(ctx, want.id)
		if err != nil {
			t.Fatal(err)
		}
		if got.State != "failed" || got.ArtifactStage != want.wantStage ||
			got.ArtifactState != want.wantArtifactState || got.ArtifactCurrent ||
			got.RecordingHeartbeatAt == nil || !got.RecordingHeartbeatAt.After(oldHeartbeat) ||
			!strings.Contains(got.Error, "interrupted by conductor restart") {
			t.Fatalf("reconciled %s artifact=%+v", want.stage, got)
		}
		hasRestartArtifactError := strings.Contains(
			got.ArtifactError, "interrupted by conductor restart")
		if hasRestartArtifactError != want.wantArtifactError {
			t.Fatalf("reconciled %s artifact_error=%q, want restart reason=%v",
				want.stage, got.ArtifactError, want.wantArtifactError)
		}
		if got.ArtifactFilesystemID != want.wantFilesystemID {
			t.Fatalf("reconciled %s filesystem_id=%q, want %q",
				want.stage, got.ArtifactFilesystemID, want.wantFilesystemID)
		}
	}

	publishes, err := db.ListInterruptedDVRPublishes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(publishes) != 1 || publishes[0].ArtifactStage != "publishing" {
		t.Fatalf("interrupted publish recovery rows=%+v, want one publishing row", publishes)
	}
}

func TestIntegrationHardStaleStreamFinalizationIsGenerationBound(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	providerID, _ := seedProviderWithCredentials(t, db, 1, 8)
	resolver := store.PassthroughResolverForTest{}
	cutoff := time.Now().Add(-3 * time.Minute)

	acquireStale := func(number float64, claimPump bool) (store.Lease, store.HardStaleStreamCandidate) {
		t.Helper()
		channel := seedLeaseChannel(t, db, providerID, number, 1)
		clientID := uuid.New()
		lease, err := db.AcquireLeaseForClient(ctx, channel.ID, resolver, clientID)
		if err != nil {
			t.Fatal(err)
		}
		if claimPump {
			lease, err = db.ClaimPumpLeaseForClient(ctx, lease.ActiveStreamID, clientID)
			if err != nil {
				t.Fatal(err)
			}
			if err := db.MarkRunning(ctx, lease.ActiveStreamID); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := db.Pool.Exec(ctx, `
			UPDATE active_stream
			   SET last_heartbeat = clock_timestamp() - interval '10 minutes'
			 WHERE id = $1`, lease.ActiveStreamID); err != nil {
			t.Fatal(err)
		}
		candidates, err := db.ListHardStaleStreams(ctx, cutoff)
		if err != nil {
			t.Fatal(err)
		}
		for _, candidate := range candidates {
			if candidate.ID == lease.ActiveStreamID {
				return lease, candidate
			}
		}
		t.Fatalf("hard-stale candidate %s not listed in %v", lease.ActiveStreamID, candidates)
		return store.Lease{}, store.HardStaleStreamCandidate{}
	}

	// No claimed generation means the singleton runtime has no physical pump
	// to stop. Capacity becomes free immediately, while exact token deletion on
	// this stream leaves an unrelated healthy stream untouched.
	pumpLess, pumpLessCandidate := acquireStale(9486.1, false)
	healthyChannel := seedLeaseChannel(t, db, providerID, 9486.2, 1)
	healthyClient := uuid.New()
	healthy, err := db.AcquireLeaseForClient(ctx, healthyChannel.ID, resolver, healthyClient)
	if err != nil {
		t.Fatal(err)
	}
	finalized, err := db.FinalizeHardStaleStream(ctx, pumpLessCandidate, cutoff, false)
	if err != nil || !finalized {
		t.Fatalf("finalize pump-less stale stream: finalized=%v err=%v", finalized, err)
	}
	assertHardStaleStreamState(t, db, pumpLess.ActiveStreamID, "dead", 0, 0)
	assertHardStaleStreamState(t, db, healthy.ActiveStreamID, "starting", 1, 1)

	// A tracked generation is drained and its exact durable clients are
	// consumed, but it remains capacity-counted until that generation exits.
	tracked, trackedCandidate := acquireStale(9486.3, true)
	if trackedCandidate.PumpGeneration != tracked.PumpGeneration ||
		trackedCandidate.PumpGeneration == uuid.Nil {
		t.Fatalf("candidate generation=%s, lease generation=%s",
			trackedCandidate.PumpGeneration, tracked.PumpGeneration)
	}
	finalized, err = db.FinalizeHardStaleStream(ctx, trackedCandidate, cutoff, true)
	if err != nil || !finalized {
		t.Fatalf("finalize tracked stale stream: finalized=%v err=%v", finalized, err)
	}
	assertHardStaleStreamState(t, db, tracked.ActiveStreamID, "draining", 0, 0)
	// A lost COMMIT response is reconciled by replaying the exact candidate.
	// The draining generation is already terminal and must remain a successful,
	// idempotent result instead of looking like a rejected stale snapshot.
	replayed, err := db.FinalizeHardStaleStream(ctx, trackedCandidate, cutoff, true)
	if err != nil || !replayed {
		t.Fatalf("replay tracked stale stream: finalized=%v err=%v", replayed, err)
	}
	assertHardStaleStreamState(t, db, tracked.ActiveStreamID, "draining", 0, 0)
	if marked, err := db.MarkDeadAfterPumpExit(
		ctx, tracked.ActiveStreamID, tracked.PumpGeneration); err != nil || !marked {
		t.Fatalf("tracked generation exit: marked=%v err=%v", marked, err)
	}

	// A listed snapshot cannot consume a replacement pump's clients. Claiming
	// the replacement changes only pump_generation, deliberately leaving the
	// old heartbeat stale so this assertion is exclusively generation-bound.
	replaced, oldCandidate := acquireStale(9486.4, true)
	replacement, err := db.ClaimPumpLeaseForClient(
		ctx, replaced.ActiveStreamID, replaced.LeaseClientID)
	if err != nil {
		t.Fatal(err)
	}
	if replacement.PumpGeneration == oldCandidate.PumpGeneration {
		t.Fatal("replacement pump reused stale generation")
	}
	finalized, err = db.FinalizeHardStaleStream(ctx, oldCandidate, cutoff, false)
	if err != nil {
		t.Fatal(err)
	}
	if finalized {
		t.Fatal("stale generation finalized replacement pump")
	}
	assertHardStaleStreamState(t, db, replaced.ActiveStreamID, "running", 1, 1)
	var durableGeneration uuid.UUID
	if err := db.Pool.QueryRow(ctx,
		`SELECT pump_generation FROM active_stream WHERE id=$1`, replaced.ActiveStreamID).
		Scan(&durableGeneration); err != nil {
		t.Fatal(err)
	}
	if durableGeneration != replacement.PumpGeneration {
		t.Fatalf("durable generation=%s, want replacement %s",
			durableGeneration, replacement.PumpGeneration)
	}

	// A fresh durable heartbeat after listing independently invalidates the
	// candidate even when its generation is unchanged.
	refreshed, refreshedCandidate := acquireStale(9486.5, true)
	if err := db.Heartbeat(ctx, refreshed.ActiveStreamID, 188); err != nil {
		t.Fatal(err)
	}
	finalized, err = db.FinalizeHardStaleStream(ctx, refreshedCandidate, cutoff, true)
	if err != nil {
		t.Fatal(err)
	}
	if finalized {
		t.Fatal("candidate survived a fresh durable heartbeat")
	}
	assertHardStaleStreamState(t, db, refreshed.ActiveStreamID, "running", 1, 1)
}

func assertHardStaleStreamState(
	t *testing.T,
	db *store.DB,
	streamID uuid.UUID,
	wantState string,
	wantClients, wantTokens int,
) {
	t.Helper()
	var state string
	var clients, tokens int
	if err := db.Pool.QueryRow(context.Background(), `
		SELECT a.state::text, a.client_count, COUNT(sc.id)::int
		  FROM active_stream a
	  LEFT JOIN stream_client sc ON sc.active_stream_id = a.id
		 WHERE a.id = $1
		 GROUP BY a.state, a.client_count`, streamID).Scan(
		&state, &clients, &tokens); err != nil {
		t.Fatal(err)
	}
	if state != wantState || clients != wantClients || tokens != wantTokens {
		t.Fatalf("stream %s state=%s clients=%d tokens=%d, want %s/%d/%d",
			streamID, state, clients, tokens, wantState, wantClients, wantTokens)
	}
}
