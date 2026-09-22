//go:build !short

package store_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/spencercnorton/conductor/internal/metrics"
	"github.com/spencercnorton/conductor/internal/store"
)

var validatedArtifactReport = store.DVRMediaReport{
	DurationMS:      3_590_000,
	MaxGapMS:        41,
	Discontinuities: 0,
	AVStartDeltaMS:  17,
	AVEndDeltaMS:    23,
	AVDriftMS:       6,
}

func seedDVRArtifactProgram(
	t *testing.T,
	db *store.DB,
	channelID uuid.UUID,
	start time.Time,
	title string,
) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	err := db.Pool.QueryRow(context.Background(), `
		INSERT INTO epg_program (channel_id, start_at, end_at, title, source_hash)
		VALUES ($1,$2,$3,$4,$5)
		RETURNING id`, channelID, start, start.Add(time.Hour), title,
		"op476:"+uuid.NewString()).Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func dvrArtifactIntent(
	channelID, programID uuid.UUID,
	start time.Time,
	title, output string,
) store.DVRRecording {
	return store.DVRRecording{
		ChannelID:      channelID,
		ProgramID:      &programID,
		Title:          title,
		ScheduledStart: start,
		ScheduledEnd:   start.Add(time.Hour),
		Priority:       100,
		RequestedBy:    "op476-integration",
		OutputPath:     output,
	}
}

func claimAndPromoteDVRArtifact(
	t *testing.T,
	db *store.DB,
	rec store.DVRRecording,
	operationID uuid.UUID,
	backup string,
) store.DVRRecording {
	t.Helper()
	ctx := context.Background()
	claimed, err := db.TryMarkDVRRecordingStarted(ctx, rec.ID, rec.OutputPath)
	if err != nil || !claimed {
		t.Fatalf("claim artifact recording: claimed=%v err=%v", claimed, err)
	}
	if err := db.MarkDVRArtifactStage(ctx, rec.ID, "publishing", rec.OutputPath+".normalized"); err != nil {
		t.Fatal(err)
	}
	result, err := db.PromoteDVRRecordingArtifact(
		ctx, rec.ID, operationID, 4_760_000, validatedArtifactReport,
		bytes.Repeat([]byte{0x47}, 32), "dev:op476-test",
		"op476-test-v1", rec.OutputPath, backup)
	if err != nil || result.State != "completed" || !result.OperationOwned {
		t.Fatalf("promote result=%+v err=%v", result, err)
	}
	got, err := db.GetDVRRecording(ctx, rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func TestIntegrationDVRArtifactOwnershipAndReplacement(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	providerID, _ := seedProviderWithCredentials(t, db, 1, 4)
	channel := seedLeaseChannel(t, db, providerID, 9476.1, 1)
	base := time.Now().Add(2 * time.Hour).Truncate(time.Second)

	t.Run("legacy owner remains recoverable but never suppresses", func(t *testing.T) {
		output := t.TempDir() + "/legacy.ts"
		program1 := seedDVRArtifactProgram(t, db, channel.ID, base, "Legacy owner")
		legacy, err := db.CreateDVRRecording(ctx,
			dvrArtifactIntent(channel.ID, program1, base, "Legacy owner", output))
		if err != nil {
			t.Fatal(err)
		}
		claimed, err := db.TryMarkDVRRecordingStarted(ctx, legacy.ID, output)
		if err != nil || !claimed {
			t.Fatalf("claim legacy: claimed=%v err=%v", claimed, err)
		}
		if result, err := db.MarkDVRRecordingCompleted(ctx, legacy.ID, uuid.New(), 1234); err != nil ||
			result.State != "completed" {
			t.Fatalf("legacy completion result=%+v err=%v", result, err)
		}
		if _, err := db.Pool.Exec(ctx, `
			UPDATE dvr_recording
			   SET artifact_state='legacy', artifact_current=true,
			       artifact_path=output_path, artifact_bytes=bytes_written,
			       artifact_filesystem_id='dev:legacy-preflight'
			 WHERE id=$1`, legacy.ID); err != nil {
			t.Fatal(err)
		}

		program2 := seedDVRArtifactProgram(t, db, channel.ID, base.Add(2*time.Hour), "Legacy repeat")
		candidateIntent := dvrArtifactIntent(
			channel.ID, program2, base.Add(2*time.Hour), "Legacy repeat", output)
		preflightOwner, err := db.CreateDVRRecordingWithArtifactPreflight(
			ctx, candidateIntent, nil)
		if !errors.Is(err, store.ErrDVRArtifactOwnerPreflightRequired) ||
			preflightOwner.ID != legacy.ID {
			t.Fatalf("legacy preflight owner=%+v err=%v", preflightOwner, err)
		}
		var prematureRows int
		if err := db.Pool.QueryRow(ctx, `
			SELECT count(*) FROM dvr_recording
			 WHERE channel_id=$1 AND scheduled_start=$2 AND title=$3`,
			candidateIntent.ChannelID, candidateIntent.ScheduledStart,
			candidateIntent.Title).Scan(&prematureRows); err != nil {
			t.Fatal(err)
		}
		if prematureRows != 0 {
			t.Fatalf("artifact preflight exposed %d runnable successor row(s)", prematureRows)
		}
		pending, err := db.ListPendingDVRRecordings(ctx, 3*time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		for _, pendingRec := range pending {
			if pendingRec.ProgramID != nil && *pendingRec.ProgramID == program2 {
				t.Fatalf("scheduler observed successor before artifact proof: %+v", pendingRec)
			}
		}

		unboundOwner := store.NewDVRArtifactOwnerProof(preflightOwner)
		unboundOwner.ArtifactFilesystemID = ""
		changedOwner, err := db.CreateDVRRecordingWithArtifactPreflight(
			ctx, candidateIntent, &unboundOwner)
		if !errors.Is(err, store.ErrDVRArtifactOwnerChanged) || changedOwner.ID != legacy.ID {
			t.Fatalf("unbound artifact proof owner=%+v err=%v", changedOwner, err)
		}
		wrongOwner := store.NewDVRArtifactOwnerProof(preflightOwner)
		wrongOwner.RecordingID = uuid.New()
		changedOwner, err = db.CreateDVRRecordingWithArtifactPreflight(
			ctx, candidateIntent, &wrongOwner)
		if !errors.Is(err, store.ErrDVRArtifactOwnerChanged) || changedOwner.ID != legacy.ID {
			t.Fatalf("stale artifact preflight owner=%+v err=%v", changedOwner, err)
		}
		staleEvidence := store.NewDVRArtifactOwnerProof(preflightOwner)
		staleEvidence.ArtifactBytes++
		changedOwner, err = db.CreateDVRRecordingWithArtifactPreflight(
			ctx, candidateIntent, &staleEvidence)
		if !errors.Is(err, store.ErrDVRArtifactOwnerChanged) || changedOwner.ID != legacy.ID {
			t.Fatalf("stale artifact evidence owner=%+v err=%v", changedOwner, err)
		}
		ownerProof := store.NewDVRArtifactOwnerProof(preflightOwner)
		if _, err := db.Pool.Exec(ctx, `
			UPDATE dvr_recording
			   SET artifact_current=false, artifact_state='missing'
			 WHERE id=$1`, legacy.ID); err != nil {
			t.Fatal(err)
		}
		missingOwner, err := db.CreateDVRRecordingWithArtifactPreflight(
			ctx, candidateIntent, &ownerProof)
		if !errors.Is(err, store.ErrDVRArtifactOwnerChanged) || missingOwner.ID != uuid.Nil {
			t.Fatalf("disappeared artifact owner result=%+v err=%v", missingOwner, err)
		}
		if _, err := db.Pool.Exec(ctx, `
			UPDATE dvr_recording
			   SET artifact_current=true, artifact_state='legacy'
			 WHERE id=$1`, legacy.ID); err != nil {
			t.Fatal(err)
		}

		type createResult struct {
			rec store.DVRRecording
			err error
		}
		startCreate := make(chan struct{})
		createResults := make(chan createResult, 2)
		for range 2 {
			go func() {
				<-startCreate
				rec, createErr := db.CreateDVRRecordingWithArtifactPreflight(
					context.Background(), candidateIntent, &ownerProof)
				createResults <- createResult{rec: rec, err: createErr}
			}()
		}
		close(startCreate)
		first := <-createResults
		second := <-createResults
		if first.err != nil || second.err != nil || first.rec.ID != second.rec.ID {
			t.Fatalf("concurrent checked creates first=%+v second=%+v", first, second)
		}
		candidate := first.rec
		if candidate.ReplacesRecordingID == nil || *candidate.ReplacesRecordingID != legacy.ID ||
			candidate.ExplicitRerecord {
			t.Fatalf("legacy successor provenance=%+v, want automatic replaces=%s", candidate, legacy.ID)
		}
		exact, err := db.CreateDVRRecordingWithArtifactPreflight(ctx, candidateIntent, nil)
		if err != nil || exact.ID != candidate.ID {
			t.Fatalf("preflight exact retry=%+v err=%v, want idempotent successor", exact, err)
		}

		program3 := seedDVRArtifactProgram(t, db, channel.ID, base.Add(4*time.Hour), "Busy repeat")
		busy, err := db.CreateDVRRecording(ctx,
			dvrArtifactIntent(channel.ID, program3, base.Add(4*time.Hour), "Busy repeat", output))
		if !errors.Is(err, store.ErrOutputPathBusy) || busy.ID != candidate.ID {
			t.Fatalf("active output result=%+v err=%v", busy, err)
		}
		if err := db.MarkDVRRecordingCancelled(ctx, candidate.ID); err != nil {
			t.Fatal(err)
		}
		program4 := seedDVRArtifactProgram(
			t, db, channel.ID, base.Add(6*time.Hour), "Legacy direct repeat")
		direct, err := db.CreateDVRRecording(ctx, dvrArtifactIntent(
			channel.ID, program4, base.Add(6*time.Hour), "Legacy direct repeat", output))
		if err != nil || direct.ReplacesRecordingID == nil ||
			*direct.ReplacesRecordingID != legacy.ID {
			t.Fatalf("backward-compatible legacy create=%+v err=%v", direct, err)
		}
	})

	t.Run("validated owner suppresses automation but exact airing is idempotent", func(t *testing.T) {
		output := t.TempDir() + "/validated.ts"
		program1 := seedDVRArtifactProgram(t, db, channel.ID, base.Add(10*time.Hour), "Validated owner")
		intent := dvrArtifactIntent(channel.ID, program1, base.Add(10*time.Hour), "Validated owner", output)
		intent.EpisodeNumOnscreen = "S01E01"
		if _, err := db.Pool.Exec(ctx,
			`UPDATE epg_program SET episode_num_onscreen=$2 WHERE id=$1`, program1, "S01E01"); err != nil {
			t.Fatal(err)
		}
		owner, err := db.CreateDVRRecording(ctx, intent)
		if err != nil {
			t.Fatal(err)
		}
		operationID := uuid.New()
		owner = claimAndPromoteDVRArtifact(t, db, owner, operationID, "")
		if !owner.ArtifactCurrent || owner.ArtifactState != "validated" ||
			owner.ArtifactValidatorVersion != "op476-test-v1" ||
			owner.ArtifactFilesystemID != "dev:op476-test" ||
			!bytes.Equal(owner.ArtifactSHA256, bytes.Repeat([]byte{0x47}, 32)) {
			t.Fatalf("validated evidence not persisted: %+v", owner)
		}
		if owner.MediaAVStartDeltaMS != validatedArtifactReport.AVStartDeltaMS ||
			owner.MediaAVEndDeltaMS != validatedArtifactReport.AVEndDeltaMS ||
			owner.MediaAVDriftMS != validatedArtifactReport.AVDriftMS {
			t.Fatalf("A/V evidence not persisted: %+v", owner)
		}
		if err := db.BindCurrentDVRArtifactFilesystem(
			ctx, owner.ID, output, owner.ArtifactFilesystemID,
		); err != nil {
			t.Fatalf("exact filesystem binding replay: %v", err)
		}
		if err := db.BindCurrentDVRArtifactFilesystem(
			ctx, owner.ID, output, "dev:conflicting-filesystem",
		); !errors.Is(err, store.ErrDVRArtifactFilesystemConflict) {
			t.Fatalf("conflicting filesystem bind err=%v", err)
		}
		if demoted, err := db.DemoteMissingDVRArtifact(
			ctx, owner.ID, output, "dev:stale-inspection",
			"stale filesystem identity must not demote",
		); demoted || !errors.Is(err, store.ErrDVRArtifactFilesystemConflict) {
			t.Fatalf("stale filesystem demotion=%v err=%v", demoted, err)
		}

		exact, err := db.CreateDVRRecording(ctx, intent)
		if err != nil || exact.ID != owner.ID {
			t.Fatalf("exact validated re-grab result=%+v err=%v", exact, err)
		}
		replay, err := db.PromoteDVRRecordingArtifact(
			ctx, owner.ID, operationID, 1, validatedArtifactReport,
			bytes.Repeat([]byte{0x99}, 32), "dev:ignored-replay",
			"ignored-replay", output, "")
		if err != nil || replay.State != "completed" || !replay.OperationOwned {
			t.Fatalf("exact promotion replay=%+v err=%v", replay, err)
		}
		foreign, err := db.PromoteDVRRecordingArtifact(
			ctx, owner.ID, uuid.New(), 1, validatedArtifactReport,
			bytes.Repeat([]byte{0x99}, 32), "dev:ignored-replay",
			"ignored-replay", output, "")
		if err != nil || foreign.State != "completed" || foreign.OperationOwned {
			t.Fatalf("foreign promotion replay=%+v err=%v", foreign, err)
		}

		program2 := seedDVRArtifactProgram(t, db, channel.ID, base.Add(12*time.Hour), "Suppressed repeat")
		suppressed, err := db.CreateDVRRecording(ctx,
			dvrArtifactIntent(channel.ID, program2, base.Add(12*time.Hour), "Suppressed repeat", output))
		if !errors.Is(err, store.ErrValidatedArtifactExists) || suppressed.ID != owner.ID {
			t.Fatalf("validated suppression result=%+v err=%v", suppressed, err)
		}

		bad := dvrArtifactIntent(channel.ID, uuid.Nil, base.Add(14*time.Hour), "Unbound", output)
		bad.ProgramID = nil
		if _, err := db.CreateDVRReplacement(ctx, owner.ID, bad); !errors.Is(err, store.ErrArtifactReplacementMismatch) {
			t.Fatalf("unbound replacement err=%v", err)
		}

		targetID := seedDVRArtifactProgram(t, db, channel.ID, base.Add(14*time.Hour), "Validated owner")
		if _, err := db.Pool.Exec(ctx,
			`UPDATE epg_program SET episode_num_onscreen=$2 WHERE id=$1`, targetID, "S01E01"); err != nil {
			t.Fatal(err)
		}
		replacementIntent := dvrArtifactIntent(
			channel.ID, targetID, base.Add(14*time.Hour), "Validated owner", output)
		replacementIntent.EpisodeNumOnscreen = "S01E01"
		forgedIntent := replacementIntent
		forgedIntent.EpisodeNumOnscreen = "S99E99"
		if _, err := db.CreateDVRReplacement(ctx, owner.ID, forgedIntent); !errors.Is(err, store.ErrArtifactReplacementMismatch) {
			t.Fatalf("forged replacement metadata err=%v", err)
		}
		noncanonicalID := seedDVRArtifactProgram(
			t, db, channel.ID, base.Add(16*time.Hour), "Validated owner")
		if _, err := db.Pool.Exec(ctx,
			`UPDATE epg_program SET is_canonical=false, episode_num_onscreen=$2 WHERE id=$1`,
			noncanonicalID, "S01E01"); err != nil {
			t.Fatal(err)
		}
		noncanonicalIntent := dvrArtifactIntent(
			channel.ID, noncanonicalID, base.Add(16*time.Hour), "Validated owner", output)
		noncanonicalIntent.EpisodeNumOnscreen = "S01E01"
		if _, err := db.CreateDVRReplacement(ctx, owner.ID, noncanonicalIntent); !errors.Is(err, store.ErrArtifactReplacementMismatch) {
			t.Fatalf("noncanonical replacement target err=%v", err)
		}
		replacement, err := db.CreateDVRReplacement(ctx, owner.ID, replacementIntent)
		if err != nil {
			t.Fatal(err)
		}
		if !replacement.ExplicitRerecord || replacement.ReplacesRecordingID == nil ||
			*replacement.ReplacesRecordingID != owner.ID || replacement.State != "scheduled" {
			t.Fatalf("replacement not atomically target-bound: %+v", replacement)
		}
		retried, err := db.CreateDVRReplacement(ctx, owner.ID, replacementIntent)
		if err != nil || retried.ID != replacement.ID {
			t.Fatalf("scheduled replacement retry=%+v err=%v", retried, err)
		}

		backup := output + ".predecessor"
		replacement = claimAndPromoteDVRArtifact(t, db, replacement, uuid.New(), backup)
		owner, err = db.GetDVRRecording(ctx, owner.ID)
		if err != nil {
			t.Fatal(err)
		}
		if owner.ArtifactCurrent || owner.ArtifactState != "superseded" || owner.ArtifactPath != backup {
			t.Fatalf("predecessor ownership not transferred: %+v", owner)
		}
		if !replacement.ArtifactCurrent || replacement.ArtifactState != "validated" {
			t.Fatalf("replacement did not become current: %+v", replacement)
		}
		// The same target-bound API request stays idempotent after ownerID was
		// demoted; no detached authorization is recreated.
		retried, err = db.CreateDVRReplacement(ctx, owner.ID, replacementIntent)
		if err != nil || retried.ID != replacement.ID || retried.State != "completed" {
			t.Fatalf("completed replacement retry=%+v err=%v", retried, err)
		}

		missingTargetID := seedDVRArtifactProgram(
			t, db, channel.ID, base.Add(18*time.Hour), "Validated owner")
		if _, err := db.Pool.Exec(ctx,
			`UPDATE epg_program SET episode_num_onscreen=$2 WHERE id=$1`,
			missingTargetID, "S01E01"); err != nil {
			t.Fatal(err)
		}
		missingIntent := dvrArtifactIntent(
			channel.ID, missingTargetID, base.Add(18*time.Hour), "Validated owner", output)
		missingIntent.EpisodeNumOnscreen = "S01E01"
		missingSuccessor, err := db.CreateDVRReplacement(ctx, replacement.ID, missingIntent)
		if err != nil {
			t.Fatal(err)
		}
		demoted, err := db.DemoteMissingDVRArtifact(
			ctx, replacement.ID, output, replacement.ArtifactFilesystemID,
			"canonical artifact missing in integration test")
		if err != nil || !demoted {
			t.Fatalf("demote missing owner=%v err=%v", demoted, err)
		}
		replacement, err = db.GetDVRRecording(ctx, replacement.ID)
		if err != nil || replacement.ArtifactCurrent || replacement.ArtifactState != "missing" {
			t.Fatalf("missing owner state=%+v err=%v", replacement, err)
		}
		missingSuccessor, err = db.GetDVRRecording(ctx, missingSuccessor.ID)
		if err != nil || missingSuccessor.ReplacesRecordingID != nil ||
			missingSuccessor.ExplicitRerecord {
			t.Fatalf("missing-owner successor retained replacement provenance=%+v err=%v",
				missingSuccessor, err)
		}
	})

	t.Run("rejection and interrupted publish retain diagnostics", func(t *testing.T) {
		output := t.TempDir() + "/rejected.ts"
		program := seedDVRArtifactProgram(t, db, channel.ID, base.Add(20*time.Hour), "Rejected artifact")
		rec, err := db.CreateDVRRecording(ctx,
			dvrArtifactIntent(channel.ID, program, base.Add(20*time.Hour), "Rejected artifact", output))
		if err != nil {
			t.Fatal(err)
		}
		claimed, err := db.TryMarkDVRRecordingStarted(ctx, rec.ID, output)
		if err != nil || !claimed {
			t.Fatalf("claim rejected row: claimed=%v err=%v", claimed, err)
		}
		candidatePath := output + ".normalized"
		if err := db.MarkDVRArtifactStage(ctx, rec.ID, "normalizing", candidatePath); err != nil {
			t.Fatal(err)
		}
		rejection := validatedArtifactReport
		rejection.Discontinuities = 7
		updated, err := db.RejectDVRRecordingArtifact(
			ctx, rec.ID, "A/V coverage failed", candidatePath, 9876, rejection)
		if err != nil || !updated {
			t.Fatalf("reject updated=%v err=%v", updated, err)
		}
		got, err := db.GetDVRRecording(ctx, rec.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.State != "failed" || got.ArtifactState != "rejected" ||
			got.ArtifactStage != "none" || got.ArtifactPath != candidatePath ||
			got.ArtifactError != "A/V coverage failed" || got.MediaDiscontinuities != 7 {
			t.Fatalf("rejection diagnostics=%+v", got)
		}

		publishOutput := t.TempDir() + "/interrupted.ts"
		publishProgram := seedDVRArtifactProgram(
			t, db, channel.ID, base.Add(22*time.Hour), "Interrupted publish")
		publish, err := db.CreateDVRRecording(ctx, dvrArtifactIntent(
			channel.ID, publishProgram, base.Add(22*time.Hour), "Interrupted publish", publishOutput))
		if err != nil {
			t.Fatal(err)
		}
		claimed, err = db.TryMarkDVRRecordingStarted(ctx, publish.ID, publishOutput)
		if err != nil || !claimed {
			t.Fatalf("claim publish row: claimed=%v err=%v", claimed, err)
		}
		publishCandidate := publishOutput + ".normalized"
		if err := db.MarkDVRArtifactStage(ctx, publish.ID, "publishing", publishCandidate); err != nil {
			t.Fatal(err)
		}
		failed, err := db.MarkDVRRecordingFailed(ctx, publish.ID, "process interrupted", 555)
		if err != nil || !failed {
			t.Fatalf("mark interrupted failed=%v err=%v", failed, err)
		}
		interrupted, err := db.ListInterruptedDVRPublishes(ctx)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, row := range interrupted {
			if row.ID == publish.ID {
				found = row.ArtifactStage == "publishing" && row.ArtifactPath == publishCandidate
			}
		}
		if !found {
			t.Fatalf("interrupted publish not listed: %+v", interrupted)
		}
		quarantined := publishOutput + ".quarantine"
		if err := db.FinishDVRPublishRecovery(ctx, publish.ID, "publish rolled back", quarantined); err != nil {
			t.Fatal(err)
		}
		if err := db.FinishDVRPublishRecovery(ctx, publish.ID, "publish rolled back", quarantined); err != nil {
			t.Fatalf("recovery replay: %v", err)
		}
		got, err = db.GetDVRRecording(ctx, publish.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.ArtifactStage != "none" || got.ArtifactState != "rejected" ||
			got.ArtifactPath != quarantined || !strings.Contains(got.Error, "publish rolled back") {
			t.Fatalf("publish recovery diagnostics=%+v", got)
		}
	})
}

func TestIntegrationDVRArtifactDemotionFencesTerminalPublishRecovery(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	providerID, _ := seedProviderWithCredentials(t, db, 1, 4)
	channel := seedLeaseChannel(t, db, providerID, 9476.3, 1)
	base := time.Now().Add(24 * time.Hour).UTC().Truncate(time.Second)

	for i, terminalState := range []string{"cancelled", "failed"} {
		t.Run(terminalState, func(t *testing.T) {
			ownerStart := base.Add(time.Duration(i) * 4 * time.Hour)
			output := t.TempDir() + "/" + terminalState + ".ts"
			title := "Terminal publish " + terminalState
			ownerProgram := seedDVRArtifactProgram(
				t, db, channel.ID, ownerStart, title)
			if _, err := db.Pool.Exec(ctx,
				`UPDATE epg_program SET episode_num_onscreen='S01E01' WHERE id=$1`,
				ownerProgram); err != nil {
				t.Fatal(err)
			}
			ownerIntent := dvrArtifactIntent(
				channel.ID, ownerProgram, ownerStart, title, output)
			ownerIntent.EpisodeNumOnscreen = "S01E01"
			owner, err := db.CreateDVRRecording(ctx, ownerIntent)
			if err != nil {
				t.Fatal(err)
			}
			owner = claimAndPromoteDVRArtifact(t, db, owner, uuid.New(), "")

			targetStart := ownerStart.Add(2 * time.Hour)
			targetProgram := seedDVRArtifactProgram(
				t, db, channel.ID, targetStart, title)
			if _, err := db.Pool.Exec(ctx,
				`UPDATE epg_program SET episode_num_onscreen='S01E01' WHERE id=$1`,
				targetProgram); err != nil {
				t.Fatal(err)
			}
			targetIntent := dvrArtifactIntent(
				channel.ID, targetProgram, targetStart, title, output)
			targetIntent.EpisodeNumOnscreen = "S01E01"
			successor, err := db.CreateDVRReplacement(ctx, owner.ID, targetIntent)
			if err != nil {
				t.Fatal(err)
			}
			claimed, err := db.TryMarkDVRRecordingStarted(ctx, successor.ID, output)
			if err != nil || !claimed {
				t.Fatalf("claim successor: claimed=%v err=%v", claimed, err)
			}
			if err := db.MarkDVRArtifactStage(
				ctx, successor.ID, "publishing", output+".candidate",
			); err != nil {
				t.Fatal(err)
			}
			switch terminalState {
			case "cancelled":
				if err := db.MarkDVRRecordingCancelled(ctx, successor.ID); err != nil {
					t.Fatal(err)
				}
			case "failed":
				updated, err := db.MarkDVRRecordingFailed(
					ctx, successor.ID, "terminal publish test", 1234)
				if err != nil || !updated {
					t.Fatalf("fail successor: updated=%v err=%v", updated, err)
				}
			}

			demoted, err := db.DemoteMissingDVRArtifact(
				ctx, owner.ID, output, owner.ArtifactFilesystemID,
				"candidate bytes differ before recovery")
			if demoted || !errors.Is(err, store.ErrDVRArtifactPublishInProgress) {
				t.Fatalf("terminal publish demotion=%v err=%v", demoted, err)
			}
			owner, err = db.GetDVRRecording(ctx, owner.ID)
			if err != nil {
				t.Fatal(err)
			}
			successor, err = db.GetDVRRecording(ctx, successor.ID)
			if err != nil {
				t.Fatal(err)
			}
			if !owner.ArtifactCurrent || owner.ArtifactState != "validated" ||
				successor.State != terminalState || successor.ArtifactStage != "publishing" ||
				successor.ReplacesRecordingID == nil ||
				*successor.ReplacesRecordingID != owner.ID {
				t.Fatalf("terminal publish fence lost ownership: owner=%+v successor=%+v",
					owner, successor)
			}
		})
	}
}

func TestIntegrationDVRCommittedIntentSurvivesForecastFailure(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	providerID, _ := seedProviderWithCredentials(t, db, 1, 4)
	channel := seedLeaseChannel(t, db, providerID, 9476.15, 1)
	start := time.Now().Add(2 * time.Hour).UTC().Truncate(time.Second)
	programID := seedDVRArtifactProgram(
		t, db, channel.ID, start, "Post-commit forecast owner")
	intent := dvrArtifactIntent(
		channel.ID, programID, start, "Post-commit forecast owner",
		t.TempDir()+"/postcommit.ts")

	lockTx, err := db.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lockTx.Rollback(context.Background()) }()
	if _, err := lockTx.Exec(ctx, `
		SELECT pg_advisory_xact_lock(
			hashtextextended('conductor:dvr-admission-forecast', 0)
		)`); err != nil {
		t.Fatal(err)
	}

	before := metrics.DVRAdmissionPostCommitObservationFailures.Value()
	createCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	rec, err := db.CreateDVRRecording(createCtx, intent)
	cancel()
	if err != nil {
		t.Fatalf("committed intent reported false failure after forecast timeout: %v", err)
	}
	if rec.ID == uuid.Nil || rec.State != "scheduled" {
		t.Fatalf("committed intent result=%+v", rec)
	}
	if got := metrics.DVRAdmissionPostCommitObservationFailures.Value(); got != before+1 {
		t.Fatalf("post-commit observation failures=%d, want %d", got, before+1)
	}
	if err := lockTx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	persisted, err := db.GetDVRRecording(ctx, rec.ID)
	if err != nil || persisted.State != "scheduled" {
		t.Fatalf("durable intent=%+v err=%v", persisted, err)
	}
}

func TestIntegrationDVRReplacementAdmissionDeadlineIsLockBound(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	providerID, _ := seedProviderWithCredentials(t, db, 1, 4)
	channel := seedLeaseChannel(t, db, providerID, 9476.2, 1)
	output := t.TempDir() + "/deadline.ts"
	ownerStart := time.Now().Add(-2 * time.Hour).UTC().Truncate(time.Second)
	ownerProgram := seedDVRArtifactProgram(
		t, db, channel.ID, ownerStart, "Deadline owner")
	if _, err := db.Pool.Exec(ctx,
		`UPDATE epg_program SET episode_num_onscreen='S01E01' WHERE id=$1`,
		ownerProgram); err != nil {
		t.Fatal(err)
	}
	ownerIntent := dvrArtifactIntent(
		channel.ID, ownerProgram, ownerStart, "Deadline owner", output)
	ownerIntent.EpisodeNumOnscreen = "S01E01"
	owner, err := db.CreateDVRRecording(ctx, ownerIntent)
	if err != nil {
		t.Fatal(err)
	}
	owner = claimAndPromoteDVRArtifact(t, db, owner, uuid.New(), "")

	startSlack := time.Second
	targetStart := time.Now().Add(3 * time.Second).UTC().Truncate(time.Millisecond)
	targetProgram := seedDVRArtifactProgram(
		t, db, channel.ID, targetStart, "Deadline owner")
	if _, err := db.Pool.Exec(ctx,
		`UPDATE epg_program SET episode_num_onscreen='S01E01' WHERE id=$1`,
		targetProgram); err != nil {
		t.Fatal(err)
	}
	targetIntent := dvrArtifactIntent(
		channel.ID, targetProgram, targetStart, "Deadline owner", output)
	targetIntent.EpisodeNumOnscreen = "S01E01"

	// Hold the same path lock CreateDVRReplacement acquires first. The request
	// begins while the target is admissible, but cannot reach its lock-bound
	// database-clock decision until after padded start.
	lockTx, err := db.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lockTx.Rollback(ctx) }()
	if _, err := lockTx.Exec(ctx, `
		SELECT pg_advisory_xact_lock(
			hashtextextended('conductor:dvr-output:' || $1, 0)
		)`, output); err != nil {
		t.Fatal(err)
	}
	boundary := targetStart.Add(-startSlack)
	var initiallyFuture bool
	if err := db.Pool.QueryRow(ctx,
		`SELECT clock_timestamp() < $1::timestamptz`, boundary).Scan(&initiallyFuture); err != nil {
		t.Fatal(err)
	}
	if !initiallyFuture {
		t.Fatal("test setup crossed padded start before the lock waiter began")
	}

	type replacementResult struct {
		rec store.DVRRecording
		err error
	}
	resultCh := make(chan replacementResult, 1)
	requestStarted := make(chan struct{})
	go func() {
		close(requestStarted)
		rec, createErr := db.CreateDVRReplacement(
			context.Background(), owner.ID, targetIntent,
			store.DVRAdmissionPolicy{StartSlack: startSlack, MaxOverrun: time.Second},
		)
		resultCh <- replacementResult{rec: rec, err: createErr}
	}()
	<-requestStarted
	for {
		var crossed bool
		if err := db.Pool.QueryRow(ctx,
			`SELECT clock_timestamp() >= $1::timestamptz`, boundary).Scan(&crossed); err != nil {
			t.Fatal(err)
		}
		if crossed {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := lockTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	result := <-resultCh
	if !errors.Is(result.err, store.ErrArtifactReplacementTargetNotFuture) ||
		result.rec.ID != uuid.Nil {
		t.Fatalf("post-boundary lock waiter result=%+v err=%v", result.rec, result.err)
	}
	var persisted int
	if err := db.Pool.QueryRow(ctx,
		`SELECT count(*) FROM dvr_recording WHERE program_id=$1`, targetProgram).
		Scan(&persisted); err != nil {
		t.Fatal(err)
	}
	if persisted != 0 {
		t.Fatalf("late replacement rows=%d, want 0", persisted)
	}

	// A row persisted while admissible remains an exact replay even when a
	// later request supplies a padding budget whose boundary has passed.
	replayStart := time.Now().Add(4 * time.Hour).UTC().Truncate(time.Second)
	replayProgram := seedDVRArtifactProgram(
		t, db, channel.ID, replayStart, "Deadline owner")
	if _, err := db.Pool.Exec(ctx,
		`UPDATE epg_program SET episode_num_onscreen='S01E01' WHERE id=$1`,
		replayProgram); err != nil {
		t.Fatal(err)
	}
	replayIntent := dvrArtifactIntent(
		channel.ID, replayProgram, replayStart, "Deadline owner", output)
	replayIntent.EpisodeNumOnscreen = "S01E01"
	created, err := db.CreateDVRReplacement(
		ctx, owner.ID, replayIntent,
		store.DVRAdmissionPolicy{StartSlack: time.Second, MaxOverrun: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := db.CreateDVRReplacement(
		ctx, owner.ID, replayIntent,
		store.DVRAdmissionPolicy{StartSlack: 24 * time.Hour, MaxOverrun: time.Second})
	if err != nil || replayed.ID != created.ID {
		t.Fatalf("post-boundary exact replay=%+v err=%v, want id=%s",
			replayed, err, created.ID)
	}
}

func TestIntegrationDVRArtifactMigrationBackfillsLegacyWithoutValidation(t *testing.T) {
	skipIfNoIntegration(t)
	ctx := context.Background()
	dsn := startTestPostgres(t)
	schema := "op476_" + strings.ReplaceAll(uuid.NewString(), "-", "")

	root, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := root.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		_ = root.Close(ctx)
		t.Fatal(err)
	}
	_ = root.Close(ctx)

	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema + ",public"
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	migrations, err := store.LoadMigrations()
	if err != nil {
		t.Fatal(err)
	}
	var migration24 store.Migration
	for _, migration := range migrations {
		if migration.Name == "0024_dvr_artifact_integrity.sql" {
			migration24 = migration
			break
		}
		if err := op476ApplyMigration(ctx, pool, migration); err != nil {
			t.Fatalf("apply pre-0024 %s: %v", migration.Name, err)
		}
	}
	if migration24.Name == "" {
		t.Fatal("migration 0024 not found")
	}

	var channelID uuid.UUID
	if err := pool.QueryRow(ctx, `
		INSERT INTO channel (number, name, enabled)
		VALUES (9476.24, 'FX476 migration', true)
		RETURNING id`).Scan(&channelID); err != nil {
		t.Fatal(err)
	}
	output := "/recordings/op476/legacy.ts"
	base := time.Date(2026, time.August, 23, 8, 0, 0, 0, time.UTC)
	completedIDs := make([]uuid.UUID, 0, 3)
	for i := 0; i < 3; i++ {
		var id uuid.UUID
		if err := pool.QueryRow(ctx, `
			INSERT INTO dvr_recording (
				channel_id, title, scheduled_start, scheduled_end, state,
				output_path, bytes_written, completed_at, created_at
			) VALUES ($1,$2,$3,$4,'completed',$5,$6,$7,$8)
			RETURNING id`, channelID, fmt.Sprintf("legacy-%d", i),
			base.Add(time.Duration(i)*time.Hour),
			base.Add(time.Duration(i+1)*time.Hour), output, 1000+i,
			base.Add(time.Duration(i+1)*time.Hour),
			base.Add(time.Duration(i)*time.Minute)).Scan(&id); err != nil {
			t.Fatal(err)
		}
		completedIDs = append(completedIDs, id)
	}
	var scheduledID uuid.UUID
	if err := pool.QueryRow(ctx, `
		INSERT INTO dvr_recording (
			channel_id, title, scheduled_start, scheduled_end, state, output_path
		) VALUES ($1,'already queued repeat',$2,$3,'scheduled',$4)
		RETURNING id`, channelID, base.Add(8*time.Hour), base.Add(9*time.Hour), output).
		Scan(&scheduledID); err != nil {
		t.Fatal(err)
	}

	if err := op476ApplyMigration(ctx, pool, migration24); err != nil {
		t.Fatal(err)
	}
	type artifactRow struct {
		id           uuid.UUID
		state        string
		current      bool
		sha          []byte
		filesystemID string
		validatedAt  *time.Time
	}
	rows, err := pool.Query(ctx, `
		SELECT id, artifact_state, artifact_current, artifact_sha256,
		       artifact_filesystem_id,
		       artifact_validated_at
		  FROM dvr_recording
		 WHERE state='completed'
		 ORDER BY completed_at`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []artifactRow
	for rows.Next() {
		var row artifactRow
		if err := rows.Scan(&row.id, &row.state, &row.current, &row.sha,
			&row.filesystemID, &row.validatedAt); err != nil {
			t.Fatal(err)
		}
		got = append(got, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("completed artifacts=%+v", got)
	}
	for i, row := range got {
		wantState := "historical"
		wantCurrent := false
		if i == len(got)-1 {
			wantState = "legacy"
			wantCurrent = true
		}
		if row.state != wantState || row.current != wantCurrent || row.sha != nil ||
			row.filesystemID != "" || row.validatedAt != nil {
			t.Fatalf("backfilled row %d=%+v, want state=%s current=%v and no validation evidence",
				i, row, wantState, wantCurrent)
		}
	}
	var scheduledState string
	var replaces *uuid.UUID
	var explicit bool
	if err := pool.QueryRow(ctx, `
		SELECT state::text, replaces_recording_id, explicit_rerecord
		  FROM dvr_recording WHERE id=$1`, scheduledID).
		Scan(&scheduledState, &replaces, &explicit); err != nil {
		t.Fatal(err)
	}
	newest := completedIDs[len(completedIDs)-1]
	if scheduledState != "scheduled" || replaces == nil || *replaces != newest || explicit {
		t.Fatalf("queued legacy successor state=%s replaces=%v explicit=%v, want scheduled/%s/false",
			scheduledState, replaces, explicit, newest)
	}
}

func op476ApplyMigration(ctx context.Context, pool *pgxpool.Pool, migration store.Migration) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, migration.SQL); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
