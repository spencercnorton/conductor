package store

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestPromoteDVRRecordingArtifactRejectsBlankFilesystemIdentity(t *testing.T) {
	db := &DB{}
	result, err := db.PromoteDVRRecordingArtifact(
		context.Background(), uuid.New(), uuid.New(), 1,
		DVRMediaReport{DurationMS: 1}, make([]byte, 32), "",
		"op476-test-v1", "/recordings/episode.ts", "")
	if err == nil || !strings.Contains(err.Error(), "filesystem identity") ||
		result != (DVRCompletionResult{}) {
		t.Fatalf("result=%+v err=%v, want pre-transaction filesystem identity rejection",
			result, err)
	}
}

func TestSameDVRProgramIdentity(t *testing.T) {
	tests := []struct {
		name          string
		owner, target DVRRecording
		want          bool
	}{
		{
			name:   "episode number survives corrected subtitle",
			owner:  DVRRecording{Title: "Show", SubTitle: "Old", EpisodeNumOnscreen: "S01E02"},
			target: DVRRecording{Title: "Show", SubTitle: "Corrected", EpisodeNumOnscreen: "S01E02"},
			want:   true,
		},
		{
			name:   "different episode rejected",
			owner:  DVRRecording{Title: "Show", EpisodeNumOnscreen: "S01E02"},
			target: DVRRecording{Title: "Show", EpisodeNumOnscreen: "S01E03"},
		},
		{
			name:   "xmltv identity fallback",
			owner:  DVRRecording{Title: "Show", EpisodeNumXMLTV: "0.1/1"},
			target: DVRRecording{Title: "Show", EpisodeNumXMLTV: "0.1/1"},
			want:   true,
		},
		{
			name:   "unnumbered exact subtitle",
			owner:  DVRRecording{Title: "Special", SubTitle: "Part One"},
			target: DVRRecording{Title: "Special", SubTitle: "Part One"},
			want:   true,
		},
		{
			name:   "unnumbered missing subtitle fails closed",
			owner:  DVRRecording{Title: "Special"},
			target: DVRRecording{Title: "Special"},
		},
		{
			name:   "same movie title",
			owner:  DVRRecording{Title: "Heat", IsMovie: true},
			target: DVRRecording{Title: "Heat", IsMovie: true},
			want:   true,
		},
		{
			name:   "different movie title",
			owner:  DVRRecording{Title: "Heat", IsMovie: true},
			target: DVRRecording{Title: "Speed", IsMovie: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sameDVRProgramIdentity(tt.owner, tt.target); got != tt.want {
				t.Fatalf("sameDVRProgramIdentity=%v, want %v", got, tt.want)
			}
		})
	}
}

func TestIntegrationDVRArtifactPromotionCommitAmbiguityReplaysExactOperation(t *testing.T) {
	db := openCommitAmbiguityDB(t)
	recording := seedClaimedCommitAmbiguityRecording(t, db, "ambiguous artifact promotion")
	ctx := context.Background()
	if err := db.MarkDVRArtifactStage(
		ctx, recording.ID, "publishing", recording.OutputPath+".normalized"); err != nil {
		t.Fatal(err)
	}

	operationID := uuid.New()
	sha := bytes.Repeat([]byte{0x47}, 32)
	filesystemID := "dev:op476-commit-test"
	report := DVRMediaReport{
		DurationMS: 3_500_000, MaxGapMS: 40,
		AVStartDeltaMS: 12, AVEndDeltaMS: 18, AVDriftMS: 6,
	}
	simulated := errors.New("simulated lost DVR artifact promotion COMMIT response")
	db.dvrCompletionCommitResultHook = func() error {
		db.dvrCompletionCommitResultHook = nil
		return simulated
	}

	provisional, err := db.PromoteDVRRecordingArtifact(
		ctx, recording.ID, operationID, 4_760_000, report, sha,
		filesystemID, "op476-commit-test-v1", recording.OutputPath, "")
	if !errors.Is(err, simulated) || !errors.Is(err, ErrDVRArtifactCommitOutcomeUnknown) {
		t.Fatalf("ambiguous promotion err=%v, want simulated+unknown-outcome markers", err)
	}
	if provisional != (DVRCompletionResult{}) {
		t.Fatalf("ambiguous promotion leaked provisional result %+v", provisional)
	}

	var state, artifactState, persistedFilesystemID, validatorVersion string
	var persistedOperationID *uuid.UUID
	var persistedSHA []byte
	var current bool
	if err := db.Pool.QueryRow(ctx, `
		SELECT state::text, completion_operation_id, artifact_state,
		       artifact_current, artifact_sha256, artifact_filesystem_id,
		       artifact_validator_version
		  FROM dvr_recording WHERE id=$1`, recording.ID).
		Scan(&state, &persistedOperationID, &artifactState, &current,
			&persistedSHA, &persistedFilesystemID, &validatorVersion); err != nil {
		t.Fatal(err)
	}
	if state != "completed" || persistedOperationID == nil ||
		*persistedOperationID != operationID || artifactState != "validated" ||
		!current || !bytes.Equal(persistedSHA, sha) || persistedFilesystemID != filesystemID ||
		validatorVersion != "op476-commit-test-v1" {
		t.Fatalf("durable promotion state=%s operation=%v artifact=%s current=%v sha=%x filesystem=%q validator=%q",
			state, persistedOperationID, artifactState, current, persistedSHA,
			persistedFilesystemID, validatorVersion)
	}

	replayed, err := db.PromoteDVRRecordingArtifact(
		ctx, recording.ID, operationID, 1, report, bytes.Repeat([]byte{0x99}, 32),
		"dev:ignored-replay", "ignored-replay", recording.OutputPath, "")
	if err != nil || replayed.State != "completed" || !replayed.OperationOwned {
		t.Fatalf("exact replay result=%+v err=%v", replayed, err)
	}
	foreign, err := db.PromoteDVRRecordingArtifact(
		ctx, recording.ID, uuid.New(), 1, report, bytes.Repeat([]byte{0x99}, 32),
		"dev:ignored-replay", "ignored-replay", recording.OutputPath, "")
	if err != nil || foreign.State != "completed" || foreign.OperationOwned {
		t.Fatalf("foreign replay result=%+v err=%v", foreign, err)
	}
}
