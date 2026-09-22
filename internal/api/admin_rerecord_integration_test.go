package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/auth"
	"github.com/spencercnorton/conductor/internal/dvr"
	"github.com/spencercnorton/conductor/internal/store"
)

func seedAPIRerecordProgram(
	t *testing.T,
	db *store.DB,
	channelID uuid.UUID,
	start time.Time,
	title, subtitle, onscreen string,
) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	err := db.Pool.QueryRow(context.Background(), `
		INSERT INTO epg_program (
			channel_id, start_at, end_at, title, sub_title,
			episode_num_xmltv, episode_num_onscreen, is_canonical, source_hash
		) VALUES ($1,$2,$3,$4,$5,'0.0/1',$6,true,$7)
		RETURNING id`, channelID, start, start.Add(time.Hour), title, subtitle,
		onscreen, "op476-api:"+uuid.NewString()).Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func rerecordRequest(t *testing.T, mux http.Handler, key string, ownerID, programID uuid.UUID) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(map[string]any{"program_id": programID})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost,
		"/admin/dvr/recordings/"+ownerID.String()+"/rerecord", bytes.NewReader(body))
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, req)
	return response
}

func TestIntegrationAdminRerecordIsAuthenticatedTargetBoundAndIdempotent(t *testing.T) {
	db := openAPICancelIntegrationDB(t)
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	provider, err := db.CreateProvider(ctx, store.Provider{
		Name: "api-rerecord-" + uuid.NewString(), Kind: "m3u_xtream",
		BaseURL: "https://example.invalid", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateCredential(ctx, store.ProviderCredential{
		ProviderID: provider.ID, Username: "api-rerecord", PasswordEnc: []byte{1},
		MaxStreams: 2, Priority: 100, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	var channelNumber float64
	if err := db.Pool.QueryRow(ctx,
		`SELECT (COALESCE(MAX(number), 9899) + 0.1)::float8 FROM channel`).Scan(&channelNumber); err != nil {
		t.Fatal(err)
	}
	channel, err := db.CreateChannel(ctx, store.Channel{
		Number: channelNumber, Name: "api-rerecord-" + uuid.NewString(), Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateChannelSource(ctx, store.ChannelSource{
		ChannelID: channel.ID, ProviderID: provider.ID,
		UpstreamURL: "https://example.invalid/live.ts", Priority: 0,
		HealthScore: 1, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}

	root := t.TempDir()
	title := "FX476Target" + uuid.NewString()
	output := filepath.Join(root, "TV", title, "Season 01",
		title+".S01E01.Pilot.ts")
	if err := os.MkdirAll(filepath.Dir(output), 0o755); err != nil {
		t.Fatal(err)
	}
	ownerBytes := bytes.Repeat([]byte{0x47}, 1_000_000)
	if err := os.WriteFile(output, ownerBytes, 0o644); err != nil {
		t.Fatal(err)
	}
	ownerStart := time.Now().Add(-3 * time.Hour).Truncate(time.Second)
	owner, err := db.CreateDVRRecording(ctx, store.DVRRecording{
		ChannelID: channel.ID, Title: title, SubTitle: "Pilot",
		EpisodeNumXMLTV: "0.0/1", EpisodeNumOnscreen: "S01E01",
		ScheduledStart: ownerStart, ScheduledEnd: ownerStart.Add(time.Hour),
		Priority: 100, RequestedBy: "op476-api-test", OutputPath: output,
	})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := db.TryMarkDVRRecordingStarted(ctx, owner.ID, output)
	if err != nil || !claimed {
		t.Fatalf("claim owner: claimed=%v err=%v", claimed, err)
	}
	if err := db.MarkDVRArtifactStage(ctx, owner.ID, "publishing", output+".candidate"); err != nil {
		t.Fatal(err)
	}
	ownerSHA := sha256.Sum256(ownerBytes)
	filesystemID, err := dvr.ArtifactFilesystemIdentity(output)
	if err != nil {
		t.Fatal(err)
	}
	result, err := db.PromoteDVRRecordingArtifact(
		ctx, owner.ID, uuid.New(), 1_000_000,
		store.DVRMediaReport{DurationMS: 3_590_000, MaxGapMS: 40},
		ownerSHA[:], filesystemID, "op476-api-test-v1", output, "")
	if err != nil || result.State != "completed" || !result.OperationOwned {
		t.Fatalf("promote owner result=%+v err=%v", result, err)
	}

	targetStart := time.Now().Add(3 * time.Hour).Truncate(time.Second)
	targetID := seedAPIRerecordProgram(
		t, db, channel.ID, targetStart, title, "Pilot", "S01E01")
	pastID := seedAPIRerecordProgram(
		t, db, channel.ID, time.Now().Add(-2*time.Hour).Truncate(time.Second),
		title, "Pilot", "S01E01")
	activeID := seedAPIRerecordProgram(
		t, db, channel.ID, time.Now().Add(-30*time.Minute).Truncate(time.Second),
		title, "Pilot", "S01E01")
	mismatchID := seedAPIRerecordProgram(
		t, db, channel.ID, targetStart.Add(2*time.Hour), title, "Second", "S01E02")

	const adminKey = "rerecord-key"
	mux := http.NewServeMux()
	MountAdmin(mux, AdminDeps{
		Logger: logger,
		DB:     db,
		Auth: auth.Config{
			Enabled: true, AdminAPIKey: adminKey,
		},
		DVRSchedule: &dvr.ScheduleHandler{DB: db, OutputDir: root, Priority: 100},
	})

	unauthorized := rerecordRequest(t, mux, "", owner.ID, targetID)
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status=%d body=%s, want 401",
			unauthorized.Code, unauthorized.Body.String())
	}

	created := rerecordRequest(t, mux, adminKey, owner.ID, targetID)
	if created.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s, want 201", created.Code, created.Body.String())
	}
	var replacement store.DVRRecording
	if err := json.NewDecoder(created.Body).Decode(&replacement); err != nil {
		t.Fatal(err)
	}
	if !replacement.ExplicitRerecord || replacement.ReplacesRecordingID == nil ||
		*replacement.ReplacesRecordingID != owner.ID || replacement.ProgramID == nil ||
		*replacement.ProgramID != targetID || replacement.OutputPath != output {
		t.Fatalf("replacement was not exactly target-bound: %+v", replacement)
	}
	if got := created.Header().Get("Location"); got != "/admin/dvr/recordings/"+replacement.ID.String() {
		t.Fatalf("Location=%q", got)
	}

	replayed := rerecordRequest(t, mux, adminKey, owner.ID, targetID)
	if replayed.Code != http.StatusCreated {
		t.Fatalf("replay status=%d body=%s, want 201", replayed.Code, replayed.Body.String())
	}
	var replay store.DVRRecording
	if err := json.NewDecoder(replayed.Body).Decode(&replay); err != nil {
		t.Fatal(err)
	}
	if replay.ID != replacement.ID {
		t.Fatalf("idempotent replay id=%s, want %s", replay.ID, replacement.ID)
	}

	missing := rerecordRequest(t, mux, adminKey, owner.ID, uuid.New())
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing target status=%d body=%s, want 404", missing.Code, missing.Body.String())
	}
	past := rerecordRequest(t, mux, adminKey, owner.ID, pastID)
	if past.Code != http.StatusUnprocessableEntity {
		t.Fatalf("past target status=%d body=%s, want 422", past.Code, past.Body.String())
	}
	active := rerecordRequest(t, mux, adminKey, owner.ID, activeID)
	if active.Code != http.StatusUnprocessableEntity {
		t.Fatalf("active target status=%d body=%s, want 422", active.Code, active.Body.String())
	}
	mismatch := rerecordRequest(t, mux, adminKey, owner.ID, mismatchID)
	if mismatch.Code != http.StatusConflict {
		t.Fatalf("mismatched target status=%d body=%s, want 409", mismatch.Code, mismatch.Body.String())
	}

	// Exact retries bypass mutable predecessor bytes and guide canonicality.
	// This reproduces the filesystem-first seam after the replacement has
	// installed different-size bytes but before database ownership promotion.
	claimed, err = db.TryMarkDVRRecordingStarted(ctx, replacement.ID, output)
	if err != nil || !claimed {
		t.Fatalf("claim publishing replacement: claimed=%v err=%v", claimed, err)
	}
	if err := db.MarkDVRArtifactStage(
		ctx, replacement.ID, "publishing", output+".candidate",
	); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(output, output+".predecessor"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(output, bytes.Repeat([]byte{0x99}, 4096), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool.Exec(ctx,
		`UPDATE epg_program SET is_canonical=false WHERE id=$1`, targetID); err != nil {
		t.Fatal(err)
	}
	seamReplay := rerecordRequest(t, mux, adminKey, owner.ID, targetID)
	if seamReplay.Code != http.StatusCreated {
		t.Fatalf("publishing-seam replay status=%d body=%s, want 201",
			seamReplay.Code, seamReplay.Body.String())
	}
	var replayDuringPublish store.DVRRecording
	if err := json.NewDecoder(seamReplay.Body).Decode(&replayDuringPublish); err != nil {
		t.Fatal(err)
	}
	if replayDuringPublish.ID != replacement.ID ||
		replayDuringPublish.ArtifactStage != "publishing" {
		t.Fatalf("publishing-seam replay=%+v, want exact %s", replayDuringPublish, replacement.ID)
	}
}
