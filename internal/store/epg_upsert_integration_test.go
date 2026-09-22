// epg_upsert_integration_test.go — verifies the (channel_id, start_at)
// upsert semantics from migration 0013 (audit 2026-06-09 E3): one row per
// slot, priority-guarded DO UPDATE, content-hash change detection.
//
//	CONDUCTOR_INT_TEST=1 go test -race -count=1 ./internal/store -run Integration
package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/store"
)

func TestIntegrationEPGUpsertSemantics(t *testing.T) {
	skipIfNoIntegration(t)
	dsn := startTestPostgres(t)
	ctx := context.Background()

	db, err := store.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	ch, err := db.CreateChannel(ctx, store.Channel{Number: 901, Name: "Upsert Test", Enabled: true})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}

	start := time.Date(2026, 6, 10, 20, 0, 0, 0, time.UTC)
	prog := store.EPGProgram{
		ChannelID: ch.ID,
		StartAt:   start, EndAt: start.Add(time.Hour),
		Title: "Original Title", Description: "first",
		SourceHash: "hash-a", SourcePriority: 2,
	}

	// Fresh slot inserts.
	if action, err := db.UpsertEPGProgram(ctx, prog); err != nil || action != "added" {
		t.Fatalf("initial insert: action=%q err=%v", action, err)
	}

	// Identical content (same hash, same priority) is a no-op.
	if action, err := db.UpsertEPGProgram(ctx, prog); err != nil || action != "unchanged" {
		t.Fatalf("identical re-send: action=%q err=%v", action, err)
	}

	// Same priority + changed content updates in place (no second row).
	changed := prog
	changed.Title = "Renamed Title"
	changed.SourceHash = "hash-b"
	if action, err := db.UpsertEPGProgram(ctx, changed); err != nil || action != "updated" {
		t.Fatalf("same-priority content change: action=%q err=%v", action, err)
	}
	assertSlotState(t, db, ch, start, "Renamed Title", 2, 1)

	// A weaker source (higher number) must not clobber the slot.
	weaker := prog
	weaker.Title = "Weak Overwrite"
	weaker.SourceHash = "hash-c"
	weaker.SourcePriority = 9
	if action, err := db.UpsertEPGProgram(ctx, weaker); err != nil || action != "unchanged" {
		t.Fatalf("weaker-source write: action=%q err=%v", action, err)
	}
	assertSlotState(t, db, ch, start, "Renamed Title", 2, 1)

	// A stronger source (lower number) takes the slot over.
	stronger := prog
	stronger.Title = "PPV: Real Event Name"
	stronger.SourceHash = "ppv-parse:xtream:42:1765000000:PPV: Real Event Name"
	stronger.SourcePriority = store.PriorityPPVSync
	if action, err := db.UpsertEPGProgram(ctx, stronger); err != nil || action != "updated" {
		t.Fatalf("stronger-source write: action=%q err=%v", action, err)
	}
	assertSlotState(t, db, ch, start, "PPV: Real Event Name", store.PriorityPPVSync, 1)

	// ...and the original priority-2 source can no longer touch it.
	late := changed
	late.SourceHash = "hash-d"
	if action, err := db.UpsertEPGProgram(ctx, late); err != nil || action != "unchanged" {
		t.Fatalf("late weaker write: action=%q err=%v", action, err)
	}
	assertSlotState(t, db, ch, start, "PPV: Real Event Name", store.PriorityPPVSync, 1)

	// A different start_at on the same channel is its own slot.
	second := prog
	second.StartAt = start.Add(time.Hour)
	second.EndAt = start.Add(2 * time.Hour)
	second.SourceHash = "hash-e"
	if action, err := db.UpsertEPGProgram(ctx, second); err != nil || action != "added" {
		t.Fatalf("second slot insert: action=%q err=%v", action, err)
	}
}

func TestIntegrationSDDirectUpsertClaimsWholeLegacySlotBeforeXMLTVStartup(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	ch, err := db.CreateChannel(ctx, store.Channel{
		Number: 9475.01, Name: "SD-first legacy claim", EpgChannelID: "66442", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now().UTC().Truncate(time.Minute).Add(24 * time.Hour)
	if _, err := db.Pool.Exec(ctx, `
		INSERT INTO epg_program (
			channel_id, start_at, end_at, title, sub_title, description,
			episode_num_xmltv, episode_num_onscreen, source_poster_url,
			source_hash, source_priority, is_canonical, is_legacy
		) VALUES ($1,$2,$3,'Ambiguous legacy winner','Legacy subtitle','Legacy description',
		          '8.8.','S09E09','https://legacy.invalid/poster.jpg',
		          'opaque-pre-upgrade-hash',-50,true,true)`,
		ch.ID, start, start.Add(3*time.Hour)); err != nil {
		t.Fatal(err)
	}

	sdProgram := store.EPGProgram{
		ChannelID: ch.ID, StartAt: start, EndAt: start.Add(time.Hour),
		Title: "Schedules Direct programme", Description: "Explicit SD metadata",
		SourceHash: "sd:explicit-first-pass", SourcePriority: store.PrioritySD,
	}
	if action, err := db.UpsertEPGProgram(ctx, sdProgram); err != nil || action != "updated" {
		t.Fatalf("SD claim = action %q err %v; want whole legacy replacement", action, err)
	}
	var got store.EPGProgram
	var legacy bool
	if err := db.Pool.QueryRow(ctx, `
		SELECT end_at, title, sub_title, description, episode_num_xmltv,
		       episode_num_onscreen, source_poster_url, source_hash,
		       source_priority, is_legacy
		  FROM epg_program
		 WHERE channel_id=$1 AND start_at=$2 AND epg_source_id IS NULL`, ch.ID, start).Scan(
		&got.EndAt, &got.Title, &got.SubTitle, &got.Description,
		&got.EpisodeNumXMLTV, &got.EpisodeNumOnscreen, &got.SourcePosterURL,
		&got.SourceHash, &got.SourcePriority, &legacy,
	); err != nil {
		t.Fatal(err)
	}
	if !got.EndAt.Equal(sdProgram.EndAt) || got.Title != sdProgram.Title ||
		got.Description != sdProgram.Description || got.SubTitle != "" ||
		got.EpisodeNumXMLTV != "" || got.EpisodeNumOnscreen != "" ||
		got.SourcePosterURL != "" || got.SourceHash != sdProgram.SourceHash ||
		got.SourcePriority != store.PrioritySD || legacy {
		t.Fatalf("legacy slot was not wholly claimed by SD: %+v legacy=%v", got, legacy)
	}
}

func TestIntegrationDirectOverrideInvalidatesAndRestoresSDSnapshot(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	channel, err := db.CreateChannel(ctx, store.Channel{
		Number: 9475.02, Name: "SD override watermark", EpgChannelID: "9475.02", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	unrelatedChannel, err := db.CreateChannel(ctx, store.Channel{
		Number: 9475.12, Name: "Unrelated SD watermark", EpgChannelID: "9475.12", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now().UTC().Truncate(time.Minute).Add(36 * time.Hour)
	const (
		lineupID          = "fx475-sd-override-lineup"
		stationID         = "fx475-sd-override-station"
		stationMD5        = "unchanged-provider-md5"
		unrelatedLineupID = "fx475-sd-unrelated-lineup"
		unrelatedStation  = "fx475-sd-unrelated-station"
		unrelatedMD5      = "unrelated-provider-md5"
		streamKey         = "fx475-sd-override-stream"
	)
	sdProgram := store.EPGProgram{
		ChannelID: channel.ID, StartAt: start, EndAt: start.Add(time.Hour),
		Title: "Schedules Direct retained programme", EpisodeNumOnscreen: "S01E01",
		SourceHash: "sd:fx475-override-original", SourcePriority: store.PrioritySD,
	}
	unrelatedProgram := sdProgram
	unrelatedProgram.ChannelID = unrelatedChannel.ID
	unrelatedProgram.Title = "Unrelated Schedules Direct programme"
	unrelatedProgram.SourceHash = "sd:fx475-unrelated"
	for _, lineup := range []string{lineupID, unrelatedLineupID} {
		if _, err := db.Pool.Exec(ctx, `
			INSERT INTO sd_lineup (sd_lineup_id, name) VALUES ($1, $1)`, lineup); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ReplaceSDStationSnapshot(ctx, unrelatedLineupID, unrelatedStation, unrelatedMD5,
		unrelatedChannel.ID, []store.EPGProgram{unrelatedProgram}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ReplaceSDStationSnapshot(ctx, lineupID, stationID, stationMD5, channel.ID, []store.EPGProgram{sdProgram}); err != nil {
		t.Fatal(err)
	}
	if md5, err := db.GetSDStationMD5(ctx, lineupID, stationID); err != nil || md5 != stationMD5 {
		t.Fatalf("initial SD watermark = %q err %v", md5, err)
	}

	observedAt := time.Now().UTC()
	ppv := store.EPGProgram{
		ChannelID: channel.ID, StartAt: start, EndAt: start.Add(time.Hour),
		Title:      "Temporary stronger override",
		SourceHash: "ppv-parse:" + streamKey + ":temporary", SourcePriority: store.PriorityPPVSync,
	}
	if action, _, applied, err := db.ReplacePPVParsedRowsForChannel(ctx, ppv, observedAt); err != nil || action != "updated" || !applied {
		t.Fatalf("PPV override = action %q applied %v err %v", action, applied, err)
	}
	if md5, err := db.GetSDStationMD5(ctx, lineupID, stationID); err != nil || md5 != "" {
		t.Fatalf("displaced SD watermark = %q err %v; want empty", md5, err)
	}
	if md5, err := db.GetSDStationMD5(ctx, unrelatedLineupID, unrelatedStation); err != nil || md5 != unrelatedMD5 {
		t.Fatalf("unrelated SD watermark = %q err %v; want %q", md5, err, unrelatedMD5)
	}
	if deleted, applied, err := db.DeletePPVParsedRowsForChannelStream(ctx, channel.ID, streamKey, observedAt.Add(time.Second)); err != nil || deleted != 1 || !applied {
		t.Fatalf("PPV withdrawal = deleted %d applied %v err %v", deleted, applied, err)
	}

	// The provider still reports the exact same MD5. Because displacement
	// cleared the watermark, the worker's repository step is not skipped and the
	// valid SD candidate can become durable again.
	if result, err := db.ReplaceSDStationSnapshot(ctx, lineupID, stationID, stationMD5, channel.ID, []store.EPGProgram{sdProgram}); err != nil || result.Added != 1 {
		t.Fatalf("unchanged-MD5 SD restore = %+v err %v", result, err)
	}
	var title, hash string
	if err := db.Pool.QueryRow(ctx, `
		SELECT title, source_hash FROM epg_program
		 WHERE channel_id=$1 AND start_at=$2 AND epg_source_id IS NULL`, channel.ID, start,
	).Scan(&title, &hash); err != nil {
		t.Fatal(err)
	}
	if title != sdProgram.Title || hash != sdProgram.SourceHash {
		t.Fatalf("restored direct candidate = %q/%q", title, hash)
	}
	if md5, err := db.GetSDStationMD5(ctx, lineupID, stationID); err != nil || md5 != stationMD5 {
		t.Fatalf("restored SD watermark = %q err %v", md5, err)
	}
}

func TestIntegrationDifferentSDStationDisplacementInvalidatesPriorWatermark(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	channel, err := db.CreateChannel(ctx, store.Channel{
		Number: 9475.03, Name: "Competing SD stations", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	unrelatedChannel, err := db.CreateChannel(ctx, store.Channel{
		Number: 9475.13, Name: "Unrelated competing SD station", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now().UTC().Truncate(time.Minute).Add(36 * time.Hour)
	const (
		lineupA          = "fx475-sd-displacement-a"
		lineupB          = "fx475-sd-displacement-b"
		unrelatedLineup  = "fx475-sd-displacement-unrelated"
		stationA         = "fx475-sd-station-a"
		stationB         = "fx475-sd-station-b"
		unrelatedStation = "fx475-sd-station-unrelated"
		md5A             = "station-a-unchanged-md5"
		md5B             = "station-b-md5"
		unrelatedMD5     = "station-unrelated-md5"
	)
	for _, lineup := range []string{lineupA, lineupB, unrelatedLineup} {
		if _, err := db.Pool.Exec(ctx, `
			INSERT INTO sd_lineup (sd_lineup_id, name) VALUES ($1, $1)`, lineup); err != nil {
			t.Fatal(err)
		}
	}
	programA := store.EPGProgram{
		ChannelID: channel.ID, StartAt: start, EndAt: start.Add(time.Hour),
		Title: "Station A programme", SourceHash: "sd:station-a", SourcePriority: store.PrioritySD,
	}
	programB := programA
	programB.StartAt = start.Add(2 * time.Hour)
	programB.EndAt = programB.StartAt.Add(time.Hour)
	programB.Title = "Station B programme"
	programB.SourceHash = "sd:station-b"
	unrelatedProgram := programA
	unrelatedProgram.ChannelID = unrelatedChannel.ID
	unrelatedProgram.Title = "Unrelated station programme"
	unrelatedProgram.SourceHash = "sd:station-unrelated"
	if _, err := db.ReplaceSDStationSnapshot(ctx, unrelatedLineup, unrelatedStation, unrelatedMD5,
		unrelatedChannel.ID, []store.EPGProgram{unrelatedProgram}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ReplaceSDStationSnapshot(ctx, lineupA, stationA, md5A, channel.ID, []store.EPGProgram{programA}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ReplaceSDStationSnapshot(ctx, lineupB, stationB, md5B, channel.ID, []store.EPGProgram{programB}); err != nil {
		t.Fatal(err)
	}
	if got, err := db.GetSDStationMD5(ctx, lineupA, stationA); err != nil || got != "" {
		t.Fatalf("displaced station A watermark = %q err %v; want empty", got, err)
	}
	if got, err := db.GetSDStationMD5(ctx, lineupB, stationB); err != nil || got != md5B {
		t.Fatalf("durable station B watermark = %q err %v", got, err)
	}
	if got, err := db.GetSDStationMD5(ctx, unrelatedLineup, unrelatedStation); err != nil || got != unrelatedMD5 {
		t.Fatalf("unrelated station watermark = %q err %v; want %q", got, err, unrelatedMD5)
	}

	// Once B withdraws, A's provider can report the exact same MD5 and still
	// restore because displacement cleared A's skip watermark.
	if _, err := db.ReplaceSDStationSnapshot(ctx, lineupB, stationB, "station-b-empty", channel.ID, nil); err != nil {
		t.Fatal(err)
	}
	if got, err := db.GetSDStationMD5(ctx, lineupB, stationB); err != nil || got != "station-b-empty" {
		t.Fatalf("authoritative empty station B watermark = %q err %v", got, err)
	}
	if result, err := db.ReplaceSDStationSnapshot(ctx, lineupA, stationA, md5A, channel.ID, []store.EPGProgram{programA}); err != nil || result.Added != 1 {
		t.Fatalf("station A restore after B withdrawal = %+v err %v", result, err)
	}
	var title, sourceHash string
	if err := db.Pool.QueryRow(ctx, `
		SELECT title, source_hash FROM epg_program
		 WHERE channel_id=$1 AND start_at=$2 AND epg_source_id IS NULL`, channel.ID, start,
	).Scan(&title, &sourceHash); err != nil {
		t.Fatal(err)
	}
	if title != programA.Title || sourceHash != programA.SourceHash {
		t.Fatalf("restored station A candidate = %q/%q", title, sourceHash)
	}

	// An authoritative empty snapshot can also displace a different station's
	// disjoint row. The deleted station must retry even though no same-start
	// upsert occurred to trigger the ordinary replacement invalidation path.
	if _, err := db.ReplaceSDStationSnapshot(ctx, lineupB, stationB, "station-b-empty-displaces-a", channel.ID, nil); err != nil {
		t.Fatal(err)
	}
	if got, err := db.GetSDStationMD5(ctx, lineupA, stationA); err != nil || got != "" {
		t.Fatalf("station A watermark after empty B displacement = %q err %v; want empty", got, err)
	}
	if got, err := db.GetSDStationMD5(ctx, lineupB, stationB); err != nil || got != "station-b-empty-displaces-a" {
		t.Fatalf("durable empty station B watermark = %q err %v", got, err)
	}
	if got, err := db.GetSDStationMD5(ctx, unrelatedLineup, unrelatedStation); err != nil || got != unrelatedMD5 {
		t.Fatalf("unrelated station watermark after channel cleanup = %q err %v; want %q", got, err, unrelatedMD5)
	}
}

func TestIntegrationSDStationSnapshotCorrectsAndWithdrawsCurrentlyAiringRow(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	channel, err := db.CreateChannel(ctx, store.Channel{
		Number: 9475.04, Name: "Current SD correction", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	const (
		lineupID  = "fx475-current-correction-lineup"
		stationID = "fx475-current-correction-station"
	)
	if _, err := db.Pool.Exec(ctx, `
		INSERT INTO sd_lineup (sd_lineup_id, name) VALUES ($1, $1)`, lineupID); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Minute)
	oldStart := now.Add(-30 * time.Minute)
	oldProgram := store.EPGProgram{
		ChannelID: channel.ID, StartAt: oldStart, EndAt: now.Add(30 * time.Minute),
		Title: "Stale current airing", SourceHash: "sd:current-old", SourcePriority: store.PrioritySD,
	}
	if _, err := db.ReplaceSDStationSnapshot(ctx, lineupID, stationID, "current-old-md5", channel.ID, []store.EPGProgram{oldProgram}); err != nil {
		t.Fatal(err)
	}

	corrected := oldProgram
	corrected.StartAt = now.Add(-10 * time.Minute)
	corrected.EndAt = now.Add(50 * time.Minute)
	corrected.Title = "Corrected current airing"
	corrected.SourceHash = "sd:current-corrected"
	result, err := db.ReplaceSDStationSnapshot(ctx, lineupID, stationID, "current-corrected-md5", channel.ID, []store.EPGProgram{corrected})
	if err != nil || result.Added != 1 || result.Deleted != 1 {
		t.Fatalf("current correction = %+v err %v", result, err)
	}
	var oldRows, correctedRows int
	if err := db.Pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE start_at=$2),
		       count(*) FILTER (WHERE start_at=$3 AND source_hash=$4)
		  FROM epg_program WHERE channel_id=$1`, channel.ID, oldStart, corrected.StartAt, corrected.SourceHash,
	).Scan(&oldRows, &correctedRows); err != nil {
		t.Fatal(err)
	}
	if oldRows != 0 || correctedRows != 1 {
		t.Fatalf("current correction rows = old %d corrected %d", oldRows, correctedRows)
	}

	result, err = db.ReplaceSDStationSnapshot(ctx, lineupID, stationID, "current-withdrawn-md5", channel.ID, nil)
	if err != nil || result.Deleted != 1 {
		t.Fatalf("current withdrawal = %+v err %v", result, err)
	}
	var remaining int
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM epg_program WHERE channel_id=$1`, channel.ID).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 0 {
		t.Fatalf("current withdrawal retained %d rows", remaining)
	}
}

func TestIntegrationMovieYearCorrectionRequeuesEnrichment(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	channel, err := db.CreateChannel(ctx, store.Channel{
		Number: 9871.5, Name: "Movie year correction", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now().UTC().Truncate(time.Minute).Add(48 * time.Hour)
	year1984 := time.Date(1984, time.December, 14, 0, 0, 0, 0, time.UTC)
	program := store.EPGProgram{
		ChannelID: channel.ID, StartAt: start, EndAt: start.Add(2 * time.Hour),
		Title: "Dune", IsMovie: true, OriginalAirDate: &year1984,
		SourceHash: "movie:dune:1984", SourcePriority: 1,
	}
	if action, err := db.UpsertEPGProgram(ctx, program); err != nil || action != "added" {
		t.Fatalf("seed movie = action %q err %v", action, err)
	}
	pending := storePendingForChannel(t, db, channel.ID)
	if err := db.UpsertEnrichmentResultGuarded(ctx, pending.ID, pending.EnrichmentInputKey, store.EnrichmentResult{TMDbID: 841984}); err != nil {
		t.Fatal(err)
	}
	year2021 := time.Date(2021, time.October, 22, 0, 0, 0, 0, time.UTC)
	program.OriginalAirDate = &year2021
	program.SourceHash = "movie:dune:2021"
	if action, err := db.UpsertEPGProgram(ctx, program); err != nil || action != "updated" {
		t.Fatalf("correct movie year = action %q err %v", action, err)
	}
	corrected := storePendingForChannel(t, db, channel.ID)
	if corrected.EnrichmentInputKey == pending.EnrichmentInputKey {
		t.Fatal("movie year correction retained stale enrichment input key")
	}
	var status string
	var retryReady bool
	if err := db.Pool.QueryRow(ctx, `
		SELECT enrichment_status::text, enrichment_retry_at='-infinity'::timestamptz
		  FROM epg_program WHERE id=$1`, pending.ID).Scan(&status, &retryReady); err != nil {
		t.Fatal(err)
	}
	if status != "pending" || !retryReady {
		t.Fatalf("movie year correction = status %q retry_ready %v; want pending/true", status, retryReady)
	}
	if err := db.UpsertEnrichmentResultGuarded(ctx, pending.ID, pending.EnrichmentInputKey, store.EnrichmentResult{TMDbID: 841984}); err != nil {
		t.Fatal(err)
	}
	if err := db.Pool.QueryRow(ctx, `SELECT enrichment_status::text FROM epg_program WHERE id=$1`, pending.ID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "pending" {
		t.Fatalf("stale movie-year result changed status to %q", status)
	}
}

func TestIntegrationNonEnrichmentMetadataRefreshAcceptsInFlightResult(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	channel, err := db.CreateChannel(ctx, store.Channel{
		Number: 9871.6, Name: "Metadata churn", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now().UTC().Truncate(time.Minute).Add(48 * time.Hour)
	program := store.EPGProgram{
		ChannelID: channel.ID, StartAt: start, EndAt: start.Add(time.Hour),
		Title: "Stable lookup title", SubTitle: "Original subtitle",
		Description: "Original description", EpisodeNumOnscreen: "S02E03",
		Category: []string{"Comedy"}, Rating: "TV-14",
		SourcePosterURL: "https://example.test/old.jpg",
		SourceHash:      "metadata-churn:v1", SourcePriority: 1,
	}
	if action, err := db.UpsertEPGProgram(ctx, program); err != nil || action != "added" {
		t.Fatalf("seed programme = action %q err %v", action, err)
	}
	pending := storePendingForChannel(t, db, channel.ID)

	// These fields change the provider content hash but are not inputs to an
	// enrichment lookup. A valid in-flight result must survive such churn.
	program.EndAt = start.Add(90 * time.Minute)
	program.SubTitle = "Corrected subtitle"
	program.Description = "Corrected description"
	program.Category = []string{"Comedy", "Series"}
	program.Rating = "TV-MA"
	program.SourcePosterURL = "https://example.test/new.jpg"
	program.SourceHash = "metadata-churn:v2"
	if action, err := db.UpsertEPGProgram(ctx, program); err != nil || action != "updated" {
		t.Fatalf("refresh non-enrichment metadata = action %q err %v", action, err)
	}
	refreshed := storePendingForChannel(t, db, channel.ID)
	if refreshed.EnrichmentInputKey != pending.EnrichmentInputKey {
		t.Fatalf("non-enrichment refresh changed input key: before %q after %q",
			pending.EnrichmentInputKey, refreshed.EnrichmentInputKey)
	}
	if err := db.UpsertEnrichmentResultGuarded(ctx, pending.ID, pending.EnrichmentInputKey,
		store.EnrichmentResult{TMDbID: 475123}); err != nil {
		t.Fatal(err)
	}
	var status string
	var tmdbID int
	if err := db.Pool.QueryRow(ctx, `
		SELECT p.enrichment_status::text, COALESCE(e.tmdb_id, 0)
		  FROM epg_program p
		  LEFT JOIN epg_program_enrichment e ON e.program_id=p.id
		 WHERE p.id=$1`, pending.ID).Scan(&status, &tmdbID); err != nil {
		t.Fatal(err)
	}
	if status != "matched" || tmdbID != 475123 {
		t.Fatalf("in-flight result after metadata refresh = status %q tmdb %d; want matched/475123", status, tmdbID)
	}
}

func storePendingForChannel(t *testing.T, db *store.DB, channelID uuid.UUID) store.PendingEnrichmentRow {
	t.Helper()
	rows, err := db.ListPendingEnrichment(context.Background(), 500)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.ChannelID == channelID {
			return row
		}
	}
	t.Fatalf("pending enrichment for channel %s not found", channelID)
	return store.PendingEnrichmentRow{}
}

// TestIntegrationEPGDirectEpisodeFieldsFollowSlotOwner verifies a weaker
// direct writer cannot leave an unwithdrawable field-level donation inside the
// stronger owner's collapsed row. XMLTV/SD supplementation is covered by the
// retained independent candidates in snapshot_integration_test.go.
func TestIntegrationEPGDirectEpisodeFieldsFollowSlotOwner(t *testing.T) {
	skipIfNoIntegration(t)
	dsn := startTestPostgres(t)
	ctx := context.Background()

	db, err := store.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	ch, err := db.CreateChannel(ctx, store.Channel{Number: 902, Name: "Merge Test", Enabled: true})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	start := time.Date(2026, 6, 11, 20, 0, 0, 0, time.UTC)

	readEp := func() string {
		var se string
		if err := db.Pool.QueryRow(ctx,
			`SELECT episode_num_onscreen FROM epg_program WHERE channel_id=$1 AND start_at=$2`,
			ch.ID, start).Scan(&se); err != nil {
			t.Fatalf("read episode: %v", err)
		}
		return se
	}

	// Strong XMLTV-style source owns the slot but carries no episode number.
	strong := store.EPGProgram{
		ChannelID: ch.ID, StartAt: start, EndAt: start.Add(time.Hour),
		Title: "The Office", Description: "tbs airing",
		SourceHash: "xmltv-a", SourcePriority: 3,
	}
	if action, err := db.UpsertEPGProgram(ctx, strong); err != nil || action != "added" {
		t.Fatalf("strong insert: action=%q err=%v", action, err)
	}
	if se := readEp(); se != "" {
		t.Fatalf("expected empty episode, got %q", se)
	}

	// A weaker direct source cannot backfill the collapsed slot: there would be
	// no durable donor row from which to recompute a correction or withdrawal.
	weakSD := store.EPGProgram{
		ChannelID: ch.ID, StartAt: start, EndAt: start.Add(time.Hour),
		Title: "The Office (SD)", EpisodeNumOnscreen: "S06E18", EpisodeNumXMLTV: "5.17.",
		SourceHash: "sd-a", SourcePriority: store.PrioritySD, // 10, weaker than 3
	}
	if action, err := db.UpsertEPGProgram(ctx, weakSD); err != nil || action != "unchanged" {
		t.Fatalf("weak donation: action=%q err=%v", action, err)
	}
	assertSlotState(t, db, ch, start, "The Office", 3, 1)
	if se := readEp(); se != "" {
		t.Fatalf("weaker direct donation persisted as %q", se)
	}

	// The owning direct source can publish and later withdraw its own episode
	// identity exactly.
	strong2 := strong
	strong2.EpisodeNumOnscreen = "S06E18"
	strong2.EpisodeNumXMLTV = "5.17."
	strong2.Description = "tbs airing v2"
	strong2.SourceHash = "xmltv-b"
	if action, err := db.UpsertEPGProgram(ctx, strong2); err != nil || action != "updated" {
		t.Fatalf("strong refresh: action=%q err=%v", action, err)
	}
	assertSlotState(t, db, ch, start, "The Office", 3, 1)
	if se := readEp(); se != "S06E18" {
		t.Fatalf("owner episode publish: got %q, want S06E18", se)
	}
	strong3 := strong2
	strong3.EpisodeNumOnscreen = ""
	strong3.EpisodeNumXMLTV = ""
	strong3.SourceHash = "xmltv-c"
	if action, err := db.UpsertEPGProgram(ctx, strong3); err != nil || action != "updated" {
		t.Fatalf("owner episode withdrawal: action=%q err=%v", action, err)
	}
	if se := readEp(); se != "" {
		t.Fatalf("owner episode withdrawal retained %q", se)
	}
}

func TestIntegrationDeletePPVOffRowsIsChannelWide(t *testing.T) {
	skipIfNoIntegration(t)
	dsn := startTestPostgres(t)
	ctx := context.Background()

	db, err := store.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	ch, err := db.CreateChannel(ctx, store.Channel{Number: 903, Name: "PPV Cleanup", Enabled: true})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	other, err := db.CreateChannel(ctx, store.Channel{Number: 904, Name: "Other PPV", Enabled: true})
	if err != nil {
		t.Fatalf("create other channel: %v", err)
	}

	gridStart := time.Date(2026, 7, 23, 18, 0, 0, 0, time.UTC)
	eventStart := gridStart.Add(2 * time.Hour)
	eventEnd := gridStart.Add(5 * time.Hour)
	rows := []store.EPGProgram{
		{
			ChannelID: ch.ID, StartAt: gridStart.Add(-6 * time.Hour), EndAt: gridStart,
			Title: "stale", SourceHash: "ppv-off:xtream:old:stale", SourcePriority: store.PriorityPPVOff,
		},
		{
			ChannelID: ch.ID, StartAt: gridStart, EndAt: gridStart.Add(6 * time.Hour),
			Title: "overlap from another stream", SourceHash: "ppv-off:xtream:999:overlap", SourcePriority: store.PriorityPPVOff,
		},
		{
			ChannelID: ch.ID, StartAt: gridStart.Add(6 * time.Hour), EndAt: gridStart.Add(12 * time.Hour),
			Title: "keep", SourceHash: "ppv-off:xtream:999:keep", SourcePriority: store.PriorityPPVOff,
		},
		{
			ChannelID: other.ID, StartAt: gridStart, EndAt: gridStart.Add(6 * time.Hour),
			Title: "other channel", SourceHash: "ppv-off:xtream:999:other", SourcePriority: store.PriorityPPVOff,
		},
	}
	for _, row := range rows {
		if action, err := db.UpsertEPGProgram(ctx, row); err != nil || action != "added" {
			t.Fatalf("seed %q: action=%q err=%v", row.Title, action, err)
		}
	}

	deleted, err := db.DeletePPVOffRows(ctx, ch.ID, gridStart, &eventStart, &eventEnd)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 2 {
		t.Fatalf("deleted %d rows, want stale + overlapping rows", deleted)
	}

	countHash := func(hash string) int {
		t.Helper()
		var n int
		if err := db.Pool.QueryRow(ctx,
			`SELECT count(*) FROM epg_program WHERE source_hash=$1`, hash,
		).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if countHash("ppv-off:xtream:old:stale") != 0 ||
		countHash("ppv-off:xtream:999:overlap") != 0 {
		t.Fatal("stale or cross-stream overlapping row survived cleanup")
	}
	if countHash("ppv-off:xtream:999:keep") != 1 ||
		countHash("ppv-off:xtream:999:other") != 1 {
		t.Fatal("cleanup removed a non-overlapping or other-channel row")
	}
}

func TestIntegrationReplacePPVParsedRowsForChannelIsAtomicAndChannelWide(t *testing.T) {
	skipIfNoIntegration(t)
	dsn := startTestPostgres(t)
	ctx := context.Background()

	db, err := store.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	ch, err := db.CreateChannel(ctx, store.Channel{Number: 905, Name: "PPV Replace", Enabled: true})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	other, err := db.CreateChannel(ctx, store.Channel{Number: 906, Name: "Other Replace", Enabled: true})
	if err != nil {
		t.Fatalf("create other channel: %v", err)
	}

	base := time.Date(2026, 7, 23, 18, 0, 0, 0, time.UTC)
	seeds := []store.EPGProgram{
		{
			ChannelID: ch.ID, StartAt: base, EndAt: base.Add(2 * time.Hour),
			Title: "Old Primary", SourceHash: "ppv-parse:xtream:100:old",
			SourcePriority: store.PriorityPPVSync,
		},
		{
			ChannelID: ch.ID, StartAt: base.Add(8 * time.Hour), EndAt: base.Add(10 * time.Hour),
			Title: "Old Ambiguous Fallback", SourceHash: "ppv-parse:xtream:101:old",
			SourcePriority: store.PriorityPPVSync,
		},
		{
			ChannelID: ch.ID, StartAt: base.Add(12 * time.Hour), EndAt: base.Add(13 * time.Hour),
			Title: "Non PPV Row", SourceHash: "xmltv:keep", SourcePriority: 0,
		},
		{
			ChannelID: other.ID, StartAt: base, EndAt: base.Add(2 * time.Hour),
			Title: "Other Channel Event", SourceHash: "ppv-parse:xtream:999:keep",
			SourcePriority: store.PriorityPPVSync,
		},
	}
	for _, row := range seeds {
		if action, err := db.UpsertEPGProgram(ctx, row); err != nil || action != "added" {
			t.Fatalf("seed %q: action=%q err=%v", row.Title, action, err)
		}
	}

	selected := store.EPGProgram{
		ChannelID: ch.ID, StartAt: base.Add(3 * time.Hour), EndAt: base.Add(6 * time.Hour),
		Title: "Exact Winner", SourceHash: "ppv-parse:xtream:100:exact",
		SourcePriority: store.PriorityPPVSync,
	}
	observedAt := time.Now().UTC()
	action, deleted, applied, err := db.ReplacePPVParsedRowsForChannel(ctx, selected, observedAt)
	if err != nil || !applied || action != "added" || deleted != 2 {
		t.Fatalf("replace: action=%q deleted=%d applied=%v err=%v", action, deleted, applied, err)
	}

	var selectedRows, oldRows, nonPPVRows, otherRows int
	if err := db.Pool.QueryRow(ctx, `
		SELECT
			count(*) FILTER (WHERE channel_id=$1 AND source_hash=$2),
			count(*) FILTER (WHERE channel_id=$1 AND source_hash LIKE 'ppv-parse:%' AND source_hash<>$2),
			count(*) FILTER (WHERE channel_id=$1 AND source_hash='xmltv:keep'),
			count(*) FILTER (WHERE channel_id=$3 AND source_hash='ppv-parse:xtream:999:keep')
		  FROM epg_program`,
		ch.ID, selected.SourceHash, other.ID,
	).Scan(&selectedRows, &oldRows, &nonPPVRows, &otherRows); err != nil {
		t.Fatal(err)
	}
	if selectedRows != 1 || oldRows != 0 || nonPPVRows != 1 || otherRows != 1 {
		t.Fatalf(
			"replacement state selected=%d old=%d nonPPV=%d other=%d",
			selectedRows, oldRows, nonPPVRows, otherRows,
		)
	}
	if action, deleted, applied, err := db.ReplacePPVParsedRowsForChannel(ctx, selected, observedAt); err != nil || !applied || action != "unchanged" || deleted != 0 {
		t.Fatalf("steady-state replace: action=%q deleted=%d applied=%v err=%v", action, deleted, applied, err)
	}

	// A stronger row at the selected kickoff makes the replacement fail.
	// The transaction must roll back, preserving the prior PPV state.
	blockedStart := base.Add(16 * time.Hour)
	prior := store.EPGProgram{
		ChannelID: ch.ID, StartAt: base.Add(14 * time.Hour), EndAt: base.Add(15 * time.Hour),
		Title: "Prior PPV State", SourceHash: "ppv-parse:xtream:100:prior",
		SourcePriority: store.PriorityPPVSync,
	}
	blocker := store.EPGProgram{
		ChannelID: ch.ID, StartAt: blockedStart, EndAt: blockedStart.Add(time.Hour),
		Title: "Stronger Owner", SourceHash: "manual:stronger", SourcePriority: store.PriorityPPVSync - 1,
	}
	for _, row := range []store.EPGProgram{prior, blocker} {
		if action, err := db.UpsertEPGProgram(ctx, row); err != nil || action != "added" {
			t.Fatalf("seed rollback row %q: action=%q err=%v", row.Title, action, err)
		}
	}
	blocked := store.EPGProgram{
		ChannelID: ch.ID, StartAt: blockedStart, EndAt: blockedStart.Add(2 * time.Hour),
		Title: "Blocked Exact", SourceHash: "ppv-parse:xtream:100:blocked",
		SourcePriority: store.PriorityPPVSync,
	}
	if _, _, _, err := db.ReplacePPVParsedRowsForChannel(ctx, blocked, observedAt); err == nil {
		t.Fatal("expected stronger slot owner to reject PPV replacement")
	}
	var priorCount, blockerCount int
	if err := db.Pool.QueryRow(ctx, `
		SELECT
			count(*) FILTER (WHERE source_hash='ppv-parse:xtream:100:prior'),
			count(*) FILTER (WHERE source_hash='manual:stronger')
		  FROM epg_program
		 WHERE channel_id=$1`,
		ch.ID,
	).Scan(&priorCount, &blockerCount); err != nil {
		t.Fatal(err)
	}
	if priorCount != 1 || blockerCount != 1 {
		t.Fatalf("failed replacement was not atomic: prior=%d blocker=%d", priorCount, blockerCount)
	}
}

// assertSlotState checks the surviving row count for the slot plus the
// winning title and priority.
func assertSlotState(t *testing.T, db *store.DB, ch store.Channel, start time.Time, wantTitle string, wantPriority, wantRows int) {
	t.Helper()
	ctx := context.Background()
	var n int
	if err := db.Pool.QueryRow(ctx,
		`SELECT count(*) FROM epg_program WHERE channel_id = $1 AND start_at = $2`,
		ch.ID, start).Scan(&n); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if n != wantRows {
		t.Fatalf("slot rows = %d, want %d", n, wantRows)
	}
	var title string
	var prio int
	if err := db.Pool.QueryRow(ctx,
		`SELECT title, source_priority FROM epg_program WHERE channel_id = $1 AND start_at = $2`,
		ch.ID, start).Scan(&title, &prio); err != nil {
		t.Fatalf("read slot: %v", err)
	}
	if title != wantTitle || prio != wantPriority {
		t.Fatalf("slot = (%q, prio %d), want (%q, prio %d)", title, prio, wantTitle, wantPriority)
	}
}
