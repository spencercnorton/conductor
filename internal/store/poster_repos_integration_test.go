package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/store"
)

func TestIntegrationPosterBindingSourceHashAndPrecedence(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	_, programID, _ := seedThreeProgramsAcrossWindow(t, db)

	const generated = "/posters/assets/generated/sha256/aa/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.png"
	if _, err := db.ApplyPosterToProgram(ctx, programID, "stale-hash", generated, false); err != nil {
		t.Fatal(err)
	}
	var status, generatedURL string
	if err := db.Pool.QueryRow(ctx,
		`SELECT enrichment_status::text, generated_poster_url FROM epg_program WHERE id=$1`, programID,
	).Scan(&status, &generatedURL); err != nil {
		t.Fatal(err)
	}
	if status != "pending" || generatedURL != "" {
		t.Fatalf("stale generated binding changed row: status=%s url=%q", status, generatedURL)
	}

	if _, err := db.ApplyPosterToProgram(ctx, programID, "win-future", generated, false); err != nil {
		t.Fatal(err)
	}
	if err := db.Pool.QueryRow(ctx,
		`SELECT enrichment_status::text, generated_poster_url FROM epg_program WHERE id=$1`, programID,
	).Scan(&status, &generatedURL); err != nil {
		t.Fatal(err)
	}
	if status != "pending" || generatedURL != generated {
		t.Fatalf("generated fallback masqueraded as enrichment: status=%s url=%q", status, generatedURL)
	}
	var channelID uuid.UUID
	var startAt, endAt time.Time
	if err := db.Pool.QueryRow(ctx,
		`SELECT channel_id, start_at, end_at FROM epg_program WHERE id=$1`, programID,
	).Scan(&channelID, &startAt, &endAt); err != nil {
		t.Fatal(err)
	}
	if action, err := db.UpsertEPGProgram(ctx, store.EPGProgram{
		ChannelID: channelID, StartAt: startAt, EndAt: endAt,
		Title: "Replacement matchup", Category: []string{"Sports"},
		SourceHash: "replacement-hash",
	}); err != nil || action != "updated" {
		t.Fatalf("replace program: action=%q err=%v", action, err)
	}
	if err := db.Pool.QueryRow(ctx,
		`SELECT enrichment_status::text, generated_poster_url FROM epg_program WHERE id=$1`, programID,
	).Scan(&status, &generatedURL); err != nil {
		t.Fatal(err)
	}
	if status != "pending" || generatedURL != "" {
		t.Fatalf("replacement retained stale generated art: status=%s url=%q", status, generatedURL)
	}

	const researched = "/posters/assets/researched/sha256/bb/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.jpg"
	if _, err := db.ApplyPosterToProgram(ctx, programID, "win-future", researched, true); err != nil {
		t.Fatal(err)
	}
	var staleCount int
	if err := db.Pool.QueryRow(ctx,
		`SELECT count(*) FROM epg_program_enrichment WHERE program_id=$1`, programID,
	).Scan(&staleCount); err != nil || staleCount != 0 {
		t.Fatalf("stale researched override persisted: count=%d err=%v", staleCount, err)
	}
	if _, err := db.ApplyPosterToProgram(ctx, programID, "replacement-hash", researched, true); err != nil {
		t.Fatal(err)
	}
	var enrichedURL string
	if err := db.Pool.QueryRow(ctx, `
		SELECT p.enrichment_status::text, p.generated_poster_url, e.poster_url
		  FROM epg_program p JOIN epg_program_enrichment e ON e.program_id=p.id
		 WHERE p.id=$1`, programID).Scan(&status, &generatedURL, &enrichedURL); err != nil {
		t.Fatal(err)
	}
	if status != "manual" || generatedURL != "" || enrichedURL != researched {
		t.Fatalf("researched override precedence failed: status=%s generated=%q enriched=%q", status, generatedURL, enrichedURL)
	}

	// A source identity change resets the program to pending but deliberately
	// retains the enrichment row. Reapplying the same researched asset must
	// restore manual status rather than treating the matching URL as a no-op.
	if _, err := db.Pool.Exec(ctx, `
		UPDATE epg_program
		   SET source_hash = 'replacement-hash-2', enrichment_status = 'pending',
		       generated_poster_url = $2
		 WHERE id = $1`, programID, generated); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ApplyPosterToProgram(ctx, programID, "replacement-hash-2", researched, true); err != nil {
		t.Fatal(err)
	}
	if err := db.Pool.QueryRow(ctx, `
		SELECT p.enrichment_status::text, p.generated_poster_url, e.poster_url
		  FROM epg_program p JOIN epg_program_enrichment e ON e.program_id=p.id
		 WHERE p.id=$1`, programID).Scan(&status, &generatedURL, &enrichedURL); err != nil {
		t.Fatal(err)
	}
	if status != "manual" || generatedURL != "" || enrichedURL != researched {
		t.Fatalf("same researched override was not restored: status=%s generated=%q enriched=%q", status, generatedURL, enrichedURL)
	}
	var updatedAt time.Time
	if err := db.Pool.QueryRow(ctx, `SELECT updated_at FROM epg_program WHERE id=$1`, programID).Scan(&updatedAt); err != nil {
		t.Fatal(err)
	}
	changed, err := db.ApplyPosterToProgram(ctx, programID, "replacement-hash-2", researched, true)
	if err != nil {
		t.Fatal(err)
	}
	if changed {
		t.Fatal("idempotent researched reapply reported a mutation")
	}
	var updatedAgain time.Time
	if err := db.Pool.QueryRow(ctx, `SELECT updated_at FROM epg_program WHERE id=$1`, programID).Scan(&updatedAgain); err != nil {
		t.Fatal(err)
	}
	if !updatedAgain.Equal(updatedAt) {
		t.Fatalf("idempotent researched reapply mutated updated_at: %s -> %s", updatedAt, updatedAgain)
	}
}

func TestIntegrationStaleEnrichmentCannotMutateReplacement(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	_, programID, _ := seedThreeProgramsAcrossWindow(t, db)

	if err := db.UpsertEnrichmentResultGuarded(ctx, programID, "stale-hash", store.EnrichmentResult{
		TMDbID: 42, PosterURL: "https://image.example/stale.jpg",
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.MarkEnrichmentFailedGuarded(ctx, programID, "stale-hash"); err != nil {
		t.Fatal(err)
	}
	if err := db.DeferEnrichmentGuarded(ctx, programID, "stale-hash", time.Hour); err != nil {
		t.Fatal(err)
	}

	var status string
	var retryUntouched bool
	if err := db.Pool.QueryRow(ctx,
		`SELECT enrichment_status::text, enrichment_retry_at = '-infinity'::timestamptz FROM epg_program WHERE id=$1`, programID,
	).Scan(&status, &retryUntouched); err != nil {
		t.Fatal(err)
	}
	if status != "pending" || !retryUntouched {
		t.Fatalf("stale enrichment mutated replacement: status=%s retry_untouched=%v", status, retryUntouched)
	}
	var count int
	if err := db.Pool.QueryRow(ctx,
		`SELECT count(*) FROM epg_program_enrichment WHERE program_id=$1`, programID,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("stale enrichment row persisted: count=%d", count)
	}
}
