// upsert_enrichment_integration_test.go — verifies the jsonb columns
// (cast_json, sports_event_json) on epg_program_enrichment accept the
// upsert path without the SQLSTATE 22P02 "invalid input syntax for type
// json" error that blocked all poster persistence prior to 2026-05-07.
package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/store"
)

// seedProgramRow drops a minimal program row in via raw SQL — bypasses
// the higher-level orchestration helpers so the test stays focused on the
// enrichment-persist path.
func seedProgramRow(t *testing.T, db *store.DB) uuid.UUID {
	t.Helper()
	ctx := context.Background()

	if _, err := db.CreateProvider(ctx, store.Provider{
		Name: "test-prov", Kind: "m3u_xtream", BaseURL: "http://x.test", Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	ch, err := db.CreateChannel(ctx, store.Channel{
		Number: 100, Name: "Test Channel", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	_, err = db.UpsertEPGProgram(ctx, store.EPGProgram{
		ChannelID: ch.ID,
		StartAt:   now, EndAt: now.Add(30 * time.Minute),
		Title:      "Test Title",
		Category:   []string{"Series"},
		SourceHash: "hash-test-jsonb-fix-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	// Look up the inserted row's id.
	var pid uuid.UUID
	row := db.Pool.QueryRow(ctx,
		`SELECT id FROM epg_program WHERE source_hash=$1 LIMIT 1`,
		"hash-test-jsonb-fix-1")
	if err := row.Scan(&pid); err != nil {
		t.Fatalf("look up program id: %v", err)
	}
	return pid
}

// TestIntegrationUpsertEnrichmentResult_PersistsValidJsonbNull is the
// regression test for the bug fixed 2026-05-07: the UpsertEnrichmentResult
// SQL writes empty []byte to two jsonb columns, which Postgres rejects
// with SQLSTATE 22P02. With the fix (initialize to []byte("null"), the
// SQL's NULLIF strips it back to a real SQL NULL), the upsert succeeds.
func TestIntegrationUpsertEnrichmentResult_PersistsValidJsonbNull(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	pid := seedProgramRow(t, db)

	if err := db.UpsertEnrichmentResult(ctx, pid, store.EnrichmentResult{
		TMDbID:        12345,
		IMDbID:        "tt0000001",
		PosterURL:     "https://example.test/poster.jpg",
		BackdropURL:   "https://example.test/backdrop.jpg",
		Overview:      "Plot summary.",
		Confidence:    0.95,
		MatchedSource: "tmdb",
	}); err != nil {
		t.Fatalf("upsert returned error: %v", err)
	}

	// Read the row back from the table directly.
	var (
		gotTMDb       int
		gotIMDb       string
		gotPoster     string
		gotBackdrop   string
		gotOverview   string
		gotConfidence float64
	)
	row := db.Pool.QueryRow(ctx,
		`SELECT tmdb_id, imdb_id, poster_url, backdrop_url, overview, match_confidence
		   FROM epg_program_enrichment WHERE program_id = $1`, pid)
	if err := row.Scan(&gotTMDb, &gotIMDb, &gotPoster, &gotBackdrop, &gotOverview, &gotConfidence); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if gotTMDb != 12345 || gotIMDb != "tt0000001" || gotPoster != "https://example.test/poster.jpg" {
		t.Errorf("row mismatch: tmdb=%d imdb=%q poster=%q", gotTMDb, gotIMDb, gotPoster)
	}
	if gotConfidence < 0.94 || gotConfidence > 0.96 {
		t.Errorf("match_confidence: got %v want ~0.95", gotConfidence)
	}

	// Confirm program's enrichment_status flipped to 'matched'.
	var status string
	row = db.Pool.QueryRow(ctx, `SELECT enrichment_status FROM epg_program WHERE id=$1`, pid)
	if err := row.Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "matched" {
		t.Errorf("enrichment_status: got %q want 'matched'", status)
	}
}

// Idempotency: second upsert exercises the ON CONFLICT (program_id) DO UPDATE
// path, which also has to handle the jsonb columns correctly.
func TestIntegrationUpsertEnrichmentResult_Idempotent(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	pid := seedProgramRow(t, db)

	if err := db.UpsertEnrichmentResult(ctx, pid, store.EnrichmentResult{
		TMDbID: 1, PosterURL: "http://a.test/1.jpg",
	}); err != nil {
		t.Fatalf("first upsert: %v", err)
	}
	if err := db.UpsertEnrichmentResult(ctx, pid, store.EnrichmentResult{
		TMDbID: 2, PosterURL: "http://a.test/2.jpg",
	}); err != nil {
		t.Fatalf("second upsert (ON CONFLICT path): %v", err)
	}
	// Final state matches the second write.
	var gotTMDb int
	var gotPoster string
	row := db.Pool.QueryRow(ctx,
		`SELECT tmdb_id, poster_url FROM epg_program_enrichment WHERE program_id = $1`, pid)
	if err := row.Scan(&gotTMDb, &gotPoster); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if gotTMDb != 2 || gotPoster != "http://a.test/2.jpg" {
		t.Errorf("idempotent overwrite mismatch: tmdb=%d poster=%q", gotTMDb, gotPoster)
	}
}
