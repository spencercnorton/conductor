// metrics_repos_integration_test.go — sanity for the /metrics gauge queries
// (audit Q1). Shares the pg bootstrap in lease_integration_test.go.
package store_test

import (
	"context"
	"testing"

	"github.com/spencercnorton/conductor/internal/store"
)

func TestIntegrationMetricsQueries(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	resolver := store.PassthroughResolverForTest{}

	// Empty database: everything answers zero/empty without erroring.
	if n, err := db.CountActiveStreams(ctx); err != nil || n != 0 {
		t.Fatalf("CountActiveStreams empty = %d, %v", n, err)
	}
	if d, err := db.EPGHorizonDays(ctx); err != nil || d != 0 {
		t.Fatalf("EPGHorizonDays empty = %f, %v", d, err)
	}
	if n, err := db.CountEPGPrograms(ctx); err != nil || n != 0 {
		t.Fatalf("CountEPGPrograms empty = %d, %v", n, err)
	}
	if rows, err := db.ListEPGSourceFreshness(ctx); err != nil || len(rows) != 0 {
		t.Fatalf("ListEPGSourceFreshness empty = %v, %v", rows, err)
	}

	// One leased stream shows up in the active count; capacity reflects the
	// seeded credential.
	provID, _ := seedProviderWithCredentials(t, db, 1, 2)
	ch, _ := seedChannelWithSources(t, db, provID, "http://m.test/stream")
	if _, err := db.AcquireLease(ctx, ch.ID, resolver); err != nil {
		t.Fatal(err)
	}
	if n, err := db.CountActiveStreams(ctx); err != nil || n != 1 {
		t.Fatalf("CountActiveStreams = %d, %v (want 1)", n, err)
	}
	if n, err := db.CountTuners(ctx); err != nil || n != 2 {
		t.Fatalf("CountTuners = %d, %v (want 2)", n, err)
	}

	// Future programmes move the horizon; sources report freshness rows.
	if _, err := db.Pool.Exec(ctx, `
		INSERT INTO epg_source (name, url, enabled, last_ok_at)
		VALUES ('m-src', 'http://m.test/xmltv', true, now() - interval '90 seconds')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool.Exec(ctx, `
		INSERT INTO epg_program (channel_id, start_at, end_at, title, source_hash)
		VALUES ($1, now() + interval '2 days', now() + interval '2 days 1 hour', 'future show', 'h1')`,
		ch.ID); err != nil {
		t.Fatal(err)
	}
	if d, err := db.EPGHorizonDays(ctx); err != nil || d < 1.9 || d > 2.1 {
		t.Fatalf("EPGHorizonDays = %f, %v (want ~2)", d, err)
	}
	if n, err := db.CountEPGPrograms(ctx); err != nil || n != 1 {
		t.Fatalf("CountEPGPrograms = %d, %v (want 1)", n, err)
	}
	rows, err := db.ListEPGSourceFreshness(ctx)
	if err != nil || len(rows) != 1 || rows[0].Name != "m-src" || rows[0].LastOKAt == nil {
		t.Fatalf("ListEPGSourceFreshness = %+v, %v", rows, err)
	}
}
