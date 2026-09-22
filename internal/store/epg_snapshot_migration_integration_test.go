//go:build !short

package store_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/spencercnorton/conductor/internal/store"
)

// TestIntegrationEPGCanonicalMigrationFromOverlappingRows proves migration
// 0023 can upgrade the pre-snapshot schema even when production-like rows
// already overlap. It must preserve candidates, publish a deterministic
// overlap-free initial view, reset stale validators, and install the database
// invariant before Conductor starts.
func TestIntegrationEPGCanonicalMigrationFromOverlappingRows(t *testing.T) {
	skipIfNoIntegration(t)
	ctx := context.Background()
	dsn := startTestPostgres(t)
	schema := "fx475_" + strings.ReplaceAll(uuid.NewString(), "-", "")

	root, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := root.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		root.Close(ctx)
		t.Fatal(err)
	}
	root.Close(ctx)
	t.Cleanup(func() {
		conn, err := pgx.Connect(context.Background(), dsn)
		if err == nil {
			_, _ = conn.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
			_ = conn.Close(context.Background())
		}
	})

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
	var migration23 store.Migration
	var post23Migrations []store.Migration
	foundMigration23 := false
	for _, migration := range migrations {
		if foundMigration23 {
			post23Migrations = append(post23Migrations, migration)
			continue
		}
		if migration.Name == "0023_epg_canonical_snapshots.sql" {
			migration23 = migration
			foundMigration23 = true
			continue
		}
		if err := fx475ApplyMigration(ctx, pool, migration); err != nil {
			t.Fatalf("apply pre-0023 %s: %v", migration.Name, err)
		}
	}
	if migration23.Name == "" {
		t.Fatal("migration 0023 not found")
	}
	customProfileDescriptions := map[string]string{
		"nvenc-1080p":      "operator-owned 1080p migration notes",
		"nvenc-720p":       "operator-owned 720p migration notes",
		"hdr-to-sdr-nvenc": "operator-owned HDR migration notes",
	}
	for name, description := range customProfileDescriptions {
		result, err := pool.Exec(ctx, `
			UPDATE transcode_profile SET description=$2 WHERE name=$1`, name, description)
		if err != nil {
			t.Fatal(err)
		}
		if result.RowsAffected() != 1 {
			t.Fatalf("customize pre-0023 profile %s: rows=%d, want 1", name, result.RowsAffected())
		}
	}

	var channelID uuid.UUID
	if err := pool.QueryRow(ctx, `
		INSERT INTO channel (number, name, call_sign, epg_channel_id)
		VALUES (9475.5, 'FX475 migration', 'FX475M', 'fx475.migration')
		RETURNING id`).Scan(&channelID); err != nil {
		t.Fatal(err)
	}
	var numericChannelID, ambiguousExplicitChannelID uuid.UUID
	if err := pool.QueryRow(ctx, `
		INSERT INTO channel (number, name, call_sign, epg_channel_id)
		VALUES (9475.6, 'FX475 numeric migration', 'FX475N', '')
		RETURNING id`).Scan(&numericChannelID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO channel (number, name, call_sign, epg_channel_id)
		VALUES (9475.7, 'FX475 ambiguous migration', 'FX475A', '9475.5')
		RETURNING id`).Scan(&ambiguousExplicitChannelID); err != nil {
		t.Fatal(err)
	}
	var sourceID uuid.UUID
	if err := pool.QueryRow(ctx, `
		INSERT INTO epg_source (name, url, priority, enabled, last_etag, last_modified)
		VALUES ('old-feed', 'https://example.invalid/guide.xml', 0, true, '"old"', 'Fri, 21 Aug 2026 12:00:00 GMT')
		RETURNING id`).Scan(&sourceID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO sd_lineup (sd_lineup_id, name)
		VALUES ('FX475-LINEUP', 'FX475 migration'),
		       ('FX475-NUMERIC-LINEUP', 'FX475 numeric migration'),
		       ('FX475-AMBIGUOUS-LINEUP', 'FX475 ambiguous migration');
		INSERT INTO sd_station_state (sd_lineup_id, sd_station_id, last_md5)
		VALUES ('FX475-LINEUP', 'fx475.migration', 'pre-migration-md5'),
		       ('FX475-NUMERIC-LINEUP', '9475.6', 'pre-migration-numeric-md5'),
		       ('FX475-AMBIGUOUS-LINEUP', '9475.5', 'pre-migration-ambiguous-md5')`); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2099, time.August, 24, 0, 0, 0, 0, time.UTC)
	for i, row := range []struct {
		start    time.Time
		end      time.Time
		priority int
		hash     string
	}{
		{base, base.Add(time.Hour), 0, "legacy-hash-0"},
		{base.Add(5 * time.Minute), base.Add(65 * time.Minute), 10, "opaque-priority-10"},
		{base.Add(time.Hour), base.Add(90 * time.Minute), 10, "sd:explicit-provenance"},
		{base.Add(90 * time.Minute), base.Add(2 * time.Hour), 100, "legacy-hash-3"},
		{base.Add(30 * time.Minute), base.Add(30 * time.Minute), 0, "legacy-zero-duration"},
	} {
		if _, err := pool.Exec(ctx, `
			INSERT INTO epg_program (
				channel_id, start_at, end_at, title, source_hash, source_priority
			) VALUES ($1,$2,$3,$4,$5,$6)`, channelID, row.start, row.end,
			fmt.Sprintf("legacy-%d", i), row.hash, row.priority); err != nil {
			t.Fatal(err)
		}
	}
	for _, row := range []struct {
		channelID uuid.UUID
		hash      string
	}{
		{numericChannelID, "pre-migration-numeric-sd-hash"},
		{ambiguousExplicitChannelID, "pre-migration-ambiguous-sd-hash"},
	} {
		if _, err := pool.Exec(ctx, `
			INSERT INTO epg_program (
				channel_id, start_at, end_at, title, source_hash, source_priority
			) VALUES ($1,$2,$3,$4,$4,10)`, row.channelID, base, base.Add(time.Hour), row.hash); err != nil {
			t.Fatal(err)
		}
	}
	// Migration 0018's range exclusion already makes negative durations
	// structurally unrepresentable. Zero-duration ranges remain legal/empty and
	// are the retained-history edge that 0023 must keep noncanonical.
	if _, err := pool.Exec(ctx, `
		INSERT INTO epg_program (
			channel_id, start_at, end_at, title, source_hash, source_priority
		) VALUES ($1,$2,$3,'negative duration','legacy-negative-duration',0)`,
		channelID, base.Add(45*time.Minute), base.Add(30*time.Minute)); err == nil {
		t.Fatal("pre-0023 schema unexpectedly accepted a negative-duration row")
	}

	// Complete the existing seven rows to the audited production footprint
	// (50,783 rows / 189 channels / 1,489 rows on the largest channel) so the
	// migration's greedy overlap probes exercise their partial GiST index instead
	// of regressing to a retained-history table scan for every candidate.
	if _, err := pool.Exec(ctx, `
		INSERT INTO channel (number, name, call_sign, epg_channel_id)
		SELECT 20000 + n,
		       'FX475 migration scale ' || n,
		       'FX475S' || n,
		       'fx475.scale.' || n
		  FROM generate_series(1, 186) AS n`); err != nil {
		t.Fatal(err)
	}
	var scaleHeavyChannelID uuid.UUID
	if err := pool.QueryRow(ctx, `SELECT id FROM channel WHERE number=20001`).Scan(&scaleHeavyChannelID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		WITH numbered AS (
			SELECT id, row_number() OVER (ORDER BY number) AS channel_n
			  FROM channel
			 WHERE epg_channel_id LIKE 'fx475.scale.%'
		)
		INSERT INTO epg_program (
			channel_id, start_at, end_at, title, source_hash, source_priority
		)
		SELECT numbered.id,
		       $1::timestamptz + (slot_n - 1) * interval '30 minutes',
		       $1::timestamptz + (slot_n + 1) * interval '30 minutes',
		       'scale programme ' || numbered.channel_n || '/' || slot_n,
		       'scale:' || numbered.channel_n || ':' || slot_n,
		       0
		  FROM numbered
		 CROSS JOIN LATERAL generate_series(
			1,
			CASE
				WHEN numbered.channel_n = 1 THEN 1489
				WHEN numbered.channel_n <= 78 THEN 267
				ELSE 266
			END
		) AS slot_n`, base); err != nil {
		t.Fatal(err)
	}

	migrationStarted := time.Now()
	if err := fx475ApplyMigration(ctx, pool, migration23); err != nil {
		t.Fatalf("apply 0023 over overlap chain: %v", err)
	}
	migrationElapsed := time.Since(migrationStarted)
	t.Logf("migration 0023 canonicalized production-scale retained history in %s", migrationElapsed)
	var total, canonical, legacy, supplemented, overlapPairs, nonemptyValidators int
	if err := pool.QueryRow(ctx, `
		SELECT count(*), count(*) FILTER (WHERE is_canonical),
		       count(*) FILTER (WHERE is_legacy),
		       count(*) FILTER (WHERE episode_num_xmltv_supplemented OR episode_num_onscreen_supplemented)
		  FROM epg_program WHERE channel_id=$1`, channelID).Scan(&total, &canonical, &legacy, &supplemented); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		  FROM epg_program a
		  JOIN epg_program b ON a.channel_id=b.channel_id AND a.id < b.id
		 WHERE a.channel_id=$1 AND a.is_canonical AND b.is_canonical
		   AND a.start_at < b.end_at AND a.end_at > b.start_at`, channelID).Scan(&overlapPairs); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM epg_source
		 WHERE last_etag <> '' OR last_modified <> ''`).Scan(&nonemptyValidators); err != nil {
		t.Fatal(err)
	}
	if total != 5 || canonical != 3 || legacy != 4 || supplemented != 0 || overlapPairs != 0 || nonemptyValidators != 0 {
		t.Fatalf("post-migration state = total %d canonical %d legacy %d supplemented %d overlap pairs %d validators %d; want 5,3,4,0,0,0",
			total, canonical, legacy, supplemented, overlapPairs, nonemptyValidators)
	}
	var invalidCanonical int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		  FROM epg_program
		 WHERE source_hash = 'legacy-zero-duration'
		   AND is_canonical`).Scan(&invalidCanonical); err != nil {
		t.Fatal(err)
	}
	if invalidCanonical != 0 {
		t.Fatalf("invalid retained intervals made canonical: %d", invalidCanonical)
	}
	healthDB := &store.DB{Pool: pool}
	_, _, healthOverlaps, err := healthDB.EPGCanonicalHealth(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if healthOverlaps != 0 {
		t.Fatalf("post-migration canonical health overlaps = %d; want 0", healthOverlaps)
	}
	var scaleRows int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		  FROM epg_program p
		  JOIN channel c ON c.id=p.channel_id
		 WHERE c.epg_channel_id LIKE 'fx475.scale.%'`).Scan(&scaleRows); err != nil {
		t.Fatal(err)
	}
	if scaleRows != 50776 {
		t.Fatalf("production-scale added rows = %d; want 50776", scaleRows)
	}
	var fixtureRows, fixtureChannels, fixtureMaxChannel int
	if err := pool.QueryRow(ctx, `
		SELECT sum(channel_rows), count(*), max(channel_rows)
		  FROM (
			SELECT channel_id, count(*) AS channel_rows
			  FROM epg_program
			 GROUP BY channel_id
		  ) fixture`).Scan(&fixtureRows, &fixtureChannels, &fixtureMaxChannel); err != nil {
		t.Fatal(err)
	}
	if fixtureRows != 50783 || fixtureChannels != 189 || fixtureMaxChannel != 1489 {
		t.Fatalf("migration fixture = %d rows / %d channels / max %d; want 50783 / 189 / 1489",
			fixtureRows, fixtureChannels, fixtureMaxChannel)
	}
	planRows, err := pool.Query(ctx, `
		EXPLAIN (COSTS OFF)
		SELECT 1
		  FROM epg_program selected
		 WHERE selected.channel_id=$1
		   AND selected.is_canonical
		   AND tstzrange(selected.start_at, selected.end_at, '[)') &&
		       tstzrange($2::timestamptz, $3::timestamptz, '[)')`,
		scaleHeavyChannelID, base.Add(12*time.Hour), base.Add(13*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	var planLines []string
	for planRows.Next() {
		var line string
		if err := planRows.Scan(&line); err != nil {
			planRows.Close()
			t.Fatal(err)
		}
		planLines = append(planLines, line)
	}
	if err := planRows.Err(); err != nil {
		planRows.Close()
		t.Fatal(err)
	}
	planRows.Close()
	plan := strings.Join(planLines, "\n")
	if !strings.Contains(plan, "epg_program_canonical_no_overlap") {
		t.Fatalf("canonical overlap probe did not use exclusion index:\n%s", plan)
	}
	var ambiguousLegacy, explicitSDLegacy bool
	if err := pool.QueryRow(ctx, `
		SELECT
			(SELECT is_legacy FROM epg_program WHERE source_hash='opaque-priority-10'),
			(SELECT is_legacy FROM epg_program WHERE source_hash='sd:explicit-provenance')`).Scan(
		&ambiguousLegacy, &explicitSDLegacy); err != nil {
		t.Fatal(err)
	}
	if !ambiguousLegacy || explicitSDLegacy {
		t.Fatalf("provenance classification = ambiguous legacy %t, explicit SD legacy %t; want true,false",
			ambiguousLegacy, explicitSDLegacy)
	}
	var stationMD5, numericStationMD5, ambiguousStationMD5 string
	var stationChannelID uuid.UUID
	var numericStationChannelID, ambiguousStationChannelID *uuid.UUID
	if err := pool.QueryRow(ctx, `
		SELECT last_md5, channel_id FROM sd_station_state
		 WHERE sd_lineup_id='FX475-LINEUP' AND sd_station_id='fx475.migration'`).Scan(&stationMD5, &stationChannelID); err != nil {
		t.Fatal(err)
	}
	if stationMD5 != "" || stationChannelID != channelID {
		t.Fatalf("post-migration SD station state = MD5 %q channel %s; want empty/%s",
			stationMD5, stationChannelID, channelID)
	}
	if err := pool.QueryRow(ctx, `
		SELECT last_md5, channel_id FROM sd_station_state
		 WHERE sd_lineup_id='FX475-NUMERIC-LINEUP' AND sd_station_id='9475.6'`).Scan(
		&numericStationMD5, &numericStationChannelID); err != nil {
		t.Fatal(err)
	}
	if numericStationMD5 != "" || numericStationChannelID == nil || *numericStationChannelID != numericChannelID {
		t.Fatalf("post-migration numeric SD state = MD5 %q channel %v; want empty/%s",
			numericStationMD5, numericStationChannelID, numericChannelID)
	}
	if err := pool.QueryRow(ctx, `
		SELECT last_md5, channel_id FROM sd_station_state
		 WHERE sd_lineup_id='FX475-AMBIGUOUS-LINEUP' AND sd_station_id='9475.5'`).Scan(
		&ambiguousStationMD5, &ambiguousStationChannelID); err != nil {
		t.Fatal(err)
	}
	if ambiguousStationMD5 != "" || ambiguousStationChannelID != nil {
		t.Fatalf("post-migration ambiguous SD state = MD5 %q channel %v; want empty/nil",
			ambiguousStationMD5, ambiguousStationChannelID)
	}
	rows, err := pool.Query(ctx, `
		SELECT name, description
		  FROM transcode_profile
		 WHERE name IN ('nvenc-1080p', 'nvenc-720p', 'hdr-to-sdr-nvenc')
		 ORDER BY name`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var profileDescriptions int
	for rows.Next() {
		var name, description string
		if err := rows.Scan(&name, &description); err != nil {
			t.Fatal(err)
		}
		profileDescriptions++
		if want := customProfileDescriptions[name]; description != want {
			t.Errorf("migration 0023 changed customized profile %s description to %q; want %q",
				name, description, want)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if profileDescriptions != 3 {
		t.Fatalf("updated GPU profile descriptions = %d; want 3", profileDescriptions)
	}
	rows.Close()

	// Source deletion is deliberately fail-safe at the foreign key. Repository
	// deletion must remove candidates, promote fallbacks, and only then remove
	// the source; raw SQL cannot silently cascade away the winning guide rows.
	if _, err := pool.Exec(ctx, `
		UPDATE epg_program
		   SET epg_source_id=$1, source_generation=1
		 WHERE source_hash='legacy-hash-3'`, sourceID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM epg_source WHERE id=$1`, sourceID); err == nil {
		t.Fatal("raw epg_source delete unexpectedly cascaded a candidate after migration 0023")
	}
	var sourceCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM epg_source WHERE id=$1`, sourceID).Scan(&sourceCount); err != nil {
		t.Fatal(err)
	}
	if sourceCount != 1 {
		t.Fatalf("source count after rejected raw delete = %d; want 1", sourceCount)
	}

	// The partial GiST exclusion is the final fail-closed boundary: even a
	// future writer that bypasses the repository cannot commit an overlap to
	// the canonical guide.
	if _, err := pool.Exec(ctx, `
		INSERT INTO epg_program (
			channel_id, start_at, end_at, title, source_hash, source_priority,
			is_canonical, is_legacy
		) VALUES ($1,$2,$3,'forbidden overlap','forbidden-overlap',0,true,false)`,
		channelID, base.Add(15*time.Minute), base.Add(30*time.Minute)); err == nil {
		t.Fatal("canonical overlap insert unexpectedly succeeded after migration 0023")
	}

	// Production applies every remaining migration before current repository
	// code can serve traffic. Keep the assertions above pinned to migration
	// 0023, then advance the fixture before exercising today's reconciliation
	// queries, which intentionally depend on later episode metadata columns.
	for _, migration := range post23Migrations {
		if err := fx475ApplyMigration(ctx, pool, migration); err != nil {
			t.Fatalf("apply post-0023 %s: %v", migration.Name, err)
		}
	}

	// An ambiguous symbolic/numeric legacy owner cannot prove either candidate
	// channel and must fail safe: withdraw only its watermark, not either guide.
	ambiguousRemoval, err := healthDB.ReconcileSDLineupStations(ctx, "FX475-AMBIGUOUS-LINEUP", nil)
	if err != nil {
		t.Fatal(err)
	}
	if ambiguousRemoval.StationsWithdrawn != 1 || ambiguousRemoval.ProgramsDeleted != 0 {
		t.Fatalf("ambiguous legacy removal = %+v; want one state and zero candidates", ambiguousRemoval)
	}
	var ambiguousRows int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM epg_program
		 WHERE channel_id=$1 AND source_hash='pre-migration-ambiguous-sd-hash'`,
		ambiguousExplicitChannelID).Scan(&ambiguousRows); err != nil {
		t.Fatal(err)
	}
	if ambiguousRows != 1 {
		t.Fatalf("ambiguous migration candidate rows = %d; want preserved", ambiguousRows)
	}

	// The explicit state proves ownership of this channel. Its authoritative
	// removal must retire unnamespaced v0.37 direct rows and explicit sd: rows,
	// while preserving a source-owned legacy fallback on the same channel.
	explicitRemoval, err := healthDB.ReconcileSDLineupStations(ctx, "FX475-LINEUP", nil)
	if err != nil {
		t.Fatal(err)
	}
	if explicitRemoval.StationsWithdrawn != 1 || explicitRemoval.ProgramsDeleted != 4 {
		t.Fatalf("explicit legacy removal = %+v; want one state and four direct candidates", explicitRemoval)
	}
	var primaryDirectRows, primarySourceRows int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE epg_source_id IS NULL),
		       count(*) FILTER (WHERE source_hash='legacy-hash-3' AND epg_source_id=$2)
		  FROM epg_program
		 WHERE channel_id=$1`, channelID, sourceID).Scan(&primaryDirectRows, &primarySourceRows); err != nil {
		t.Fatal(err)
	}
	if primaryDirectRows != 0 || primarySourceRows != 1 {
		t.Fatalf("explicit legacy survivors = direct/source %d/%d; want 0/1", primaryDirectRows, primarySourceRows)
	}

	// A pre-0023 station published through channel.number receives the same
	// durable provenance as runtime lookup and therefore withdraws exactly its
	// own unnamespaced legacy candidate.
	numericRemoval, err := healthDB.ReconcileSDLineupStations(ctx, "FX475-NUMERIC-LINEUP", nil)
	if err != nil {
		t.Fatal(err)
	}
	if numericRemoval.StationsWithdrawn != 1 || numericRemoval.ProgramsDeleted != 1 {
		t.Fatalf("numeric legacy removal = %+v; want one state and one candidate", numericRemoval)
	}
	var numericRows, remainingStates int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM epg_program WHERE channel_id=$1`, numericChannelID).Scan(&numericRows); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM sd_station_state`).Scan(&remainingStates); err != nil {
		t.Fatal(err)
	}
	if numericRows != 0 || remainingStates != 0 {
		t.Fatalf("post-withdrawal numeric rows/station states = %d/%d; want 0/0", numericRows, remainingStates)
	}
}

func fx475ApplyMigration(ctx context.Context, pool *pgxpool.Pool, migration store.Migration) error {
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
