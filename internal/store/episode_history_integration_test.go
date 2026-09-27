package store_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/spencercnorton/conductor/internal/store"
)

func TestEpisodeHistoryMigrationFailsClosedOnLegacyRows(t *testing.T) {
	skipIfNoIntegration(t)
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, startTestPostgres(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	migrations, err := store.LoadMigrations()
	if err != nil {
		t.Fatal(err)
	}
	var migration25 store.Migration
	for _, migration := range migrations {
		if migration.Name == "0025_episode_live_metadata.sql" {
			migration25 = migration
			break
		}
		if err := fx475ApplyMigration(ctx, pool, migration); err != nil {
			t.Fatalf("apply pre-0025 %s: %v", migration.Name, err)
		}
	}
	if migration25.Name == "" {
		t.Fatal("migration 0025 not found")
	}
	var channelID string
	if err := pool.QueryRow(ctx, `
		INSERT INTO channel (number, name) VALUES (94777, 'FX477 legacy history')
		RETURNING id`).Scan(&channelID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO channel_episode_seen (channel_id, episode_num)
		VALUES ($1, 'S01E01')`, channelID); err != nil {
		t.Fatal(err)
	}

	err = fx475ApplyMigration(ctx, pool, migration25)
	if err == nil || !strings.Contains(err.Error(), "channel_episode_seen is nonempty") {
		t.Fatalf("migration error = %v, want nonempty legacy-history refusal", err)
	}
	var legacyTablePresent bool
	if err := pool.QueryRow(ctx, `SELECT to_regclass('channel_episode_seen') IS NOT NULL`).Scan(&legacyTablePresent); err != nil {
		t.Fatal(err)
	}
	if !legacyTablePresent {
		t.Fatal("failed migration did not roll back the legacy table drop")
	}
	var provenanceColumns int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		  FROM information_schema.columns
		 WHERE table_schema = current_schema()
		   AND table_name = 'epg_program'
		   AND column_name IN (
		       'new_explicit','previously_shown_explicit','provider_episode_id',
		       'provider_episode_id_supplemented','airing_civil_date'
		   )`).Scan(&provenanceColumns); err != nil {
		t.Fatal(err)
	}
	if provenanceColumns != 0 {
		t.Fatalf("failed migration leaked %d provenance columns", provenanceColumns)
	}
}

func TestEffectiveNewIsHistoricalCanonicalAndShared(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	channels := make([]store.Channel, 3)
	for i := range channels {
		channel, err := db.CreateChannel(ctx, store.Channel{
			Number: float64(94770 + i), Name: "FX477 history", Enabled: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		channels[i] = channel
	}

	// PostgreSQL timestamps retain microseconds; use that precision for map keys.
	now := time.Now().UTC().Truncate(time.Microsecond)
	program := func(channel int, start time.Time, title, episode, hash string) store.EPGProgram {
		return store.EPGProgram{
			ChannelID: channels[channel].ID, StartAt: start, EndAt: start.Add(time.Hour),
			Title: title, EpisodeNumOnscreen: episode,
			IsNew: true, NewExplicit: true, SourceHash: hash, SourcePriority: 0,
		}
	}
	insert := func(row store.EPGProgram) {
		t.Helper()
		if _, err := db.UpsertEPGProgram(ctx, row); err != nil {
			t.Fatal(err)
		}
	}

	first := now.Add(2 * time.Hour)
	later := now.Add(4 * time.Hour)
	insert(program(0, first, "Shared Identity", "S01E03", "op477-shared-first"))
	insert(program(1, later, "Shared Identity", "S01E03", "op477-shared-later"))
	insert(program(2, now.Add(6*time.Hour), "Different Series", "S01E03", "op477-different-series"))
	insert(program(0, now.Add(8*time.Hour), "Shared Identity", "S01E04", "op477-different-episode"))

	// A stable provider episode ID must outrank optional formal numbering. The
	// same episode can carry S/E metadata on one channel and omit it on another;
	// both rows must still share one cross-channel history identity.
	providerFirst := program(0, now.Add(9*time.Hour), "Mixed Provider Identity", "S07E02", "op477-provider-first")
	providerFirst.ProviderEpisodeID = "EP012345670702"
	providerLater := program(1, now.Add(11*time.Hour), "Mixed Provider Identity", "", "op477-provider-later")
	providerLater.ProviderEpisodeID = providerFirst.ProviderEpisodeID
	insert(providerFirst)
	insert(providerLater)

	// A future canonical explicit repeat is durable evidence of a prior airing,
	// even though the provider omitted its historical time.
	explicitRepeat := program(1, now.Add(10*time.Hour), "Explicit Repeat", "S02E01", "op477-explicit-repeat")
	explicitRepeat.IsNew = false
	explicitRepeat.NewExplicit = false
	explicitRepeat.PreviouslyShownExplicit = true
	insert(explicitRepeat)
	insert(program(2, now.Add(12*time.Hour), "Explicit Repeat", "S02E01", "op477-false-new-after-repeat"))

	// An airing that already started is archived before deletion and continues
	// to suppress a later cross-channel claim.
	past := program(0, now.Add(-90*time.Minute), "Durable History", "S03E02", "op477-past")
	insert(past)
	insert(program(1, now.Add(14*time.Hour), "Durable History", "S03E02", "op477-after-past"))
	if _, err := db.Pool.Exec(ctx, `DELETE FROM epg_program WHERE source_hash='op477-past'`); err != nil {
		t.Fatal(err)
	}

	// A suppressed earlier candidate must never contaminate history or earliest
	// identity selection.
	blocker := program(0, now.Add(15*time.Hour+30*time.Minute), "Canonical Blocker", "S09E09", "op477-canonical-blocker")
	blocker.EndAt = now.Add(17*time.Hour + 30*time.Minute)
	blocker.IsNew = false
	blocker.NewExplicit = false
	insert(blocker)
	if _, err := db.Pool.Exec(ctx, `
		INSERT INTO epg_program (
			channel_id, start_at, end_at, title, episode_num_onscreen,
			is_new, new_explicit, source_hash, source_priority,
			is_canonical, is_legacy
		) VALUES ($1,$2,$3,'Suppressed Identity','S04E09',true,true,
		          'op477-suppressed',10,false,false)`,
		channels[0].ID, now.Add(16*time.Hour), now.Add(17*time.Hour)); err != nil {
		t.Fatal(err)
	}
	insert(program(2, now.Add(18*time.Hour), "Suppressed Identity", "S04E09", "op477-visible-after-suppressed"))
	var suppressedCanonical bool
	if err := db.Pool.QueryRow(ctx, `
		SELECT is_canonical FROM epg_program WHERE source_hash='op477-suppressed'`,
	).Scan(&suppressedCanonical); err != nil {
		t.Fatal(err)
	}
	if suppressedCanonical {
		t.Fatal("suppressed-history fixture unexpectedly became canonical")
	}

	// A moved/deleted future first slot is not history; the next airing becomes
	// the first run immediately through the shared view.
	moveFirst := program(0, now.Add(20*time.Hour), "Movable Identity", "S05E01", "op477-move-first")
	moveNext := program(1, now.Add(22*time.Hour), "Movable Identity", "S05E01", "op477-move-next")
	insert(moveFirst)
	insert(moveNext)
	if _, err := db.Pool.Exec(ctx, `DELETE FROM epg_program WHERE source_hash='op477-move-first'`); err != nil {
		t.Fatal(err)
	}

	want := map[string]bool{
		"op477-shared-first":             true,
		"op477-shared-later":             false,
		"op477-different-series":         true,
		"op477-different-episode":        true,
		"op477-provider-first":           true,
		"op477-provider-later":           false,
		"op477-explicit-repeat":          false,
		"op477-false-new-after-repeat":   false,
		"op477-after-past":               false,
		"op477-visible-after-suppressed": true,
		"op477-move-next":                true,
	}
	rows, err := db.Pool.Query(ctx, `
		SELECT source_hash, effective_is_new
		  FROM epg_program_effective
		 WHERE source_hash = ANY($1::text[])`, mapKeys(want))
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]bool, len(want))
	for rows.Next() {
		var hash string
		var isNew bool
		if err := rows.Scan(&hash, &isNew); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		seen[hash] = true
		if isNew != want[hash] {
			t.Errorf("effective New for %s = %t, want %t", hash, isNew, want[hash])
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	if len(seen) != len(want) {
		t.Fatalf("saw %d effective rows, want %d", len(seen), len(want))
	}

	// XMLTV, DVR indexer, and proactive reconciler must all expose the exact
	// same effective decision rather than reading raw epg_program.is_new.
	assertSharedNew := func(source string, output, indexer, reconciler bool) {
		t.Helper()
		if output != indexer || output != reconciler || output != want[source] {
			t.Fatalf("%s effective New output=%t indexer=%t reconciler=%t want=%t",
				source, output, indexer, reconciler, want[source])
		}
	}
	outputRows, err := db.ListProgramsForOutput(ctx, now, now.Add(24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	outputByStart := make(map[int64]bool)
	for _, row := range outputRows {
		outputByStart[row.StartAt.UnixNano()] = row.IsNew
	}
	hits, err := db.SearchEPGForIndexer(ctx, store.EPGIndexerSearch{Query: "Shared Identity"})
	if err != nil {
		t.Fatal(err)
	}
	indexerByStart := make(map[int64]bool)
	for _, hit := range hits {
		indexerByStart[hit.StartAt.UnixNano()] = hit.IsNew
	}
	reconcileRows, err := db.ListUpcomingForReconcile(ctx, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	reconcileByStart := make(map[int64]bool)
	for _, row := range reconcileRows {
		reconcileByStart[row.StartAt.UnixNano()] = row.IsNew
	}
	assertSharedNew("op477-shared-first", outputByStart[first.UnixNano()], indexerByStart[first.UnixNano()], reconcileByStart[first.UnixNano()])
	assertSharedNew("op477-shared-later", outputByStart[later.UnixNano()], indexerByStart[later.UnixNano()], reconcileByStart[later.UnixNano()])
}

func TestEffectiveNewConvergesProviderAndFormalAliasesWithinSeries(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	channels := make([]store.Channel, 3)
	for i := range channels {
		channel, err := db.CreateChannel(ctx, store.Channel{
			Number: float64(94780 + i), Name: "FX477 episode aliases", Enabled: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		channels[i] = channel
	}

	now := time.Now().UTC()
	program := func(channel int, offset time.Duration, title, formal, providerID, hash string) store.EPGProgram {
		return store.EPGProgram{
			ChannelID: channels[channel].ID, StartAt: now.Add(offset), EndAt: now.Add(offset + time.Hour),
			Title: title, EpisodeNumOnscreen: formal, ProviderEpisodeID: providerID,
			IsNew: true, NewExplicit: true, SourceHash: hash, SourcePriority: 0,
		}
	}
	insert := func(row store.EPGProgram) {
		t.Helper()
		if _, err := db.UpsertEPGProgram(ctx, row); err != nil {
			t.Fatal(err)
		}
	}

	// A composite airing bridges both directions: provider+formal then
	// formal-only, and formal-only then provider+formal.
	insert(program(0, time.Hour, "Composite First", "S01E02", "EP047700000102", "op477-composite-first"))
	insert(program(1, 2*time.Hour, "Composite First", "S01E02", "", "op477-formal-after-composite"))
	insert(program(0, 3*time.Hour, "Formal First", "S02E03", "", "op477-formal-first"))
	insert(program(1, 4*time.Hour, "Formal First", "S02E03", "EP047700000203", "op477-composite-after-formal"))

	// Alias equality is series-scoped. Reusing either half of the first pair on
	// an unrelated series must not suppress that series' first airing.
	insert(program(2, 5*time.Hour, "Unrelated Formal Series", "S01E02", "", "op477-cross-series-formal"))
	insert(program(2, 7*time.Hour, "Unrelated Provider Series", "", "EP047700000102", "op477-cross-series-provider"))

	// The history trigger must archive every alias too, not only make currently
	// visible rows converge. Exercise both directions after deleting a past row.
	pastComposite := program(0, -2*time.Hour, "Archived Composite", "S05E06", "EP047700000506", "op477-past-composite")
	insert(pastComposite)
	if _, err := db.Pool.Exec(ctx, `DELETE FROM epg_program WHERE source_hash=$1`, pastComposite.SourceHash); err != nil {
		t.Fatal(err)
	}
	insert(program(1, 8*time.Hour, "Archived Composite", "S05E06", "", "op477-formal-after-archived-composite"))

	pastFormal := program(0, -4*time.Hour, "Archived Formal", "S06E07", "", "op477-past-formal")
	insert(pastFormal)
	if _, err := db.Pool.Exec(ctx, `DELETE FROM epg_program WHERE source_hash=$1`, pastFormal.SourceHash); err != nil {
		t.Fatal(err)
	}
	insert(program(1, 10*time.Hour, "Archived Formal", "S06E07", "EP047700000607", "op477-composite-after-archived-formal"))

	want := map[string]bool{
		"op477-composite-first":                 true,
		"op477-formal-after-composite":          false,
		"op477-formal-first":                    true,
		"op477-composite-after-formal":          false,
		"op477-cross-series-formal":             true,
		"op477-cross-series-provider":           true,
		"op477-formal-after-archived-composite": false,
		"op477-composite-after-archived-formal": false,
	}
	rows, err := db.Pool.Query(ctx, `
		SELECT source_hash, effective_is_new
		  FROM epg_program_effective
		 WHERE source_hash = ANY($1::text[])`, mapKeys(want))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	seen := make(map[string]bool, len(want))
	for rows.Next() {
		var hash string
		var isNew bool
		if err := rows.Scan(&hash, &isNew); err != nil {
			t.Fatal(err)
		}
		seen[hash] = true
		if isNew != want[hash] {
			t.Errorf("effective New for %s = %t, want %t", hash, isNew, want[hash])
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(seen) != len(want) {
		t.Fatalf("saw %d alias rows, want %d", len(seen), len(want))
	}
}

func TestEpisodeHistoryProgramScopeAvoidsCrossAliasDeadlockAndSkipsUnchangedEvidence(t *testing.T) {
	db := freshDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	channels := make([]store.Channel, 4)
	for i := range channels {
		channel, err := db.CreateChannel(ctx, store.Channel{
			Number: float64(94790 + i), Name: "FX477 history serialization", Enabled: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		channels[i] = channel
	}
	now := time.Now().UTC()
	rows := []store.EPGProgram{
		{ChannelID: channels[0].ID, StartAt: now.Add(-8 * time.Hour), EndAt: now.Add(-7 * time.Hour), Title: "Alias Lock Series", EpisodeNumOnscreen: "S01E01", ProviderEpisodeID: "EP047790000101", IsNew: true, NewExplicit: true, SourceHash: "op477-lock-a1"},
		{ChannelID: channels[1].ID, StartAt: now.Add(-6 * time.Hour), EndAt: now.Add(-5 * time.Hour), Title: "Alias Lock Series", EpisodeNumOnscreen: "S01E02", ProviderEpisodeID: "EP047790000102", IsNew: true, NewExplicit: true, SourceHash: "op477-lock-a2"},
		{ChannelID: channels[2].ID, StartAt: now.Add(-4 * time.Hour), EndAt: now.Add(-3 * time.Hour), Title: "Alias Lock Series", EpisodeNumOnscreen: "S01E02", ProviderEpisodeID: "EP047790000202", IsNew: true, NewExplicit: true, SourceHash: "op477-lock-b1"},
		{ChannelID: channels[3].ID, StartAt: now.Add(-2 * time.Hour), EndAt: now.Add(-time.Hour), Title: "Alias Lock Series", EpisodeNumOnscreen: "S01E01", ProviderEpisodeID: "EP047790000201", IsNew: true, NewExplicit: true, SourceHash: "op477-lock-b2"},
	}
	for _, row := range rows {
		if _, err := db.UpsertEPGProgram(ctx, row); err != nil {
			t.Fatal(err)
		}
	}

	txA, err := db.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer txA.Rollback(ctx) //nolint:errcheck -- safe after commit
	txB, err := db.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer txB.Rollback(ctx) //nolint:errcheck -- safe after commit
	if _, err := txA.Exec(ctx, `
		UPDATE epg_program SET previously_shown_explicit=true WHERE source_hash='op477-lock-a1'`); err != nil {
		t.Fatal(err)
	}
	bFirst := make(chan error, 1)
	go func() {
		_, execErr := txB.Exec(ctx, `
			UPDATE epg_program SET previously_shown_explicit=true WHERE source_hash='op477-lock-b1'`)
		bFirst <- execErr
	}()
	select {
	case err := <-bFirst:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("disjoint first history update was globally serialized")
	}

	// Each transaction now owns one half of the opposite alias order. A history
	// table keyed only by series+episode deadlocks here: A waits on B's S01E02
	// key while B waits on A's S01E01 key. Programme-scoped provenance keeps the
	// lookup global without making distinct guide rows share a unique lock.
	aSecond := make(chan error, 1)
	bSecond := make(chan error, 1)
	go func() {
		_, execErr := txA.Exec(ctx, `
			UPDATE epg_program SET previously_shown_explicit=true WHERE source_hash='op477-lock-a2'`)
		aSecond <- execErr
	}()
	go func() {
		_, execErr := txB.Exec(ctx, `
			UPDATE epg_program SET previously_shown_explicit=true WHERE source_hash='op477-lock-b2'`)
		bSecond <- execErr
	}()
	for name, result := range map[string]<-chan error{"A": aSecond, "B": bSecond} {
		select {
		case err := <-result:
			if err != nil {
				t.Fatalf("cross-alias transaction %s: %v", name, err)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("cross-alias transaction %s did not complete", name)
		}
	}
	if err := txA.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := txB.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	const sentinel = "2000-01-01 00:00:00+00"
	if _, err := db.Pool.Exec(ctx, `
		UPDATE episode_airing_history
		   SET last_observed_at=$1::timestamptz
		 WHERE series_identity='aliaslockseries'
		   AND episode_identity='provider:ep047790000101'`, sentinel); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool.Exec(ctx, `
		UPDATE epg_program SET description='non-history metadata changed'
		 WHERE source_hash='op477-lock-a1'`); err != nil {
		t.Fatal(err)
	}
	var unchanged bool
	if err := db.Pool.QueryRow(ctx, `
		SELECT COALESCE(bool_and(last_observed_at=$1::timestamptz), false)
		  FROM episode_airing_history
		 WHERE series_identity='aliaslockseries'
		   AND episode_identity='provider:ep047790000101'`, sentinel).Scan(&unchanged); err != nil {
		t.Fatal(err)
	}
	if !unchanged {
		t.Fatal("non-history update churned episode history")
	}
}

func TestEffectiveNewUsesFinalCanonicalSupplementedIdentity(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	channel, err := db.CreateChannel(ctx, store.Channel{
		Number: 94779, Name: "FX477 supplemented", EpgChannelID: "op477.supplemented", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	laterChannel, err := db.CreateChannel(ctx, store.Channel{
		Number: 94779.1, Name: "FX477 supplemented simulcast", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	strong, err := db.CreateEPGSource(ctx, store.EPGSource{
		Name: "FX477 strong", URL: "https://example.invalid/strong", Priority: 0, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	weak, err := db.CreateEPGSource(ctx, store.EPGSource{
		Name: "FX477 weak", URL: "https://example.invalid/weak", Priority: 10, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	invalid, err := db.CreateEPGSource(ctx, store.EPGSource{
		Name: "FX477 invalid provider", URL: "https://example.invalid/invalid", Priority: 5, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now().UTC().Add(3 * time.Hour)
	mapping := []store.EPGSnapshotMapping{{ProviderChannelID: "op477.supplemented", ChannelID: channel.ID}}
	weakProgram := store.EPGProgram{
		ChannelID: channel.ID, StartAt: start, EndAt: start.Add(time.Hour),
		Title: "Supplemented Series", EpisodeNumOnscreen: "S06E18",
		ProviderEpisodeID: "EP047700000618",
		SourceHash:        "op477-weak-donor", SourcePriority: 10,
	}
	weakResult, err := db.ReplaceEPGSourceSnapshot(ctx, weak, []store.EPGProgram{weakProgram}, mapping, "", "")
	if err != nil {
		t.Fatal(err)
	}
	weak.SnapshotGeneration = weakResult.Generation
	invalidProgram := weakProgram
	invalidProgram.ProviderEpisodeID = "SH047700000618"
	invalidProgram.SourceHash = "op477-invalid-provider-donor"
	invalidProgram.SourcePriority = 5
	if _, err := db.ReplaceEPGSourceSnapshot(ctx, invalid, []store.EPGProgram{invalidProgram}, mapping, "", ""); err != nil {
		t.Fatal(err)
	}
	strongProgram := store.EPGProgram{
		ChannelID: channel.ID, StartAt: start, EndAt: start.Add(time.Hour),
		Title: "Supplemented Series", IsNew: true, NewExplicit: true,
		SourceHash: "op477-strong-winner", SourcePriority: 0,
	}
	if _, err := db.ReplaceEPGSourceSnapshot(ctx, strong, []store.EPGProgram{strongProgram}, mapping, "", ""); err != nil {
		t.Fatal(err)
	}

	var episode, providerID string
	var isNew, canonical, providerSupplemented bool
	if err := db.Pool.QueryRow(ctx, `
		SELECT episode_identity, provider_episode_id,
		       provider_episode_id_supplemented, effective_is_new, is_canonical
		  FROM epg_program_effective
		 WHERE source_hash='op477-strong-winner'`).Scan(
		&episode, &providerID, &providerSupplemented, &isNew, &canonical,
	); err != nil {
		t.Fatal(err)
	}
	if episode != "provider:ep047700000618" || providerID != "EP047700000618" ||
		!providerSupplemented || !isNew || !canonical {
		t.Fatalf("supplemented winner episode=%q provider=%q supplemented=%t new=%t canonical=%t",
			episode, providerID, providerSupplemented, isNew, canonical)
	}

	later := store.EPGProgram{
		ChannelID: laterChannel.ID, StartAt: start.Add(2 * time.Hour), EndAt: start.Add(3 * time.Hour),
		Title: "Supplemented Series", ProviderEpisodeID: weakProgram.ProviderEpisodeID,
		IsNew: true, NewExplicit: true, SourceHash: "op477-provider-after-composite",
	}
	if _, err := db.UpsertEPGProgram(ctx, later); err != nil {
		t.Fatal(err)
	}
	var laterNew bool
	if err := db.Pool.QueryRow(ctx, `
		SELECT effective_is_new
		  FROM epg_program_effective
		 WHERE source_hash='op477-provider-after-composite'`).Scan(&laterNew); err != nil {
		t.Fatal(err)
	}
	if laterNew {
		t.Fatal("later provider-identified airing remained New after canonical donor supplementation")
	}

	if _, err := db.ReplaceEPGSourceSnapshot(ctx, weak, nil, mapping, "", ""); err != nil {
		t.Fatal(err)
	}
	if err := db.Pool.QueryRow(ctx, `
		SELECT provider_episode_id, provider_episode_id_supplemented, episode_identity
		  FROM epg_program_effective
		 WHERE source_hash='op477-strong-winner'`).Scan(
		&providerID, &providerSupplemented, &episode,
	); err != nil {
		t.Fatal(err)
	}
	if providerID != "" || providerSupplemented || episode != "s6e18" {
		t.Fatalf("withdrawn donor left provider provenance provider=%q supplemented=%t episode=%q",
			providerID, providerSupplemented, episode)
	}
	if err := db.Pool.QueryRow(ctx, `
		SELECT effective_is_new
		  FROM epg_program_effective
		 WHERE source_hash='op477-provider-after-composite'`).Scan(&laterNew); err != nil {
		t.Fatal(err)
	}
	if !laterNew {
		t.Fatal("later provider airing did not become New after future donor withdrawal")
	}
}

func TestEpisodeIdentityNormalization(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	var sameSeries, enDashSeries, sameEpisode, providerFallback, providerPreferred, completeAliases bool
	var oversizedOnscreenSafe, oversizedXMLTVSafe, oversizedFormalOnlySafe bool
	var oversizedProviderSafe, oversizedProviderFormalSafe, oversizedSeriesSafe bool
	if err := db.Pool.QueryRow(ctx, `
		SELECT epg_series_identity('LIVE: Grey''s Anatomy') = epg_series_identity('Greys  Anatomy (LIVE)'),
		       epg_series_identity('LIVE – Grey''s Anatomy') = epg_series_identity('Greys Anatomy'),
		       epg_episode_identity('S03E07', '', '') = epg_episode_identity('', '2.6.', ''),
		       epg_episode_identity('', '', 'EP012345670307') = 'provider:ep012345670307',
		       epg_episode_identity('S99E99', '98.98.', 'EP012345670307') = 'provider:ep012345670307',
		       epg_episode_identities('S99E99', '98.98.', 'EP012345670307') =
		           ARRAY['provider:ep012345670307','s99e99']::text[],
		       epg_episode_identities('S999999999999999999999999999E1', '', 'EP012345670307') =
		           ARRAY['provider:ep012345670307']::text[],
		       epg_episode_identities('', '999999999999999999999999999.0.', 'EP012345670307') =
		           ARRAY['provider:ep012345670307']::text[],
		       cardinality(epg_episode_identities(
		           'S999999999999999999999999999E1',
		           '999999999999999999999999999.0.',
		           ''
		       )) = 0,
		       cardinality(epg_episode_identities('', '', 'EP' || repeat('A1', 100))) = 0,
		       epg_episode_identities('S03E07', '', 'EP' || repeat('A1', 100)) =
		           ARRAY['s3e7']::text[],
		       epg_series_identity(repeat('A', 513)) = ''`).Scan(
		&sameSeries, &enDashSeries, &sameEpisode, &providerFallback, &providerPreferred, &completeAliases,
		&oversizedOnscreenSafe, &oversizedXMLTVSafe, &oversizedFormalOnlySafe,
		&oversizedProviderSafe, &oversizedProviderFormalSafe, &oversizedSeriesSafe,
	); err != nil {
		t.Fatal(err)
	}
	if !sameSeries || !enDashSeries || !sameEpisode || !providerFallback || !providerPreferred ||
		!completeAliases || !oversizedOnscreenSafe || !oversizedXMLTVSafe || !oversizedFormalOnlySafe ||
		!oversizedProviderSafe || !oversizedProviderFormalSafe || !oversizedSeriesSafe {
		t.Fatalf("identity normalization series=%t en_dash=%t episode=%t provider=%t preferred=%t aliases=%t oversized_onscreen=%t oversized_xmltv=%t oversized_formal_only=%t oversized_provider=%t oversized_provider_formal=%t oversized_series=%t",
			sameSeries, enDashSeries, sameEpisode, providerFallback, providerPreferred, completeAliases,
			oversizedOnscreenSafe, oversizedXMLTVSafe, oversizedFormalOnlySafe,
			oversizedProviderSafe, oversizedProviderFormalSafe, oversizedSeriesSafe)
	}

	channel, err := db.CreateChannel(ctx, store.Channel{
		Number: 94798, Name: "FX477 oversized identities", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	oversizedProvider := "EP" + strings.Repeat("A1B2C3D4", 800)
	assertDMLSafe := func(row store.EPGProgram, wantEpisode string, wantHistory int) {
		t.Helper()
		if _, err := db.UpsertEPGProgram(ctx, row); err != nil {
			t.Fatalf("upsert %s: %v", row.SourceHash, err)
		}
		var id, episode string
		if err := db.Pool.QueryRow(ctx, `
			SELECT id::text, episode_identity
			  FROM epg_program_effective
			 WHERE source_hash=$1`, row.SourceHash).Scan(&id, &episode); err != nil {
			t.Fatal(err)
		}
		if episode != wantEpisode {
			t.Fatalf("%s episode=%q, want %q", row.SourceHash, episode, wantEpisode)
		}
		var history int
		if err := db.Pool.QueryRow(ctx, `
			SELECT count(*) FROM episode_airing_history WHERE program_id=$1`, id).Scan(&history); err != nil {
			t.Fatal(err)
		}
		if history != wantHistory {
			t.Fatalf("%s history rows=%d, want %d", row.SourceHash, history, wantHistory)
		}
	}
	base := store.EPGProgram{
		ChannelID: channel.ID, StartAt: now.Add(-4 * time.Hour), EndAt: now.Add(-3 * time.Hour),
		Title: "Oversized Identity Series", IsNew: true, NewExplicit: true,
	}
	formalFallback := base
	formalFallback.EpisodeNumOnscreen = "S03E07"
	formalFallback.ProviderEpisodeID = oversizedProvider
	formalFallback.SourceHash = "op477-oversized-provider-formal"
	assertDMLSafe(formalFallback, "s3e7", 1)

	providerOnly := base
	providerOnly.StartAt = now.Add(-3 * time.Hour)
	providerOnly.EndAt = now.Add(-2 * time.Hour)
	providerOnly.ProviderEpisodeID = oversizedProvider
	providerOnly.SourceHash = "op477-oversized-provider-only"
	assertDMLSafe(providerOnly, "", 0)

	oversizedTitle := base
	oversizedTitle.StartAt = now.Add(-2 * time.Hour)
	oversizedTitle.EndAt = now.Add(-time.Hour)
	oversizedTitle.Title = strings.Repeat("OversizedTitle", 500)
	oversizedTitle.ProviderEpisodeID = "EP047700009999"
	oversizedTitle.SourceHash = "op477-oversized-series"
	assertDMLSafe(oversizedTitle, "provider:ep047700009999", 0)
}

func mapKeys(values map[string]bool) []string {
	out := make([]string, 0, len(values))
	for key := range values {
		out = append(out, key)
	}
	return out
}
