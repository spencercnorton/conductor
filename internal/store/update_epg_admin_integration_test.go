// update_epg_admin_integration_test.go — exercises UpdateEPGSource and
// UpdateChannel against a real Postgres. Same gating as
// lease_integration_test.go (CONDUCTOR_INT_TEST=1).
package store_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/store"
)

func strp(s string) *string { return &s }
func intp(i int) *int       { return &i }
func boolp(b bool) *bool    { return &b }

func storeTestEPGSourceByID(t *testing.T, db *store.DB, id uuid.UUID) store.EPGSource {
	t.Helper()
	sources, err := db.ListEPGSources(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range sources {
		if source.ID == id {
			return source
		}
	}
	t.Fatalf("EPG source %s not found", id)
	return store.EPGSource{}
}

func storeTestRecordEPGPoll(t *testing.T, db *store.DB, source store.EPGSource, status string, ok bool) (int, int) {
	t.Helper()
	startedAt, err := db.BeginEPGSourcePollAttempt(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	prev, cur, err := db.UpdateEPGSourcePollState(context.Background(), source, startedAt, status, ok)
	if err != nil {
		t.Fatal(err)
	}
	return prev, cur
}

func TestUpdateEPGSource_PartialAndPollStateReset(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()

	src, err := db.CreateEPGSource(ctx, store.EPGSource{
		Name: "dispatcharr", URL: "http://old.example/guide.xml",
		Priority: 0, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Seed poll state: a failure streak + etag on the old URL.
	if _, err := db.Pool.Exec(ctx, `UPDATE epg_source SET last_etag='"v1"', last_modified='lm' WHERE id=$1`, src.ID); err != nil {
		t.Fatal(err)
	}
	pollSource := storeTestEPGSourceByID(t, db, src.ID)
	for i := 0; i < 3; i++ {
		storeTestRecordEPGPoll(t, db, pollSource, "error:502", false)
	}

	// Partial: flip enabled only. The configuration generation advances and
	// validators clear so an in-flight/next request cannot reuse the enabled
	// generation's cached representation; the failure streak remains useful.
	got, err := db.UpdateEPGSource(ctx, src.ID, store.EPGSourceUpdate{Enabled: boolp(false)})
	if err != nil {
		t.Fatal(err)
	}
	if got.Enabled || got.URL != "http://old.example/guide.xml" || got.Priority != 0 {
		t.Fatalf("partial update touched other fields: %+v", got)
	}
	if got.ConsecutiveFailures != 3 || got.LastETag != "" || got.LastModified != "" || got.SnapshotGeneration != 1 {
		t.Fatalf("enabled-only update did not fence the old representation: %+v", got)
	}

	// URL change resets etag/last_modified/failure streak.
	got, err = db.UpdateEPGSource(ctx, src.ID, store.EPGSourceUpdate{
		URL: strp("http://new.example/guide.xml"), Priority: intp(5),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.URL != "http://new.example/guide.xml" || got.Priority != 5 {
		t.Fatalf("url/priority not applied: %+v", got)
	}
	if got.ConsecutiveFailures != 0 || got.LastETag != "" || got.LastModified != "" {
		t.Fatalf("url change must reset poll state: %+v", got)
	}
	if got.SnapshotGeneration != 2 {
		t.Fatalf("URL+priority config edit generation = %d, want one increment to 2", got.SnapshotGeneration)
	}

	// Same-URL update must NOT reset poll state.
	if _, err := db.Pool.Exec(ctx, `
		UPDATE epg_source SET last_etag='"v2"', consecutive_failures=2 WHERE id=$1`, src.ID); err != nil {
		t.Fatal(err)
	}
	got, err = db.UpdateEPGSource(ctx, src.ID, store.EPGSourceUpdate{
		URL: strp("http://new.example/guide.xml"), Name: strp("provider"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "provider" || got.ConsecutiveFailures != 2 || got.LastETag != `"v2"` {
		t.Fatalf("same-url update reset poll state: %+v", got)
	}

	// Unknown id → ErrNotFound.
	if _, err := db.UpdateEPGSource(ctx, uuid.New(), store.EPGSourceUpdate{Enabled: boolp(true)}); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestUpdateEPGSource_ConfigGenerationAndValidatorRules(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	src, err := db.CreateEPGSource(ctx, store.EPGSource{
		Name: "fenced-source", URL: "http://one.invalid/guide.xml",
		AuthHeader: "Authorization: one", Priority: 1, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	setValidators := func() {
		t.Helper()
		if _, err := db.Pool.Exec(ctx, `UPDATE epg_source SET last_etag='etag', last_modified='modified' WHERE id=$1`, src.ID); err != nil {
			t.Fatal(err)
		}
	}
	assert := func(wantGeneration int64, wantValidators bool) store.EPGSource {
		t.Helper()
		sources, err := db.ListEPGSources(ctx)
		if err != nil {
			t.Fatalf("list source = %+v, %v", sources, err)
		}
		var got store.EPGSource
		for _, source := range sources {
			if source.ID == src.ID {
				got = source
				break
			}
		}
		if got.ID != src.ID {
			t.Fatalf("source %s missing from %+v", src.ID, sources)
		}
		if got.SnapshotGeneration != wantGeneration {
			t.Fatalf("generation = %d, want %d", got.SnapshotGeneration, wantGeneration)
		}
		hasValidators := got.LastETag != "" || got.LastModified != ""
		if hasValidators != wantValidators {
			t.Fatalf("validators = %q/%q, want present=%v", got.LastETag, got.LastModified, wantValidators)
		}
		return got
	}

	setValidators()
	wantHeader := "Authorization: two"
	if _, err := db.UpdateEPGSource(ctx, src.ID, store.EPGSourceUpdate{AuthHeader: &wantHeader}); err != nil {
		t.Fatal(err)
	}
	assert(1, false)

	setValidators()
	priority := 2
	if _, err := db.UpdateEPGSource(ctx, src.ID, store.EPGSourceUpdate{Priority: &priority}); err != nil {
		t.Fatal(err)
	}
	assert(2, true)

	disabled := false
	if _, err := db.UpdateEPGSource(ctx, src.ID, store.EPGSourceUpdate{Enabled: &disabled}); err != nil {
		t.Fatal(err)
	}
	assert(3, false)

	newURL := "http://two.invalid/guide.xml"
	if _, err := db.UpdateEPGSource(ctx, src.ID, store.EPGSourceUpdate{URL: &newURL}); err != nil {
		t.Fatal(err)
	}
	assert(4, false)

	// Re-applying identical values is not a new source generation.
	if _, err := db.UpdateEPGSource(ctx, src.ID, store.EPGSourceUpdate{
		URL: &newURL, AuthHeader: &wantHeader, Priority: &priority, Enabled: &disabled,
	}); err != nil {
		t.Fatal(err)
	}
	assert(4, false)
}

func TestUpdateEPGSource_DisablePromotesFallbackAndReenableRequiresFreshSnapshot(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	const epgID = "fx475.source-lifecycle.example"
	channel, err := db.CreateChannel(ctx, store.Channel{
		Number: 9475.66, Name: "EPG source lifecycle", EpgChannelID: epgID, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	low, err := db.CreateEPGSource(ctx, store.EPGSource{
		Name: "source-lifecycle-fallback", URL: "https://fallback.invalid/guide.xml",
		Priority: 10, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	high, err := db.CreateEPGSource(ctx, store.EPGSource{
		Name: "source-lifecycle-winner", URL: "https://winner.invalid/guide.xml",
		Priority: 0, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now().UTC().Truncate(time.Minute).Add(48 * time.Hour)
	mappings := []store.EPGSnapshotMapping{{ProviderChannelID: epgID, ChannelID: channel.ID}}
	publish := func(source store.EPGSource, title, hash, etag string) store.EPGSource {
		t.Helper()
		if _, err := db.ReplaceEPGSourceSnapshot(ctx, source, []store.EPGProgram{{
			ChannelID: channel.ID, StartAt: start, EndAt: start.Add(time.Hour),
			Title: title, SourceHash: hash,
		}}, mappings, etag, etag+"-modified"); err != nil {
			t.Fatalf("publish %s: %v", source.Name, err)
		}
		return storeTestEPGSourceByID(t, db, source.ID)
	}
	canonicalTitle := func() string {
		t.Helper()
		var title string
		if err := db.Pool.QueryRow(ctx, `
			SELECT title FROM epg_program
			 WHERE channel_id = $1 AND is_canonical`, channel.ID).Scan(&title); err != nil {
			t.Fatal(err)
		}
		return title
	}

	low = publish(low, "Retained fallback", "xmltv:lifecycle:fallback", `"fallback-v1"`)
	high = publish(high, "Preferred winner", "xmltv:lifecycle:winner", `"winner-v1"`)
	if got := canonicalTitle(); got != "Preferred winner" {
		t.Fatalf("canonical title before disable = %q", got)
	}
	beforeDisable, err := db.LatestProgramUpdate(ctx)
	if err != nil {
		t.Fatal(err)
	}

	disabled, err := db.UpdateEPGSource(ctx, high.ID, store.EPGSourceUpdate{Enabled: boolp(false)})
	if err != nil {
		t.Fatal(err)
	}
	if disabled.Enabled || disabled.SnapshotGeneration != high.SnapshotGeneration+1 ||
		disabled.LastETag != "" || disabled.LastModified != "" {
		t.Fatalf("disabled source did not advance and clear its fetch fence: %+v", disabled)
	}
	if got := canonicalTitle(); got != "Retained fallback" {
		t.Fatalf("canonical title after disable = %q; want retained fallback", got)
	}
	var disabledRows int
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM epg_program WHERE epg_source_id = $1`, high.ID).Scan(&disabledRows); err != nil {
		t.Fatal(err)
	}
	if disabledRows != 0 {
		t.Fatalf("disable retained %d stale source candidates", disabledRows)
	}
	afterDisable, err := db.LatestProgramUpdate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !afterDisable.After(beforeDisable) {
		t.Fatalf("disable did not advance guide revision: before=%s after=%s", beforeDisable, afterDisable)
	}

	// Simulate a candidate retained by a pre-fix runtime while the source was
	// disabled. Both an edit while disabled and the later re-enable must remove
	// it and atomically restore the fallback; otherwise a priority change could
	// re-rank the stale candidate back into the guide.
	seedStaleDisabledCandidate := func(hash string) {
		t.Helper()
		if _, err := db.Pool.Exec(ctx, `
			UPDATE epg_program SET is_canonical = false WHERE epg_source_id = $1`, low.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Pool.Exec(ctx, `
			INSERT INTO epg_program (
				channel_id, start_at, end_at, title, source_hash, source_priority,
				epg_source_id, source_generation, is_canonical, is_legacy
			) VALUES ($1, $2, $3, 'Persisted stale disabled candidate',
			          $4, $5, $6, $7, true, false)`,
			channel.ID, start, start.Add(time.Hour), hash, disabled.Priority,
			high.ID, disabled.SnapshotGeneration); err != nil {
			t.Fatal(err)
		}
	}
	seedStaleDisabledCandidate("xmltv:lifecycle:persisted-disabled-edit")
	repaired, err := db.UpdateEPGSource(ctx, high.ID, store.EPGSourceUpdate{Priority: intp(1)})
	if err != nil {
		t.Fatal(err)
	}
	if repaired.Enabled || repaired.Priority != 1 {
		t.Fatalf("disabled-source repair update = %+v", repaired)
	}
	disabled = repaired
	if got := canonicalTitle(); got != "Retained fallback" {
		t.Fatalf("stale winner survived disabled-source edit as %q", got)
	}
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM epg_program WHERE epg_source_id = $1`, high.ID).Scan(&disabledRows); err != nil {
		t.Fatal(err)
	}
	if disabledRows != 0 {
		t.Fatalf("disabled-source edit retained %d stale candidates", disabledRows)
	}
	seedStaleDisabledCandidate("xmltv:lifecycle:persisted-reenable")

	reenabled, err := db.UpdateEPGSource(ctx, high.ID, store.EPGSourceUpdate{Enabled: boolp(true)})
	if err != nil {
		t.Fatal(err)
	}
	if !reenabled.Enabled || reenabled.SnapshotGeneration != disabled.SnapshotGeneration+1 ||
		reenabled.LastETag != "" || reenabled.LastModified != "" {
		t.Fatalf("re-enabled source did not retain an unconditional fresh fence: %+v", reenabled)
	}
	if got := canonicalTitle(); got != "Retained fallback" {
		t.Fatalf("stale winner resurfaced on re-enable: %q", got)
	}
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM epg_program WHERE epg_source_id = $1`, high.ID).Scan(&disabledRows); err != nil {
		t.Fatal(err)
	}
	if disabledRows != 0 {
		t.Fatalf("re-enable retained %d pre-fix stale source candidates", disabledRows)
	}

	// A request that began before disable cannot publish after the complete
	// disable/re-enable cycle even though enabled=true again.
	if _, err := db.ReplaceEPGSourceSnapshot(ctx, high, []store.EPGProgram{{
		ChannelID: channel.ID, StartAt: start, EndAt: start.Add(time.Hour),
		Title: "Stale pre-disable winner", SourceHash: "xmltv:lifecycle:stale",
	}}, mappings, `"stale"`, "stale-modified"); !errors.Is(err, store.ErrStaleEPGSnapshot) {
		t.Fatalf("pre-disable snapshot after re-enable error = %v; want typed stale", err)
	}
	if got := canonicalTitle(); got != "Retained fallback" {
		t.Fatalf("stale pre-disable snapshot changed canonical title to %q", got)
	}

	fresh := publish(reenabled, "Fresh re-enabled winner", "xmltv:lifecycle:fresh", `"winner-v2"`)
	if got := canonicalTitle(); got != "Fresh re-enabled winner" {
		t.Fatalf("canonical title after fresh re-enabled snapshot = %q", got)
	}
	var candidateGeneration int64
	if err := db.Pool.QueryRow(ctx, `
		SELECT source_generation FROM epg_program
		 WHERE epg_source_id = $1`, high.ID).Scan(&candidateGeneration); err != nil {
		t.Fatal(err)
	}
	if candidateGeneration != fresh.SnapshotGeneration {
		t.Fatalf("fresh candidate generation = %d, source = %d", candidateGeneration, fresh.SnapshotGeneration)
	}
}

func TestRetireLegacyEPGRowsWaitsForEveryEnabledSnapshotAndPromotesFallback(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	first, err := db.CreateEPGSource(ctx, store.EPGSource{
		Name: "legacy-bootstrap-first", URL: "https://first.invalid/guide.xml",
		Priority: 0, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := db.CreateEPGSource(ctx, store.EPGSource{
		Name: "legacy-bootstrap-second", URL: "https://second.invalid/guide.xml",
		Priority: 1, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	disabled, err := db.CreateEPGSource(ctx, store.EPGSource{
		Name: "legacy-bootstrap-already-disabled", URL: "https://disabled.invalid/guide.xml",
		Priority: 2, Enabled: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	retireChannel, err := db.CreateChannel(ctx, store.Channel{
		Number: 9475.7, Name: "Legacy XMLTV retirement", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	ambiguousChannel, err := db.CreateChannel(ctx, store.Channel{
		Number: 9475.8, Name: "Ambiguous legacy producers", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Channel creation invalidates every source mapping generation.
	first = storeTestEPGSourceByID(t, db, first.ID)
	second = storeTestEPGSourceByID(t, db, second.ID)
	start := time.Now().UTC().Truncate(time.Minute).Add(72 * time.Hour)
	const legacyXMLTVHash = "11111111111111111111111111111111"
	if _, err := db.Pool.Exec(ctx, `
		INSERT INTO epg_program (
			channel_id, start_at, end_at, title, source_hash, source_priority,
			is_canonical, is_legacy
		) VALUES
			($1, $2, $3, 'Stale disabled-source XMLTV', $4, 2, true, true),
			($1, $5, $6, 'Opaque fallback', 'opaque-legacy-fallback', 3, false, true),
			($7, $2, $3, 'Historical SD ambiguity', '22222222222222222222222222222222', 10, true, true),
			($7, $3, $8, 'Migration 0013 ambiguity', '33333333333333333333333333333333', 100, true, true),
			($7, $8, $9, 'Opaque direct producer', 'opaque-direct-producer', 4, true, true)`,
		retireChannel.ID, start, start.Add(time.Hour), legacyXMLTVHash,
		start.Add(5*time.Minute), start.Add(55*time.Minute), ambiguousChannel.ID,
		start.Add(2*time.Hour), start.Add(3*time.Hour)); err != nil {
		t.Fatal(err)
	}
	beforeRetirement, err := db.LatestProgramUpdate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	assertLegacyCount := func(want int) {
		t.Helper()
		var got int
		if err := db.Pool.QueryRow(ctx, `
			SELECT count(*) FROM epg_program WHERE source_hash=$1`, legacyXMLTVHash).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("recognizable legacy XMLTV rows=%d, want %d", got, want)
		}
	}
	retire := func(want int64) {
		t.Helper()
		got, err := db.RetireLegacyEPGRowsAfterSnapshotBootstrap(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("retired legacy rows=%d, want %d", got, want)
		}
	}

	retire(0)
	assertLegacyCount(1)
	if _, err := db.ReplaceEPGSourceSnapshot(ctx, first, nil, nil, `"first"`, "first-modified"); err != nil {
		t.Fatal(err)
	}
	retire(0)
	assertLegacyCount(1)
	if _, err := db.ReplaceEPGSourceSnapshot(ctx, second, nil, nil, `"second"`, "second-modified"); err != nil {
		t.Fatal(err)
	}

	// Hold an enable transition uncommitted while retirement starts. The
	// retirement transaction must wait on the source row, then observe the new
	// enabled-but-unpublished generation and preserve the legacy guide. An
	// unlocked readiness aggregate would see the old disabled row and delete it.
	enableTx, err := db.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := enableTx.Exec(ctx, `
		UPDATE epg_source
		   SET enabled=true,
		       snapshot_generation=snapshot_generation+1,
		       last_etag='', last_modified=''
		 WHERE id=$1`, disabled.ID); err != nil {
		_ = enableTx.Rollback(ctx)
		t.Fatal(err)
	}
	type retirementResult struct {
		deleted int64
		err     error
	}
	retirementDone := make(chan retirementResult, 1)
	go func() {
		deleted, err := db.RetireLegacyEPGRowsAfterSnapshotBootstrap(ctx)
		retirementDone <- retirementResult{deleted: deleted, err: err}
	}()
	select {
	case result := <-retirementDone:
		_ = enableTx.Rollback(ctx)
		t.Fatalf("legacy retirement bypassed source transition lock: %+v", result)
	case <-time.After(250 * time.Millisecond):
	}
	if err := enableTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	result := <-retirementDone
	if result.err != nil || result.deleted != 0 {
		t.Fatalf("legacy retirement raced enabled source: %+v", result)
	}
	assertLegacyCount(1)
	disabled = storeTestEPGSourceByID(t, db, disabled.ID)
	if _, err := db.ReplaceEPGSourceSnapshot(ctx, disabled, nil, nil, `"reenabled"`, "reenabled-modified"); err != nil {
		t.Fatal(err)
	}

	retire(1)
	assertLegacyCount(0)
	var fallbackCanonical bool
	if err := db.Pool.QueryRow(ctx, `
		SELECT is_canonical FROM epg_program
		 WHERE source_hash='opaque-legacy-fallback'`).Scan(&fallbackCanonical); err != nil {
		t.Fatal(err)
	}
	if !fallbackCanonical {
		t.Fatal("legacy retirement did not promote the retained opaque fallback")
	}
	var ambiguousSurvivors int
	if err := db.Pool.QueryRow(ctx, `
		SELECT count(*) FROM epg_program
		 WHERE channel_id=$1 AND is_legacy`, ambiguousChannel.ID).Scan(&ambiguousSurvivors); err != nil {
		t.Fatal(err)
	}
	if ambiguousSurvivors != 3 {
		t.Fatalf("ambiguous SD/backfill/direct survivors=%d, want 3", ambiguousSurvivors)
	}
	afterRetirement, err := db.LatestProgramUpdate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !afterRetirement.After(beforeRetirement) {
		t.Fatalf("legacy retirement did not advance guide revision: before=%s after=%s",
			beforeRetirement, afterRetirement)
	}
	retire(0)
}

func TestEPGPollStateCASFences304And200ConfigRacesAndDeletion(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()

	// A 304 may validate the exact cached generation, then lose a race with an
	// auth edit before worker health bookkeeping. The old success must be neutral.
	source304, err := db.CreateEPGSource(ctx, store.EPGSource{
		Name: "cas-304", URL: "http://cas-304.invalid", AuthHeader: "Authorization: old", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool.Exec(ctx, `
		UPDATE epg_source
		   SET snapshot_generation=1, last_etag='"cas-304"',
		       consecutive_failures=2, last_status='error: prior'
		 WHERE id=$1`, source304.ID); err != nil {
		t.Fatal(err)
	}
	expected304 := storeTestEPGSourceByID(t, db, source304.ID)
	started304, err := db.BeginEPGSourcePollAttempt(ctx, expected304)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.ValidateEPGSourceSnapshot(ctx, expected304); err != nil {
		t.Fatal(err)
	}
	wantHeader := "Authorization: new"
	if _, err := db.UpdateEPGSource(ctx, source304.ID, store.EPGSourceUpdate{AuthHeader: &wantHeader}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.UpdateEPGSourcePollState(ctx, expected304, started304, "no_change", true); !errors.Is(err, store.ErrStaleEPGPollAttempt) {
		t.Fatalf("post-config 304 bookkeeping error = %v; want typed neutral stale", err)
	}
	persisted304 := storeTestEPGSourceByID(t, db, source304.ID)
	if persisted304.SnapshotGeneration != 2 ||
		persisted304.AuthHeader != wantHeader ||
		persisted304.ConsecutiveFailures != 0 ||
		persisted304.LastStatus != "error: prior" ||
		persisted304.LastFetchedAt != nil {
		t.Fatalf("old 304 mutated new config diagnostics: %+v", persisted304)
	}

	// The same CAS applies after a successful 200 has atomically published rows
	// and next-generation validators but before its later health update.
	channel, err := db.CreateChannel(ctx, store.Channel{
		Number: 9465, Name: "CAS 200", EpgChannelID: "cas.200", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	source200, err := db.CreateEPGSource(ctx, store.EPGSource{
		Name: "cas-200", URL: "http://cas-200.invalid", AuthHeader: "Authorization: old", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	started200, err := db.BeginEPGSourcePollAttempt(ctx, source200)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now().UTC().Truncate(time.Minute).Add(24 * time.Hour)
	if _, err := db.ReplaceEPGSourceSnapshot(ctx, source200, []store.EPGProgram{{
		ChannelID: channel.ID, StartAt: start, EndAt: start.Add(time.Hour),
		Title: "CAS 200 programme", SourceHash: "xmltv:cas-200",
	}}, []store.EPGSnapshotMapping{{ProviderChannelID: "cas.200", ChannelID: channel.ID}},
		`"cas-200"`, "cas-200-modified"); err != nil {
		t.Fatal(err)
	}
	expected200 := source200
	expected200.SnapshotGeneration = 1
	expected200.LastETag = `"cas-200"`
	expected200.LastModified = "cas-200-modified"
	if _, err := db.UpdateEPGSource(ctx, source200.ID, store.EPGSourceUpdate{AuthHeader: &wantHeader}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.UpdateEPGSourcePollState(ctx, expected200, started200, "ok", true); !errors.Is(err, store.ErrStaleEPGPollAttempt) {
		t.Fatalf("post-config 200 bookkeeping error = %v; want typed neutral stale", err)
	}

	// Source deletion after fetch is the same neutral outcome, not ErrNotFound
	// and never an upstream-failure attribution.
	deleted, err := db.CreateEPGSource(ctx, store.EPGSource{
		Name: "cas-deleted", URL: "http://cas-deleted.invalid", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	startedDeleted, err := db.BeginEPGSourcePollAttempt(ctx, deleted)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.DeleteEPGSource(ctx, deleted.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.UpdateEPGSourcePollState(ctx, deleted, startedDeleted, "error: HTTP 502", false); !errors.Is(err, store.ErrStaleEPGPollAttempt) {
		t.Fatalf("post-delete bookkeeping error = %v; want typed neutral stale", err)
	}
}

func TestUpdateChannel_EpgChannelIDSetAndClear(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()

	c, err := db.CreateChannel(ctx, store.Channel{
		Number: 77, Name: "TBS", CallSign: "TBS", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Set the mapping; other fields untouched.
	got, err := db.UpdateChannel(ctx, c.ID, store.ChannelUpdate{EpgChannelID: strp("TBS.us")})
	if err != nil {
		t.Fatal(err)
	}
	if got.EpgChannelID != "TBS.us" || got.Name != "TBS" || !got.Enabled || got.Number != 77 {
		t.Fatalf("unexpected channel after set: %+v", got)
	}

	// Pointer-to-empty clears it (distinct from nil = unchanged).
	got, err = db.UpdateChannel(ctx, c.ID, store.ChannelUpdate{EpgChannelID: strp("")})
	if err != nil {
		t.Fatal(err)
	}
	if got.EpgChannelID != "" {
		t.Fatalf("expected cleared epg_channel_id, got %q", got.EpgChannelID)
	}

	// nil leaves it alone.
	got, err = db.UpdateChannel(ctx, c.ID, store.ChannelUpdate{Enabled: boolp(false)})
	if err != nil {
		t.Fatal(err)
	}
	if got.Enabled || got.EpgChannelID != "" || got.Name != "TBS" {
		t.Fatalf("partial update wrong: %+v", got)
	}

	// Unknown id → ErrNotFound.
	if _, err := db.UpdateChannel(ctx, uuid.New(), store.ChannelUpdate{Enabled: boolp(true)}); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestChannelEPGIDRejectsDuplicateExplicitMappings(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	first, err := db.CreateChannel(ctx, store.Channel{
		Number: 78, Name: "First mapping", EpgChannelID: "duplicate.example", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateChannel(ctx, store.Channel{
		Number: 79, Name: "Duplicate create", EpgChannelID: first.EpgChannelID, Enabled: true,
	}); err == nil {
		t.Fatal("CreateChannel accepted duplicate non-empty epg_channel_id")
	}
	second, err := db.CreateChannel(ctx, store.Channel{
		Number: 79, Name: "Duplicate update", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.LookupChannelByEPGID(ctx, ""); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("blank EPG identity resolved to an unmapped channel: %v", err)
	}
	if _, err := db.UpdateChannel(ctx, second.ID, store.ChannelUpdate{EpgChannelID: strp(first.EpgChannelID)}); err == nil {
		t.Fatal("UpdateChannel accepted duplicate non-empty epg_channel_id")
	}
	var persistedEPGID string
	if err := db.Pool.QueryRow(ctx, `SELECT epg_channel_id FROM channel WHERE id=$1`, second.ID).Scan(&persistedEPGID); err != nil {
		t.Fatal(err)
	}
	if persistedEPGID != "" {
		t.Fatalf("rejected duplicate update persisted mapping %q", persistedEPGID)
	}
}

func TestListAllChannels_IncludesDisabledForAdminRepair(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()

	enabled, err := db.CreateChannel(ctx, store.Channel{
		Number: 80, Name: "Enabled", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	disabled, err := db.CreateChannel(ctx, store.Channel{
		Number: 81, Name: "Partial Provision", Enabled: false,
	})
	if err != nil {
		t.Fatal(err)
	}

	public, err := db.ListChannels(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(public) != 1 || public[0].ID != enabled.ID {
		t.Fatalf("ListChannels = %+v; want enabled row only", public)
	}
	admin, err := db.ListAllChannels(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(admin) != 2 || admin[0].ID != enabled.ID || admin[1].ID != disabled.ID {
		t.Fatalf("ListAllChannels = %+v; want enabled + disabled", admin)
	}
}

func TestPPVChannelLogoMigration_IsGuardedAndIdempotent(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()

	seeds := []store.Channel{
		{Number: 360, Name: "EPL PPV 1", Enabled: true},
		{Number: 362, Name: "Sky Sports Premier League", Enabled: true},
		{Number: 370, Name: "PPV EVENT 01", Enabled: true},
		{Number: 385, Name: "Big Ten PPV 1", Enabled: true},
		{
			Number: 386, Name: "Big Ten PPV 2",
			LogoURL: "/logos/operator-override.png", Enabled: true,
		},
	}
	for _, channel := range seeds {
		if _, err := db.CreateChannel(ctx, channel); err != nil {
			t.Fatal(err)
		}
	}

	migrations, err := store.LoadMigrations()
	if err != nil {
		t.Fatal(err)
	}
	var repairSQL string
	for _, migration := range migrations {
		if migration.Name == "0016_ppv_channel_logos.sql" {
			repairSQL = migration.SQL
			break
		}
	}
	if repairSQL == "" {
		t.Fatal("repair migration not found")
	}
	if _, err := db.Pool.Exec(ctx, repairSQL); err != nil {
		t.Fatal(err)
	}
	// A second pass must be harmless.
	if _, err := db.Pool.Exec(ctx, repairSQL); err != nil {
		t.Fatal(err)
	}

	wants := map[float64]struct {
		logo string
	}{
		360: {logo: "/logos/epl-ppv-2c7b878f.png"},
		362: {logo: "/logos/sky-sports-premier-league-75c224d8.png"},
		370: {logo: "/logos/ppv-event-6358638d.png"},
		385: {logo: "/logos/big-ten-plus-925e1063.jpg"},
		386: {logo: "/logos/operator-override.png"},
	}
	for number, want := range wants {
		got, err := db.GetChannelByNumber(ctx, number)
		if err != nil {
			t.Fatal(err)
		}
		if want.logo != "" && got.LogoURL != want.logo {
			t.Errorf("channel %.0f logo = %q, want %q", number, got.LogoURL, want.logo)
		}
	}
}
