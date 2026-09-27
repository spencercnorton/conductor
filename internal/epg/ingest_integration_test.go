// ingest_integration_test.go — end-to-end EPG roundtrip against real
// Postgres. Gated on CONDUCTOR_INT_TEST=1 (same harness as
// internal/store/lease_integration_test.go).
package epg_test

import (
	"context"
	"fmt"
	"github.com/spencercnorton/conductor/internal/testdb"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/spencercnorton/conductor/internal/epg"
	"github.com/spencercnorton/conductor/internal/store"
)

const (
	pgImage         = "postgres:16-alpine"
	pgContainerName = "conductor-epg-test-pg"
	pgPort          = "55434"
	pgPassword      = "conductor-test"
)

func skipIfNoIntegration(t *testing.T) {
	t.Helper()
	if os.Getenv("CONDUCTOR_INT_TEST") != "1" {
		t.Skip("integration test skipped (set CONDUCTOR_INT_TEST=1 to enable)")
	}
}

func startTestPostgres(t *testing.T) string {
	t.Helper()
	if dsn := os.Getenv("CONDUCTOR_TEST_DSN"); dsn != "" {
		return testdb.New(t, dsn)
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not available and CONDUCTOR_TEST_DSN not set")
	}
	_ = exec.Command("docker", "rm", "-f", "-v", pgContainerName).Run()
	out, err := exec.Command("docker", "run", "-d",
		"--name", pgContainerName,
		"-p", pgPort+":5432",
		"-e", "POSTGRES_PASSWORD="+pgPassword,
		"-e", "POSTGRES_DB=conductor_epg_test",
		pgImage).CombinedOutput()
	if err != nil {
		t.Fatalf("docker run: %v: %s", err, out)
	}
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", "-v", pgContainerName).Run() })

	dsn := fmt.Sprintf("postgres://postgres:%s@127.0.0.1:%s/conductor_epg_test?sslmode=disable", pgPassword, pgPort)
	deadline := time.Now().Add(30 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatal("postgres container did not become ready in time")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		conn, err := pgx.Connect(ctx, dsn)
		cancel()
		if err == nil {
			conn.Close(context.Background())
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	return dsn
}

func freshDB(t *testing.T) *store.DB {
	t.Helper()
	skipIfNoIntegration(t)
	dsn := startTestPostgres(t)
	db, err := store.Open(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if err := db.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	return db
}

// servesXMLTV is a tiny HTTP server returning a fixed XMLTV body.
func servesXMLTV(t *testing.T, body string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml; charset=utf-8")
		w.Header().Set("ETag", `"v1"`)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(body))
	}))
}

func TestEpisodeMetadataMigrationForcesPopulatedSourceReingest(t *testing.T) {
	skipIfNoIntegration(t)
	ctx := context.Background()
	dsn := startTestPostgres(t)
	schema := "op477_reingest_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	root, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := root.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		_ = root.Close(ctx)
		t.Fatal(err)
	}
	_ = root.Close(ctx)
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
	var migration25 store.Migration
	for _, migration := range migrations {
		if migration.Name == "0025_episode_live_metadata.sql" {
			migration25 = migration
			break
		}
		if err := applyEPGTestMigration(ctx, pool, migration); err != nil {
			t.Fatalf("apply pre-0025 %s: %v", migration.Name, err)
		}
	}
	if migration25.Name == "" {
		t.Fatal("migration 0025 not found")
	}

	var conditionalETag, conditionalModified string
	body := `<?xml version="1.0" encoding="UTF-8"?>
<tv>
  <channel id="op477.reingest"><display-name>FX477 Reingest</display-name></channel>
  <programme channel="op477.reingest" start="20260821233000 -0400" stop="20260822003000 -0400">
    <title>Migration Metadata</title>
    <date>20260822</date>
    <episode-num system="onscreen">S01E02</episode-num>
    <episode-num system="dd_progid">EP047700000102</episode-num>
  </programme>
</tv>`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conditionalETag = r.Header.Get("If-None-Match")
		conditionalModified = r.Header.Get("If-Modified-Since")
		w.Header().Set("ETag", `"op477-new"`)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)

	var channelID, sourceID string
	if err := pool.QueryRow(ctx, `
		INSERT INTO channel (number, name, epg_channel_id, enabled)
		VALUES (9477.25, 'FX477 reingest', 'op477.reingest', true)
		RETURNING id`).Scan(&channelID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO epg_source (
			name, url, priority, enabled, last_etag, last_modified,
			snapshot_generation, snapshot_applied_at
		) VALUES ('FX477 old source',$1,0,true,'"op477-old"','Fri, 21 Aug 2026 12:00:00 GMT',7,now())
		RETURNING id`, server.URL).Scan(&sourceID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO sd_lineup (sd_lineup_id, name) VALUES ('FX477-LINEUP','FX477 lineup');
		INSERT INTO sd_station_state (sd_lineup_id, sd_station_id, last_md5)
		VALUES ('FX477-LINEUP','FX477-STATION','op477-old-md5')`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO epg_program (
			channel_id, start_at, end_at, title, episode_num_onscreen,
			original_air_date, is_new, source_hash, source_priority,
			epg_source_id, source_generation, is_canonical, is_legacy
		) VALUES (
			$1,'2026-08-22 03:30:00+00','2026-08-22 04:30:00+00',
			'Migration Metadata','S01E02','2026-08-22',true,'op477-pre-migration',0,
			$2,7,true,false
		)`, channelID, sourceID); err != nil {
		t.Fatal(err)
	}

	if err := applyEPGTestMigration(ctx, pool, migration25); err != nil {
		t.Fatal(err)
	}
	var resetETag, resetModified, resetMD5 string
	if err := pool.QueryRow(ctx, `SELECT last_etag, last_modified FROM epg_source WHERE id=$1`, sourceID).
		Scan(&resetETag, &resetModified); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT last_md5 FROM sd_station_state
		 WHERE sd_lineup_id='FX477-LINEUP' AND sd_station_id='FX477-STATION'`).Scan(&resetMD5); err != nil {
		t.Fatal(err)
	}
	if resetETag != "" || resetModified != "" || resetMD5 != "" {
		t.Fatalf("migration did not clear conditional state etag=%q modified=%q md5=%q",
			resetETag, resetModified, resetMD5)
	}

	db := &store.DB{Pool: pool}
	sources, err := db.ListEnabledEPGSources(ctx)
	if err != nil || len(sources) != 1 {
		t.Fatalf("list reset source: count=%d err=%v", len(sources), err)
	}
	_, _, _, status, _, _, err := epg.Ingest(ctx, db, sources[0], nil)
	if err != nil || status != "ok" {
		t.Fatalf("forced reingest status=%q err=%v", status, err)
	}
	if conditionalETag != "" || conditionalModified != "" {
		t.Fatalf("post-migration fetch remained conditional etag=%q modified=%q",
			conditionalETag, conditionalModified)
	}

	var providerID string
	var airingCivilDate time.Time
	var rawNew, providerSupplemented bool
	if err := pool.QueryRow(ctx, `
		SELECT provider_episode_id, provider_episode_id_supplemented,
		       airing_civil_date, is_new
		  FROM epg_program
		 WHERE epg_source_id=$1 AND is_canonical`, sourceID).Scan(
		&providerID, &providerSupplemented, &airingCivilDate, &rawNew,
	); err != nil {
		t.Fatal(err)
	}
	if providerID != "EP047700000102" || providerSupplemented || rawNew ||
		airingCivilDate.Format("2006-01-02") != "2026-08-21" {
		t.Fatalf("reingested metadata provider=%q supplemented=%t airing_date=%s raw_new=%t",
			providerID, providerSupplemented, airingCivilDate.Format("2006-01-02"), rawNew)
	}
}

func applyEPGTestMigration(ctx context.Context, pool *pgxpool.Pool, migration store.Migration) error {
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

// TestIntegrationEPGRoundtrip exercises ingest → DB → /xmltv.xml emit.
//
// Validates spec §7 essentials in the emitted XML:
//   - <channel id="..."> matches the channel.epg_channel_id we configured
//   - movie program has <category>Movie</category> + <date>YYYYMMDD</date>
//   - episode program has both episode-num system="xmltv_ns" and "onscreen"
//   - <live/> emitted for sports, <new/> for recent original_air_date
func TestIntegrationEPGRoundtrip(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()

	// Seed a channel so LookupChannelByEPGID succeeds.
	ch, err := db.CreateChannel(ctx, store.Channel{
		Number: 100, Name: "FX", CallSign: "FX",
		EpgChannelID: "ch.fx.us", Enabled: true,
		LogoURL: "https://example.com/fx.png",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Mint a tiny XMLTV: one movie + one formal episode + one live sport,
	// plus an episode whose only S/E identity is in the description and a
	// conflict proving formal episode-num always wins over that fallback.
	now := time.Now().UTC()
	xmltvBody := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<tv>
  <channel id="ch.fx.us">
    <display-name>FX</display-name>
  </channel>
  <programme channel="ch.fx.us" start="%s" stop="%s">
    <title>Heat</title>
    <desc>A career thief plans one last big score.</desc>
    <date>1995</date>
    <category>Movie</category>
    <category>Action</category>
    <length units="minutes">170</length>
  </programme>
  <programme channel="ch.fx.us" start="%s" stop="%s">
    <title>The Bear</title>
    <sub-title>System</sub-title>
    <desc>Carmy struggles with the launch.</desc>
    <category>Series</category>
    <episode-num system="onscreen">S03E07</episode-num>
    <episode-num system="dd_progid">EP012345670307</episode-num>
    <new/>
    <premiere/>
    <finale/>
  </programme>
  <programme channel="ch.fx.us" start="%s" stop="%s">
    <title>Broncos vs Chiefs</title>
    <desc>Week 9 AFC West matchup.</desc>
    <category>Sports</category>
    <category>Football</category>
    <live/>
  </programme>
  <programme channel="ch.fx.us" start="%s" stop="%s">
    <title>Law &amp; Order</title>
    <desc>S20 E02 - Man Down. Detectives investigate a construction-site death.</desc>
    <category>Series</category>
  </programme>
  <programme channel="ch.fx.us" start="%s" stop="%s">
    <title>Formal Episode Wins</title>
    <desc>S20 E02 - This conflicting fallback must not replace formal metadata.</desc>
    <category>Series</category>
    <episode-num system="onscreen">S04E05</episode-num>
    <new/>
    <previously-shown/>
  </programme>
</tv>`,
		fmtXMLTVTime(now), fmtXMLTVTime(now.Add(2*time.Hour)),
		fmtXMLTVTime(now.Add(2*time.Hour)), fmtXMLTVTime(now.Add(3*time.Hour)),
		fmtXMLTVTime(now.Add(3*time.Hour)), fmtXMLTVTime(now.Add(6*time.Hour)),
		fmtXMLTVTime(now.Add(6*time.Hour)), fmtXMLTVTime(now.Add(7*time.Hour)),
		fmtXMLTVTime(now.Add(7*time.Hour)), fmtXMLTVTime(now.Add(8*time.Hour)),
	)

	srv := servesXMLTV(t, xmltvBody)
	t.Cleanup(srv.Close)

	src, err := db.CreateEPGSource(ctx, store.EPGSource{
		// Priority 100 is permitted for ordinary XMLTV sources. Placeholder
		// classification must use ppv-off provenance, not this numeric value.
		Name: "test", URL: srv.URL, Enabled: true, Priority: store.PriorityPPVOff,
	})
	if err != nil {
		t.Fatal(err)
	}

	added, _, _, status, _, _, ierr := epg.Ingest(ctx, db, src, nil)
	if ierr != nil {
		t.Fatalf("ingest: %v", ierr)
	}
	if status != "ok" {
		t.Errorf("status: got %q want ok", status)
	}
	if added != 5 {
		t.Errorf("added: got %d want 5", added)
	}

	// Now exercise /xmltv.xml output.
	out := epg.NewOutput(db, "http://test.local")
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/xmltv.xml", nil)
	out.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("xmltv: got %d want 200", rr.Code)
	}
	body := rr.Body.String()

	for _, want := range []string{
		`<channel id="ch.fx.us">`,
		`<display-name>FX</display-name>`,
		`<title>Heat</title>`,
		`<category>Movie</category>`,
		`<date>19950101</date>`,
		`<title>The Bear</title>`,
		`<sub-title>System</sub-title>`,
		`<episode-num system="xmltv_ns">2.6.</episode-num>`,
		`<episode-num system="onscreen">S03E07</episode-num>`,
		`<new/>`,
		`<premiere/>`,
		`<finale/>`,
		`<title>LIVE broadcast — Broncos vs Chiefs</title>`,
		`<live/>`,
		`<title>Law &amp; Order</title>`,
		`S20 E02 - Man Down. Detectives investigate a construction-site death.`,
		`<episode-num system="xmltv_ns">19.1.</episode-num>`,
		`<episode-num system="onscreen">S20E02</episode-num>`,
		`<title>Formal Episode Wins</title>`,
		`<episode-num system="xmltv_ns">3.4.</episode-num>`,
		`<episode-num system="onscreen">S04E05</episode-num>`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/xmltv.xml missing %q\n--- body ---\n%s", want, body)
		}
	}
	var providerEpisodeID string
	var newExplicit, previouslyShownExplicit, isPremiere, isFinale bool
	if err := db.Pool.QueryRow(ctx, `
		SELECT provider_episode_id, new_explicit, previously_shown_explicit,
		       is_premiere, is_finale
		  FROM epg_program
		 WHERE title = 'The Bear' AND is_canonical`).Scan(
		&providerEpisodeID, &newExplicit, &previouslyShownExplicit,
		&isPremiere, &isFinale,
	); err != nil {
		t.Fatal(err)
	}
	if providerEpisodeID != "EP012345670307" || !newExplicit || previouslyShownExplicit || !isPremiere || !isFinale {
		t.Fatalf("provenance provider=%q new=%t repeat=%t premiere=%t finale=%t",
			providerEpisodeID, newExplicit, previouslyShownExplicit, isPremiere, isFinale)
	}
	formalStart := strings.Index(body, `<title>Formal Episode Wins</title>`)
	if formalStart < 0 {
		t.Fatal("formal episode block not found")
	}
	formalEnd := strings.Index(body[formalStart:], `</programme>`)
	if formalEnd < 0 {
		t.Fatal("formal episode block is unterminated")
	}
	formalBlock := body[formalStart : formalStart+formalEnd]
	if strings.Contains(formalBlock, `S20E02`) || !strings.Contains(formalBlock, `S04E05`) {
		t.Fatalf("formal episode-num did not outrank description fallback:\n%s", formalBlock)
	}
	if strings.Contains(formalBlock, `<new/>`) || !strings.Contains(formalBlock, `<previously-shown/>`) {
		t.Fatalf("explicit repeat did not override explicit New:\n%s", formalBlock)
	}
	var formalNewExplicit, formalPreviouslyShownExplicit bool
	if err := db.Pool.QueryRow(ctx, `
		SELECT new_explicit, previously_shown_explicit
		  FROM epg_program
		 WHERE title='Formal Episode Wins' AND is_canonical`).Scan(
		&formalNewExplicit, &formalPreviouslyShownExplicit,
	); err != nil {
		t.Fatal(err)
	}
	if !formalNewExplicit || !formalPreviouslyShownExplicit {
		t.Fatalf("explicit signal provenance new=%t repeat=%t", formalNewExplicit, formalPreviouslyShownExplicit)
	}

	// ETag/If-None-Match should produce 304.
	etag := rr.Header().Get("ETag")
	if etag == "" {
		t.Error("expected ETag header on 200 response")
	}
	rr2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodGet, "/xmltv.xml", nil)
	req2.Header.Set("If-None-Match", etag)
	out.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusNotModified {
		t.Errorf("If-None-Match should produce 304; got %d", rr2.Code)
	}

	lastModified := rr.Header().Get("Last-Modified")
	if lastModified == "" {
		t.Error("expected Last-Modified header on 200 response")
	}
	rr3 := httptest.NewRecorder()
	req3 := httptest.NewRequest(http.MethodGet, "/xmltv.xml", nil)
	req3.Header.Set("If-Modified-Since", lastModified)
	out.ServeHTTP(rr3, req3)
	if rr3.Code != http.StatusOK {
		t.Errorf("IMS-only request must return 200 for the moving guide window; got %d", rr3.Code)
	}
	if got := rr3.Header().Get("ETag"); got != etag {
		t.Errorf("IMS-only 200 changed stable content ETag from %q to %q", etag, got)
	}

	// A channel-only change does not move LatestProgramUpdate. When both
	// validators are present, the changed content ETag must take precedence
	// over the now-stale Last-Modified value.
	if _, err := db.Pool.Exec(ctx, `UPDATE channel SET logo_url=$2 WHERE id=$1`, ch.ID, "https://example.com/fx-v2.png"); err != nil {
		t.Fatal(err)
	}
	rr4 := httptest.NewRecorder()
	req4 := httptest.NewRequest(http.MethodGet, "/xmltv.xml", nil)
	req4.Header.Set("If-None-Match", etag)
	req4.Header.Set("If-Modified-Since", lastModified)
	out.ServeHTTP(rr4, req4)
	if rr4.Code != http.StatusOK {
		t.Errorf("mismatched ETag must override matching Last-Modified; got %d", rr4.Code)
	}
	if got := rr4.Header().Get("ETag"); got == etag {
		t.Error("channel-only change did not produce a new ETag")
	}
}

func fmtXMLTVTime(t time.Time) string {
	return t.UTC().Format("20060102150405") + " +0000"
}

// TestIntegrationEPGChannelNumberFallback verifies the ingester accepts an
// XMLTV source where <channel id="..."> is the channel number rather than
// the tvg_id-style symbolic id. This is the format Dispatcharr emits
// (`<channel id="2">` for channel number 2), and Conductor must still match.
func TestIntegrationEPGChannelNumberFallback(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()

	// Two channels: one with a tvg_id-style epg_channel_id, one with empty
	// epg_channel_id (the common Dispatcharr-import case). Both should match
	// when the XMLTV uses the channel number as its <channel id>.
	if _, err := db.CreateChannel(ctx, store.Channel{
		Number: 2, Name: "ABC Denver", CallSign: "KMGH",
		EpgChannelID: "KMGH.us", Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateChannel(ctx, store.Channel{
		Number: 10, Name: "Local Sports", CallSign: "LOCAL",
		EpgChannelID: "", Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	xmltvBody := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<tv>
  <channel id="2"><display-name>ABC Denver</display-name></channel>
  <channel id="10"><display-name>Local Sports</display-name></channel>
  <programme channel="2" start="%s" stop="%s">
    <title>News at Six</title>
  </programme>
  <programme channel="10" start="%s" stop="%s">
    <title>Broncos vs Chiefs</title>
    <category>Sports</category>
    <live/>
  </programme>
</tv>`,
		fmtXMLTVTime(now), fmtXMLTVTime(now.Add(30*time.Minute)),
		fmtXMLTVTime(now), fmtXMLTVTime(now.Add(3*time.Hour)),
	)

	srv := servesXMLTV(t, xmltvBody)
	t.Cleanup(srv.Close)

	src, err := db.CreateEPGSource(ctx, store.EPGSource{
		Name: "dispatcharr", URL: srv.URL, Enabled: true, Priority: 0,
	})
	if err != nil {
		t.Fatal(err)
	}

	added, _, _, status, _, _, ierr := epg.Ingest(ctx, db, src, nil)
	if ierr != nil {
		t.Fatalf("ingest: %v", ierr)
	}
	if status != "ok" {
		t.Errorf("status: got %q want ok", status)
	}
	if added != 2 {
		t.Errorf("added: got %d want 2 (one program per channel via channel-number fallback)", added)
	}
}
