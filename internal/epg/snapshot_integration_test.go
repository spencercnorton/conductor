//go:build !short

package epg_test

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/alerts"
	"github.com/spencercnorton/conductor/internal/api"
	"github.com/spencercnorton/conductor/internal/auth"
	"github.com/spencercnorton/conductor/internal/epg"
	"github.com/spencercnorton/conductor/internal/hdhr"
	"github.com/spencercnorton/conductor/internal/store"
)

type fx475Programme struct {
	Start time.Time
	End   time.Time
	Title string
}

type fx475SnapshotFeed struct {
	mu           sync.Mutex
	body         string
	etag         string
	lastModified string
	force304     bool
	statusCode   int
	requests     [][2]string
}

func (f *fx475SnapshotFeed) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, [2]string{r.Header.Get("If-None-Match"), r.Header.Get("If-Modified-Since")})
	if f.force304 || (f.etag != "" && r.Header.Get("If-None-Match") == f.etag) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.Header().Set("ETag", f.etag)
	w.Header().Set("Last-Modified", f.lastModified)
	if f.statusCode != 0 {
		w.WriteHeader(f.statusCode)
	}
	_, _ = w.Write([]byte(f.body))
}

func (f *fx475SnapshotFeed) set(body, etag string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.body = body
	f.etag = etag
	f.force304 = false
	f.statusCode = 0
}

func (f *fx475SnapshotFeed) setStatus(body, etag string, statusCode int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.body = body
	f.etag = etag
	f.force304 = false
	f.statusCode = statusCode
}

func (f *fx475SnapshotFeed) resetRequests() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = nil
}

func (f *fx475SnapshotFeed) lastRequest() [2]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.requests) == 0 {
		return [2]string{}
	}
	return f.requests[len(f.requests)-1]
}

func TestIntegrationEPGSourceSnapshotMovesDeletesAndRollsBack(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Minute).Add(24 * time.Hour)
	const epgID = "fx475.snapshot.example"
	ch, err := db.CreateChannel(ctx, store.Channel{
		Number: 9475.1, Name: "FX475 Snapshot", CallSign: "FX475S",
		EpgChannelID: epgID, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	feed := &fx475SnapshotFeed{lastModified: time.Now().UTC().Format(http.TimeFormat)}
	feed.set(fx475XML(epgID,
		fx475Programme{base, base.Add(30 * time.Minute), "Snapshot A"},
		fx475Programme{base.Add(30 * time.Minute), base.Add(60 * time.Minute), "Snapshot B"},
	), `"snapshot-v1"`)
	srv := httptest.NewServer(feed)
	t.Cleanup(srv.Close)
	src, err := db.CreateEPGSource(ctx, store.EPGSource{Name: "fx475-snapshot", URL: srv.URL, Enabled: true, Priority: 0})
	if err != nil {
		t.Fatal(err)
	}

	added, _, _, status, _, _, err := epg.Ingest(ctx, db, src, nil)
	if err != nil || status != "ok" || added != 2 {
		t.Fatalf("first ingest = added %d status %q err %v; want 2, ok, nil", added, status, err)
	}
	fx475AssertSourceState(t, db, src.ID, 1, 2, 2)
	src = fx475SourceByID(t, db, src.ID)
	if added, updated, unchanged, status, _, _, err := epg.Ingest(ctx, db, src, nil); err != nil ||
		status != "no_change" || added != 0 || updated != 0 || unchanged != 0 {
		t.Fatalf("conditional 304 = %d/%d/%d status %q err %v; want zero counts/no_change", added, updated, unchanged, status, err)
	}
	fx475AssertSourceState(t, db, src.ID, 1, 2, 2)

	// A provider can move only the stop time and temporarily overlap its own
	// adjacent row. Candidate upserts first leave the visible set, allowing the
	// resolver to choose a deterministic whole-program winner without tripping
	// the canonical exclusion constraint mid-transaction.
	feed.set(fx475XML(epgID,
		fx475Programme{base, base.Add(45 * time.Minute), "Snapshot A expanded"},
		fx475Programme{base.Add(30 * time.Minute), base.Add(60 * time.Minute), "Snapshot B"},
	), `"snapshot-v2"`)
	src = fx475SourceByID(t, db, src.ID)
	if _, _, _, status, _, _, err := epg.Ingest(ctx, db, src, nil); err != nil || status != "ok" {
		t.Fatalf("same-start overlap ingest = status %q err %v", status, err)
	}
	fx475AssertSourceState(t, db, src.ID, 2, 2, 1)

	moved := base.Add(5 * time.Minute)
	feed.set(fx475XML(epgID,
		fx475Programme{moved, moved.Add(30 * time.Minute), "Snapshot A moved"},
	), `"snapshot-v3"`)
	src = fx475SourceByID(t, db, src.ID)
	added, updated, _, status, _, _, err := epg.Ingest(ctx, db, src, nil)
	if err != nil || status != "ok" || added != 1 || updated != 0 {
		t.Fatalf("replacement ingest = added %d updated %d status %q err %v", added, updated, status, err)
	}
	fx475AssertSourceState(t, db, src.ID, 3, 1, 1)
	var gotStart time.Time
	var gotTitle string
	if err := db.Pool.QueryRow(ctx, `
		SELECT start_at, title
		  FROM epg_program
		 WHERE epg_source_id = $1`, src.ID).Scan(&gotStart, &gotTitle); err != nil {
		t.Fatal(err)
	}
	if !gotStart.Equal(moved) || gotTitle != "Snapshot A moved" {
		t.Fatalf("replacement row = (%s, %q), want (%s, %q)", gotStart, gotTitle, moved, "Snapshot A moved")
	}

	// Parsing completes before snapshot replacement begins. A broken payload
	// must leave the prior generation and its canonical representation intact.
	feed.set(`<tv><channel id="fx475.snapshot.example">`, `"snapshot-v4"`)
	src = fx475SourceByID(t, db, src.ID)
	if _, _, _, status, _, _, err := epg.Ingest(ctx, db, src, nil); err == nil || !strings.HasPrefix(status, "error:") {
		t.Fatalf("broken ingest = status %q err %v; want error", status, err)
	}
	fx475AssertSourceState(t, db, src.ID, 3, 1, 1)

	// A syntactically valid empty snapshot is authoritative and clears rows
	// absent from the provider's latest complete representation.
	feed.set(fx475XML(epgID), `"snapshot-v5"`)
	src = fx475SourceByID(t, db, src.ID)
	if _, _, _, status, _, _, err := epg.Ingest(ctx, db, src, nil); err != nil || status != "ok" {
		t.Fatalf("empty ingest = status %q err %v; want ok", status, err)
	}
	fx475AssertSourceState(t, db, src.ID, 4, 0, 0)
	var channelRows int
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM epg_program WHERE channel_id=$1`, ch.ID).Scan(&channelRows); err != nil {
		t.Fatal(err)
	}
	if channelRows != 0 {
		t.Fatalf("rows after authoritative empty snapshot = %d, want 0", channelRows)
	}
}

func TestIntegrationEPGIdenticalSuccessfulSnapshotDoesNotChurnOutputRevision(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Minute).Add(24 * time.Hour)
	const epgID = "fx475.noop.example"
	if _, err := db.CreateChannel(ctx, store.Channel{
		Number: 9475.11, Name: "FX475 No-op", CallSign: "FX475N",
		EpgChannelID: epgID, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}

	body := fx475XML(epgID,
		fx475Programme{base, base.Add(30 * time.Minute), "Stable Snapshot"},
	)
	feed := &fx475SnapshotFeed{lastModified: time.Now().UTC().Format(http.TimeFormat)}
	feed.set(body, `"stable-v1"`)
	srv := httptest.NewServer(feed)
	t.Cleanup(srv.Close)
	src, err := db.CreateEPGSource(ctx, store.EPGSource{Name: "fx475-noop", URL: srv.URL, Enabled: true, Priority: 0})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, status, _, _, err := epg.Ingest(ctx, db, src, nil); err != nil || status != "ok" {
		t.Fatalf("first ingest = status %q err %v", status, err)
	}
	before, err := db.LatestProgramUpdate(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// A provider may rotate validators while returning an identical successful
	// representation. The snapshot generation advances for freshness/accounting,
	// but the published guide revision must remain stable so Plex does not refetch
	// unchanged XMLTV merely because canonical flags were internally rechecked.
	feed.set(body, `"stable-v2"`)
	src = fx475SourceByID(t, db, src.ID)
	if _, _, unchanged, status, _, _, err := epg.Ingest(ctx, db, src, nil); err != nil || status != "ok" || unchanged != 1 {
		t.Fatalf("identical 200 ingest = unchanged %d status %q err %v; want 1/ok/nil", unchanged, status, err)
	}
	after, err := db.LatestProgramUpdate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !after.Equal(before) {
		t.Fatalf("identical successful snapshot changed output revision from %s to %s", before, after)
	}
	fx475AssertSourceState(t, db, src.ID, 2, 1, 1)
}

func TestIntegrationEPGCanonicalPriorityOffsetFallbackAndPromotion(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Minute).Add(48 * time.Hour)
	const epgID = "fx475.offset.example"
	ch, err := db.CreateChannel(ctx, store.Channel{
		Number: 9475.2, Name: "FX475 Offset", CallSign: "FX475O",
		EpgChannelID: epgID, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	lowFeed := &fx475SnapshotFeed{lastModified: time.Now().UTC().Format(http.TimeFormat)}
	lowFeed.set(fx475XML(epgID,
		fx475Programme{base.Add(5 * time.Minute), base.Add(20 * time.Minute), "Offset fallback A"},
		fx475Programme{base.Add(20 * time.Minute), base.Add(40 * time.Minute), "Complete fallback gap"},
		fx475Programme{base.Add(55 * time.Minute), base.Add(70 * time.Minute), "Partial fallback tail"},
	), `"low-v1"`)
	lowServer := httptest.NewServer(lowFeed)
	t.Cleanup(lowServer.Close)
	low, err := db.CreateEPGSource(ctx, store.EPGSource{Name: "fx475-low", URL: lowServer.URL, Enabled: true, Priority: 10})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, _, _, err := epg.Ingest(ctx, db, low, nil); err != nil {
		t.Fatal(err)
	}

	highFeed := &fx475SnapshotFeed{lastModified: time.Now().UTC().Format(http.TimeFormat)}
	highFeed.set(fx475XML(epgID,
		fx475Programme{base, base.Add(20 * time.Minute), "Preferred A"},
		fx475Programme{base.Add(40 * time.Minute), base.Add(60 * time.Minute), "Preferred B"},
	), `"high-v1"`)
	highServer := httptest.NewServer(highFeed)
	t.Cleanup(highServer.Close)
	high, err := db.CreateEPGSource(ctx, store.EPGSource{Name: "fx475-high", URL: highServer.URL, Enabled: true, Priority: 0})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, _, _, err := epg.Ingest(ctx, db, high, nil); err != nil {
		t.Fatal(err)
	}

	want := []string{"Preferred A", "Complete fallback gap", "Preferred B"}
	if got := fx475CanonicalTitles(t, db, ch.ID); !slices.Equal(got, want) {
		t.Fatalf("canonical titles = %v, want %v", got, want)
	}
	var total, canonical, overlaps int
	if err := db.Pool.QueryRow(ctx, `
		SELECT count(*), count(*) FILTER (WHERE is_canonical)
		  FROM epg_program
		 WHERE channel_id=$1`, ch.ID).Scan(&total, &canonical); err != nil {
		t.Fatal(err)
	}
	if err := db.Pool.QueryRow(ctx, `
		SELECT count(*)
		  FROM epg_program a
		  JOIN epg_program b ON a.channel_id=b.channel_id AND a.id < b.id
		 WHERE a.channel_id=$1 AND a.is_canonical AND b.is_canonical
		   AND a.start_at < b.end_at AND a.end_at > b.start_at`, ch.ID).Scan(&overlaps); err != nil {
		t.Fatal(err)
	}
	if total != 5 || canonical != 3 || overlaps != 0 {
		t.Fatalf("candidate health = total %d canonical %d overlaps %d; want 5, 3, 0", total, canonical, overlaps)
	}

	out := epg.NewOutput(db, "http://fx475.test")
	rr := httptest.NewRecorder()
	out.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/xmltv.xml", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("output status = %d, want 200: %s", rr.Code, rr.Body.String())
	}
	for _, hidden := range []string{"Offset fallback A", "Partial fallback tail"} {
		if strings.Contains(rr.Body.String(), hidden) {
			t.Errorf("output contains suppressed candidate %q", hidden)
		}
	}
	wantConsumerTitles := slices.Clone(want)
	slices.Sort(wantConsumerTitles)
	pending, err := db.ListPendingEnrichment(ctx, 50)
	if err != nil {
		t.Fatal(err)
	}
	pendingTitles := make([]string, 0, len(pending))
	for _, row := range pending {
		if row.ChannelID == ch.ID {
			pendingTitles = append(pendingTitles, row.Title)
		}
	}
	slices.Sort(pendingTitles)
	if !slices.Equal(pendingTitles, wantConsumerTitles) {
		t.Fatalf("enrichment consumer titles = %v, want canonical %v", pendingTitles, wantConsumerTitles)
	}
	posters, err := db.ListPosterCandidates(ctx, 50)
	if err != nil {
		t.Fatal(err)
	}
	posterTitles := make([]string, 0, len(posters))
	for _, row := range posters {
		if row.ChannelID == ch.ID {
			posterTitles = append(posterTitles, row.Title)
		}
	}
	slices.Sort(posterTitles)
	if !slices.Equal(posterTitles, wantConsumerTitles) {
		t.Fatalf("poster consumer titles = %v, want canonical %v", posterTitles, wantConsumerTitles)
	}
	reconcileRows, err := db.ListUpcomingForReconcile(ctx, 14*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	reconcileTitles := make([]string, 0, len(reconcileRows))
	for _, row := range reconcileRows {
		if row.ChannelID == ch.ID {
			reconcileTitles = append(reconcileTitles, row.Title)
		}
	}
	slices.Sort(reconcileTitles)
	if !slices.Equal(reconcileTitles, wantConsumerTitles) {
		t.Fatalf("reconcile consumer titles = %v, want canonical %v", reconcileTitles, wantConsumerTitles)
	}
	for query, wantHits := range map[string]int{"Offset fallback A": 0, "Preferred A": 1} {
		hits, err := db.SearchEPGForIndexer(ctx, store.EPGIndexerSearch{Query: query})
		if err != nil {
			t.Fatal(err)
		}
		if len(hits) != wantHits {
			t.Fatalf("indexer query %q returned %d rows, want %d", query, len(hits), wantHits)
		}
	}

	// Removing the preferred source promotes retained lower-priority rows
	// immediately; the fallback feed is not fetched a second time.
	highFeed.set(fx475XML(epgID), `"high-v2"`)
	high = fx475SourceByID(t, db, high.ID)
	if _, _, _, _, _, _, err := epg.Ingest(ctx, db, high, nil); err != nil {
		t.Fatal(err)
	}
	want = []string{"Offset fallback A", "Complete fallback gap", "Partial fallback tail"}
	if got := fx475CanonicalTitles(t, db, ch.ID); !slices.Equal(got, want) {
		t.Fatalf("promoted fallback titles = %v, want %v", got, want)
	}

	// Reintroduce the preferred feed, then demote it through the admin path.
	// Candidate priority and the canonical view must change without relying on
	// a future source refresh (which may legitimately return 304).
	highFeed.set(fx475XML(epgID,
		fx475Programme{base, base.Add(20 * time.Minute), "Preferred A"},
		fx475Programme{base.Add(40 * time.Minute), base.Add(60 * time.Minute), "Preferred B"},
	), `"high-v3"`)
	high = fx475SourceByID(t, db, high.ID)
	if _, _, _, _, _, _, err := epg.Ingest(ctx, db, high, nil); err != nil {
		t.Fatal(err)
	}
	demoted := 20
	if _, err := db.UpdateEPGSource(ctx, high.ID, store.EPGSourceUpdate{Priority: &demoted}); err != nil {
		t.Fatal(err)
	}
	if got := fx475CanonicalTitles(t, db, ch.ID); !slices.Equal(got, want) {
		t.Fatalf("priority-change canonical titles = %v, want %v", got, want)
	}
	var stalePriority int
	if err := db.Pool.QueryRow(ctx, `
		SELECT count(*) FROM epg_program
		 WHERE epg_source_id=$1 AND source_priority <> $2`, high.ID, demoted).Scan(&stalePriority); err != nil {
		t.Fatal(err)
	}
	if stalePriority != 0 {
		t.Fatalf("source rows retaining stale priority = %d, want 0", stalePriority)
	}
}

func TestIntegrationEPGRemapInvalidatesValidatorsAndReprocesses(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Minute).Add(72 * time.Hour)
	const oldID = "fx475.remap.old"
	const newID = "fx475.remap.new"
	ch, err := db.CreateChannel(ctx, store.Channel{
		Number: 9475.3, Name: "FX475 Remap", CallSign: "FX475R",
		EpgChannelID: oldID, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	feed := &fx475SnapshotFeed{lastModified: "Fri, 21 Aug 2026 12:00:00 GMT"}
	feed.set(fx475XML(oldID, fx475Programme{base, base.Add(time.Hour), "Old mapping row"}), `"remap-v1"`)
	srv := httptest.NewServer(feed)
	t.Cleanup(srv.Close)
	src, err := db.CreateEPGSource(ctx, store.EPGSource{Name: "fx475-remap", URL: srv.URL, Enabled: true, Priority: 0})
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, status, etag, lastModified, err := epg.Ingest(ctx, db, src, nil)
	if err != nil || status != "ok" {
		t.Fatalf("initial remap ingest = status %q err %v", status, err)
	}
	persisted := fx475SourceByID(t, db, src.ID)
	if persisted.LastETag != etag || persisted.LastModified != lastModified {
		t.Fatalf("published validators = %q/%q, want %q/%q", persisted.LastETag, persisted.LastModified, etag, lastModified)
	}

	if _, err := db.UpdateChannel(ctx, ch.ID, store.ChannelUpdate{EpgChannelID: ptrString(newID)}); err != nil {
		t.Fatal(err)
	}
	var oldFuture, validatorCount int
	if err := db.Pool.QueryRow(ctx, `
		SELECT count(*) FROM epg_program
		 WHERE channel_id=$1 AND end_at > now() AND epg_source_id IS NOT NULL`, ch.ID).Scan(&oldFuture); err != nil {
		t.Fatal(err)
	}
	if err := db.Pool.QueryRow(ctx, `
		SELECT count(*) FROM epg_source
		 WHERE last_etag <> '' OR last_modified <> ''`).Scan(&validatorCount); err != nil {
		t.Fatal(err)
	}
	if oldFuture != 0 || validatorCount != 0 {
		t.Fatalf("after remap old future rows=%d nonempty validators=%d; want 0,0", oldFuture, validatorCount)
	}

	// Model a response normalized before the remap but attempting to publish
	// after it. The source lock orders the transactions and the mapping recheck
	// rejects both its stale rows and validators atomically.
	staleSourceID := src.ID
	if _, err := db.ReplaceEPGSourceSnapshot(ctx, src, []store.EPGProgram{{
		ChannelID: ch.ID, StartAt: base, EndAt: base.Add(time.Hour), Title: "Stale in-flight row", SourceHash: "fx475-stale-remap",
		EPGSourceID: &staleSourceID, SourceGeneration: 2,
	}}, []store.EPGSnapshotMapping{{ProviderChannelID: oldID, ChannelID: ch.ID}}, `"stale-etag"`, "stale-last-modified"); !errors.Is(err, store.ErrStaleEPGSnapshot) {
		t.Fatalf("stale post-remap snapshot error = %v; want typed stale snapshot", err)
	}
	if err := db.Pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE p.end_at > now()),
		       count(*) FILTER (WHERE s.last_etag <> '' OR s.last_modified <> '')
		  FROM epg_source s
		  LEFT JOIN epg_program p ON p.epg_source_id=s.id
		 WHERE s.id=$1`, src.ID).Scan(&oldFuture, &validatorCount); err != nil {
		t.Fatal(err)
	}
	if oldFuture != 0 || validatorCount != 0 {
		t.Fatalf("after rejected stale snapshot old future rows=%d nonempty validators=%d; want 0,0", oldFuture, validatorCount)
	}

	// Keep the provider ETag unchanged. The reset source state makes this an
	// unconditional request, so the same cached document is reprocessed under
	// the new channel mapping instead of being lost behind a 304.
	feed.set(fx475XML(newID, fx475Programme{base, base.Add(time.Hour), "New mapping row"}), `"remap-v1"`)
	feed.resetRequests()
	src = fx475SourceByID(t, db, src.ID)
	if _, _, _, status, _, _, err := epg.Ingest(ctx, db, src, nil); err != nil || status != "ok" {
		t.Fatalf("post-remap ingest = status %q err %v", status, err)
	}
	if request := feed.lastRequest(); request != [2]string{} {
		t.Fatalf("post-remap request validators = %q/%q, want unconditional", request[0], request[1])
	}
	if got := fx475CanonicalTitles(t, db, ch.ID); !slices.Equal(got, []string{"New mapping row"}) {
		t.Fatalf("post-remap canonical titles = %v", got)
	}

	bad304 := &fx475SnapshotFeed{force304: true}
	badServer := httptest.NewServer(bad304)
	t.Cleanup(badServer.Close)
	badSource, err := db.CreateEPGSource(ctx, store.EPGSource{Name: "fx475-unconditional-304", URL: badServer.URL, Enabled: true, Priority: 50})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, status, _, _, err := epg.Ingest(ctx, db, badSource, nil); err == nil || !strings.Contains(status, "unconditional") {
		t.Fatalf("unconditional 304 = status %q err %v; want explicit error", status, err)
	}
}

func TestIntegrationEPGCreateChannelInvalidatesPreviouslyUnmappedSnapshots(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Minute).Add(6 * 24 * time.Hour)
	tests := []struct {
		name         string
		providerID   string
		number       float64
		epgChannelID string
	}{
		{name: "explicit provider id", providerID: "fx475.create.explicit", number: 9871.1, epgChannelID: "fx475.create.explicit"},
		{name: "numeric provider id", providerID: "9871.2", number: 9871.2},
	}
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			feed := &fx475SnapshotFeed{lastModified: "Fri, 21 Aug 2026 12:00:00 GMT"}
			feed.set(fx475XML(tc.providerID,
				fx475Programme{base.Add(time.Duration(i) * time.Hour), base.Add(time.Duration(i+1) * time.Hour), "Newly mapped " + tc.name},
			), `"create-stable-`+fmt.Sprint(i)+`"`)
			server := httptest.NewServer(feed)
			t.Cleanup(server.Close)
			source, err := db.CreateEPGSource(ctx, store.EPGSource{
				Name: "fx475-create-" + fmt.Sprint(i), URL: server.URL, Enabled: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, _, _, status, _, _, err := epg.Ingest(ctx, db, source, nil); err != nil || status != "ok" {
				t.Fatalf("unmapped ingest = status %q err %v", status, err)
			}
			persisted := fx475SourceByID(t, db, source.ID)
			if persisted.SnapshotGeneration != 1 || persisted.LastETag == "" {
				t.Fatalf("unmapped snapshot state = generation %d etag %q", persisted.SnapshotGeneration, persisted.LastETag)
			}
			staleSnapshot := persisted
			lineupID, stationID := "fx475-create-lineup-"+fmt.Sprint(i), "fx475-create-station-"+fmt.Sprint(i)
			if _, err := db.Pool.Exec(ctx, `
				INSERT INTO sd_lineup (sd_lineup_id, name) VALUES ($1, $1)`, lineupID); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Pool.Exec(ctx, `
				INSERT INTO sd_station_state (sd_lineup_id, sd_station_id, last_md5)
				VALUES ($1, $2, 'cached-md5')`, lineupID, stationID); err != nil {
				t.Fatal(err)
			}

			channel, err := db.CreateChannel(ctx, store.Channel{
				Number: tc.number, Name: "FX475 Create " + tc.name,
				EpgChannelID: tc.epgChannelID, Enabled: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			persisted = fx475SourceByID(t, db, source.ID)
			if persisted.SnapshotGeneration != 2 || persisted.LastETag != "" || persisted.LastModified != "" {
				t.Fatalf("post-create source = generation %d validators %q/%q; want 2/empty",
					persisted.SnapshotGeneration, persisted.LastETag, persisted.LastModified)
			}
			if md5, err := db.GetSDStationMD5(ctx, lineupID, stationID); err != nil || md5 != "" {
				t.Fatalf("post-create SD MD5 = %q err %v; want empty", md5, err)
			}
			staleSourceID := staleSnapshot.ID
			if _, err := db.ReplaceEPGSourceSnapshot(ctx, staleSnapshot, []store.EPGProgram{{
				ChannelID: channel.ID, StartAt: base, EndAt: base.Add(time.Hour),
				Title: "Stale pre-create normalization", SourceHash: "xmltv:stale-create",
				EPGSourceID: &staleSourceID, SourceGeneration: staleSnapshot.SnapshotGeneration + 1,
			}}, []store.EPGSnapshotMapping{{ProviderChannelID: tc.providerID, ChannelID: channel.ID}}, `"stale-create"`, ""); !errors.Is(err, store.ErrStaleEPGSnapshot) {
				t.Fatalf("pre-create snapshot publish error = %v; want typed stale snapshot", err)
			}

			feed.resetRequests()
			if _, _, _, status, _, _, err := epg.Ingest(ctx, db, persisted, nil); err != nil || status != "ok" {
				t.Fatalf("post-create refetch = status %q err %v", status, err)
			}
			if request := feed.lastRequest(); request != [2]string{} {
				t.Fatalf("post-create request reused validators %q/%q", request[0], request[1])
			}
			if got := fx475CanonicalTitles(t, db, channel.ID); !slices.Equal(got, []string{"Newly mapped " + tc.name}) {
				t.Fatalf("post-create guide = %v", got)
			}
		})
	}
}

func TestIntegrationEPGDuplicateStartKeepsMaximumCoverage(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	base := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	const epgID = "fx475.duplicate-start"
	channelID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	channel, err := db.CreateChannel(ctx, store.Channel{
		ID: channelID, Number: 9871.3, Name: "FX475 duplicate start",
		EpgChannelID: epgID, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	feed := &fx475SnapshotFeed{lastModified: "Fri, 21 Aug 2026 12:00:00 GMT"}
	feed.set(fx475XML(epgID,
		fx475Programme{base, base.Add(5 * time.Minute), "Short"},
		fx475Programme{base, base.Add(2 * time.Hour), "Main Event"},
	), `"duplicate-start"`)
	server := httptest.NewServer(feed)
	t.Cleanup(server.Close)
	source, err := db.CreateEPGSource(ctx, store.EPGSource{Name: "fx475-duplicate-start", URL: server.URL, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, status, _, _, err := epg.Ingest(ctx, db, source, nil); err != nil || status != "ok" {
		t.Fatalf("duplicate-start ingest = status %q err %v", status, err)
	}
	var title string
	var end time.Time
	var count int
	if err := db.Pool.QueryRow(ctx, `
		SELECT min(title), max(end_at), count(*)
		  FROM epg_program WHERE channel_id=$1 AND epg_source_id=$2`, channel.ID, source.ID,
	).Scan(&title, &end, &count); err != nil {
		t.Fatal(err)
	}
	if title != "Main Event" || !end.Equal(base.Add(2*time.Hour)) || count != 1 {
		t.Fatalf("duplicate-start winner = title %q end %s count %d; want Main Event/%s/1",
			title, end, count, base.Add(2*time.Hour))
	}
}

func TestIntegrationEPGMappedSemanticErrorRetainsPriorSnapshotAndValidators(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Minute).Add(7 * 24 * time.Hour)
	const epgID = "fx475.semantic-invalid"
	channel, err := db.CreateChannel(ctx, store.Channel{
		Number: 9871.4, Name: "FX475 semantic invalid", EpgChannelID: epgID, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	feed := &fx475SnapshotFeed{lastModified: "Fri, 21 Aug 2026 12:00:00 GMT"}
	feed.set(fx475XML(epgID, fx475Programme{base, base.Add(time.Hour), "Last known valid"}), `"semantic-valid"`)
	server := httptest.NewServer(feed)
	t.Cleanup(server.Close)
	source, err := db.CreateEPGSource(ctx, store.EPGSource{Name: "fx475-semantic-invalid", URL: server.URL, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, status, _, _, err := epg.Ingest(ctx, db, source, nil); err != nil || status != "ok" {
		t.Fatalf("valid seed = status %q err %v", status, err)
	}
	seed := fx475SourceByID(t, db, source.ID)
	feed.set(fmt.Sprintf(`<?xml version="1.0"?><tv>
		<channel id="%s"><display-name>FX475</display-name></channel>
		<programme channel="%s" start="garbage" stop="%s"><title>Must not erase valid row</title></programme>
	</tv>`, epgID, epgID, fmtXMLTVTime(base.Add(2*time.Hour))), `"semantic-invalid"`)
	if _, _, _, status, _, _, err := epg.Ingest(ctx, db, seed, nil); err == nil ||
		!strings.Contains(status, "no valid mapped XMLTV programmes survived") ||
		!strings.Contains(status, "unparseable_start=1") {
		t.Fatalf("semantic-invalid ingest = status %q err %v; want fail-closed nothing-survived error", status, err)
	}
	after := fx475SourceByID(t, db, source.ID)
	if after.SnapshotGeneration != seed.SnapshotGeneration || after.LastETag != seed.LastETag || after.LastModified != seed.LastModified {
		t.Fatalf("semantic error mutated source: before generation/validators %d/%q/%q after %d/%q/%q",
			seed.SnapshotGeneration, seed.LastETag, seed.LastModified,
			after.SnapshotGeneration, after.LastETag, after.LastModified)
	}
	if got := fx475CanonicalTitles(t, db, channel.ID); !slices.Equal(got, []string{"Last known valid"}) {
		t.Fatalf("semantic error replaced prior snapshot: %v", got)
	}

	// A programme reference without its required channel declaration is also a
	// malformed representation, not an authoritative empty snapshot.
	feed.set(fmt.Sprintf(`<?xml version="1.0"?><tv>
		<programme channel="%s" start="%s" stop="%s"><title>Undeclared reference</title></programme>
	</tv>`, epgID, fmtXMLTVTime(base), fmtXMLTVTime(base.Add(time.Hour))), `"semantic-undeclared"`)
	if _, _, _, status, _, _, err := epg.Ingest(ctx, db, seed, nil); err == nil ||
		!strings.Contains(status, "no valid mapped XMLTV programmes survived") ||
		!strings.Contains(status, "undeclared_channel_ref=1") {
		t.Fatalf("undeclared-channel ingest = status %q err %v; want fail-closed nothing-survived error", status, err)
	}
	after = fx475SourceByID(t, db, source.ID)
	if after.SnapshotGeneration != seed.SnapshotGeneration || after.LastETag != seed.LastETag || after.LastModified != seed.LastModified {
		t.Fatalf("undeclared reference mutated source: before generation/validators %d/%q/%q after %d/%q/%q",
			seed.SnapshotGeneration, seed.LastETag, seed.LastModified,
			after.SnapshotGeneration, after.LastETag, after.LastModified)
	}
	if got := fx475CanonicalTitles(t, db, channel.ID); !slices.Equal(got, []string{"Last known valid"}) {
		t.Fatalf("undeclared reference replaced prior snapshot: %v", got)
	}

	// Blank declaration ids and blank programme references are skipped, and
	// with nothing else in the feed the nothing-survived guard rejects the
	// representation — a channel configured with a blank EPG mapping must
	// never turn malformed XMLTV into an authoritative snapshot.
	feed.set(fmt.Sprintf(`<?xml version="1.0"?><tv>
		<channel id=""><display-name>Malformed</display-name></channel>
		<programme channel="" start="%s" stop="%s"><title>Blank channel id</title></programme>
	</tv>`, fmtXMLTVTime(base), fmtXMLTVTime(base.Add(time.Hour))), `"semantic-empty-channel"`)
	if _, _, _, status, _, _, err := epg.Ingest(ctx, db, seed, nil); err == nil ||
		!strings.Contains(status, "no valid mapped XMLTV programmes survived") ||
		!strings.Contains(status, "empty_channel_id=1") ||
		!strings.Contains(status, "empty_channel_ref=1") {
		t.Fatalf("empty-channel ingest = status %q err %v; want fail-closed nothing-survived error", status, err)
	}
	after = fx475SourceByID(t, db, source.ID)
	if after.SnapshotGeneration != seed.SnapshotGeneration || after.LastETag != seed.LastETag || after.LastModified != seed.LastModified {
		t.Fatalf("empty channel id mutated source: before generation/validators %d/%q/%q after %d/%q/%q",
			seed.SnapshotGeneration, seed.LastETag, seed.LastModified,
			after.SnapshotGeneration, after.LastETag, after.LastModified)
	}
	if got := fx475CanonicalTitles(t, db, channel.ID); !slices.Equal(got, []string{"Last known valid"}) {
		t.Fatalf("empty channel id replaced prior snapshot: %v", got)
	}

	feed.set(fmt.Sprintf(`<?xml version="1.0"?><tv>
		<channel id="%s"><display-name>FX475</display-name></channel>
		<programme channel="%s" start="%s" stop="%s"><title>  </title></programme>
	</tv>`, epgID, epgID, fmtXMLTVTime(base), fmtXMLTVTime(base.Add(time.Hour))), `"semantic-empty-title"`)
	if _, _, _, status, _, _, err := epg.Ingest(ctx, db, seed, nil); err == nil ||
		!strings.Contains(status, "no valid mapped XMLTV programmes survived") ||
		!strings.Contains(status, "empty_title=1") {
		t.Fatalf("empty-title ingest = status %q err %v; want fail-closed nothing-survived error", status, err)
	}
	after = fx475SourceByID(t, db, source.ID)
	if after.SnapshotGeneration != seed.SnapshotGeneration || after.LastETag != seed.LastETag || after.LastModified != seed.LastModified {
		t.Fatalf("empty title mutated source: before generation/validators %d/%q/%q after %d/%q/%q",
			seed.SnapshotGeneration, seed.LastETag, seed.LastModified,
			after.SnapshotGeneration, after.LastETag, after.LastModified)
	}
	if got := fx475CanonicalTitles(t, db, channel.ID); !slices.Equal(got, []string{"Last known valid"}) {
		t.Fatalf("empty title replaced prior snapshot: %v", got)
	}
}

// TestIntegrationEPGSkipsMalformedRowsAndPublishesValidOnes is the tolerance
// half of the contract: single junk rows (the 2026-08-29 outage class —
// one zero-duration programme dark-ing iptv-epg US for 41h, one empty channel
// id dark-ing the iBoost XMLTV) are skipped and tallied while every valid row
// still publishes.
func TestIntegrationEPGSkipsMalformedRowsAndPublishesValidOnes(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Minute).Add(7 * 24 * time.Hour)
	const epgID = "fx475.row-tolerance"
	channel, err := db.CreateChannel(ctx, store.Channel{
		Number: 9871.6, Name: "FX475 row tolerance", EpgChannelID: epgID, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	feed := &fx475SnapshotFeed{lastModified: "Fri, 21 Aug 2026 12:00:00 GMT"}
	// Two valid programmes surrounded by junk: an empty-id declaration, a
	// zero-duration programme, and a garbage-start programme.
	feed.set(fmt.Sprintf(`<?xml version="1.0"?><tv>
		<channel id=""><display-name>Malformed</display-name></channel>
		<channel id="%s"><display-name>FX475</display-name></channel>
		<programme channel="%s" start="%s" stop="%s"><title>Valid one</title></programme>
		<programme channel="%s" start="%s" stop="%s"><title>Zero duration</title></programme>
		<programme channel="%s" start="garbage" stop="%s"><title>Garbage start</title></programme>
		<programme channel="%s" start="%s" stop="%s"><title>Valid two</title></programme>
	</tv>`,
		epgID,
		epgID, fmtXMLTVTime(base), fmtXMLTVTime(base.Add(time.Hour)),
		epgID, fmtXMLTVTime(base.Add(time.Hour)), fmtXMLTVTime(base.Add(time.Hour)),
		epgID, fmtXMLTVTime(base.Add(2*time.Hour)),
		epgID, fmtXMLTVTime(base.Add(2*time.Hour)), fmtXMLTVTime(base.Add(3*time.Hour)),
	), `"row-tolerance-v1"`)
	server := httptest.NewServer(feed)
	t.Cleanup(server.Close)
	source, err := db.CreateEPGSource(ctx, store.EPGSource{Name: "fx475-row-tolerance", URL: server.URL, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	added, _, _, status, _, _, err := epg.Ingest(ctx, db, source, nil)
	if err != nil {
		t.Fatalf("row-tolerant ingest failed: status %q err %v", status, err)
	}
	if added != 2 {
		t.Fatalf("expected 2 valid programmes published, got %d (status %q)", added, status)
	}
	for _, want := range []string{"ok (skipped 3 malformed", "empty_channel_id=1", "non_positive_interval=1", "unparseable_start=1"} {
		if !strings.Contains(status, want) {
			t.Fatalf("status %q missing %q", status, want)
		}
	}
	if got := fx475CanonicalTitles(t, db, channel.ID); !slices.Equal(got, []string{"Valid one", "Valid two"}) {
		t.Fatalf("published titles = %v; want the two valid rows", got)
	}
}

func TestIntegrationEPGAmbiguousChannelMappingRetainsPriorSnapshotAndValidators(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Minute).Add(7 * 24 * time.Hour)
	const epgID = "9871.7"
	primary, err := db.CreateChannel(ctx, store.Channel{
		Number: 9871.8, Name: "Explicit EPG mapping", EpgChannelID: epgID, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	feed := &fx475SnapshotFeed{lastModified: "Fri, 21 Aug 2026 12:00:00 GMT"}
	feed.set(fx475XML(epgID, fx475Programme{base, base.Add(time.Hour), "Last known unambiguous"}), `"ambiguous-valid"`)
	server := httptest.NewServer(feed)
	t.Cleanup(server.Close)
	source, err := db.CreateEPGSource(ctx, store.EPGSource{Name: "fx475-ambiguous", URL: server.URL, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, status, _, _, err := epg.Ingest(ctx, db, source, nil); err != nil || status != "ok" {
		t.Fatalf("valid seed = status %q err %v", status, err)
	}
	seed := fx475SourceByID(t, db, source.ID)

	// Model a pre-existing/admin-SQL explicit-ID ↔ numeric fallback collision.
	// It remains representable even though duplicate explicit IDs are now
	// rejected by the schema.
	if _, err := db.Pool.Exec(ctx, `
		INSERT INTO channel (number, name, enabled)
		VALUES (9871.7, 'Numeric fallback collision', true)`); err != nil {
		t.Fatal(err)
	}
	feed.set(fx475XML(epgID, fx475Programme{base, base.Add(time.Hour), "Must not route arbitrarily"}), `"ambiguous-next"`)
	if _, _, _, status, _, _, err := epg.Ingest(ctx, db, seed, nil); !errors.Is(err, store.ErrAmbiguousEPGChannel) {
		t.Fatalf("ambiguous ingest = status %q err %v; want typed fail-closed mapping error", status, err)
	}
	after := fx475SourceByID(t, db, source.ID)
	if after.SnapshotGeneration != seed.SnapshotGeneration || after.LastETag != seed.LastETag || after.LastModified != seed.LastModified {
		t.Fatalf("ambiguous mapping mutated source: before generation/validators %d/%q/%q after %d/%q/%q",
			seed.SnapshotGeneration, seed.LastETag, seed.LastModified,
			after.SnapshotGeneration, after.LastETag, after.LastModified)
	}
	if got := fx475CanonicalTitles(t, db, primary.ID); !slices.Equal(got, []string{"Last known unambiguous"}) {
		t.Fatalf("ambiguous mapping replaced prior snapshot: %v", got)
	}
}

func TestIntegrationEPGRejectsDistinctProviderIDsClaimingOneChannel(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Minute).Add(7 * 24 * time.Hour)
	const explicitID = "fx475.explicit-channel-claim"
	channel, err := db.CreateChannel(ctx, store.Channel{
		Number: 9871.9, Name: "FX475 provider channel claim", EpgChannelID: explicitID, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	feed := &fx475SnapshotFeed{lastModified: "Fri, 21 Aug 2026 12:00:00 GMT"}
	feed.set(fx475XML(explicitID, fx475Programme{base, base.Add(time.Hour), "Last known single claim"}), `"single-claim-valid"`)
	server := httptest.NewServer(feed)
	t.Cleanup(server.Close)
	source, err := db.CreateEPGSource(ctx, store.EPGSource{Name: "fx475-provider-channel-claim", URL: server.URL, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, status, _, _, err := epg.Ingest(ctx, db, source, nil); err != nil || status != "ok" {
		t.Fatalf("valid seed = status %q err %v", status, err)
	}
	seed := fx475SourceByID(t, db, source.ID)

	feed.set(fmt.Sprintf(`<?xml version="1.0"?><tv>
		<channel id="%s"><display-name>Explicit claim</display-name></channel>
		<channel id="9871.9"><display-name>Numeric claim</display-name></channel>
		<programme channel="%s" start="%s" stop="%s"><title>Explicit programme</title></programme>
		<programme channel="9871.9" start="%s" stop="%s"><title>Numeric programme</title></programme>
	</tv>`, explicitID, explicitID, fmtXMLTVTime(base), fmtXMLTVTime(base.Add(time.Hour)),
		fmtXMLTVTime(base), fmtXMLTVTime(base.Add(time.Hour))), `"duplicate-channel-claim"`)
	if _, _, _, status, _, _, err := epg.Ingest(ctx, db, seed, nil); err == nil ||
		!strings.Contains(status, "map to the same Conductor channel") {
		t.Fatalf("duplicate provider-channel claim ingest = status %q err %v; want fail-closed claim error", status, err)
	}
	after := fx475SourceByID(t, db, source.ID)
	if after.SnapshotGeneration != seed.SnapshotGeneration || after.LastETag != seed.LastETag || after.LastModified != seed.LastModified {
		t.Fatalf("duplicate provider-channel claim mutated source: before generation/validators %d/%q/%q after %d/%q/%q",
			seed.SnapshotGeneration, seed.LastETag, seed.LastModified,
			after.SnapshotGeneration, after.LastETag, after.LastModified)
	}
	if got := fx475CanonicalTitles(t, db, channel.ID); !slices.Equal(got, []string{"Last known single claim"}) {
		t.Fatalf("duplicate provider-channel claim replaced prior snapshot: %v", got)
	}
}

func TestIntegrationEPGCanonicalWinnerKeepsExactSlotEpisodeSupplement(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Minute).Add(5 * 24 * time.Hour)
	const epgID = "fx475.episode.example"
	ch, err := db.CreateChannel(ctx, store.Channel{
		Number: 9475.6, Name: "FX475 Episode", CallSign: "FX475E",
		EpgChannelID: epgID, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	feed := &fx475SnapshotFeed{lastModified: time.Now().UTC().Format(http.TimeFormat)}
	body := fx475XML(epgID, fx475Programme{base, base.Add(time.Hour), "Preferred XMLTV title"})
	feed.set(body, `"episode-v1"`)
	srv := httptest.NewServer(feed)
	t.Cleanup(srv.Close)
	src, err := db.CreateEPGSource(ctx, store.EPGSource{Name: "fx475-episode", URL: srv.URL, Enabled: true, Priority: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, _, _, err := epg.Ingest(ctx, db, src, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool.Exec(ctx, `
		INSERT INTO sd_lineup (sd_lineup_id, name) VALUES ('fx475-lineup', 'FX475 episode lineup')`); err != nil {
		t.Fatal(err)
	}
	sdProgram := store.EPGProgram{
		ChannelID:          ch.ID,
		StartAt:            base,
		EndAt:              base.Add(time.Hour),
		Title:              "Preferred XMLTV title",
		EpisodeNumXMLTV:    "5.17.",
		EpisodeNumOnscreen: "S06E18",
		SourceHash:         "sd:fx475-supplement-s06e18",
		SourcePriority:     store.PrioritySD,
	}
	if _, err := db.ReplaceSDStationSnapshot(ctx, "fx475-lineup", "fx475-station", "md5-s06e18", ch.ID, []store.EPGProgram{sdProgram}); err != nil {
		t.Fatal(err)
	}
	fx475AssertCanonicalEpisode(t, db, ch.ID, "Preferred XMLTV title", "5.17.", "S06E18")
	var xmltvSupplemented, onscreenSupplemented bool
	if err := db.Pool.QueryRow(ctx, `
		SELECT episode_num_xmltv_supplemented, episode_num_onscreen_supplemented
		  FROM epg_program WHERE channel_id=$1 AND is_canonical`, ch.ID,
	).Scan(&xmltvSupplemented, &onscreenSupplemented); err != nil {
		t.Fatal(err)
	}
	if !xmltvSupplemented || !onscreenSupplemented {
		t.Fatalf("canonical supplement provenance = xmltv %v onscreen %v; want true/true", xmltvSupplemented, onscreenSupplemented)
	}
	beforeNoop, err := db.LatestProgramUpdate(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Reprocessing the source's still-empty episode fields must not erase the
	// exact-slot supplement or churn the externally-visible guide revision.
	feed.set(body, `"episode-v2"`)
	src = fx475SourceByID(t, db, src.ID)
	if _, _, _, _, _, _, err := epg.Ingest(ctx, db, src, nil); err != nil {
		t.Fatal(err)
	}
	fx475AssertCanonicalEpisode(t, db, ch.ID, "Preferred XMLTV title", "5.17.", "S06E18")
	afterNoop, err := db.LatestProgramUpdate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !afterNoop.Equal(beforeNoop) {
		t.Fatalf("unchanged supplementation churned revision %s -> %s", beforeNoop, afterNoop)
	}

	oldPending := fx475PendingEnrichmentForChannel(t, db, ch.ID)
	sdProgram.EpisodeNumXMLTV = "5.18."
	sdProgram.EpisodeNumOnscreen = "S06E19"
	sdProgram.SourceHash = "sd:fx475-supplement-s06e19"
	if _, err := db.ReplaceSDStationSnapshot(ctx, "fx475-lineup", "fx475-station", "md5-s06e19", ch.ID, []store.EPGProgram{sdProgram}); err != nil {
		t.Fatal(err)
	}
	fx475AssertCanonicalEpisode(t, db, ch.ID, "Preferred XMLTV title", "5.18.", "S06E19")

	// Work fetched for S06E18 cannot succeed, fail, or defer the corrected
	// S06E19 row even though the XMLTV winner's own source hash is unchanged.
	if err := db.UpsertEnrichmentResultGuarded(ctx, oldPending.ID, oldPending.EnrichmentInputKey, store.EnrichmentResult{
		TMDbID: 618, PosterURL: "https://invalid.example/stale-s06e18.jpg",
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.MarkEnrichmentFailedGuarded(ctx, oldPending.ID, oldPending.EnrichmentInputKey); err != nil {
		t.Fatal(err)
	}
	if err := db.DeferEnrichmentGuarded(ctx, oldPending.ID, oldPending.EnrichmentInputKey, time.Hour); err != nil {
		t.Fatal(err)
	}
	var status string
	var retryReady bool
	var enrichmentRows int
	if err := db.Pool.QueryRow(ctx, `
		SELECT p.enrichment_status::text,
		       p.enrichment_retry_at = '-infinity'::timestamptz,
		       count(e.program_id)
		  FROM epg_program p
		  LEFT JOIN epg_program_enrichment e ON e.program_id=p.id
		 WHERE p.id=$1
		 GROUP BY p.id`, oldPending.ID).Scan(&status, &retryReady, &enrichmentRows); err != nil {
		t.Fatal(err)
	}
	if status != "pending" || !retryReady || enrichmentRows != 0 {
		t.Fatalf("stale enrichment mutated correction: status %q ready %v rows %d", status, retryReady, enrichmentRows)
	}
	newPending := fx475PendingEnrichmentForChannel(t, db, ch.ID)
	if newPending.EnrichmentInputKey == oldPending.EnrichmentInputKey || newPending.EpisodeNumOnscreen != "S06E19" {
		t.Fatalf("corrected enrichment work = key %q episode %q; old key %q", newPending.EnrichmentInputKey, newPending.EpisodeNumOnscreen, oldPending.EnrichmentInputKey)
	}
	if err := db.UpsertEnrichmentResultGuarded(ctx, newPending.ID, newPending.EnrichmentInputKey, store.EnrichmentResult{
		TMDbID: 619, PosterURL: "https://example.test/s06e19.jpg",
	}); err != nil {
		t.Fatal(err)
	}

	// Withdrawing the current SD snapshot must clear the derived identity and
	// immediately requeue a previously matched winner.
	if _, err := db.ReplaceSDStationSnapshot(ctx, "fx475-lineup", "fx475-station", "md5-empty", ch.ID, nil); err != nil {
		t.Fatal(err)
	}
	fx475AssertCanonicalEpisode(t, db, ch.ID, "Preferred XMLTV title", "", "")
	if pending := fx475PendingEnrichmentForChannel(t, db, ch.ID); pending.EpisodeNumOnscreen != "" {
		t.Fatalf("withdrawn supplement pending episode = %q, want empty", pending.EpisodeNumOnscreen)
	}

	// Exact wall-clock alignment is not enough to identify a programme. A
	// same-start row for a different title/movie must never donate its episode
	// number to the canonical winner.
	sdProgram.Title = "Unrelated same-start movie"
	sdProgram.IsMovie = true
	sdProgram.EpisodeNumXMLTV = "8.8."
	sdProgram.EpisodeNumOnscreen = "S09E09"
	sdProgram.SourceHash = "sd:fx475-unrelated-same-start"
	if _, err := db.ReplaceSDStationSnapshot(ctx, "fx475-lineup", "fx475-station", "md5-unrelated", ch.ID, []store.EPGProgram{sdProgram}); err != nil {
		t.Fatal(err)
	}
	fx475AssertCanonicalEpisode(t, db, ch.ID, "Preferred XMLTV title", "", "")
	if _, err := db.ReplaceSDStationSnapshot(ctx, "fx475-lineup", "fx475-station", "md5-empty-again", ch.ID, nil); err != nil {
		t.Fatal(err)
	}
	sdProgram.Title = "Preferred XMLTV title"
	sdProgram.IsMovie = false

	// Native winner metadata supersedes any later exact-slot supplement, and a
	// native correction also requeues enrichment rather than retaining a match
	// for the prior episode.
	src = fx475SourceByID(t, db, src.ID)
	sourceID := src.ID
	owned := store.EPGProgram{
		ChannelID: ch.ID, StartAt: base, EndAt: base.Add(time.Hour), Title: "Preferred XMLTV title",
		EpisodeNumXMLTV: "6.0.", EpisodeNumOnscreen: "S07E01", SourceHash: "xmltv:owned-s07e01",
		EPGSourceID: &sourceID, SourceGeneration: src.SnapshotGeneration + 1,
	}
	if _, err := db.ReplaceEPGSourceSnapshot(ctx, src, []store.EPGProgram{owned}, []store.EPGSnapshotMapping{{ProviderChannelID: epgID, ChannelID: ch.ID}}, `"owned-1"`, ""); err != nil {
		t.Fatal(err)
	}
	fx475AssertCanonicalEpisode(t, db, ch.ID, "Preferred XMLTV title", "6.0.", "S07E01")
	ownedPending := fx475PendingEnrichmentForChannel(t, db, ch.ID)
	if err := db.UpsertEnrichmentResultGuarded(ctx, ownedPending.ID, ownedPending.EnrichmentInputKey, store.EnrichmentResult{TMDbID: 701}); err != nil {
		t.Fatal(err)
	}
	src = fx475SourceByID(t, db, src.ID)
	owned.EpisodeNumXMLTV = "6.1."
	owned.EpisodeNumOnscreen = "S07E02"
	owned.SourceHash = "xmltv:owned-s07e02"
	if _, err := db.ReplaceEPGSourceSnapshot(ctx, src, []store.EPGProgram{owned}, []store.EPGSnapshotMapping{{ProviderChannelID: epgID, ChannelID: ch.ID}}, `"owned-2"`, ""); err != nil {
		t.Fatal(err)
	}
	fx475AssertCanonicalEpisode(t, db, ch.ID, "Preferred XMLTV title", "6.1.", "S07E02")
	if pending := fx475PendingEnrichmentForChannel(t, db, ch.ID); pending.EpisodeNumOnscreen != "S07E02" {
		t.Fatalf("native correction pending episode = %q", pending.EpisodeNumOnscreen)
	}
	sdProgram.EpisodeNumXMLTV = "9.9."
	sdProgram.EpisodeNumOnscreen = "S10E10"
	sdProgram.SourceHash = "sd:fx475-weaker-owned"
	if _, err := db.ReplaceSDStationSnapshot(ctx, "fx475-lineup", "fx475-station", "md5-weaker", ch.ID, []store.EPGProgram{sdProgram}); err != nil {
		t.Fatal(err)
	}
	fx475AssertCanonicalEpisode(t, db, ch.ID, "Preferred XMLTV title", "6.1.", "S07E02")
	if err := db.Pool.QueryRow(ctx, `
		SELECT episode_num_xmltv_supplemented, episode_num_onscreen_supplemented
		  FROM epg_program WHERE channel_id=$1 AND is_canonical`, ch.ID,
	).Scan(&xmltvSupplemented, &onscreenSupplemented); err != nil {
		t.Fatal(err)
	}
	if xmltvSupplemented || onscreenSupplemented {
		t.Fatalf("winner-owned provenance = xmltv %v onscreen %v; want false/false", xmltvSupplemented, onscreenSupplemented)
	}
}

func TestIntegrationEPGEpisodeSupplementRequiresCompatibleSingleDonor(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	base := time.Date(2026, 8, 30, 18, 0, 0, 0, time.UTC)
	airDate := time.Date(2026, 8, 29, 0, 0, 0, 0, time.UTC)
	conflictingAirDate := airDate.AddDate(0, 0, -7)
	const epgID = "fx475.episode-donor-identity"
	channel, err := db.CreateChannel(ctx, store.Channel{
		Number: 9872.1, Name: "FX475 episode donor identity",
		EpgChannelID: epgID, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	publish := func(name string, priority int, program store.EPGProgram) {
		t.Helper()
		source, err := db.CreateEPGSource(ctx, store.EPGSource{
			Name: "fx475-donor-" + name, URL: "https://" + name + ".invalid/guide.xml",
			Priority: priority, Enabled: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		program.ChannelID = channel.ID
		program.StartAt = base
		program.EndAt = base.Add(time.Hour)
		program.Title = "Shared programme"
		program.SourceHash = "xmltv:fx475-donor-" + name
		if _, err := db.ReplaceEPGSourceSnapshot(ctx, source, []store.EPGProgram{program}, []store.EPGSnapshotMapping{{
			ProviderChannelID: epgID, ChannelID: channel.ID,
		}}, `"`+name+`"`, ""); err != nil {
			t.Fatal(err)
		}
	}
	assertEpisode := func(wantXMLTV, wantOnscreen string, wantXMLTVSupplemented, wantOnscreenSupplemented bool) {
		t.Helper()
		var gotXMLTV, gotOnscreen string
		var gotXMLTVSupplemented, gotOnscreenSupplemented bool
		if err := db.Pool.QueryRow(ctx, `
			SELECT episode_num_xmltv, episode_num_onscreen,
			       episode_num_xmltv_supplemented, episode_num_onscreen_supplemented
			  FROM epg_program
			 WHERE channel_id=$1 AND is_canonical`, channel.ID,
		).Scan(&gotXMLTV, &gotOnscreen, &gotXMLTVSupplemented, &gotOnscreenSupplemented); err != nil {
			t.Fatal(err)
		}
		if gotXMLTV != wantXMLTV || gotOnscreen != wantOnscreen ||
			gotXMLTVSupplemented != wantXMLTVSupplemented || gotOnscreenSupplemented != wantOnscreenSupplemented {
			t.Fatalf("canonical episode = %q/%q supplemented %v/%v; want %q/%q supplemented %v/%v",
				gotXMLTV, gotOnscreen, gotXMLTVSupplemented, gotOnscreenSupplemented,
				wantXMLTV, wantOnscreen, wantXMLTVSupplemented, wantOnscreenSupplemented)
		}
	}

	publish("winner", 0, store.EPGProgram{
		SubTitle: "Part One", OriginalAirDate: &airDate,
	})
	publish("conflicting-subtitle", 1, store.EPGProgram{
		SubTitle: "Part Two", OriginalAirDate: &airDate,
		EpisodeNumXMLTV: "1.1.", EpisodeNumOnscreen: "S02E02",
	})
	publish("conflicting-air-date", 2, store.EPGProgram{
		SubTitle: "Part One", OriginalAirDate: &conflictingAirDate,
		EpisodeNumXMLTV: "2.2.", EpisodeNumOnscreen: "S03E03",
	})
	assertEpisode("", "", false, false)

	// Compatible candidates may supplement, but both numbering representations
	// must come from the single strongest donor. These deliberately split the
	// fields across two rows; the weaker row must not be spliced into the winner.
	publish("xmltv-only", 3, store.EPGProgram{
		SubTitle: "  part   one ", OriginalAirDate: &airDate,
		EpisodeNumXMLTV: "3.3.",
	})
	publish("onscreen-only", 4, store.EPGProgram{
		SubTitle: "Part One", OriginalAirDate: &airDate,
		EpisodeNumOnscreen: "S04E04",
	})
	assertEpisode("3.3.", "", true, false)
}

func TestIntegrationEPGEpisodeSupplementRejectsDonorContradictingOwnedField(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	base := time.Date(2026, 8, 30, 20, 0, 0, 0, time.UTC)
	const epgID = "fx475.episode-owned-identity"
	channel, err := db.CreateChannel(ctx, store.Channel{
		Number: 9872.2, Name: "FX475 owned episode identity",
		EpgChannelID: epgID, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	publish := func(name string, priority int, xmltvEpisode, onscreenEpisode string) {
		t.Helper()
		source, err := db.CreateEPGSource(ctx, store.EPGSource{
			Name: "fx475-owned-" + name, URL: "https://" + name + ".invalid/guide.xml",
			Priority: priority, Enabled: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		sourceID := source.ID
		program := store.EPGProgram{
			ChannelID: channel.ID, StartAt: base, EndAt: base.Add(time.Hour),
			Title: "Partially identified programme", SubTitle: "Episode identity",
			EpisodeNumXMLTV: xmltvEpisode, EpisodeNumOnscreen: onscreenEpisode,
			SourceHash:  "xmltv:fx475-owned-" + name,
			EPGSourceID: &sourceID, SourceGeneration: 1,
		}
		if _, err := db.ReplaceEPGSourceSnapshot(ctx, source, []store.EPGProgram{program}, []store.EPGSnapshotMapping{{
			ProviderChannelID: epgID, ChannelID: channel.ID,
		}}, `"`+name+`"`, ""); err != nil {
			t.Fatal(err)
		}
	}
	assertEpisode := func(wantOnscreen string, wantSupplemented bool) {
		t.Helper()
		var xmltvEpisode, onscreenEpisode string
		var xmltvSupplemented, onscreenSupplemented bool
		if err := db.Pool.QueryRow(ctx, `
			SELECT episode_num_xmltv, episode_num_onscreen,
			       episode_num_xmltv_supplemented, episode_num_onscreen_supplemented
			  FROM epg_program
			 WHERE channel_id=$1 AND is_canonical`, channel.ID,
		).Scan(&xmltvEpisode, &onscreenEpisode, &xmltvSupplemented, &onscreenSupplemented); err != nil {
			t.Fatal(err)
		}
		if xmltvEpisode != "0.0." || xmltvSupplemented ||
			onscreenEpisode != wantOnscreen || onscreenSupplemented != wantSupplemented {
			t.Fatalf("canonical episode = %q/%q supplemented %v/%v; want 0.0./%q false/%v",
				xmltvEpisode, onscreenEpisode, xmltvSupplemented, onscreenSupplemented,
				wantOnscreen, wantSupplemented)
		}
	}

	publish("winner", 0, "0.0.", "")
	publish("cross-form-only", 1, "", "S02E02")
	assertEpisode("", false)
	publish("contradicting", 2, "1.1.", "S02E02")
	assertEpisode("", false)
	publish("compatible", 3, "0.0.", "S01E01")
	assertEpisode("S01E01", true)

	// The inverse is equally unsafe: an XMLTV-only donor cannot supplement a
	// winner-owned onscreen identity without carrying the same native onscreen
	// representation.
	inverseEPGID := "fx475.episode-owned-identity-inverse"
	inverseChannel, err := db.CreateChannel(ctx, store.Channel{
		Number: 9872.3, Name: "FX475 inverse owned episode identity",
		EpgChannelID: inverseEPGID, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	publishInverse := func(name string, priority int, xmltvEpisode, onscreenEpisode string) {
		t.Helper()
		source, err := db.CreateEPGSource(ctx, store.EPGSource{
			Name: "fx475-owned-inverse-" + name, URL: "https://inverse-" + name + ".invalid/guide.xml",
			Priority: priority, Enabled: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		sourceID := source.ID
		program := store.EPGProgram{
			ChannelID: inverseChannel.ID, StartAt: base, EndAt: base.Add(time.Hour),
			Title: "Inverse partially identified programme", SubTitle: "Episode identity",
			EpisodeNumXMLTV: xmltvEpisode, EpisodeNumOnscreen: onscreenEpisode,
			SourceHash:  "xmltv:fx475-owned-inverse-" + name,
			EPGSourceID: &sourceID, SourceGeneration: 1,
		}
		if _, err := db.ReplaceEPGSourceSnapshot(ctx, source, []store.EPGProgram{program}, []store.EPGSnapshotMapping{{
			ProviderChannelID: inverseEPGID, ChannelID: inverseChannel.ID,
		}}, `"`+name+`"`, ""); err != nil {
			t.Fatal(err)
		}
	}
	publishInverse("winner", 0, "", "S01E01")
	publishInverse("cross-form-only", 1, "1.1.", "")
	var inverseXMLTV, inverseOnscreen string
	var inverseXMLTVSupplemented, inverseOnscreenSupplemented bool
	if err := db.Pool.QueryRow(ctx, `
		SELECT episode_num_xmltv, episode_num_onscreen,
		       episode_num_xmltv_supplemented, episode_num_onscreen_supplemented
		  FROM epg_program
		 WHERE channel_id=$1 AND is_canonical`, inverseChannel.ID,
	).Scan(&inverseXMLTV, &inverseOnscreen, &inverseXMLTVSupplemented, &inverseOnscreenSupplemented); err != nil {
		t.Fatal(err)
	}
	if inverseXMLTV != "" || inverseOnscreen != "S01E01" || inverseXMLTVSupplemented || inverseOnscreenSupplemented {
		t.Fatalf("inverse canonical episode = %q/%q supplemented %v/%v; want /S01E01 false/false",
			inverseXMLTV, inverseOnscreen, inverseXMLTVSupplemented, inverseOnscreenSupplemented)
	}
}

func TestIntegrationEPGDirectWinnerRefreshPreservesCurrentSupplement(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Minute).Add(8 * 24 * time.Hour)
	const epgID = "fx475.direct-winner-supplement"
	channel, err := db.CreateChannel(ctx, store.Channel{
		Number: 9871.6, Name: "FX475 direct supplement", EpgChannelID: epgID, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	direct := store.EPGProgram{
		ChannelID: channel.ID, StartAt: base, EndAt: base.Add(time.Hour),
		Title: "Shared programme", Description: "stable description",
		SourceHash: "sports:direct-supplement:v1", SourcePriority: store.PrioritySports,
	}
	if action, err := db.UpsertEPGProgram(ctx, direct); err != nil || action != "added" {
		t.Fatalf("direct winner seed = action %q err %v", action, err)
	}
	source, err := db.CreateEPGSource(ctx, store.EPGSource{
		Name: "fx475-direct-supplement-donor", URL: "http://donor.invalid", Priority: 10, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	sourceID := source.ID
	donor := store.EPGProgram{
		ChannelID: channel.ID, StartAt: base, EndAt: base.Add(time.Hour),
		Title: "Shared programme", EpisodeNumXMLTV: "0.0.", EpisodeNumOnscreen: "S01E01",
		SourceHash: "xmltv:direct-supplement-donor", EPGSourceID: &sourceID, SourceGeneration: 1,
	}
	if _, err := db.ReplaceEPGSourceSnapshot(ctx, source, []store.EPGProgram{donor}, []store.EPGSnapshotMapping{{ProviderChannelID: epgID, ChannelID: channel.ID}}, `"donor-v1"`, ""); err != nil {
		t.Fatal(err)
	}
	var programID uuid.UUID
	var episode, status string
	var supplemented bool
	if err := db.Pool.QueryRow(ctx, `
		SELECT id, episode_num_onscreen, episode_num_onscreen_supplemented, enrichment_status::text
		  FROM epg_program WHERE channel_id=$1 AND is_canonical`, channel.ID,
	).Scan(&programID, &episode, &supplemented, &status); err != nil {
		t.Fatal(err)
	}
	if episode != "S01E01" || !supplemented || status != "pending" {
		t.Fatalf("direct winner supplement = episode %q supplemented %v status %q", episode, supplemented, status)
	}
	pending := fx475PendingEnrichmentForChannel(t, db, channel.ID)
	if err := db.UpsertEnrichmentResultGuarded(ctx, pending.ID, pending.EnrichmentInputKey, store.EnrichmentResult{TMDbID: 101}); err != nil {
		t.Fatal(err)
	}

	// A rating-only owner refresh is accepted and changes output metadata, but
	// its still-empty native episode must retain the current donor provenance and
	// must not hide a valid matched enrichment through clear/re-add churn.
	direct.Rating = "TV-14"
	direct.SourceHash = "sports:direct-supplement:v2"
	if action, err := db.UpsertEPGProgram(ctx, direct); err != nil || action != "updated" {
		t.Fatalf("direct winner refresh = action %q err %v", action, err)
	}
	if err := db.Pool.QueryRow(ctx, `
		SELECT episode_num_onscreen, episode_num_onscreen_supplemented, enrichment_status::text
		  FROM epg_program WHERE id=$1`, programID,
	).Scan(&episode, &supplemented, &status); err != nil {
		t.Fatal(err)
	}
	if episode != "S01E01" || !supplemented || status != "matched" {
		t.Fatalf("post-refresh supplement = episode %q supplemented %v status %q; want S01E01/true/matched",
			episode, supplemented, status)
	}

	// Once the retained donor snapshot withdraws the row, canonicalization can
	// prove the derived field is stale and clears/requeues it.
	source = fx475SourceByID(t, db, source.ID)
	if _, err := db.ReplaceEPGSourceSnapshot(ctx, source, nil, []store.EPGSnapshotMapping{{ProviderChannelID: epgID, ChannelID: channel.ID}}, `"donor-empty"`, ""); err != nil {
		t.Fatal(err)
	}
	if err := db.Pool.QueryRow(ctx, `
		SELECT episode_num_onscreen, episode_num_onscreen_supplemented, enrichment_status::text
		  FROM epg_program WHERE id=$1`, programID,
	).Scan(&episode, &supplemented, &status); err != nil {
		t.Fatal(err)
	}
	if episode != "" || supplemented || status != "pending" {
		t.Fatalf("post-withdraw supplement = episode %q supplemented %v status %q; want empty/false/pending",
			episode, supplemented, status)
	}
}

func TestIntegrationEPGRejectsPartialAndUnexpectedSuccessfulHTTPResponses(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Minute).Add(24 * time.Hour)
	const epgID = "fx475.http-status.example"
	if _, err := db.CreateChannel(ctx, store.Channel{
		Number: 9475.61, Name: "FX475 HTTP status", EpgChannelID: epgID, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	feed := &fx475SnapshotFeed{lastModified: "Fri, 21 Aug 2026 12:00:00 GMT"}
	feed.set(fx475XML(epgID, fx475Programme{base, base.Add(time.Hour), "Accepted complete snapshot"}), `"accepted"`)
	server := httptest.NewServer(feed)
	t.Cleanup(server.Close)
	src, err := db.CreateEPGSource(ctx, store.EPGSource{
		Name: "fx475-http-status", URL: server.URL, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, _, _, err := epg.Ingest(ctx, db, src, nil); err != nil {
		t.Fatal(err)
	}
	src = fx475SourceByID(t, db, src.ID)

	for _, statusCode := range []int{http.StatusPartialContent, http.StatusNoContent} {
		feed.setStatus(fx475XML(epgID), fmt.Sprintf(`"rejected-%d"`, statusCode), statusCode)
		if _, _, _, status, _, _, err := epg.Ingest(ctx, db, src, nil); err == nil ||
			!strings.Contains(status, fmt.Sprintf("HTTP %d", statusCode)) {
			t.Fatalf("HTTP %d ingest = status %q err %v; want rejection", statusCode, status, err)
		}
		fx475AssertSourceState(t, db, src.ID, 1, 1, 1)
		persisted := fx475SourceByID(t, db, src.ID)
		if persisted.LastETag != `"accepted"` {
			t.Fatalf("HTTP %d replaced validator with %q", statusCode, persisted.LastETag)
		}
	}
}

func TestIntegrationEPGOlderResponseArrivingLastIsFenced(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Minute).Add(25 * time.Hour)
	const epgID = "fx475.concurrent.example"
	ch, err := db.CreateChannel(ctx, store.Channel{
		Number: 9475.62, Name: "FX475 concurrent", EpgChannelID: epgID, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var releaseFirstOnce sync.Once
	releaseOlder := func() { releaseFirstOnce.Do(func() { close(releaseFirst) }) }
	defer releaseOlder()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		request := requests.Add(1)
		if request == 1 {
			close(firstStarted)
			<-releaseFirst
			w.Header().Set("ETag", `"older"`)
			_, _ = w.Write([]byte(fx475XML(epgID,
				fx475Programme{base, base.Add(time.Hour), "Older response"})))
			return
		}
		w.Header().Set("ETag", `"newer"`)
		_, _ = w.Write([]byte(fx475XML(epgID,
			fx475Programme{base, base.Add(time.Hour), "Newer response"})))
	}))
	t.Cleanup(server.Close)
	src, err := db.CreateEPGSource(ctx, store.EPGSource{
		Name: "fx475-concurrent", URL: server.URL, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	olderErr := make(chan error, 1)
	go func() {
		_, _, _, _, _, _, err := epg.Ingest(ctx, db, src, nil)
		olderErr <- err
	}()
	<-firstStarted
	_, _, _, status, _, _, newerErr := epg.Ingest(ctx, db, src, nil)
	releaseOlder()
	if newerErr != nil || status != "ok" {
		t.Fatalf("newer response = status %q err %v", status, newerErr)
	}
	if err := <-olderErr; !errors.Is(err, store.ErrStaleEPGSnapshot) {
		t.Fatalf("older response error = %v; want typed stale response", err)
	} else {
		var stale *store.StaleEPGSnapshotError
		if !errors.As(err, &stale) || stale.SourceID != src.ID {
			t.Fatalf("stale response type = %#v", err)
		}
	}
	if got := fx475CanonicalTitles(t, db, ch.ID); !slices.Equal(got, []string{"Newer response"}) {
		t.Fatalf("winner after response inversion = %v", got)
	}
	persisted := fx475SourceByID(t, db, src.ID)
	if persisted.SnapshotGeneration != 1 || persisted.LastETag != `"newer"` {
		t.Fatalf("source after response inversion = generation %d etag %q", persisted.SnapshotGeneration, persisted.LastETag)
	}
}

func TestIntegrationEPGWorkerTreatsFencedResponseAsNeutral(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Minute).Add(25 * time.Hour)
	const epgID = "fx475.worker-stale.example"
	if _, err := db.CreateChannel(ctx, store.Channel{
		Number: 9475.621, Name: "FX475 stale worker", EpgChannelID: epgID, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseFirst) }) }
	defer release()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if requests.Add(1) == 1 {
			close(firstStarted)
			<-releaseFirst
			w.Header().Set("ETag", `"worker-older"`)
			_, _ = w.Write([]byte(fx475XML(epgID,
				fx475Programme{base, base.Add(time.Hour), "Worker older response"})))
			return
		}
		w.Header().Set("ETag", `"worker-newer"`)
		_, _ = w.Write([]byte(fx475XML(epgID,
			fx475Programme{base, base.Add(time.Hour), "Worker newer response"})))
	}))
	t.Cleanup(server.Close)
	src, err := db.CreateEPGSource(ctx, store.EPGSource{
		Name: "fx475-worker-stale", URL: server.URL, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool.Exec(ctx, `
		UPDATE epg_source
		   SET consecutive_failures=2, last_status='error: prior'
		 WHERE id=$1`, src.ID); err != nil {
		t.Fatal(err)
	}

	worker := epg.NewWorker(slog.New(slog.NewTextHandler(io.Discard, nil)), db, time.Hour)
	statsCh := make(chan epg.IngestStats, 1)
	go func() { statsCh <- worker.RunOnce(ctx) }()
	<-firstStarted
	if _, _, _, status, _, _, err := epg.Ingest(ctx, db, src, nil); err != nil || status != "ok" {
		release()
		t.Fatalf("newer direct response = status %q err %v", status, err)
	}
	release()
	stats := <-statsCh
	if stats.SourcesFailed != 0 || stats.SourcesNoChange != 1 {
		t.Fatalf("worker stale stats = %+v; want neutral no-change", stats)
	}
	persisted := fx475SourceByID(t, db, src.ID)
	if persisted.ConsecutiveFailures != 2 || persisted.LastStatus != "error: prior" {
		t.Fatalf("stale worker mutated current poll diagnostics: failures=%d status=%q",
			persisted.ConsecutiveFailures, persisted.LastStatus)
	}
}

func TestIntegrationEPGWorkerFencesOldFailureAfterNewerSuccess(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Minute).Add(25 * time.Hour)
	const epgID = "fx475.worker-failure-success.example"
	if _, err := db.CreateChannel(ctx, store.Channel{
		Number: 9461, Name: "FX475 failure inversion", EpgChannelID: epgID, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseFirst) }) }
	defer release()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if requests.Add(1) == 1 {
			close(firstStarted)
			<-releaseFirst
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte("old failed response"))
			return
		}
		w.Header().Set("ETag", `"newer-success"`)
		_, _ = w.Write([]byte(fx475XML(epgID,
			fx475Programme{base, base.Add(time.Hour), "Newer successful response"})))
	}))
	t.Cleanup(server.Close)
	src, err := db.CreateEPGSource(ctx, store.EPGSource{
		Name: "fx475-failure-success", URL: server.URL, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool.Exec(ctx, `
		UPDATE epg_source SET consecutive_failures=2, last_status='error: prior' WHERE id=$1`, src.ID); err != nil {
		t.Fatal(err)
	}
	worker := epg.NewWorker(slog.New(slog.NewTextHandler(io.Discard, nil)), db, time.Hour)
	olderStats := make(chan epg.IngestStats, 1)
	newerStats := make(chan epg.IngestStats, 1)
	go func() { olderStats <- worker.RunOnce(ctx) }()
	<-firstStarted
	go func() { newerStats <- worker.RunOnce(ctx) }()
	newer := <-newerStats
	if newer.SourcesOK != 1 || newer.SourcesFailed != 0 {
		release()
		t.Fatalf("newer success stats = %+v", newer)
	}
	release()
	older := <-olderStats
	if older.SourcesFailed != 0 || older.SourcesNoChange != 1 {
		t.Fatalf("older failure stats = %+v; want neutral superseded attempt", older)
	}
	persisted := fx475SourceByID(t, db, src.ID)
	if persisted.SnapshotGeneration != 1 || persisted.LastETag != `"newer-success"` ||
		persisted.ConsecutiveFailures != 0 || persisted.LastStatus != "ok" {
		t.Fatalf("newer health overwritten by old failure: generation=%d etag=%q failures=%d status=%q",
			persisted.SnapshotGeneration, persisted.LastETag,
			persisted.ConsecutiveFailures, persisted.LastStatus)
	}
}

func TestIntegrationEPGWorkerHighWaterFencesOldFailureAfterNewer304(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Minute).Add(25 * time.Hour)
	const epgID = "fx475.worker-failure-304.example"
	if _, err := db.CreateChannel(ctx, store.Channel{
		Number: 9462, Name: "FX475 304 inversion", EpgChannelID: epgID, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseFirst) }) }
	defer release()
	var phase atomic.Int32
	var concurrentRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if phase.Load() == 0 {
			w.Header().Set("ETag", `"stable-304"`)
			_, _ = w.Write([]byte(fx475XML(epgID,
				fx475Programme{base, base.Add(time.Hour), "Stable snapshot"})))
			return
		}
		if concurrentRequests.Add(1) == 1 {
			close(firstStarted)
			<-releaseFirst
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte("old failed response"))
			return
		}
		w.WriteHeader(http.StatusNotModified)
	}))
	t.Cleanup(server.Close)
	src, err := db.CreateEPGSource(ctx, store.EPGSource{
		Name: "fx475-failure-304", URL: server.URL, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, _, _, err := epg.Ingest(ctx, db, src, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool.Exec(ctx, `
		UPDATE epg_source SET consecutive_failures=2, last_status='error: prior' WHERE id=$1`, src.ID); err != nil {
		t.Fatal(err)
	}
	phase.Store(1)
	worker := epg.NewWorker(slog.New(slog.NewTextHandler(io.Discard, nil)), db, time.Hour)
	olderStats := make(chan epg.IngestStats, 1)
	newerStats := make(chan epg.IngestStats, 1)
	go func() { olderStats <- worker.RunOnce(ctx) }()
	<-firstStarted
	go func() { newerStats <- worker.RunOnce(ctx) }()
	newer := <-newerStats
	if newer.SourcesNoChange != 1 || newer.SourcesFailed != 0 {
		release()
		t.Fatalf("newer 304 stats = %+v", newer)
	}
	afterNewer := fx475SourceByID(t, db, src.ID)
	if afterNewer.LastFetchedAt == nil {
		release()
		t.Fatal("newer 304 did not publish its attempt high-water")
	}
	release()
	older := <-olderStats
	if older.SourcesFailed != 0 || older.SourcesNoChange != 1 {
		t.Fatalf("older failure stats = %+v; want neutral high-water rejection", older)
	}
	persisted := fx475SourceByID(t, db, src.ID)
	if persisted.ConsecutiveFailures != 0 || persisted.LastStatus != "no_change" ||
		persisted.LastFetchedAt == nil || !persisted.LastFetchedAt.Equal(*afterNewer.LastFetchedAt) {
		t.Fatalf("newer 304 health overwritten: failures=%d status=%q fetched=%v want %v",
			persisted.ConsecutiveFailures, persisted.LastStatus,
			persisted.LastFetchedAt, afterNewer.LastFetchedAt)
	}
}

func TestIntegrationEPGWorkerOrdersPollTransitionsWithAlerts(t *testing.T) {
	db := freshDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	firstRequestStarted := make(chan struct{})
	secondRequestStarted := make(chan struct{})
	releaseFirstResponse := make(chan struct{})
	releaseSecondResponse := make(chan struct{})
	var releaseFirstOnce, releaseSecondOnce sync.Once
	releaseFirst := func() { releaseFirstOnce.Do(func() { close(releaseFirstResponse) }) }
	releaseSecond := func() { releaseSecondOnce.Do(func() { close(releaseSecondResponse) }) }
	defer releaseFirst()
	defer releaseSecond()
	var requests atomic.Int32
	sourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		switch requests.Add(1) {
		case 1:
			close(firstRequestStarted)
			<-releaseFirstResponse
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte("older failure"))
		case 2:
			close(secondRequestStarted)
			<-releaseSecondResponse
			w.WriteHeader(http.StatusNotModified)
		default:
			w.WriteHeader(http.StatusNotModified)
		}
	}))
	t.Cleanup(sourceServer.Close)

	failingAlertStarted := make(chan struct{})
	releaseFailingAlert := make(chan struct{})
	var releaseAlertOnce sync.Once
	releaseAlert := func() { releaseAlertOnce.Do(func() { close(releaseFailingAlert) }) }
	defer releaseAlert()
	var eventsMu sync.Mutex
	var severities []alerts.Severity
	alertServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var event alerts.Event
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if event.Severity == alerts.SevError {
			close(failingAlertStarted)
			<-releaseFailingAlert
		}
		eventsMu.Lock()
		severities = append(severities, event.Severity)
		eventsMu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(alertServer.Close)

	src, err := db.CreateEPGSource(ctx, store.EPGSource{
		Name: "fx475-alert-order", URL: sourceServer.URL, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	// A cached generation lets the later request return 304, while a failure
	// streak of two makes the older request's 502 cross the alert threshold.
	if _, err := db.Pool.Exec(ctx, `
		UPDATE epg_source
		   SET snapshot_generation=1, last_etag='"alert-order"',
		       consecutive_failures=2, last_status='error: prior'
		 WHERE id=$1`, src.ID); err != nil {
		t.Fatal(err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	worker := epg.NewWorker(logger, db, time.Hour)
	worker.Alerts = alerts.New(alertServer.URL, "", logger)
	olderStats := make(chan epg.IngestStats, 1)
	newerStats := make(chan epg.IngestStats, 1)
	go func() { olderStats <- worker.RunOnce(ctx) }()
	<-firstRequestStarted
	go func() { newerStats <- worker.RunOnce(ctx) }()
	<-secondRequestStarted

	// Let the older failure commit its 2->3 transition and block inside alert
	// delivery. The newer 304 has a later attempt high-water, but must not commit
	// 3->0 or emit recovery until that failure alert is externally ordered.
	releaseFirst()
	<-failingAlertStarted
	releaseSecond()
	select {
	case stats := <-newerStats:
		t.Fatalf("newer poll completed while older transition alert was blocked: %+v", stats)
	case <-time.After(500 * time.Millisecond):
	}
	releaseAlert()

	older := <-olderStats
	newer := <-newerStats
	if older.SourcesFailed != 1 || newer.SourcesNoChange != 1 {
		t.Fatalf("ordered poll stats = older %+v newer %+v", older, newer)
	}
	persisted := fx475SourceByID(t, db, src.ID)
	if persisted.ConsecutiveFailures != 0 || persisted.LastStatus != "no_change" {
		t.Fatalf("ordered poll state = failures %d status %q", persisted.ConsecutiveFailures, persisted.LastStatus)
	}
	eventsMu.Lock()
	got := append([]alerts.Severity(nil), severities...)
	eventsMu.Unlock()
	if !slices.Equal(got, []alerts.Severity{alerts.SevError, alerts.SevInfo}) {
		t.Fatalf("poll alerts = %v; want failure then recovery", got)
	}
}

func TestIntegrationEPGWorkerFencesOldFailureAfterConfigEdit(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	const epgID = "fx475.worker-failure-config.example"
	if _, err := db.CreateChannel(ctx, store.Channel{
		Number: 9463, Name: "FX475 config inversion", EpgChannelID: epgID, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	requestStarted := make(chan struct{})
	releaseResponse := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseResponse) }) }
	defer release()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(requestStarted)
		<-releaseResponse
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte("old config failed"))
	}))
	t.Cleanup(server.Close)
	src, err := db.CreateEPGSource(ctx, store.EPGSource{
		Name: "fx475-failure-config", URL: server.URL,
		AuthHeader: "Authorization: old", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool.Exec(ctx, `
		UPDATE epg_source SET consecutive_failures=2, last_status='error: prior' WHERE id=$1`, src.ID); err != nil {
		t.Fatal(err)
	}
	worker := epg.NewWorker(slog.New(slog.NewTextHandler(io.Discard, nil)), db, time.Hour)
	statsCh := make(chan epg.IngestStats, 1)
	go func() { statsCh <- worker.RunOnce(ctx) }()
	<-requestStarted
	newAuth := "Authorization: new"
	if _, err := db.UpdateEPGSource(ctx, src.ID, store.EPGSourceUpdate{AuthHeader: &newAuth}); err != nil {
		release()
		t.Fatal(err)
	}
	release()
	stats := <-statsCh
	if stats.SourcesFailed != 0 || stats.SourcesNoChange != 1 {
		t.Fatalf("old-config failure stats = %+v; want neutral", stats)
	}
	persisted := fx475SourceByID(t, db, src.ID)
	if persisted.SnapshotGeneration != 1 || persisted.AuthHeader != newAuth ||
		persisted.ConsecutiveFailures != 0 || persisted.LastStatus != "error: prior" {
		t.Fatalf("old failure attributed to new config: generation=%d auth=%q failures=%d status=%q",
			persisted.SnapshotGeneration, persisted.AuthHeader,
			persisted.ConsecutiveFailures, persisted.LastStatus)
	}
}

func TestIntegrationEPGInFlightRemapFencesOldResponse(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Minute).Add(26 * time.Hour)
	const oldID = "fx475.inflight.old"
	const newID = "fx475.inflight.new"
	ch, err := db.CreateChannel(ctx, store.Channel{
		Number: 9475.63, Name: "FX475 in-flight remap", EpgChannelID: oldID, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	requestStarted := make(chan struct{})
	releaseResponse := make(chan struct{})
	var releaseResponseOnce sync.Once
	releaseOldResponse := func() { releaseResponseOnce.Do(func() { close(releaseResponse) }) }
	defer releaseOldResponse()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(requestStarted)
		<-releaseResponse
		w.Header().Set("ETag", `"old-config"`)
		_, _ = w.Write([]byte(fx475XML(oldID,
			fx475Programme{base, base.Add(time.Hour), "Old in-flight mapping"})))
	}))
	t.Cleanup(server.Close)
	src, err := db.CreateEPGSource(ctx, store.EPGSource{
		Name: "fx475-inflight-remap", URL: server.URL, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	ingestErr := make(chan error, 1)
	go func() {
		_, _, _, _, _, _, err := epg.Ingest(ctx, db, src, nil)
		ingestErr <- err
	}()
	<-requestStarted
	_, remapErr := db.UpdateChannel(ctx, ch.ID, store.ChannelUpdate{EpgChannelID: ptrString(newID)})
	releaseOldResponse()
	if remapErr != nil {
		t.Fatal(remapErr)
	}
	if err := <-ingestErr; !errors.Is(err, store.ErrStaleEPGSnapshot) {
		t.Fatalf("in-flight pre-remap response = %v; want typed stale", err)
	}
	var rows int
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM epg_program WHERE channel_id=$1`, ch.ID).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	persisted := fx475SourceByID(t, db, src.ID)
	if rows != 0 || persisted.SnapshotGeneration != 1 || persisted.LastETag != "" {
		t.Fatalf("post-remap state = rows %d generation %d etag %q", rows, persisted.SnapshotGeneration, persisted.LastETag)
	}
}

func TestIntegrationEPGNewSourceNonemptySnapshotBeforeRemapForcesRefetch(t *testing.T) {
	fx475TestNewSourceSnapshotBeforeRemap(t, false)
}

func TestIntegrationEPGNewSourceEmptySnapshotBeforeRemapForcesRefetch(t *testing.T) {
	fx475TestNewSourceSnapshotBeforeRemap(t, true)
}

func fx475TestNewSourceSnapshotBeforeRemap(t *testing.T, empty bool) {
	t.Helper()
	db := freshDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	base := time.Now().UTC().Truncate(time.Minute).Add(26 * time.Hour)
	suffix := "nonempty"
	channelNumber := 9475.631
	if empty {
		suffix = "empty"
		channelNumber = 9475.632
	}
	oldID := "fx475.source-lock." + suffix + ".old"
	newID := "fx475.source-lock." + suffix + ".new"
	ch, err := db.CreateChannel(ctx, store.Channel{
		Number: channelNumber, Name: "FX475 source-lock " + suffix, EpgChannelID: oldID, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	feed := &fx475SnapshotFeed{}
	feed.set(fx475XML(newID,
		fx475Programme{base, base.Add(time.Hour), "Unconditional new mapping " + suffix}),
		`"source-lock-new-`+suffix+`"`)
	server := httptest.NewServer(feed)
	t.Cleanup(server.Close)
	src, err := db.CreateEPGSource(ctx, store.EPGSource{
		Name: "fx475-source-lock-" + suffix, URL: server.URL, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Hold channel authority so replacement reaches its advisory-lock wait only
	// after taking the source row FOR UPDATE. Starting the remap at that point
	// reproduces the old cycle: remap held the channel row while waiting for the
	// source, then replacement acquired channel authority and waited for that row.
	blocker, err := db.Pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	const epgLockNamespace int32 = 0x45504731
	shard := int32(binary.BigEndian.Uint32(ch.ID[:4]))
	if _, err := blocker.Exec(ctx, `SELECT pg_advisory_lock($1, $2)`, epgLockNamespace, shard); err != nil {
		blocker.Release()
		t.Fatal(err)
	}
	var unblockOnce sync.Once
	unblock := func() {
		unblockOnce.Do(func() {
			unlockCtx, unlockCancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer unlockCancel()
			var unlocked bool
			_ = blocker.QueryRow(unlockCtx, `SELECT pg_advisory_unlock($1, $2)`, epgLockNamespace, shard).Scan(&unlocked)
			blocker.Release()
		})
	}
	defer unblock()

	waitForLockWaiter := func(advisory bool) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			var waiters int
			operator := "="
			if !advisory {
				operator = "<>"
			}
			query := fmt.Sprintf(`
				SELECT count(*)
				  FROM pg_stat_activity
				 WHERE datname=current_database()
				   AND pid <> pg_backend_pid()
				   AND wait_event_type='Lock'
				   AND lower(coalesce(wait_event, '')) %s 'advisory'`, operator)
			if err := db.Pool.QueryRow(ctx, query).Scan(&waiters); err != nil {
				t.Fatal(err)
			}
			if waiters > 0 {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for advisory=%v lock waiter", advisory)
	}

	var programs []store.EPGProgram
	if !empty {
		programs = []store.EPGProgram{{
			ChannelID: ch.ID, StartAt: base, EndAt: base.Add(time.Hour),
			Title: "Serialized old mapping", SourceHash: "xmltv:source-lock-remap-" + suffix,
		}}
	}
	replaceDone := make(chan error, 1)
	go func() {
		_, err := db.ReplaceEPGSourceSnapshot(ctx, src, programs,
			[]store.EPGSnapshotMapping{{ProviderChannelID: oldID, ChannelID: ch.ID}},
			`"source-lock-old-`+suffix+`"`, "source-lock-old-modified")
		replaceDone <- err
	}()
	waitForLockWaiter(true)

	remapDone := make(chan error, 1)
	go func() {
		_, err := db.UpdateChannel(ctx, ch.ID, store.ChannelUpdate{EpgChannelID: ptrString(newID)})
		remapDone <- err
	}()
	// The remap must now wait on replacement's source row without first owning
	// either the channel row or channel authority.
	waitForLockWaiter(false)
	unblock()

	select {
	case err := <-replaceDone:
		if err != nil {
			t.Fatalf("snapshot replacement failed instead of serializing first: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("snapshot replacement/remap lock order timed out")
	}
	select {
	case err := <-remapDone:
		if err != nil {
			t.Fatalf("remap failed after snapshot replacement: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("remap remained blocked after snapshot replacement")
	}

	var mapping string
	var rows int
	if err := db.Pool.QueryRow(ctx, `SELECT epg_channel_id FROM channel WHERE id=$1`, ch.ID).Scan(&mapping); err != nil {
		t.Fatal(err)
	}
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM epg_program WHERE channel_id=$1`, ch.ID).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	persisted := fx475SourceByID(t, db, src.ID)
	if mapping != newID || rows != 0 || persisted.SnapshotGeneration != 2 ||
		persisted.LastETag != "" || persisted.LastModified != "" {
		t.Fatalf("serialized remap left stale state: mapping=%q rows=%d generation=%d validators=%q/%q",
			mapping, rows, persisted.SnapshotGeneration, persisted.LastETag, persisted.LastModified)
	}

	// The remap must force an unconditional refetch even when the just-published
	// source snapshot was empty. Reusing the old validators could produce a 304
	// after the remap deleted its candidates and leave a permanent guide hole.
	feed.resetRequests()
	added, _, _, status, _, _, err := epg.Ingest(ctx, db, persisted, nil)
	if err != nil || status != "ok" || added != 1 {
		t.Fatalf("post-remap refetch = added %d status %q err %v", added, status, err)
	}
	request := feed.lastRequest()
	if request[0] != "" || request[1] != "" {
		t.Fatalf("post-remap refetch reused stale validators %q/%q", request[0], request[1])
	}
	if got := fx475CanonicalTitles(t, db, ch.ID); !slices.Equal(got, []string{"Unconditional new mapping " + suffix}) {
		t.Fatalf("post-remap guide = %v", got)
	}
	persisted = fx475SourceByID(t, db, src.ID)
	if persisted.SnapshotGeneration != 3 || persisted.LastETag != `"source-lock-new-`+suffix+`"` {
		t.Fatalf("post-remap source generation/etag = %d/%q", persisted.SnapshotGeneration, persisted.LastETag)
	}
}

func TestIntegrationEPGConcurrentSourceCreateFetchAndRemapCannotDeadlock(t *testing.T) {
	db := freshDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	base := time.Now().UTC().Truncate(time.Minute).Add(26 * time.Hour)
	const oldID = "fx475.create-remap.old"
	const newID = "fx475.create-remap.new"
	ch, err := db.CreateChannel(ctx, store.Channel{
		Number: 9464, Name: "FX475 create/remap", EpgChannelID: oldID, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Ensure the remap has at least one pre-existing source row to lock and
	// invalidate before it waits for channel authority.
	if _, err := db.CreateEPGSource(ctx, store.EPGSource{
		Name: "fx475-existing-before-remap", URL: "http://existing.invalid", Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}

	blocker, err := db.Pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	const epgLockNamespace int32 = 0x45504731
	shard := int32(binary.BigEndian.Uint32(ch.ID[:4]))
	if _, err := blocker.Exec(ctx, `SELECT pg_advisory_lock($1, $2)`, epgLockNamespace, shard); err != nil {
		blocker.Release()
		t.Fatal(err)
	}
	var unblockOnce sync.Once
	unblock := func() {
		unblockOnce.Do(func() {
			unlockCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			var unlocked bool
			_ = blocker.QueryRow(unlockCtx, `SELECT pg_advisory_unlock($1, $2)`, epgLockNamespace, shard).Scan(&unlocked)
			blocker.Release()
		})
	}
	defer unblock()

	remapDone := make(chan error, 1)
	go func() {
		_, err := db.UpdateChannel(ctx, ch.ID, store.ChannelUpdate{EpgChannelID: ptrString(newID)})
		remapDone <- err
	}()
	waitForAdvisoryWaiters := func(want int) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			var waiters int
			if err := db.Pool.QueryRow(ctx, `
				SELECT count(*)
				  FROM pg_stat_activity
				 WHERE datname=current_database()
				   AND wait_event_type='Lock'
				   AND lower(coalesce(wait_event, ''))='advisory'`).Scan(&waiters); err != nil {
				t.Fatal(err)
			}
			if waiters >= want {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for %d advisory-lock waiters", want)
	}
	waitForAdvisoryWaiters(1)

	// Source creation joins remap's source-set authority before opening its
	// insert transaction. It therefore cannot appear just after the locked
	// source scan and publish validators for the mapping being removed.
	type createResult struct {
		source store.EPGSource
		err    error
	}
	createDone := make(chan createResult, 1)
	go func() {
		created, err := db.CreateEPGSource(ctx, store.EPGSource{
			Name: "fx475-created-during-remap", URL: "http://created.invalid", Enabled: true,
		})
		createDone <- createResult{source: created, err: err}
	}()
	select {
	case result := <-createDone:
		t.Fatalf("source creation escaped remap authority: source=%+v err=%v", result.source, result.err)
	case <-time.After(150 * time.Millisecond):
	}
	unblock()

	select {
	case err := <-remapDone:
		if err != nil {
			t.Fatalf("remap failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("create/fetch/remap cycle deadlocked remap")
	}
	var created store.EPGSource
	select {
	case result := <-createDone:
		if result.err != nil {
			t.Fatalf("serialized source creation failed: %v", result.err)
		}
		created = result.source
	case <-time.After(5 * time.Second):
		t.Fatal("source creation remained blocked after remap")
	}
	_, err = db.ReplaceEPGSourceSnapshot(ctx, created, []store.EPGProgram{{
		ChannelID: ch.ID, StartAt: base, EndAt: base.Add(time.Hour),
		Title: "Created-source stale row", SourceHash: "xmltv:create-remap",
	}}, []store.EPGSnapshotMapping{{ProviderChannelID: oldID, ChannelID: ch.ID}},
		`"created-etag"`, "created-modified")
	if !errors.Is(err, store.ErrStaleEPGSnapshot) {
		t.Fatalf("post-remap source refresh error = %v; want typed stale mapping", err)
	}
	var rows int
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM epg_program WHERE channel_id=$1`, ch.ID).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 0 {
		t.Fatalf("created source published %d stale pre-remap rows", rows)
	}
	persisted := fx475SourceByID(t, db, created.ID)
	if persisted.SnapshotGeneration != 0 || persisted.LastETag != "" || persisted.LastModified != "" {
		t.Fatalf("serialized new source started stale: generation=%d validators=%q/%q",
			persisted.SnapshotGeneration, persisted.LastETag, persisted.LastModified)
	}
}

func TestIntegrationDeleteEPGSourcePromotesFallbackAndBumpsRevision(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Minute).Add(27 * time.Hour)
	const epgID = "fx475.delete.example"
	ch, err := db.CreateChannel(ctx, store.Channel{
		Number: 9475.64, Name: "FX475 source delete", EpgChannelID: epgID, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	makeSource := func(name, title string, priority int) store.EPGSource {
		t.Helper()
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(fx475XML(epgID,
				fx475Programme{base, base.Add(time.Hour), title})))
		}))
		t.Cleanup(server.Close)
		source, err := db.CreateEPGSource(ctx, store.EPGSource{
			Name: name, URL: server.URL, Priority: priority, Enabled: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, _, _, _, _, err := epg.Ingest(ctx, db, source, nil); err != nil {
			t.Fatal(err)
		}
		return source
	}
	low := makeSource("fx475-delete-fallback", "Retained fallback", 10)
	high := makeSource("fx475-delete-winner", "Deleted winner", 0)
	if got := fx475CanonicalTitles(t, db, ch.ID); !slices.Equal(got, []string{"Deleted winner"}) {
		t.Fatalf("winner before delete = %v", got)
	}
	before, err := db.LatestProgramUpdate(ctx)
	if err != nil {
		t.Fatal(err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	router := api.NewRouter(api.Deps{
		Logger: logger, DB: db,
		Device: hdhr.Device{DeviceID: "FX475DEL", BaseURL: "http://fx475.test", TunerCount: hdhr.StaticTunerCount(2)},
		Auth:   auth.Config{Enabled: true, AdminAPIKey: "fx475-test-key"},
	})
	request := httptest.NewRequest(http.MethodDelete, "/admin/epg/sources/"+high.ID.String(), nil)
	request.Header.Set("Authorization", "Bearer fx475-test-key")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("delete source response = %d %q", response.Code, response.Body.String())
	}
	if got := fx475CanonicalTitles(t, db, ch.ID); !slices.Equal(got, []string{"Retained fallback"}) {
		t.Fatalf("canonical guide after source delete = %v", got)
	}
	after, err := db.LatestProgramUpdate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !after.After(before) {
		t.Fatalf("guide revision did not advance on winner delete: before=%s after=%s", before, after)
	}
	var deletedSources, fallbackRows int
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM epg_source WHERE id=$1`, high.ID).Scan(&deletedSources); err != nil {
		t.Fatal(err)
	}
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM epg_program WHERE epg_source_id=$1`, low.ID).Scan(&fallbackRows); err != nil {
		t.Fatal(err)
	}
	if deletedSources != 0 || fallbackRows != 1 {
		t.Fatalf("delete cleanup = deleted source rows %d fallback candidates %d", deletedSources, fallbackRows)
	}

	request = httptest.NewRequest(http.MethodDelete, "/admin/epg/sources/"+high.ID.String(), nil)
	request.Header.Set("Authorization", "Bearer fx475-test-key")
	response = httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("second delete response = %d, want 404", response.Code)
	}
}

func TestIntegrationEPGOutputRejectsCanonicalOverlap(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Minute).Add(96 * time.Hour)
	ch, err := db.CreateChannel(ctx, store.Channel{
		Number: 9475.4, Name: "FX475 Health Gate", CallSign: "FX475H",
		EpgChannelID: "fx475.health.example", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool.Exec(ctx, `ALTER TABLE epg_program DROP CONSTRAINT epg_program_canonical_no_overlap`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.Pool.Exec(context.Background(), `DELETE FROM epg_program WHERE channel_id=$1`, ch.ID)
		_, _ = db.Pool.Exec(context.Background(), `
			ALTER TABLE epg_program
			ADD CONSTRAINT epg_program_canonical_no_overlap
			EXCLUDE USING gist (
				channel_id WITH =,
				tstzrange(start_at, end_at, '[)') WITH &&
			) WHERE (is_canonical)
			DEFERRABLE INITIALLY IMMEDIATE`)
	})
	for i, interval := range [][2]time.Time{
		{base, base.Add(time.Hour)},
		{base.Add(30 * time.Minute), base.Add(90 * time.Minute)},
	} {
		if _, err := db.Pool.Exec(ctx, `
			INSERT INTO epg_program (
				channel_id, start_at, end_at, title, source_hash,
				source_priority, is_canonical, is_legacy
			) VALUES ($1,$2,$3,$4,$5,0,true,false)`,
			ch.ID, interval[0], interval[1], fmt.Sprintf("Overlap %d", i), fmt.Sprintf("fx475-overlap-%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	out := epg.NewOutput(db, "http://fx475.test")
	rr := httptest.NewRecorder()
	out.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/xmltv.xml", nil))
	if rr.Code != http.StatusServiceUnavailable || rr.Header().Get("Retry-After") == "" {
		t.Fatalf("overlap output = status %d Retry-After %q body %q; want 503 with retry", rr.Code, rr.Header().Get("Retry-After"), rr.Body.String())
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	router := api.NewRouter(api.Deps{
		Logger: logger,
		DB:     db,
		Device: hdhr.Device{
			DeviceID:   "FX475001",
			BaseURL:    "http://fx475.test",
			TunerCount: hdhr.StaticTunerCount(2),
		},
		Auth: auth.Config{Enabled: true, AdminAPIKey: "fx475-test-key"},
	})
	ready := httptest.NewRecorder()
	router.ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if ready.Code != http.StatusServiceUnavailable ||
		!strings.HasPrefix(ready.Header().Get("Content-Type"), "application/json") ||
		!strings.Contains(ready.Body.String(), `"epg_canonical_overlaps":1`) {
		t.Fatalf("ready overlap gate = status %d body %q; want 503 with overlap count", ready.Code, ready.Body.String())
	}
	adminHealth := httptest.NewRecorder()
	adminRequest := httptest.NewRequest(http.MethodGet, "/admin/epg/health", nil)
	adminRequest.Header.Set("Authorization", "Bearer fx475-test-key")
	router.ServeHTTP(adminHealth, adminRequest)
	if adminHealth.Code != http.StatusServiceUnavailable ||
		!strings.Contains(adminHealth.Body.String(), `"canonical_healthy":false`) ||
		!strings.Contains(adminHealth.Body.String(), `"canonical_overlaps":1`) {
		t.Fatalf("admin overlap gate = status %d body %q; want 503 unhealthy", adminHealth.Code, adminHealth.Body.String())
	}
}

func fx475XML(channelID string, programmes ...fx475Programme) string {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?><tv>`)
	fmt.Fprintf(&b, `<channel id="%s"><display-name>FX475</display-name></channel>`, channelID)
	for _, programme := range programmes {
		fmt.Fprintf(&b, `<programme channel="%s" start="%s" stop="%s"><title>%s</title></programme>`,
			channelID, fmtXMLTVTime(programme.Start), fmtXMLTVTime(programme.End), programme.Title)
	}
	b.WriteString(`</tv>`)
	return b.String()
}

func fx475AssertSourceState(t *testing.T, db *store.DB, sourceID uuid.UUID, generation int64, rows, canonical int) {
	t.Helper()
	ctx := context.Background()
	var gotGeneration int64
	var appliedAt *time.Time
	if err := db.Pool.QueryRow(ctx, `
		SELECT snapshot_generation, snapshot_applied_at
		  FROM epg_source WHERE id=$1`, sourceID).Scan(&gotGeneration, &appliedAt); err != nil {
		t.Fatal(err)
	}
	var gotRows, gotCanonical int
	if err := db.Pool.QueryRow(ctx, `
		SELECT count(*), count(*) FILTER (WHERE is_canonical)
		  FROM epg_program WHERE epg_source_id=$1`, sourceID).Scan(&gotRows, &gotCanonical); err != nil {
		t.Fatal(err)
	}
	if gotGeneration != generation || appliedAt == nil || gotRows != rows || gotCanonical != canonical {
		t.Fatalf("source state = generation %d applied %v rows %d canonical %d; want %d nonnil %d %d",
			gotGeneration, appliedAt, gotRows, gotCanonical, generation, rows, canonical)
	}
}

func fx475CanonicalTitles(t *testing.T, db *store.DB, channelID uuid.UUID) []string {
	t.Helper()
	rows, err := db.Pool.Query(context.Background(), `
		SELECT title FROM epg_program
		 WHERE channel_id=$1 AND is_canonical
		 ORDER BY start_at, title`, channelID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var titles []string
	for rows.Next() {
		var title string
		if err := rows.Scan(&title); err != nil {
			t.Fatal(err)
		}
		titles = append(titles, title)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return titles
}

func fx475AssertCanonicalEpisode(t *testing.T, db *store.DB, channelID uuid.UUID, title, xmltv, onscreen string) {
	t.Helper()
	var gotTitle, gotXMLTV, gotOnscreen string
	var sourceID uuid.UUID
	if err := db.Pool.QueryRow(context.Background(), `
		SELECT title, episode_num_xmltv, episode_num_onscreen, epg_source_id
		  FROM epg_program
		 WHERE channel_id=$1 AND is_canonical`, channelID).Scan(&gotTitle, &gotXMLTV, &gotOnscreen, &sourceID); err != nil {
		t.Fatal(err)
	}
	if gotTitle != title || gotXMLTV != xmltv || gotOnscreen != onscreen || sourceID == uuid.Nil {
		t.Fatalf("canonical episode = title %q xmltv %q onscreen %q source %v; want %q/%q/%q/source-backed",
			gotTitle, gotXMLTV, gotOnscreen, sourceID, title, xmltv, onscreen)
	}
}

func fx475PendingEnrichmentForChannel(t *testing.T, db *store.DB, channelID uuid.UUID) store.PendingEnrichmentRow {
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

func fx475SourceByID(t *testing.T, db *store.DB, id uuid.UUID) store.EPGSource {
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

func ptrString(value string) *string { return &value }

// TestIntegrationEPGWorkerSettlesHealthOnSkippedRowPublishes pins the
// v0.47.1 regression: a publishing pass whose status is "ok (skipped …)"
// must have its snapshot-generation bump anticipated by the worker, or the
// poll-state update is fenced as superseded on EVERY pass and the source's
// failure streak freezes at its pre-recovery value (observed live on the
// iBoost XMLTV: no HTTP validators, republishes each pass, stuck at 15).
func TestIntegrationEPGWorkerSettlesHealthOnSkippedRowPublishes(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Minute).Add(7 * 24 * time.Hour)
	const epgID = "op570.skip-health"
	if _, err := db.CreateChannel(ctx, store.Channel{
		Number: 9871.9, Name: "FX570 skip health", EpgChannelID: epgID, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	// No ETag / Last-Modified: every pass re-downloads and republishes,
	// like the provider XMLTV. One junk row keeps the skip suffix present.
	body := fmt.Sprintf(`<?xml version="1.0"?><tv>
		<channel id=""><display-name>junk</display-name></channel>
		<channel id="%s"><display-name>FX570</display-name></channel>
		<programme channel="%s" start="%s" stop="%s"><title>Keeps publishing</title></programme>
	</tv>`, epgID, epgID, fmtXMLTVTime(base), fmtXMLTVTime(base.Add(time.Hour)))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	src, err := db.CreateEPGSource(ctx, store.EPGSource{Name: "op570-skip-health", URL: server.URL, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	// Seed a failure streak so settling is observable.
	for i := 0; i < 4; i++ {
		at, berr := db.BeginEPGSourcePollAttempt(ctx, src)
		if berr != nil {
			t.Fatal(berr)
		}
		if _, _, uerr := db.UpdateEPGSourcePollState(ctx, src, at, "error: seeded", false); uerr != nil {
			t.Fatal(uerr)
		}
		src = fx475SourceByID(t, db, src.ID)
	}

	w := epg.NewWorker(slog.New(slog.NewTextHandler(io.Discard, nil)), db, time.Hour)
	for pass := 1; pass <= 2; pass++ {
		w.RunOnce(ctx)
		after := fx475SourceByID(t, db, src.ID)
		if after.ConsecutiveFailures != 0 {
			t.Fatalf("pass %d: consecutive_failures=%d, want 0 (health update fenced?)", pass, after.ConsecutiveFailures)
		}
		if !strings.HasPrefix(after.LastStatus, "ok (skipped 1 malformed") {
			t.Fatalf("pass %d: last_status=%q, want ok-with-skips", pass, after.LastStatus)
		}
		if after.SnapshotGeneration != int64(pass) {
			t.Fatalf("pass %d: snapshot_generation=%d, want %d", pass, after.SnapshotGeneration, pass)
		}
	}
}
