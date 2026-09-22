package epg

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/ppvparse"
	"github.com/spencercnorton/conductor/internal/store"
)

// TestEmitProgramme_EnrichmentEmitsImageAndIcon covers the 2026-05-08
// fix that gets posters showing in Plex DVR. Tunarr's working approach
// emits BOTH <image size="3" type="poster">URL</image> AND
// <icon src="URL"/> — Plex DVR's parser reads the newer <image> element
// (introduced in xmltv 1.61) but ignores programme-level <icon>. We
// emit both so any other XMLTV consumer also gets art.
func TestEmitProgramme_EnrichmentEmitsImageAndIcon(t *testing.T) {
	row := store.ProgramOutputRow{
		ChannelID:             uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		ChannelName:           "Test Channel",
		ChannelNumber:         1.0,
		Title:                 "The Italian Job",
		StartAt:               time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC),
		EndAt:                 time.Date(2026, 5, 8, 14, 30, 0, 0, time.UTC),
		EnrichmentPosterURL:   "https://image.tmdb.org/t/p/w500/poster.jpg",
		EnrichmentBackdropURL: "https://image.tmdb.org/t/p/w1280/backdrop.jpg",
	}
	w := httptest.NewRecorder()
	emitProgramme(w, row, "")
	out := w.Body.String()

	// The new <image> element — what Plex DVR actually reads
	if !strings.Contains(out, `<image size="3" type="poster">https://image.tmdb.org/t/p/w500/poster.jpg</image>`) {
		t.Errorf("missing <image> poster element, got:\n%s", out)
	}
	// Backdrop also emitted as <image>
	if !strings.Contains(out, `<image size="3" orient="L" type="backdrop">https://image.tmdb.org/t/p/w1280/backdrop.jpg</image>`) {
		t.Errorf("missing <image> backdrop element, got:\n%s", out)
	}
	// <icon> still emitted for fallback / non-Plex consumers
	if !strings.Contains(out, `<icon src="https://image.tmdb.org/t/p/w500/poster.jpg"/>`) {
		t.Errorf("missing <icon> poster fallback, got:\n%s", out)
	}
}

func TestEmitProgramme_SourcePosterFallback(t *testing.T) {
	row := store.ProgramOutputRow{
		ChannelID:       uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		ChannelName:     "Test Channel",
		ChannelNumber:   1,
		Title:           "Local News",
		StartAt:         time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC),
		EndAt:           time.Date(2026, 5, 8, 13, 0, 0, 0, time.UTC),
		SourcePosterURL: "https://feed.example/local-news.jpg",
	}
	w := httptest.NewRecorder()
	emitProgramme(w, row, "http://conductor.local")
	out := w.Body.String()
	if !strings.Contains(out, `<image size="3" type="poster">https://feed.example/local-news.jpg</image>`) {
		t.Fatalf("source poster not emitted: %s", out)
	}

	row.EnrichmentPosterURL = "https://image.tmdb.org/enriched.jpg"
	w = httptest.NewRecorder()
	emitProgramme(w, row, "http://conductor.local")
	out = w.Body.String()
	if !strings.Contains(out, "https://image.tmdb.org/enriched.jpg") || strings.Contains(out, "feed.example") {
		t.Fatalf("enrichment must win over source art: %s", out)
	}
}

func TestEmitProgramme_SourceArtSupersedesGeneratedFallback(t *testing.T) {
	row := store.ProgramOutputRow{
		ChannelID:          uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		ChannelName:        "Test Channel",
		ChannelNumber:      1,
		Title:              "Matchup",
		StartAt:            time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC),
		EndAt:              time.Date(2026, 5, 8, 15, 0, 0, 0, time.UTC),
		SourcePosterURL:    "https://feed.example/real.jpg?a=1&b=2",
		GeneratedPosterURL: "/posters/assets/generated/sha256/ab/abcdef.png",
	}
	w := httptest.NewRecorder()
	emitProgramme(w, row, "http://conductor.local")
	out := w.Body.String()
	if strings.Contains(out, "generated/sha256") {
		t.Fatalf("generated fallback outranked later source art: %s", out)
	}
	if !strings.Contains(out, `https://feed.example/real.jpg?a=1&amp;b=2`) {
		t.Fatalf("source poster URL was not XML escaped: %s", out)
	}
}

func TestEmitProgramme_SourceArtSupersedesResearchedFallback(t *testing.T) {
	row := store.ProgramOutputRow{
		ChannelID:           uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		ChannelName:         "Test Channel",
		ChannelNumber:       1,
		Title:               "Local News",
		StartAt:             time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC),
		EndAt:               time.Date(2026, 5, 8, 13, 0, 0, 0, time.UTC),
		SourcePosterURL:     "https://feed.example/local-news.jpg",
		EnrichmentPosterURL: "/posters/assets/researched/sha256/ab/abcdef.jpg",
	}
	w := httptest.NewRecorder()
	emitProgramme(w, row, "http://conductor.local")
	out := w.Body.String()
	if strings.Contains(out, "/posters/assets/researched/") {
		t.Fatalf("researched fallback outranked later source art: %s", out)
	}
	if !strings.Contains(out, "https://feed.example/local-news.jpg") {
		t.Fatalf("source poster not emitted: %s", out)
	}

	row.SourcePosterURL = ""
	w = httptest.NewRecorder()
	emitProgramme(w, row, "http://conductor.local")
	out = w.Body.String()
	if !strings.Contains(out, "http://conductor.local/posters/assets/researched/sha256/ab/abcdef.jpg") {
		t.Fatalf("researched poster not used when source art is absent: %s", out)
	}
}

func TestOutputETagTracksCompleteOutputModel(t *testing.T) {
	row := store.ProgramOutputRow{
		ChannelID:       uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		ChannelName:     "Test Channel",
		ChannelLogoURL:  "https://feed.example/logo.png",
		Title:           "Local News",
		StartAt:         time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC),
		EndAt:           time.Date(2026, 5, 8, 13, 0, 0, 0, time.UTC),
		SourcePosterURL: "https://feed.example/news.jpg",
	}
	original := outputETag([]store.ProgramOutputRow{row}, "http://conductor.local")

	changed := row
	changed.SourcePosterURL = "https://feed.example/news-v2.jpg"
	if got := outputETag([]store.ProgramOutputRow{changed}, "http://conductor.local"); got == original {
		t.Fatal("source poster change did not invalidate ETag")
	}
	changed = row
	changed.ChannelLogoURL = "https://feed.example/logo-v2.png"
	if got := outputETag([]store.ProgramOutputRow{changed}, "http://conductor.local"); got == original {
		t.Fatal("channel metadata change did not invalidate ETag")
	}
	if got := outputETag(nil, "http://conductor.local"); got == original {
		t.Fatal("program deletion did not invalidate ETag")
	}
	if got := outputETag([]store.ProgramOutputRow{row}, "https://conductor.example"); got == original {
		t.Fatal("public base URL change did not invalidate ETag")
	}
}

func TestIfNoneMatchUsesWeakListSemantics(t *testing.T) {
	const current = `"epg-0123456789abcdef"`
	tests := []struct {
		name   string
		header string
		match  bool
	}{
		{name: "exact", header: current, match: true},
		{name: "weak", header: `W/` + current, match: true},
		{name: "list", header: `"other", W/` + current + `, "last"`, match: true},
		{name: "wildcard", header: `*`, match: true},
		{name: "different", header: `W/"other"`, match: false},
		{name: "empty", header: ``, match: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ifNoneMatchMatches(tt.header, current); got != tt.match {
				t.Fatalf("ifNoneMatchMatches(%q, %q) = %v; want %v", tt.header, current, got, tt.match)
			}
		})
	}
}

func TestEmitChannel_LocalLogoBecomesAbsolute(t *testing.T) {
	row := store.ProgramOutputRow{
		ChannelName:    "Big Ten PPV 1",
		ChannelNumber:  385,
		ChannelLogoURL: "/logos/big-ten-plus-925e1063.jpg",
	}
	w := httptest.NewRecorder()
	emitChannel(w, row, "ch.test", "http://conductor.local/")
	if !strings.Contains(
		w.Body.String(),
		`<icon src="http://conductor.local/logos/big-ten-plus-925e1063.jpg"/>`,
	) {
		t.Fatalf("relative channel logo was not made absolute: %s", w.Body.String())
	}
}

func TestEmitProgramme_LocalPosterBecomesAbsolute(t *testing.T) {
	row := store.ProgramOutputRow{
		ChannelID:          uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		ChannelName:        "Test Channel",
		ChannelNumber:      1,
		Title:              "Matchup",
		StartAt:            time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC),
		EndAt:              time.Date(2026, 5, 8, 15, 0, 0, 0, time.UTC),
		GeneratedPosterURL: "/posters/assets/generated/sha256/ab/abcdef.png",
	}
	w := httptest.NewRecorder()
	emitProgramme(w, row, "http://conductor.local/")
	if !strings.Contains(w.Body.String(), "http://conductor.local/posters/assets/generated") {
		t.Fatalf("local poster URL was not made absolute: %s", w.Body.String())
	}
}

func TestEmitProgramme_EpisodeDescriptionOutranksSeriesOverview(t *testing.T) {
	row := store.ProgramOutputRow{
		ChannelID:          uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		ChannelName:        "Test Channel",
		ChannelNumber:      1,
		Title:              "Law & Order",
		Description:        "S20 E02 - Man Down. Detectives investigate a construction-site death.",
		EpisodeNumXMLTV:    "19.1.",
		EpisodeNumOnscreen: "S20E02",
		EnrichmentOverview: "In the criminal justice system, the people are represented by two separate groups.",
		StartAt:            time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC),
		EndAt:              time.Date(2026, 7, 23, 13, 0, 0, 0, time.UTC),
	}
	w := httptest.NewRecorder()
	emitProgramme(w, row, "")
	out := w.Body.String()
	if !strings.Contains(out, "S20 E02 - Man Down.") {
		t.Fatalf("episode-specific source description was discarded: %s", out)
	}
	if strings.Contains(out, "criminal justice system") {
		t.Fatalf("generic series overview replaced episode-specific description: %s", out)
	}

	row.EpisodeNumXMLTV = "3.4."
	row.EpisodeNumOnscreen = "S04E05"
	w = httptest.NewRecorder()
	emitProgramme(w, row, "")
	out = w.Body.String()
	if strings.Contains(out, "S20 E02 - Man Down.") || !strings.Contains(out, "criminal justice system") {
		t.Fatalf("conflicting description outranked formal episode metadata: %s", out)
	}

	row.Description = "Generic provider description"
	row.EpisodeNumXMLTV = ""
	row.EpisodeNumOnscreen = ""
	w = httptest.NewRecorder()
	emitProgramme(w, row, "")
	out = w.Body.String()
	if !strings.Contains(out, "criminal justice system") {
		t.Fatalf("normal source description should still yield to enrichment overview: %s", out)
	}
}

// TestEmitProgramme_NoEnrichmentNoImageElements: an unenriched programme
// must not emit empty <image> or <icon> elements (would confuse parsers).
func TestEmitProgramme_NoEnrichmentNoImageElements(t *testing.T) {
	row := store.ProgramOutputRow{
		ChannelID:     uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		ChannelName:   "Test Channel",
		ChannelNumber: 1.0,
		Title:         "Programme With No Match",
		StartAt:       time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC),
		EndAt:         time.Date(2026, 5, 8, 13, 0, 0, 0, time.UTC),
	}
	w := httptest.NewRecorder()
	emitProgramme(w, row, "")
	out := w.Body.String()
	if strings.Contains(out, `<image`) {
		t.Errorf("did not expect <image> for unenriched programme, got:\n%s", out)
	}
	if strings.Contains(out, `<icon`) {
		t.Errorf("did not expect <icon> for unenriched programme, got:\n%s", out)
	}
}

// TestEmitProgramme_BarePreviouslyShownWhenNoOAD covers the
// 2026-05-08 fix: when a programme is not new and lacks an
// OriginalAirDate, emit a bare <previously-shown/> so Plex doesn't
// default to NEW. Without this, the Plex guide stamps roughly half of
// programmes (anything missing both <new/> and <previously-shown>) as
// NEW even when they aren't.
func TestEmitProgramme_BarePreviouslyShownWhenNoOAD(t *testing.T) {
	row := store.ProgramOutputRow{
		ChannelID:     uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		ChannelName:   "Test Channel",
		ChannelNumber: 1.0,
		Title:         "Generic Talk Show",
		StartAt:       time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC),
		EndAt:         time.Date(2026, 5, 8, 13, 0, 0, 0, time.UTC),
		IsLive:        false,
		IsNew:         false,
		// OriginalAirDate intentionally nil
	}
	w := httptest.NewRecorder()
	emitProgramme(w, row, "")
	out := w.Body.String()
	if !strings.Contains(out, `<previously-shown/>`) {
		t.Errorf("expected bare <previously-shown/> in emit, got:\n%s", out)
	}
	if strings.Contains(out, `<new/>`) {
		t.Errorf("did not expect <new/> in emit, got:\n%s", out)
	}
}

// TestEmitProgramme_PreviouslyShownWithOAD: when OriginalAirDate is
// known, emit <previously-shown start="..."/> with the date.
func TestEmitProgramme_PreviouslyShownWithOAD(t *testing.T) {
	oad := time.Date(2003, 5, 30, 0, 0, 0, 0, time.UTC)
	row := store.ProgramOutputRow{
		ChannelID:       uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		ChannelName:     "Test Channel",
		ChannelNumber:   1.0,
		Title:           "The Italian Job",
		StartAt:         time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC),
		EndAt:           time.Date(2026, 5, 8, 14, 30, 0, 0, time.UTC),
		IsLive:          false,
		IsNew:           false,
		OriginalAirDate: &oad,
	}
	w := httptest.NewRecorder()
	emitProgramme(w, row, "")
	out := w.Body.String()
	if !strings.Contains(out, `<previously-shown start="20030530000000 +0000"/>`) {
		t.Errorf("expected dated <previously-shown> in emit, got:\n%s", out)
	}
	// Make sure we did NOT also emit a bare one (mutually exclusive paths).
	if strings.Contains(out, `<previously-shown/>`) {
		t.Errorf("did not expect bare <previously-shown/> alongside dated one, got:\n%s", out)
	}
}

// TestEmitProgramme_NewPath: when IsNew=true, emit <new/>, never
// <previously-shown> (mutually exclusive — programme is either new or
// previously-shown).
func TestEmitProgramme_NewPath(t *testing.T) {
	row := store.ProgramOutputRow{
		ChannelID:     uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		ChannelName:   "Test Channel",
		ChannelNumber: 1.0,
		Title:         "Brand New Series",
		StartAt:       time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC),
		EndAt:         time.Date(2026, 5, 8, 13, 0, 0, 0, time.UTC),
		IsLive:        false,
		IsNew:         true,
	}
	w := httptest.NewRecorder()
	emitProgramme(w, row, "")
	out := w.Body.String()
	if !strings.Contains(out, `<new/>`) {
		t.Errorf("expected <new/> in emit, got:\n%s", out)
	}
	if strings.Contains(out, `<previously-shown`) {
		t.Errorf("did not expect any <previously-shown> alongside <new/>, got:\n%s", out)
	}
}

// TestEmitProgramme_BrandedFallbackPoster: when a programme has no
// enrichment poster, no specific cat hint, and a baseURL is configured,
// the emitter falls back to /posters/branded/{channel_id}.jpg with the
// programme's primary category as a query string. Channels without a
// logo skip the fallback (a coloured square is worse than a blank tile).
func TestEmitProgramme_BrandedFallbackPoster(t *testing.T) {
	row := store.ProgramOutputRow{
		ChannelID:      uuid.MustParse("33333333-3333-3333-3333-333333333333"),
		ChannelLogoURL: "http://conductor.local/logos/abc.png",
		ChannelName:    "ESPN",
		ChannelNumber:  201.0,
		Title:          "SportsCenter",
		Category:       []string{"Sports", "Sports talk"},
		StartAt:        time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC),
		EndAt:          time.Date(2026, 5, 8, 13, 0, 0, 0, time.UTC),
	}
	w := httptest.NewRecorder()
	emitProgramme(w, row, "http://conductor.local")
	out := w.Body.String()

	// Sports programmes emit NO fallback poster. The card would have to be
	// channel-independent (Plex merges programme identities across
	// channels, so per-channel art on a sports airing leaks one channel's
	// logo onto every other airing — the 2026-06-10 "ABC 7 logo on sports
	// shows" report), and a channel-independent card has no logo and no
	// caption, which renders in Plex as a bare coloured rectangle. Same
	// rule as a logo-less channel: Plex's own tile is the smaller visual.
	if strings.Contains(out, "/posters/branded/") {
		t.Errorf("sports programme must emit no branded fallback poster, got:\n%s", out)
	}
	if strings.Contains(out, "<image") || strings.Contains(out, "<icon") {
		t.Errorf("sports programme with no enrichment must emit no artwork, got:\n%s", out)
	}

	// Non-sports programmes keep the per-channel branded card.
	row.Title = "Local Morning News"
	row.Category = []string{"News"}
	row.IsLive = true
	w2 := httptest.NewRecorder()
	emitProgramme(w2, row, "http://conductor.local")
	out2 := w2.Body.String()
	wantSrc2 := `http://conductor.local/posters/branded/33333333-3333-3333-3333-333333333333.jpg?cat=News`
	if !strings.Contains(out2, `<image size="3" type="poster">`+wantSrc2+`</image>`) {
		t.Errorf("expected channel-branded fallback for non-sports:\n%s\ngot:\n%s", wantSrc2, out2)
	}
}

// TestEmitProgramme_NoFallbackWithoutBaseURL: an empty baseURL (Phase 0
// mode, before BaseURL config wired) suppresses the fallback so we
// never emit a relative or empty src.
func TestEmitProgramme_NoFallbackWithoutBaseURL(t *testing.T) {
	row := store.ProgramOutputRow{
		ChannelID:      uuid.MustParse("44444444-4444-4444-4444-444444444444"),
		ChannelLogoURL: "http://conductor.local/logos/abc.png",
		ChannelNumber:  1.0,
		Title:          "Show",
		StartAt:        time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC),
		EndAt:          time.Date(2026, 5, 8, 13, 0, 0, 0, time.UTC),
	}
	w := httptest.NewRecorder()
	emitProgramme(w, row, "")
	out := w.Body.String()
	if strings.Contains(out, `posters/branded`) {
		t.Errorf("expected no branded fallback when baseURL is empty, got:\n%s", out)
	}
}

// TestEmitProgramme_NoFallbackWithoutChannelLogo: a channel with no
// logo URL also skips the fallback. (See brandedFallbackPosterURL —
// rendering a tinted card without a logo would just look like an
// errored tile.)
func TestEmitProgramme_NoFallbackWithoutChannelLogo(t *testing.T) {
	row := store.ProgramOutputRow{
		ChannelID:     uuid.MustParse("55555555-5555-5555-5555-555555555555"),
		ChannelNumber: 1.0,
		Title:         "Show",
		StartAt:       time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC),
		EndAt:         time.Date(2026, 5, 8, 13, 0, 0, 0, time.UTC),
	}
	w := httptest.NewRecorder()
	emitProgramme(w, row, "http://conductor.local")
	out := w.Body.String()
	if strings.Contains(out, `posters/branded`) {
		t.Errorf("expected no branded fallback when channel has no logo, got:\n%s", out)
	}
}

// TestEmitProgramme_LiveRepeatStateIsIndependent covers the Plex XMLTV
// semantics audited in 2026-07: <live/> is a non-standard extension that Plex
// ignores, while absence of <previously-shown> makes episode-shaped rows look
// New. A live broadcast that is not new therefore needs both markers.
func TestEmitProgramme_LiveRepeatStateIsIndependent(t *testing.T) {
	row := store.ProgramOutputRow{
		ChannelID:     uuid.MustParse("66666666-6666-6666-6666-666666666666"),
		ChannelName:   "NHL PPV 1",
		ChannelNumber: 700,
		Title:         "Carolina Hurricanes @ Buffalo Sabres",
		StartAt:       time.Date(2026, 5, 8, 23, 0, 0, 0, time.UTC),
		EndAt:         time.Date(2026, 5, 9, 2, 0, 0, 0, time.UTC),
		IsLive:        true,
		IsNew:         false,
	}
	w := httptest.NewRecorder()
	emitProgramme(w, row, "")
	out := w.Body.String()
	if !strings.Contains(out, `<live/>`) {
		t.Errorf("expected <live/> on a live sports row, got:\n%s", out)
	}
	if !strings.Contains(out, `<previously-shown/>`) {
		t.Errorf("expected repeat marker on live, non-new row, got:\n%s", out)
	}
	if strings.Contains(out, `<new/>`) {
		t.Errorf("did not expect <new/> alongside <live/>, got:\n%s", out)
	}

	oad := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	row.OriginalAirDate = &oad
	wDated := httptest.NewRecorder()
	emitProgramme(wDated, row, "")
	if !strings.Contains(
		wDated.Body.String(),
		`<previously-shown start="20260501000000 +0000"/>`,
	) {
		t.Errorf(
			"expected dated repeat marker on live, non-new row, got:\n%s",
			wDated.Body.String(),
		)
	}

	row.IsNew = true
	w2 := httptest.NewRecorder()
	emitProgramme(w2, row, "")
	out2 := w2.Body.String()
	if !strings.Contains(out2, `<live/>`) || !strings.Contains(out2, `<new/>`) {
		t.Errorf("expected independent live and new markers, got:\n%s", out2)
	}
	if strings.Contains(out2, `<previously-shown`) {
		t.Errorf("did not expect repeat marker on live, new row, got:\n%s", out2)
	}
}

func TestProjectProgramsForOutput_LiveCueBoundariesAndIdentity(t *testing.T) {
	start := time.Date(2026, 8, 23, 16, 0, 0, 0, time.UTC)
	event := store.ProgramOutputRow{
		Title: "Championship Final (LIVE)", StartAt: start,
		EndAt: start.Add(3 * time.Hour), IsLive: true,
	}
	episode := event
	episode.Title = "Recognized Series"
	episode.SubTitle = "The Finale"
	episode.HasEpisodeIdentity = true

	tests := []struct {
		name         string
		row          store.ProgramOutputRow
		now          time.Time
		wantTitle    string
		wantSubTitle string
	}{
		{name: "upcoming describes scheduled broadcast", row: event, now: start.Add(-time.Nanosecond), wantTitle: "LIVE broadcast — Championship Final"},
		{name: "exact start", row: event, now: start, wantTitle: "LIVE broadcast — Championship Final"},
		{name: "mid window", row: event, now: start.Add(time.Hour), wantTitle: "LIVE broadcast — Championship Final"},
		{name: "exact end", row: event, now: event.EndAt, wantTitle: "LIVE broadcast — Championship Final"},
		{name: "episode preserves shared series and episode titles", row: episode, now: start, wantTitle: "Recognized Series", wantSubTitle: "The Finale"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			original := tc.row
			got := projectProgramsForOutput([]store.ProgramOutputRow{tc.row}, tc.now)[0]
			if got.Title != tc.wantTitle || got.SubTitle != tc.wantSubTitle {
				t.Fatalf("projection = title %q subtitle %q, want %q / %q", got.Title, got.SubTitle, tc.wantTitle, tc.wantSubTitle)
			}
			if tc.row.Title != original.Title || tc.row.SubTitle != original.SubTitle {
				t.Fatal("projection mutated its input row")
			}
		})
	}
}

func TestProjectProgramsForOutput_StripsCoverageDecorationAtomically(t *testing.T) {
	start := time.Date(2026, 8, 23, 16, 0, 0, 0, time.UTC)
	for _, decorated := range []string{
		"LIVE Coverage: Championship Final",
		"LIVE Coverage - Championship Final",
		"LIVE Coverage – Championship Final",
		"LIVE Coverage — Championship Final",
		"LIVE Broadcast: Championship Final",
		"LIVE Broadcast-Championship Final",
		"LIVE Broadcast–Championship Final",
		"LIVE Broadcast—Championship Final",
	} {
		t.Run(decorated, func(t *testing.T) {
			row := store.ProgramOutputRow{
				Title: decorated, StartAt: start, EndAt: start.Add(time.Hour), IsLive: true,
			}
			before := projectProgramsForOutput([]store.ProgramOutputRow{row}, start.Add(-time.Nanosecond))[0]
			if before.Title != "LIVE broadcast — Championship Final" {
				t.Fatalf("upcoming title = %q", before.Title)
			}
			during := projectProgramsForOutput([]store.ProgramOutputRow{row}, start)[0]
			if during.Title != "LIVE broadcast — Championship Final" {
				t.Fatalf("live title = %q", during.Title)
			}
			after := projectProgramsForOutput([]store.ProgramOutputRow{row}, row.EndAt)[0]
			if after.Title != "LIVE broadcast — Championship Final" {
				t.Fatalf("ended title = %q", after.Title)
			}
		})
	}
}

func TestProjectProgramsForOutput_DeniesPlaceholderCues(t *testing.T) {
	now := time.Date(2026, 8, 23, 16, 0, 0, 0, time.UTC)
	base := store.ProgramOutputRow{
		Title: "Broncos @ Chiefs", StartAt: now.Add(-time.Hour),
		EndAt: now.Add(time.Hour), IsLive: true,
	}
	tests := []struct {
		name string
		row  store.ProgramOutputRow
	}{
		{
			name: "ppv off placeholder",
			row: store.ProgramOutputRow{
				Title: "UFC — No Event Scheduled", StartAt: now.Add(-time.Hour),
				EndAt: now.Add(time.Hour), IsLive: true, IsPlaceholder: true,
			},
		},
		{
			name: "next event placeholder",
			row: store.ProgramOutputRow{
				Title: "Next 7:00 PM MDT — Main Event", StartAt: now.Add(-time.Hour),
				EndAt: now.Add(time.Hour), IsLive: true, IsPlaceholder: true,
			},
		},
		{
			name: "text classified idle placeholder",
			row: store.ProgramOutputRow{
				Title: "NFL", SubTitle: "Nothing Scheduled", StartAt: now.Add(-time.Hour),
				EndAt: now.Add(time.Hour), IsLive: true,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := projectProgramsForOutput([]store.ProgramOutputRow{tc.row}, now)[0]
			if strings.HasPrefix(got.Title, "LIVE broadcast —") || strings.HasPrefix(got.SubTitle, "LIVE broadcast —") {
				t.Fatalf("false live cue projected: %+v", got)
			}
		})
	}
	legitimate := base
	legitimate.Title = "No Game No Life Special"
	if got := projectProgramsForOutput([]store.ProgramOutputRow{legitimate}, now)[0]; got.Title != "LIVE broadcast — No Game No Life Special" {
		t.Fatalf("legitimate title was mistaken for a placeholder: %+v", got)
	}
	splitMarker := base
	splitMarker.Title, splitMarker.SubTitle = "LIVE", "Special Event"
	if got := projectProgramsForOutput([]store.ProgramOutputRow{splitMarker}, now)[0]; got.Title != "LIVE broadcast — Special Event" || got.SubTitle != "" {
		t.Fatalf("split LIVE marker lost the event title: %+v", got)
	}
}

func TestProjectProgramsForOutput_RealPPVNominalAndOverrunBoundaries(t *testing.T) {
	start := time.Date(2026, 8, 23, 20, 0, 0, 0, time.UTC)
	parsed := ppvparse.ParsedStream{
		Status: ppvparse.StatusUpcoming, Title: "Broncos @ Chiefs",
		StartAt: start, Duration: 3 * time.Hour, Slot: "NFL PPV 01", Sport: "nfl",
	}
	program, ok := ppvparse.ToEPGProgram(uuid.New(), parsed, "xtream:9477")
	if !ok {
		t.Fatal("PPV fixture did not produce a guide row")
	}
	row := store.ProgramOutputRow{
		Title: program.Title, StartAt: program.StartAt, EndAt: program.EndAt,
		IsLive: program.IsLive,
	}
	if got := projectProgramsForOutput([]store.ProgramOutputRow{row}, start.Add(-time.Nanosecond))[0]; got.Title != "LIVE broadcast — Broncos @ Chiefs" {
		t.Fatalf("upcoming real PPV lost broadcast descriptor: %+v", got)
	}
	if got := projectProgramsForOutput([]store.ProgramOutputRow{row}, start)[0]; got.Title != "LIVE broadcast — Broncos @ Chiefs" {
		t.Fatalf("PPV at kickoff title = %q", got.Title)
	}

	// ppvsync may extend a confirmed-live row beyond its nominal stop. The
	// stored finite extension remains authoritative for guide times. Describing
	// the airing as a live broadcast does not extend those times.
	nominalEnd := row.EndAt
	row.EndAt = nominalEnd.Add(90 * time.Minute)
	if got := projectProgramsForOutput([]store.ProgramOutputRow{row}, nominalEnd.Add(time.Minute))[0]; got.Title != "LIVE broadcast — Broncos @ Chiefs" {
		t.Fatalf("confirmed overrun lost cue early: %q", got.Title)
	}
	if got := projectProgramsForOutput([]store.ProgramOutputRow{row}, row.EndAt)[0]; got.Title != "LIVE broadcast — Broncos @ Chiefs" {
		t.Fatalf("broadcast descriptor changed at finite overrun end: %q", got.Title)
	}

	off := ppvparse.ParsedStream{Status: ppvparse.StatusOff, Slot: "NFL PPV 01", Sport: "nfl", OffIdle: true}
	placeholders := ppvparse.OffSlotPrograms(program.ChannelID, off, "xtream:9477", &program, start.Add(-time.Hour), time.UTC)
	if len(placeholders) == 0 {
		t.Fatal("PPV fixture produced no pre-event placeholder")
	}
	placeholder := store.ProgramOutputRow{
		Title: placeholders[0].Title, StartAt: placeholders[0].StartAt,
		EndAt: placeholders[0].EndAt, IsLive: placeholders[0].IsLive,
		IsPlaceholder: placeholders[0].SourcePriority == store.PriorityPPVOff,
	}
	if got := projectProgramsForOutput([]store.ProgramOutputRow{placeholder}, start.Add(-time.Hour))[0]; strings.HasPrefix(got.Title, "LIVE broadcast —") {
		t.Fatalf("real PPV Next/off placeholder received cue: %+v", got)
	}
}

func TestOutputETagTracksLiveEvidenceRatherThanWallClock(t *testing.T) {
	start := time.Date(2026, 8, 23, 16, 0, 0, 0, time.UTC)
	row := store.ProgramOutputRow{Title: "Event", StartAt: start, EndAt: start.Add(time.Hour), IsLive: true}
	before := outputETag(projectProgramsForOutput([]store.ProgramOutputRow{row}, start.Add(-time.Hour)), "")
	for _, at := range []time.Time{start, start.Add(time.Minute), row.EndAt} {
		if got := outputETag(projectProgramsForOutput([]store.ProgramOutputRow{row}, at), ""); got != before {
			t.Fatalf("unchanged scheduled broadcast changed ETag at %s", at)
		}
	}
	row.PreviouslyShownExplicit = true
	if got := outputETag(projectProgramsForOutput([]store.ProgramOutputRow{row}, start), ""); got == before {
		t.Fatal("new repeat evidence did not invalidate the live description")
	}
}

func TestXMLTVValidatorsIMSOnlyCannotHideLiveEvidenceChanges(t *testing.T) {
	// An import is stable across clock boundaries, but an explicit source
	// correction must invalidate it even when Last-Modified has not advanced.
	start := time.Date(2026, 8, 23, 16, 0, 0, 500_000_000, time.UTC)
	end := start.Add(time.Hour)
	latest := start.Add(-10 * time.Minute)
	row := store.ProgramOutputRow{
		ChannelID: uuid.MustParse("91919191-9191-9191-9191-919191919191"),
		Title:     "Boundary Event", StartAt: start, EndAt: end, IsLive: true,
	}

	before := recordXMLTVRepresentation(t, http.MethodGet, []store.ProgramOutputRow{row}, latest,
		start.Add(-time.Nanosecond), "", "")
	if before.Code != http.StatusOK || !strings.Contains(before.Body.String(), "LIVE broadcast —") {
		t.Fatalf("pre-start response code=%d body=%s", before.Code, before.Body.String())
	}

	atStart := recordXMLTVRepresentation(t, http.MethodGet, []store.ProgramOutputRow{row}, latest,
		start, "", before.Header().Get("Last-Modified"))
	if atStart.Code != http.StatusOK || !strings.Contains(atStart.Body.String(), "LIVE broadcast — Boundary Event") {
		t.Fatalf("IMS-only start response code=%d body=%s", atStart.Code, atStart.Body.String())
	}
	if atStart.Header().Get("ETag") != before.Header().Get("ETag") {
		t.Fatal("live start changed an unchanged airing representation")
	}

	during := recordXMLTVRepresentation(t, http.MethodGet, []store.ProgramOutputRow{row}, latest,
		end.Add(-time.Nanosecond), "", atStart.Header().Get("Last-Modified"))
	if during.Code != http.StatusOK || !strings.Contains(during.Body.String(), "LIVE broadcast — Boundary Event") {
		t.Fatalf("pre-end response code=%d body=%s", during.Code, during.Body.String())
	}
	row.PreviouslyShownExplicit = true
	after := recordXMLTVRepresentation(t, http.MethodGet, []store.ProgramOutputRow{row}, latest,
		end, "", during.Header().Get("Last-Modified"))
	if after.Code != http.StatusOK || strings.Contains(after.Body.String(), "LIVE broadcast —") {
		t.Fatalf("IMS-only end response code=%d body=%s", after.Code, after.Body.String())
	}
	if after.Header().Get("ETag") == during.Header().Get("ETag") {
		t.Fatal("repeat evidence did not change the representation ETag")
	}
}

func TestXMLTVValidatorsETagPrecedesIMSForGETAndHEAD(t *testing.T) {
	now := time.Date(2026, 8, 23, 16, 30, 0, 0, time.UTC)
	latest := now.Add(-time.Minute)
	row := store.ProgramOutputRow{
		ChannelID: uuid.MustParse("92929292-9292-9292-9292-929292929292"),
		Title:     "Conditional Event", StartAt: now.Add(-time.Minute), EndAt: now.Add(time.Hour), IsLive: true,
	}
	baseline := recordXMLTVRepresentation(t, http.MethodGet, []store.ProgramOutputRow{row}, latest, now, "", "")
	etag := baseline.Header().Get("ETag")
	lastModified := baseline.Header().Get("Last-Modified")

	matching := recordXMLTVRepresentation(t, http.MethodGet, []store.ProgramOutputRow{row}, latest, now,
		`"other", W/`+etag, now.Add(24*time.Hour).Format(http.TimeFormat))
	if matching.Code != http.StatusNotModified || matching.Body.Len() != 0 {
		t.Fatalf("matching list/weak ETag response code=%d body=%q", matching.Code, matching.Body.String())
	}

	mismatch := recordXMLTVRepresentation(t, http.MethodGet, []store.ProgramOutputRow{row}, latest, now,
		`"stale"`, lastModified)
	if mismatch.Code != http.StatusOK || !strings.Contains(mismatch.Body.String(), "LIVE broadcast — Conditional Event") {
		t.Fatalf("mismatched ETag fell through to IMS: code=%d body=%s", mismatch.Code, mismatch.Body.String())
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveXMLTVRepresentation(w, r, []store.ProgramOutputRow{row}, "", latest, now)
	}))
	t.Cleanup(server.Close)
	head, headBody := requestXMLTVRepresentationOverHTTP(t, server, `"stale"`, lastModified)
	if head.StatusCode != http.StatusOK || len(headBody) != 0 {
		t.Fatalf("HEAD mismatch response code=%d body=%q", head.StatusCode, headBody)
	}
	for _, header := range []string{"ETag", "Last-Modified", "Content-Type"} {
		if head.Header.Get(header) != mismatch.Header().Get(header) {
			t.Errorf("GET/HEAD %s mismatch: %q != %q", header,
				head.Header.Get(header), mismatch.Header().Get(header))
		}
	}
	headMatch, headMatchBody := requestXMLTVRepresentationOverHTTP(t, server, "*", lastModified)
	if headMatch.StatusCode != http.StatusNotModified || len(headMatchBody) != 0 {
		t.Fatalf("HEAD wildcard response code=%d body=%q", headMatch.StatusCode, headMatchBody)
	}
}

func TestXMLTVValidatorsLastModifiedIgnoresFutureBoundariesAndTracksDBRevision(t *testing.T) {
	now := time.Date(2026, 8, 23, 16, 30, 0, 750_000_000, time.UTC)
	row := store.ProgramOutputRow{
		ChannelID: uuid.MustParse("93939393-9393-9393-9393-939393939393"),
		Title:     "Future Boundary Fixture", StartAt: now.Add(time.Hour), EndAt: now.Add(2 * time.Hour), IsLive: true,
	}

	firstRevision := now.Add(-10 * time.Second)
	first := recordXMLTVRepresentation(t, http.MethodGet, []store.ProgramOutputRow{row}, firstRevision, now, "", "")
	firstLM, err := http.ParseTime(first.Header().Get("Last-Modified"))
	if err != nil {
		t.Fatal(err)
	}
	if !firstLM.Equal(firstRevision.Truncate(time.Second)) || !firstLM.Before(now) || !firstLM.Before(row.StartAt) {
		t.Fatalf("future guide boundary leaked into Last-Modified: revision=%s header=%s start=%s",
			firstRevision, firstLM, row.StartAt)
	}

	secondRevision := now.Add(-2 * time.Second)
	second := recordXMLTVRepresentation(t, http.MethodGet, []store.ProgramOutputRow{row}, secondRevision, now,
		"", first.Header().Get("Last-Modified"))
	secondLM, err := http.ParseTime(second.Header().Get("Last-Modified"))
	if err != nil {
		t.Fatal(err)
	}
	if !secondLM.After(firstLM) || second.Code != http.StatusOK {
		t.Fatalf("revision Last-Modified did not advance safely: first=%s second=%s code=%d",
			firstLM, secondLM, second.Code)
	}
	if second.Header().Get("ETag") != first.Header().Get("ETag") {
		t.Fatal("database revision telemetry changed an otherwise identical representation ETag")
	}

	zero := recordXMLTVRepresentation(t, http.MethodGet, []store.ProgramOutputRow{row}, time.Time{}, now, "", "")
	if got := zero.Header().Get("Last-Modified"); got != "" {
		t.Fatalf("zero revision emitted Last-Modified %q", got)
	}
}

func TestXMLTVValidatorsScheduledBroadcastAndPlaceholderStayStable(t *testing.T) {
	now := time.Date(2026, 8, 23, 16, 0, 0, 0, time.UTC)
	latest := now.Add(-time.Hour)
	rows := []store.ProgramOutputRow{
		{
			ChannelID: uuid.MustParse("94949494-9494-9494-9494-949494949494"),
			Title:     "Upcoming Event", StartAt: now.Add(time.Hour), EndAt: now.Add(2 * time.Hour), IsLive: true,
		},
		{
			ChannelID: uuid.MustParse("95959595-9595-9595-9595-959595959595"),
			Title:     "Next Event", StartAt: now.Add(-time.Hour), EndAt: now.Add(time.Hour),
			IsLive: true, IsPlaceholder: true,
		},
	}
	first := recordXMLTVRepresentation(t, http.MethodGet, rows, latest, now, "", "")
	second := recordXMLTVRepresentation(t, http.MethodGet, rows, latest, now.Add(time.Second),
		"", first.Header().Get("Last-Modified"))
	if first.Code != http.StatusOK || second.Code != http.StatusOK {
		t.Fatalf("stable IMS-only responses = %d/%d", first.Code, second.Code)
	}
	if !strings.Contains(first.Body.String(), "LIVE broadcast — Upcoming Event") ||
		!strings.Contains(second.Body.String(), "LIVE broadcast — Upcoming Event") ||
		strings.Contains(second.Body.String(), "LIVE broadcast — Next Event") {
		t.Fatalf("scheduled broadcast or placeholder mislabeled:\n%s", second.Body.String())
	}
	if first.Header().Get("ETag") != second.Header().Get("ETag") {
		t.Fatal("stable upcoming/placeholder projection changed ETag")
	}
}

func recordXMLTVRepresentation(
	t *testing.T,
	method string,
	rows []store.ProgramOutputRow,
	latest, now time.Time,
	ifNoneMatch, ifModifiedSince string,
) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, "/xmltv.xml", nil)
	if ifNoneMatch != "" {
		req.Header.Set("If-None-Match", ifNoneMatch)
	}
	if ifModifiedSince != "" {
		req.Header.Set("If-Modified-Since", ifModifiedSince)
	}
	w := httptest.NewRecorder()
	serveXMLTVRepresentation(w, req, rows, "", latest, now)
	return w
}

func requestXMLTVRepresentationOverHTTP(
	t *testing.T,
	server *httptest.Server,
	ifNoneMatch, ifModifiedSince string,
) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodHead, server.URL+"/xmltv.xml", nil)
	if err != nil {
		t.Fatal(err)
	}
	if ifNoneMatch != "" {
		req.Header.Set("If-None-Match", ifNoneMatch)
	}
	if ifModifiedSince != "" {
		req.Header.Set("If-Modified-Since", ifModifiedSince)
	}
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	return resp, body
}

func TestEmitProgramme_FutureOADAndPremiereFinaleSemantics(t *testing.T) {
	start := time.Date(2026, 8, 23, 16, 0, 0, 0, time.UTC)
	future := start.AddDate(0, 0, 1)
	row := store.ProgramOutputRow{
		ChannelID: uuid.MustParse("87878787-8787-8787-8787-878787878787"),
		Title:     "Metadata Fixture", StartAt: start, EndAt: start.Add(time.Hour),
		OriginalAirDate: &future, IsPremiere: true, IsFinale: true,
	}
	w := httptest.NewRecorder()
	emitProgramme(w, row, "")
	got := w.Body.String()
	for _, want := range []string{`<previously-shown/>`, `<premiere/>`, `<finale/>`} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q:\n%s", want, got)
		}
	}
	for _, notWant := range []string{`<new/>`, `<date>20260824</date>`, `previously-shown start="20260824`} {
		if strings.Contains(got, notWant) {
			t.Errorf("future OAD leaked as %q:\n%s", notWant, got)
		}
	}
}

func TestEmitProgramme_UsesPersistedSourceCivilDateForLateEveningOAD(t *testing.T) {
	for _, zone := range []string{"America/New_York", "America/Chicago", "America/Los_Angeles"} {
		t.Run(zone, func(t *testing.T) {
			location, err := time.LoadLocation(zone)
			if err != nil {
				t.Fatal(err)
			}
			localStart := time.Date(2026, 8, 21, 23, 30, 0, 0, location)
			startAfterDBRoundtrip := localStart.UTC()
			airingDate := SourceCivilDate(localStart)
			valid := time.Date(2026, 8, 21, 0, 0, 0, 0, time.UTC)
			future := time.Date(2026, 8, 22, 0, 0, 0, 0, time.UTC)

			row := store.ProgramOutputRow{
				ChannelID:       uuid.MustParse("89898989-8989-8989-8989-898989898989"),
				Title:           "Late Local Airing",
				StartAt:         startAfterDBRoundtrip,
				EndAt:           startAfterDBRoundtrip.Add(time.Hour),
				AiringCivilDate: &airingDate,
				OriginalAirDate: &valid,
			}
			w := httptest.NewRecorder()
			emitProgramme(w, row, "")
			if got := w.Body.String(); !strings.Contains(got, `<date>20260821</date>`) ||
				!strings.Contains(got, `previously-shown start="20260821000000 +0000"`) {
				t.Fatalf("valid local OAD was suppressed after DB roundtrip:\n%s", got)
			}

			row.OriginalAirDate = &future
			w = httptest.NewRecorder()
			emitProgramme(w, row, "")
			if got := w.Body.String(); strings.Contains(got, `<date>20260822</date>`) ||
				strings.Contains(got, `previously-shown start="20260822`) ||
				!strings.Contains(got, `<previously-shown/>`) {
				t.Fatalf("future local OAD leaked after DB roundtrip:\n%s", got)
			}
		})
	}
}

func TestEmitProgramme_LegacyUnknownCivilDateFailsClosed(t *testing.T) {
	start := time.Date(2026, 8, 22, 3, 30, 0, 0, time.UTC)
	equalUTCDate := time.Date(2026, 8, 22, 0, 0, 0, 0, time.UTC)
	safelyEarlier := time.Date(2026, 8, 21, 0, 0, 0, 0, time.UTC)
	row := store.ProgramOutputRow{
		ChannelID:       uuid.MustParse("90909090-9090-9090-9090-909090909090"),
		Title:           "Legacy Civil Date",
		StartAt:         start,
		EndAt:           start.Add(time.Hour),
		OriginalAirDate: &equalUTCDate,
		// AiringCivilDate intentionally nil: this is a pre-0025/manual row.
	}
	w := httptest.NewRecorder()
	emitProgramme(w, row, "")
	if got := w.Body.String(); strings.Contains(got, `<date>20260822</date>`) ||
		strings.Contains(got, `previously-shown start="20260822`) ||
		!strings.Contains(got, `<previously-shown/>`) {
		t.Fatalf("ambiguous legacy equal-date OAD did not fail closed:\n%s", got)
	}

	row.OriginalAirDate = &safelyEarlier
	w = httptest.NewRecorder()
	emitProgramme(w, row, "")
	if got := w.Body.String(); !strings.Contains(got, `<date>20260821</date>`) ||
		!strings.Contains(got, `previously-shown start="20260821000000 +0000"`) {
		t.Fatalf("safe legacy earlier OAD was suppressed:\n%s", got)
	}
}

// TestRewriteOffSeasonTitle: dispatcharr ships placeholder rows as
// title="NFL", sub-title="No Event Scheduled" — Plex's tile loses the
// sub-title at common font sizes so the user just sees "NFL". This
// rewrite collapses both into a single title line.
func TestRewriteOffSeasonTitle(t *testing.T) {
	cases := []struct {
		title, sub   string
		wantT, wantS string
	}{
		{"NFL", "No Event Scheduled", "NFL — No Event Scheduled", ""},
		{"nhl", "No Event Scheduled", "NHL — No Event Scheduled", ""},
		{"Premier League", "No Event Scheduled", "PREMIER LEAGUE — No Event Scheduled", ""},
		// Already-titled programmes pass through unchanged.
		{"Carolina Hurricanes @ Buffalo Sabres", "", "Carolina Hurricanes @ Buffalo Sabres", ""},
		// Subtitle that isn't the placeholder pattern passes through.
		{"NFL", "Recap", "NFL", "Recap"},
	}
	for _, c := range cases {
		gotT, gotS := rewriteOffSeasonTitle(c.title, c.sub)
		if gotT != c.wantT || gotS != c.wantS {
			t.Errorf("rewriteOffSeasonTitle(%q, %q) = (%q, %q); want (%q, %q)",
				c.title, c.sub, gotT, gotS, c.wantT, c.wantS)
		}
	}
}

// Explicit PPV notices are an intentional guide-grid fallback. Generic idle
// filler remains hidden, and every channel keeps its identity and logo.
func TestEmitXMLTV_RestoresPPVNoticesWithoutGenericFiller(t *testing.T) {
	start := time.Now().UTC().Add(-time.Hour)
	end := start.Add(6 * time.Hour)
	mk := func(ch, name, title string, placeholder bool) store.ProgramOutputRow {
		return store.ProgramOutputRow{
			ChannelID:        uuid.MustParse(ch),
			ChannelName:      name,
			ChannelLogoURL:   "http://conductor.local/logos/" + name + ".png",
			Title:            title,
			StartAt:          start,
			EndAt:            end,
			IsPlaceholder:    placeholder,
			IsPPVPlaceholder: placeholder,
		}
	}
	rows := []store.ProgramOutputRow{
		mk("11111111-1111-1111-1111-111111111111", "AMC", "Jaws 2", false),
		// ppv-off row: IsPlaceholder arrives from source_hash LIKE 'ppv-off:%'.
		mk("22222222-2222-2222-2222-222222222222", "NFLPPV", "NFL — No Event Scheduled", true),
		// Filler from a source that does not set the flag — caught by the
		// isIdleGuidePlaceholder title heuristic during projection.
		mk("33333333-3333-3333-3333-333333333333", "RSN", "No Game Today", false),
	}

	w := httptest.NewRecorder()
	emitXMLTV(w, rows, "http://conductor.local")
	out := w.Body.String()

	// Every channel keeps its <channel> entry, so the lineup and logos
	// Plex renders are unchanged by the programme filter.
	for _, name := range []string{"AMC", "NFLPPV", "RSN"} {
		if !strings.Contains(out, "<display-name>"+name+"</display-name>") {
			t.Errorf("channel %q must still be declared, got:\n%s", name, out)
		}
	}
	if got := strings.Count(out, "<channel id="); got != 3 {
		t.Errorf("expected 3 channels, got %d:\n%s", got, out)
	}

	if !strings.Contains(out, "<title>Jaws 2</title>") {
		t.Errorf("real programme must be emitted, got:\n%s", out)
	}
	if !strings.Contains(out, "<title>PPV idle — NFL — No Event Scheduled</title>") {
		t.Errorf("explicit PPV idle notice missing:\n%s", out)
	}
	if strings.Contains(out, "No Game Today") {
		t.Errorf("generic placeholder must stay hidden:\n%s", out)
	}
	if got := strings.Count(out, "<programme "); got != 2 {
		t.Errorf("expected real programme and PPV notice, got %d:\n%s", got, out)
	}
}

// Bare relay labels have no title-heuristic marker. Exact ppv-off provenance
// must render them as an idle notice, never as the network's actual show.
func TestEmitXMLTV_LabelsRelayPlaceholderAsIdle(t *testing.T) {
	for _, label := range []string{"ESPN2", "CBSSN", "BIG TEN NETWORK ALT"} {
		if isIdleGuidePlaceholder(label, "") {
			t.Fatalf("premise broken: title heuristic now matches relay label %q, "+
				"so this test no longer proves the source_hash path carries it", label)
		}
	}

	start := time.Now().UTC().Add(-time.Hour)
	rows := []store.ProgramOutputRow{{
		ChannelID:   uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		ChannelName: "NCAAF 02",
		Title:       "Jaws 2",
		StartAt:     start,
		EndAt:       start.Add(time.Hour),
	}, {
		// Exact live shape: bare network label, ppv-off source hash.
		ChannelID:        uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		ChannelName:      "NCAAF 02",
		Title:            "ESPN2",
		Description:      `PPV slot NCAAF 02 is currently labeled "ESPN2" — no scheduled event time announced.`,
		Category:         []string{"Sports"},
		StartAt:          start,
		EndAt:            start.Add(6 * time.Hour),
		IsPlaceholder:    true,
		IsPPVPlaceholder: true,
	}}

	w := httptest.NewRecorder()
	emitXMLTV(w, rows, "http://conductor.local")
	out := w.Body.String()

	if strings.Contains(out, "<title>ESPN2</title>") || !strings.Contains(out, "<title>PPV idle — ESPN2</title>") {
		t.Errorf("relay label must be an explicit idle notice, got:\n%s", out)
	}
	if !strings.Contains(out, "<title>Jaws 2</title>") {
		t.Errorf("real programme must still be emitted, got:\n%s", out)
	}
	// The slot keeps its channel entry, so the lineup is unchanged.
	if !strings.Contains(out, "<display-name>NCAAF 02</display-name>") {
		t.Errorf("channel must still be declared, got:\n%s", out)
	}
}
