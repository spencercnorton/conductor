package epg_test

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/epg"
	"github.com/spencercnorton/conductor/internal/store"
)

func TestCleanText(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"Hello&nbsp;world", "Hello world"},
		{"<p>Some <em>HTML</em> here</p>", "Some HTML here"},
		{"  multi   space  \n trim ", "multi space trim"},
		{"smart “quotes” and — dashes", `smart "quotes" and - dashes`},
		{"", ""},
	}
	for _, tc := range cases {
		got := epg.CleanText(tc.in)
		if got != tc.want {
			t.Errorf("CleanText(%q): got %q want %q", tc.in, got, tc.want)
		}
	}
}

func TestStripProviderSuperscriptBadges(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		// Real-world examples from iBoost via Dispatcharr (the
		// programme titles seen in the Plex guide on 2026-05-08):
		{"live phonetic-extensions super", "Bloomberg Surveillance ᴸᶦᵛᵉ", "Bloomberg Surveillance"},
		{"hd phonetic-extensions super", "Squawk Box ᴴᴰ", "Squawk Box"},
		{"raw phonetic-extensions super", "FOX BUSINESS ᴿᴬᵂ", "FOX BUSINESS"},
		{"new phonetic-extensions super", "The Bachelor ᴺᴱᵂ", "The Bachelor"},
		// Numeric super (Superscripts and Subscripts block + phonetic ext)
		{"60fps super", "NBC Sports ⁶⁰ᶠᵖˢ", "NBC Sports"},
		{"4K super", "Discovery ⁴ᴷ", "Discovery"},
		// Multi-token at end ("ᴴᴰ ⁶⁰ᶠᵖˢ")
		{"two badges", "Showtime ᴴᴰ ⁶⁰ᶠᵖˢ", "Showtime"},
		// Slash-delimited ("ᴴᴰ/ᴿᴬᵂ")
		{"slash badges", "FX ᴴᴰ/ᴿᴬᵂ", "FX"},
		// Empty / no match — pass-through
		{"empty", "", ""},
		{"no badge", "Plain Title", "Plain Title"},
		{"unicode in middle is preserved", "Café ᴴᴰ", "Café"},
		// Edge: badge-only string (after stripping whitespace) → empty
		{"badge only", "ᴸᶦᵛᵉ", ""},
		// Mid-string badge is NOT stripped (anchored at end + needs space delimiter)
		{"badge mid-title preserved", "Theᴸᶦᵛᵉ Today", "Theᴸᶦᵛᵉ Today"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := epg.StripProviderSuperscriptBadges(tc.in)
			if got != tc.want {
				t.Errorf("StripProviderSuperscriptBadges(%q): got %q want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestSplitTitleSubTitle(t *testing.T) {
	cases := []struct {
		in       string
		title    string
		subTitle string
	}{
		{"The Bear: System", "The Bear", "System"},
		{"30 for 30 - The Two Escobars", "30 for 30", "The Two Escobars"},
		{"Movie Night", "Movie Night", ""},
		{"", "", ""},
	}
	for _, tc := range cases {
		t1, st := epg.SplitTitleSubTitle(tc.in)
		if t1 != tc.title || st != tc.subTitle {
			t.Errorf("SplitTitleSubTitle(%q): got (%q, %q) want (%q, %q)",
				tc.in, t1, st, tc.title, tc.subTitle)
		}
	}
}

func TestParseEpisodeNum(t *testing.T) {
	// Pure xmltv_ns "2.6." should produce S03E07.
	xn, on := epg.ParseEpisodeNum([]epg.EpisodeNumInput{
		{System: "xmltv_ns", Value: "2.6."},
	})
	if xn != "2.6." || on != "S03E07" {
		t.Errorf("xmltv_ns only: got (%q,%q) want (2.6., S03E07)", xn, on)
	}

	// Pure onscreen "S03E07" should produce 2.6.
	xn, on = epg.ParseEpisodeNum([]epg.EpisodeNumInput{
		{System: "onscreen", Value: "S03E07"},
	})
	if xn != "2.6." || on != "S03E07" {
		t.Errorf("onscreen only: got (%q,%q) want (2.6., S03E07)", xn, on)
	}

	// "0.0." → S01E01
	xn, on = epg.ParseEpisodeNum([]epg.EpisodeNumInput{
		{System: "xmltv_ns", Value: "0.0."},
	})
	if on != "S01E01" {
		t.Errorf("0.0. should produce S01E01, got %q", on)
	}

	// xmltv_ns with episode-counts "0.1/2." should still parse
	xn, on = epg.ParseEpisodeNum([]epg.EpisodeNumInput{
		{System: "xmltv_ns", Value: "0.1/2."},
	})
	if on != "S01E02" {
		t.Errorf("0.1/2. should produce S01E02, got %q", on)
	}
}

func TestParseEpisodeNumFromDescription(t *testing.T) {
	xmltvNS, onscreen := epg.ParseEpisodeNumFromDescription(
		"S20 E02 - Man Down. Detectives investigate a construction-site death.",
	)
	if xmltvNS != "19.1." || onscreen != "S20E02" {
		t.Fatalf(
			"episode description: got (%q,%q), want (19.1.,S20E02)",
			xmltvNS,
			onscreen,
		)
	}

	for _, desc := range []string{
		"Tonight: S20 E02 - Man Down",
		"S20E02 - formal no-space style is handled by episode-num, not prose fallback",
		"S00 E02 - invalid season",
		"S20 E00 - invalid episode",
		"Season 20 Episode 2",
	} {
		if xmltvNS, onscreen := epg.ParseEpisodeNumFromDescription(desc); xmltvNS != "" || onscreen != "" {
			t.Errorf("ParseEpisodeNumFromDescription(%q) = (%q,%q), want empty", desc, xmltvNS, onscreen)
		}
	}
}

func TestIsLikelyMovie(t *testing.T) {
	if !epg.IsLikelyMovie([]string{"Movie", "Action"}, 0, "Heat", false) {
		t.Error("category=Movie should classify as movie")
	}
	if !epg.IsLikelyMovie([]string{"Drama"}, 130, "Heat (1995)", false) {
		t.Error("year-in-title + no episode num should classify as movie")
	}
	if !epg.IsLikelyMovie([]string{"Drama"}, 110, "Long Drama", false) {
		t.Error("runtime > 80 min + no episode num should classify as movie")
	}
	if epg.IsLikelyMovie([]string{"Drama"}, 60, "Episode 5", true) {
		t.Error("episode num present should NOT classify as movie")
	}
}

func TestIsLiveBySignal(t *testing.T) {
	if !epg.IsLiveBySignal(true, nil, "anything", false) {
		t.Error("xmltv <live/> flag should always count as live")
	}
	if epg.IsLiveBySignal(false, []string{"Sports"}, "Game", false) {
		t.Error("Sports category alone must not imply live")
	}
	if epg.IsLiveBySignal(false, []string{"News"}, "Evening News", false) {
		t.Error("News category alone must not imply live")
	}
	if epg.IsLiveBySignal(false, []string{"News"}, "Encore: Evening News", false) {
		t.Error("News with 'Encore' should NOT be live")
	}
	if !epg.IsLiveBySignal(false, nil, "LIVE: Special Event", false) {
		t.Error("explicit LIVE prefix should be live")
	}
	splitTitle, _ := epg.SplitTitleSubTitle("LIVE: Special Event")
	if splitTitle != "LIVE" || !epg.IsLiveBySignal(false, nil, splitTitle, false) {
		t.Error("LIVE marker should survive the ingest title/sub-title split")
	}
	if !epg.IsLiveBySignal(false, nil, "Championship Final (LIVE)", false) {
		t.Error("parenthesized LIVE marker should be live")
	}
	if epg.IsLiveBySignal(false, nil, "Live with Kelly and Mark", false) {
		t.Error("show title containing Live must not imply a live broadcast")
	}
	if epg.IsLiveBySignal(false, []string{"Sports"}, "No Game Today", false) {
		t.Error("idle sports placeholder should NOT be live")
	}
	if epg.IsLiveBySignal(false, []string{"Sports"}, "Sports Movie", true) {
		t.Error("isMovie should override live signals")
	}
}

func TestIsNewBySignal(t *testing.T) {
	newYork, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name       string
		explicit   bool
		oad, start time.Time
		repeat     bool
		want       bool
	}{
		{name: "explicit new", explicit: true, start: time.Now(), want: true},
		{name: "explicit repeat wins", explicit: true, start: time.Now(), repeat: true},
		{
			name:  "same civil date despite later OAD clock",
			oad:   time.Date(2026, 8, 22, 23, 0, 0, 0, time.UTC),
			start: time.Date(2026, 8, 22, 0, 5, 0, 0, time.UTC), want: true,
		},
		{
			name:  "date rollover",
			oad:   time.Date(2026, 8, 21, 0, 0, 0, 0, time.UTC),
			start: time.Date(2026, 8, 22, 0, 5, 0, 0, newYork), want: true,
		},
		{
			name:  "seven civil days across spring gap",
			oad:   time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC),
			start: time.Date(2026, 3, 8, 3, 30, 0, 0, newYork), want: true,
		},
		{
			name:  "seven civil days across fall fold",
			oad:   time.Date(2026, 10, 25, 0, 0, 0, 0, time.UTC),
			start: time.Date(2026, 11, 1, 1, 30, 0, 0, newYork), want: true,
		},
		{
			name:  "eight civil days",
			oad:   time.Date(2026, 8, 14, 0, 0, 0, 0, time.UTC),
			start: time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC),
		},
		{
			name:  "future OAD",
			oad:   time.Date(2026, 8, 23, 0, 0, 0, 0, time.UTC),
			start: time.Date(2026, 8, 22, 23, 59, 0, 0, time.UTC),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := epg.IsNewBySignal(tc.explicit, tc.oad, tc.start, tc.repeat); got != tc.want {
				t.Fatalf("IsNewBySignal() = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestProgramContentHash(t *testing.T) {
	t1 := time.Date(2026, 5, 6, 20, 0, 0, 0, time.UTC)
	base := store.EPGProgram{
		ChannelID: uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		StartAt:   t1, EndAt: t1.Add(time.Hour),
		Title: "title", Description: "desc",
		Category: []string{"Series"},
	}
	a := epg.ProgramContentHash(base)
	if !strings.HasPrefix(a, "xmltv:") {
		t.Fatalf("XMLTV content hash %q lacks explicit provenance prefix", a)
	}
	if a != epg.ProgramContentHash(base) {
		t.Error("hash should be stable for identical content")
	}

	mutations := map[string]store.EPGProgram{}
	m := base
	m.Title = "other title"
	mutations["title"] = m
	m = base
	m.Description = "new description"
	mutations["description"] = m
	m = base
	m.EndAt = t1.Add(2 * time.Hour)
	mutations["end time"] = m
	m = base
	m.Category = []string{"Series", "Drama"}
	mutations["category"] = m
	m = base
	m.IsLive = true
	mutations["live flag"] = m
	m = base
	m.SourcePriority = 5
	mutations["source priority"] = m
	m = base
	m.SourcePosterURL = "https://images.example/poster.jpg"
	mutations["source poster"] = m
	m = base
	m.ProviderEpisodeID = "EP012345670001"
	mutations["provider episode id"] = m
	m = base
	m.NewExplicit = true
	mutations["explicit new provenance"] = m
	m = base
	m.PreviouslyShownExplicit = true
	mutations["explicit repeat provenance"] = m
	m = base
	airingCivilDate := time.Date(2026, 5, 6, 0, 0, 0, 0, time.UTC)
	m.AiringCivilDate = &airingCivilDate
	mutations["airing civil date"] = m
	for name, mut := range mutations {
		if epg.ProgramContentHash(mut) == a {
			t.Errorf("hash should differ when %s changes", name)
		}
	}

	// Field-boundary ambiguity: ["ab"] vs ["a","b"]-style shifts must not
	// collide (the count prefix + separators guard this).
	x, y := base, base
	x.Category = []string{"ab"}
	y.Category = []string{"a", "b"}
	if epg.ProgramContentHash(x) == epg.ProgramContentHash(y) {
		t.Error("hash should differ for different category splits")
	}
}

func TestParseXMLTVMinimal(t *testing.T) {
	doc := `<?xml version="1.0" encoding="UTF-8"?>
<tv>
  <channel id="ch.test">
    <display-name>Test Channel</display-name>
    <icon src="http://example.com/logo.png"/>
  </channel>
  <programme channel="ch.test" start="20260506200000 +0000" stop="20260506220000 +0000">
    <title>Test Movie</title>
    <desc>A test description</desc>
    <date>20240101</date>
    <category>Movie</category>
    <category>Drama</category>
    <length units="minutes">120</length>
  </programme>
</tv>`
	parsed, err := epg.ParseXMLTV(strings.NewReader(doc))
	if err != nil {
		t.Fatal(err)
	}
	_ = parsed // ParseXMLTV is package-private internals; smoke test only
}
