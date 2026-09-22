package reconcile

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func prog(title, sub string, isMovie bool) Program {
	return Program{
		ProgramID: uuid.New(), ChannelID: uuid.New(),
		ChannelName: "TEST", Title: title, SubTitle: sub, IsMovie: isMovie,
		StartAt: time.Now().Add(2 * time.Hour),
		EndAt:   time.Now().Add(3 * time.Hour),
	}
}

func TestMatchMovies_TMDbIDWins(t *testing.T) {
	p := prog("The Matrix (1999)", "", true)
	p.TMDbID = 603
	got := MatchMovies([]Program{p}, []MovieWant{{TMDbID: 603, Title: "The Matrix", Year: 1999}})
	if len(got) != 1 {
		t.Fatalf("want 1 match, got %d", len(got))
	}
	if got[0].Confidence != confTMDbID || got[0].Reason != "tmdb-id" {
		t.Errorf("expected tmdb-id @ %.2f, got %s @ %.2f", confTMDbID, got[0].Reason, got[0].Confidence)
	}
}

func TestMatchMovies_TitleYearFallbackIsLowConfidence(t *testing.T) {
	p := prog("Heat (1995)", "", true) // no enrichment ids
	got := MatchMovies([]Program{p}, []MovieWant{{Title: "Heat", Year: 1995}})
	if len(got) != 1 || got[0].Reason != "title+year" || got[0].Confidence != confMovieTitle {
		t.Fatalf("expected title+year @ %.2f, got %+v", confMovieTitle, got)
	}
}

func TestMatchMovies_WrongYearNoMatch(t *testing.T) {
	p := prog("Heat (1986)", "", true)
	got := MatchMovies([]Program{p}, []MovieWant{{Title: "Heat", Year: 1995}})
	if len(got) != 0 {
		t.Fatalf("expected no match for wrong year, got %+v", got)
	}
}

func TestMatchMovies_IgnoresEpisodes(t *testing.T) {
	p := prog("The Matrix", "", false) // not a movie row
	p.TMDbID = 603
	if got := MatchMovies([]Program{p}, []MovieWant{{TMDbID: 603}}); len(got) != 0 {
		t.Fatalf("movie matcher should ignore non-movie rows, got %+v", got)
	}
}

func TestMatchEpisodes_EpisodeTitleWins(t *testing.T) {
	p := prog("The Bear", "Funeral", false)
	want := EpisodeWant{SeriesTitle: "The Bear", EpisodeTitle: "Funeral", Season: 3, Episode: 7}
	got := MatchEpisodes([]Program{p}, []EpisodeWant{want})
	if len(got) != 1 || got[0].Reason != "episode-title" || got[0].Confidence != confEpisodeTitle {
		t.Fatalf("expected episode-title @ %.2f, got %+v", confEpisodeTitle, got)
	}
	if got[0].Season != 3 || got[0].Episode != 7 {
		t.Errorf("season/episode not carried: %+v", got[0])
	}
}

func TestMatchEpisodes_OnscreenSE(t *testing.T) {
	p := prog("Severance", "", false)
	p.EpisodeNumOnscreen = "S02E03"
	want := EpisodeWant{SeriesTitle: "Severance", Season: 2, Episode: 3}
	got := MatchEpisodes([]Program{p}, []EpisodeWant{want})
	if len(got) != 1 || got[0].Reason != "onscreen-se" {
		t.Fatalf("expected onscreen-se, got %+v", got)
	}
}

func TestMatchEpisodes_AirDateIsLowConfidence(t *testing.T) {
	day := time.Date(2026, 7, 1, 20, 0, 0, 0, time.UTC)
	p := prog("Nova", "", false)
	p.IsNew = true
	p.OriginalAirDate = &day
	want := EpisodeWant{SeriesTitle: "Nova", AirDate: &day}
	got := MatchEpisodes([]Program{p}, []EpisodeWant{want})
	if len(got) != 1 || got[0].Reason != "airdate+new" || got[0].Confidence != confAirDate {
		t.Fatalf("expected airdate+new @ %.2f, got %+v", confAirDate, got)
	}
}

func TestMatchEpisodes_SeriesMismatchNoMatch(t *testing.T) {
	p := prog("Some Other Show", "Funeral", false)
	want := EpisodeWant{SeriesTitle: "The Bear", EpisodeTitle: "Funeral", Season: 3, Episode: 7}
	if got := MatchEpisodes([]Program{p}, []EpisodeWant{want}); len(got) != 0 {
		t.Fatalf("episode title alone must not match across series, got %+v", got)
	}
}

func TestMatchEpisodes_PicksEarliestAiring(t *testing.T) {
	early := prog("The Bear", "Funeral", false)
	early.StartAt = time.Now().Add(1 * time.Hour)
	late := prog("The Bear", "Funeral", false)
	late.StartAt = time.Now().Add(5 * time.Hour)
	want := EpisodeWant{SeriesTitle: "The Bear", EpisodeTitle: "Funeral", Season: 3, Episode: 7}
	got := MatchEpisodes([]Program{late, early}, []EpisodeWant{want})
	if len(got) != 1 || !got[0].Program.StartAt.Equal(early.StartAt) {
		t.Fatalf("expected earliest airing, got %+v", got)
	}
}

func TestMatchEpisodes_TVDBIdBridgesTitleMismatch(t *testing.T) {
	// EPG carries a localized series title but the same TVDB id; the want's
	// English title won't title-match, so only the TVDB-id index can find it.
	p := prog("Kimetsu no Yaiba", "Funeral", false)
	p.TVDBID = 355567
	want := EpisodeWant{
		SeriesTitle: "Demon Slayer", TVDBID: 355567,
		EpisodeTitle: "Funeral", Season: 1, Episode: 5,
	}
	got := MatchEpisodes([]Program{p}, []EpisodeWant{want})
	if len(got) != 1 || got[0].Reason != "episode-title" {
		t.Fatalf("expected tvdb-id to bridge the title mismatch, got %+v", got)
	}
}

func TestMatchEpisodes_NoDuplicateWhenTitleAndTVDBBothMatch(t *testing.T) {
	p := prog("The Bear", "Funeral", false)
	p.TVDBID = 999
	want := EpisodeWant{
		SeriesTitle: "The Bear", TVDBID: 999,
		EpisodeTitle: "Funeral", Season: 3, Episode: 7,
	}
	if got := MatchEpisodes([]Program{p}, []EpisodeWant{want}); len(got) != 1 {
		t.Fatalf("program in both indexes must yield one match, got %d", len(got))
	}
}

func TestBuildOutputPath(t *testing.T) {
	w := &Worker{OutputDir: "/dvr"}
	movie := Match{Kind: "movie", Program: prog("Heat (1995)", "", true)}
	if p := w.buildOutputPath(movie, ""); p != "/dvr/Movies/Heat (1995)/Heat (1995).ts" {
		t.Errorf("movie path: %s", p)
	}
	ep := Match{Kind: "episode", Season: 3, Episode: 7,
		Program: prog("The Bear", "Funeral", false)}
	want := "/dvr/TV/The Bear/Season 03/The Bear.S03E07.Funeral.ts"
	if p := w.buildOutputPath(ep, "S03E07"); p != want {
		t.Errorf("episode path: %s want %s", p, want)
	}
}

// A recording must be named from the *arr's series title, not the EPG's. The
// matcher unions title matches with TVDb-id matches precisely so an alternate
// EPG title still matches the right want — and when the id is what matched,
// the titles differ. Naming the file from the EPG title made Sonarr reject the
// import as "Unknown Series", so the episode stayed missing and the reconciler
// re-booked every re-airing (measured: EPG "90 Day" vs Sonarr "90 Day Fiancé",
// one episode recorded 11 times, 74 GB across that series).
func TestBuildOutputPathPrefersArrTitleOverEPGTitle(t *testing.T) {
	w := &Worker{OutputDir: "/dvr"}

	ep := Match{Kind: "episode", Season: 3, Episode: 9,
		Program:  prog("90 Day", "The Last Resort", false),
		ArrTitle: "90 Day Fiancé"}
	want := "/dvr/TV/90 Day Fiancé/Season 03/90 Day Fiancé.S03E09.The Last Resort.ts"
	if got := w.buildOutputPath(ep, "S03E09"); got != want {
		t.Errorf("episode path:\n got %s\nwant %s", got, want)
	}

	movie := Match{Kind: "movie", ArrTitle: "Heat (1995)",
		Program: prog("Heat", "", true)}
	if got := w.buildOutputPath(movie, ""); got != "/dvr/Movies/Heat (1995)/Heat (1995).ts" {
		t.Errorf("movie path: %s", got)
	}

	// plex-gap matches carry no ArrTitle — the gap only forms when the EPG
	// title already normalizes onto the owned series key, so falling back to
	// the EPG title is correct there, not a bug.
	gap := Match{Kind: "episode", Source: "plex-gap", Season: 3, Episode: 39,
		Program: prog("Impractical Jokers", "", false)}
	wantGap := "/dvr/TV/Impractical Jokers/Season 03/Impractical Jokers.S03E39.ts"
	if got := w.buildOutputPath(gap, "S03E39"); got != wantGap {
		t.Errorf("plex-gap path:\n got %s\nwant %s", got, wantGap)
	}
}

// The matcher must carry the want's series title onto the Match, otherwise
// buildOutputPath has nothing to prefer.
func TestMatchEpisodesCarriesArrTitle(t *testing.T) {
	p := prog("90 Day", "The Last Resort", false)
	p.TVDBID = 4242
	p.EpisodeNumOnscreen = "S03E09"
	want := EpisodeWant{SeriesTitle: "90 Day Fiancé", TVDBID: 4242, Season: 3, Episode: 9}
	got := MatchEpisodes([]Program{p}, []EpisodeWant{want})
	if len(got) != 1 {
		t.Fatalf("want 1 match, got %d", len(got))
	}
	if got[0].ArrTitle != "90 Day Fiancé" {
		t.Errorf("ArrTitle = %q, want the Sonarr series title", got[0].ArrTitle)
	}
}

// A SxxExx match while both episode titles are present and DISAGREE is weak
// evidence: sibling shows in one franchise share a numbering space and the EPG
// tags them with the parent's ids. Real case (TLC, 2026-08-27): EPG "90 Day" /
// "The Last Resort" S03E09 matched Sonarr's "90 Day Fiancé" S03E09 -
// "What Do You Know About Love?" at 0.90 on the SxxExx alone. Different show,
// same slot. It must stay below the auto-record threshold.
func TestOnscreenSEDoesNotAutoRecordWhenEpisodeTitlesConflict(t *testing.T) {
	p := prog("90 Day", "The Last Resort", false)
	p.TVDBID = 277092 // the EPG tags the spinoff with the parent franchise id
	p.EpisodeNumOnscreen = "S03E09"
	want := EpisodeWant{
		SeriesTitle: "90 Day Fiancé", TVDBID: 277092,
		Season: 3, Episode: 9, EpisodeTitle: "What Do You Know About Love?",
	}
	got := MatchEpisodes([]Program{p}, []EpisodeWant{want})
	if len(got) != 1 {
		t.Fatalf("want the candidate kept for visibility, got %d matches", len(got))
	}
	m := got[0]
	if m.Reason != "onscreen-se-title-conflict" {
		t.Errorf("reason = %q, want onscreen-se-title-conflict", m.Reason)
	}
	w := &Worker{Threshold: 0.80}
	if w.eligible(m) {
		t.Errorf("confidence %.2f is auto-recordable; a conflicting episode "+
			"title must stay below the threshold", m.Confidence)
	}
}

// The veto must not fire when the EPG simply has no subtitle — that is the
// ordinary case for many channels and SxxExx is the only signal available.
func TestOnscreenSEStillAutoRecordsWithoutAnEPGSubtitle(t *testing.T) {
	p := prog("Body Cam", "", false)
	p.TVDBID = 321
	p.EpisodeNumOnscreen = "S11E07"
	want := EpisodeWant{
		SeriesTitle: "Body Cam", TVDBID: 321,
		Season: 11, Episode: 7, EpisodeTitle: "Line of Duty",
	}
	got := MatchEpisodes([]Program{p}, []EpisodeWant{want})
	if len(got) != 1 {
		t.Fatalf("want 1 match, got %d", len(got))
	}
	if got[0].Reason != "onscreen-se" {
		t.Errorf("reason = %q, want onscreen-se", got[0].Reason)
	}
	w := &Worker{Threshold: 0.80}
	if !w.eligible(got[0]) {
		t.Errorf("confidence %.2f should still auto-record", got[0].Confidence)
	}
}
