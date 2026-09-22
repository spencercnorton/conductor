package epg

import (
	"bytes"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/spencercnorton/conductor/internal/store"
)

// Plex imports a whole guide before a game starts. An ETag change at kickoff
// cannot fix the already imported title unless Plex makes another request.
func TestScheduledLiveCueSurvivesCachedGuide(t *testing.T) {
	start := time.Date(2026, 9, 6, 16, 0, 0, 0, time.UTC)
	rows := []store.ProgramOutputRow{{
		Title: "Championship Final", StartAt: start,
		EndAt: start.Add(3 * time.Hour), IsLive: true,
	}}
	before := recordXMLTVRepresentation(t, http.MethodGet, rows, start.Add(-time.Hour), start.Add(-time.Hour), "", "")
	if !strings.Contains(before.Body.String(), "LIVE broadcast — Championship Final") {
		t.Fatalf("Plex's pre-event guide import lacks the broadcast descriptor: %s", before.Body.String())
	}
	for _, at := range []time.Time{start, start.Add(time.Hour), rows[0].EndAt} {
		response := recordXMLTVRepresentation(t, http.MethodGet, rows, start.Add(-time.Hour), at, "", "")
		if !bytes.Equal(response.Body.Bytes(), before.Body.Bytes()) {
			t.Fatalf("an unchanged airing needs another guide import at %s", at)
		}
		cached := recordXMLTVRepresentation(t, http.MethodGet, rows, start.Add(-time.Hour), at, before.Header().Get("ETag"), "")
		if cached.Code != http.StatusNotModified {
			t.Fatalf("stable descriptor invalidates cache at %s: %d", at, cached.Code)
		}
	}
}

func TestScheduledLiveCuePreservesEpisodeAndNewSemantics(t *testing.T) {
	start := time.Date(2026, 9, 6, 16, 0, 0, 0, time.UTC)
	for _, isNew := range []bool{false, true} {
		row := store.ProgramOutputRow{
			Title: "Recognized Series", SubTitle: "The Final",
			EpisodeNumXMLTV: "2.6.", EpisodeNumOnscreen: "S03E07", HasEpisodeIdentity: true,
			StartAt: start, EndAt: start.Add(time.Hour), IsLive: true, IsNew: isNew,
		}
		projected := projectProgramsForOutput([]store.ProgramOutputRow{row}, start.Add(-time.Hour))[0]
		if projected.Title != row.Title || projected.SubTitle != row.SubTitle || !projected.IsLive ||
			projected.EpisodeNumXMLTV != row.EpisodeNumXMLTV || projected.EpisodeNumOnscreen != row.EpisodeNumOnscreen || projected.IsNew != isNew {
			t.Fatalf("broadcast description changed episode/first-run identity: %+v", projected)
		}
		reprojected := projectProgramsForOutput([]store.ProgramOutputRow{projected}, start.Add(time.Hour))[0]
		if reprojected.Title != projected.Title || reprojected.SubTitle != projected.SubTitle {
			t.Fatalf("repeated projection duplicated the descriptor: %+v", reprojected)
		}
		row.SubTitle = ""
		if got := projectProgramsForOutput([]store.ProgramOutputRow{row}, start)[0]; got.SubTitle != "" || got.Title != row.Title || !got.IsLive {
			t.Fatalf("missing subtitle changed series identity or asserts now: %+v", got)
		}
	}
}

func TestScheduledLiveCueRejectsRepeatPlaceholderAndUnprovedLive(t *testing.T) {
	start := time.Date(2026, 9, 6, 16, 0, 0, 0, time.UTC)
	for _, row := range []store.ProgramOutputRow{
		{Title: "LIVE: Replay", IsLive: true, PreviouslyShownExplicit: true},
		{Title: "Known Repeat", IsLive: true, HasEarlierAiring: true},
		{Title: "Encore: Championship", IsLive: true},
		{Title: "Championship [Replay]", IsLive: true},
		{Title: "LIVE: No Event Scheduled", IsLive: true, IsPlaceholder: true},
		{Title: "Sports Show", Category: []string{"Sports"}},
		{Title: "News", Category: []string{"News"}},
	} {
		row.StartAt, row.EndAt = start, start.Add(time.Hour)
		response := recordXMLTVRepresentation(t, http.MethodGet, []store.ProgramOutputRow{row}, start, start, "", "")
		if strings.Contains(response.Body.String(), "<live/>") || strings.Contains(response.Body.String(), "LIVE broadcast") || strings.Contains(response.Body.String(), "LIVE:") {
			t.Fatalf("false live broadcast claim for %+v: %s", row, response.Body.String())
		}
		if !row.IsPlaceholder && !strings.Contains(response.Body.String(), "<previously-shown/>") {
			t.Fatalf("live suppression changed conservative first-run state: %s", response.Body.String())
		}
	}
}

func TestScheduledLiveEpisodeKeepsSharedMetadataOnLiveAndRepeat(t *testing.T) {
	start := time.Date(2026, 9, 6, 16, 0, 0, 0, time.UTC)
	row := store.ProgramOutputRow{Title: "Recognized Series", SubTitle: "The Final", HasEpisodeIdentity: true,
		EpisodeNumOnscreen: "S03E07", StartAt: start, EndAt: start.Add(time.Hour), IsLive: true}
	first := projectProgramsForOutput([]store.ProgramOutputRow{row}, start)[0]
	row.HasEarlierAiring = true
	row.StartAt, row.EndAt = start.Add(24*time.Hour), start.Add(25*time.Hour)
	repeat := projectProgramsForOutput([]store.ProgramOutputRow{row}, start)[0]
	if first.Title != repeat.Title || first.SubTitle != repeat.SubTitle || first.EpisodeNumOnscreen != repeat.EpisodeNumOnscreen || !first.IsLive || repeat.IsLive {
		t.Fatalf("airing flags changed shared episode identity: first=%+v repeat=%+v", first, repeat)
	}
}

func TestExplicitLiveFlagHonorsNarrowNegativeEvidence(t *testing.T) {
	for _, title := range []string{"Encore: Final", "Replay - Final", "Final (Replay)", "LIVE: Final [Repeat]", "LIVE broadcast — Rebroadcast: Final", "LIVE: No Event Scheduled"} {
		if IsLiveBySignal(true, []string{"Sports"}, title, false) {
			t.Errorf("contradictory source live flag overrode %q", title)
		}
	}
	for _, title := range []string{"Instant Replay", "No Game No Life Special", "Live with Kelly and Mark", "2026 Championship Final"} {
		if !IsLiveBySignal(true, []string{"Sports"}, title, true) {
			t.Errorf("unproved movie/title heuristic suppressed explicit live for %q", title)
		}
	}
	if IsLiveBySignal(false, nil, "Live with Kelly and Mark", false) {
		t.Fatal("ordinary series title gained inferred live state")
	}
}
