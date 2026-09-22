package epg

import (
	"encoding/xml"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/spencercnorton/conductor/internal/store"
)

func TestRecognizedEpisodeProviderLiveCueIsAiringIndependent(t *testing.T) {
	start := time.Date(2026, 9, 6, 16, 0, 0, 0, time.UTC)
	for _, cue := range []struct{ title, subtitle, wantTitle, wantSubtitle string }{
		{"LIVE: Recognized Series", "The Finale [LIVE]", "Recognized Series", "The Finale"},
		{"LIVE Broadcast — Recognized Series", "LIVE - The Finale", "Recognized Series", "The Finale"},
		{"Recognized Series (LIVE)", "LIVE Coverage: The Finale", "Recognized Series", "The Finale"},
		{"LIVE: Recognized Series [LIVE]", "LIVE: The Finale (LIVE)", "Recognized Series", "The Finale"},
	} {
		t.Run(cue.title, func(t *testing.T) {
			var sharedTitle, sharedSubtitle string
			for _, state := range []struct {
				name                    string
				live, explicit, earlier bool
			}{
				{"live", true, false, false}, {"explicit repeat", true, true, false}, {"earlier actual airing", true, false, true}, {"source live already false", false, true, false}, {"unflagged", false, false, false},
			} {
				t.Run(state.name, func(t *testing.T) {
					row := store.ProgramOutputRow{Title: cue.title, SubTitle: cue.subtitle, HasEpisodeIdentity: true, EpisodeNumXMLTV: "2.6.", EpisodeNumOnscreen: "S03E07", StartAt: start, EndAt: start.Add(time.Hour), IsLive: state.live, PreviouslyShownExplicit: state.explicit, HasEarlierAiring: state.earlier, IsNew: false}
					original := row
					got := projectProgramsForOutput([]store.ProgramOutputRow{row}, start)[0]
					if got.Title != cue.wantTitle || got.SubTitle != cue.wantSubtitle {
						t.Fatalf("provider cue survived recognized %s: title=%q subtitle=%q", state.name, got.Title, got.SubTitle)
					}
					if sharedTitle == "" {
						sharedTitle, sharedSubtitle = got.Title, got.SubTitle
					} else if got.Title != sharedTitle || got.SubTitle != sharedSubtitle {
						t.Fatal("airing flags changed Plex shared episode metadata")
					}
					if strings.Contains(got.Title, "LIVE broadcast") || strings.Contains(got.SubTitle, "LIVE broadcast") {
						t.Fatal("recognized episode gained visible broadcast descriptor")
					}
					if got.EpisodeNumXMLTV != row.EpisodeNumXMLTV || got.EpisodeNumOnscreen != row.EpisodeNumOnscreen || got.IsNew != row.IsNew || got.HasEpisodeIdentity != row.HasEpisodeIdentity {
						t.Fatal("formal episode/New identity changed")
					}
					if got.IsLive != (state.live && !state.explicit && !state.earlier) {
						t.Fatalf("compatibility live flag changed incorrectly: %+v", got)
					}
					if !reflect.DeepEqual(row, original) {
						t.Fatal("projection mutated input")
					}
					again := projectProgramsForOutput([]store.ProgramOutputRow{got}, start.Add(24*time.Hour))[0]
					if !reflect.DeepEqual(again, got) {
						t.Fatalf("projection is not idempotent: first=%+v again=%+v", got, again)
					}
				})
			}
		})
	}
}

func TestRecognizedEpisodeLiveCuePreservesOrdinaryNames(t *testing.T) {
	for _, title := range []string{"Live with Kelly and Mark", "Saturday Night Live", "Live PD", "Live-Action Adventures", "Live", "Live Coverage", "Live Broadcast", "Recognized Series"} {
		for _, repeat := range []bool{false, true} {
			row := store.ProgramOutputRow{Title: title, SubTitle: title, HasEpisodeIdentity: true, EpisodeNumOnscreen: "S01E01", IsLive: true, PreviouslyShownExplicit: repeat}
			got := projectProgramsForOutput([]store.ProgramOutputRow{row}, time.Time{})[0]
			if got.Title != title || got.SubTitle != title {
				t.Fatalf("ordinary name %q changed: %+v", title, got)
			}
		}
	}
}

func TestRecognizedEpisodeRepeatXMLHasNoProviderLiveCue(t *testing.T) {
	start := time.Date(2026, 9, 6, 16, 0, 0, 0, time.UTC)
	first := store.ProgramOutputRow{Title: "LIVE: Recognized Series", SubTitle: "LIVE: The Finale", HasEpisodeIdentity: true, EpisodeNumXMLTV: "2.6.", EpisodeNumOnscreen: "S03E07", StartAt: start, EndAt: start.Add(time.Hour), IsLive: true}
	repeat := first
	repeat.StartAt = start.Add(24 * time.Hour)
	repeat.EndAt = start.Add(25 * time.Hour)
	repeat.PreviouslyShownExplicit = true
	response := recordXMLTVRepresentation(t, http.MethodGet, []store.ProgramOutputRow{first, repeat}, start, start, "", "")
	var guide struct {
		Programs []struct {
			Title    string    `xml:"title"`
			Subtitle string    `xml:"sub-title"`
			Live     *struct{} `xml:"live"`
			Repeat   *struct{} `xml:"previously-shown"`
			Episodes []struct {
				System string `xml:"system,attr"`
				Value  string `xml:",chardata"`
			} `xml:"episode-num"`
		} `xml:"programme"`
	}
	if err := xml.Unmarshal(response.Body.Bytes(), &guide); err != nil {
		t.Fatal(err)
	}
	if len(guide.Programs) != 2 {
		t.Fatalf("programmes=%d", len(guide.Programs))
	}
	a, b := guide.Programs[0], guide.Programs[1]
	if a.Title != "Recognized Series" || b.Title != a.Title || a.Subtitle != "The Finale" || b.Subtitle != a.Subtitle || !reflect.DeepEqual(a.Episodes, b.Episodes) {
		t.Fatalf("shared metadata/cues wrong: %s", response.Body.String())
	}
	if a.Live == nil || b.Live != nil || b.Repeat == nil {
		t.Fatalf("airing-specific flags wrong: %s", response.Body.String())
	}
}
