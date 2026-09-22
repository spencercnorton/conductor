package epg

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/spencercnorton/conductor/internal/ppvparse"
	"github.com/spencercnorton/conductor/internal/store"
	"github.com/google/uuid"
)

func TestPPVIdleNoticeKeepsEventBoundaryWithoutAiringIdentity(t *testing.T) {
	loc, err := time.LoadLocation("America/Denver")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 6, 18, 5, 0, 0, time.UTC)
	channelID := uuid.New()
	event := store.EPGProgram{ChannelID: channelID, Title: "Home & Away", StartAt: now.Add(time.Hour), EndAt: now.Add(3 * time.Hour)}
	notices := ppvparse.OffSlotPrograms(channelID, ppvparse.ParsedStream{Slot: "PPV 01", Status: ppvparse.StatusUpcoming}, "fixture", &event, now, loc)
	if len(notices) == 0 || !notices[0].EndAt.Equal(event.StartAt) {
		t.Fatal("producer did not preserve the real event's kickoff boundary")
	}
	n := notices[0]
	row := store.ProgramOutputRow{
		ChannelID: channelID, ChannelEPGID: "ppv.test", ChannelName: "PPV 01",
		Title: n.Title, Description: n.Description, StartAt: n.StartAt, EndAt: n.EndAt,
		IsPlaceholder: true, IsPPVPlaceholder: true,
		// Poison every programme/enrichment field: the idle notice must never
		// acquire show/movie identity or first-run/live flags after projection.
		IsMovie: true, IsLive: true, IsNew: true, IsPremiere: true, IsFinale: true,
		HasEpisodeIdentity: true, EpisodeNumXMLTV: "1.2.", EpisodeNumOnscreen: "S02E03",
		EnrichmentTMDbID: 42, EnrichmentIMDbID: "tt123", EnrichmentOverview: "Wrong film plot",
		EnrichmentPosterURL: "http://example.test/poster.jpg", SourcePosterURL: "http://example.test/source.jpg",
		Category: []string{"Movie"}, Rating: "R", OriginalAirDate: &now,
	}
	for _, at := range []time.Time{now, now.Add(10 * time.Minute)} {
		w := httptest.NewRecorder()
		emitXMLTVProjected(w, projectProgramsForOutput([]store.ProgramOutputRow{row}, at), "http://conductor.test")
		out := w.Body.String()
		if !strings.Contains(out, "<title>Next Sep 6 1:05 PM MDT — Home &amp; Away</title>") ||
			!strings.Contains(out, "stop=\""+formatXMLTVTime(event.StartAt)+"\"") ||
			!strings.Contains(out, "PPV guide notice, not a currently airing event.") ||
			strings.Count(out, "<previously-shown/>") != 1 {
			t.Fatalf("lost explicit dated notice or exact boundary:\n%s", out)
		}
		for _, forbidden := range []string{"<live", "<new", "<premiere", "<finale", "<episode-num", "<date", "<image", "<category", "<rating", "Wrong film plot", "<sub-title"} {
			if strings.Contains(out, forbidden) {
				t.Fatalf("notice gained programme metadata %q:\n%s", forbidden, out)
			}
		}
	}
	if row.Title != n.Title || !row.IsNew || row.EnrichmentTMDbID != 42 {
		t.Fatal("projection changed the stored input model")
	}

	// Ordinary movie/show serialization is byte-for-byte unchanged alongside
	// notices; their legitimate metadata and exact airing intervals survive.
	for _, movie := range []bool{false, true} {
		real := store.ProgramOutputRow{ChannelID: channelID, ChannelEPGID: "ppv.test", Title: "Real programme", StartAt: event.StartAt, EndAt: event.EndAt, IsMovie: movie, IsNew: true}
		want := httptest.NewRecorder()
		emitProgramme(want, real, "http://conductor.test")
		got := httptest.NewRecorder()
		emitXMLTVProjected(got, []store.ProgramOutputRow{row, real}, "http://conductor.test")
		if !strings.Contains(got.Body.String(), want.Body.String()) {
			t.Fatalf("ordinary programme changed (movie=%v)", movie)
		}
	}
}
