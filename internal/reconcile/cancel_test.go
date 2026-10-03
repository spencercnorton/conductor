package reconcile

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/store"
)

// booked is the row scheduleOne writes for m: same airing key, same path.
func booked(w *Worker, m Match) store.DVRRecording {
	return store.DVRRecording{
		ID: uuid.New(), ChannelID: m.Program.ChannelID, Title: m.Program.Title,
		ScheduledStart: m.Program.StartAt, State: "scheduled",
		RequestedBy: requestedBy(m), OutputPath: w.outputFor(m),
	}
}

func episodeMatch(series string, season, episode int) Match {
	p := prog(series, "", false)
	p.StartAt = p.StartAt.Truncate(time.Microsecond)
	return Match{Program: p, Kind: "episode", ArrTitle: series, Season: season, Episode: episode, Confidence: 0.9}
}

func TestUnwantedRecordings_OnlyRowsNoWantClaims(t *testing.T) {
	w := &Worker{OutputDir: "/dvr"}
	stillWanted := episodeMatch("Show A", 1, 9)
	gotElsewhere := episodeMatch("Show B", 1, 6)  // the *arr grabbed it: no match this cycle
	offGuide := episodeMatch("Show C", 2, 1)      // airing missing from this snapshot
	lowConfidence := episodeMatch("Show D", 3, 4) // still wanted, just below threshold now
	retitled := episodeMatch("Show E", 5, 5)      // same airing, want now builds another path
	gapOnly := episodeMatch("Show F", 1, 1)       // only an inferred Plex gap still points at it
	lowConfidence.Confidence = 0.6

	rows := []store.DVRRecording{
		booked(w, stillWanted), booked(w, gotElsewhere), booked(w, offGuide),
		booked(w, lowConfidence), booked(w, retitled), booked(w, gapOnly),
	}
	retitledNow := retitled
	retitledNow.ArrTitle = "Show E (2026)"
	gapNow := gapOnly
	gapNow.Source = "plex-gap"

	g := gathered{
		progs: []Program{stillWanted.Program, gotElsewhere.Program, lowConfidence.Program,
			retitled.Program, gapOnly.Program},
		matches: []Match{stillWanted, lowConfidence, retitledNow, gapNow},
	}
	got := unwantedRecordings(rows, g, w.outputFor)
	if len(got) != 2 {
		t.Fatalf("unwanted = %d rows, want 2 (Show B, Show F): %+v", len(got), got)
	}
	if got[0].ID != rows[1].ID || got[1].ID != rows[5].ID {
		t.Fatalf("unwanted = %s, %s; want Show B and Show F", got[0].Title, got[1].Title)
	}
}

func TestConfirmUnwanted_NeedsConsecutiveCycles(t *testing.T) {
	w := &Worker{}
	a := store.DVRRecording{ID: uuid.New()}
	b := store.DVRRecording{ID: uuid.New()}

	if due := w.confirmUnwanted([]store.DVRRecording{a, b}); len(due) != 0 {
		t.Fatalf("first sighting cancelled %d rows, want 0", len(due))
	}
	// b is claimed again this cycle, so its count starts over.
	due := w.confirmUnwanted([]store.DVRRecording{a})
	if len(due) != 1 || due[0].ID != a.ID {
		t.Fatalf("second consecutive sighting = %+v, want only a", due)
	}
	if due := w.confirmUnwanted([]store.DVRRecording{b}); len(due) != 0 {
		t.Fatal("b must start over after a cycle in which it was wanted")
	}
}

func TestOutputForMatchesScheduleOnePath(t *testing.T) {
	w := &Worker{OutputDir: "/dvr"}
	m := episodeMatch("90 Day", 12, 21)
	m.ArrTitle = "90 Day Fiancé"
	if got, want := w.outputFor(m), "/dvr/TV/90 Day Fiancé/Season 12/90 Day Fiancé.S12E21.ts"; got != want {
		t.Fatalf("outputFor = %q, want %q", got, want)
	}
}
