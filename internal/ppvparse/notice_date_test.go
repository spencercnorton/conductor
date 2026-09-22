package ppvparse

import (
	"testing"
	"time"

	"github.com/spencercnorton/conductor/internal/store"
	"github.com/google/uuid"
)

func TestNextEventNoticeRemainsDatedAcrossCachedLocalMidnight(t *testing.T) {
	loc, err := time.LoadLocation("America/Denver")
	if err != nil {
		t.Fatal(err)
	}
	// Winter midnight is 07Z, inside the same six-hour UTC grid block.
	now := time.Date(2026, 1, 10, 6, 59, 0, 0, time.UTC)
	event := store.EPGProgram{Title: "Known event", StartAt: now.Add(4 * time.Hour), EndAt: now.Add(6 * time.Hour)}
	id := uuid.New()
	p := ParsedStream{Slot: "PPV 01", Status: StatusUpcoming}
	before := OffSlotPrograms(id, p, "test", &event, now, loc)
	after := OffSlotPrograms(id, p, "test", &event, now.Add(2*time.Minute), loc)
	if len(before) == 0 || len(after) == 0 || before[0].Title != "Next Jan 10 3:59 AM MST — Known event" || before[0].Title != after[0].Title || before[0].SourceHash != after[0].SourceHash {
		t.Fatalf("cached date/identity changed across midnight: before=%+v after=%+v", before, after)
	}
}

func TestNextEventNoticeDisambiguatesRepeatedDSTHour(t *testing.T) {
	loc, err := time.LoadLocation("America/Denver")
	if err != nil {
		t.Fatal(err)
	}
	first := time.Date(2026, 11, 1, 7, 30, 0, 0, time.UTC)
	if got := formatNextEventTitle("Event", first, loc); got != "Next Nov 1 1:30 AM MDT — Event" {
		t.Fatal(got)
	}
	if got := formatNextEventTitle("Event", first.Add(time.Hour), loc); got != "Next Nov 1 1:30 AM MST — Event" {
		t.Fatal(got)
	}
}
