package ppvparse

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/store"
)

func TestToEPGProgram_LivePipe(t *testing.T) {
	chID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	p, ok := Parse("LIVE | CHAMPIONSHIP SOUTHAMPTON - BLACKBURN | Tue 14 Apr 20:55 CEST (DK) | 8K EXCLUSIVE | DK: VIAPLAY PPV 9")
	if !ok {
		t.Fatal("Parse failed")
	}
	prog, ok := ToEPGProgram(chID, p, "stream-606180")
	if !ok {
		t.Fatal("ToEPGProgram returned !ok for live stream")
	}
	if prog.ChannelID != chID {
		t.Errorf("channel id: got %v want %v", prog.ChannelID, chID)
	}
	if prog.Title != "CHAMPIONSHIP SOUTHAMPTON - BLACKBURN" {
		t.Errorf("title got %q", prog.Title)
	}
	if !prog.IsLive {
		t.Error("expected IsLive=true for status=live")
	}
	if prog.EndAt.Sub(prog.StartAt) != 3*time.Hour {
		t.Errorf("duration not 3h default: %v", prog.EndAt.Sub(prog.StartAt))
	}
	if !strings.HasPrefix(prog.SourceHash, "ppv-parse:stream-606180:") {
		t.Errorf("source_hash got %q", prog.SourceHash)
	}
	if len(prog.Category) < 1 || prog.Category[0] != "Sports" {
		t.Errorf("category got %v", prog.Category)
	}
	if !strings.Contains(prog.Description, "VIAPLAY") {
		t.Errorf("description should carry broadcaster, got %q", prog.Description)
	}
}

func TestToEPGProgram_UpcomingIsLiveOriginBroadcast(t *testing.T) {
	chID := uuid.MustParse("77777777-7777-7777-7777-777777777777")
	start := time.Now().UTC().Add(6 * time.Hour).Truncate(time.Second)
	p := ParsedStream{
		Status:   StatusUpcoming,
		Title:    "UAE Warriors 73",
		StartAt:  start,
		Duration: 4 * time.Hour,
		Slot:     "LIVE EVENT 01",
		Sport:    "mma",
	}
	prog, ok := ToEPGProgram(chID, p, "xtream:123")
	if !ok {
		t.Fatal("ToEPGProgram returned !ok for upcoming event")
	}
	if !prog.IsLive {
		t.Error("upcoming live-origin PPV event should retain live semantics")
	}
}

func TestToEPGProgram_RejectEnded(t *testing.T) {
	chID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	p, ok := Parse("ENDED | RIO AMERICANO VS. OAKMONT | Tue 14 Apr 06:15 EDT (US) | 8K EXCLUSIVE | US: NFHS PPV 4")
	if !ok {
		t.Fatal("Parse failed")
	}
	if _, ok := ToEPGProgram(chID, p, ""); ok {
		t.Error("expected !ok — ended games shouldn't be injected")
	}
}

func TestToEPGProgram_RejectNoStartTime(t *testing.T) {
	chID := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	// Live with no date → ParsedStream.StartAt is zero.
	p, ok := Parse("Live | Atlético Madrid vs. Barcelona | UEFA Champions League | 8K EXCLUSIVE | CA: DAZN PPV 5")
	if !ok {
		t.Fatal("Parse failed")
	}
	if !p.StartAt.IsZero() {
		t.Skip("This test assumes no date is parsed; pattern changed")
	}
	if _, ok := ToEPGProgram(chID, p, ""); ok {
		t.Error("expected !ok when StartAt is zero — Plex can't slot at 00:00 UTC")
	}
}

func TestToEPGProgram_MLSWithExplicitDuration(t *testing.T) {
	chID := uuid.MustParse("44444444-4444-4444-4444-444444444444")
	p, ok := Parse("MLS LIVE 19: Dallas vs. St. Louis start:2025-07-20 01:25:00 stop:2025-07-20 04:07:00")
	if !ok {
		t.Fatal("Parse failed")
	}
	prog, ok := ToEPGProgram(chID, p, "")
	if !ok {
		// MLS event from 2025 might be in the past — ToEPGProgram
		// rejects ended status. That's the correct behaviour; just
		// skip this check if the test runs after stop time.
		if p.Status == StatusEnded {
			t.Skip("MLS sample event has aired; skipping")
		}
		t.Fatal("ToEPGProgram returned !ok unexpectedly")
	}
	want := 2*time.Hour + 42*time.Minute
	if prog.EndAt.Sub(prog.StartAt) != want {
		t.Errorf("duration honoured got %v want %v", prog.EndAt.Sub(prog.StartAt), want)
	}
}

// ─── OffSlotPrograms block coverage (2026-08-29 NCAAF guide-hole fix) ───

func offSlotFixtureNow() time.Time {
	// Mid-block so trims are visible: 01:00Z inside the [00:00,06:00) block.
	return time.Date(2026, 9, 5, 1, 0, 0, 0, time.UTC)
}

func TestOffSlotPrograms_PostEventRemainderFillsTheBlock(t *testing.T) {
	now := offSlotFixtureNow()
	chID := uuid.New()
	ev := &store.EPGProgram{
		ChannelID: chID,
		StartAt:   time.Date(2026, 9, 4, 23, 30, 0, 0, time.UTC),
		EndAt:     time.Date(2026, 9, 5, 2, 30, 0, 0, time.UTC),
		Title:     "NC State at Wake Forest",
	}
	rows := OffSlotPrograms(chID, ParsedStream{Slot: "NCAAF 01", Sport: "ncaaf", Status: StatusOff, OffIdle: true},
		"xtream:1", ev, now, time.UTC)
	var post *store.EPGProgram
	for i := range rows {
		if rows[i].StartAt.Equal(ev.EndAt) {
			post = &rows[i]
		}
	}
	if post == nil {
		t.Fatalf("no post-event remainder row; rows=%v", rows)
	}
	if !post.EndAt.Equal(time.Date(2026, 9, 5, 6, 0, 0, 0, time.UTC)) {
		t.Fatalf("post remainder must run to the block edge, got end %v", post.EndAt)
	}
	if post.Title != "NCAAF — No Event Scheduled" {
		t.Fatalf("post remainder title %q", post.Title)
	}
}

func TestOffSlotPrograms_SubSliverRemainderSuppressed(t *testing.T) {
	now := offSlotFixtureNow()
	chID := uuid.New()
	ev := &store.EPGProgram{
		ChannelID: chID,
		StartAt:   time.Date(2026, 9, 5, 2, 0, 0, 0, time.UTC),
		EndAt:     time.Date(2026, 9, 5, 5, 50, 0, 0, time.UTC), // 10 min left in block
		Title:     "Late Kick",
	}
	rows := OffSlotPrograms(chID, ParsedStream{Slot: "NCAAF 01", Sport: "ncaaf", Status: StatusOff, OffIdle: true},
		"xtream:1", ev, now, time.UTC)
	for _, r := range rows {
		if r.StartAt.Equal(ev.EndAt) {
			t.Fatalf("10-minute remainder must be suppressed, got row %v-%v", r.StartAt, r.EndAt)
		}
	}
}

func TestOffSlotPrograms_UpcomingEventGetsPreAndPost(t *testing.T) {
	now := offSlotFixtureNow()
	chID := uuid.New()
	ev := &store.EPGProgram{
		ChannelID: chID,
		StartAt:   time.Date(2026, 9, 5, 2, 0, 0, 0, time.UTC),
		EndAt:     time.Date(2026, 9, 5, 4, 0, 0, 0, time.UTC),
		Title:     "Campbell at ETSU",
	}
	rows := OffSlotPrograms(chID, ParsedStream{Slot: "NCAAF 05", Sport: "ncaaf", Status: StatusOff, OffIdle: true},
		"xtream:5", ev, now, time.UTC)
	var pre, post bool
	for _, r := range rows {
		if r.EndAt.Equal(ev.StartAt) && strings.HasPrefix(r.Title, "Next ") {
			pre = true
		}
		if r.StartAt.Equal(ev.EndAt) && r.EndAt.Equal(time.Date(2026, 9, 5, 6, 0, 0, 0, time.UTC)) {
			post = true
		}
	}
	if !pre || !post {
		t.Fatalf("want pre banner + post remainder around an in-block event, got pre=%v post=%v rows=%v", pre, post, rows)
	}
}

func TestOffSlotPrograms_AnnouncedOffTitleFillsRelaySlots(t *testing.T) {
	now := offSlotFixtureNow()
	rows := OffSlotPrograms(uuid.New(),
		ParsedStream{Slot: "NCAAF 02", Sport: "ncaaf", Status: StatusOff, OffIdle: false, Title: "ESPN2"},
		"xtream:2", nil, now, time.UTC)
	if len(rows) != 4 {
		t.Fatalf("relay slot must fill all 4 blocks, got %d", len(rows))
	}
	for _, r := range rows {
		if r.Title != "ESPN2" {
			t.Fatalf("announced-off blocks must carry the announced label, got %q", r.Title)
		}
	}
}
