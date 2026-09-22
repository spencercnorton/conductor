package ppvparse

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/store"
)

func TestParse_PipeFormatLive(t *testing.T) {
	in := "LIVE | CHAMPIONSHIP SOUTHAMPTON - BLACKBURN | Tue 14 Apr 20:55 CEST (DK) | 8K EXCLUSIVE | DK: VIAPLAY PPV 9"
	got, ok := Parse(in)
	if !ok {
		t.Fatal("expected ok")
	}
	if got.Status != StatusLive {
		t.Errorf("status got %q want live", got.Status)
	}
	if got.Title != "CHAMPIONSHIP SOUTHAMPTON - BLACKBURN" {
		t.Errorf("title got %q", got.Title)
	}
	if got.CountryCode != "DK" {
		t.Errorf("country got %q want DK", got.CountryCode)
	}
	if got.Broadcaster != "VIAPLAY" {
		t.Errorf("broadcaster got %q want VIAPLAY", got.Broadcaster)
	}
	if got.Slot != "VIAPLAY PPV 9" {
		t.Errorf("slot got %q", got.Slot)
	}
	// Time check: the parsed segment was "Tue 14 Apr 20:55 CEST".
	// CEST = UTC+2 → 18:55 UTC. The year is current-or-next; we just
	// validate the hour/minute came out right.
	if got.StartAt.IsZero() {
		t.Error("expected non-zero StartAt")
	}
	if got.StartAt.Hour() != 18 || got.StartAt.Minute() != 55 {
		t.Errorf("kickoff got %v want 18:55 UTC", got.StartAt)
	}
	if got.Duration != 3*time.Hour {
		t.Errorf("duration got %v want 3h default", got.Duration)
	}
	if got.Confidence < 0.85 {
		t.Errorf("confidence got %.2f want >=0.85 (live + parsed time)", got.Confidence)
	}
}

func TestParse_PipeFormatEnded(t *testing.T) {
	in := "ENDED | RIO AMERICANO VS. OAKMONT | Tue 14 Apr 06:15 EDT (US) | 8K EXCLUSIVE | US: NFHS PPV 4"
	got, ok := Parse(in)
	if !ok {
		t.Fatal("expected ok")
	}
	if got.Status != StatusEnded {
		t.Errorf("status got %q want ended", got.Status)
	}
	// EDT = UTC-4 → 06:15 EDT = 10:15 UTC.
	if got.StartAt.Hour() != 10 || got.StartAt.Minute() != 15 {
		t.Errorf("kickoff got %v want 10:15 UTC", got.StartAt)
	}
	if got.CountryCode != "US" {
		t.Errorf("country got %q", got.CountryCode)
	}
}

func TestParse_PipeFormatTwoSegmentDate(t *testing.T) {
	// Date and time on separate pipes.
	in := "End | Good Morning Football | NFL Game Pass | 2026-04-14 | 12:00 (GMT) | 8K EXCLUSIVE | BE: DAZN PPV 3"
	got, ok := Parse(in)
	if !ok {
		t.Fatal("expected ok")
	}
	if got.Status != StatusEnded {
		t.Errorf("status got %q", got.Status)
	}
	// GMT = UTC, so kickoff is 12:00 UTC.
	if got.StartAt.Hour() != 12 || got.StartAt.Minute() != 0 {
		t.Errorf("kickoff got %v want 12:00 UTC", got.StartAt)
	}
	if got.StartAt.Year() != 2026 || got.StartAt.Month() != time.April || got.StartAt.Day() != 14 {
		t.Errorf("date got %v want 2026-04-14", got.StartAt)
	}
	// Title should combine the non-date segments and skip the
	// "8K EXCLUSIVE" flag.
	if !strings.Contains(got.Title, "Good Morning Football") {
		t.Errorf("title missing event, got %q", got.Title)
	}
	if strings.Contains(got.Title, "8K") {
		t.Errorf("title contained flag noise: %q", got.Title)
	}
	if got.CountryCode != "BE" {
		t.Errorf("country got %q want BE", got.CountryCode)
	}
}

func TestParse_PipeFormatNoDate(t *testing.T) {
	// 4-segment LIVE variant — no date provided. P10: a live event with no
	// parseable date is anchored to the current hour (low confidence) so the
	// tile still exists, instead of being dropped for a zero StartAt.
	in := "Live | Atlético Madrid vs. Barcelona | UEFA Champions League | 8K EXCLUSIVE | CA: DAZN PPV 5"
	got, ok := Parse(in)
	if !ok {
		t.Fatal("expected ok")
	}
	if got.Status != StatusLive {
		t.Errorf("status got %q want live", got.Status)
	}
	if !strings.Contains(got.Title, "Atlético Madrid") {
		t.Errorf("title missing matchup, got %q", got.Title)
	}
	if got.StartAt.IsZero() {
		t.Error("live no-date event should be anchored to the current hour, not zero")
	}
	if d := time.Since(got.StartAt); d < 0 || d > time.Hour {
		t.Errorf("StartAt %v not within the last hour (current-hour anchor)", got.StartAt)
	}
	if got.Confidence > 0.5 {
		t.Errorf("no-date live parse should be low confidence, got %.2f", got.Confidence)
	}
	if got.Sport != "epl" {
		t.Errorf("sport got %q want epl (UEFA)", got.Sport)
	}
}

func TestParse_MLSStartStop(t *testing.T) {
	in := "MLS LIVE 19: Dallas vs. St. Louis start:2025-07-20 01:25:00 stop:2025-07-20 04:07:00"
	got, ok := Parse(in)
	if !ok {
		t.Fatal("expected ok")
	}
	if got.Title != "Dallas vs. St. Louis" {
		t.Errorf("title got %q", got.Title)
	}
	if got.Slot != "MLS LIVE 19" {
		t.Errorf("slot got %q", got.Slot)
	}
	if got.Sport != "mls" {
		t.Errorf("sport got %q", got.Sport)
	}
	if got.Duration != 2*time.Hour+42*time.Minute {
		t.Errorf("duration got %v want 2h42m", got.Duration)
	}
	if got.Confidence < 0.9 {
		t.Errorf("confidence got %.2f, expected high for explicit start+stop", got.Confidence)
	}
}

func TestParse_FLSPFormat(t *testing.T) {
	in := "(FLSP 632) | flolive: 2026 Claremont M_S vs Occidental _ Tennis (Court 6) (2026-04-18 13:04:20)"
	got, ok := Parse(in)
	if !ok {
		t.Fatal("expected ok")
	}
	if got.Slot != "FLSP 632" {
		t.Errorf("slot got %q", got.Slot)
	}
	if !strings.Contains(got.Title, "Claremont") {
		t.Errorf("title got %q", got.Title)
	}
	if got.Sport != "tennis" {
		t.Errorf("sport got %q want tennis (inferred)", got.Sport)
	}
	if got.StartAt.Year() != 2026 || got.StartAt.Month() != time.April || got.StartAt.Day() != 18 {
		t.Errorf("date got %v", got.StartAt)
	}
}

func TestParse_StaticChannel_NotPPV(t *testing.T) {
	// Static channel names that aren't recognised PPV slots should fall
	// through to (zero, false) so the worker skips them. (The original
	// rationale — "dispatcharr EPG ingest takes over" — is obsolete since
	// dispatcharr's retirement 2026-06-10.) NOTE: "UK: EPL 1 PPV …" was
	// moved OUT of this list — it's one of our EPL PPV channels, now parsed
	// as a StatusOff slot for guide presence (see TestParse_EPLCountryPPVSlot).
	// NOTE: "LIVE EVENT 27 - OUTDOOR THEATRE …" moved out too — the generic
	// slots have channels mapped since 2026-07-11, so untimed labels are now
	// StatusOff (see TestParse_GenericSlotOff).
	cases := []string{
		"VIP: CANAL+ LIVE 12 ᴿᴬᵂ",
		"FR: CANAL+ LIVE 8 ᴿᴬᵂ",
		"DE GO: DAZN 14 HD [LIVE-EVENT]",
		"AR: Arryadia Live ᴿᴬᵂ",
		"###### UFC PPV #######", // catalogue section header
	}
	for _, c := range cases {
		t.Run(c, func(t *testing.T) {
			if _, ok := Parse(c); ok {
				t.Errorf("unexpected parse success for %q", c)
			}
		})
	}
}

func TestParse_EmptyAndJunk(t *testing.T) {
	cases := []string{
		"",
		"   ",
		"|||",
		"some random text",
	}
	for _, c := range cases {
		if _, ok := Parse(c); ok {
			t.Errorf("expected fail for %q", c)
		}
	}
}

func TestClassifyStatus(t *testing.T) {
	cases := map[string]Status{
		"LIVE":     StatusLive,
		"Live":     StatusLive,
		"ENDED":    StatusEnded,
		"End":      StatusEnded,
		"Upcoming": StatusUpcoming,
		"random":   StatusUnknown,
		"":         StatusUnknown,
	}
	for in, want := range cases {
		if got := classifyStatus(in); got != want {
			t.Errorf("classifyStatus(%q) = %q; want %q", in, got, want)
		}
	}
}

func TestLoadKnownTZ_Coverage(t *testing.T) {
	// Fixed-offset abbreviations resolve to a constant offset year-round.
	// GMT stays UTC (a "GMT" label means UTC; it is NOT folded into
	// Europe/London — that would add BST's +1 in summer).
	fixed := []struct {
		abbrev string
		offset int // seconds east of UTC
	}{
		{"GMT", 0}, {"UTC", 0}, {"Z", 0},
		{"CST", -6 * 3600}, {"CDT", -5 * 3600},
		{"MST", -7 * 3600}, {"MDT", -6 * 3600},
		{"JST", 9 * 3600},
	}
	for _, c := range fixed {
		loc := loadKnownTZ(c.abbrev)
		if loc == nil {
			t.Errorf("loadKnownTZ(%q) returned nil", c.abbrev)
			continue
		}
		if _, off := time.Date(2026, 6, 1, 12, 0, 0, 0, loc).Zone(); off != c.offset {
			t.Errorf("loadKnownTZ(%q) offset=%d want %d", c.abbrev, off, c.offset)
		}
	}

	// DST-pair abbreviations resolve to their IANA zone, so the offset follows
	// the season of the date being parsed — a seasonally-wrong printed abbrev
	// ("EDT" in January) still lands on the correct wall-clock instant (P6).
	jan := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
	jul := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	seasonal := []struct {
		abbrev           string
		wantJan, wantJul int
	}{
		{"EST", -5 * 3600, -4 * 3600},
		{"EDT", -5 * 3600, -4 * 3600}, // wrong-season EDT in Jan still → EST wall clock
		{"ET", -5 * 3600, -4 * 3600},
		{"PST", -8 * 3600, -7 * 3600},
		{"PDT", -8 * 3600, -7 * 3600},
		{"PT", -8 * 3600, -7 * 3600},
		{"CET", 1 * 3600, 2 * 3600},
		{"CEST", 1 * 3600, 2 * 3600},
		{"BST", 0, 1 * 3600}, // London: GMT in winter, BST in summer
	}
	for _, c := range seasonal {
		loc := loadKnownTZ(c.abbrev)
		if loc == nil {
			t.Errorf("loadKnownTZ(%q) returned nil", c.abbrev)
			continue
		}
		if _, off := jan.In(loc).Zone(); off != c.wantJan {
			t.Errorf("loadKnownTZ(%q) Jan offset=%d want %d", c.abbrev, off, c.wantJan)
		}
		if _, off := jul.In(loc).Zone(); off != c.wantJul {
			t.Errorf("loadKnownTZ(%q) Jul offset=%d want %d", c.abbrev, off, c.wantJul)
		}
	}

	// Case-insensitive lookup.
	if loadKnownTZ("edt") == nil {
		t.Error("loadKnownTZ should be case-insensitive")
	}
	// Unknown abbreviation returns nil (the caller then stops the title leak).
	if loadKnownTZ("ZZZ") != nil {
		t.Error("expected nil for unknown TZ")
	}
}

func TestParseSlot_Variants(t *testing.T) {
	cases := []struct {
		in       string
		ok       bool
		country  string
		brodcast string
	}{
		{"DK: VIAPLAY PPV 9", true, "DK", "VIAPLAY"},
		{"US: ESPN+ PPV 19", true, "US", "ESPN+"},
		{"DAZN PPV 5", true, "", "DAZN"},
		{"random text", false, "", ""},
		{"DK: NO MATCH", false, "", ""},
	}
	for _, c := range cases {
		_, country, broadcaster, ok := parseSlot(c.in)
		if ok != c.ok {
			t.Errorf("parseSlot(%q) ok=%v want %v", c.in, ok, c.ok)
		}
		if c.ok {
			if country != c.country {
				t.Errorf("parseSlot(%q) country=%q want %q", c.in, country, c.country)
			}
			if broadcaster != c.brodcast {
				t.Errorf("parseSlot(%q) broadcaster=%q want %q", c.in, broadcaster, c.brodcast)
			}
		}
	}
}

func TestInferSport(t *testing.T) {
	cases := []struct {
		title, bcast, want string
	}{
		{"NFL: Eagles @ Cowboys", "ESPN", "nfl"},
		{"Lakers vs Warriors", "NBA TV", "nba"},
		{"World Series Game 7", "MLB Network", "mlb"},
		{"Bruins vs Rangers", "NHL Network", "nhl"},
		{"Atlético Madrid vs. Barcelona", "DAZN UEFA Champions League", "epl"},
		{"ATP Tennis Wimbledon", "ESPN", "tennis"},
		{"UFC 300", "DAZN", "ufc"},
		{"Random Show", "Some Network", ""},
	}
	for _, c := range cases {
		got := inferSport(c.title, c.bcast)
		if got != c.want {
			t.Errorf("inferSport(%q,%q) = %q; want %q", c.title, c.bcast, got, c.want)
		}
	}
}

func TestParse_IboostShorthand_Live(t *testing.T) {
	in := "NHL  | 01 - 7pm Canadiens @ Sabres"
	got, ok := Parse(in)
	if !ok {
		t.Fatal("expected ok")
	}
	if got.Title != "Canadiens @ Sabres" {
		t.Errorf("title got %q want 'Canadiens @ Sabres'", got.Title)
	}
	if got.Slot != "NHL PPV 01" {
		t.Errorf("slot got %q want 'NHL PPV 01'", got.Slot)
	}
	if got.Sport != "nhl" {
		t.Errorf("sport got %q want nhl", got.Sport)
	}
	if got.Broadcaster != "NHL PPV" {
		t.Errorf("broadcaster got %q", got.Broadcaster)
	}
	// Kickoff: 7pm ET. EDT (March-Nov) → 23:00 UTC; EST → 00:00 UTC next day.
	// We just check that StartAt is not zero and the hour is one of {23, 0}.
	h := got.StartAt.Hour()
	if h != 23 && h != 0 {
		t.Errorf("kickoff hour got %d, expected 23 (EDT) or 0 (EST)", h)
	}
}

func TestParse_IboostShorthand_HalfHour(t *testing.T) {
	in := "NHL | 02 - 9:30pm Golden Knights @ Ducks"
	got, ok := Parse(in)
	if !ok {
		t.Fatal("expected ok")
	}
	if got.Title != "Golden Knights @ Ducks" {
		t.Errorf("title got %q", got.Title)
	}
	if got.StartAt.Minute() != 30 {
		t.Errorf("expected :30 minute kickoff, got %d", got.StartAt.Minute())
	}
}

func TestParse_IboostShorthand_4KSlot(t *testing.T) {
	in := "NHL  |  4K - 7pm Canadiens @ Sabres"
	got, ok := Parse(in)
	if !ok {
		t.Fatal("expected ok")
	}
	if got.Slot != "NHL PPV 4K" {
		t.Errorf("4K slot label got %q", got.Slot)
	}
}

func TestParse_IboostShorthand_OffStateRecognised(t *testing.T) {
	// Pre-2026-06-10 the off-state returned !ok and the dispatcharr
	// ppv-epg.xml placeholder feed filled the guide. That feed is
	// retired; the off-state now parses to StatusOff so ppvsync can
	// inject the placeholders itself (see TestParse_ShorthandOffState
	// for field-level assertions).
	cases := []string{
		"NFL  | 01 -",
		"NHL | 03 -",
		"MLB | 02 -",
		"NHL   | 04 - ",
	}
	for _, c := range cases {
		t.Run(c, func(t *testing.T) {
			p, ok := Parse(c)
			if !ok || p.Status != StatusOff {
				t.Errorf("off-state %q: got (ok=%v, status=%q), want (true, off)", c, ok, p.Status)
			}
		})
	}
}

func TestParse_IboostShorthand_NonLeaguePrefixRejected(t *testing.T) {
	// Non-iboost stream should fall through.
	cases := []string{
		"VIP: CANAL+ LIVE 12 ᴿᴬᵂ",
		"DE GO: DAZN 14 HD [LIVE-EVENT]",
	}
	for _, c := range cases {
		if _, ok := Parse(c); ok {
			t.Errorf("expected !ok for %q", c)
		}
	}
}

func TestParse_IboostShorthand_AmPmHandling(t *testing.T) {
	in := "NHL | 01 - 12pm Test Team @ Other Team"
	got, ok := Parse(in)
	if !ok {
		t.Fatal("expected ok")
	}
	// 12pm ET = 16:00 UTC EDT, 17:00 UTC EST. Just check non-zero hour.
	if got.StartAt.IsZero() {
		t.Error("expected non-zero StartAt")
	}
}

// TestParse_ShorthandOffState: "LEAGUE | NN -" with no event is a
// recognised slot in its off-state — ppvsync needs the slot identity to
// inject "No Event Scheduled" placeholders (the dispatcharr placeholder
// feed that used to cover this is retired).
func TestParse_ShorthandOffState(t *testing.T) {
	for _, c := range []string{"NFL  | 01 -", "NHL | 17 -", "MLB  | 09 - "} {
		t.Run(c, func(t *testing.T) {
			p, ok := Parse(c)
			if !ok {
				t.Fatalf("off-state should parse: %q", c)
			}
			if p.Status != StatusOff {
				t.Errorf("status got %q want %q", p.Status, StatusOff)
			}
			if p.Slot == "" || p.Sport == "" {
				t.Errorf("off-state must carry slot identity, got slot=%q sport=%q", p.Slot, p.Sport)
			}
			if !p.StartAt.IsZero() {
				t.Errorf("off-state must not invent a kickoff, got %v", p.StartAt)
			}
		})
	}
}

// TestParse_SlotStartStop_UFC: the live iboost UFC slot format found
// 2026-06-10 — "UFC NN : EVENT start:<ts> stop:<ts>" (spacing varies,
// event text may itself contain colons). Timestamps are UTC.
func TestParse_SlotStartStop_UFC(t *testing.T) {
	cases := []struct {
		name      string
		wantTitle string
		wantSlot  string
		wantStart time.Time
		wantDur   time.Duration
	}{
		{
			"UFC 00 : UWC 58 start:2026-06-08 00:55:00 stop:2026-06-08 06:00:00",
			"UWC 58", "UFC 00",
			time.Date(2026, 6, 8, 0, 55, 0, 0, time.UTC), 5*time.Hour + 5*time.Minute,
		},
		{
			"UFC 02: UFC 328: PRELIMS start:2026-05-10 00:55:00 stop:2026-05-10 03:00:00",
			"UFC 328: PRELIMS", "UFC 02",
			time.Date(2026, 5, 10, 0, 55, 0, 0, time.UTC), 2*time.Hour + 5*time.Minute,
		},
		{
			"UFC 04: UFC FIGHT NIGHT: DELLA MADDALENA VS PRATES start:2026-05-02 12:55:00 stop:2026-05-02 16:00:00",
			"UFC FIGHT NIGHT: DELLA MADDALENA VS PRATES", "UFC 04",
			time.Date(2026, 5, 2, 12, 55, 0, 0, time.UTC), 3*time.Hour + 5*time.Minute,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p, ok := Parse(c.name)
			if !ok {
				t.Fatalf("should parse: %q", c.name)
			}
			if p.Title != c.wantTitle {
				t.Errorf("title got %q want %q", p.Title, c.wantTitle)
			}
			if p.Slot != c.wantSlot {
				t.Errorf("slot got %q want %q", p.Slot, c.wantSlot)
			}
			if !p.StartAt.Equal(c.wantStart) {
				t.Errorf("start got %v want %v", p.StartAt, c.wantStart)
			}
			if p.Duration != c.wantDur {
				t.Errorf("duration got %v want %v", p.Duration, c.wantDur)
			}
			if p.Sport != "ufc" {
				t.Errorf("sport got %q want ufc", p.Sport)
			}
		})
	}
}

// TestParse_PPVEvent: iboost's generic numbered event slots, found live
// 2026-07-11 when UFC 329 was published on them while the permanent
// "UFC NN :" slots sat empty — the whole card vanished from the guide.
// The parenthesised suffix is US-style month.day + ET time-of-day; the
// event's year isn't in the name, so assertions convert StartAt back to
// ET and check wall-clock month/day/time only.
func TestParse_PPVEvent(t *testing.T) {
	cases := []struct {
		in        string
		wantTitle string
		wantSlot  string
		wantSport string
		wantHour  int // ET wall-clock, 24h
		wantMin   int
		wantDur   time.Duration // 5h combat main card, 3h prelims (P11)
	}{
		{
			"PPV EVENT 05: UFC 329 McGregor vs. Holloway 2 (7.11 9:00 PM ET)",
			"UFC 329 McGregor vs. Holloway 2", "PPV EVENT 05", "ufc", 21, 0, 5 * time.Hour,
		},
		{
			"PPV EVENT 03: PRELIMS UFC 329 (7.11 7:00 PM ET)",
			"PRELIMS UFC 329", "PPV EVENT 03", "ufc", 19, 0, 3 * time.Hour,
		},
		{
			"PPV EVENT 01: EARLY PRELIMS UFC 329 (7.11 5:00 PM ET)",
			"EARLY PRELIMS UFC 329", "PPV EVENT 01", "ufc", 17, 0, 3 * time.Hour,
		},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			p, ok := Parse(c.in)
			if !ok {
				t.Fatalf("should parse: %q", c.in)
			}
			if p.Title != c.wantTitle {
				t.Errorf("title got %q want %q", p.Title, c.wantTitle)
			}
			if p.Slot != c.wantSlot {
				t.Errorf("slot got %q want %q", p.Slot, c.wantSlot)
			}
			if p.Sport != c.wantSport {
				t.Errorf("sport got %q want %q", p.Sport, c.wantSport)
			}
			et := p.StartAt.In(etLocation())
			if et.Month() != time.July || et.Day() != 11 {
				t.Errorf("ET date got %v want July 11", et)
			}
			if et.Hour() != c.wantHour || et.Minute() != c.wantMin {
				t.Errorf("ET time got %02d:%02d want %02d:%02d", et.Hour(), et.Minute(), c.wantHour, c.wantMin)
			}
			if p.Duration != c.wantDur {
				t.Errorf("duration got %v want %v", p.Duration, c.wantDur)
			}
			if p.Status == StatusOff || p.Status == StatusUnknown {
				t.Errorf("status got %q, want a scheduled state", p.Status)
			}
		})
	}
}

// TestParse_LiveEvent: dash-delimited sibling of PPV EVENT — time-of-day
// but no date, day anchor resolves like the league shorthand (today in
// ET, rolling forward when >12h past).
func TestParse_LiveEvent(t *testing.T) {
	in := "LIVE EVENT 02 - 9pm UFC 329 McGregor v Holloway II"
	p, ok := Parse(in)
	if !ok {
		t.Fatal("expected ok")
	}
	if p.Title != "UFC 329 McGregor v Holloway II" {
		t.Errorf("title got %q", p.Title)
	}
	if p.Slot != "LIVE EVENT 02" {
		t.Errorf("slot got %q want 'LIVE EVENT 02'", p.Slot)
	}
	if p.Sport != "ufc" {
		t.Errorf("sport got %q want ufc (from title)", p.Sport)
	}
	et := p.StartAt.In(etLocation())
	if et.Hour() != 21 || et.Minute() != 0 {
		t.Errorf("ET kickoff got %02d:%02d want 21:00", et.Hour(), et.Minute())
	}

	// A timed non-sport event still parses (guide presence), Sport empty.
	p, ok = Parse("LIVE EVENT 11 - 8pm Some Concert Night 2")
	if !ok {
		t.Fatal("timed non-sport event should parse")
	}
	if p.Sport != "" {
		t.Errorf("sport got %q want empty for non-sport title", p.Sport)
	}

	// No time-of-day token = venue/idle label — StatusOff since the
	// generic slots got channels (see TestParse_GenericSlotOff).
	p, ok = Parse("LIVE EVENT 27 - OUTDOOR THEATRE Live From Coachella 2026")
	if !ok || p.Status != StatusOff {
		t.Errorf("untimed LIVE EVENT label: got ok=%v status=%q, want StatusOff", ok, p.Status)
	}
}

// TestParse_GenericSlotOff: idle/untimed shapes of the generic PPV EVENT /
// LIVE EVENT slots become StatusOff so mapped channels keep guide presence
// (same rationale as parseColonSlotOff for "UFC 00 :"). Shapes observed in
// the live iboost catalogue 2026-07-11.
func TestParse_GenericSlotOff(t *testing.T) {
	cases := []struct {
		in       string
		wantSlot string
	}{
		{"PPV EVENT 53:", "PPV EVENT 53"},
		{"PPV EVENT 08: TBA", "PPV EVENT 08"},
		{"LIVE EVENT 03 - NO EVENT", "LIVE EVENT 03"},
		{"LIVE EVENT 33 -", "LIVE EVENT 33"},
		{"LIVE EVENT 34", "LIVE EVENT 34"},
		{"LIVE EVENT 27 - OUTDOOR THEATRE Live From Coachella 2026", "LIVE EVENT 27"},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			p, ok := Parse(c.in)
			if !ok {
				t.Fatalf("should parse as off-slot: %q", c.in)
			}
			if p.Status != StatusOff {
				t.Errorf("status got %q want StatusOff", p.Status)
			}
			if p.Slot != c.wantSlot {
				t.Errorf("slot got %q want %q", p.Slot, c.wantSlot)
			}
			if p.Sport != "" {
				t.Errorf("sport got %q want empty (generic slot)", p.Sport)
			}
			if !p.StartAt.IsZero() {
				t.Errorf("StartAt got %v want zero (nothing schedulable)", p.StartAt)
			}
		})
	}

	// Regression: scheduled events on the same slots still parse as events.
	for _, in := range []string{
		"PPV EVENT 05: UFC 329 McGregor vs. Holloway 2 (7.11 9:00 PM ET)",
		"LIVE EVENT 02 - 9pm UFC 329 McGregor v Holloway II",
	} {
		if p, _ := Parse(in); p.Status == StatusOff {
			t.Errorf("%q degraded to StatusOff; must stay a scheduled event", in)
		}
	}
}

// TestOffSlotPrograms_IdleGrid: an idle slot keeps the existing deterministic
// UTC-aligned 6 h placeholder grid.
func TestOffSlotPrograms_IdleGrid(t *testing.T) {
	chID := uuid.MustParse("55555555-5555-5555-5555-555555555555")
	now := time.Date(2026, 6, 10, 21, 10, 0, 0, time.UTC) // grid start 18:00
	ps := ParsedStream{Status: StatusOff, Slot: "UFC 02", Sport: "ufc"}
	displayLoc := time.FixedZone("MDT", -6*60*60)

	rows := OffSlotPrograms(chID, ps, "xtream:716554", nil, now, displayLoc)
	if len(rows) != 4 {
		t.Fatalf("want 4 blocks, got %d", len(rows))
	}
	if !rows[0].StartAt.Equal(time.Date(2026, 6, 10, 18, 0, 0, 0, time.UTC)) {
		t.Errorf("grid not aligned: first block %v", rows[0].StartAt)
	}
	for i, r := range rows {
		if r.Title != "UFC — No Event Scheduled" {
			t.Errorf("title got %q", r.Title)
		}
		if r.SourcePriority != store.PriorityPPVOff {
			t.Errorf("priority got %d", r.SourcePriority)
		}
		want := time.Date(2026, 6, 10, 18, 0, 0, 0, time.UTC).Add(time.Duration(i) * 6 * time.Hour)
		if !r.StartAt.Equal(want) || !r.EndAt.Equal(want.Add(6*time.Hour)) {
			t.Errorf("block %d window got %v-%v", i, r.StartAt, r.EndAt)
		}
		if r.SourceHash != fmt.Sprintf("ppv-off:xtream:716554:%d", want.Unix()) {
			t.Errorf("block %d hash got %q", i, r.SourceHash)
		}
	}
}

func TestOffSlotPrograms_UpcomingEventNamesExactChannelUntilKickoff(t *testing.T) {
	chID := uuid.MustParse("55555555-5555-5555-5555-555555555555")
	now := time.Date(2026, 6, 10, 21, 10, 0, 0, time.UTC) // 15:10 MDT
	ps := ParsedStream{Status: StatusUpcoming, Slot: "UFC 02", Sport: "ufc"}
	displayLoc := time.FixedZone("MDT", -6*60*60)
	ev := store.EPGProgram{
		StartAt: time.Date(2026, 6, 10, 23, 0, 0, 0, time.UTC),
		EndAt:   time.Date(2026, 6, 11, 2, 0, 0, 0, time.UTC),
		Title:   "UFC 400: Example vs. Opponent",
	}
	rows := OffSlotPrograms(chID, ps, "xtream:716554", &ev, now, displayLoc)
	if len(rows) != 4 {
		t.Fatalf("want pre-event row, post-event remainder, 2 later idle blocks, got %d: %+v", len(rows), rows)
	}

	pre := rows[0]
	wantStart := time.Date(2026, 6, 10, 18, 0, 0, 0, time.UTC)
	if !pre.StartAt.Equal(wantStart) || !pre.EndAt.Equal(ev.StartAt) {
		t.Fatalf("pre-event window got %v-%v want %v-%v", pre.StartAt, pre.EndAt, wantStart, ev.StartAt)
	}
	if pre.Title != "Next Jun 10 5:00 PM MDT — UFC 400: Example vs. Opponent" {
		t.Errorf("pre-event title got %q", pre.Title)
	}
	if !strings.Contains(pre.Description, "Wednesday, June 10, 2026 at 5:00 PM MDT") {
		t.Errorf("pre-event description lacks exact local start: %q", pre.Description)
	}
	if !strings.HasPrefix(pre.SourceHash, "ppv-off:xtream:716554:") ||
		!strings.Contains(pre.SourceHash, ":next:") {
		t.Errorf("pre-event hash is not event-aware: %q", pre.SourceHash)
	}
	if pre.IsLive {
		t.Error("pre-event status row must not be marked live")
	}
	post := rows[1]
	if !post.StartAt.Equal(ev.EndAt) || !post.EndAt.Equal(time.Date(2026, 6, 11, 6, 0, 0, 0, time.UTC)) {
		t.Errorf("post-event remainder got %v-%v, want event end to block end", post.StartAt, post.EndAt)
	}
	if post.Title != "UFC — No Event Scheduled" {
		t.Errorf("post-event remainder title got %q", post.Title)
	}
	if !strings.Contains(post.SourceHash, ":post:") {
		t.Errorf("post-event remainder hash got %q", post.SourceHash)
	}
	for _, r := range rows {
		if r.StartAt.Before(ev.EndAt) && r.EndAt.After(ev.StartAt) {
			t.Errorf("placeholder %v-%v overlaps event", r.StartAt, r.EndAt)
		}
	}

	renamed := ev
	renamed.Title = "UFC 400: Corrected Main Event"
	renamedRows := OffSlotPrograms(chID, ps, "xtream:716554", &renamed, now, displayLoc)
	if renamedRows[0].SourceHash == pre.SourceHash {
		t.Fatal("same-time event rename must change the placeholder hash so the upsert updates")
	}
	if renamedRows[0].Title != "Next Jun 10 5:00 PM MDT — UFC 400: Corrected Main Event" {
		t.Errorf("renamed title got %q", renamedRows[0].Title)
	}

	idleRows := OffSlotPrograms(chID, ps, "xtream:716554", nil, now, displayLoc)
	if !idleRows[0].StartAt.Equal(pre.StartAt) {
		t.Fatalf("event-to-idle row moved off its stable grid start: %v vs %v", idleRows[0].StartAt, pre.StartAt)
	}
	if idleRows[0].SourceHash == pre.SourceHash {
		t.Fatal("event-to-idle transition must change the hash so the title updates")
	}
	if idleRows[0].Title != "UFC — No Event Scheduled" {
		t.Errorf("event-to-idle title got %q", idleRows[0].Title)
	}
}

func TestOffSlotPrograms_UpcomingEventInLaterGridBlock(t *testing.T) {
	chID := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	now := time.Date(2026, 6, 10, 19, 0, 0, 0, time.UTC)
	ps := ParsedStream{Status: StatusUpcoming, Slot: "NHL PPV 04", Sport: "nhl"}
	displayLoc := time.FixedZone("MDT", -6*60*60)
	ev := store.EPGProgram{
		StartAt: time.Date(2026, 6, 11, 7, 30, 0, 0, time.UTC),
		EndAt:   time.Date(2026, 6, 11, 10, 30, 0, 0, time.UTC),
		Title:   "Avalanche @ Stars",
	}

	rows := OffSlotPrograms(chID, ps, "xtream:44", &ev, now, displayLoc)
	if len(rows) != 5 {
		t.Fatalf("want 3 pre-event rows, post-event remainder, 1 later idle row, got %d: %+v", len(rows), rows)
	}
	for i, r := range rows[:3] {
		if !strings.HasPrefix(r.Title, "Next Jun 11 1:30 AM MDT — Avalanche @ Stars") {
			t.Errorf("pre-event block %d title got %q", i, r.Title)
		}
		if i < 2 && r.EndAt.Sub(r.StartAt) != 6*time.Hour {
			t.Errorf("full pre-event block %d duration got %v", i, r.EndAt.Sub(r.StartAt))
		}
	}
	if !rows[2].EndAt.Equal(ev.StartAt) {
		t.Errorf("containing block ends %v, want exact kickoff %v", rows[2].EndAt, ev.StartAt)
	}
	if !rows[3].StartAt.Equal(ev.EndAt) || !rows[3].EndAt.Equal(time.Date(2026, 6, 11, 12, 0, 0, 0, time.UTC)) {
		t.Errorf("post-event remainder got %v-%v, want event end to block end", rows[3].StartAt, rows[3].EndAt)
	}
	for i, r := range rows[3:] {
		if r.Title != "NHL — No Event Scheduled" {
			t.Errorf("post-event block %d got %q", i, r.Title)
		}
	}
	for _, r := range rows {
		if r.StartAt.Before(ev.EndAt) && r.EndAt.After(ev.StartAt) {
			t.Errorf("placeholder %v-%v overlaps event", r.StartAt, r.EndAt)
		}
	}
}

func TestOffSlotPrograms_KickoffOnGridBoundaryHasNoZeroLengthRow(t *testing.T) {
	chID := uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
	now := time.Date(2026, 6, 10, 21, 0, 0, 0, time.UTC)
	ps := ParsedStream{Status: StatusUpcoming, Slot: "NFL PPV 02", Sport: "nfl"}
	displayLoc := time.FixedZone("MDT", -6*60*60)
	ev := store.EPGProgram{
		StartAt: time.Date(2026, 6, 11, 0, 0, 0, 0, time.UTC),
		EndAt:   time.Date(2026, 6, 11, 3, 0, 0, 0, time.UTC),
		Title:   "Broncos @ Chiefs",
	}

	rows := OffSlotPrograms(chID, ps, "xtream:45", &ev, now, displayLoc)
	if len(rows) != 4 {
		t.Fatalf("want boundary pre-row, post-event remainder, 2 later idle rows, got %d: %+v", len(rows), rows)
	}
	if !rows[0].StartAt.Equal(time.Date(2026, 6, 10, 18, 0, 0, 0, time.UTC)) ||
		!rows[0].EndAt.Equal(ev.StartAt) {
		t.Errorf("boundary pre-row got %v-%v", rows[0].StartAt, rows[0].EndAt)
	}
	if !rows[1].StartAt.Equal(ev.EndAt) || !rows[1].EndAt.Equal(time.Date(2026, 6, 11, 6, 0, 0, 0, time.UTC)) {
		t.Errorf("post-event remainder got %v-%v, want event end to block end", rows[1].StartAt, rows[1].EndAt)
	}
	for _, r := range rows {
		if !r.EndAt.After(r.StartAt) {
			t.Errorf("zero/negative placeholder emitted: %+v", r)
		}
	}
}

func TestOffSlotPrograms_FutureEventBeyondWindowStillAdvertised(t *testing.T) {
	chID := uuid.MustParse("88888888-8888-8888-8888-888888888888")
	now := time.Date(2026, 7, 23, 23, 10, 0, 0, time.UTC) // Jul 23 17:10 MDT
	ps := ParsedStream{Status: StatusUpcoming, Slot: "NFL PPV 01", Sport: "nfl"}
	displayLoc := time.FixedZone("MDT", -6*60*60)
	ev := store.EPGProgram{
		StartAt: time.Date(2026, 7, 25, 1, 15, 0, 0, time.UTC),
		EndAt:   time.Date(2026, 7, 25, 4, 15, 0, 0, time.UTC),
		Title:   "Broncos @ Seahawks",
	}

	rows := OffSlotPrograms(chID, ps, "xtream:42", &ev, now, displayLoc)
	if len(rows) != 4 {
		t.Fatalf("event outside 24 h window should keep all 4 blocks, got %d", len(rows))
	}
	for _, r := range rows {
		if r.Title != "Next Jul 24 7:15 PM MDT — Broncos @ Seahawks" {
			t.Errorf("future-date title got %q", r.Title)
		}
	}
}

func TestFormatNextEventTitle_AmericaDenverDSTAndLocalDate(t *testing.T) {
	loc, err := time.LoadLocation("America/Denver")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name       string
		now, start time.Time
		want       string
	}{
		{
			name:  "winter MST",
			now:   time.Date(2026, 1, 10, 18, 0, 0, 0, time.UTC),
			start: time.Date(2026, 1, 10, 20, 0, 0, 0, time.UTC),
			want:  "Next Jan 10 1:00 PM MST — Test Event",
		},
		{
			name:  "summer MDT",
			now:   time.Date(2026, 7, 10, 18, 0, 0, 0, time.UTC),
			start: time.Date(2026, 7, 10, 20, 0, 0, 0, time.UTC),
			want:  "Next Jul 10 2:00 PM MDT — Test Event",
		},
		{
			name:  "next local date",
			now:   time.Date(2026, 7, 11, 5, 30, 0, 0, time.UTC), // Jul 10 23:30 MDT
			start: time.Date(2026, 7, 11, 6, 30, 0, 0, time.UTC),
			want:  "Next Jul 11 12:30 AM MDT — Test Event",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := formatNextEventTitle("Test Event", tc.start, loc); got != tc.want {
				t.Errorf("got %q want %q", got, tc.want)
			}
		})
	}
}

func TestOffSlotPrograms_AlreadyStartedEventIsNotNext(t *testing.T) {
	chID := uuid.MustParse("99999999-9999-9999-9999-999999999999")
	now := time.Date(2026, 7, 23, 23, 10, 0, 0, time.UTC)
	ps := ParsedStream{Status: StatusLive, Slot: "NHL PPV 03", Sport: "nhl"}
	displayLoc := time.FixedZone("MDT", -6*60*60)
	ev := store.EPGProgram{
		StartAt: time.Date(2026, 7, 23, 22, 0, 0, 0, time.UTC),
		EndAt:   time.Date(2026, 7, 24, 1, 0, 0, 0, time.UTC),
		Title:   "Avalanche @ Stars",
	}

	rows := OffSlotPrograms(chID, ps, "xtream:43", &ev, now, displayLoc)
	for _, r := range rows {
		if strings.HasPrefix(r.Title, "Next ") {
			t.Errorf("already-started event advertised as next: %+v", r)
		}
		if r.StartAt.Before(ev.EndAt) && r.EndAt.After(ev.StartAt) {
			t.Errorf("placeholder %v-%v overlaps live event", r.StartAt, r.EndAt)
		}
	}
}

// TestParse_SlotStartStop_TruncatedStop: the provider caps name length
// and cuts mid-timestamp (live UFC 01 slot, 2026-06-10). Malformed stop
// falls back to the 3 h default instead of failing the parse.
func TestParse_SlotStartStop_TruncatedStop(t *testing.T) {
	p, ok := Parse("UFC 01 : UFC 328: POST-FIGHT PRESS CONFERENCE start:2026-05-10 06:10:00 stop:2026-05-10 08:15:")
	if !ok {
		t.Fatal("truncated-stop name should still parse")
	}
	if p.Title != "UFC 328: POST-FIGHT PRESS CONFERENCE" {
		t.Errorf("title got %q", p.Title)
	}
	if !p.StartAt.Equal(time.Date(2026, 5, 10, 6, 10, 0, 0, time.UTC)) {
		t.Errorf("start got %v", p.StartAt)
	}
	// "08:15:" loses its seconds → minute-precision parse still works.
	if p.Duration != 2*time.Hour+5*time.Minute {
		t.Errorf("duration got %v want 2h5m (minute-precision stop)", p.Duration)
	}
}

// --- PPV off-state coverage (audit 2026-06-30): UFC colon slots + EPL
// country-prefixed slots were unparsed, so those channels had no guide. ---

func TestParse_UFCColonOffState(t *testing.T) {
	for _, in := range []string{"UFC 00 :", "UFC 00 : ", "UFC 03 : TBA", "BOXING 1 :"} {
		got, ok := Parse(in)
		if !ok {
			t.Fatalf("%q: expected ok", in)
		}
		if got.Status != StatusOff {
			t.Errorf("%q: status got %q want off", in, got.Status)
		}
		if got.Slot == "" {
			t.Errorf("%q: empty slot — OffSlotPrograms would emit nothing", in)
		}
		if got.Sport == "" {
			t.Errorf("%q: empty sport", in)
		}
	}
}

func TestParse_UFCColonEventStillParsesAsEvent(t *testing.T) {
	// Regression guard: a real card with start: must remain a real event,
	// not get swallowed by the off-state handler.
	in := "UFC 02 : UFC 328: PRELIMS start:2026-05-10 00:55:00 stop:2026-05-10 03:00:00"
	got, ok := Parse(in)
	if !ok {
		t.Fatal("expected ok")
	}
	if got.Status == StatusOff {
		t.Error("a card with start:/stop: must not parse as off-state")
	}
	if got.StartAt.IsZero() {
		t.Error("expected a real StartAt")
	}
}

func TestParse_EPLCountryPPVSlot(t *testing.T) {
	got, ok := Parse("UK: EPL 1 PPV ᵁᴴᴰ ³⁸⁴⁰ᴾ")
	if !ok {
		t.Fatal("expected ok")
	}
	if got.Status != StatusOff {
		t.Errorf("status got %q want off", got.Status)
	}
	if got.Slot != "EPL 1" {
		t.Errorf("slot got %q want 'EPL 1'", got.Slot)
	}
	if got.Sport != "epl" {
		t.Errorf("sport got %q want epl", got.Sport)
	}
	if got.CountryCode != "UK" {
		t.Errorf("country got %q want UK", got.CountryCode)
	}
}

func TestParse_CountryPPVRejectsUnknownLeague(t *testing.T) {
	// Don't false-positive on arbitrary "XX: <word> N PPV" strings.
	if _, ok := Parse("US: ACME 1 PPV"); ok {
		t.Error("unknown league should not parse as a PPV slot")
	}
}

func TestParse_JunkSeparatorRejected(t *testing.T) {
	if _, ok := Parse("##### PREMIER LEAGUE #####"); ok {
		t.Error("decorative category separator should not parse")
	}
}

func TestParse_NFLShorthandOffStateStillWorks(t *testing.T) {
	// Regression: the working NFL/NHL shorthand off-state must be untouched.
	got, ok := Parse("NFL  | 01 -")
	if !ok || got.Status != StatusOff || got.Slot == "" {
		t.Fatalf("NFL shorthand off-state broke: ok=%v status=%q slot=%q", ok, got.Status, got.Slot)
	}
}

// --- v0.34.x PPV robustness (P1-P11) ---------------------------------------

// P1: the flolive catalogue moved to per-sport prefixes; flo<sport>: must parse.
func TestParse_FLSPPerSportPrefix(t *testing.T) {
	cases := []struct {
		in        string
		wantSlot  string
		wantTitle string
		wantStart time.Time
	}{
		{
			"(FLSP 701) | flobaseball: 2026 Team A vs Team B (2026-07-11 17:00:00)",
			"FLSP 701", "2026 Team A vs Team B", time.Date(2026, 7, 11, 17, 0, 0, 0, time.UTC),
		},
		{
			"(FLSP 802) | flograppling: 2026 Club X vs Club Y (2026-07-11 18:30:00)",
			"FLSP 802", "2026 Club X vs Club Y", time.Date(2026, 7, 11, 18, 30, 0, 0, time.UTC),
		},
		{
			"(FLSP 632) | flolive: 2026 Claremont M_S vs Occidental _ Tennis (Court 6) (2026-04-18 13:04:20)",
			"FLSP 632", "", time.Date(2026, 4, 18, 13, 4, 20, 0, time.UTC),
		},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			p, ok := Parse(c.in)
			if !ok {
				t.Fatalf("should parse: %q", c.in)
			}
			if p.Slot != c.wantSlot {
				t.Errorf("slot got %q want %q", p.Slot, c.wantSlot)
			}
			if c.wantTitle != "" && p.Title != c.wantTitle {
				t.Errorf("title got %q want %q", p.Title, c.wantTitle)
			}
			if !p.StartAt.Equal(c.wantStart) {
				t.Errorf("start got %v want %v", p.StartAt, c.wantStart)
			}
		})
	}
}

// P2: the MLB slate uses "|" (not ":") between slot and event, with explicit
// start AND stop.
func TestParse_MLBPipeStartStop(t *testing.T) {
	in := "MLB 01 | Brewers x Pirates start:2026-07-11 17:05:00 stop:2026-07-12 00:18:20"
	p, ok := Parse(in)
	if !ok {
		t.Fatal("MLB pipe start/stop should parse")
	}
	if p.Title != "Brewers x Pirates" {
		t.Errorf("title got %q", p.Title)
	}
	if p.Slot != "MLB 01" {
		t.Errorf("slot got %q want 'MLB 01'", p.Slot)
	}
	if p.Sport != "mlb" {
		t.Errorf("sport got %q want mlb", p.Sport)
	}
	if !p.StartAt.Equal(time.Date(2026, 7, 11, 17, 5, 0, 0, time.UTC)) {
		t.Errorf("start got %v", p.StartAt)
	}
	if want := 7*time.Hour + 13*time.Minute + 20*time.Second; p.Duration != want {
		t.Errorf("duration got %v want %v (explicit stop)", p.Duration, want)
	}
}

// P3: announced colon-slot fixtures with a time-of-day (but no "start:") become
// real events. EPL kickoffs are UK wall-clock; other leagues default ET. An
// untimed tail stays StatusOff with the title preserved.
func TestParse_ColonSlotAnnouncedFixtures(t *testing.T) {
	london, _ := time.LoadLocation("Europe/London")

	p, ok := Parse("EPL 01: 19:30 AFC Bournemouth vs Manchester City")
	if !ok {
		t.Fatal("EPL announced fixture should parse")
	}
	if p.Status == StatusOff || p.Status == StatusUnknown {
		t.Errorf("EPL fixture status got %q, want a scheduled state", p.Status)
	}
	if p.Title != "AFC Bournemouth vs Manchester City" {
		t.Errorf("EPL title got %q", p.Title)
	}
	if p.Slot != "EPL 01" || p.Sport != "epl" {
		t.Errorf("EPL slot/sport got %q/%q", p.Slot, p.Sport)
	}
	if lt := p.StartAt.In(london); lt.Hour() != 19 || lt.Minute() != 30 {
		t.Errorf("EPL kickoff got %02d:%02d London, want 19:30", lt.Hour(), lt.Minute())
	}

	p, ok = Parse("Boxing 1 :  FURY vs HALL  6PM")
	if !ok {
		t.Fatal("boxing announced fixture (time last) should parse")
	}
	if p.Status == StatusOff {
		t.Errorf("boxing fixture degraded to off")
	}
	if p.Title != "FURY vs HALL" {
		t.Errorf("boxing title got %q want 'FURY vs HALL'", p.Title)
	}
	if p.Slot != "BOXING 1" || p.Sport != "boxing" {
		t.Errorf("boxing slot/sport got %q/%q", p.Slot, p.Sport)
	}
	if et := p.StartAt.In(etLocation()); et.Hour() != 18 || et.Minute() != 0 {
		t.Errorf("boxing kickoff got %02d:%02d ET, want 18:00", et.Hour(), et.Minute())
	}

	// Untimed announced tail: no guessed kickoff, stays off, keeps title.
	p, ok = Parse("UFC 03 : TBA")
	if !ok || p.Status != StatusOff {
		t.Fatalf("untimed colon tail: got ok=%v status=%q, want StatusOff", ok, p.Status)
	}
	if !p.StartAt.IsZero() {
		t.Errorf("untimed tail must not invent a kickoff, got %v", p.StartAt)
	}
	if p.Title != "TBA" {
		t.Errorf("untimed tail should keep its title, got %q", p.Title)
	}
}

// P4: the 8K "NO EVENT STREAMING" placeholder binds to its trailing slot.
func TestParse_NoEventStreaming(t *testing.T) {
	cases := []string{
		"- NO EVENT STREAMING - | 8K EXCLUSIVE | US: ESPN+ PPV 51",
		"NO EVENT STREAMING | 8K EXCLUSIVE | US: ESPN+ PPV 51",
	}
	for _, in := range cases {
		t.Run(in, func(t *testing.T) {
			p, ok := Parse(in)
			if !ok {
				t.Fatalf("should parse: %q", in)
			}
			if p.Status != StatusOff {
				t.Errorf("status got %q want StatusOff", p.Status)
			}
			if !p.OffIdle {
				t.Error("NO EVENT STREAMING must be definitive idle so stale events are swept")
			}
			if p.Slot != "ESPN+ PPV 51" {
				t.Errorf("slot got %q want 'ESPN+ PPV 51'", p.Slot)
			}
			if p.CountryCode != "US" {
				t.Errorf("country got %q want US", p.CountryCode)
			}
			if !p.StartAt.IsZero() {
				t.Errorf("placeholder must not carry a kickoff, got %v", p.StartAt)
			}
		})
	}
}

// P5: two-segment dates also accept DD-MM-YYYY, and "all"/flag junk segments
// don't leak into the title.
func TestParse_SplitDateDMYAndJunk(t *testing.T) {
	p, ok := Parse("End | Some Match | 11-07-2026 | 20:00 (GMT) | 8K EXCLUSIVE | US: DAZN PPV 7")
	if !ok {
		t.Fatal("DD-MM-YYYY split date should parse")
	}
	if p.StartAt.Year() != 2026 || p.StartAt.Month() != time.July || p.StartAt.Day() != 11 {
		t.Errorf("date got %v want 2026-07-11", p.StartAt)
	}
	if p.StartAt.Hour() != 20 || p.StartAt.Minute() != 0 {
		t.Errorf("time got %v want 20:00 UTC", p.StartAt)
	}
	if !strings.Contains(p.Title, "Some Match") {
		t.Errorf("title missing event, got %q", p.Title)
	}

	p, ok = Parse("Live | Some Match | all | 8K EXCLUSIVE | US: DAZN PPV 7")
	if !ok {
		t.Fatal("expected ok")
	}
	if strings.Contains(strings.ToLower(p.Title), "all") {
		t.Errorf("junk 'all' segment leaked into title: %q", p.Title)
	}
	if p.Title != "Some Match" {
		t.Errorf("title got %q want 'Some Match'", p.Title)
	}
}

// P6: a seasonally-wrong printed abbrev in a real name still lands on the right
// wall clock, while an unknown explicit zone fails closed.
func TestParse_TZSeasonalAndUnknownLeak(t *testing.T) {
	ny := etLocation()

	// "EDT" printed in January → America/New_York in January is EST; the
	// 20:00 wall clock must survive (NOT a fixed -4 offset).
	p, ok := Parse("ENDED | Team A vs Team B | Tue 14 Jan 20:00 EDT (US) | US: DAZN PPV 3")
	if !ok {
		t.Fatal("expected ok")
	}
	if lt := p.StartAt.In(ny); lt.Hour() != 20 || lt.Minute() != 0 {
		t.Errorf("kickoff got %02d:%02d ET, want 20:00 (EST wall clock in Jan)", lt.Hour(), lt.Minute())
	}

	// An explicit but unknown zone cannot be placed safely and must not be
	// reinterpreted as a date-less LIVE event at the current hour.
	if p, ok = Parse("Live | Team A vs Team B | Tue 14 Jul 20:00 XYZ (US) | 8K EXCLUSIVE | US: DAZN PPV 3"); ok && !p.StartAt.IsZero() {
		t.Fatalf("unknown explicit timezone scheduled at %v", p.StartAt)
	}
}

// P7: the day-anchored kickoff rolls FORWARD only. A time-of-day is never
// anchored to more than 12h in the past (it would roll to tomorrow), and never
// more than 24h in the future — the forward-only window is [now-12h, now+24h).
// This differs from the retired symmetric ±12h clamp, which shoved evening games
// parsed in the morning to yesterday (→ StatusEnded → dropped from the guide).
func TestParse_KickoffForwardRollBound(t *testing.T) {
	for _, tod := range []string{"1am", "6am", "11am", "1pm", "4pm", "9pm", "11pm"} {
		in := "NHL | 05 - " + tod + " Team A @ Team B"
		p, ok := Parse(in)
		if !ok {
			t.Fatalf("should parse: %q", in)
		}
		if d := p.StartAt.Sub(time.Now()); d < -12*time.Hour-time.Minute || d >= 24*time.Hour+time.Minute {
			t.Errorf("%q kickoff %v is %v from now, want [-12h, +24h)", in, p.StartAt, d)
		}
	}
}

// TestKickoffAnchored_ForwardOnly is the Finding-2/3 regression guard, run
// against a fixed clock so the disputed 12–24h-ahead case is deterministic.
// The retired symmetric roll turned an evening kickoff parsed in the morning
// into a yesterday (StatusEnded) row; forward-only keeps it today/upcoming.
func TestKickoffAnchored_ForwardOnly(t *testing.T) {
	loc := time.UTC
	// Morning parse (08:00) of a 21:00 kickoff — 13h ahead, >12h. Must stay
	// TODAY 21:00 (upcoming), NOT roll back to yesterday 21:00 (→ ended → dropped).
	now := time.Date(2026, 7, 11, 8, 0, 0, 0, loc)
	if got, ok := kickoffAnchoredUTC(now, 21, 0, loc); !ok || !got.Equal(time.Date(2026, 7, 11, 21, 0, 0, 0, loc)) {
		t.Errorf("evening kickoff parsed in the morning = %v, want 2026-07-11 21:00 (today/upcoming)", got)
	}
	// A kickoff >12h in the past rolls FORWARD to tomorrow, never stays yesterday.
	// 20:00 now, 02:00 time-of-day is 18h in the past → tomorrow 02:00.
	if got, ok := kickoffAnchoredUTC(time.Date(2026, 7, 11, 20, 0, 0, 0, loc), 2, 0, loc); !ok || !got.Equal(time.Date(2026, 7, 12, 2, 0, 0, 0, loc)) {
		t.Errorf("past kickoff = %v, want 2026-07-12 02:00 (forward roll)", got)
	}
	// Near-term kickoffs (a few hours either side) are never rolled.
	if got, ok := kickoffAnchoredUTC(time.Date(2026, 7, 11, 12, 0, 0, 0, loc), 15, 0, loc); !ok || !got.Equal(time.Date(2026, 7, 11, 15, 0, 0, 0, loc)) {
		t.Errorf("near-future kickoff wrongly rolled: %v", got)
	}
	if got, ok := kickoffAnchoredUTC(time.Date(2026, 7, 11, 12, 0, 0, 0, loc), 9, 0, loc); !ok || !got.Equal(time.Date(2026, 7, 11, 9, 0, 0, 0, loc)) {
		t.Errorf("recent-past kickoff wrongly rolled: %v", got)
	}
}

// P9: an impossible date ("2.30" = Feb 30) is rejected as an event instead of
// normalising to Mar 2; the slot degrades to a StatusOff placeholder.
func TestParse_PPVEventRejectsImpossibleDate(t *testing.T) {
	p, ok := Parse("PPV EVENT 07: Bad Date Event (2.30 8:00 PM ET)")
	if !ok {
		t.Fatal("expected ok (falls through to off-slot)")
	}
	if p.Status != StatusOff {
		t.Errorf("impossible date must not schedule an event; got status %q", p.Status)
	}
	if p.Slot != "PPV EVENT 07" {
		t.Errorf("slot got %q", p.Slot)
	}
	if !p.StartAt.IsZero() {
		t.Errorf("impossible date must not produce a kickoff, got %v", p.StartAt)
	}
}

// P11: combat-sports names default to a 5h window; prelims and non-combat names
// keep 3h.
func TestDefaultDuration_SportKeyed(t *testing.T) {
	cases := []struct {
		title string
		want  time.Duration
	}{
		{"UFC 329 McGregor vs. Holloway 2", 5 * time.Hour},
		{"Canelo vs Charlo Boxing", 5 * time.Hour},
		{"UFC Fight Night: Della Maddalena vs Prates", 5 * time.Hour},
		{"Some Main Event Spectacular", 5 * time.Hour},
		{"PRELIMS UFC 329", 3 * time.Hour},
		{"EARLY PRELIMS UFC 329", 3 * time.Hour},
		{"Canadiens @ Sabres", 3 * time.Hour},
	}
	for _, c := range cases {
		if got := defaultDuration(c.title); got != c.want {
			t.Errorf("defaultDuration(%q) = %v; want %v", c.title, got, c.want)
		}
	}
}

// Invalid explicit wall clocks must never be normalised by time.Date into a
// different real event. Pipe-format failures are especially important: they
// must not fall through to P10's current-hour LIVE placement.
func TestParse_RejectsMalformedExplicitDateTimes(t *testing.T) {
	cases := []string{
		"LIVE | Team A vs Team B | Tue 31 Apr 20:55 CEST (DK) | 8K EXCLUSIVE | DK: VIAPLAY PPV 9",
		"LIVE | Team A vs Team B | Tue 31 Apr 20:55 XYZ (DK) | 8K EXCLUSIVE | DK: VIAPLAY PPV 9",
		"LIVE | Team A vs Team B | Tue 14 Apr 25:99 CEST (DK) | 8K EXCLUSIVE | DK: VIAPLAY PPV 9",
		"LIVE | Team A vs Team B | 11-07-2026 | 25:99 (GMT) | 8K EXCLUSIVE | US: DAZN PPV 7",
		"LIVE | Team A vs Team B | 2026-08-13 | 25:9 (GMT) | US: DAZN PPV 7",
		"LIVE | Team A vs Team B | 2026-08-13 | 123:00 (GMT) | US: DAZN PPV 7",
		"LIVE | Team A vs Team B | 2026-08-13 | 12:000 (GMT) | US: DAZN PPV 7",
		"LIVE | Team A vs Team B | 30-02-2026 | 20:00 (GMT) | 8K EXCLUSIVE | US: DAZN PPV 7",
		// Unsupported combined layouts are still explicit schedule intent. They
		// must never fall through to the current-hour no-date placement.
		"LIVE | Team A vs Team B | 2026-02-30 25:99 GMT | US: DAZN PPV 7",
		"LIVE | Team A vs Team B | 2026-08-13T25:99:00Z | US: DAZN PPV 7",
		"LIVE | Team A vs Team B | 2026/13/40 20:00 UTC+99:99 | US: DAZN PPV 7",
		"LIVE | Team A vs Team B | Tue, 31 Apr 25:99 CEST | DK: VIAPLAY PPV 9",
		"LIVE | Team A vs Team B | Tuesday 31 Apr at 8:99 PM ET | US: DAZN PPV 7",
		"LIVE | Team A vs Team B | Apr 31, 2026 20:00 Europe/London | UK: DAZN PPV 7",
		"LIVE | Team A vs Team B | 31-Apr-2026 20.00 XYZ | UK: DAZN PPV 7",
		"LIVE | Team A vs Team B | Tomorrow 8:99 PM ET | US: DAZN PPV 7",
		"LIVE | Team A vs Team B | 123:000 Europe/London | US: DAZN PPV 7",
		"LIVE | Team A vs Team B | Tuesday 123:000 | US: DAZN PPV 7",
		"LIVE | Team A vs Team B | GMT | US: DAZN PPV 7",
		"LIVE | Team A vs Team B | Kickoff: 2026-02-30 | US: DAZN PPV 7",
		"LIVE | Team A vs Team B | Schedule 31-Apr-2026 | US: DAZN PPV 7",
		"LIVE | Team A vs Team B | Europe//London | US: DAZN PPV 7",
		"LIVE | Team A vs Team B | Kickoff 25:99 | US: DAZN PPV 7",
		"LIVE | Team A vs Team B | Starts 123:000 | US: DAZN PPV 7",
		"PPV EVENT 07: Bad Time Event (8.13 8:99 PM ET)",
		"NHL | 02 - 13:99pm Golden Knights @ Ducks",
		"LIVE EVENT 02 - 0pm UFC 329 McGregor v Holloway II",
		"EPL 01: 24:00 AFC Bournemouth vs Manchester City",
		"Boxing 1 : FURY vs HALL 13PM",
		"NHL 08 : Team A @ Team B start:2026-02-30 20:00:00 stop:2026-03-01 23:00:00",
		"NHL 08 : Team A @ Team B start:2026-08-13 20:00:00 stop:2026-08-13 25:99:00",
		"(US) (BTN+ 001) | Baseball: Team A Vs Team B (2026-02-30 20:00:00)",
		"(FLSP 701) | flobaseball: Team A vs Team B (2026-08-13 25:99:00)",
	}
	for _, in := range cases {
		t.Run(in, func(t *testing.T) {
			p, ok := Parse(in)
			if ok && !p.StartAt.IsZero() {
				t.Fatalf("malformed date/time scheduled as a real event: status=%q start=%v title=%q", p.Status, p.StartAt, p.Title)
			}
		})
	}
}

func TestParse_PipeScheduleIntentDoesNotTreatScoreAsClock(t *testing.T) {
	for _, in := range []string{
		"Live | Team A/B 2:1 Team C | UEFA Champions League | 8K EXCLUSIVE | CA: DAZN PPV 5",
		"Live | Team A at Team B 2:1 | UEFA Champions League | 8K EXCLUSIVE | CA: DAZN PPV 5",
		"ENDED | Full Time Team A 2:1 Team B | US: DAZN PPV 7",
	} {
		p, ok := Parse(in)
		if !ok {
			t.Fatalf("ordinary score title rejected as schedule intent: %q", in)
		}
		if strings.HasPrefix(strings.ToLower(in), "live") && (p.StartAt.IsZero() || p.Status != StatusLive) {
			t.Fatalf("live score did not retain low-confidence placement: parsed=%+v", p)
		}
		if strings.HasPrefix(strings.ToLower(in), "ended") && p.Status != StatusEnded {
			t.Fatalf("ended score lost definitive state: parsed=%+v", p)
		}
	}
}

func TestParse_SlotStartStopRejectsMalformedExplicitStop(t *testing.T) {
	const prefix = "NHL 08 : Team A @ Team B start:2026-08-13 20:00:00 stop:"
	for _, stop := range []string{
		"",
		"garbage",
		"2026-08-13 2:99",
		"2026-08-13 25:99:00",
		"2026-08-13 23:00:00 trailing-data",
		"2026-08-13 23:00 extra-stop:2026-08-14 00:00",
		"2026-08-13 23:00:00:",
		"2026-08-13 19:59:59", // stop must be after start
	} {
		in := prefix + stop
		if p, ok := Parse(in); ok {
			t.Fatalf("malformed explicit stop parsed: %q => %+v", in, p)
		}
	}
}

func TestParse_SlotStartStopAcceptsOnlyDocumentedStopPrecisions(t *testing.T) {
	const prefix = "NHL 08 : Team A @ Team B start:2090-08-13 20:00:00"
	cases := []struct {
		suffix  string
		wantDur time.Duration
	}{
		{"", 3 * time.Hour},
		{" stop:2090-08-13 23:00:00", 3 * time.Hour},
		{" stop:2090-08-13 23:15", 3*time.Hour + 15*time.Minute},
		{" stop:2090-08-13 23:15:", 3*time.Hour + 15*time.Minute},
	}
	for _, tc := range cases {
		p, ok := Parse(prefix + tc.suffix)
		if !ok {
			t.Fatalf("documented stop form rejected: %q", tc.suffix)
		}
		if p.Duration != tc.wantDur {
			t.Fatalf("stop %q duration=%v want=%v", tc.suffix, p.Duration, tc.wantDur)
		}
	}
}

func TestParse_RejectsDSTGapAndAmbiguousExplicitWallClocks(t *testing.T) {
	cases := []string{
		// America/New_York skips 02:00-02:59 on 2026-03-08.
		"LIVE | Team A vs Team B | 2026-03-08 | 02:30 (ET) | US: DAZN PPV 7",
		// It repeats 01:00-01:59 on 2026-11-01. ET supplies no offset with
		// which to choose one of the two possible instants.
		"LIVE | Team A vs Team B | 2026-11-01 | 01:30 (ET) | US: DAZN PPV 7",
	}
	for _, in := range cases {
		if p, ok := Parse(in); ok && !p.StartAt.IsZero() {
			t.Fatalf("DST-invalid explicit wall clock scheduled: %q => %v", in, p.StartAt)
		}
	}

	ny := etLocation()
	if got, ok := kickoffAnchoredUTC(time.Date(2026, 3, 8, 0, 30, 0, 0, ny), 2, 30, ny); ok {
		t.Fatalf("date-less DST-gap wall clock normalised into %v", got)
	}
	if got, ok := kickoffAnchoredUTC(time.Date(2026, 11, 1, 0, 30, 0, 0, ny), 1, 30, ny); ok {
		t.Fatalf("date-less ambiguous wall clock selected %v", got)
	}
}

func TestParse_ValidWallClockBoundariesStillParse(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		location *time.Location
		hour     int
		minute   int
	}{
		{"midnight am", "NHL | 02 - 12:00am Team A @ Team B", etLocation(), 0, 0},
		{"noon pm", "NHL | 02 - 12:59pm Team A @ Team B", etLocation(), 12, 59},
		{"24h upper bound", "EPL 01: 23:59 Team A vs Team B", ianaZone("Europe/London", "GMT", 0), 23, 59},
		{"ppv ampm upper minute", "PPV EVENT 07: Test Event (8.13 11:59 PM ET)", etLocation(), 23, 59},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, ok := Parse(tc.input)
			if !ok || p.StartAt.IsZero() {
				t.Fatalf("valid boundary did not parse: ok=%v p=%+v", ok, p)
			}
			wall := p.StartAt.In(tc.location)
			if wall.Hour() != tc.hour || wall.Minute() != tc.minute {
				t.Fatalf("wall clock = %02d:%02d, want %02d:%02d", wall.Hour(), wall.Minute(), tc.hour, tc.minute)
			}
		})
	}
}

func TestParse_DistinguishesProviderEndedFromClockElapsed(t *testing.T) {
	explicit, ok := Parse("ENDED | Team A vs Team B | 2026-08-13 | 12:00 (GMT) | US: DAZN PPV 7")
	if !ok || explicit.Status != StatusEnded || !explicit.EndExplicit {
		t.Fatalf("provider ENDED state not marked definitive: ok=%v p=%+v", ok, explicit)
	}

	start := time.Now().UTC().Add(-3 * time.Hour).Truncate(time.Second)
	stop := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	name := "NHL 08 : Team A @ Team B start:" + start.Format("2006-01-02 15:04:05") +
		" stop:" + stop.Format("2006-01-02 15:04:05")
	elapsed, ok := Parse(name)
	if !ok || elapsed.Status != StatusEnded || elapsed.EndExplicit {
		t.Fatalf("clock-elapsed unchanged name should remain overrun-eligible: ok=%v p=%+v", ok, elapsed)
	}
}

func TestInferYearlessDate_ValidatesTheInferredYear(t *testing.T) {
	now := time.Date(2027, time.December, 15, 12, 0, 0, 0, time.UTC)
	leapDay, ok := inferYearlessDate(time.February, 29, 20, 0, time.UTC, now)
	if !ok || !leapDay.Equal(time.Date(2028, time.February, 29, 20, 0, 0, 0, time.UTC)) {
		t.Fatalf("next-year leap day = (%v, %v), want 2028-02-29", leapDay, ok)
	}
	if _, ok := inferYearlessDate(time.April, 31, 20, 0, time.UTC, now); ok {
		t.Fatal("April 31 must be rejected")
	}
}

// ─── dated shorthand variant (provider addition ~2026-08) ───

func TestParse_IboostShorthand_DatedVariant_Yesterday(t *testing.T) {
	// Build yesterday's date dynamically so the assertion holds regardless
	// of when the test runs: a dated slot name from yesterday's slate must
	// anchor to YESTERDAY (no forward roll) and read as ended.
	et := etLocation()
	y := time.Now().In(et).AddDate(0, 0, -1)
	in := fmt.Sprintf("NFL  | 05 - %d/%d 6pm Commanders at Ravens", int(y.Month()), y.Day())
	got, ok := Parse(in)
	if !ok {
		t.Fatalf("expected ok for %q", in)
	}
	if got.Title != "Commanders at Ravens" {
		t.Errorf("title got %q", got.Title)
	}
	if got.Slot != "NFL PPV 05" {
		t.Errorf("slot got %q", got.Slot)
	}
	if got.Sport != "nfl" {
		t.Errorf("sport got %q", got.Sport)
	}
	local := got.StartAt.In(et)
	if local.Day() != y.Day() || local.Month() != y.Month() {
		t.Errorf("kickoff anchored to %v, want yesterday %d/%d", local, int(y.Month()), y.Day())
	}
	if local.Hour() != 18 {
		t.Errorf("kickoff hour got %d ET, want 18", local.Hour())
	}
	if got.Status != StatusEnded {
		t.Errorf("status got %q, want ended for yesterday's game", got.Status)
	}
}

func TestParse_IboostShorthand_DatedVariant_Tomorrow(t *testing.T) {
	et := etLocation()
	tm := time.Now().In(et).AddDate(0, 0, 1)
	in := fmt.Sprintf("NHL  | 03 - %d/%d 7:30pm Rangers at Penguins", int(tm.Month()), tm.Day())
	got, ok := Parse(in)
	if !ok {
		t.Fatalf("expected ok for %q", in)
	}
	local := got.StartAt.In(et)
	if local.Day() != tm.Day() || local.Hour() != 19 || local.Minute() != 30 {
		t.Errorf("kickoff got %v, want tomorrow 19:30 ET", local)
	}
	if got.Status != StatusUpcoming {
		t.Errorf("status got %q, want upcoming", got.Status)
	}
}

func TestParse_IboostShorthand_ImpossibleDateTokenRefused(t *testing.T) {
	// A PRESENT date token with an impossible month/day must refuse the
	// tail, not degrade to an undated parse anchored to today (month 0
	// would collide with timedTailFrom's no-date sentinel).
	for _, in := range []string{
		"NFL  | 05 - 0/28 6pm Commanders at Ravens",
		"NFL  | 05 - 00/28 6pm Commanders at Ravens",
		"NFL  | 05 - 13/28 6pm Commanders at Ravens",
		"NFL  | 05 - 8/0 6pm Commanders at Ravens",
		"NFL  | 05 - 8/32 6pm Commanders at Ravens",
	} {
		if got, ok := Parse(in); ok {
			t.Errorf("%q parsed (start=%v); want refusal", in, got.StartAt)
		}
	}
}
