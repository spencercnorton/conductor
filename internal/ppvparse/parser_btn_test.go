package ppvparse

import (
	"testing"
	"time"
)

// TestParse_PPVEventBareOff: iboost idles most PPV EVENT slots as a bare
// "PPV EVENT 05" with no colon (live 2026-07-20). Those must parse to a
// StatusOff slot (not fall through to false), otherwise ppvsync skips the
// channel and its guide freezes on the last event it showed.
func TestParse_PPVEventBareOff(t *testing.T) {
	for _, in := range []string{"PPV EVENT 05", "PPV EVENT 16", "PPV EVENT 5", "PPV EVENT 07"} {
		p, ok := Parse(in)
		if !ok || p.Status != StatusOff {
			t.Fatalf("%q: got ok=%v status=%q, want ok=true StatusOff", in, ok, p.Status)
		}
		if p.Slot == "" {
			t.Errorf("%q: off-state must carry a slot label", in)
		}
		if !p.StartAt.IsZero() {
			t.Errorf("%q: off-state must not invent a kickoff, got %v", in, p.StartAt)
		}
	}
	// Regression: a real "PPV EVENT 01: … (…ET)" still parses as an event,
	// not degraded to a placeholder by the now-optional colon.
	p, ok := Parse("PPV EVENT 01: WWE Monday Night Raw (7.20 8:00 PM ET)")
	if !ok || p.Status == StatusOff || p.Title == "" {
		t.Fatalf("real PPV EVENT lost its event parse: ok=%v status=%q title=%q", ok, p.Status, p.Title)
	}
}

// TestParse_OffIdleClassification: StatusOff parses must flag OffIdle only
// when the slot is genuinely idle. A substantive tail (a truncated event
// name that lost its time, or a venue label) is an AMBIGUOUS off — still
// StatusOff for guide presence, but OffIdle=false so ppvsync won't sweep a
// possibly-still-upcoming event.
func TestParse_OffIdleClassification(t *testing.T) {
	idle := []string{
		"PPV EVENT 05", "PPV EVENT 09:", "PPV EVENT 09: TBA",
		"NHL | 08 -", "NFL  | 01 -", "UFC 03 :", "UFC 03 : TBA",
		"LIVE EVENT 33 -", "LIVE EVENT 03 - NO EVENT",
		"UK: EPL 1 PPV ᵁᴴᴰ ³⁸⁴⁰ᴾ",
		"(US) (BTN+ 002) |  (2098-12-31 08:00:01)",
	}
	for _, in := range idle {
		p, ok := Parse(in)
		if !ok || p.Status != StatusOff {
			t.Fatalf("%q: want StatusOff, got ok=%v status=%q", in, ok, p.Status)
		}
		if !p.OffIdle {
			t.Errorf("%q: want OffIdle=true (genuine idle)", in)
		}
	}
	// Ambiguous off: StatusOff but must NOT be flagged idle — a truncated
	// event title or an untimed venue label could still be a real event.
	ambiguous := []string{
		"PPV EVENT 05: UFC 330 Main Card",
		"UFC 03 : UFC 330 Prelims",
		"LIVE EVENT 27 - OUTDOOR THEATRE Live From Coachella 2026",
	}
	for _, in := range ambiguous {
		p, ok := Parse(in)
		if !ok || p.Status != StatusOff {
			t.Fatalf("%q: want StatusOff, got ok=%v status=%q", in, ok, p.Status)
		}
		if p.OffIdle {
			t.Errorf("%q: want OffIdle=false (ambiguous — keep any event)", in)
		}
	}
}

// BTN+ suffixes are America/New_York wall clocks even when the home venue is
// in another zone. The suffix marks stream availability 10-30 minutes before
// official kickoff; its pre-roll is intentional and must survive conversion
// to UTC. The title-embedded schedule is stripped, not used to rewrite it.
func TestParse_BTNPlus_EventTimestampsAreEasternWallClock(t *testing.T) {
	newYork, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	chicago, err := time.LoadLocation("America/Chicago")
	if err != nil {
		t.Fatal(err)
	}
	losAngeles, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name        string
		input       string
		wantUTC     time.Time
		officialAt  time.Time
		wantPreRoll time.Duration
		wantTitle   string
		wantSlot    string
	}{
		{
			name:        "EDT Bellarmine at Indiana 30-minute preroll",
			input:       "(US) (BTN+ 001) | Soccer (W): Bellarmine at Indiana_ 21_08_2026_ 20:00 (2026-08-21 19:30:00)",
			wantUTC:     time.Date(2026, 8, 21, 23, 30, 0, 0, time.UTC),
			officialAt:  time.Date(2026, 8, 21, 20, 0, 0, 0, newYork),
			wantPreRoll: 30 * time.Minute,
			wantTitle:   "Soccer (W): Bellarmine at Indiana",
			wantSlot:    "BIG TEN 001",
		},
		{
			name:        "EST conversion rolls the UTC date",
			input:       "(US) (BTN+ 002) | Basketball: Team A Vs. Team B_ 01_01_2027_ 00:00 (2026-12-31 23:50:00)",
			wantUTC:     time.Date(2027, 1, 1, 4, 50, 0, 0, time.UTC),
			officialAt:  time.Date(2027, 1, 1, 0, 0, 0, 0, newYork),
			wantPreRoll: 10 * time.Minute,
			wantTitle:   "Basketball: Team A Vs. Team B",
			wantSlot:    "BIG TEN 002",
		},
		{
			// Wisconsin's official schedule lists Aug 23 at 2 p.m. CT.
			name:        "CT home event remains an ET suffix",
			input:       "(US) (BTN+ 003) | Soccer (M): Oakland at Wisconsin_ 23_08_2026_ 14:00 (2026-08-23 14:50:00)",
			wantUTC:     time.Date(2026, 8, 23, 18, 50, 0, 0, time.UTC),
			officialAt:  time.Date(2026, 8, 23, 14, 0, 0, 0, chicago),
			wantPreRoll: 10 * time.Minute,
			wantTitle:   "Soccer (M): Oakland at Wisconsin",
			wantSlot:    "BIG TEN 003",
		},
		{
			// Washington's official schedule lists Aug 23 at 2 p.m. PT.
			name:        "PT home event remains an ET suffix",
			input:       "(US) (BTN+ 004) | Soccer (W): Montana at Washington_ 23_08_2026_ 14:00 (2026-08-23 16:50:00)",
			wantUTC:     time.Date(2026, 8, 23, 20, 50, 0, 0, time.UTC),
			officialAt:  time.Date(2026, 8, 23, 14, 0, 0, 0, losAngeles),
			wantPreRoll: 10 * time.Minute,
			wantTitle:   "Soccer (W): Montana at Washington",
			wantSlot:    "BIG TEN 004",
		},
		{
			// UCLA's official schedule lists Aug 24 at 7 p.m. PT. This also
			// proves an EDT evening boundary can roll into the next UTC date.
			name:        "PT evening event rolls the UTC date",
			input:       "(US) (BTN+ 005) | Soccer (M): CSU Bakersfield at UCLA_ 24_08_2026_ 19:00 (2026-08-24 21:50:00)",
			wantUTC:     time.Date(2026, 8, 25, 1, 50, 0, 0, time.UTC),
			officialAt:  time.Date(2026, 8, 24, 19, 0, 0, 0, losAngeles),
			wantPreRoll: 10 * time.Minute,
			wantTitle:   "Soccer (M): CSU Bakersfield at UCLA",
			wantSlot:    "BIG TEN 005",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, ok := Parse(tc.input)
			if !ok {
				t.Fatal("BTN+ event did not parse")
			}
			if !p.StartAt.Equal(tc.wantUTC) {
				t.Fatalf("start got %v want %v", p.StartAt, tc.wantUTC)
			}
			if got := tc.officialAt.UTC().Sub(p.StartAt); got != tc.wantPreRoll {
				t.Fatalf("provider pre-roll got %v want %v", got, tc.wantPreRoll)
			}
			if p.Title != tc.wantTitle {
				t.Errorf("title got %q want %q", p.Title, tc.wantTitle)
			}
			if p.Slot != tc.wantSlot {
				t.Errorf("slot got %q want %q", p.Slot, tc.wantSlot)
			}
			if p.Duration != 3*time.Hour {
				t.Errorf("duration got %v want 3h", p.Duration)
			}
		})
	}
}

func TestParse_BTNPlus_AuditWindowIsNotFalseLive(t *testing.T) {
	const input = "(US) (BTN+ 001) | Soccer (W): Bellarmine at Indiana_ 21_08_2026_ 20:00 (2026-08-21 19:30:00)"
	p, ok := Parse(input)
	if !ok {
		t.Fatal("BTN+ event did not parse")
	}

	// Plex attempted the channel at 20:53Z. Treating 19:30 as UTC put that
	// instant inside a false three-hour live window; Eastern conversion leaves
	// it correctly before the 23:30Z provider boundary.
	auditAt := time.Date(2026, 8, 21, 20, 53, 30, 0, time.UTC)
	if !auditAt.Before(p.StartAt) {
		t.Fatalf("audit instant %v must be before corrected boundary %v", auditAt, p.StartAt)
	}
	oldWrongStart := time.Date(2026, 8, 21, 19, 30, 0, 0, time.UTC)
	if auditAt.Before(oldWrongStart) || !auditAt.Before(oldWrongStart.Add(3*time.Hour)) {
		t.Fatal("fixture no longer reproduces the old false-live window")
	}
}

func TestParseBTNPlusEasternWindowPreservesSeconds(t *testing.T) {
	got, ok := parseBTNPlusEasternWindow("2026-08-21 19:30:17")
	if !ok {
		t.Fatal("valid Eastern boundary did not parse")
	}
	want := time.Date(2026, 8, 21, 23, 30, 17, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("boundary got %v want %v", got, want)
	}
}

func TestParse_BTNPlus_RejectsMalformedOrAmbiguousEasternWindows(t *testing.T) {
	cases := []string{
		"(US) (BTN+ 001) | Soccer: Team A at Team B (2026-02-30 19:30:00)",
		"(US) (BTN+ 001) | Soccer: Team A at Team B (2026-08-21 25:30:00)",
		"(US) (BTN+ 001) | Soccer: Team A at Team B (2026-08-21 19:60:00)",
		"(US) (BTN+ 001) | Soccer: Team A at Team B (2026-08-21 19:30:60)",
		"(US) (BTN+ 001) | Soccer: Team A at Team B (2026-08-21 19:30)",
		// America/New_York skips 02:00-02:59 on 2026-03-08.
		"(US) (BTN+ 001) | Soccer: Team A at Team B (2026-03-08 02:30:00)",
		// America/New_York repeats 01:00-01:59 on 2026-11-01. With no
		// offset in the catalogue suffix, neither instant is safe to guess.
		"(US) (BTN+ 001) | Soccer: Team A at Team B (2026-11-01 01:30:00)",
	}
	for _, input := range cases {
		t.Run(input, func(t *testing.T) {
			if p, ok := Parse(input); ok {
				t.Fatalf("invalid BTN+ window parsed: %+v", p)
			}
		})
	}
}

// TestParse_BTNPlus_Off: idle BTN+ slots carry an empty title and a
// far-future sentinel year (2098). They must parse to StatusOff with no
// kickoff so the channel shows "No Event Scheduled" placeholders.
func TestParse_BTNPlus_Off(t *testing.T) {
	for _, in := range []string{
		"(US) (BTN+ 002) |  (2098-12-31 08:00:01)",
		"(US) (BTN+ 016) |  (2098-12-31 08:00:15)",
		"(US) (BTN+ 003) | _ 21_08_2026_ 20:00 (2026-08-21 19:30:00)",
		"(US) (BTN+ 004) | Soccer: Placeholder Event (2098-12-31 08:00:04)",
	} {
		p, ok := Parse(in)
		if !ok || p.Status != StatusOff {
			t.Fatalf("%q: got ok=%v status=%q, want ok=true StatusOff", in, ok, p.Status)
		}
		if !p.StartAt.IsZero() {
			t.Errorf("%q: idle BTN+ must not invent a kickoff, got %v", in, p.StartAt)
		}
		if p.Slot == "" {
			t.Errorf("%q: off-state must carry a slot label", in)
		}
	}
}
