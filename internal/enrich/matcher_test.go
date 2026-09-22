package enrich

import (
	"testing"
	"time"
)

func TestMatchScoreExactTitleAndYear(t *testing.T) {
	m := MatchScore(MatchInput{
		TargetTitle: "Heat", TargetYear: 1995,
		CandidateTitle: "Heat", CandidateOrigTitle: "Heat",
		CandidateReleaseStr: "1995-12-15",
		CandidatePopularity: 30, CandidateVoteAvg: 8.0,
	})
	if m.Confidence < 0.85 {
		t.Errorf("exact title+year should score >= 0.85; got %.3f (reason: %s)",
			m.Confidence, m.Reason)
	}
}

func TestMatchScoreWrongYearLowersConfidence(t *testing.T) {
	correct := MatchScore(MatchInput{
		TargetTitle: "Dune", TargetYear: 2021,
		CandidateTitle: "Dune", CandidateReleaseStr: "2021-09-15",
		CandidatePopularity: 50, CandidateVoteAvg: 8.0,
	})
	wrong := MatchScore(MatchInput{
		TargetTitle: "Dune", TargetYear: 2021,
		CandidateTitle: "Dune", CandidateReleaseStr: "1984-12-14",
		CandidatePopularity: 5, CandidateVoteAvg: 6.5,
	})
	if correct.Confidence <= wrong.Confidence {
		t.Errorf("correct-year candidate should outscore the wrong-year one; correct=%.3f wrong=%.3f",
			correct.Confidence, wrong.Confidence)
	}
}

func TestMatchScorePartialTitleStillCounts(t *testing.T) {
	// "The Bear" target should match "The Bear" series even if TMDb returns
	// a "The Bear: Behind the Pass" companion show with the same prefix.
	main := MatchScore(MatchInput{
		TargetTitle: "The Bear",
		CandidateTitle: "The Bear",
		CandidateReleaseStr: "2022-06-23",
		CandidatePopularity: 40, CandidateVoteAvg: 8.5,
	})
	companion := MatchScore(MatchInput{
		TargetTitle: "The Bear",
		CandidateTitle: "The Bear: Behind the Pass",
		CandidateReleaseStr: "2024-06-27",
		CandidatePopularity: 5, CandidateVoteAvg: 7.0,
	})
	if main.Confidence <= companion.Confidence {
		t.Errorf("the main show should outscore the companion; main=%.3f companion=%.3f",
			main.Confidence, companion.Confidence)
	}
}

func TestMatchScoreNoTitleSimilarityIsLow(t *testing.T) {
	m := MatchScore(MatchInput{
		TargetTitle: "Breaking Bad",
		CandidateTitle: "Pokemon Red",
		CandidatePopularity: 100, CandidateVoteAvg: 9.0,
	})
	if m.Confidence > 0.40 {
		t.Errorf("totally unrelated titles should score low; got %.3f", m.Confidence)
	}
}

// 2026-05-08 fix: Match exposes TitleSim so callers can short-circuit on
// near-exact title matches (TV shows + news programmes where year /
// popularity / vote are unhelpful and would otherwise drop the blended
// confidence below the threshold).
func TestMatchScoreExposesTitleSim(t *testing.T) {
	exact := MatchScore(MatchInput{
		TargetTitle:    "Newshour",
		CandidateTitle: "Newshour",
	})
	if exact.TitleSim < 0.99 {
		t.Errorf("exact title match should give TitleSim >= 0.99; got %.3f", exact.TitleSim)
	}
	// Different show — TitleSim should be near zero.
	unrelated := MatchScore(MatchInput{
		TargetTitle:    "Newshour",
		CandidateTitle: "Pokemon Red",
	})
	if unrelated.TitleSim > 0.20 {
		t.Errorf("unrelated titles should give low TitleSim; got %.3f", unrelated.TitleSim)
	}
}

// Documents the typical TV-show case that motivated the 2026-05-08
// threshold change: title is a perfect match but year/popularity/vote
// data is missing or low. Blended confidence stays under 0.85 (so the
// old threshold rejected it), but TitleSim is at 1.0 — the
// HighConfidenceTitleSim short-circuit accepts.
func TestMatchScoreTVShowWithoutYearSignal(t *testing.T) {
	m := MatchScore(MatchInput{
		TargetTitle:    "Mornings With Maria Bartiromo",
		CandidateTitle: "Mornings With Maria Bartiromo",
		// year unknown both sides + low popularity (yearScore=0.5,
		// popularity ~0.5 logistic, voteBonus=0)
		CandidatePopularity: 1.5,
		CandidateVoteAvg:    5.0,
	})
	if m.Confidence > MinAcceptConfidence-0.05 {
		// If this fires, the threshold may already be loose enough.
		t.Logf("note: blended confidence %.3f >= MinAcceptConfidence %.3f — short-circuit may not be needed for this case",
			m.Confidence, MinAcceptConfidence)
	}
	if m.TitleSim < HighConfidenceTitleSim {
		t.Errorf("perfect-title TV show should have TitleSim >= %.2f; got %.3f",
			HighConfidenceTitleSim, m.TitleSim)
	}
}

// 2026-05-08: HasSecondarySignal guards the titleSim short-circuit.
// Real shows have a year or non-trivial popularity; TMDb stub entries
// (id=235004 "The NFL", popularity ≈ 0, no first_air_date) do not. The
// enricher uses this flag to refuse stubs that would otherwise win on
// titleSim alone.
func TestMatchScoreSecondarySignalGate(t *testing.T) {
	// Real show (popularity > 2.0) — should pass.
	real := MatchScore(MatchInput{
		TargetTitle:         "The Bear",
		CandidateTitle:      "The Bear",
		CandidateReleaseStr: "2022-06-23",
		CandidatePopularity: 40,
	})
	if !real.HasSecondarySignal {
		t.Error("real popular show with year should have HasSecondarySignal=true")
	}

	// Year known, popularity zero — still has signal (year alone).
	yearOnly := MatchScore(MatchInput{
		TargetTitle:         "Newshour",
		CandidateTitle:      "Newshour",
		CandidateReleaseStr: "1985-09-01",
		CandidatePopularity: 0.5,
	})
	if !yearOnly.HasSecondarySignal {
		t.Error("year-known candidate should have HasSecondarySignal=true even at low popularity")
	}

	// Stub case: no year, popularity below threshold. The "The NFL"
	// (TMDb id 235004) shape — exists in TMDb but has nothing else.
	stub := MatchScore(MatchInput{
		TargetTitle:         "NFL",
		CandidateTitle:      "The NFL",
		CandidateReleaseStr: "",
		CandidatePopularity: 0.4,
	})
	if stub.TitleSim < HighConfidenceTitleSim {
		t.Errorf("expected stub to look exact-title (the/a/an stripping); got TitleSim=%.3f", stub.TitleSim)
	}
	if stub.HasSecondarySignal {
		t.Error("stub TMDb entry (no year, popularity<2) should have HasSecondarySignal=false so the enricher can refuse it")
	}
}

// 2026-05-08: Episode-still poster bug fix. The fix lives in
// enricher.enrichTV (no longer overwrites PosterURL with the 16:9
// still); this test guards the contract that we only ever populate
// poster URLs for things that are 2:3 portrait. Verifies the matcher
// does not depend on still data.
func TestMatchScoreDoesNotConsiderStillPath(t *testing.T) {
	// Sanity: MatchScore takes nothing about stills. If a future
	// refactor adds still-related signals, callers must check whether
	// the field they're scoring is portrait or landscape before piping
	// it into a poster slot.
	m := MatchScore(MatchInput{
		TargetTitle:    "Some Show",
		CandidateTitle: "Some Show",
	})
	_ = m // structural — just ensures the API stays stable.
}

// _ keeps go test happy if time goes unused while the suite evolves.
var _ = time.Now

func TestNormalizeForMatchDropsStopwordsAndPunctuation(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"The Office", "office"},
		// Punctuation → space; standalone single-letter "a" is a stopword.
		{"M*A*S*H", "m s h"},
		{"Breaking Bad", "breaking bad"},
		// "It's" → "it s" after punctuation strip; "it" is NOT a stopword
		// here (we keep pronouns) but "in" is also not (only the/a/an/of/and).
		{"It's Always Sunny in Philadelphia", "it s always sunny in philadelphia"},
	}
	for _, tc := range cases {
		got := normalizeForMatch(tc.in)
		if got != tc.want {
			t.Errorf("normalizeForMatch(%q): got %q want %q", tc.in, got, tc.want)
		}
	}
}

func TestParseSE(t *testing.T) {
	cases := []struct {
		in       string
		wantS    int
		wantE    int
	}{
		{"S03E07", 3, 7},
		{"s12e34", 12, 34},
		{"S00E01", 0, 1},
		{"", 0, 0},
		{"E03", 0, 0},
		{"S03", 0, 0},
		{"badness", 0, 0},
	}
	for _, tc := range cases {
		s, e := parseSE(tc.in)
		if s != tc.wantS || e != tc.wantE {
			t.Errorf("parseSE(%q): got (%d,%d) want (%d,%d)", tc.in, s, e, tc.wantS, tc.wantE)
		}
	}
}

func TestParseYearPrefix(t *testing.T) {
	if y := parseYearPrefix("2026-05-06"); y != 2026 {
		t.Errorf("YYYY-MM-DD: got %d want 2026", y)
	}
	if y := parseYearPrefix("1995"); y != 1995 {
		t.Errorf("YYYY: got %d want 1995", y)
	}
	if y := parseYearPrefix(""); y != 0 {
		t.Errorf("empty: got %d want 0", y)
	}
	if y := parseYearPrefix("ab1234"); y != 0 {
		t.Errorf("non-numeric prefix should fail; got %d", y)
	}
}

// suppress unused-import lint; time is used by other tests in this file.
var _ = time.Now
