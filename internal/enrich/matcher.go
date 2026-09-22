package enrich

import (
	"strings"
	"time"
	"unicode"
)

// Match is the matcher's verdict on one TMDb candidate.
type Match struct {
	TMDbID     int
	Confidence float64 // 0.0–1.0
	TitleSim   float64 // 0.0–1.0; exposed so callers can short-circuit on near-exact title
	Reason     string  // short explanation for logs

	// HasSecondarySignal is true when the candidate has at least one
	// corroborating data point beyond title — a candidate release year,
	// or non-trivial popularity, or any votes. Used by callers as a
	// guard on the titleSim short-circuit so they don't accept a stub
	// TMDb entry that happens to share a generic title (e.g. "The NFL"
	// id=235004, popularity ≈ 0, no first_air_date).
	HasSecondarySignal bool
}

// MatchScore computes a confidence score for a (candidate, target) pair.
// Used by enricher.go to pick the best of several TMDb hits.
//
// The scoring blends:
//   1. Title similarity (Jaro-Winkler-ish, weighted 0.5)
//   2. Year exact match (weighted 0.25; 0.1 if within ±1 year)
//   3. Popularity prior (weighted 0.15; logistic on TMDb popularity score)
//   4. Vote average bonus (weighted 0.1; >7.0 popular = signal of canonical entry)
//
// Returns a Match with Confidence in [0, 1]. The enricher accepts a match
// only when Confidence >= 0.85 (per spec §6.3).
type MatchInput struct {
	TargetTitle string
	TargetYear  int          // 0 = unknown
	TargetDate  time.Time    // when known, e.g. for episode airdate matching

	CandidateTitle      string
	CandidateOrigTitle  string
	CandidateReleaseStr string  // YYYY-MM-DD
	CandidatePopularity float64
	CandidateVoteAvg    float64
}

func MatchScore(in MatchInput) Match {
	titleSim := titleSimilarity(in.TargetTitle, in.CandidateTitle)
	if origSim := titleSimilarity(in.TargetTitle, in.CandidateOrigTitle); origSim > titleSim {
		titleSim = origSim
	}

	yearScore := 0.0
	candYear := parseYearPrefix(in.CandidateReleaseStr)
	switch {
	case in.TargetYear == 0 || candYear == 0:
		yearScore = 0.5 // unknown — neutral
	case candYear == in.TargetYear:
		yearScore = 1.0
	case abs(candYear-in.TargetYear) == 1:
		yearScore = 0.4
	default:
		yearScore = 0.0
	}

	popularity := logistic(in.CandidatePopularity, 5.0, 1.0) // 5.0 = midpoint
	voteBonus := 0.0
	if in.CandidateVoteAvg >= 7.0 {
		voteBonus = (in.CandidateVoteAvg - 7.0) / 3.0 // 7.0 → 0, 10.0 → 1.0
		if voteBonus > 1.0 {
			voteBonus = 1.0
		}
	}

	conf := 0.5*titleSim + 0.25*yearScore + 0.15*popularity + 0.1*voteBonus

	// Secondary signal = anything beyond title that says this is a real
	// TMDb entry, not an empty stub. Threshold for popularity is set
	// just above the floor where genuinely-tracked shows sit (logistic
	// midpoint is 5.0; raw popularity ≥ 2.0 is "TMDb actually has
	// metadata for this"). Vote-average is meaningless without count
	// so we don't gate on it here.
	hasSecondary := candYear != 0 || in.CandidatePopularity >= 2.0

	return Match{
		Confidence:         conf,
		TitleSim:           titleSim,
		HasSecondarySignal: hasSecondary,
		Reason: shortJoin(
			"title=" + ftoa(titleSim),
			"year=" + ftoa(yearScore),
			"pop=" + ftoa(popularity),
			"vote=" + ftoa(voteBonus),
		),
	}
}

// titleSimilarity is a cheap fuzzy-match: token-set Jaccard with a bonus
// for shared word-prefix. Not as strong as Jaro-Winkler but good enough
// for "The Bear" vs "The Bear: Behind the Pass" type calls. Returns 0..1.
func titleSimilarity(a, b string) float64 {
	if a == "" || b == "" {
		return 0
	}
	a = normalizeForMatch(a)
	b = normalizeForMatch(b)
	if a == b {
		return 1.0
	}

	tokensA := tokenSet(a)
	tokensB := tokenSet(b)
	inter := 0
	for t := range tokensA {
		if _, ok := tokensB[t]; ok {
			inter++
		}
	}
	union := len(tokensA) + len(tokensB) - inter
	if union == 0 {
		return 0
	}
	jaccard := float64(inter) / float64(union)

	// Bonus when one string is a prefix of the other (handles "The Bear"
	// vs "The Bear: Behind the Pass").
	if strings.HasPrefix(a, b) || strings.HasPrefix(b, a) {
		jaccard = jaccard + 0.2
		if jaccard > 1.0 {
			jaccard = 1.0
		}
	}
	return jaccard
}

func normalizeForMatch(s string) string {
	s = strings.ToLower(s)
	var b strings.Builder
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == ' ' {
			b.WriteRune(r)
		} else {
			b.WriteRune(' ')
		}
	}
	// Collapse whitespace.
	parts := strings.Fields(b.String())
	// Drop common stop words that don't help disambiguate.
	stop := map[string]bool{"the": true, "a": true, "an": true, "of": true, "and": true}
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if stop[p] {
			continue
		}
		out = append(out, p)
	}
	return strings.Join(out, " ")
}

func tokenSet(s string) map[string]struct{} {
	m := map[string]struct{}{}
	for _, t := range strings.Fields(s) {
		m[t] = struct{}{}
	}
	return m
}

// parseYearPrefix extracts a year from "YYYY-MM-DD" or "YYYY".
func parseYearPrefix(s string) int {
	if len(s) < 4 {
		return 0
	}
	y := 0
	for i := 0; i < 4; i++ {
		c := s[i]
		if c < '0' || c > '9' {
			return 0
		}
		y = y*10 + int(c-'0')
	}
	return y
}

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

// logistic maps x ∈ ℝ to (0, 1) via 1/(1+exp(-(x-mid)/width)).
func logistic(x, mid, width float64) float64 {
	if width <= 0 {
		width = 1
	}
	z := -(x - mid) / width
	// Cheap approx — exact math/log is overkill here.
	if z > 30 {
		return 0
	}
	if z < -30 {
		return 1
	}
	// Use library exp via a closure to avoid an import cycle in tests.
	return 1.0 / (1.0 + expApprox(z))
}

// expApprox is a 6-term Taylor approximation around 0; clamped on |z|>10.
// Good enough for confidence scoring; we don't need IEEE precision.
func expApprox(z float64) float64 {
	if z > 10 {
		return 22026.46
	}
	if z < -10 {
		return 0.0000453999
	}
	// e^z = 1 + z + z²/2! + z³/3! + …
	t := 1.0
	sum := 1.0
	for i := 1; i < 12; i++ {
		t *= z / float64(i)
		sum += t
	}
	return sum
}

func ftoa(f float64) string {
	// 2 decimals, no trailing zeros for compact log lines.
	if f == 0 {
		return "0"
	}
	if f == 1 {
		return "1"
	}
	whole := int(f * 100)
	dec := whole % 100
	out := ""
	if dec == 0 {
		out = "0"
	} else if dec%10 == 0 {
		out = "0." + string(rune('0'+dec/10))
	} else {
		out = "0." + string(rune('0'+dec/10)) + string(rune('0'+dec%10))
	}
	if whole/100 > 0 {
		out = "1"
	}
	return out
}

func shortJoin(parts ...string) string {
	return strings.Join(parts, " ")
}
