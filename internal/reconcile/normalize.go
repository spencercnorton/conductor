package reconcile

import (
	"regexp"
	"strings"
)

// normalizeTitle reduces a title to a comparison key: lowercase, accents
// left as-is (rare in EPG), punctuation dropped, a few noise tokens and a
// trailing "(2019)"-style year removed, whitespace collapsed.
//
// The goal is that "Marvel's Daredevil" / "Daredevil (2015)" / "daredevil"
// all collapse to the same key, while keeping enough signal that distinct
// shows don't collide.
func normalizeTitle(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = yearParens.ReplaceAllString(s, " ")
	// Drop apostrophes WITHOUT inserting a gap so "marvel's"/"it's" collapse
	// to "marvels"/"its" (matching the possessive-prefix stripping below and
	// the apostrophe-free spelling providers sometimes use).
	s = apostrophes.ReplaceAllString(s, "")
	s = strings.ReplaceAll(s, "&", " and ")
	s = nonAlnum.ReplaceAllString(s, " ")
	for _, stop := range stopPrefixes {
		s = strings.TrimPrefix(s, stop+" ")
	}
	s = strings.Join(strings.Fields(s), " ")
	return s
}

var (
	yearParens  = regexp.MustCompile(`\((19|20)\d{2}\)`)
	apostrophes = regexp.MustCompile("['’ʼ`]")
	nonAlnum    = regexp.MustCompile(`[^a-z0-9]+`)
	// Country/edition suffix tokens that *arr appends but the EPG omits
	// (and vice versa). Stripped from the END of an already-normalized key.
	editionSuffixes = []string{"us", "uk", "au", "ca"}
	// Possessive-style prefixes that providers add inconsistently.
	stopPrefixes = []string{"marvels", "dcs", "the"}
)

// titleKey is normalizeTitle plus trailing edition-suffix removal, used for
// the coarse series/movie title equality test. "Temptation Island US" and
// "Temptation Island" share a key; "The Office" and "Office" share a key.
func titleKey(s string) string {
	k := normalizeTitle(s)
	for {
		trimmed := false
		for _, suf := range editionSuffixes {
			if strings.HasSuffix(k, " "+suf) {
				k = strings.TrimSuffix(k, " "+suf)
				trimmed = true
			}
		}
		if !trimmed {
			break
		}
	}
	return strings.TrimSpace(k)
}

// canonicalIMDb strips a leading "tt" and any zero-padding so "tt0848228",
// "0848228" and "848228" all compare equal. Returns "" for empty input.
func canonicalIMDb(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.TrimPrefix(s, "tt")
	s = strings.TrimLeft(s, "0")
	return s
}
