package store

import (
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestSelectCanonicalEPGCandidates_MaximizesCoverageWithinSource(t *testing.T) {
	base := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)
	long := epgCandidate("00000000-0000-0000-0000-000000000001", base, 0, 45, 0, "1:primary", "long")
	first := epgCandidate("00000000-0000-0000-0000-000000000002", base, 0, 30, 0, "1:primary", "first")
	second := epgCandidate("00000000-0000-0000-0000-000000000003", base, 30, 60, 0, "1:primary", "second")

	got := canonicalIDs(selectCanonicalEPGCandidates([]canonicalEPGCandidate{long, second, first}))
	want := []uuid.UUID{first.ID, second.ID}
	if !slices.Equal(got, want) {
		t.Fatalf("selected IDs = %v, want adjacent maximum-coverage rows %v", got, want)
	}
}

func TestSelectCanonicalEPGCandidates_WholeProgramPriorityAndGaps(t *testing.T) {
	base := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)
	highA := epgCandidate("00000000-0000-0000-0000-000000000011", base, 0, 20, 0, "1:primary", "high-a")
	highB := epgCandidate("00000000-0000-0000-0000-000000000012", base, 40, 60, 0, "1:primary", "high-b")
	offsetA := epgCandidate("00000000-0000-0000-0000-000000000013", base, 5, 25, 10, "1:offset", "offset-a")
	offsetB := epgCandidate("00000000-0000-0000-0000-000000000014", base, 35, 55, 10, "1:offset", "offset-b")
	gap := epgCandidate("00000000-0000-0000-0000-000000000015", base, 20, 40, 10, "1:supplement", "gap")
	partial := epgCandidate("00000000-0000-0000-0000-000000000016", base, 55, 70, 10, "1:supplement", "partial")

	input := []canonicalEPGCandidate{partial, offsetB, highB, gap, offsetA, highA}
	want := []uuid.UUID{highA.ID, gap.ID, highB.ID}
	got := canonicalIDs(selectCanonicalEPGCandidates(input))
	if !slices.Equal(got, want) {
		t.Fatalf("selected IDs = %v, want %v", got, want)
	}

	// Canonical rows are never clipped or split. The lower-priority partial
	// row [55,70) overlaps the preferred [40,60), so it stays suppressed as a
	// whole and [60,70) remains an explicit guide gap instead of fabricated
	// programme metadata. A complete lower-priority row filling [20,40) wins.
	reversed := slices.Clone(input)
	slices.Reverse(reversed)
	gotReversed := canonicalIDs(selectCanonicalEPGCandidates(reversed))
	if !slices.Equal(gotReversed, want) {
		t.Fatalf("reordered input selected IDs = %v, want deterministic %v", gotReversed, want)
	}
}

func TestSelectCanonicalEPGCandidates_TiePrefersMorePrograms(t *testing.T) {
	base := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)
	whole := epgCandidate("00000000-0000-0000-0000-000000000021", base, 0, 60, 0, "1:primary", "whole")
	first := epgCandidate("00000000-0000-0000-0000-000000000022", base, 0, 30, 0, "1:primary", "first")
	second := epgCandidate("00000000-0000-0000-0000-000000000023", base, 30, 60, 0, "1:primary", "second")

	got := canonicalIDs(selectCanonicalEPGCandidates([]canonicalEPGCandidate{whole, first, second}))
	want := []uuid.UUID{first.ID, second.ID}
	if !slices.Equal(got, want) {
		t.Fatalf("selected IDs = %v, want two-program tie-break %v", got, want)
	}
}

func TestSelectCanonicalEPGCandidates_MaximizesCoverageInsidePreferredGaps(t *testing.T) {
	base := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)
	preferred := epgCandidate("00000000-0000-0000-0000-000000000031", base, 0, 20, 0, "1:primary", "preferred")
	malformed := epgCandidate("00000000-0000-0000-0000-000000000032", base, 10, 100, 10, "1:fallback", "malformed-long")
	gapA := epgCandidate("00000000-0000-0000-0000-000000000033", base, 20, 60, 10, "1:fallback", "gap-a")
	gapB := epgCandidate("00000000-0000-0000-0000-000000000034", base, 60, 100, 10, "1:fallback", "gap-b")

	// The malformed fallback is longer than the two valid rows considered
	// alone, but it cannot be admitted whole because it overlaps the preferred
	// row. It must be removed before weighted selection so it cannot punch an
	// 80-minute hole by crowding valid fallback rows out of that source's set.
	got := canonicalIDs(selectCanonicalEPGCandidates([]canonicalEPGCandidate{gapB, malformed, preferred, gapA}))
	want := []uuid.UUID{preferred.ID, gapA.ID, gapB.ID}
	if !slices.Equal(got, want) {
		t.Fatalf("selected IDs = %v, want preferred row plus complete fallback gap %v", got, want)
	}
}

func epgCandidate(id string, base time.Time, startMinute, endMinute, priority int, sourceKey, hash string) canonicalEPGCandidate {
	return canonicalEPGCandidate{
		ID:         uuid.MustParse(id),
		Start:      base.Add(time.Duration(startMinute) * time.Minute),
		End:        base.Add(time.Duration(endMinute) * time.Minute),
		Priority:   priority,
		SourceKey:  sourceKey,
		SourceHash: hash,
	}
}

func canonicalIDs(candidates []canonicalEPGCandidate) []uuid.UUID {
	ids := make([]uuid.UUID, 0, len(candidates))
	for _, candidate := range candidates {
		ids = append(ids, candidate.ID)
	}
	return ids
}
