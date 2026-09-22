package reconcile

import "testing"

func TestEligible_GapAutoRecordAdmitsTheGapClass(t *testing.T) {
	gap := Match{Source: "plex-gap", Confidence: confPlexGap}
	wanted := Match{Confidence: 0.9}
	low := Match{Confidence: 0.6}

	off := &Worker{Threshold: 0.80}
	if off.eligible(gap) {
		t.Fatal("gap match must not record with GapAutoRecord off")
	}
	on := &Worker{Threshold: 0.80, GapAutoRecord: true}
	if !on.eligible(gap) {
		t.Fatal("enabling GapAutoRecord must admit the confPlexGap class (D3: the old Threshold gate made the toggle unreachable)")
	}
	if !on.eligible(wanted) || on.eligible(low) {
		t.Fatal("non-gap matches must keep the global threshold")
	}
	weak := Match{Source: "plex-gap", Confidence: confPlexGap - 0.05}
	if on.eligible(weak) {
		t.Fatal("sub-confPlexGap gap matches stay deferred even when opted in")
	}
}
