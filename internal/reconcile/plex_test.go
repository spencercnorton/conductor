package reconcile

import "testing"

func ownedWith(seriesKey, epTitle string, s, e int) *OwnedIndex {
	o := newOwnedIndex()
	o.add(seriesKey, epTitle, s, e)
	return o
}

func TestMatchPlexGaps_OwnedSeriesMissingEpisodeIsGap(t *testing.T) {
	// Own The Bear S03E07 "Funeral"; S03E08 "Forever" is airing and missing.
	owned := ownedWith(titleKey("The Bear"), "Funeral", 3, 7)
	airing := prog("The Bear", "Forever", false)
	airing.EpisodeNumOnscreen = "S03E08"
	got := MatchPlexGaps([]Program{airing}, owned, nil)
	if len(got) != 1 || got[0].Source != "plex-gap" || got[0].Reason != "plex-owned-gap" {
		t.Fatalf("expected one plex-gap match, got %+v", got)
	}
	if got[0].Season != 3 || got[0].Episode != 8 {
		t.Errorf("season/episode parse: %+v", got[0])
	}
}

func TestMatchPlexGaps_AlreadyOwnedEpisodeSkipped(t *testing.T) {
	owned := ownedWith(titleKey("The Bear"), "Funeral", 3, 7)
	airing := prog("The Bear", "Funeral", false) // same episode we own
	airing.EpisodeNumOnscreen = "S03E07"
	if got := MatchPlexGaps([]Program{airing}, owned, nil); len(got) != 0 {
		t.Fatalf("owned episode must not be a gap, got %+v", got)
	}
}

func TestMatchPlexGaps_UnownedSeriesIgnored(t *testing.T) {
	owned := ownedWith(titleKey("The Bear"), "Funeral", 3, 7)
	airing := prog("Some Show I Dont Own", "Pilot", false)
	if got := MatchPlexGaps([]Program{airing}, owned, nil); len(got) != 0 {
		t.Fatalf("series not in library must be ignored, got %+v", got)
	}
}

func TestMatchPlexGaps_ExcludesSonarrCoveredSeries(t *testing.T) {
	owned := ownedWith(titleKey("The Bear"), "Funeral", 3, 7)
	airing := prog("The Bear", "Forever", false)
	airing.EpisodeNumOnscreen = "S03E08" // real identity; dropped only by exclude
	exclude := map[string]bool{titleKey("The Bear"): true}
	if got := MatchPlexGaps([]Program{airing}, owned, exclude); len(got) != 0 {
		t.Fatalf("series on the Sonarr wanted path must be excluded, got %+v", got)
	}
}

func TestMatchPlexGaps_CollapsesDuplicateAirings(t *testing.T) {
	owned := ownedWith(titleKey("The Bear"), "Funeral", 3, 7)
	a := prog("The Bear", "Forever", false)
	a.EpisodeNumOnscreen = "S03E08"
	b := prog("The Bear", "Forever", false) // same missing ep, later airing
	b.EpisodeNumOnscreen = "S03E08"
	got := MatchPlexGaps([]Program{a, b}, owned, nil)
	if len(got) != 1 {
		t.Fatalf("duplicate airings of one missing episode should collapse, got %d", len(got))
	}
}

func TestMatchPlexGaps_NoEpisodeNumberSkipped(t *testing.T) {
	// Syndicated rerun: owned series, but the EPG carries no onscreen S/E. We
	// can't name the episode, so it can't be reliably deduped, ownership-checked,
	// or recorded for import — skip it. Without this guard, daily reruns flood
	// the gap list (e.g. 141 "South Park" airings/cycle).
	owned := ownedWith(titleKey("South Park"), "Cartman Gets an Anal Probe", 1, 1)
	a := prog("South Park", "", false) // no subtitle, no onscreen S/E
	b := prog("South Park", "", false) // another airing, same lack of identity
	if got := MatchPlexGaps([]Program{a, b}, owned, nil); len(got) != 0 {
		t.Fatalf("airings without an episode number must be skipped, got %+v", got)
	}
}

func TestMatchPlexGaps_FranchiseSubtitleWithoutNumberSkipped(t *testing.T) {
	// The real-world false positive: the EPG sub_title is a FRANCHISE qualifier,
	// not an episode title, and there's no onscreen S/E. "Law & Order" airing
	// with sub_title "Special Victims Unit" must NOT be treated as a missing
	// episode just because that string isn't an owned episode title.
	owned := ownedWith(titleKey("Law & Order"), "Subterranean Homeboy Blues", 1, 1)
	airing := prog("Law & Order", "Special Victims Unit", false) // franchise name, no S/E
	if got := MatchPlexGaps([]Program{airing}, owned, nil); len(got) != 0 {
		t.Fatalf("franchise-name subtitle without an episode number must be skipped, got %+v", got)
	}
}

func TestMatchPlexGaps_OwnedByEpisodeTitleSkippedDespiteNumber(t *testing.T) {
	// Subtitle still works as a secondary ownership signal: if we own the
	// episode by title, a fresh-looking onscreen S/E shouldn't resurrect it.
	owned := ownedWith(titleKey("The Bear"), "Forever", 3, 8)
	airing := prog("The Bear", "Forever", false)
	airing.EpisodeNumOnscreen = "S03E08"
	if got := MatchPlexGaps([]Program{airing}, owned, nil); len(got) != 0 {
		t.Fatalf("episode owned by title must not be a gap, got %+v", got)
	}
}
