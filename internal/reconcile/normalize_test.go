package reconcile

import "testing"

func TestTitleKeyCollapsesVariants(t *testing.T) {
	cases := []struct{ a, b string }{
		{"Marvel's Daredevil", "Daredevil"},
		{"Daredevil (2015)", "daredevil"},
		{"Temptation Island US", "Temptation Island"},
		{"The Office", "Office"},
		{"It's Always Sunny", "Its Always Sunny"},
		{"Tom & Jerry", "Tom and Jerry"},
	}
	for _, c := range cases {
		if titleKey(c.a) != titleKey(c.b) {
			t.Errorf("titleKey(%q)=%q != titleKey(%q)=%q",
				c.a, titleKey(c.a), c.b, titleKey(c.b))
		}
	}
}

func TestTitleKeyKeepsDistinctTitlesDistinct(t *testing.T) {
	if titleKey("The Bear") == titleKey("Bear Grylls") {
		t.Error("distinct titles collapsed to the same key")
	}
}

func TestCanonicalIMDb(t *testing.T) {
	for _, in := range []string{"tt0848228", "0848228", "848228", "TT0848228"} {
		if got := canonicalIMDb(in); got != "848228" {
			t.Errorf("canonicalIMDb(%q)=%q want 848228", in, got)
		}
	}
	if canonicalIMDb("") != "" {
		t.Error("empty imdb should stay empty")
	}
}
