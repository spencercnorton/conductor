package reconcile

import (
	"strings"
	"time"
)

// yearMatches reports whether a movie airing plausibly belongs to the
// given release year: either its original_air_date falls in that year, or
// the year appears parenthesized in the title (the EPG's common shape).
func yearMatches(p Program, year int) bool {
	if p.OriginalAirDate != nil && p.OriginalAirDate.UTC().Year() == year {
		return true
	}
	return strings.Contains(p.Title, "("+itoaYear(year)+")")
}

// sameDay compares two instants by calendar day in UTC.
func sameDay(a, b time.Time) bool {
	a, b = a.UTC(), b.UTC()
	ay, am, ad := a.Date()
	by, bm, bd := b.Date()
	return ay == by && am == bm && ad == bd
}

func itoaYear(y int) string {
	// Years are always 4 positive digits in practice; keep it allocation-light.
	buf := [4]byte{}
	for i := 3; i >= 0; i-- {
		buf[i] = byte('0' + y%10)
		y /= 10
	}
	return string(buf[:])
}
