package epg

import (
	"testing"
	"time"
)

func TestParseXMLTVTimePreservesProviderCivilOffset(t *testing.T) {
	got, err := parseXMLTVTime("20260822003000 -0400")
	if err != nil {
		t.Fatal(err)
	}
	if _, offset := got.Zone(); offset != -4*60*60 {
		t.Fatalf("offset = %d, want EDT", offset)
	}
	if year, month, day := got.Date(); year != 2026 || month != time.August || day != 22 {
		t.Fatalf("civil date = %04d-%02d-%02d, want 2026-08-22", year, month, day)
	}
	if !got.UTC().Equal(time.Date(2026, 8, 22, 4, 30, 0, 0, time.UTC)) {
		t.Fatalf("instant = %s", got.UTC())
	}
}

func TestProgrammePosterURLPrecedenceAndSafety(t *testing.T) {
	tests := []struct {
		name string
		in   xmltvProgramme
		want string
	}{
		{
			name: "explicit poster wins",
			in: xmltvProgramme{
				Images: []xmltvImage{
					{Type: "backdrop", Orient: "L", URL: "https://img.example/backdrop.jpg"},
					{Type: "poster", URL: "https://img.example/poster.jpg"},
				},
				Icon: xmltvIcon{Src: "https://img.example/icon.jpg"},
			},
			want: "https://img.example/poster.jpg",
		},
		{
			name: "non-landscape image before legacy icon",
			in: xmltvProgramme{
				Images: []xmltvImage{
					{Orient: "landscape", URL: "https://img.example/wide.jpg"},
					{Orient: "P", URL: "http://img.example/tall.jpg"},
				},
				Icon: xmltvIcon{Src: "https://img.example/icon.jpg"},
			},
			want: "http://img.example/tall.jpg",
		},
		{
			name: "legacy icon fallback",
			in:   xmltvProgramme{Icon: xmltvIcon{Src: "https://img.example/icon.jpg"}},
			want: "https://img.example/icon.jpg",
		},
		{
			name: "unsafe scheme rejected",
			in: xmltvProgramme{
				Images: []xmltvImage{{Type: "poster", URL: "file:///etc/passwd"}},
				Icon:   xmltvIcon{Src: "javascript:alert(1)"},
			},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := programmePosterURL(tt.in); got != tt.want {
				t.Fatalf("programmePosterURL() = %q, want %q", got, tt.want)
			}
		})
	}
}
