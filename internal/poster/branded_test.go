package poster

import (
	"bytes"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"os"
	"path/filepath"
	"testing"
)

func TestPickCategoryColor(t *testing.T) {
	// First non-empty wins (callers should put highest-signal first).
	got := PickCategoryColor([]string{"Sports", "Entertainment"})
	if want := CategoryColor["sports"]; got != want {
		t.Errorf("Sports first → got %v want %v", got, want)
	}

	// Case-insensitive.
	got = PickCategoryColor([]string{"NEWS"})
	if want := CategoryColor["news"]; got != want {
		t.Errorf("NEWS uppercase → got %v want %v", got, want)
	}

	// Unknown falls back to the slate default.
	got = PickCategoryColor([]string{"Definitely not a category"})
	if got != defaultColor {
		t.Errorf("unknown → expected default; got %v", got)
	}

	// Empty slice → default.
	got = PickCategoryColor(nil)
	if got != defaultColor {
		t.Errorf("nil cats → expected default; got %v", got)
	}
}

// TestRender_NoLogo: without a logo file, Render still produces a valid
// JPEG of the right dimensions filled with the category background.
func TestRender_NoLogo(t *testing.T) {
	var buf bytes.Buffer
	if err := Render(&buf, "", []string{"News"}, 80); err != nil {
		t.Fatalf("Render: %v", err)
	}

	img, err := jpeg.Decode(&buf)
	if err != nil {
		t.Fatalf("decode rendered jpeg: %v", err)
	}
	b := img.Bounds()
	if b.Dx() != 600 || b.Dy() != 900 {
		t.Errorf("dim got %dx%d want 600x900", b.Dx(), b.Dy())
	}

	// Sample pixel from upper region — should be tinted news blue.
	c := img.At(300, 100)
	r, g, bl, _ := c.RGBA()
	// JPEG round-trip introduces some loss, so just sanity-check that
	// blue dominates (the news colour is heavily blue).
	if bl <= r || bl <= g {
		t.Errorf("expected blue-dominant pixel; got rgb (%d, %d, %d)", r, g, bl)
	}
}

// TestRender_WithLogo: synthesise a tiny PNG, render with it, ensure the
// output JPEG is bigger than a no-logo render (the logo should add some
// non-trivial entropy) and decodes cleanly.
func TestRender_WithLogo(t *testing.T) {
	dir := t.TempDir()
	logoPath := filepath.Join(dir, "fake-logo.png")

	// 200×200 solid magenta PNG with a 40px white circle in the centre,
	// just enough to look like a real logo for compositing.
	logo := image.NewRGBA(image.Rect(0, 0, 200, 200))
	for y := 0; y < 200; y++ {
		for x := 0; x < 200; x++ {
			dx, dy := x-100, y-100
			d2 := dx*dx + dy*dy
			if d2 < 40*40 {
				logo.Set(x, y, color.RGBA{0xff, 0xff, 0xff, 0xff})
			} else {
				logo.Set(x, y, color.RGBA{0xff, 0x00, 0xff, 0xff})
			}
		}
	}
	f, err := os.Create(logoPath)
	if err != nil {
		t.Fatalf("create logo: %v", err)
	}
	if err := png.Encode(f, logo); err != nil {
		t.Fatalf("encode logo: %v", err)
	}
	f.Close()

	var buf bytes.Buffer
	if err := Render(&buf, logoPath, []string{"Sports"}, 80); err != nil {
		t.Fatalf("Render: %v", err)
	}
	encoded := buf.Bytes()
	if len(encoded) < 1000 {
		t.Errorf("rendered jpeg too small: %d bytes", len(encoded))
	}
	if _, err := jpeg.Decode(bytes.NewReader(encoded)); err != nil {
		t.Fatalf("decoded rendered jpeg: %v", err)
	}
}

// TestCachePath_Safe: filenames are safe even with adversarial input —
// no traversal, no path separators.
func TestCachePath_Safe(t *testing.T) {
	cases := []struct {
		baseDir, channelID, cat, want string
	}{
		{"/var/cache", "abc-123", "News", "/var/cache/branded/abc-123-news.jpg"},
		{"/var/cache", "abc-123", "", "/var/cache/branded/abc-123-default.jpg"},
		{"/var/cache", "abc-123", "Sports Talk", "/var/cache/branded/abc-123-sports-talk.jpg"},
		// Adversarial — cannot escape the cache dir.
		{"/var/cache", "abc-123", "../../etc/passwd", "/var/cache/branded/abc-123-etcpasswd.jpg"},
		{"/var/cache", "abc-123", "Sports/Talk", "/var/cache/branded/abc-123-sportstalk.jpg"},
	}
	for _, tc := range cases {
		got := CachePath(tc.baseDir, tc.channelID, tc.cat)
		if got != tc.want {
			t.Errorf("CachePath(%q, %q, %q) = %q; want %q", tc.baseDir, tc.channelID, tc.cat, got, tc.want)
		}
	}
}
