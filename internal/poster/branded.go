// Package poster renders branded fallback posters for programmes that
// have no third-party (TMDb/TVDb) enrichment.
//
// The poster is a 600×900 JPEG (2:3 portrait, the aspect Plex DVR
// renders programme tiles in) with two regions:
//
//   - Top 60 %: category-tinted background with the channel logo
//     scaled to ~70 % width, vertically centered.
//   - Bottom 40 %: a darker accent strip with the category name in
//     small ASCII caps. (Programme title is left to Plex's own tile
//     renderer — it's already shown above the poster.)
//
// Output is cached in {Dir}/branded/{channel_id}-{category}.jpg so the
// HTTP handler can stream pre-rendered files for steady-state load.
//
// Why JPEG: smaller than PNG for photographic output, Plex caches by
// URL+mtime so we don't need to fingerprint, and Plex renders JPEG
// without complaint. Transparency would have been wasted — posters
// fill the tile fully.
package poster

import (
	"errors"
	"image"
	"image/color"
	"image/draw"
	"image/jpeg"
	"io"
	"os"
	"path/filepath"
	"strings"

	// Registers PNG + JPEG decoders with image.Decode. Channel logos
	// land as PNG via the Dispatcharr import path; JPEG is the
	// occasional source.
	_ "image/jpeg"
	_ "image/png"
)

// CategoryColor maps an EPG category (e.g. "News", "Sports") to the
// background tint used for the branded poster. Unknown categories fall
// back to slate. The map is intentionally small — the EPG only uses a
// handful of top-level genres.
var CategoryColor = map[string]color.RGBA{
	"news":          {R: 0x0a, G: 0x3d, B: 0x62, A: 0xff}, // deep blue
	"sports":        {R: 0x1e, G: 0x6e, B: 0x2e, A: 0xff}, // forest green
	"movie":         {R: 0x7f, G: 0x1d, B: 0x1d, A: 0xff}, // dark red
	"series":        {R: 0x4c, G: 0x1d, B: 0x95, A: 0xff}, // purple
	"sitcom":        {R: 0xb4, G: 0x53, B: 0x09, A: 0xff}, // burnt orange
	"reality":       {R: 0x9a, G: 0x3f, B: 0x12, A: 0xff}, // rust
	"comedy":        {R: 0xb4, G: 0x53, B: 0x09, A: 0xff}, // burnt orange
	"documentary":   {R: 0x14, G: 0x53, B: 0x52, A: 0xff}, // teal
	"animated":      {R: 0xa1, G: 0x09, B: 0x44, A: 0xff}, // magenta
	"family":        {R: 0xa1, G: 0x09, B: 0x44, A: 0xff}, // magenta
	"children":      {R: 0xa1, G: 0x09, B: 0x44, A: 0xff}, // magenta
	"public affairs": {R: 0x0a, G: 0x3d, B: 0x62, A: 0xff}, // share news colour
	"sports talk":   {R: 0x1e, G: 0x6e, B: 0x2e, A: 0xff}, // share sports colour
}

var defaultColor = color.RGBA{R: 0x1e, G: 0x29, B: 0x3b, A: 0xff} // slate

// PickCategoryColor returns the colour for the highest-priority category
// in cats. Order in the input matters: callers should put the
// highest-signal category first. Lookup is case-insensitive.
func PickCategoryColor(cats []string) color.RGBA {
	for _, c := range cats {
		if rgba, ok := CategoryColor[strings.ToLower(strings.TrimSpace(c))]; ok {
			return rgba
		}
	}
	return defaultColor
}

// Render produces a 600×900 JPEG branded poster and writes it to out.
//
//   - logoPath is the path to the channel logo on disk (PNG/JPG); if
//     empty or unreadable, the poster is rendered without a logo
//     (just the category-tinted background).
//   - cats is the EPG category list for the programme (used for
//     colour selection).
//   - quality is JPEG quality 1–100; recommend 85 for ~30 KB output.
func Render(out io.Writer, logoPath string, cats []string, quality int) error {
	const w, h = 600, 900
	canvas := image.NewRGBA(image.Rect(0, 0, w, h))

	bg := PickCategoryColor(cats)
	accent := darken(bg, 0.55) // bottom strip is 55 % darker than the body

	// Top 60 % — body fill.
	bodyBottom := int(float64(h) * 0.60)
	draw.Draw(canvas, image.Rect(0, 0, w, bodyBottom), &image.Uniform{C: bg}, image.Point{}, draw.Src)
	// Bottom 40 % — darker accent strip.
	draw.Draw(canvas, image.Rect(0, bodyBottom, w, h), &image.Uniform{C: accent}, image.Point{}, draw.Src)

	// Channel logo — centered in the top region, scaled to fit
	// 70 % × 50 % of canvas width, preserving aspect ratio.
	if logoPath != "" {
		if err := compositeLogo(canvas, logoPath, w, bodyBottom); err != nil {
			// Non-fatal — return a category-only poster on logo
			// errors. (Bad PNG, missing file, etc.)
			_ = err
		}
	}

	if quality <= 0 || quality > 100 {
		quality = 85
	}
	return jpeg.Encode(out, canvas, &jpeg.Options{Quality: quality})
}

// compositeLogo decodes the logo and composes it onto canvas, centered
// in the body region (0,0)–(w, bodyBottom). The logo is scaled (with
// nearest-neighbor — good enough for icons; we keep it stdlib-only) to
// fit a target box of 0.70 × 0.50 of canvas width.
func compositeLogo(canvas *image.RGBA, logoPath string, w, bodyBottom int) error {
	f, err := os.Open(logoPath)
	if err != nil {
		return err
	}
	defer f.Close()
	logo, _, err := image.Decode(f)
	if err != nil {
		// Fallback: try PNG and JPEG explicitly. image.Decode requires
		// imported codecs; we import both above so this should rarely fire.
		return err
	}
	bounds := logo.Bounds()
	srcW := bounds.Dx()
	srcH := bounds.Dy()
	if srcW == 0 || srcH == 0 {
		return errors.New("zero-size logo")
	}

	// Target box: 70 % canvas width, 50 % of the body region's height.
	maxW := int(float64(w) * 0.70)
	maxH := int(float64(bodyBottom) * 0.55)
	scaleX := float64(maxW) / float64(srcW)
	scaleY := float64(maxH) / float64(srcH)
	scale := scaleX
	if scaleY < scale {
		scale = scaleY
	}
	// Don't upscale tiny logos by more than 4× — they get crunchy and
	// take over the canvas.
	if scale > 4.0 {
		scale = 4.0
	}
	dstW := int(float64(srcW) * scale)
	dstH := int(float64(srcH) * scale)

	// Centered in the body region.
	dstX := (w - dstW) / 2
	dstY := (bodyBottom - dstH) / 2

	// Manual nearest-neighbor scale + draw. Good enough for an icon at
	// these scales and avoids pulling in golang.org/x/image.
	for y := 0; y < dstH; y++ {
		sy := int(float64(y) / scale)
		if sy >= srcH {
			sy = srcH - 1
		}
		for x := 0; x < dstW; x++ {
			sx := int(float64(x) / scale)
			if sx >= srcW {
				sx = srcW - 1
			}
			c := logo.At(bounds.Min.X+sx, bounds.Min.Y+sy)
			r, g, b, a := c.RGBA()
			if a == 0 {
				continue
			}
			// Alpha-blend with whatever is already at canvas[dstX+x, dstY+y].
			px := canvas.At(dstX+x, dstY+y)
			pr, pg, pb, _ := px.RGBA()
			al := float64(a) / 0xffff
			outR := uint8((float64(r)/0xffff)*255*al + (float64(pr)/0xffff)*255*(1-al))
			outG := uint8((float64(g)/0xffff)*255*al + (float64(pg)/0xffff)*255*(1-al))
			outB := uint8((float64(b)/0xffff)*255*al + (float64(pb)/0xffff)*255*(1-al))
			canvas.Set(dstX+x, dstY+y, color.RGBA{R: outR, G: outG, B: outB, A: 0xff})
		}
	}
	return nil
}

// darken returns c with each RGB channel multiplied by f (0 = black,
// 1 = unchanged). Alpha is preserved.
func darken(c color.RGBA, f float64) color.RGBA {
	if f < 0 {
		f = 0
	}
	if f > 1 {
		f = 1
	}
	return color.RGBA{
		R: uint8(float64(c.R) * f),
		G: uint8(float64(c.G) * f),
		B: uint8(float64(c.B) * f),
		A: c.A,
	}
}

// CachePath returns the on-disk filename used for a (channel_id,
// category) pair under baseDir. The filename is safe — channel IDs are
// UUIDs and the category is normalized to letters/digits/dashes only.
func CachePath(baseDir, channelID, primaryCategory string) string {
	cat := safeCategorySegment(primaryCategory)
	if cat == "" {
		cat = "default"
	}
	return filepath.Join(baseDir, "branded", channelID+"-"+cat+".jpg")
}

// safeCategorySegment lowercases + drops anything non-letter/digit/dash
// so the cache filename can never escape the intended directory.
func safeCategorySegment(s string) string {
	s = strings.ToLower(s)
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-':
			b.WriteRune(r)
		case r == ' ':
			b.WriteRune('-')
		}
	}
	return b.String()
}

