package poster

import (
	"bytes"
	"image"
	"image/color"
	"image/png"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testPNG(t *testing.T, width, height int) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, width, height))
	img.Set(0, 0, color.RGBA{R: 1, G: 2, B: 3, A: 255})
	var b bytes.Buffer
	if err := png.Encode(&b, img); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func TestAssetStoreGeneratedAndServe(t *testing.T) {
	root := t.TempDir()
	store := NewAssetStore(root)
	first, err := store.PutGeneratedPNG(testPNG(t, 200, 300))
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.PutGeneratedPNG(testPNG(t, 200, 300))
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("content-addressed write was not stable: %#v != %#v", first, second)
	}
	if _, err := os.Stat(filepath.Join(root, first.RelativePath)); err != nil {
		t.Fatalf("stored poster missing: %v", err)
	}
	if got := RelativePathFromLocalURL(first.LocalURL); got != first.RelativePath {
		t.Fatalf("local URL round-trip = %q, want %q", got, first.RelativePath)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /posters/assets/{asset_path...}", NewAssetServer(root).ServeHTTP)
	r := httptest.NewRequest(http.MethodGet, first.LocalURL, nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusOK || !strings.Contains(w.Header().Get("Cache-Control"), "immutable") {
		t.Fatalf("asset serve = %d, cache=%q", w.Code, w.Header().Get("Cache-Control"))
	}
}

func TestAssetStoreExistsDetectsCorruption(t *testing.T) {
	root := t.TempDir()
	store := NewAssetStore(root)
	asset, err := store.PutGeneratedPNG(testPNG(t, 200, 300))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, asset.RelativePath)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)-1] ^= 0xff
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
	if store.Exists(asset.RelativePath) {
		t.Fatal("content-addressed asset corruption was not detected")
	}
}

func TestAssetStoreRejectsWrongGeneratedAspect(t *testing.T) {
	_, err := NewAssetStore(t.TempDir()).PutGeneratedPNG(testPNG(t, 300, 300))
	if err == nil || !strings.Contains(err.Error(), "portrait") {
		t.Fatalf("expected portrait validation error, got %v", err)
	}
}

func TestValidateGeneratedPNGRejectsTruncatedBody(t *testing.T) {
	b := testPNG(t, 200, 300)
	_, _, err := ValidateGeneratedPNG(b[:len(b)-8])
	if err == nil || !strings.Contains(err.Error(), "complete PNG") {
		t.Fatalf("expected full-decode failure, got %v", err)
	}
}

func TestPosterDimensionsRejectDecompressionBomb(t *testing.T) {
	if err := validatePortrait(10_000, 20_000, false); err == nil || !strings.Contains(err.Error(), "safe decode") {
		t.Fatalf("expected oversized image rejection, got %v", err)
	}
}

func TestAssetServerRejectsTraversal(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /posters/assets/{asset_path...}", NewAssetServer(t.TempDir()).ServeHTTP)
	r := httptest.NewRequest(http.MethodGet, "/posters/assets/generated/sha256/aa/not-a-digest.png", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusNotFound {
		t.Fatalf("invalid asset path status = %d", w.Code)
	}
}

func TestAssetStoreExistsRejectsTraversal(t *testing.T) {
	if NewAssetStore(t.TempDir()).Exists("assets/../secret") {
		t.Fatal("Exists accepted traversal path")
	}
}
