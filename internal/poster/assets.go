package poster

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"image"
	"image/jpeg"
	_ "image/png"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

const (
	maxPosterBytes  = 20 << 20
	maxPosterPixels = int64(40_000_000)
	maxPosterSide   = 10_000
)

type StoredAsset struct {
	LocalURL     string
	RelativePath string
	SHA256       string
	Width        int
	Height       int
}

// AssetStore owns immutable, content-addressed artwork under the existing
// persistent poster cache directory.
type AssetStore struct {
	Root string
}

func NewAssetStore(root string) *AssetStore { return &AssetStore{Root: root} }

func (s *AssetStore) Exists(relativePath string) bool {
	rel := filepath.ToSlash(relativePath)
	if s == nil || s.Root == "" || !strings.HasPrefix(rel, "assets/") || strings.Contains(rel, "..") {
		return false
	}
	assetRel := strings.TrimPrefix(rel, "assets/")
	if !assetPathRE.MatchString(assetRel) {
		return false
	}
	full := filepath.Join(s.Root, filepath.FromSlash(relativePath))
	b, err := os.ReadFile(full)
	if err != nil || len(b) == 0 || len(b) > maxPosterBytes {
		return false
	}
	want := strings.TrimSuffix(filepath.Base(rel), filepath.Ext(rel))
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]) == want
}

func LocalAssetURL(relativePath string) string {
	rel := filepath.ToSlash(relativePath)
	if !strings.HasPrefix(rel, "assets/") || strings.Contains(rel, "..") {
		return ""
	}
	return "/posters/" + rel
}

func RelativePathFromLocalURL(localURL string) string {
	const prefix = "/posters/assets/"
	if !strings.HasPrefix(localURL, prefix) {
		return ""
	}
	rel := "assets/" + strings.TrimPrefix(localURL, prefix)
	if strings.Contains(rel, "..") || !assetPathRE.MatchString(strings.TrimPrefix(rel, "assets/")) {
		return ""
	}
	return rel
}

func (s *AssetStore) PutGeneratedPNG(data []byte) (StoredAsset, error) {
	width, height, err := ValidateGeneratedPNG(data)
	if err != nil {
		return StoredAsset{}, err
	}
	return s.put("generated", "png", data, width, height)
}

// ValidateGeneratedPNG fully decodes gateway output after bounding its encoded
// size and declared dimensions. DecodeConfig alone accepts truncated files and
// would let an unusable remote result remain attached to a persisted job id.
func ValidateGeneratedPNG(data []byte) (width, height int, err error) {
	if len(data) == 0 || len(data) > maxPosterBytes {
		return 0, 0, fmt.Errorf("generated poster size %d is outside limits", len(data))
	}
	cfg, format, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil || format != "png" {
		return 0, 0, fmt.Errorf("generated poster is not a valid PNG")
	}
	if err := validatePortrait(cfg.Width, cfg.Height, true); err != nil {
		return 0, 0, err
	}
	if _, format, err = image.Decode(bytes.NewReader(data)); err != nil || format != "png" {
		return 0, 0, fmt.Errorf("generated poster is not a complete PNG")
	}
	return cfg.Width, cfg.Height, nil
}

func (s *AssetStore) PutResearchedImage(data []byte) (StoredAsset, error) {
	if len(data) == 0 || len(data) > maxPosterBytes {
		return StoredAsset{}, fmt.Errorf("researched poster size %d is outside limits", len(data))
	}
	cfg, _, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil {
		return StoredAsset{}, fmt.Errorf("decode researched poster config: %w", err)
	}
	if err := validatePortrait(cfg.Width, cfg.Height, false); err != nil {
		return StoredAsset{}, err
	}
	// Decode only after dimensions/pixel count are bounded. Compressed image
	// size alone does not protect the process from decompression bombs.
	img, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return StoredAsset{}, fmt.Errorf("decode researched poster: %w", err)
	}
	b := img.Bounds()

	// Normalize researched art to a safe JPEG. This strips metadata and makes
	// the public serving path independent of whatever the remote host claimed.
	var out bytes.Buffer
	if err := jpeg.Encode(&out, img, &jpeg.Options{Quality: 90}); err != nil {
		return StoredAsset{}, fmt.Errorf("encode researched poster: %w", err)
	}
	if out.Len() > maxPosterBytes {
		return StoredAsset{}, fmt.Errorf("normalized researched poster exceeds %d bytes", maxPosterBytes)
	}
	return s.put("researched", "jpg", out.Bytes(), b.Dx(), b.Dy())
}

func validatePortrait(width, height int, exactTwoByThree bool) error {
	if width < 200 || height < 300 || height <= width {
		return fmt.Errorf("poster dimensions must be portrait and at least 200x300, got %dx%d", width, height)
	}
	if width > maxPosterSide || height > maxPosterSide || int64(width)*int64(height) > maxPosterPixels {
		return fmt.Errorf("poster dimensions exceed safe decode limits, got %dx%d", width, height)
	}
	if exactTwoByThree && width*3 != height*2 {
		return fmt.Errorf("generated poster must be 2:3, got %dx%d", width, height)
	}
	return nil
}

func (s *AssetStore) put(kind, ext string, data []byte, width, height int) (StoredAsset, error) {
	if s == nil || s.Root == "" {
		return StoredAsset{}, fmt.Errorf("poster asset store is not configured")
	}
	sum := sha256.Sum256(data)
	digest := hex.EncodeToString(sum[:])
	rel := filepath.Join("assets", kind, "sha256", digest[:2], digest+"."+ext)
	full := filepath.Join(s.Root, rel)
	if s.Exists(filepath.ToSlash(rel)) {
		return storedAsset(kind, ext, rel, digest, width, height), nil
	}
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		return StoredAsset{}, fmt.Errorf("create poster asset dir: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(full), ".poster-*.tmp")
	if err != nil {
		return StoredAsset{}, fmt.Errorf("create poster temp: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return StoredAsset{}, fmt.Errorf("write poster temp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return StoredAsset{}, fmt.Errorf("sync poster temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return StoredAsset{}, fmt.Errorf("close poster temp: %w", err)
	}
	if err := os.Rename(tmpName, full); err != nil {
		return StoredAsset{}, fmt.Errorf("publish poster asset: %w", err)
	}
	return storedAsset(kind, ext, rel, digest, width, height), nil
}

func storedAsset(kind, ext, rel, digest string, width, height int) StoredAsset {
	return StoredAsset{
		LocalURL:     "/posters/assets/" + kind + "/sha256/" + digest[:2] + "/" + digest + "." + ext,
		RelativePath: filepath.ToSlash(rel),
		SHA256:       digest,
		Width:        width,
		Height:       height,
	}
}

var assetPathRE = regexp.MustCompile(`^(generated|researched)/sha256/[0-9a-f]{2}/[0-9a-f]{64}\.(png|jpg)$`)

type AssetServer struct {
	Root string
}

func NewAssetServer(root string) *AssetServer { return &AssetServer{Root: root} }

func (s *AssetServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rel := r.PathValue("asset_path")
	if s == nil || s.Root == "" || !assetPathRE.MatchString(rel) {
		http.NotFound(w, r)
		return
	}
	full := filepath.Join(s.Root, "assets", filepath.FromSlash(rel))
	st, err := os.Stat(full)
	if err != nil || !st.Mode().IsRegular() {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	http.ServeFile(w, r, full)
}
