// Server: GET /posters/branded/{channel_id}.jpg
//
// First request renders the branded poster (logo composited on a
// category-tinted background) and caches the result on disk. Subsequent
// requests serve from the cache.
//
// The cache key is (channel_id, primary category). The category comes
// from the query string `?cat=<name>` so callers can request the right
// background colour for a given programme genre. Calls without ?cat
// fall back to the channel's "default" colour.
//
// Logo discovery: looks up the channel's logo_url, then re-resolves the
// last URL segment to a local file under the configured logos
// directory. If the channel has no logo or the URL points outside our
// logos dir, the poster renders without a logo (just the tinted
// background — still useful as a placeholder).
package poster

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
)

// ChannelLookup is the contract Server uses to resolve a channel ID to
// its logo URL. Implemented by the store package.
type ChannelLookup interface {
	GetChannelLogoURL(ctx context.Context, channelID uuid.UUID) (string, error)
}

// Server handles GET /posters/branded/{channel_id}.jpg.
type Server struct {
	// CacheDir is where rendered JPEGs are stored. The rendered files
	// land at CacheDir/branded/{channel_id}-{category}.jpg. Empty
	// CacheDir disables caching (every request renders fresh — useful
	// in tests, wasteful in prod).
	CacheDir string

	// LogosDir is the directory the logo HTTP handler serves from. We
	// re-resolve channel logo URLs to filenames under this directory
	// rather than fetching them over HTTP — every Conductor that
	// serves channels has the logos locally already.
	LogosDir string

	// Channels resolves channel_id → logo URL.
	Channels ChannelLookup

	// JPEGQuality is the encoder quality for rendered output. 85 is a
	// reasonable default (~30 KB per poster).
	JPEGQuality int
}

// New constructs a Server. cacheDir or logosDir may be empty; rendering
// still works in degraded mode.
func New(cacheDir, logosDir string, channels ChannelLookup) *Server {
	return &Server{
		CacheDir:    cacheDir,
		LogosDir:    logosDir,
		Channels:    channels,
		JPEGQuality: 85,
	}
}

// ServeHTTP handles GET /posters/branded/{channel_id}.jpg. The path
// must end with .jpg; anything else returns 404. ?cat= picks the
// category colour.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.Channels == nil {
		http.Error(w, "branded posters disabled", http.StatusServiceUnavailable)
		return
	}

	rawID := r.PathValue("channel_id")
	if rawID == "" {
		http.NotFound(w, r)
		return
	}
	rawID = strings.TrimSuffix(rawID, ".jpg")

	// "sport" is a channel-independent pseudo-id: a neutral
	// sports-tinted card with no channel logo, shared by every sports
	// programme so Plex's cross-channel identity merge can't propagate
	// one channel's branding onto another (see epg/output.go).
	cacheKey := rawID
	var chID uuid.UUID
	if rawID != "sport" {
		var err error
		chID, err = uuid.Parse(rawID)
		if err != nil {
			http.Error(w, "invalid channel id", http.StatusBadRequest)
			return
		}
		cacheKey = chID.String()
	}

	cat := r.URL.Query().Get("cat")

	// Cache lookup. If a fresh file exists, serve it directly.
	cached := ""
	if s.CacheDir != "" {
		cached = CachePath(s.CacheDir, cacheKey, cat)
		if st, err := os.Stat(cached); err == nil && st.Size() > 0 {
			w.Header().Set("Content-Type", "image/jpeg")
			w.Header().Set("Cache-Control", "public, max-age=86400")
			http.ServeFile(w, r, cached)
			return
		}
	}

	// Render fresh. The sport pseudo-card renders logo-less.
	logoPath := ""
	if rawID != "sport" {
		logoPath, _ = s.resolveLogoPath(r.Context(), chID)
	}

	cats := []string{}
	if cat != "" {
		cats = append(cats, cat)
	}

	// Write to a temp file inside the cache dir, fsync, then rename
	// into place — concurrent readers either see the old file or the
	// fully-written new file, never a half-written one.
	w.Header().Set("Content-Type", "image/jpeg")
	w.Header().Set("Cache-Control", "public, max-age=86400")

	if cached == "" {
		// Caching disabled. Render straight to the response.
		_ = Render(w, logoPath, cats, s.JPEGQuality)
		return
	}

	if err := os.MkdirAll(filepath.Dir(cached), 0o755); err != nil {
		http.Error(w, "cache mkdir: "+err.Error(), http.StatusInternalServerError)
		return
	}
	tmp, err := os.CreateTemp(filepath.Dir(cached), ".branded-*.jpg.tmp")
	if err != nil {
		http.Error(w, "cache tmp: "+err.Error(), http.StatusInternalServerError)
		return
	}
	tmpPath := tmp.Name()
	if err := Render(tmp, logoPath, cats, s.JPEGQuality); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		http.Error(w, "render: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		http.Error(w, "render close: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if err := os.Rename(tmpPath, cached); err != nil {
		_ = os.Remove(tmpPath)
		http.Error(w, "rename cache: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Stamp the file mtime to "now" to make cache eviction predictable.
	_ = os.Chtimes(cached, time.Now(), time.Now())

	http.ServeFile(w, r, cached)
}

// resolveLogoPath turns the channel's logo URL into a local filesystem
// path under LogosDir. Returns ("", err) when the channel has no logo
// or the URL points outside our logos dir — caller is expected to
// degrade gracefully and render without a logo overlay.
func (s *Server) resolveLogoPath(ctx context.Context, chID uuid.UUID) (string, error) {
	if s.LogosDir == "" {
		return "", errors.New("logos dir not configured")
	}
	url, err := s.Channels.GetChannelLogoURL(ctx, chID)
	if err != nil {
		return "", err
	}
	if url == "" {
		return "", errors.New("channel has no logo")
	}
	// URL is expected to look like ".../logos/<filename>". We only
	// trust the LAST segment and only when it reads as a regular file
	// inside LogosDir (no subdirectories, no traversal).
	idx := strings.LastIndex(url, "/")
	if idx < 0 || idx == len(url)-1 {
		return "", errors.New("logo URL has no filename segment")
	}
	name := url[idx+1:]
	if name == "" || strings.ContainsAny(name, `/\`) || name == "." || name == ".." {
		return "", errors.New("invalid logo filename")
	}
	full := filepath.Join(s.LogosDir, name)
	st, err := os.Stat(full)
	if err != nil {
		return "", err
	}
	if !st.Mode().IsRegular() {
		return "", errors.New("logo path is not a regular file")
	}
	return full, nil
}
