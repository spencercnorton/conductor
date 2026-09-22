// Package logo serves channel logo images from a local directory.
//
// Conductor self-hosts channel logos rather than depending on the source
// EPG provider's HTTP endpoints. The migration script
// (scripts/migrate_logos_from_dispatcharr.py) downloads logos once from
// Dispatcharr (or any other source) into the configured directory; this
// package serves them.
//
// Security: the handler resolves the requested filename against
// `Dir` and rejects any path that escapes (../) or is anything other
// than a regular file. Symlinks are intentionally NOT followed.
package logo

import (
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

// Server is the HTTP handler for `GET /logos/{filename}`.
type Server struct {
	// Dir is the absolute path to the logos directory on the local
	// filesystem. Must exist; created via volume mount + Conductor
	// container init. Empty Dir means logo serving is disabled and the
	// handler returns 503.
	Dir string
}

// New constructs a Server. dir may be empty (disables serving).
func New(dir string) *Server {
	return &Server{Dir: dir}
}

// ServeHTTP handles GET /logos/{filename}. The filename comes from the
// path. Any embedded '/' or '..' is rejected — the handler only serves
// files directly inside Dir, never subdirectories or escapes.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.Dir == "" {
		http.Error(w, "logo serving disabled", http.StatusServiceUnavailable)
		return
	}
	name := r.PathValue("filename")
	if !validFilename(name) {
		http.Error(w, "bad filename", http.StatusBadRequest)
		return
	}
	full := filepath.Join(s.Dir, name)
	// Defense in depth: re-check that the resolved path is still inside Dir.
	// Filepath.Clean would already strip any "..", but use this as a belt-
	// and-suspenders guard against future refactors.
	abs, err := filepath.Abs(full)
	if err != nil {
		http.Error(w, "bad path", http.StatusBadRequest)
		return
	}
	dirAbs, err := filepath.Abs(s.Dir)
	if err != nil {
		http.Error(w, "bad config", http.StatusInternalServerError)
		return
	}
	if !strings.HasPrefix(abs, dirAbs+string(filepath.Separator)) && abs != dirAbs {
		http.Error(w, "path escape rejected", http.StatusForbidden)
		return
	}
	info, err := os.Lstat(full)
	if err != nil {
		if os.IsNotExist(err) {
			http.NotFound(w, r)
			return
		}
		http.Error(w, "logo unavailable", http.StatusInternalServerError)
		return
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		http.Error(w, "logo is not a regular file", http.StatusForbidden)
		return
	}

	// Plex caches the URL, so 1d cache is fine. Logos rarely change
	// without an explicit re-sync.
	w.Header().Set("Cache-Control", "public, max-age=86400")
	http.ServeFile(w, r, full)
}

// validFilename rejects anything that isn't a single safe filename.
// Allow letters, digits, dashes, underscores, dots, and spaces (some
// existing logos have spaces in the name, e.g. "Greenshot 2025-11-08
// 11.36.35.png"). Reject path separators outright.
func validFilename(name string) bool {
	if name == "" {
		return false
	}
	if name == "." || name == ".." {
		return false
	}
	if strings.ContainsAny(name, "/\\\x00") {
		return false
	}
	if strings.HasPrefix(name, ".") {
		return false
	}
	return true
}

// ErrEmptyName is returned by helpers when a filename is empty.
var ErrEmptyName = errors.New("logo: empty filename")
