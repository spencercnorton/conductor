package logo

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// helper: serve via http.ServeMux so we can use {filename} path values.
func newTestServer(t *testing.T, dir string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /logos/{filename}", New(dir).ServeHTTP)
	return httptest.NewServer(mux)
}

func TestServeFile(t *testing.T) {
	dir := t.TempDir()
	const payload = "PNG-bytes-here"
	if err := os.WriteFile(filepath.Join(dir, "abc.png"), []byte(payload), 0o644); err != nil {
		t.Fatal(err)
	}

	srv := newTestServer(t, dir)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/logos/abc.png")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("status: got %d want 200", resp.StatusCode)
	}
	if cc := resp.Header.Get("Cache-Control"); !strings.Contains(cc, "public") {
		t.Errorf("missing Cache-Control: %q", cc)
	}
}

func TestServeMissingReturns404(t *testing.T) {
	srv := newTestServer(t, t.TempDir())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/logos/does-not-exist.png")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Errorf("status: got %d want 404", resp.StatusCode)
	}
}

func TestServeSymlinkRejected(t *testing.T) {
	dir := t.TempDir()
	outside := filepath.Join(t.TempDir(), "secret.txt")
	if err := os.WriteFile(outside, []byte("not a logo"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(dir, "linked.png")); err != nil {
		t.Fatal(err)
	}

	srv := newTestServer(t, dir)
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/logos/linked.png")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("symlink status = %d, want %d", resp.StatusCode, http.StatusForbidden)
	}
}

func TestPathTraversalRejected(t *testing.T) {
	dir := t.TempDir()
	parent := filepath.Dir(dir)
	if err := os.WriteFile(filepath.Join(parent, "secret.txt"), []byte("nope"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(filepath.Join(parent, "secret.txt")) })

	srv := newTestServer(t, dir)
	defer srv.Close()

	// Various traversal attempts the handler should reject.
	cases := []string{
		"%2E%2E%2Fsecret.txt", // ../secret.txt URL-encoded
		"%2F" + "secret.txt",  // /secret.txt URL-encoded
		"..",
		".",
	}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			resp, err := http.Get(srv.URL + "/logos/" + name)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			// Anything 2xx would be a security bug.
			if resp.StatusCode/100 == 2 {
				t.Errorf("traversal %q returned 2xx — security bug", name)
			}
		})
	}
}

func TestEmptyDirDisabled(t *testing.T) {
	srv := newTestServer(t, "")
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/logos/x.png")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status: got %d want 503", resp.StatusCode)
	}
}

func TestValidFilename(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"plain.png", true},
		{"with space.png", true},
		{"NBC_Peacock_1986.png", true},
		{"Greenshot 2025-11-08 11.36.35.png", true},
		{"name.with.dots.svg", true},
		// Rejected
		{"", false},
		{".", false},
		{"..", false},
		{"path/file.png", false},
		{"path\\file.png", false},
		{"\x00null.png", false},
		{".hidden.png", false},
	}
	for _, tc := range cases {
		got := validFilename(tc.in)
		if got != tc.want {
			t.Errorf("validFilename(%q): got %v want %v", tc.in, got, tc.want)
		}
	}
}
