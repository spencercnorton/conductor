// Package version exposes build-time version metadata.
// Values are set via -ldflags by the Dockerfile / Makefile.
package version

var (
	Version = "0.7.0-dev"
	Commit  = "unknown"
	Date    = "unknown"
)
