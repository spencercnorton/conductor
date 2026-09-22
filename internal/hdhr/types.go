// Package hdhr implements the subset of the HDHomeRun HTTP API that Plex
// Live TV consumes. Reference: SiliconDust HDHomeRun device JSON spec.
//
// Endpoint surface (see handlers.go):
//
//	GET /discover.json       device descriptor
//	GET /lineup.json         channel list
//	GET /lineup.xml          same, XML form (some clients prefer)
//	GET /lineup_status.json  scan status (always Idle for us)
//	GET /device.xml          UPnP descriptor
//	GET /auto/v<channel>     stream endpoint Plex hits to play a channel
//
// The shape of these payloads matters: Plex parses strictly and silently
// degrades on unexpected fields. Keep this file in sync with what real
// HDHomeRun devices return.
package hdhr

// Device is the Plex-facing identity of one virtual tuner.
// Each Conductor lineup advertises its own Device.
//
// TunerCount is a callback so /discover.json reflects the current sum of
// max_streams across enabled credentials without requiring a Conductor
// restart. Implementations should be cheap (single SQL count).
type Device struct {
	FriendlyName    string
	DeviceID        string
	DeviceAuth      string
	ModelNumber     string
	FirmwareName    string
	FirmwareVersion string
	TunerCount      func() int
	BaseURL         string
}

// StaticTunerCount returns a TunerCount callback that always returns n.
// Useful for tests, the discovery-only fallback, and the boot-time default
// before a DB is wired in.
func StaticTunerCount(n int) func() int { return func() int { return n } }

// Channel is one row in the lineup.
type Channel struct {
	GuideNumber string // "100" or "100.1"
	GuideName   string // human-readable
	URL         string // absolute stream URL Plex will fetch
	HD          int    // 1 if HD, 0 otherwise (Plex shows badge)
	VideoCodec  string // "H264", "HEVC", etc.
	AudioCodec  string // "AC3", "AAC"
}
