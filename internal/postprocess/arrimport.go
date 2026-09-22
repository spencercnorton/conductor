package postprocess

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// ArrTarget is the connection info for one *arr instance the import stage
// can poke. Zero value (empty BaseURL) means "not configured".
type ArrTarget struct {
	BaseURL string // e.g. http://192.0.2.10:8989
	APIKey  string
}

func (t ArrTarget) configured() bool { return t.BaseURL != "" && t.APIKey != "" }

// ArrImportStage triggers Sonarr/Radarr to scan-and-import a finished
// recording's folder, so reconcile-initiated recordings (which have no
// pending download-client grab) land in the library promptly instead of
// waiting on the next scheduled library scan. For torznab-grab recordings
// the scan is a harmless no-op — *arr won't import an episode it already has.
//
// It POSTs /api/v3/command with DownloadedEpisodesScan (Sonarr) or
// DownloadedMoviesScan (Radarr) against the recording's FILE. *arr treats
// that path as a completed download and imports/renames into the library per
// its own rules.
//
// The path must be the file, not its directory. *arr resolves the series from
// the directory name it is handed, and a TV recording lives at
// "<Series>/Season NN/<file>" — handing it the season folder made Sonarr look
// for a series literally called "Season 02" and refuse the whole batch:
//
//	DownloadedEpisodesImportService|Processing path: .../American Greed/Season 02
//	DownloadedEpisodesImportService|Unknown Series Season 02
//
// The files inside parsed fine ("Episode Parsed. American Greed - S02E01"), so
// nothing looked broken — the recordings simply never imported, the episode
// stayed missing, and the reconciler re-booked the next airing forever ("90 Day"
// S03E09 recorded 11 times). Passing the file skips folder-name resolution.
// Verified against live Sonarr 4.0.19 on 2026-08-27.
type ArrImportStage struct {
	Sonarr ArrTarget
	Radarr ArrTarget
	HTTP   *http.Client
}

func NewArrImportStage(sonarr, radarr ArrTarget) *ArrImportStage {
	return &ArrImportStage{
		Sonarr: sonarr, Radarr: radarr,
		HTTP: &http.Client{Timeout: 20 * time.Second},
	}
}

func (s *ArrImportStage) Name() string { return "arr-import" }

func (s *ArrImportStage) Run(ctx context.Context, in *Input) StageResult {
	target := s.Sonarr
	command := "DownloadedEpisodesScan"
	if in.IsMovie {
		target = s.Radarr
		command = "DownloadedMoviesScan"
	}
	if !target.configured() {
		kind := "sonarr"
		if in.IsMovie {
			kind = "radarr"
		}
		return okResult(s.Name(), "skipped: no "+kind+" configured")
	}

	// Copy, not Move: Conductor owns its DVR tree as 10001:10001 mode 0755 and
	// the *arrs run as PUID 1000, so they can read but not unlink. Move fails.
	// Retention of the source recording stays Conductor's job.
	body, _ := json.Marshal(map[string]any{
		"name":       command,
		"path":       in.FilePath,
		"importMode": "Copy",
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		target.BaseURL+"/api/v3/command", bytes.NewReader(body))
	if err != nil {
		return failResult(s.Name(), err.Error())
	}
	req.Header.Set("X-Api-Key", target.APIKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.HTTP.Do(req)
	if err != nil {
		return failResult(s.Name(), err.Error())
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return failResult(s.Name(),
			fmt.Sprintf("%s: status %d: %s", command, resp.StatusCode, snippet))
	}
	return okResult(s.Name(), fmt.Sprintf("%s queued for %s", command, in.FilePath))
}
