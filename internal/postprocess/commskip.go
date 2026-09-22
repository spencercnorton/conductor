package postprocess

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// CommskipStage POSTs the recording file to the external commskip-auto service
// (a hybrid Comskip + Whisper + LLM pipeline). The service produces .edl and
// .srt sidecars next to the source file.
//
// Conductor doesn't need to handle Whisper directly — commskip-auto already
// bundles it. One integration covers both the user's Comskip + Whisper asks.
type CommskipStage struct {
	BaseURL string // e.g. http://192.0.2.11:8800
	APIKey  string // commskip API key, from your secret manager
	Logger  *slog.Logger
	HC      *http.Client
}

func NewCommskipStage(baseURL, apiKey string, logger *slog.Logger) *CommskipStage {
	if baseURL == "" {
		return nil
	}
	return &CommskipStage{
		BaseURL: strings.TrimRight(baseURL, "/"),
		APIKey:  apiKey,
		Logger:  logger,
		// Long timeout: hybrid pipeline (Comskip + Whisper + LLM) on a 1h
		// recording can take 5-15min. Set to 30min upper-bound.
		HC: &http.Client{Timeout: 30 * time.Minute},
	}
}

func (s *CommskipStage) Name() string { return "commskip-auto" }

type commskipReq struct {
	FilePath string            `json:"file_path"`
	Title    string            `json:"title,omitempty"`
	Episode  string            `json:"episode,omitempty"`
	IsMovie  bool              `json:"is_movie,omitempty"`
	Tags     map[string]string `json:"tags,omitempty"`
}

type commskipResp struct {
	JobID    string  `json:"job_id"`
	Status   string  `json:"status"`
	EDLPath  string  `json:"edl_path"`
	SRTPath  string  `json:"srt_path"`
	DurationS float64 `json:"duration_seconds"`
	Error    string  `json:"error"`
}

func (s *CommskipStage) Run(ctx context.Context, in *Input) StageResult {
	body, err := json.Marshal(commskipReq{
		FilePath: in.FilePath,
		Title:    in.Title,
		Episode:  in.EpisodeOnscreen,
		IsMovie:  in.IsMovie,
		Tags: map[string]string{
			"source":       "conductor-dvr",
			"recording_id": in.RecordingID,
		},
	})
	if err != nil {
		return failResult(s.Name(), err.Error())
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		s.BaseURL+"/api/v1/process", bytes.NewReader(body))
	if err != nil {
		return failResult(s.Name(), err.Error())
	}
	req.Header.Set("Content-Type", "application/json")
	if s.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+s.APIKey)
	}

	resp, err := s.HC.Do(req)
	if err != nil {
		return failResult(s.Name(), "request: "+err.Error())
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if resp.StatusCode >= 400 {
		return failResult(s.Name(),
			fmt.Sprintf("HTTP %d: %s", resp.StatusCode, snip(string(respBody), 256)))
	}

	var out commskipResp
	if err := json.Unmarshal(respBody, &out); err != nil {
		// Older commskip-auto versions may return non-JSON success; treat 2xx
		// as success with the raw body in detail.
		return okResult(s.Name(), "non-json response (treated as success): "+snip(string(respBody), 128))
	}

	if out.Error != "" {
		return failResult(s.Name(), out.Error)
	}

	parts := []string{}
	if out.EDLPath != "" {
		parts = append(parts, "edl="+out.EDLPath)
	}
	if out.SRTPath != "" {
		parts = append(parts, "srt="+out.SRTPath)
	}
	if out.JobID != "" {
		parts = append(parts, "job="+out.JobID)
	}
	if len(parts) == 0 {
		parts = append(parts, "ok")
	}
	return okResult(s.Name(), strings.Join(parts, " "))
}

func snip(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "…"
}
