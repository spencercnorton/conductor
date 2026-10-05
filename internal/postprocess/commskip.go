package postprocess

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// CommskipStage submits the recording to the external commskip-auto service
// (Comskip + Whisper + LLM) as a one-file batch, then waits for that batch to
// finish. Waiting matters: the *arr import stage runs after this one, and a
// real (non-dry-run) cut must be written before Sonarr or Radarr picks the
// file up.
//
// API: POST /api/v1/batches returns 202 {batch_id}; GET /api/v1/batches/{id}
// reports state queued|running|done|cancelled|failed; DELETE cancels the
// episodes still pending.
type CommskipStage struct {
	BaseURL string // e.g. http://192.0.2.11:8910
	APIKey  string // this caller's commskip key, sent as a Bearer token
	DryRun  bool   // commskip writes a report but does not cut the file
	Logger  *slog.Logger
	HC      *http.Client

	// PollInterval and PollTimeout bound the wait for the batch. The
	// timeout stays under the recorder's 30-minute pipeline budget so the
	// import stage still gets to run.
	PollInterval time.Duration
	PollTimeout  time.Duration
}

func NewCommskipStage(baseURL, apiKey string, dryRun bool, logger *slog.Logger) *CommskipStage {
	if baseURL == "" {
		return nil
	}
	return &CommskipStage{
		BaseURL:      strings.TrimRight(baseURL, "/"),
		APIKey:       apiKey,
		DryRun:       dryRun,
		Logger:       logger,
		HC:           &http.Client{Timeout: 30 * time.Second},
		PollInterval: 15 * time.Second,
		PollTimeout:  25 * time.Minute,
	}
}

func (s *CommskipStage) Name() string { return "commskip-auto" }

type commskipTarget struct {
	Type string `json:"type"`
	Path string `json:"path"`
}

type commskipCreate struct {
	Target      commskipTarget `json:"target"`
	Job         string         `json:"job"`
	DryRun      bool           `json:"dry_run"`
	SkipIfFresh bool           `json:"skip_if_fresh"`
	Label       string         `json:"label"`
}

type commskipBatch struct {
	State       string `json:"state"`
	FailedCount int    `json:"failed_count"`
	Episodes    []struct {
		State        string  `json:"state"`
		ApprovedCuts int     `json:"approved_cuts"`
		TotalCutS    float64 `json:"total_cut_s"`
		ErrorTail    string  `json:"error_tail"`
	} `json:"episodes"`
}

func (s *CommskipStage) Run(ctx context.Context, in *Input) StageResult {
	label := strings.Join(strings.Fields(strings.Join(
		[]string{"conductor-dvr", in.Title, in.EpisodeOnscreen, "rec=" + in.RecordingID}, " ")), " ")
	var created struct {
		BatchID string `json:"batch_id"`
	}
	if err := s.call(ctx, http.MethodPost, "/api/v1/batches", commskipCreate{
		Target:      commskipTarget{Type: "file", Path: in.FilePath},
		Job:         "commskip",
		DryRun:      s.DryRun,
		SkipIfFresh: true,
		Label:       label,
	}, &created); err != nil {
		return failResult(s.Name(), "create batch: "+err.Error())
	}
	if created.BatchID == "" {
		return failResult(s.Name(), "create batch: response has no batch_id")
	}
	id := created.BatchID
	path := "/api/v1/batches/" + url.PathEscape(id)

	start := time.Now()
	wait, cancel := context.WithTimeout(ctx, s.PollTimeout)
	defer cancel()
	state, lastErr := "queued", ""
	for {
		var b commskipBatch
		if err := s.call(wait, http.MethodGet, path, nil, &b); err != nil {
			lastErr = err.Error() // transient: keep polling until the deadline
		} else {
			state = b.State
			switch b.State {
			case "done", "failed", "cancelled":
				return s.finish(id, b)
			}
		}
		select {
		case <-wait.Done():
			// Cancel what is still pending so a queued batch does not run
			// later against a file the import stage has already moved.
			dctx, dcancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := s.call(dctx, http.MethodDelete, path, nil, nil); err != nil && s.Logger != nil {
				s.Logger.Warn("commskip cancel failed", "batch", id, "err", err)
			}
			dcancel()
			msg := fmt.Sprintf("batch %s still %s after %s", id, state, time.Since(start).Round(time.Second))
			if lastErr != "" {
				msg += "; last poll error: " + lastErr
			}
			return failResult(s.Name(), msg)
		case <-time.After(s.PollInterval):
		}
	}
}

func (s *CommskipStage) finish(id string, b commskipBatch) StageResult {
	if b.State == "done" && b.FailedCount == 0 {
		cuts, cutS := 0, 0.0
		for _, e := range b.Episodes {
			cuts += e.ApprovedCuts
			cutS += e.TotalCutS
		}
		return okResult(s.Name(), fmt.Sprintf("batch=%s dry_run=%t cuts=%d cut_s=%.1f",
			id, s.DryRun, cuts, cutS))
	}
	msg := fmt.Sprintf("batch=%s state=%s failed=%d", id, b.State, b.FailedCount)
	for _, e := range b.Episodes {
		if e.State != "ok" && e.State != "skipped" {
			msg += " episode=" + e.State
			if e.ErrorTail != "" {
				msg += ": " + snip(e.ErrorTail, 256)
			}
			break
		}
	}
	return failResult(s.Name(), msg)
}

// call sends one JSON request and decodes a 2xx JSON response into out
// (when out is non-nil).
func (s *CommskipStage) call(ctx context.Context, method, path string, body, out any) error {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, s.BaseURL+path, rd)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if s.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+s.APIKey)
	}
	resp, err := s.HC.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, snip(string(raw), 256))
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("decode: %v: %s", err, snip(string(raw), 128))
	}
	return nil
}

func snip(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "…"
}
