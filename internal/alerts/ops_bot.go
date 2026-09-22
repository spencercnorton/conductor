// Package alerts forwards operational notifications to the ops
// Bot's webhook surface so they show up alongside the rest of the
// home-lab fleet alerts.
//
// The bot accepts JSON over an HMAC-authed POST. Configuration lives in
// internal/config:
//
//	CONDUCTOR_OPSBOT_WEBHOOK   e.g. https://ops.example.com/webhooks/conductor
//	CONDUCTOR_OPSBOT_SECRET    shared HMAC secret 
//
// If either is unset the alerter no-ops — useful for local dev.
package alerts

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

type Severity string

const (
	SevInfo  Severity = "info"
	SevWarn  Severity = "warn"
	SevError Severity = "error"
	SevPage  Severity = "page" // wakes the on-call operator
)

type Event struct {
	Severity Severity       `json:"severity"`
	Source   string         `json:"source"` // e.g. "conductor.stream"
	Title    string         `json:"title"`
	Detail   string         `json:"detail,omitempty"`
	Tags     map[string]any `json:"tags,omitempty"`
}

type Client struct {
	URL    string
	Secret string
	Logger *slog.Logger
	HC     *http.Client
}

func New(url, secret string, logger *slog.Logger) *Client {
	return &Client{
		URL:    url,
		Secret: secret,
		Logger: logger,
		HC:     &http.Client{Timeout: 5 * time.Second},
	}
}

// Send posts an event. Errors are logged and swallowed — alerts must not
// block the stream hot path.
func (c *Client) Send(ctx context.Context, ev Event) {
	if c == nil || c.URL == "" {
		return
	}
	body, err := json.Marshal(ev)
	if err != nil {
		c.Logger.Warn("alert marshal", "err", err)
		return
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL, bytes.NewReader(body))
	if err != nil {
		c.Logger.Warn("alert request build", "err", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if c.Secret != "" {
		mac := hmac.New(sha256.New, []byte(c.Secret))
		mac.Write(body)
		req.Header.Set("X-Conductor-Signature", "sha256="+hex.EncodeToString(mac.Sum(nil)))
	}

	resp, err := c.HC.Do(req)
	if err != nil {
		c.Logger.Warn("alert send", "err", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		c.Logger.Warn("alert non-2xx", "status", resp.StatusCode, "title", ev.Title)
	}
}

// Convenience constructors.

func StreamFailover(channel, fromProvider, toProvider, reason string) Event {
	return Event{
		Severity: SevWarn,
		Source:   "conductor.stream",
		Title:    fmt.Sprintf("Channel %s failed over %s → %s", channel, fromProvider, toProvider),
		Detail:   reason,
		Tags:     map[string]any{"channel": channel, "from": fromProvider, "to": toProvider},
	}
}

func SlotExhausted(provider, credential string, max int) Event {
	return Event{
		Severity: SevError,
		Source:   "conductor.pool",
		Title:    fmt.Sprintf("Slot exhausted: %s/%s (max=%d)", provider, credential, max),
		Tags:     map[string]any{"provider": provider, "credential": credential, "max": max},
	}
}

func EPGStale(channelCount int, hoursOld float64) Event {
	return Event{
		Severity: SevWarn,
		Source:   "conductor.epg",
		Title:    fmt.Sprintf("EPG is %.1fh stale across %d channels", hoursOld, channelCount),
	}
}

func EPGSourceFailing(name string, consecutive int, lastErr string) Event {
	return Event{
		Severity: SevError,
		Source:   "conductor.epg",
		Title:    fmt.Sprintf("EPG source %q failing: %d consecutive fetch failures", name, consecutive),
		Detail:   lastErr,
		Tags:     map[string]any{"source_name": name, "consecutive_failures": consecutive},
	}
}

func EPGSourceRecovered(name string, failures int) Event {
	return Event{
		Severity: SevInfo,
		Source:   "conductor.epg",
		Title:    fmt.Sprintf("EPG source %q recovered after %d failed fetches", name, failures),
		Tags:     map[string]any{"source_name": name},
	}
}

func EPGHorizonLow(medianDays float64, channelCount int) Event {
	return Event{
		Severity: SevError,
		Source:   "conductor.epg",
		Title:    fmt.Sprintf("Guide horizon low: %.1f days of future EPG (median across %d channels)", medianDays, channelCount),
		Detail:   "Plex's guide goes dark when this reaches 0 — check /admin/epg/health for the failing source.",
		Tags:     map[string]any{"horizon_days": medianDays, "channels": channelCount},
	}
}

func WorkerStalled(worker string, sinceLastSuccess time.Duration, detail string) Event {
	return Event{
		Severity: SevError,
		Source:   "conductor." + worker,
		Title:    fmt.Sprintf("%s worker has not succeeded in %s", worker, sinceLastSuccess.Round(time.Minute)),
		Detail:   detail,
		Tags:     map[string]any{"worker": worker},
	}
}

func WorkerRecovered(worker string, downFor time.Duration) Event {
	return Event{
		Severity: SevInfo,
		Source:   "conductor." + worker,
		Title:    fmt.Sprintf("%s worker recovered after %s", worker, downFor.Round(time.Minute)),
		Tags:     map[string]any{"worker": worker},
	}
}
