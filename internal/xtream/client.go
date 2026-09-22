// Package xtream is a minimal client for the Xtream-Codes panel API the
// IPTV providers behind our channel_source rows expose. Conductor uses it
// to read the live-stream catalogue (player_api.php?action=get_live_streams)
// — the same data Dispatcharr used to mirror into its internal Postgres —
// so ppvsync can resolve "what is the provider currently calling the
// stream behind this source URL?" without any middleman.
//
// Deliberately tiny: one endpoint, no auth state (Xtream auth is just
// username/password query params), no panel-management surface.
package xtream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Client talks to one Xtream panel with one credential pair.
type Client struct {
	baseURL  string // scheme://host[:port], no trailing slash
	username string
	password string
	hc       *http.Client
}

// New builds a Client. hc == nil gets a default with a generous timeout —
// live-stream catalogues run to tens of MB (48K streams ≈ 17 MB JSON on
// iboost; Go's transport negotiates gzip transparently so the wire cost
// is ~10× smaller).
func New(baseURL, username, password string, hc *http.Client) *Client {
	if hc == nil {
		hc = &http.Client{Timeout: 120 * time.Second}
	}
	return &Client{
		baseURL:  strings.TrimRight(baseURL, "/"),
		username: username,
		password: password,
		hc:       hc,
	}
}

// BaseURL returns the normalized panel URL (for logging/dedupe).
func (c *Client) BaseURL() string { return c.baseURL }

// LiveStream is the subset of get_live_streams fields Conductor consumes.
type LiveStream struct {
	StreamID     int64
	Name         string
	EPGChannelID string
}

// maxCatalogueBytes caps the response read. iboost's full catalogue is
// ~18 MB; 256 MB leaves room for much larger panels while still bounding
// a misbehaving endpoint.
const maxCatalogueBytes = 256 << 20

// GetLiveStreams fetches the panel's full live-stream catalogue.
func (c *Client) GetLiveStreams(ctx context.Context) ([]LiveStream, error) {
	u := fmt.Sprintf("%s/player_api.php?username=%s&password=%s&action=get_live_streams",
		c.baseURL, url.QueryEscape(c.username), url.QueryEscape(c.password))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Conductor")

	resp, err := c.hc.Do(req)
	if err != nil {
		// net/http wraps transport failures in *url.Error, whose Error method
		// includes the complete request URL. Xtream credentials live in that
		// query string, so unwrap the transport cause before returning/logging.
		var urlErr *url.Error
		if errors.As(err, &urlErr) {
			err = urlErr.Err
		}
		return nil, fmt.Errorf("xtream get_live_streams: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("xtream get_live_streams: HTTP %d", resp.StatusCode)
	}

	var raw []liveStreamRaw
	dec := json.NewDecoder(io.LimitReader(resp.Body, maxCatalogueBytes))
	if err := dec.Decode(&raw); err != nil {
		return nil, fmt.Errorf("xtream get_live_streams decode: %w", err)
	}

	out := make([]LiveStream, 0, len(raw))
	for _, r := range raw {
		id := int64(r.StreamID)
		if id <= 0 {
			continue
		}
		out = append(out, LiveStream{
			StreamID:     id,
			Name:         r.Name,
			EPGChannelID: strings.TrimSpace(r.EPGChannelID),
		})
	}
	return out, nil
}

// liveStreamRaw tolerates the schema drift across Xtream panel forks:
// stream_id arrives as a JSON number on most panels but as a quoted
// string on some; epg_channel_id can be null.
type liveStreamRaw struct {
	StreamID     flexInt64 `json:"stream_id"`
	Name         string    `json:"name"`
	EPGChannelID string    `json:"epg_channel_id"`
}

type flexInt64 int64

func (f *flexInt64) UnmarshalJSON(b []byte) error {
	s := strings.Trim(strings.TrimSpace(string(b)), `"`)
	if s == "" || s == "null" {
		*f = 0
		return nil
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		// Some panels emit floats ("2012667.0"); tolerate.
		fv, ferr := strconv.ParseFloat(s, 64)
		if ferr != nil {
			return fmt.Errorf("stream_id %q: %w", s, err)
		}
		v = int64(fv)
	}
	*f = flexInt64(v)
	return nil
}
