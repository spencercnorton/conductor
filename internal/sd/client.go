// Package sd is the Schedules Direct (api20141201) client.
//
// SD is a paid EPG service ($25/yr) that's the gold standard for US/CA
// broadcast TV — full descriptions, episode metadata, original air dates,
// poster URLs, ratings. Per spec §6.1.
//
// Auth flow:
//  1. POST /token with {username, sha1(password)} → {token, expires}.
//  2. Cache the token (~24h TTL) and pass via "token" header on
//     subsequent calls.
//
// Lineup model:
//   - User subscribes to N lineups (each is a city/ZIP/satellite-package)
//     via the SD web UI.
//   - GET /lineups → operator's lineup list.
//   - GET /lineups/{id} → stations in that lineup.
//   - POST /schedules with [{stationID, dates}] → 14 days of schedule
//     (just program IDs + air times + flags).
//   - POST /programs with [programID, ...] → full program details for
//     the IDs in the schedule.
//
// SD is request-batched: instead of one request per station, you POST
// arrays. We chunk to 5000 entries per request (SD's documented limit).
package sd

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

const DefaultBaseURL = "https://json.schedulesdirect.org/20141201"

// Client wraps a single SD account.
type Client struct {
	BaseURL  string
	Username string
	// Password is hashed once at construction and never stored cleartext.
	passwordSHA1 string

	HC *http.Client

	tokenMu  sync.Mutex
	token    string
	tokenExp time.Time
}

// New constructs a client. Password is hashed on the way in.
func New(baseURL, username, password string) *Client {
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}
	h := sha1.Sum([]byte(password))
	return &Client{
		BaseURL:      strings.TrimRight(baseURL, "/"),
		Username:     username,
		passwordSHA1: hex.EncodeToString(h[:]),
		HC:           &http.Client{Timeout: 60 * time.Second},
	}
}

// NewWithSHA1 lets the operator skip the in-process hash if the password
// is already SHA-1'd (e.g., loaded from sd_credentials.password_sha1).
func NewWithSHA1(baseURL, username, sha1Hex string) *Client {
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}
	return &Client{
		BaseURL:  strings.TrimRight(baseURL, "/"),
		Username: username, passwordSHA1: sha1Hex,
		HC: &http.Client{Timeout: 60 * time.Second},
	}
}

// ─────────────────────── Token ───────────────────────

type tokenResp struct {
	Code     int    `json:"code"`
	Message  string `json:"message"`
	Token    string `json:"token"`
	Datetime string `json:"datetime"`
}

func (c *Client) ensureToken(ctx context.Context) error {
	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()

	if c.token != "" && time.Now().Before(c.tokenExp) {
		return nil
	}

	body, _ := json.Marshal(map[string]string{
		"username": c.Username,
		"password": c.passwordSHA1,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.BaseURL+"/token", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HC.Do(req)
	if err != nil {
		return fmt.Errorf("sd /token: %w", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if resp.StatusCode >= 400 {
		return fmt.Errorf("sd /token HTTP %d: %s", resp.StatusCode, snip(string(respBody), 256))
	}
	var t tokenResp
	if err := json.Unmarshal(respBody, &t); err != nil {
		return fmt.Errorf("sd /token decode: %w", err)
	}
	if t.Code != 0 {
		return fmt.Errorf("sd /token code=%d msg=%q", t.Code, t.Message)
	}
	if t.Token == "" {
		return fmt.Errorf("sd /token returned empty token")
	}
	c.token = t.Token
	// SD tokens are documented as 24h. Use 23h to leave margin.
	c.tokenExp = time.Now().Add(23 * time.Hour)
	return nil
}

// ─────────────────────── Status (account info + lineups subscribed) ──

type StatusResp struct {
	Code           int            `json:"code"`
	Message        string         `json:"message"`
	Account        StatusAcct     `json:"account"`
	Lineups        []StatusLineup `json:"lineups"`
	LastDataUpdate string         `json:"lastDataUpdate"`
}

type StatusAcct struct {
	Expires        string `json:"expires"`
	MaxLineups     int    `json:"maxLineups"`
	MessagesUnread int    `json:"unread,omitempty"`
}

type StatusLineup struct {
	Lineup   string `json:"lineup"`
	Modified string `json:"modified"`
	URI      string `json:"uri"`
	Name     string `json:"name,omitempty"`
	Location string `json:"location,omitempty"`
}

// Status fetches /status — account expiration, lineup list, last data
// update timestamp. Also serves as a lightweight reachability check.
func (c *Client) Status(ctx context.Context) (*StatusResp, error) {
	body, err := c.do(ctx, http.MethodGet, "/status", nil)
	if err != nil {
		return nil, err
	}
	var out StatusResp
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("sd /status decode: %w", err)
	}
	if out.Code != 0 {
		return nil, fmt.Errorf("sd /status code=%d msg=%q", out.Code, out.Message)
	}
	return &out, nil
}

// ─────────────────────── Lineup details (stations) ───────────────────

type LineupResp struct {
	Stations []Station    `json:"stations"`
	Map      []ChannelMap `json:"map"`
	Metadata LineupMeta   `json:"metadata"`
}

type Station struct {
	StationID string `json:"stationID"`
	Name      string `json:"name"`
	Callsign  string `json:"callsign"`
	Affiliate string `json:"affiliate,omitempty"`
	Logo      *struct {
		URL    string `json:"URL"`
		Height int    `json:"height,omitempty"`
		Width  int    `json:"width,omitempty"`
	} `json:"logo,omitempty"`
}

type ChannelMap struct {
	StationID string `json:"stationID"`
	Channel   string `json:"channel"`
	UHFVHF    int    `json:"uhfVhf,omitempty"`
	AtscMajor int    `json:"atscMajor,omitempty"`
	AtscMinor int    `json:"atscMinor,omitempty"`
}

type LineupMeta struct {
	Lineup   string `json:"lineup"`
	Modified string `json:"modified"`
}

// GetLineup fetches one lineup's full station+channel mapping.
func (c *Client) GetLineup(ctx context.Context, lineupID string) (*LineupResp, error) {
	body, err := c.do(ctx, http.MethodGet, "/lineups/"+lineupID, nil)
	if err != nil {
		return nil, err
	}
	var out LineupResp
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("sd /lineups decode: %w", err)
	}
	return &out, nil
}

// ─────────────────────── Schedules + programs ────────────────────

type ScheduleRequest struct {
	StationID string   `json:"stationID"`
	Dates     []string `json:"date,omitempty"` // YYYY-MM-DD; empty = next 14 days
}

type ScheduleResp struct {
	StationID string             `json:"stationID"`
	Code      int                `json:"code,omitempty"`
	Message   string             `json:"message,omitempty"`
	Response  string             `json:"response,omitempty"`
	Programs  []ScheduledProgram `json:"programs"`
	Metadata  ScheduleMeta       `json:"metadata"`
}

type ScheduledProgram struct {
	ProgramID        string           `json:"programID"`
	AirDateTime      string           `json:"airDateTime"` // RFC3339-ish
	Duration         int              `json:"duration"`    // seconds
	MD5              string           `json:"md5"`
	New              bool             `json:"new,omitempty"`
	Premiere         bool             `json:"premiere,omitempty"`
	LiveTapeDelay    string           `json:"liveTapeDelay,omitempty"`
	PremiereOrFinale string           `json:"isPremiereOrFinale,omitempty"`
	Audio            []string         `json:"audioProperties,omitempty"`
	Video            []string         `json:"videoProperties,omitempty"`
	Ratings          []ScheduleRating `json:"ratings,omitempty"`
}

type ScheduleRating struct {
	Body string `json:"body"`
	Code string `json:"code"`
}

type ScheduleMeta struct {
	Modified  string `json:"modified"`
	MD5       string `json:"md5"`
	StartDate string `json:"startDate"`
	Code      int    `json:"code,omitempty"`
}

type ScheduleMD5Entry struct {
	Code         int    `json:"code"`
	Message      string `json:"message"`
	LastModified string `json:"lastModified"`
	MD5          string `json:"md5"`
}

// ScheduleMD5Response is keyed first by station ID and then by YYYY-MM-DD.
// The provider's response is the authoritative manifest of available days for
// each requested station.
type ScheduleMD5Response map[string]map[string]ScheduleMD5Entry

// ScheduleMD5 fetches the provider's station/day availability and cache
// watermarks. Callers should use this manifest before requesting explicit
// dates from Schedules.
func (c *Client) ScheduleMD5(ctx context.Context, reqs []ScheduleRequest) (ScheduleMD5Response, error) {
	if len(reqs) == 0 {
		return ScheduleMD5Response{}, nil
	}
	const chunk = 5000
	out := make(ScheduleMD5Response, len(reqs))
	for start := 0; start < len(reqs); start += chunk {
		end := start + chunk
		if end > len(reqs) {
			end = len(reqs)
		}
		body, _ := json.Marshal(reqs[start:end])
		respBody, err := c.do(ctx, http.MethodPost, "/schedules/md5", body)
		if err != nil {
			return nil, err
		}
		var batch ScheduleMD5Response
		if err := json.Unmarshal(respBody, &batch); err != nil {
			return nil, fmt.Errorf("sd /schedules/md5 decode: %w", err)
		}
		for stationID, dates := range batch {
			if _, duplicate := out[stationID]; duplicate {
				return nil, fmt.Errorf("sd /schedules/md5 returned duplicate station %q across batches", stationID)
			}
			out[stationID] = dates
		}
	}
	return out, nil
}

// Schedules fetches schedule entries for the given station-date pairs.
// SD batches: chunked to 5000 per request.
func (c *Client) Schedules(ctx context.Context, reqs []ScheduleRequest) ([]ScheduleResp, error) {
	if len(reqs) == 0 {
		return nil, nil
	}
	const chunk = 5000
	var out []ScheduleResp
	for start := 0; start < len(reqs); start += chunk {
		end := start + chunk
		if end > len(reqs) {
			end = len(reqs)
		}
		body, _ := json.Marshal(reqs[start:end])
		respBody, err := c.do(ctx, http.MethodPost, "/schedules", body)
		if err != nil {
			return nil, err
		}
		var batch []ScheduleResp
		if err := json.Unmarshal(respBody, &batch); err != nil {
			return nil, fmt.Errorf("sd /schedules decode: %w", err)
		}
		out = append(out, batch...)
	}
	return out, nil
}

type ProgramDetails struct {
	ProgramID       string           `json:"programID"`
	MD5             string           `json:"md5"`
	Code            int              `json:"code,omitempty"`
	Message         string           `json:"message,omitempty"`
	Titles          []ProgramTitle   `json:"titles"`
	Descriptions    ProgramDescs     `json:"descriptions,omitempty"`
	OriginalAirDate string           `json:"originalAirDate,omitempty"`
	Genres          []string         `json:"genres,omitempty"`
	EpisodeTitle    string           `json:"episodeTitle150,omitempty"`
	Metadata        []map[string]any `json:"metadata,omitempty"`
	HasImageArtwork bool             `json:"hasImageArtwork,omitempty"`
}

// EpisodeSeasonNum returns the 1-indexed season and episode number from SD's
// metadata array, preferring the Gracenote provider (SD's primary) and falling
// back to any provider that supplies both fields. Returns 0,0 when the program
// carries no episode numbering — movies, specials, and most sports/news.
//
// SD shape: "metadata": [ { "Gracenote": { "season": 5, "episode": 23 } } ].
// Some older provider rows use "episodeNum" instead of "episode".
func (p ProgramDetails) EpisodeSeasonNum() (season, episode int) {
	read := func(provider string) (int, int) {
		for _, m := range p.Metadata {
			for prov, raw := range m {
				if provider != "" && prov != provider {
					continue
				}
				obj, ok := raw.(map[string]any)
				if !ok {
					continue
				}
				s := intFromAny(obj["season"])
				e := intFromAny(obj["episode"])
				if e == 0 {
					e = intFromAny(obj["episodeNum"])
				}
				if s > 0 && e > 0 {
					return s, e
				}
			}
		}
		return 0, 0
	}
	if s, e := read("Gracenote"); s > 0 && e > 0 {
		return s, e
	}
	return read("") // any provider with both fields
}

// intFromAny coerces a JSON-decoded number (float64 via encoding/json) to int.
func intFromAny(v any) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	}
	return 0
}

type ProgramTitle struct {
	Title120 string `json:"title120"`
}

type ProgramDescs struct {
	Description1000 []ProgramDesc `json:"description1000,omitempty"`
	Description100  []ProgramDesc `json:"description100,omitempty"`
}

type ProgramDesc struct {
	DescriptionLanguage string `json:"descriptionLanguage"`
	Description         string `json:"description"`
}

// Programs fetches full program details for the given program IDs.
// Chunked at 5000 per SD's docs.
func (c *Client) Programs(ctx context.Context, programIDs []string) ([]ProgramDetails, error) {
	if len(programIDs) == 0 {
		return nil, nil
	}
	const chunk = 5000
	var out []ProgramDetails
	for start := 0; start < len(programIDs); start += chunk {
		end := start + chunk
		if end > len(programIDs) {
			end = len(programIDs)
		}
		body, _ := json.Marshal(programIDs[start:end])
		respBody, err := c.do(ctx, http.MethodPost, "/programs", body)
		if err != nil {
			return nil, err
		}
		var batch []ProgramDetails
		if err := json.Unmarshal(respBody, &batch); err != nil {
			return nil, fmt.Errorf("sd /programs decode: %w", err)
		}
		out = append(out, batch...)
	}
	return out, nil
}

// ─────────────────────── HTTP plumbing ───────────────────────

func (c *Client) do(ctx context.Context, method, path string, body []byte) ([]byte, error) {
	if err := c.ensureToken(ctx); err != nil {
		return nil, err
	}

	var br io.Reader
	if body != nil {
		br = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, br)
	if err != nil {
		return nil, err
	}
	c.tokenMu.Lock()
	tok := c.token
	c.tokenMu.Unlock()
	req.Header.Set("token", tok)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.HC.Do(req)
	if err != nil {
		return nil, fmt.Errorf("sd %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20)) // 64MB cap
	if err != nil {
		return nil, fmt.Errorf("sd read: %w", err)
	}

	// On 401, refresh + retry once.
	if resp.StatusCode == http.StatusUnauthorized {
		c.tokenMu.Lock()
		c.token = ""
		c.tokenMu.Unlock()
		if err := c.ensureToken(ctx); err != nil {
			return nil, err
		}
		return c.do(ctx, method, path, body)
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("sd %s %s: HTTP %d: %s",
			method, path, resp.StatusCode, snip(string(respBody), 256))
	}
	return respBody, nil
}

func snip(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "…"
}
