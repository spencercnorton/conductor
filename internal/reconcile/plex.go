package reconcile

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

// PlexClient is a minimal read-only Plex client used for the
// owned-but-incomplete gap diff: it enumerates the episodes already in the
// Plex TV libraries so the reconciler can spot a series you own that's
// airing an episode you're missing (and that Sonarr isn't monitoring).
type PlexClient struct {
	BaseURL string // e.g. https://192.0.2.10:32400
	Token   string // X-Plex-Token
	HTTP    *http.Client
}

func NewPlexClient(baseURL, token string) *PlexClient {
	return &PlexClient{
		BaseURL: trimSlash(baseURL),
		Token:   token,
		// Plex serves the API over TLS on :32400 and presents a
		// *.plex.direct cert that never validates against a LAN IP, so a
		// by-IP connection must skip verification. This is a trusted-LAN
		// call (token-authed, internal address) — standard for Plex clients.
		HTTP: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
		},
	}
}

// OwnedIndex records which episodes are already in the Plex library, keyed
// by normalized series title. An episode is "owned" if EITHER its
// normalized title OR its SxxExx number is present for that series.
type OwnedIndex struct {
	// seriesKey -> set of owned episode keys ("t:<norm title>" / "s:SxxExx")
	episodes map[string]map[string]struct{}
}

func newOwnedIndex() *OwnedIndex {
	return &OwnedIndex{episodes: map[string]map[string]struct{}{}}
}

func (o *OwnedIndex) add(seriesKey, epTitle string, season, episode int) {
	set := o.episodes[seriesKey]
	if set == nil {
		set = map[string]struct{}{}
		o.episodes[seriesKey] = set
	}
	if t := normalizeTitle(epTitle); t != "" {
		set["t:"+t] = struct{}{}
	}
	if se := onscreen(season, episode); se != "" {
		set["s:"+se] = struct{}{}
	}
}

// OwnsSeries reports whether the Plex library has any episode of the series.
func (o *OwnedIndex) OwnsSeries(seriesKey string) bool {
	return len(o.episodes[seriesKey]) > 0
}

// OwnsEpisode reports whether the specific episode (by title or SxxExx) is
// already in the library for an owned series.
func (o *OwnedIndex) OwnsEpisode(seriesKey, epTitle, se string) bool {
	set := o.episodes[seriesKey]
	if set == nil {
		return false
	}
	if t := normalizeTitle(epTitle); t != "" {
		if _, ok := set["t:"+t]; ok {
			return true
		}
	}
	if se != "" {
		if _, ok := set["s:"+se]; ok {
			return true
		}
	}
	return false
}

// SeriesCount is the number of distinct owned series (for logging).
func (o *OwnedIndex) SeriesCount() int { return len(o.episodes) }

// ─────────────────────── Plex API ───────────────────────

type plexMediaContainer struct {
	MediaContainer struct {
		Directory []plexDirectory `json:"Directory"`
		Metadata  []plexMetadata  `json:"Metadata"`
	} `json:"MediaContainer"`
}

type plexDirectory struct {
	Key  string `json:"key"`
	Type string `json:"type"` // "show" | "movie" | "artist"
}

type plexMetadata struct {
	Type             string `json:"type"`             // "episode"
	Title            string `json:"title"`            // episode title
	GrandparentTitle string `json:"grandparentTitle"` // series title
	ParentIndex      int    `json:"parentIndex"`      // season number
	Index            int    `json:"index"`            // episode number
}

// OwnedSeriesEpisodes builds the OwnedIndex by listing every episode in
// each TV (type "show") library section. One bulk call per show section.
func (c *PlexClient) OwnedSeriesEpisodes(ctx context.Context) (*OwnedIndex, error) {
	var sections plexMediaContainer
	if err := c.get(ctx, "/library/sections", nil, &sections); err != nil {
		return nil, fmt.Errorf("plex sections: %w", err)
	}

	idx := newOwnedIndex()
	for _, d := range sections.MediaContainer.Directory {
		if d.Type != "show" {
			continue
		}
		// type=4 → episodes (flat list across the whole section).
		var eps plexMediaContainer
		q := url.Values{"type": {"4"}}
		if err := c.get(ctx, "/library/sections/"+d.Key+"/all", q, &eps); err != nil {
			return nil, fmt.Errorf("plex section %s episodes: %w", d.Key, err)
		}
		for _, m := range eps.MediaContainer.Metadata {
			if m.GrandparentTitle == "" {
				continue
			}
			idx.add(titleKey(m.GrandparentTitle), m.Title, m.ParentIndex, m.Index)
		}
	}
	return idx, nil
}

func (c *PlexClient) get(ctx context.Context, path string, q url.Values, out any) error {
	if q == nil {
		q = url.Values{}
	}
	q.Set("X-Plex-Token", c.Token)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		c.BaseURL+path+"?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("plex %s: status %d", path, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
