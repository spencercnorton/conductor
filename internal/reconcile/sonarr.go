package reconcile

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// SonarrClient is a minimal read-only Sonarr v3 API client. It reads the
// wanted/missing list (monitored episodes with no file) with the series
// resource included, so each want carries its series title + TVDB id.
type SonarrClient struct {
	BaseURL string // e.g. http://192.0.2.10:8989
	APIKey  string
	HTTP    *http.Client
}

func NewSonarrClient(baseURL, apiKey string) *SonarrClient {
	return &SonarrClient{
		BaseURL: trimSlash(baseURL),
		APIKey:  apiKey,
		HTTP:    &http.Client{Timeout: 20 * time.Second},
	}
}

type sonarrSeries struct {
	Title  string `json:"title"`
	Year   int    `json:"year"`
	TVDBID int    `json:"tvdbId"`
}

type sonarrEpisode struct {
	Title         string        `json:"title"` // episode title
	SeasonNumber  int           `json:"seasonNumber"`
	EpisodeNumber int           `json:"episodeNumber"`
	AirDateUTC    *time.Time    `json:"airDateUtc"`
	HasFile       bool          `json:"hasFile"`
	Monitored     bool          `json:"monitored"`
	Series        *sonarrSeries `json:"series"`
}

type sonarrMissingPage struct {
	Records      []sonarrEpisode `json:"records"`
	TotalRecords int             `json:"totalRecords"`
}

// WantedEpisodes returns the monitored, file-less episodes Sonarr wants,
// each with its series context. Episodes whose series didn't come back
// inline are skipped (can't match without a series title).
func (c *SonarrClient) WantedEpisodes(ctx context.Context) ([]EpisodeWant, error) {
	const pageSize = 200
	var wants []EpisodeWant
	for page := 1; ; page++ {
		q := url.Values{}
		q.Set("page", strconv.Itoa(page))
		q.Set("pageSize", strconv.Itoa(pageSize))
		q.Set("includeSeries", "true")
		q.Set("sortKey", "airDateUtc")

		var pg sonarrMissingPage
		if err := c.get(ctx, "/api/v3/wanted/missing", q, &pg); err != nil {
			return nil, err
		}
		for _, e := range pg.Records {
			if e.HasFile || e.Series == nil || e.Series.Title == "" {
				continue
			}
			wants = append(wants, EpisodeWant{
				SeriesTitle:  e.Series.Title,
				SeriesYear:   e.Series.Year,
				TVDBID:       e.Series.TVDBID,
				Season:       e.SeasonNumber,
				Episode:      e.EpisodeNumber,
				EpisodeTitle: e.Title,
				AirDate:      e.AirDateUTC,
			})
		}
		if page*pageSize >= pg.TotalRecords || len(pg.Records) == 0 {
			break
		}
	}
	return wants, nil
}

func (c *SonarrClient) get(ctx context.Context, path string, q url.Values, out any) error {
	u := c.BaseURL + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Api-Key", c.APIKey)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("sonarr %s: status %d", path, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func trimSlash(s string) string { return strings.TrimRight(s, "/") }
