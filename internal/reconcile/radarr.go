package reconcile

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// RadarrClient is a minimal read-only Radarr v3 API client. It reads the
// wanted/missing list — monitored movies with no file — which is exactly
// the set the reconciler should try to record off live TV.
type RadarrClient struct {
	BaseURL string // e.g. http://192.0.2.10:7878
	APIKey  string
	HTTP    *http.Client
}

func NewRadarrClient(baseURL, apiKey string) *RadarrClient {
	return &RadarrClient{
		BaseURL: trimSlash(baseURL),
		APIKey:  apiKey,
		HTTP:    &http.Client{Timeout: 20 * time.Second},
	}
}

// radarrMovie is the subset of Radarr's MovieResource we consume.
type radarrMovie struct {
	Title     string `json:"title"`
	Year      int    `json:"year"`
	TMDbID    int    `json:"tmdbId"`
	IMDbID    string `json:"imdbId"`
	HasFile   bool   `json:"hasFile"`
	Monitored bool   `json:"monitored"`
}

type radarrMissingPage struct {
	Records      []radarrMovie `json:"records"`
	TotalRecords int           `json:"totalRecords"`
}

// WantedMovies returns the monitored, file-less movies Radarr wants. The
// wanted/missing endpoint already filters to monitored && !hasFile, so the
// result is ready to match against the EPG.
func (c *RadarrClient) WantedMovies(ctx context.Context) ([]MovieWant, error) {
	const pageSize = 200
	var wants []MovieWant
	for page := 1; ; page++ {
		q := url.Values{}
		q.Set("page", strconv.Itoa(page))
		q.Set("pageSize", strconv.Itoa(pageSize))
		q.Set("sortKey", "movies.sortTitle")

		var pg radarrMissingPage
		if err := c.get(ctx, "/api/v3/wanted/missing", q, &pg); err != nil {
			return nil, err
		}
		for _, m := range pg.Records {
			if m.HasFile {
				continue
			}
			wants = append(wants, MovieWant{
				TMDbID: m.TMDbID, IMDbID: m.IMDbID,
				Title: m.Title, Year: m.Year,
			})
		}
		if page*pageSize >= pg.TotalRecords || len(pg.Records) == 0 {
			break
		}
	}
	return wants, nil
}

func (c *RadarrClient) get(ctx context.Context, path string, q url.Values, out any) error {
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
		return fmt.Errorf("radarr %s: status %d", path, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
