package postprocess

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestArrImportStage_RoutesEpisodeToSonarr(t *testing.T) {
	var gotPath, gotName, gotKey, gotMode string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("X-Api-Key")
		var body map[string]any
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &body)
		gotName, _ = body["name"].(string)
		gotPath, _ = body["path"].(string)
		gotMode, _ = body["importMode"].(string)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":1}`))
	}))
	defer srv.Close()

	st := NewArrImportStage(
		ArrTarget{BaseURL: srv.URL, APIKey: "sonarr-key"},
		ArrTarget{}, // radarr unconfigured
	)
	res := st.Run(context.Background(), &Input{
		FilePath: "/dvr/TV/The Bear/Season 03/The Bear.S03E07.ts", IsMovie: false,
	})
	if !res.Success {
		t.Fatalf("expected success, got %q", res.Detail)
	}
	if gotName != "DownloadedEpisodesScan" {
		t.Errorf("command = %q, want DownloadedEpisodesScan", gotName)
	}
	// The FILE, not its directory. *arr resolves the series from the directory
	// name it is given, so the season folder made Sonarr hunt for a series
	// called "Season 03" and reject the batch ("Unknown Series Season 03").
	if gotPath != "/dvr/TV/The Bear/Season 03/The Bear.S03E07.ts" {
		t.Errorf("path = %q, want the recording file (not its folder)", gotPath)
	}
	// Copy: Conductor's tree is 10001:10001 0755 and the *arrs are PUID 1000,
	// so they cannot unlink. Move fails.
	if gotMode != "Copy" {
		t.Errorf("importMode = %q, want Copy", gotMode)
	}
	if gotKey != "sonarr-key" {
		t.Errorf("api key = %q", gotKey)
	}
}

func TestArrImportStage_MovieRoutesToRadarr(t *testing.T) {
	var gotName string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &body)
		gotName, _ = body["name"].(string)
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	st := NewArrImportStage(ArrTarget{}, ArrTarget{BaseURL: srv.URL, APIKey: "k"})
	res := st.Run(context.Background(), &Input{
		FilePath: "/dvr/Movies/Heat (1995)/Heat (1995).ts", IsMovie: true,
	})
	if !res.Success || gotName != "DownloadedMoviesScan" {
		t.Fatalf("movie should route to radarr DownloadedMoviesScan, got success=%v name=%q", res.Success, gotName)
	}
}

func TestArrImportStage_SkipsWhenUnconfigured(t *testing.T) {
	st := NewArrImportStage(ArrTarget{}, ArrTarget{}) // neither configured
	res := st.Run(context.Background(), &Input{FilePath: "/dvr/TV/x/x.ts"})
	if !res.Success {
		t.Errorf("unconfigured target should be a no-op success, got %q", res.Detail)
	}
}
