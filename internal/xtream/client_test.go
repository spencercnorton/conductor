package xtream

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGetLiveStreams_ParsesNumericAndStringIDs(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/player_api.php" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		q := r.URL.Query()
		if q.Get("username") != "acc01" || q.Get("password") != "p&ss" || q.Get("action") != "get_live_streams" {
			t.Errorf("unexpected query %s", r.URL.RawQuery)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[
			{"stream_id": 2012667, "name": "LIVE | NFL 01", "epg_channel_id": "foxsports1.us"},
			{"stream_id": "555", "name": "string-id stream", "epg_channel_id": null},
			{"stream_id": 0, "name": "bogus zero id"},
			{"stream_id": 777, "name": "no epg", "epg_channel_id": "  "}
		]`))
	}))
	defer srv.Close()

	c := New(srv.URL+"/", "acc01", "p&ss", srv.Client())
	got, err := c.GetLiveStreams(context.Background())
	if err != nil {
		t.Fatalf("GetLiveStreams: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 streams (zero-id dropped), got %d: %+v", len(got), got)
	}
	if got[0].StreamID != 2012667 || got[0].EPGChannelID != "foxsports1.us" {
		t.Errorf("numeric id row mangled: %+v", got[0])
	}
	if got[1].StreamID != 555 || got[1].Name != "string-id stream" || got[1].EPGChannelID != "" {
		t.Errorf("string id / null epg row mangled: %+v", got[1])
	}
	if got[2].EPGChannelID != "" {
		t.Errorf("whitespace epg id should trim to empty: %+v", got[2])
	}
}

func TestGetLiveStreams_HTTPErrorSurfaces(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusForbidden)
	}))
	defer srv.Close()

	c := New(srv.URL, "u", "p", srv.Client())
	if _, err := c.GetLiveStreams(context.Background()); err == nil {
		t.Fatal("want error on HTTP 403")
	}
}

func TestGetLiveStreams_BadJSONSurfaces(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"not": "an array"`))
	}))
	defer srv.Close()

	c := New(srv.URL, "u", "p", srv.Client())
	if _, err := c.GetLiveStreams(context.Background()); err == nil {
		t.Fatal("want decode error")
	}
}

func TestGetLiveStreams_TransportErrorRedactsCredentials(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	baseURL := srv.URL
	srv.Close()

	c := New(baseURL, "secret-user", "secret-password", &http.Client{})
	_, err := c.GetLiveStreams(context.Background())
	if err == nil {
		t.Fatal("want transport error from closed server")
	}
	if msg := err.Error(); strings.Contains(msg, "secret-user") || strings.Contains(msg, "secret-password") {
		t.Fatalf("transport error leaked Xtream credentials: %q", msg)
	}
}
