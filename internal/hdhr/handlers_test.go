package hdhr_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/spencercnorton/conductor/internal/hdhr"
)

func newTestHandlers() *hdhr.Handlers {
	d := hdhr.Device{
		FriendlyName:    "Conductor (Test)",
		DeviceID:        "DEADBEEF",
		DeviceAuth:      "conductor",
		ModelNumber:     "HDTC-2US",
		FirmwareName:    "hdhomeruntc_atsc",
		FirmwareVersion: "20200225",
		TunerCount:      hdhr.StaticTunerCount(4),
		BaseURL:         "http://test.local:8409",
	}
	return hdhr.NewStatic(d, hdhr.SeedLineup(d.BaseURL))
}

func TestDiscoverShape(t *testing.T) {
	h := newTestHandlers()
	req := httptest.NewRequest(http.MethodGet, "/discover.json", nil)
	rr := httptest.NewRecorder()
	h.Discover(rr, req)

	if got, want := rr.Code, http.StatusOK; got != want {
		t.Fatalf("status: got %d want %d", got, want)
	}
	var body map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}

	// Plex parses these specific keys; if a refactor renames any of them,
	// pairing breaks silently. Lock them in.
	for _, k := range []string{
		"FriendlyName", "Manufacturer", "ModelNumber", "FirmwareName",
		"TunerCount", "FirmwareVersion", "DeviceID", "DeviceAuth",
		"BaseURL", "LineupURL",
	} {
		if _, ok := body[k]; !ok {
			t.Errorf("discover.json missing required key %q", k)
		}
	}

	if got, want := body["LineupURL"], "http://test.local:8409/lineup.json"; got != want {
		t.Errorf("LineupURL: got %v want %v", got, want)
	}
	if got, want := body["Manufacturer"], "Conductor"; got != want {
		t.Errorf("Manufacturer: got %v want %v", got, want)
	}
}

func TestLineupShape(t *testing.T) {
	h := newTestHandlers()
	req := httptest.NewRequest(http.MethodGet, "/lineup.json", nil)
	rr := httptest.NewRecorder()
	h.LineupJSON(rr, req)

	if got, want := rr.Code, http.StatusOK; got != want {
		t.Fatalf("status: got %d want %d", got, want)
	}
	var entries []map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &entries); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("lineup is empty")
	}
	for i, e := range entries {
		for _, k := range []string{"GuideNumber", "GuideName", "URL"} {
			if _, ok := e[k]; !ok {
				t.Errorf("entry %d missing required key %q", i, k)
			}
		}
		if u, _ := e["URL"].(string); !strings.HasPrefix(u, "http") {
			t.Errorf("entry %d URL is not absolute: %v", i, u)
		}
	}
}

func TestLineupStatusIdle(t *testing.T) {
	h := newTestHandlers()
	req := httptest.NewRequest(http.MethodGet, "/lineup_status.json", nil)
	rr := httptest.NewRecorder()
	h.LineupStatus(rr, req)

	var body map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got, want := body["ScanInProgress"], float64(0); got != want {
		t.Errorf("ScanInProgress: got %v want %v", got, want)
	}
}

func TestDeviceXML(t *testing.T) {
	h := newTestHandlers()
	req := httptest.NewRequest(http.MethodGet, "/device.xml", nil)
	rr := httptest.NewRecorder()
	h.DeviceXML(rr, req)

	body := rr.Body.String()
	for _, want := range []string{
		"<URLBase>http://test.local:8409</URLBase>",
		"<friendlyName>Conductor (Test)</friendlyName>",
		"<UDN>uuid:DEADBEEF</UDN>",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("device.xml missing %q", want)
		}
	}
}
