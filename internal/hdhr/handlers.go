package hdhr

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
)

// LineupFunc returns the current lineup. Called fresh on every /lineup.json
// request so admin edits show up without a restart. Implementations should
// be cheap (single DB query).
type LineupFunc func(ctx context.Context) ([]Channel, error)

// Handlers is the HDHomeRun emulator surface mounted by internal/api.
type Handlers struct {
	Device  Device
	Lineup  LineupFunc
}

// New constructs a Handlers. Use NewStatic if you have a fixed lineup
// (tests, dev seed).
func New(d Device, lineup LineupFunc) *Handlers {
	return &Handlers{Device: d, Lineup: lineup}
}

// NewStatic is a convenience for tests and the Phase 0 boot path.
func NewStatic(d Device, channels []Channel) *Handlers {
	return New(d, func(context.Context) ([]Channel, error) { return channels, nil })
}

// SeedLineup returns a single test channel so a fresh deploy can complete
// HDHomeRun pairing in Plex without provider creds.
//
// Phase 0 scope is device discovery, not playback: the channel URL points at
// a public MP4 that exists for end-to-end smoke testing of the proxy, but
// Plex Live TV expects MPEG-TS and will not play it. Real playback lands in
// Phase 1 once the provider/credential model is in place and the upstream
// switches to real Xtream/M3U sources (which ARE MPEG-TS).
func SeedLineup(baseURL string) []Channel {
	return []Channel{
		{
			GuideNumber: "1",
			GuideName:   "Conductor Pairing Test",
			URL:         baseURL + "/auto/v1",
			HD:          1,
			VideoCodec:  "H264",
			AudioCodec:  "AAC",
		},
	}
}

// Discover serves /discover.json. Plex polls this for device identity and
// derives lineup URL + tuner count from it. Field names must match what real
// HDHomeRun firmware returns; case matters.
func (h *Handlers) Discover(w http.ResponseWriter, r *http.Request) {
	type discover struct {
		FriendlyName    string `json:"FriendlyName"`
		Manufacturer    string `json:"Manufacturer"`
		ModelNumber     string `json:"ModelNumber"`
		FirmwareName    string `json:"FirmwareName"`
		TunerCount      int    `json:"TunerCount"`
		FirmwareVersion string `json:"FirmwareVersion"`
		DeviceID        string `json:"DeviceID"`
		DeviceAuth      string `json:"DeviceAuth"`
		BaseURL         string `json:"BaseURL"`
		LineupURL       string `json:"LineupURL"`
	}
	resp := discover{
		FriendlyName:    h.Device.FriendlyName,
		Manufacturer:    "Conductor",
		ModelNumber:     h.Device.ModelNumber,
		FirmwareName:    h.Device.FirmwareName,
		TunerCount:      h.Device.TunerCount(),
		FirmwareVersion: h.Device.FirmwareVersion,
		DeviceID:        h.Device.DeviceID,
		DeviceAuth:      h.Device.DeviceAuth,
		BaseURL:         h.Device.BaseURL,
		LineupURL:       h.Device.BaseURL + "/lineup.json",
	}
	writeJSON(w, resp)
}

// LineupJSON serves /lineup.json — the channel list Plex stores at setup.
func (h *Handlers) LineupJSON(w http.ResponseWriter, r *http.Request) {
	type entry struct {
		GuideNumber string `json:"GuideNumber"`
		GuideName   string `json:"GuideName"`
		URL         string `json:"URL"`
		HD          int    `json:"HD,omitempty"`
		VideoCodec  string `json:"VideoCodec,omitempty"`
		AudioCodec  string `json:"AudioCodec,omitempty"`
	}
	chans, err := h.Lineup(r.Context())
	if err != nil {
		http.Error(w, "lineup error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	out := make([]entry, 0, len(chans))
	for _, c := range chans {
		out = append(out, entry{
			GuideNumber: c.GuideNumber,
			GuideName:   c.GuideName,
			URL:         c.URL,
			HD:          c.HD,
			VideoCodec:  c.VideoCodec,
			AudioCodec:  c.AudioCodec,
		})
	}
	writeJSON(w, out)
}

// LineupXML serves /lineup.xml — same payload as JSON, XML-encoded.
func (h *Handlers) LineupXML(w http.ResponseWriter, r *http.Request) {
	type program struct {
		XMLName     xml.Name `xml:"Program"`
		GuideNumber string   `xml:"GuideNumber"`
		GuideName   string   `xml:"GuideName"`
		URL         string   `xml:"URL"`
	}
	type lineup struct {
		XMLName  xml.Name  `xml:"Lineup"`
		Programs []program `xml:",any"`
	}
	chans, err := h.Lineup(r.Context())
	if err != nil {
		http.Error(w, "lineup error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	out := lineup{}
	for _, c := range chans {
		out.Programs = append(out.Programs, program{
			GuideNumber: c.GuideNumber,
			GuideName:   c.GuideName,
			URL:         c.URL,
		})
	}
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	_, _ = w.Write([]byte(xml.Header))
	_ = xml.NewEncoder(w).Encode(out)
}

// LineupStatus serves /lineup_status.json. We never scan, so always Idle.
func (h *Handlers) LineupStatus(w http.ResponseWriter, r *http.Request) {
	type status struct {
		ScanInProgress int      `json:"ScanInProgress"`
		ScanPossible   int      `json:"ScanPossible"`
		Source         string   `json:"Source"`
		SourceList     []string `json:"SourceList"`
	}
	writeJSON(w, status{
		ScanInProgress: 0,
		ScanPossible:   1,
		Source:         "Cable",
		SourceList:     []string{"Cable"},
	})
}

// DeviceXML serves /device.xml — UPnP descriptor used by some Plex versions.
func (h *Handlers) DeviceXML(w http.ResponseWriter, r *http.Request) {
	body := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<root xmlns="urn:schemas-upnp-org:device-1-0">
  <specVersion><major>1</major><minor>0</minor></specVersion>
  <URLBase>%s</URLBase>
  <device>
    <deviceType>urn:schemas-upnp-org:device:MediaServer:1</deviceType>
    <friendlyName>%s</friendlyName>
    <manufacturer>Conductor</manufacturer>
    <modelName>%s</modelName>
    <modelNumber>%s</modelNumber>
    <serialNumber>%s</serialNumber>
    <UDN>uuid:%s</UDN>
  </device>
</root>`, h.Device.BaseURL, h.Device.FriendlyName, h.Device.ModelNumber, h.Device.ModelNumber, h.Device.DeviceID, h.Device.DeviceID)
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	_, _ = w.Write([]byte(body))
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(v)
}
