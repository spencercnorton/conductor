package sd_test

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/spencercnorton/conductor/internal/sd"
)

// fakeSD spins up a Schedules Direct stand-in. Tracks whether handlers
// were called and asserts the SHA-1 password handshake.
type fakeSD struct {
	srv              *httptest.Server
	loginCalls       atomic.Int32
	statusCalls      atomic.Int32
	lineupCalls      atomic.Int32
	scheduleMD5Calls atomic.Int32
	scheduleCalls    atomic.Int32
	programsCalls    atomic.Int32
	md5RequestsMu    sync.Mutex
	md5Requests      [][]sd.ScheduleRequest

	expectUsername string
	expectPwSHA1   string
}

func newFakeSD(t *testing.T, username, password string) *fakeSD {
	t.Helper()
	h := sha1.Sum([]byte(password))
	f := &fakeSD{
		expectUsername: username,
		expectPwSHA1:   hex.EncodeToString(h[:]),
	}
	mux := http.NewServeMux()

	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		f.loginCalls.Add(1)
		body, _ := io.ReadAll(r.Body)
		var got struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}
		if err := json.Unmarshal(body, &got); err != nil {
			w.WriteHeader(400)
			return
		}
		if got.Username != f.expectUsername || got.Password != f.expectPwSHA1 {
			w.WriteHeader(401)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": 4003, "message": "invalid credentials",
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 0, "token": "tok-abc-123",
		})
	})

	check := func(w http.ResponseWriter, r *http.Request) bool {
		if r.Header.Get("token") != "tok-abc-123" {
			w.WriteHeader(401)
			return false
		}
		return true
	}

	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		if !check(w, r) {
			return
		}
		f.statusCalls.Add(1)
		_, _ = w.Write([]byte(`{
			"code":0, "account":{"expires":"2027-05-06","maxLineups":4},
			"lineups":[{"lineup":"USA-OTA-80127","modified":"2026-05-06T12:00:00Z","uri":"/lineups/USA-OTA-80127","name":"Denver OTA"}]
		}`))
	})

	mux.HandleFunc("/lineups/USA-OTA-80127", func(w http.ResponseWriter, r *http.Request) {
		if !check(w, r) {
			return
		}
		f.lineupCalls.Add(1)
		_, _ = w.Write([]byte(`{
			"stations":[{"stationID":"66442","name":"KMGH-DT","callsign":"KMGH"}],
			"map":[{"stationID":"66442","channel":"7.1"}],
			"metadata":{"lineup":"USA-OTA-80127","modified":"2026-05-06T12:00:00Z"}
		}`))
	})

	mux.HandleFunc("/schedules", func(w http.ResponseWriter, r *http.Request) {
		if !check(w, r) {
			return
		}
		f.scheduleCalls.Add(1)
		// Simulate one program on station 66442.
		_, _ = w.Write([]byte(`[
			{
				"stationID":"66442",
				"programs":[
					{"programID":"EP012345670001","airDateTime":"2026-05-09T01:00:00Z","duration":3600,"md5":"prog-md5-abc","new":true,"premiere":true,"liveTapeDelay":"Live","isPremiereOrFinale":"Season Premiere"}
				],
				"metadata":{"modified":"2026-05-06T12:00:00Z","md5":"sched-md5-xyz","startDate":"2026-05-09"}
			}
		]`))
	})

	mux.HandleFunc("/schedules/md5", func(w http.ResponseWriter, r *http.Request) {
		if !check(w, r) {
			return
		}
		f.scheduleMD5Calls.Add(1)
		// Keep the fake's wire schema independent of ScheduleRequest so a future
		// tag/body regression cannot make the production encoder and fake agree on
		// the same invalid shape. API-20141201 documents stationID objects with an
		// optional singular "date" array for this endpoint.
		var wireReqs []struct {
			StationID string   `json:"stationID"`
			Dates     []string `json:"date,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&wireReqs); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		reqs := make([]sd.ScheduleRequest, 0, len(wireReqs))
		for _, req := range wireReqs {
			reqs = append(reqs, sd.ScheduleRequest{StationID: req.StationID, Dates: req.Dates})
		}
		f.md5RequestsMu.Lock()
		f.md5Requests = append(f.md5Requests, append([]sd.ScheduleRequest(nil), reqs...))
		f.md5RequestsMu.Unlock()
		manifest := make(map[string]any, len(reqs))
		for _, req := range reqs {
			dates := req.Dates
			if len(dates) == 0 {
				dates = []string{"2026-05-09"}
			}
			entries := make(map[string]any, len(dates))
			for _, date := range dates {
				entries[date] = map[string]any{
					"code": 0, "message": "OK", "lastModified": "2026-05-06T12:00:00Z", "md5": "sched-md5-xyz",
				}
			}
			manifest[req.StationID] = entries
		}
		_ = json.NewEncoder(w).Encode(manifest)
	})

	mux.HandleFunc("/programs", func(w http.ResponseWriter, r *http.Request) {
		if !check(w, r) {
			return
		}
		f.programsCalls.Add(1)
		_, _ = w.Write([]byte(`[
			{
				"programID":"EP012345670001",
				"md5":"prog-md5-abc",
				"titles":[{"title120":"The Bear"}],
				"descriptions":{
					"description1000":[{"descriptionLanguage":"en","description":"Carmy struggles with the launch."}]
				},
				"originalAirDate":"2026-05-09",
				"genres":["Series","Drama"],
				"episodeTitle150":"System"
			}
		]`))
	})

	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

func TestSDLoginHashesPasswordCorrectly(t *testing.T) {
	f := newFakeSD(t, "sd-user", "sd-pass")
	c := sd.New(f.srv.URL, "sd-user", "sd-pass")

	st, err := c.Status(context.Background())
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if st.Code != 0 {
		t.Errorf("code: got %d", st.Code)
	}
	if len(st.Lineups) != 1 || st.Lineups[0].Lineup != "USA-OTA-80127" {
		t.Errorf("lineups: %+v", st.Lineups)
	}
	if f.loginCalls.Load() != 1 {
		t.Errorf("expected 1 login; got %d", f.loginCalls.Load())
	}
}

func TestSDReusesToken(t *testing.T) {
	f := newFakeSD(t, "u", "p")
	c := sd.New(f.srv.URL, "u", "p")
	for i := 0; i < 5; i++ {
		if _, err := c.Status(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if f.loginCalls.Load() != 1 {
		t.Errorf("token reuse: got %d login calls", f.loginCalls.Load())
	}
	if f.statusCalls.Load() != 5 {
		t.Errorf("expected 5 /status calls; got %d", f.statusCalls.Load())
	}
}

func TestSDGetLineupAndSchedules(t *testing.T) {
	f := newFakeSD(t, "u", "p")
	c := sd.New(f.srv.URL, "u", "p")

	lineup, err := c.GetLineup(context.Background(), "USA-OTA-80127")
	if err != nil {
		t.Fatal(err)
	}
	if len(lineup.Stations) != 1 {
		t.Errorf("stations: %d", len(lineup.Stations))
	}
	if lineup.Stations[0].StationID != "66442" {
		t.Errorf("stationID: %q", lineup.Stations[0].StationID)
	}

	manifest, err := c.ScheduleMD5(context.Background(), []sd.ScheduleRequest{{StationID: "66442"}})
	if err != nil {
		t.Fatal(err)
	}
	if manifest["66442"]["2026-05-09"].MD5 != "sched-md5-xyz" {
		t.Fatalf("schedule manifest: %+v", manifest)
	}

	scheds, err := c.Schedules(context.Background(), []sd.ScheduleRequest{{StationID: "66442", Dates: []string{"2026-05-09"}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(scheds) != 1 {
		t.Fatalf("scheds: %d", len(scheds))
	}
	if len(scheds[0].Programs) != 1 {
		t.Errorf("programs: %d", len(scheds[0].Programs))
	}
	if scheds[0].Metadata.MD5 != "sched-md5-xyz" {
		t.Errorf("md5: %q", scheds[0].Metadata.MD5)
	}
	if !scheds[0].Programs[0].Premiere || scheds[0].Programs[0].LiveTapeDelay != "Live" ||
		scheds[0].Programs[0].PremiereOrFinale != "Season Premiere" {
		t.Errorf("schedule flags: %+v", scheds[0].Programs[0])
	}

	progs, err := c.Programs(context.Background(), []string{"EP012345670001"})
	if err != nil {
		t.Fatal(err)
	}
	if len(progs) != 1 {
		t.Fatalf("progs: %d", len(progs))
	}
	if progs[0].Titles[0].Title120 != "The Bear" {
		t.Errorf("title: %q", progs[0].Titles[0].Title120)
	}

	if !strings.Contains(progs[0].Descriptions.Description1000[0].Description, "Carmy") {
		t.Errorf("description not propagated: %+v", progs[0].Descriptions)
	}
}

func TestScheduleMD5UsesDocumentedObjectSchemaAndChunks(t *testing.T) {
	f := newFakeSD(t, "u", "p")
	c := sd.New(f.srv.URL, "u", "p")
	reqs := make([]sd.ScheduleRequest, 0, 5001)
	for index := 0; index < 5001; index++ {
		req := sd.ScheduleRequest{StationID: fmt.Sprintf("station-%04d", index)}
		if index == 0 {
			req.Dates = []string{"2026-05-08"}
		}
		reqs = append(reqs, req)
	}

	manifest, err := c.ScheduleMD5(context.Background(), reqs)
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest) != 5001 {
		t.Fatalf("manifest stations = %d, want 5001", len(manifest))
	}
	f.md5RequestsMu.Lock()
	requests := append([][]sd.ScheduleRequest(nil), f.md5Requests...)
	f.md5RequestsMu.Unlock()
	if len(requests) != 2 {
		t.Fatalf("MD5 request chunk count = %d, want 2", len(requests))
	}
	if len(requests[0]) != 5000 || len(requests[1]) != 1 {
		t.Fatalf("MD5 request chunks = %v", []int{len(requests[0]), len(requests[1])})
	}
	if requests[0][0].StationID != "station-0000" || len(requests[0][0].Dates) != 1 ||
		requests[0][0].Dates[0] != "2026-05-08" || requests[0][4999].StationID != "station-4999" ||
		requests[1][0].StationID != "station-5000" {
		t.Fatalf("MD5 request object schema/chunk order = first %+v last-first %+v second %+v",
			requests[0][0], requests[0][4999], requests[1][0])
	}
}

func TestSDLoginFailsWithWrongPassword(t *testing.T) {
	f := newFakeSD(t, "u", "right-password")
	c := sd.New(f.srv.URL, "u", "wrong-password")
	_, err := c.Status(context.Background())
	if err == nil {
		t.Error("expected error from wrong password")
	}
}

func TestProgramDetailsEpisodeSeasonNum(t *testing.T) {
	decode := func(metaJSON string) sd.ProgramDetails {
		var pd sd.ProgramDetails
		if err := json.Unmarshal([]byte(`{"programID":"EP000000010001","metadata":`+metaJSON+`}`), &pd); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		return pd
	}
	cases := []struct {
		name         string
		meta         string
		wantS, wantE int
	}{
		{"gracenote", `[{"Gracenote":{"season":3,"episode":7}}]`, 3, 7},
		{"episodeNum alias", `[{"Tribune":{"season":5,"episodeNum":23}}]`, 5, 23},
		{"prefers gracenote", `[{"Other":{"season":1,"episode":1}},{"Gracenote":{"season":4,"episode":9}}]`, 4, 9},
		{"no episode metadata (movie/special)", `[]`, 0, 0},
		{"season only is not enough", `[{"Gracenote":{"season":2}}]`, 0, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, e := decode(c.meta).EpisodeSeasonNum()
			if s != c.wantS || e != c.wantE {
				t.Fatalf("got S%02dE%02d, want S%02dE%02d", s, e, c.wantS, c.wantE)
			}
		})
	}
}
