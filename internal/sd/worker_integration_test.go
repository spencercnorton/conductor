//go:build !short

package sd_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/spencercnorton/conductor/internal/sd"
	"github.com/spencercnorton/conductor/internal/store"
)

const (
	sdPGImage     = "postgres:16-alpine"
	sdPGContainer = "conductor-sd-test-pg"
	sdPGPort      = "55449"
	sdPGPassword  = "conductor-test"
)

func freshSDIntegrationDB(t *testing.T) *store.DB {
	t.Helper()
	if os.Getenv("CONDUCTOR_INT_TEST") != "1" {
		t.Skip("integration test skipped (set CONDUCTOR_INT_TEST=1 to enable)")
	}
	dsn := os.Getenv("CONDUCTOR_TEST_DSN")
	if dsn == "" {
		if _, err := exec.LookPath("docker"); err != nil {
			t.Skip("docker not available and CONDUCTOR_TEST_DSN not set")
		}
		_ = exec.Command("docker", "rm", "-f", "-v", sdPGContainer).Run()
		out, err := exec.Command("docker", "run", "-d",
			"--name", sdPGContainer,
			"-p", sdPGPort+":5432",
			"-e", "POSTGRES_PASSWORD="+sdPGPassword,
			"-e", "POSTGRES_DB=conductor_sd_test",
			sdPGImage).CombinedOutput()
		if err != nil {
			t.Fatalf("docker run: %v: %s", err, out)
		}
		t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", "-v", sdPGContainer).Run() })
		dsn = fmt.Sprintf("postgres://postgres:%s@127.0.0.1:%s/conductor_sd_test?sslmode=disable", sdPGPassword, sdPGPort)
		deadline := time.Now().Add(30 * time.Second)
		for {
			if time.Now().After(deadline) {
				t.Fatal("postgres container did not become ready in time")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			conn, err := pgx.Connect(ctx, dsn)
			cancel()
			if err == nil {
				_ = conn.Close(context.Background())
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
	db, err := store.Open(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if err := db.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	return db
}

type workerSDFeed struct {
	mu                 sync.Mutex
	stationID          string
	programID          string
	md5                string
	title              string
	start              time.Time
	scheduleDate       string
	duration           int
	live               bool
	newAiring          bool
	empty              bool
	scheduleCode       int
	programCode        int
	programMessage     string
	originalAirDate    string
	programMD5Override string

	pauseFirstSchedule bool
	scheduleStarted    chan struct{}
	releaseSchedule    chan struct{}
	releaseOnce        sync.Once
	lineupCalls        atomic.Int32
	md5Calls           atomic.Int32
	scheduleCalls      atomic.Int32
	programCalls       atomic.Int32
}

func newWorkerSDFeed(start time.Time, pause bool) *workerSDFeed {
	return &workerSDFeed{
		stationID: "FX475-OLD", programID: "EP047500000001",
		md5: "old-schedule-md5", title: "Old SD mapping programme", start: start,
		scheduleDate:       time.Now().UTC().Format("2006-01-02"),
		duration:           3600,
		pauseFirstSchedule: pause,
		scheduleStarted:    make(chan struct{}),
		releaseSchedule:    make(chan struct{}),
	}
}

func (f *workerSDFeed) release() { f.releaseOnce.Do(func() { close(f.releaseSchedule) }) }

func (f *workerSDFeed) set(stationID, programID, md5, title string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stationID, f.programID, f.md5, f.title = stationID, programID, md5, title
	f.empty = false
}

func (f *workerSDFeed) setProgram(md5, title string, start time.Time, duration int, live, newAiring bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.md5, f.title, f.start, f.duration = md5, title, start, duration
	f.live, f.newAiring, f.empty = live, newAiring, false
}

func (f *workerSDFeed) setEmpty(md5 string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.md5, f.empty = md5, true
}

func (f *workerSDFeed) setScheduleCode(code int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.scheduleCode = code
}

func (f *workerSDFeed) setProgramError(code int, message string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.programCode, f.programMessage = code, message
}

func (f *workerSDFeed) setOriginalAirDate(value string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.originalAirDate = value
}

func (f *workerSDFeed) setProgramMD5Override(value string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.programMD5Override = value
}

func (f *workerSDFeed) snapshot() (stationID, programID, md5, title string, start time.Time, duration int, live, newAiring, empty bool, scheduleCode, programCode int, programMessage string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stationID, f.programID, f.md5, f.title, f.start, f.duration, f.live, f.newAiring, f.empty,
		f.scheduleCode, f.programCode, f.programMessage
}

func (f *workerSDFeed) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/token" {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "token": "tok-worker"})
		return
	}
	if r.Header.Get("token") != "tok-worker" {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	stationID, programID, md5, title, start, duration, live, newAiring, empty,
		scheduleCode, programCode, programMessage := f.snapshot()
	switch r.URL.Path {
	case "/lineups/FX475-LINEUP":
		f.lineupCalls.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"stations": []map[string]any{{"stationID": stationID, "name": title, "callsign": stationID}},
			"map":      []map[string]any{{"stationID": stationID, "channel": "47.5"}},
			"metadata": map[string]any{"lineup": "FX475-LINEUP", "modified": "2026-08-22T12:00:00Z"},
		})
	case "/schedules/md5":
		f.md5Calls.Add(1)
		var reqs []sd.ScheduleRequest
		if err := json.NewDecoder(r.Body).Decode(&reqs); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		manifest := map[string]map[string]map[string]any{}
		for _, req := range reqs {
			dates := req.Dates
			if len(dates) == 0 {
				dates = []string{f.scheduleDate}
			}
			entries := map[string]map[string]any{}
			for _, date := range dates {
				entries[date] = map[string]any{
					"code": 0, "message": "OK", "lastModified": "2026-08-22T12:00:00Z", "md5": md5,
				}
			}
			manifest[req.StationID] = entries
		}
		_ = json.NewEncoder(w).Encode(manifest)
	case "/schedules":
		call := f.scheduleCalls.Add(1)
		if f.pauseFirstSchedule && call == 1 {
			close(f.scheduleStarted)
			<-f.releaseSchedule
			stationID, programID, md5, _, start, duration, live, newAiring, empty,
				scheduleCode, programCode, programMessage = f.snapshot()
		}
		programs := []map[string]any{}
		if !empty {
			airing := map[string]any{
				"programID": programID, "airDateTime": start.Format(time.RFC3339),
				"duration": duration, "md5": "airing-md5", "new": newAiring,
			}
			if live {
				airing["liveTapeDelay"] = "Live"
			}
			programs = append(programs, airing)
		}
		currentDate, err := time.Parse("2006-01-02", f.scheduleDate)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		previousDate := currentDate.AddDate(0, 0, -1).Format("2006-01-02")
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{
				"stationID": stationID,
				"programs": []map[string]any{{
					"programID": "EP000000000000", "airDateTime": previousDate + "T00:00:00Z",
					"duration": 60, "md5": "expired-prior-day-program-md5",
				}},
				"metadata": map[string]any{
					"modified": "2026-08-22T12:00:00Z", "md5": md5, "startDate": previousDate,
				},
			},
			{
				"stationID": stationID,
				"programs":  programs,
				"metadata": map[string]any{
					"modified": "2026-08-22T12:00:00Z", "md5": md5,
					"startDate": f.scheduleDate, "code": scheduleCode,
				},
			},
		})
	case "/programs":
		f.programCalls.Add(1)
		f.mu.Lock()
		originalAirDate := f.originalAirDate
		programMD5Override := f.programMD5Override
		f.mu.Unlock()
		details := map[string]any{
			"programID": programID,
			"md5":       "airing-md5",
			"titles":    []map[string]string{{"title120": title}},
			"descriptions": map[string]any{
				"description1000": []map[string]string{{"descriptionLanguage": "en", "description": title + " description"}},
			},
			"genres": []string{"Series"},
		}
		if programCode != 0 {
			details["code"] = programCode
			details["message"] = programMessage
		}
		if originalAirDate != "" {
			details["originalAirDate"] = originalAirDate
		}
		if programMD5Override == "__omit__" {
			delete(details, "md5")
		} else if programMD5Override != "" {
			details["md5"] = programMD5Override
		}
		_ = json.NewEncoder(w).Encode([]map[string]any{details})
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func setupSDWorkerIntegration(t *testing.T, pause bool, channelNumber float64) (*store.DB, *workerSDFeed, *sd.Ingester, store.Channel) {
	t.Helper()
	db := freshSDIntegrationDB(t)
	ctx := context.Background()
	start := time.Now().UTC().Truncate(time.Minute).Add(48 * time.Hour)
	feed := newWorkerSDFeed(start, pause)
	server := httptest.NewServer(feed)
	t.Cleanup(func() {
		feed.release()
		server.Close()
	})
	if _, err := db.CreateSDLineup(ctx, "FX475-LINEUP", "FX475 lineup", "test", true); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool.Exec(ctx, `DELETE FROM sd_station_state WHERE sd_lineup_id='FX475-LINEUP'`); err != nil {
		t.Fatal(err)
	}
	channel, err := db.CreateChannel(ctx, store.Channel{
		Number: channelNumber, Name: "FX475 SD", CallSign: "FX475SD",
		EpgChannelID: "FX475-OLD", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	client := sd.New(server.URL, "sd-user", "password")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return db, feed, sd.NewIngester(logger, client, db, time.Hour), channel
}

func TestIntegrationSDCurrentLineupRetainsRemovedStationUntilRetainedAuthorityValidates(t *testing.T) {
	db := freshSDIntegrationDB(t)
	ctx := context.Background()
	const (
		lineupID         = "FX475-REMOVED-STATION-LINEUP"
		retainedStation  = "FX475-RETAINED-STATION"
		removedStation   = "FX475-REMOVED-STATION"
		retainedDailyMD5 = "retained-today-md5"
		retainedPriorMD5 = "retained-prior-md5"
	)
	today := time.Now().UTC().Format("2006-01-02")
	todayDate, err := time.Parse("2006-01-02", today)
	if err != nil {
		t.Fatal(err)
	}
	prior := todayDate.AddDate(0, 0, -1).Format("2006-01-02")
	watermarkHash := sha256.New()
	for _, part := range []string{prior, retainedPriorMD5, today, retainedDailyMD5} {
		_, _ = watermarkHash.Write([]byte(part))
		_, _ = watermarkHash.Write([]byte{0})
	}
	retainedWatermark := "sha256:" + hex.EncodeToString(watermarkHash.Sum(nil))
	start := time.Now().UTC().Truncate(time.Minute).Add(48 * time.Hour)
	var incompleteSchedules atomic.Bool
	incompleteSchedules.Store(true)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/token" {
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "token": "tok-lineup-removal"})
			return
		}
		if r.Header.Get("token") != "tok-lineup-removal" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/lineups/" + lineupID:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"stations": []map[string]any{{"stationID": retainedStation, "name": "Retained", "callsign": retainedStation}},
				"map":      []map[string]any{{"stationID": retainedStation, "channel": "47.91"}},
				"metadata": map[string]any{"lineup": lineupID},
			})
		case "/schedules/md5":
			var reqs []sd.ScheduleRequest
			if err := json.NewDecoder(r.Body).Decode(&reqs); err != nil || len(reqs) != 1 || reqs[0].StationID != retainedStation {
				t.Errorf("removed-station MD5 request = %+v err %v", reqs, err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			dates := reqs[0].Dates
			md5 := retainedDailyMD5
			if len(dates) == 0 {
				dates = []string{today}
			} else if len(dates) == 1 && dates[0] == prior {
				md5 = retainedPriorMD5
			} else {
				t.Errorf("removed-station explicit MD5 dates = %v, want [%s]", dates, prior)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				retainedStation: map[string]any{
					dates[0]: map[string]any{"code": 0, "message": "OK", "md5": md5},
				},
			})
		case "/schedules":
			var reqs []sd.ScheduleRequest
			if err := json.NewDecoder(r.Body).Decode(&reqs); err != nil || len(reqs) != 1 ||
				reqs[0].StationID != retainedStation || len(reqs[0].Dates) != 2 {
				t.Errorf("removed-station schedule request = %+v err %v", reqs, err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			responses := []map[string]any{
				{
					"stationID": retainedStation,
					"programs": []map[string]any{{
						"programID": "EP047500009100", "airDateTime": prior + "T00:00:00Z",
						"duration": 60, "md5": "expired-retained-airing-md5",
					}},
					"metadata": map[string]any{
						"modified": "2026-08-22T12:00:00Z", "md5": retainedPriorMD5,
						"startDate": prior, "code": 0,
					},
				},
			}
			if !incompleteSchedules.Load() {
				responses = append(responses, map[string]any{
					"stationID": retainedStation,
					"programs": []map[string]any{{
						"programID": "EP047500009101", "airDateTime": start.Format(time.RFC3339),
						"duration": 3600, "md5": "retained-airing-md5",
					}},
					"metadata": map[string]any{
						"modified": "2026-08-22T12:00:00Z", "md5": retainedDailyMD5,
						"startDate": today, "code": 0,
					},
				})
			}
			_ = json.NewEncoder(w).Encode(responses)
		case "/programs":
			_ = json.NewEncoder(w).Encode([]map[string]any{{
				"programID": "EP047500009101", "md5": "retained-airing-md5",
				"titles": []map[string]string{{"title120": "Retained lineup programme"}},
			}})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)

	if _, err := db.CreateSDLineup(ctx, lineupID, "FX475 lineup removal", "test", true); err != nil {
		t.Fatal(err)
	}
	retainedChannel, err := db.CreateChannel(ctx, store.Channel{
		Number: 9476.1, Name: "Retained SD station", CallSign: retainedStation,
		EpgChannelID: retainedStation, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	removedChannel, err := db.CreateChannel(ctx, store.Channel{
		Number: 9476.2, Name: "Removed SD station", CallSign: removedStation,
		EpgChannelID: removedStation, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	retainedProgram := store.EPGProgram{
		ChannelID: retainedChannel.ID, StartAt: start, EndAt: start.Add(time.Hour),
		Title: "Retained lineup programme", SourceHash: "sd:retained-lineup-programme", SourcePriority: store.PrioritySD,
	}
	removedProgram := store.EPGProgram{
		ChannelID: removedChannel.ID, StartAt: start, EndAt: start.Add(time.Hour),
		Title: "Removed lineup programme", SourceHash: "sd:removed-lineup-programme", SourcePriority: store.PrioritySD,
	}
	if _, err := db.ReplaceSDStationSnapshot(ctx, lineupID, retainedStation, "seed-retained", retainedChannel.ID, []store.EPGProgram{retainedProgram}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ReplaceSDStationSnapshot(ctx, lineupID, removedStation, "seed-removed", removedChannel.ID, []store.EPGProgram{removedProgram}); err != nil {
		t.Fatal(err)
	}
	const seedRetainedWatermark = "seed-retained-before-complete-authority"
	if _, err := db.Pool.Exec(ctx, `
		UPDATE sd_station_state SET last_md5=$3
		 WHERE sd_lineup_id=$1 AND sd_station_id=$2`, lineupID, retainedStation, seedRetainedWatermark); err != nil {
		t.Fatal(err)
	}

	client := sd.New(server.URL, "sd-user", "password")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ingester := sd.NewIngester(logger, client, db, time.Hour)
	failed := ingester.RunOnce(ctx)
	if failed.LineupsFailed != 1 || failed.LineupsOK != 0 || failed.StationsTotal != 1 ||
		failed.StationsWithdrawn != 0 || failed.ProgramsDeleted != 0 {
		t.Fatalf("incomplete retained-station stats = %+v", failed)
	}
	var preservedRemovedState, preservedRemovedRows, preservedRetainedState, preservedRetainedRows int
	var preservedRetainedMD5 string
	if err := db.Pool.QueryRow(ctx, `
		SELECT count(*) FROM sd_station_state
		 WHERE sd_lineup_id=$1 AND sd_station_id=$2`, lineupID, removedStation).Scan(&preservedRemovedState); err != nil {
		t.Fatal(err)
	}
	if err := db.Pool.QueryRow(ctx, `
		SELECT count(*) FROM epg_program
		 WHERE channel_id=$1 AND source_hash LIKE 'sd:%'`, removedChannel.ID).Scan(&preservedRemovedRows); err != nil {
		t.Fatal(err)
	}
	if err := db.Pool.QueryRow(ctx, `
		SELECT count(*), min(last_md5) FROM sd_station_state
		 WHERE sd_lineup_id=$1 AND sd_station_id=$2`, lineupID, retainedStation).Scan(&preservedRetainedState, &preservedRetainedMD5); err != nil {
		t.Fatal(err)
	}
	if err := db.Pool.QueryRow(ctx, `
		SELECT count(*) FROM epg_program
		 WHERE channel_id=$1 AND source_hash=$2`, retainedChannel.ID, retainedProgram.SourceHash).Scan(&preservedRetainedRows); err != nil {
		t.Fatal(err)
	}
	if preservedRemovedState != 1 || preservedRemovedRows != 1 || preservedRetainedState != 1 ||
		preservedRetainedRows != 1 || preservedRetainedMD5 != seedRetainedWatermark {
		t.Fatalf("incomplete authority changed last-good lineup: removed state/rows %d/%d retained state/rows/md5 %d/%d/%q",
			preservedRemovedState, preservedRemovedRows, preservedRetainedState, preservedRetainedRows, preservedRetainedMD5)
	}

	incompleteSchedules.Store(false)
	stats := ingester.RunOnce(ctx)
	if stats.LineupsOK != 1 || stats.LineupsFailed != 0 || stats.StationsTotal != 1 ||
		stats.StationsWithdrawn != 1 || stats.StationsFetched != 1 {
		t.Fatalf("removed-station lineup stats = %+v", stats)
	}

	var removedState, removedRows, retainedState, retainedRows int
	var durableRetainedMD5 string
	if err := db.Pool.QueryRow(ctx, `
		SELECT count(*) FROM sd_station_state
		 WHERE sd_lineup_id=$1 AND sd_station_id=$2`, lineupID, removedStation).Scan(&removedState); err != nil {
		t.Fatal(err)
	}
	if err := db.Pool.QueryRow(ctx, `
		SELECT count(*) FROM epg_program
		 WHERE channel_id=$1 AND source_hash LIKE 'sd:%'`, removedChannel.ID).Scan(&removedRows); err != nil {
		t.Fatal(err)
	}
	if err := db.Pool.QueryRow(ctx, `
		SELECT count(*), min(last_md5) FROM sd_station_state
		 WHERE sd_lineup_id=$1 AND sd_station_id=$2`, lineupID, retainedStation).Scan(&retainedState, &durableRetainedMD5); err != nil {
		t.Fatal(err)
	}
	if err := db.Pool.QueryRow(ctx, `
		SELECT count(*) FROM epg_program
		 WHERE channel_id=$1 AND title=$2`, retainedChannel.ID, retainedProgram.Title).Scan(&retainedRows); err != nil {
		t.Fatal(err)
	}
	if removedState != 0 || removedRows != 0 || retainedState != 1 || retainedRows != 1 || durableRetainedMD5 != retainedWatermark {
		t.Fatalf("removed/retained lineup state = removed state/rows %d/%d retained state/rows/md5 %d/%d/%q",
			removedState, removedRows, retainedState, retainedRows, durableRetainedMD5)
	}
}

func TestIntegrationSDMalformedLineupRetainsLastGoodAndExplicitEmptyWithdraws(t *testing.T) {
	db := freshSDIntegrationDB(t)
	ctx := context.Background()
	type lineupCase struct {
		name       string
		lineupID   string
		stationID  string
		response   map[string]any
		validEmpty bool
	}
	cases := []lineupCase{
		{name: "empty-object", lineupID: "FX475-LINEUP-EMPTY-OBJECT", stationID: "FX475-STATION-EMPTY-OBJECT"},
		{name: "wrong-identity", lineupID: "FX475-LINEUP-WRONG-IDENTITY", stationID: "FX475-STATION-WRONG-IDENTITY"},
		{name: "omitted-stations", lineupID: "FX475-LINEUP-OMITTED-STATIONS", stationID: "FX475-STATION-OMITTED-STATIONS"},
		{name: "omitted-map", lineupID: "FX475-LINEUP-OMITTED-MAP", stationID: "FX475-STATION-OMITTED-MAP"},
		{name: "set-mismatch", lineupID: "FX475-LINEUP-SET-MISMATCH", stationID: "FX475-STATION-SET-MISMATCH"},
		{name: "valid-explicit-empty", lineupID: "FX475-LINEUP-VALID-EMPTY", stationID: "FX475-STATION-VALID-EMPTY", validEmpty: true},
	}
	stationArray := func(stationID string) []map[string]any {
		return []map[string]any{{"stationID": stationID, "name": stationID, "callsign": stationID}}
	}
	mapArray := func(stationID string) []map[string]any {
		return []map[string]any{{"stationID": stationID, "channel": "47.5"}}
	}
	for index := range cases {
		test := &cases[index]
		switch test.name {
		case "empty-object":
			test.response = map[string]any{}
		case "wrong-identity":
			test.response = map[string]any{
				"stations": stationArray(test.stationID), "map": mapArray(test.stationID),
				"metadata": map[string]any{"lineup": "FX475-WRONG-LINEUP"},
			}
		case "omitted-stations":
			test.response = map[string]any{
				"map": mapArray(test.stationID), "metadata": map[string]any{"lineup": test.lineupID},
			}
		case "omitted-map":
			test.response = map[string]any{
				"stations": stationArray(test.stationID), "metadata": map[string]any{"lineup": test.lineupID},
			}
		case "set-mismatch":
			test.response = map[string]any{
				"stations": stationArray(test.stationID), "map": mapArray(test.stationID + "-OTHER"),
				"metadata": map[string]any{"lineup": test.lineupID},
			}
		case "valid-explicit-empty":
			test.response = map[string]any{
				"stations": []map[string]any{}, "map": []map[string]any{},
				"metadata": map[string]any{"lineup": test.lineupID},
			}
		}
	}

	var scheduleMD5Calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/token" {
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "token": "tok-lineup-valid"})
			return
		}
		if r.Header.Get("token") != "tok-lineup-valid" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.URL.Path == "/schedules/md5" {
			scheduleMD5Calls.Add(1)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		for _, test := range cases {
			if r.URL.Path == "/lineups/"+test.lineupID {
				_ = json.NewEncoder(w).Encode(test.response)
				return
			}
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(server.Close)

	channels := make(map[string]store.Channel, len(cases))
	for index, test := range cases {
		if _, err := db.CreateSDLineup(ctx, test.lineupID, fmt.Sprintf("%02d %s", index, test.name), "test", true); err != nil {
			t.Fatal(err)
		}
		channel, err := db.CreateChannel(ctx, store.Channel{
			Number: 9480 + float64(index)/10, Name: test.name, CallSign: test.stationID,
			EpgChannelID: test.stationID, Enabled: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		channels[test.name] = channel
	}
	start := time.Now().UTC().Truncate(time.Minute).Add(72 * time.Hour)
	for index, test := range cases {
		channel := channels[test.name]
		programStart := start.Add(time.Duration(index) * 2 * time.Hour)
		if _, err := db.ReplaceSDStationSnapshot(ctx, test.lineupID, test.stationID, "last-good-"+test.name,
			channel.ID, []store.EPGProgram{{
				ChannelID: channel.ID, StartAt: programStart, EndAt: programStart.Add(time.Hour),
				Title: "Last good " + test.name, SourceHash: "sd:" + test.name, SourcePriority: store.PrioritySD,
			}}); err != nil {
			t.Fatal(err)
		}
	}

	client := sd.New(server.URL, "sd-user", "password")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	stats := sd.NewIngester(logger, client, db, time.Hour).RunOnce(ctx)
	if stats.LineupsAttempted != len(cases) || stats.LineupsOK != 1 || stats.LineupsFailed != len(cases)-1 ||
		stats.StationsWithdrawn != 1 || stats.ProgramsDeleted != 1 || scheduleMD5Calls.Load() != 0 {
		t.Fatalf("lineup authority stats = %+v MD5 calls=%d", stats, scheduleMD5Calls.Load())
	}
	for _, test := range cases {
		var stateCount, programCount int
		var lastMD5 string
		if err := db.Pool.QueryRow(ctx, `
			SELECT count(*), COALESCE(min(last_md5), '')
			  FROM sd_station_state
			 WHERE sd_lineup_id=$1 AND sd_station_id=$2`, test.lineupID, test.stationID).Scan(&stateCount, &lastMD5); err != nil {
			t.Fatal(err)
		}
		if err := db.Pool.QueryRow(ctx, `
			SELECT count(*) FROM epg_program
			 WHERE channel_id=$1 AND source_hash=$2`, channels[test.name].ID, "sd:"+test.name).Scan(&programCount); err != nil {
			t.Fatal(err)
		}
		if test.validEmpty {
			if stateCount != 0 || programCount != 0 {
				t.Errorf("valid empty %s retained state/program = %d/%d", test.name, stateCount, programCount)
			}
			continue
		}
		if stateCount != 1 || programCount != 1 || lastMD5 != "last-good-"+test.name {
			t.Errorf("malformed %s last-good state/program/md5 = %d/%d/%q",
				test.name, stateCount, programCount, lastMD5)
		}
	}
}

func TestIntegrationSDPassRemapFenceAndNextMappingPublication(t *testing.T) {
	db, feed, ingester, channel := setupSDWorkerIntegration(t, true, 9475.71)
	ctx := context.Background()
	oldPass := make(chan sd.IngestStats, 1)
	go func() { oldPass <- ingester.RunOnce(ctx) }()
	select {
	case <-feed.scheduleStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("old SD pass did not pause during provider fetch")
	}

	newMapping := "FX475-NEW"
	remapDone := make(chan error, 1)
	go func() {
		_, err := db.UpdateChannel(ctx, channel.ID, store.ChannelUpdate{EpgChannelID: &newMapping})
		remapDone <- err
	}()
	select {
	case err := <-remapDone:
		feed.release()
		t.Fatalf("remap escaped the in-flight SD pass lease: %v", err)
	case <-time.After(150 * time.Millisecond):
		// Expected: UpdateChannel waits for the pass-wide advisory authority.
	}
	feed.release()
	stats := <-oldPass
	if stats.LineupsOK != 1 || stats.ProgramsAdded != 1 {
		t.Fatalf("old pass stats = %+v", stats)
	}
	if err := <-remapDone; err != nil {
		t.Fatal(err)
	}

	var oldRows int
	var oldMD5 string
	var oldStateChannelID *uuid.UUID
	if err := db.Pool.QueryRow(ctx, `
		SELECT count(*) FROM epg_program
		 WHERE channel_id=$1 AND source_hash LIKE 'sd:%'`, channel.ID).Scan(&oldRows); err != nil {
		t.Fatal(err)
	}
	if err := db.Pool.QueryRow(ctx, `
		SELECT last_md5, channel_id FROM sd_station_state
		 WHERE sd_lineup_id='FX475-LINEUP' AND sd_station_id='FX475-OLD'`).Scan(&oldMD5, &oldStateChannelID); err != nil {
		t.Fatal(err)
	}
	if oldRows != 0 || oldMD5 != "" || oldStateChannelID != nil {
		t.Fatalf("remap cleanup = old sd rows %d old MD5 %q old channel %v; want 0/empty/nil",
			oldRows, oldMD5, oldStateChannelID)
	}

	feed.set("FX475-NEW", "EP047500000002", "new-schedule-md5", "New SD mapping programme")
	stats = ingester.RunOnce(ctx)
	if stats.LineupsOK != 1 || stats.ProgramsAdded != 1 {
		t.Fatalf("new mapping pass stats = %+v", stats)
	}
	var count int
	var title, sourceHash, newMD5 string
	if err := db.Pool.QueryRow(ctx, `
		SELECT count(*), min(title), min(source_hash)
		  FROM epg_program
		 WHERE channel_id=$1 AND source_hash LIKE 'sd:%'`, channel.ID).Scan(&count, &title, &sourceHash); err != nil {
		t.Fatal(err)
	}
	if err := db.Pool.QueryRow(ctx, `
		SELECT last_md5 FROM sd_station_state
		 WHERE sd_lineup_id='FX475-LINEUP' AND sd_station_id='FX475-NEW'`).Scan(&newMD5); err != nil {
		t.Fatal(err)
	}
	if count != 1 || title != "New SD mapping programme" || len(sourceHash) < 4 || sourceHash[:3] != "sd:" || newMD5 == "" {
		t.Fatalf("new mapping publication = count %d title %q hash %q md5 %q", count, title, sourceHash, newMD5)
	}
}

func TestIntegrationSDPassRemapWaitersDoNotExhaustPool(t *testing.T) {
	baseDB := freshSDIntegrationDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Exercise the production floor exactly: one connection owns the pass-wide
	// session lease and seven simultaneous remaps would previously consume all
	// remaining connections in transactions waiting on that lease.
	poolConfig := baseDB.Pool.Config()
	poolConfig.MaxConns = 8
	poolConfig.MinConns = 0
	limitedPool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		t.Fatal(err)
	}
	limitedDB := &store.DB{Pool: limitedPool}
	t.Cleanup(limitedDB.Close)
	if err := limitedDB.Ping(ctx); err != nil {
		t.Fatal(err)
	}

	start := time.Now().UTC().Truncate(time.Minute).Add(48 * time.Hour)
	feed := newWorkerSDFeed(start, true)
	server := httptest.NewServer(feed)
	defer feed.release()
	t.Cleanup(server.Close)
	if _, err := limitedDB.CreateSDLineup(ctx, "FX475-LINEUP", "FX475 lineup", "test", true); err != nil {
		t.Fatal(err)
	}
	channel, err := limitedDB.CreateChannel(ctx, store.Channel{
		Number: 9475.711, Name: "FX475 SD pool bound", CallSign: "FX475SDPOOL",
		EpgChannelID: "FX475-OLD", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	client := sd.New(server.URL, "sd-user", "password")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ingester := sd.NewIngester(logger, client, limitedDB, time.Hour)

	passDone := make(chan sd.IngestStats, 1)
	go func() { passDone <- ingester.RunOnce(ctx) }()
	select {
	case <-feed.scheduleStarted:
	case <-ctx.Done():
		t.Fatal("SD pass did not pause with its session lease held")
	}

	const remapCount = 7
	started := make(chan struct{}, remapCount)
	remapDone := make(chan error, remapCount)
	newMapping := "FX475-NEW"
	for i := 0; i < remapCount; i++ {
		go func() {
			started <- struct{}{}
			_, err := limitedDB.UpdateChannel(ctx, channel.ID, store.ChannelUpdate{EpgChannelID: &newMapping})
			remapDone <- err
		}()
	}
	for i := 0; i < remapCount; i++ {
		<-started
	}
	select {
	case err := <-remapDone:
		t.Fatalf("remap escaped active SD authority: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	if acquired := limitedDB.Pool.Stat().AcquiredConns(); acquired != 1 {
		feed.release()
		t.Fatalf("waiting remaps acquired %d pool connections; want only the SD lease owner", acquired)
	}

	feed.release()
	select {
	case stats := <-passDone:
		if stats.LineupsOK != 1 || stats.ProgramsAdded != 1 {
			t.Fatalf("SD pass could not finish under remap pressure: %+v", stats)
		}
	case <-ctx.Done():
		t.Fatal("SD pass starved behind remap-held pool connections")
	}
	for i := 0; i < remapCount; i++ {
		select {
		case err := <-remapDone:
			if err != nil {
				t.Fatalf("serialized remap %d failed: %v", i, err)
			}
		case <-ctx.Done():
			t.Fatalf("serialized remap %d did not complete", i)
		}
	}

	var mapping string
	var staleRows int
	if err := limitedDB.Pool.QueryRow(ctx, `SELECT epg_channel_id FROM channel WHERE id=$1`, channel.ID).Scan(&mapping); err != nil {
		t.Fatal(err)
	}
	if err := limitedDB.Pool.QueryRow(ctx, `
		SELECT count(*) FROM epg_program
		 WHERE channel_id=$1 AND source_hash LIKE 'sd:%'`, channel.ID).Scan(&staleRows); err != nil {
		t.Fatal(err)
	}
	if mapping != newMapping || staleRows != 0 {
		t.Fatalf("post-pressure remap state = mapping %q stale SD rows %d", mapping, staleRows)
	}
}

func TestIntegrationSDStartupAndManualPassesSerialize(t *testing.T) {
	_, feed, ingester, _ := setupSDWorkerIntegration(t, true, 9475.72)
	ctx := context.Background()
	first := make(chan sd.IngestStats, 1)
	second := make(chan sd.IngestStats, 1)
	go func() { first <- ingester.RunOnce(ctx) }()
	select {
	case <-feed.scheduleStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("first SD pass did not reach provider")
	}
	go func() { second <- ingester.RunOnce(ctx) }()
	time.Sleep(150 * time.Millisecond)
	if got := feed.lineupCalls.Load(); got != 1 {
		feed.release()
		t.Fatalf("concurrent pass entered provider fetch while startup pass held lease: lineup calls=%d", got)
	}
	feed.release()
	firstStats := <-first
	secondStats := <-second
	if firstStats.LineupsOK != 1 || secondStats.LineupsOK != 1 || secondStats.StationsSkipped != 1 {
		t.Fatalf("serialized pass stats = first %+v second %+v", firstStats, secondStats)
	}
	if got := feed.programCalls.Load(); got != 1 {
		t.Fatalf("serialized unchanged second pass fetched program details %d times", got)
	}
}

func TestIntegrationSDStationSnapshotCorrectsMovesDeletesAndEmpties(t *testing.T) {
	db, feed, ingester, channel := setupSDWorkerIntegration(t, false, 9475.74)
	ctx := context.Background()
	_, _, _, _, originalStart, _, _, _, _, _, _, _ := feed.snapshot()
	legacyStart := originalStart.Add(3 * time.Hour)
	if _, err := db.Pool.Exec(ctx, `
		INSERT INTO epg_program (
			channel_id, start_at, end_at, title, source_hash, source_priority,
			is_canonical, is_legacy
		) VALUES ($1,$2,$3,'Opaque pre-upgrade SD row','opaque-priority-10',10,true,true)`,
		channel.ID, legacyStart, legacyStart.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	first := ingester.RunOnce(ctx)
	if first.LineupsOK != 1 || first.ProgramsAdded != 1 || first.ProgramsDeleted != 1 {
		t.Fatalf("initial station snapshot stats = %+v", first)
	}
	var originalHash string
	if err := db.Pool.QueryRow(ctx, `
		SELECT source_hash FROM epg_program
		 WHERE channel_id=$1 AND start_at=$2 AND source_hash LIKE 'sd:%'`,
		channel.ID, originalStart).Scan(&originalHash); err != nil {
		t.Fatal(err)
	}

	// The provider keeps the same programme ID/start while correcting every
	// match-relevant airing field. A content-sensitive sd: hash must force a
	// whole row update before the new MD5 is committed.
	feed.setProgram("corrected-md5", "Corrected SD title", originalStart, 7200, true, true)
	corrected := ingester.RunOnce(ctx)
	if corrected.LineupsOK != 1 || corrected.ProgramsUpdated != 1 || corrected.ProgramsDeleted != 0 {
		t.Fatalf("same-ID correction stats = %+v", corrected)
	}
	var correctedHash, correctedTitle string
	var correctedEnd time.Time
	var correctedLive, correctedNew bool
	if err := db.Pool.QueryRow(ctx, `
		SELECT source_hash, title, end_at, is_live, is_new
		  FROM epg_program
		 WHERE channel_id=$1 AND start_at=$2 AND source_hash LIKE 'sd:%'`,
		channel.ID, originalStart).Scan(
		&correctedHash, &correctedTitle, &correctedEnd, &correctedLive, &correctedNew,
	); err != nil {
		t.Fatal(err)
	}
	if correctedHash == originalHash || correctedTitle != "Corrected SD title" ||
		!correctedEnd.Equal(originalStart.Add(2*time.Hour)) || !correctedLive || !correctedNew {
		t.Fatalf("same-ID correction = hash %q->%q title %q end %s live/new %v/%v",
			originalHash, correctedHash, correctedTitle, correctedEnd, correctedLive, correctedNew)
	}

	// A moved airing is one atomic replacement: publish the new start and delete
	// the omitted old future slot before canonicalization/MD5 commit.
	movedStart := originalStart.Add(4 * time.Hour)
	feed.setProgram("moved-md5", "Moved SD airing", movedStart, 1800, false, false)
	moved := ingester.RunOnce(ctx)
	if moved.LineupsOK != 1 || moved.ProgramsAdded != 1 || moved.ProgramsDeleted != 1 {
		t.Fatalf("moved station snapshot stats = %+v", moved)
	}
	var rowCount int
	var storedStart time.Time
	if err := db.Pool.QueryRow(ctx, `
		SELECT count(*), min(start_at)
		  FROM epg_program
		 WHERE channel_id=$1 AND source_hash LIKE 'sd:%'`, channel.ID).Scan(&rowCount, &storedStart); err != nil {
		t.Fatal(err)
	}
	if rowCount != 1 || !storedStart.Equal(movedStart) {
		t.Fatalf("moved snapshot rows = %d start %s, want one at %s", rowCount, storedStart, movedStart)
	}

	// A code-0 daily schedule with no programmes violates the provider contract
	// and must retain the last-good snapshot/watermark. The repository's empty
	// snapshot primitive remains available for an independently authoritative
	// operator/source-withdrawal decision.
	legacyStart = movedStart.Add(2 * time.Hour)
	if _, err := db.Pool.Exec(ctx, `
		INSERT INTO epg_program (
			channel_id, start_at, end_at, title, source_hash, source_priority,
			is_canonical, is_legacy
		) VALUES ($1,$2,$3,'Opaque row before empty','opaque-before-empty',10,true,true)`,
		channel.ID, legacyStart, legacyStart.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	feed.setEmpty("empty-md5")
	empty := ingester.RunOnce(ctx)
	if empty.LineupsFailed != 1 || empty.LineupsOK != 0 || empty.ProgramsDeleted != 0 {
		t.Fatalf("invalid empty schedule stats = %+v", empty)
	}
	if err := db.Pool.QueryRow(ctx, `
		SELECT count(*) FROM epg_program
		 WHERE channel_id=$1 AND source_hash LIKE 'sd:%'`, channel.ID).Scan(&rowCount); err != nil {
		t.Fatal(err)
	}
	md5, err := db.GetSDStationMD5(ctx, "FX475-LINEUP", "FX475-OLD")
	if err != nil {
		t.Fatal(err)
	}
	if rowCount != 1 || md5 == "" {
		t.Fatalf("invalid empty response changed snapshot = rows %d md5 %q", rowCount, md5)
	}
	if result, err := db.ReplaceSDStationSnapshot(ctx, "FX475-LINEUP", "FX475-OLD", "store-empty-md5", channel.ID, nil); err != nil || result.Deleted != 2 {
		t.Fatalf("authoritative repository empty snapshot = %+v err %v", result, err)
	}
	if err := db.Pool.QueryRow(ctx, `
		SELECT count(*) FROM epg_program
		 WHERE channel_id=$1 AND source_hash LIKE 'sd:%'`, channel.ID).Scan(&rowCount); err != nil {
		t.Fatal(err)
	}
	md5, err = db.GetSDStationMD5(ctx, "FX475-LINEUP", "FX475-OLD")
	if err != nil {
		t.Fatal(err)
	}
	if rowCount != 0 || md5 != "store-empty-md5" {
		t.Fatalf("empty station snapshot = rows %d md5 %q", rowCount, md5)
	}
}

func TestIntegrationSDDoesNotAdvanceMD5WithoutDurableSDCandidate(t *testing.T) {
	db, feed, ingester, channel := setupSDWorkerIntegration(t, false, 9475.73)
	ctx := context.Background()
	_, _, _, _, start, _, _, _, _, _, _, _ := feed.snapshot()
	if _, err := db.UpsertEPGProgram(ctx, store.EPGProgram{
		ChannelID: channel.ID, StartAt: start, EndAt: start.Add(time.Hour),
		Title: "Stronger direct owner", SourceHash: "manual:strong-owner", SourcePriority: -20,
	}); err != nil {
		t.Fatal(err)
	}
	stats := ingester.RunOnce(ctx)
	if stats.LineupsFailed != 0 || stats.LineupsOK != 1 || stats.StationsBlocked != 1 {
		t.Fatalf("blocked SD pass stats = %+v", stats)
	}
	md5, err := db.GetSDStationMD5(ctx, "FX475-LINEUP", "FX475-OLD")
	if err != nil {
		t.Fatal(err)
	}
	var sdRows int
	var lineupStatus string
	if err := db.Pool.QueryRow(ctx, `
		SELECT count(*) FROM epg_program
		 WHERE channel_id=$1 AND source_hash LIKE 'sd:%'`, channel.ID).Scan(&sdRows); err != nil {
		t.Fatal(err)
	}
	if err := db.Pool.QueryRow(ctx, `
		SELECT last_status FROM sd_lineup
		 WHERE sd_lineup_id='FX475-LINEUP'`).Scan(&lineupStatus); err != nil {
		t.Fatal(err)
	}
	if md5 != "" || sdRows != 0 {
		t.Fatalf("blocked SD candidate advanced state: md5=%q rows=%d", md5, sdRows)
	}
	if lineupStatus != "partial: Schedules Direct candidate did not own its direct slot: 1 station snapshot blocked (FX475-OLD)" {
		t.Fatalf("blocked SD lineup status = %q", lineupStatus)
	}
}

func TestIntegrationSDAmbiguousStationMappingFailsLineup(t *testing.T) {
	db, feed, ingester, firstChannel := setupSDWorkerIntegration(t, false, 9475.7)
	ctx := context.Background()
	providerID := "9475.8"
	if _, err := db.UpdateChannel(ctx, firstChannel.ID, store.ChannelUpdate{EpgChannelID: &providerID}); err != nil {
		t.Fatal(err)
	}
	feed.set(providerID, "EP047500000201", "ambiguous-station-md5", "Ambiguous station programme")
	if _, err := db.CreateChannel(ctx, store.Channel{
		Number: 9475.8, Name: "Duplicate SD mapping", CallSign: "FX475DUP",
		EpgChannelID: "FX475-NUMERIC-OWNER", Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}

	stats := ingester.RunOnce(ctx)
	if stats.LineupsFailed != 1 || stats.LineupsOK != 0 ||
		stats.UnmappedStations != 0 || stats.StationsBlocked != 0 {
		t.Fatalf("ambiguous SD mapping stats = %+v", stats)
	}
	md5, err := db.GetSDStationMD5(ctx, "FX475-LINEUP", providerID)
	if err != nil {
		t.Fatal(err)
	}
	var sdRows int
	var lineupStatus string
	if err := db.Pool.QueryRow(ctx, `
		SELECT count(*) FROM epg_program
		 WHERE channel_id=$1 AND source_hash LIKE 'sd:%'`, firstChannel.ID).Scan(&sdRows); err != nil {
		t.Fatal(err)
	}
	if err := db.Pool.QueryRow(ctx, `
		SELECT last_status FROM sd_lineup
		 WHERE sd_lineup_id='FX475-LINEUP'`).Scan(&lineupStatus); err != nil {
		t.Fatal(err)
	}
	if md5 != "" || sdRows != 0 {
		t.Fatalf("ambiguous SD mapping advanced state: md5=%q rows=%d", md5, sdRows)
	}
	if lineupStatus != `error: map SD station 9475.8: ambiguous EPG channel mapping for provider channel "9475.8"` {
		t.Fatalf("ambiguous SD lineup status = %q", lineupStatus)
	}
}

func TestIntegrationSDRejectsDistinctStationsClaimingOneChannel(t *testing.T) {
	db := freshSDIntegrationDB(t)
	ctx := context.Background()
	const (
		lineupID         = "FX475-DUPLICATE-CHANNEL-CLAIM"
		followupLineupID = "FX475-DUPLICATE-CLAIM-FOLLOWUP"
		stationA         = "FX475-CLAIM-A"
		stationB         = "9475.5"
	)
	var md5Calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/token" {
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "token": "tok-claim"})
			return
		}
		if r.Header.Get("token") != "tok-claim" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/lineups/" + lineupID:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"stations": []map[string]any{
					{"stationID": stationA, "name": "Explicit claim", "callsign": stationA},
					{"stationID": stationB, "name": "Numeric claim", "callsign": stationB},
				},
				"map": []map[string]any{
					{"stationID": stationA, "channel": "47.81"},
					{"stationID": stationB, "channel": "47.82"},
				},
				"metadata": map[string]any{"lineup": lineupID},
			})
		case "/lineups/" + followupLineupID:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"stations": []map[string]any{{"stationID": stationB, "name": "Valid follow-up claim", "callsign": stationB}},
				"map":      []map[string]any{{"stationID": stationB, "channel": "47.83"}},
				"metadata": map[string]any{"lineup": followupLineupID},
			})
		case "/schedules/md5":
			md5Calls.Add(1)
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	if _, err := db.CreateSDLineup(ctx, lineupID, "FX475 duplicate channel claim", "test", true); err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateSDLineup(ctx, followupLineupID, "Z valid follow-up claim", "test", true); err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateChannel(ctx, store.Channel{
		Number: 9475.5, Name: "Claimed twice", CallSign: "FX475CLAIM",
		EpgChannelID: stationA, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	client := sd.New(server.URL, "sd-user", "password")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	stats := sd.NewIngester(logger, client, db, time.Hour).RunOnce(ctx)
	if stats.LineupsAttempted != 2 || stats.LineupsFailed != 2 || stats.LineupsOK != 0 ||
		stats.StationsTotal != 3 || md5Calls.Load() != 1 {
		t.Fatalf("duplicate channel claim stats = %+v md5 calls=%d", stats, md5Calls.Load())
	}
}

func TestIntegrationSDRejectsCrossLineupChannelClaimBeforeProviderFetch(t *testing.T) {
	db := freshSDIntegrationDB(t)
	ctx := context.Background()
	const (
		lineupA  = "FX475-CROSS-CLAIM-A"
		lineupB  = "FX475-CROSS-CLAIM-B"
		stationA = "FX475-CROSS-STATION-A"
		stationB = "9475.5"
		programA = "EP047500000501"
	)
	today := time.Now().UTC().Format("2006-01-02")
	todayDate, err := time.Parse("2006-01-02", today)
	if err != nil {
		t.Fatal(err)
	}
	previous := todayDate.AddDate(0, 0, -1).Format("2006-01-02")
	start := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	var stationAMD5Calls, stationBMD5Calls atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/token" {
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "token": "tok-cross-claim"})
			return
		}
		if r.Header.Get("token") != "tok-cross-claim" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/lineups/" + lineupA:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"stations": []map[string]any{{"stationID": stationA, "name": "First claim", "callsign": stationA}},
				"map":      []map[string]any{{"stationID": stationA, "channel": "47.51"}},
				"metadata": map[string]any{"lineup": lineupA},
			})
		case "/lineups/" + lineupB:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"stations": []map[string]any{{"stationID": stationB, "name": "Second claim", "callsign": stationB}},
				"map":      []map[string]any{{"stationID": stationB, "channel": "47.52"}},
				"metadata": map[string]any{"lineup": lineupB},
			})
		case "/schedules/md5":
			var reqs []sd.ScheduleRequest
			if err := json.NewDecoder(r.Body).Decode(&reqs); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			manifest := map[string]any{}
			for _, req := range reqs {
				if req.StationID == stationB {
					stationBMD5Calls.Add(1)
				} else if req.StationID == stationA {
					stationAMD5Calls.Add(1)
				}
				dates := req.Dates
				if len(dates) == 0 {
					dates = []string{today}
				}
				entries := map[string]any{}
				for _, date := range dates {
					md5 := "cross-claim-today-md5"
					if date == previous {
						md5 = "cross-claim-previous-md5"
					}
					entries[date] = map[string]any{"code": 0, "message": "OK", "md5": md5}
				}
				manifest[req.StationID] = entries
			}
			_ = json.NewEncoder(w).Encode(manifest)
		case "/schedules":
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{
					"stationID": stationA,
					"programs": []map[string]any{{
						"programID": "EP047500000500", "airDateTime": previous + "T00:00:00Z",
						"duration": 60, "md5": "cross-claim-expired-md5",
					}},
					"metadata": map[string]any{"md5": "cross-claim-previous-md5", "startDate": previous},
				},
				{
					"stationID": stationA,
					"programs": []map[string]any{{
						"programID": programA, "airDateTime": start.Format(time.RFC3339),
						"duration": 3600, "md5": "cross-claim-program-md5",
					}},
					"metadata": map[string]any{"md5": "cross-claim-today-md5", "startDate": today},
				},
			})
		case "/programs":
			_ = json.NewEncoder(w).Encode([]map[string]any{{
				"programID": programA, "md5": "cross-claim-program-md5",
				"titles": []map[string]string{{"title120": "First lineup programme"}},
			}})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	if _, err := db.CreateSDLineup(ctx, lineupA, "A first channel claim", "test", true); err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateSDLineup(ctx, lineupB, "B duplicate channel claim", "test", true); err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateChannel(ctx, store.Channel{
		Number: 9475.5, Name: "Cross-lineup claimed channel", CallSign: "FX475CROSSCLAIM",
		EpgChannelID: stationA, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	client := sd.New(server.URL, "sd-user", "password")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	stats := sd.NewIngester(logger, client, db, time.Hour).RunOnce(ctx)
	if stats.LineupsOK != 1 || stats.LineupsFailed != 1 || stats.StationsTotal != 2 ||
		stats.ProgramsAdded != 1 || stationAMD5Calls.Load() != 2 || stationBMD5Calls.Load() != 0 {
		t.Fatalf("cross-lineup channel claim stats = %+v station A MD5=%d station B MD5=%d",
			stats, stationAMD5Calls.Load(), stationBMD5Calls.Load())
	}
	var lineupBStatus string
	if err := db.Pool.QueryRow(ctx, `SELECT last_status FROM sd_lineup WHERE sd_lineup_id=$1`, lineupB).Scan(&lineupBStatus); err != nil {
		t.Fatal(err)
	}
	wantStatus := "error: SD stations " + stationA + " and " + stationB + " map to the same Conductor channel"
	if !strings.HasPrefix(lineupBStatus, wantStatus) {
		t.Fatalf("cross-lineup duplicate status = %q, want prefix %q", lineupBStatus, wantStatus)
	}
}

func TestIntegrationSDFailedProviderRetainsCrossLineupChannelClaim(t *testing.T) {
	db := freshSDIntegrationDB(t)
	ctx := context.Background()
	const (
		lineupA  = "FX475-FAILED-CLAIM-A"
		lineupB  = "FX475-FAILED-CLAIM-B"
		stationA = "FX475-FAILED-STATION-A"
		stationB = "9475.6"
		programB = "EP047500000601"
	)
	today := time.Now().UTC().Format("2006-01-02")
	todayDate, err := time.Parse("2006-01-02", today)
	if err != nil {
		t.Fatal(err)
	}
	previous := todayDate.AddDate(0, 0, -1).Format("2006-01-02")
	start := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	var stationAMD5Calls, stationBMD5Calls atomic.Int32
	var stationBScheduleCalls, stationBProgramCalls atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/token" {
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "token": "tok-failed-claim"})
			return
		}
		if r.Header.Get("token") != "tok-failed-claim" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/lineups/" + lineupA:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"stations": []map[string]any{{"stationID": stationA, "name": "Failed first claim", "callsign": stationA}},
				"map":      []map[string]any{{"stationID": stationA, "channel": "47.56"}},
				"metadata": map[string]any{"lineup": lineupA},
			})
		case "/lineups/" + lineupB:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"stations": []map[string]any{{"stationID": stationB, "name": "Conflicting second claim", "callsign": stationB}},
				"map":      []map[string]any{{"stationID": stationB, "channel": "47.57"}},
				"metadata": map[string]any{"lineup": lineupB},
			})
		case "/schedules/md5":
			var reqs []sd.ScheduleRequest
			if err := json.NewDecoder(r.Body).Decode(&reqs); err != nil || len(reqs) != 1 {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			req := reqs[0]
			if req.StationID == stationA {
				stationAMD5Calls.Add(1)
				http.Error(w, "first lineup manifest unavailable", http.StatusServiceUnavailable)
				return
			}
			if req.StationID != stationB {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			stationBMD5Calls.Add(1)
			dates := req.Dates
			if len(dates) == 0 {
				dates = []string{today}
			}
			entries := map[string]any{}
			for _, date := range dates {
				md5 := "failed-claim-today-md5"
				if date == previous {
					md5 = "failed-claim-previous-md5"
				}
				entries[date] = map[string]any{"code": 0, "message": "OK", "md5": md5}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{stationB: entries})
		case "/schedules":
			stationBScheduleCalls.Add(1)
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{
					"stationID": stationB,
					"programs": []map[string]any{{
						"programID": "EP047500000600", "airDateTime": previous + "T00:00:00Z",
						"duration": 60, "md5": "failed-claim-expired-md5",
					}},
					"metadata": map[string]any{"md5": "failed-claim-previous-md5", "startDate": previous},
				},
				{
					"stationID": stationB,
					"programs": []map[string]any{{
						"programID": programB, "airDateTime": start.Format(time.RFC3339),
						"duration": 3600, "md5": "failed-claim-program-md5",
					}},
					"metadata": map[string]any{"md5": "failed-claim-today-md5", "startDate": today},
				},
			})
		case "/programs":
			stationBProgramCalls.Add(1)
			_ = json.NewEncoder(w).Encode([]map[string]any{{
				"programID": programB, "md5": "failed-claim-program-md5",
				"titles": []map[string]string{{"title120": "Second lineup must not publish"}},
			}})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	if _, err := db.CreateSDLineup(ctx, lineupA, "A failed first claim", "test", true); err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateSDLineup(ctx, lineupB, "B conflicting second claim", "test", true); err != nil {
		t.Fatal(err)
	}
	channel, err := db.CreateChannel(ctx, store.Channel{
		Number: 9475.6, Name: "Failed-pass claimed channel", CallSign: "FX475FAILEDCLAIM",
		EpgChannelID: stationA, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	client := sd.New(server.URL, "sd-user", "password")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	stats := sd.NewIngester(logger, client, db, time.Hour).RunOnce(ctx)
	if stats.LineupsAttempted != 2 || stats.LineupsFailed != 2 || stats.LineupsOK != 0 ||
		stats.StationsTotal != 2 || stats.StationsFetched != 0 || stats.ProgramsAdded != 0 ||
		stationAMD5Calls.Load() != 1 || stationBMD5Calls.Load() != 0 ||
		stationBScheduleCalls.Load() != 0 || stationBProgramCalls.Load() != 0 {
		t.Fatalf("failed cross-lineup claim stats = %+v A MD5=%d B MD5=%d schedules=%d programs=%d",
			stats, stationAMD5Calls.Load(), stationBMD5Calls.Load(),
			stationBScheduleCalls.Load(), stationBProgramCalls.Load())
	}
	for _, claim := range []struct {
		lineupID string
		station  string
	}{{lineupA, stationA}, {lineupB, stationB}} {
		md5, err := db.GetSDStationMD5(ctx, claim.lineupID, claim.station)
		if err != nil {
			t.Fatal(err)
		}
		if md5 != "" {
			t.Fatalf("failed cross-lineup claim advanced %s/%s watermark to %q", claim.lineupID, claim.station, md5)
		}
	}
	var sdRows, stationStates int
	if err := db.Pool.QueryRow(ctx, `
		SELECT count(*) FROM epg_program
		 WHERE channel_id=$1 AND source_hash LIKE 'sd:%'`, channel.ID).Scan(&sdRows); err != nil {
		t.Fatal(err)
	}
	if err := db.Pool.QueryRow(ctx, `
		SELECT count(*) FROM sd_station_state
		 WHERE sd_lineup_id IN ($1, $2)`, lineupA, lineupB).Scan(&stationStates); err != nil {
		t.Fatal(err)
	}
	if sdRows != 0 || stationStates != 0 {
		t.Fatalf("failed cross-lineup claim published durable state: rows=%d station states=%d", sdRows, stationStates)
	}
	var lineupAStatus, lineupBStatus string
	if err := db.Pool.QueryRow(ctx, `SELECT last_status FROM sd_lineup WHERE sd_lineup_id=$1`, lineupA).Scan(&lineupAStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.Pool.QueryRow(ctx, `SELECT last_status FROM sd_lineup WHERE sd_lineup_id=$1`, lineupB).Scan(&lineupBStatus); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(lineupAStatus, "HTTP 503") {
		t.Fatalf("failed first lineup status = %q", lineupAStatus)
	}
	wantStatus := "error: SD stations " + stationA + " and " + stationB + " map to the same Conductor channel"
	if !strings.HasPrefix(lineupBStatus, wantStatus) {
		t.Fatalf("failed-pass duplicate status = %q, want prefix %q", lineupBStatus, wantStatus)
	}
}

func TestIntegrationSDScheduleResponseOmissionFailsLineup(t *testing.T) {
	db := freshSDIntegrationDB(t)
	ctx := context.Background()
	const (
		lineupID = "FX475-OMITTED-STATION-LINEUP"
		stationA = "FX475-OMIT-A"
		stationB = "FX475-OMIT-B"
	)
	scheduleDate := time.Now().UTC().Format("2006-01-02")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/token" {
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "token": "tok-omission"})
			return
		}
		if r.Header.Get("token") != "tok-omission" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/lineups/" + lineupID:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"stations": []map[string]any{
					{"stationID": stationA, "name": "Station A", "callsign": stationA},
					{"stationID": stationB, "name": "Station B", "callsign": stationB},
				},
				"map": []map[string]any{
					{"stationID": stationA, "channel": "47.61"},
					{"stationID": stationB, "channel": "47.62"},
				},
				"metadata": map[string]any{"lineup": lineupID, "modified": "2026-08-23T12:00:00Z"},
			})
		case "/schedules/md5":
			var reqs []sd.ScheduleRequest
			if err := json.NewDecoder(r.Body).Decode(&reqs); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			manifest := map[string]any{}
			for _, req := range reqs {
				dates := req.Dates
				if len(dates) == 0 {
					dates = []string{scheduleDate}
				}
				md5 := "station-a-md5"
				if req.StationID == stationB {
					md5 = "station-b-md5"
				}
				entries := map[string]any{}
				for _, date := range dates {
					entries[date] = map[string]any{"code": 0, "message": "OK", "md5": md5}
				}
				manifest[req.StationID] = entries
			}
			_ = json.NewEncoder(w).Encode(manifest)
		case "/schedules":
			// A successful HTTP response that silently omits station B is not
			// an authoritative lineup snapshot.
			previousDate, _ := time.Parse("2006-01-02", scheduleDate)
			prior := previousDate.AddDate(0, 0, -1).Format("2006-01-02")
			priorResponse := func(stationID, md5 string) map[string]any {
				return map[string]any{
					"stationID": stationID,
					"programs": []map[string]any{{
						"programID": "EP000000000000", "airDateTime": prior + "T00:00:00Z",
						"duration": 60, "md5": "expired-prior-day-program-md5",
					}},
					"metadata": map[string]any{"md5": md5, "startDate": prior},
				}
			}
			_ = json.NewEncoder(w).Encode([]map[string]any{
				priorResponse(stationA, "station-a-md5"),
				{
					"stationID": stationA,
					"programs": []map[string]any{{
						"programID": "EP047500000601", "airDateTime": scheduleDate + "T12:00:00Z",
						"duration": 3600, "md5": "station-a-program-md5",
					}},
					"metadata": map[string]any{
						"modified": "2026-08-23T12:00:00Z", "md5": "station-a-md5", "startDate": scheduleDate,
					},
				},
				priorResponse(stationB, "station-b-md5"),
			})
		case "/programs":
			t.Error("program details requested before schedule response-set validation")
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	if _, err := db.CreateSDLineup(ctx, lineupID, "FX475 omission lineup", "test", true); err != nil {
		t.Fatal(err)
	}
	channelNumbers := []float64{9475.25, 9475.5}
	for index, stationID := range []string{stationA, stationB} {
		if _, err := db.CreateChannel(ctx, store.Channel{
			Number:       channelNumbers[index],
			Name:         stationID,
			CallSign:     stationID,
			EpgChannelID: stationID,
			Enabled:      true,
		}); err != nil {
			t.Fatal(err)
		}
	}
	client := sd.New(server.URL, "sd-user", "password")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	stats := sd.NewIngester(logger, client, db, time.Hour).RunOnce(ctx)
	if stats.LineupsFailed != 1 || stats.LineupsOK != 0 || stats.StationsTotal != 2 ||
		stats.StationsFetched != 0 || stats.StationsSkipped != 0 {
		t.Fatalf("omitted-station stats = %+v", stats)
	}
	for _, stationID := range []string{stationA, stationB} {
		if md5, err := db.GetSDStationMD5(ctx, lineupID, stationID); err != nil || md5 != "" {
			t.Fatalf("omitted response advanced station %s MD5 to %q err %v", stationID, md5, err)
		}
	}
	var lineupStatus string
	if err := db.Pool.QueryRow(ctx, `SELECT last_status FROM sd_lineup WHERE sd_lineup_id=$1`, lineupID).Scan(&lineupStatus); err != nil {
		t.Fatal(err)
	}
	if lineupStatus != "error: schedules response omitted requested station/dates: "+stationB+"/"+scheduleDate {
		t.Fatalf("omitted-station lineup status = %q", lineupStatus)
	}
}

func TestIntegrationSDMultiDayManifestPublishesAtomicallyAndSkipsUnchanged(t *testing.T) {
	db := freshSDIntegrationDB(t)
	ctx := context.Background()
	const (
		lineupID  = "FX475-MULTIDAY-LINEUP"
		stationID = "FX475-MULTIDAY"
		programA  = "EP047500000701"
		programB  = "EP047500000702"
	)
	now := time.Now().UTC()
	dayA := now.Format("2006-01-02")
	dayB := now.AddDate(0, 0, 1).Format("2006-01-02")
	dayAStart, err := time.Parse(time.RFC3339, dayA+"T12:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	dayBStart, err := time.Parse(time.RFC3339, dayB+"T12:00:00Z")
	if err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	dayAMD5 := "multiday-a-v1"
	dayBMD5 := "multiday-b-v1"
	titleB := "Second-day finale"
	mode := "normal"
	var md5Calls, scheduleCalls, programCalls atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/token" {
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "token": "tok-multiday"})
			return
		}
		if r.Header.Get("token") != "tok-multiday" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		mu.Lock()
		currentAMD5, currentBMD5, currentTitleB, currentMode := dayAMD5, dayBMD5, titleB, mode
		mu.Unlock()
		switch r.URL.Path {
		case "/lineups/" + lineupID:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"stations": []map[string]any{{"stationID": stationID, "name": "Multi-day", "callsign": stationID}},
				"map":      []map[string]any{{"stationID": stationID, "channel": "47.71"}},
				"metadata": map[string]any{"lineup": lineupID, "modified": "2026-08-23T12:00:00Z"},
			})
		case "/schedules/md5":
			md5Calls.Add(1)
			var reqs []sd.ScheduleRequest
			if err := json.NewDecoder(r.Body).Decode(&reqs); err != nil || len(reqs) != 1 {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			entries := map[string]any{}
			if len(reqs[0].Dates) == 0 {
				entries[dayB] = map[string]any{"code": 0, "message": "OK", "md5": currentBMD5}
				entries[dayA] = map[string]any{"code": 0, "message": "OK", "md5": currentAMD5}
			} else {
				for _, date := range reqs[0].Dates {
					entries[date] = map[string]any{"code": 0, "message": "OK", "md5": "multiday-prior-md5"}
				}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{stationID: entries})
		case "/schedules":
			scheduleCalls.Add(1)
			dayAResponse := map[string]any{
				"stationID": stationID,
				"programs": []map[string]any{{
					"programID": programA, "airDateTime": dayAStart.Format(time.RFC3339),
					"duration": 3600, "md5": "multiday-program-a", "new": true,
					"liveTapeDelay": "Live", "isPremiereOrFinale": "Season Premiere",
				}},
				"metadata": map[string]any{"md5": currentAMD5, "startDate": dayA},
			}
			dayBResponse := map[string]any{
				"stationID": stationID,
				"programs": []map[string]any{{
					"programID": programB, "airDateTime": dayBStart.Format(time.RFC3339),
					"duration": 3600, "md5": "multiday-program-b", "premiere": true,
					"liveTapeDelay": "Tape", "isPremiereOrFinale": "Series Finale",
				}},
				"metadata": map[string]any{"md5": currentBMD5, "startDate": dayB},
			}
			dayADate, _ := time.Parse("2006-01-02", dayA)
			prior := dayADate.AddDate(0, 0, -1).Format("2006-01-02")
			priorResponse := map[string]any{
				"stationID": stationID,
				"programs": []map[string]any{{
					"programID": "EP000000000000", "airDateTime": prior + "T00:00:00Z",
					"duration": 60, "md5": "expired-prior-day-program-md5",
				}},
				"metadata": map[string]any{"md5": "multiday-prior-md5", "startDate": prior},
			}
			responses := []map[string]any{dayBResponse, priorResponse, dayAResponse}
			if currentMode == "duplicate" {
				responses = append(responses, dayAResponse)
			}
			_ = json.NewEncoder(w).Encode(responses)
		case "/programs":
			programCalls.Add(1)
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{
					"programID": programA,
					"md5":       "multiday-program-a",
					"titles":    []map[string]string{{"title120": "First-day live premiere"}},
					"genres":    []string{"Sports"},
				},
				{
					"programID": programB,
					"md5":       "multiday-program-b",
					"titles":    []map[string]string{{"title120": currentTitleB}},
					"genres":    []string{"Series"},
				},
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)

	if _, err := db.CreateSDLineup(ctx, lineupID, "FX475 multi-day lineup", "test", true); err != nil {
		t.Fatal(err)
	}
	channel, err := db.CreateChannel(ctx, store.Channel{
		Number: 9475.71, Name: "FX475 multi-day", CallSign: stationID,
		EpgChannelID: stationID, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	client := sd.New(server.URL, "sd-user", "password")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ingester := sd.NewIngester(logger, client, db, time.Hour)

	first := ingester.RunOnce(ctx)
	if first.LineupsOK != 1 || first.StationsFetched != 1 || first.ProgramsAdded != 2 {
		t.Fatalf("initial multi-day stats = %+v", first)
	}
	type guideRow struct {
		Start                    time.Time
		AiringCivilDate          time.Time
		Title, ProviderEpisodeID string
		Live, NewExplicit        bool
		Premiere, Finale         bool
	}
	readRows := func() []guideRow {
		rows, err := db.Pool.Query(ctx, `
			SELECT start_at, airing_civil_date, title, provider_episode_id, is_live, new_explicit,
			       is_premiere, is_finale
			  FROM epg_program
			 WHERE channel_id=$1 AND source_hash LIKE 'sd:%'
			 ORDER BY start_at`, channel.ID)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var out []guideRow
		for rows.Next() {
			var row guideRow
			if err := rows.Scan(&row.Start, &row.AiringCivilDate, &row.Title, &row.ProviderEpisodeID,
				&row.Live, &row.NewExplicit, &row.Premiere, &row.Finale); err != nil {
				t.Fatal(err)
			}
			out = append(out, row)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return out
	}
	rows := readRows()
	if len(rows) != 2 || !rows[0].Start.Equal(dayAStart) ||
		rows[0].AiringCivilDate.Format("2006-01-02") != dayA || rows[0].ProviderEpisodeID != programA ||
		!rows[0].Live || !rows[0].NewExplicit || !rows[0].Premiere || rows[0].Finale ||
		!rows[1].Start.Equal(dayBStart) || rows[1].AiringCivilDate.Format("2006-01-02") != dayB ||
		rows[1].ProviderEpisodeID != programB ||
		rows[1].Live || rows[1].NewExplicit || !rows[1].Premiere || !rows[1].Finale {
		t.Fatalf("initial multi-day rows = %+v", rows)
	}
	firstWatermark, err := db.GetSDStationMD5(ctx, lineupID, stationID)
	if err != nil || firstWatermark == "" {
		t.Fatalf("initial multi-day watermark = %q err %v", firstWatermark, err)
	}

	unchanged := ingester.RunOnce(ctx)
	if unchanged.LineupsOK != 1 || unchanged.StationsSkipped != 1 || unchanged.StationsFetched != 0 {
		t.Fatalf("unchanged multi-day stats = %+v", unchanged)
	}
	if md5Calls.Load() != 4 || scheduleCalls.Load() != 1 || programCalls.Load() != 1 {
		t.Fatalf("unchanged provider calls = md5 %d schedules %d programs %d",
			md5Calls.Load(), scheduleCalls.Load(), programCalls.Load())
	}

	mu.Lock()
	dayBMD5 = "multiday-b-v2"
	titleB = "Corrected second-day finale"
	mu.Unlock()
	corrected := ingester.RunOnce(ctx)
	if corrected.LineupsOK != 1 || corrected.StationsFetched != 1 ||
		corrected.ProgramsUpdated != 1 || corrected.ProgramsUnchanged != 1 {
		t.Fatalf("one-day correction stats = %+v", corrected)
	}
	rows = readRows()
	if len(rows) != 2 || rows[0].Title != "First-day live premiere" || rows[1].Title != "Corrected second-day finale" {
		t.Fatalf("one-day correction lost full horizon: %+v", rows)
	}
	correctedWatermark, err := db.GetSDStationMD5(ctx, lineupID, stationID)
	if err != nil || correctedWatermark == "" || correctedWatermark == firstWatermark {
		t.Fatalf("corrected multi-day watermark = %q prior %q err %v", correctedWatermark, firstWatermark, err)
	}

	mu.Lock()
	dayAMD5 = "multiday-a-v2"
	mode = "duplicate"
	mu.Unlock()
	duplicate := ingester.RunOnce(ctx)
	if duplicate.LineupsFailed != 1 || duplicate.LineupsOK != 0 || duplicate.ProgramsAdded != 0 || duplicate.ProgramsUpdated != 0 {
		t.Fatalf("duplicate station/day stats = %+v", duplicate)
	}
	afterDuplicate, err := db.GetSDStationMD5(ctx, lineupID, stationID)
	if err != nil || afterDuplicate != correctedWatermark {
		t.Fatalf("duplicate station/day changed watermark = %q want %q err %v", afterDuplicate, correctedWatermark, err)
	}
	rows = readRows()
	if len(rows) != 2 || rows[1].Title != "Corrected second-day finale" {
		t.Fatalf("duplicate station/day changed last-good rows: %+v", rows)
	}
}

func TestIntegrationSDColdStartIncludesAndWithdrawsCrossMidnightAiring(t *testing.T) {
	db := freshSDIntegrationDB(t)
	ctx := context.Background()
	const (
		lineupID  = "FX475-CROSS-MIDNIGHT-LINEUP"
		stationID = "FX475-CROSS-MIDNIGHT"
	)
	now := time.Now().UTC()
	today := now.Format("2006-01-02")
	previous := now.AddDate(0, 0, -1).Format("2006-01-02")
	tomorrow := now.AddDate(0, 0, 1).Format("2006-01-02")
	dayAfterTomorrow := now.AddDate(0, 0, 2).Format("2006-01-02")
	previousStart, err := time.Parse(time.RFC3339, previous+"T23:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	todayStart, _ := time.Parse(time.RFC3339, today+"T23:59:00Z")
	if !todayStart.After(now) {
		t.Skip("too close to UTC date rollover for stable cross-midnight fixture")
	}
	tomorrowStart, _ := time.Parse(time.RFC3339, tomorrow+"T12:00:00Z")
	dayAfterTomorrowStart, _ := time.Parse(time.RFC3339, dayAfterTomorrow+"T12:00:00Z")
	var md5Calls, scheduleCalls atomic.Int32
	var gapManifest atomic.Bool
	var withdrawPrior atomic.Bool

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/token" {
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "token": "tok-cross-midnight"})
			return
		}
		if r.Header.Get("token") != "tok-cross-midnight" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/lineups/" + lineupID:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"stations": []map[string]any{{"stationID": stationID, "name": "Cross-midnight", "callsign": stationID}},
				"map":      []map[string]any{{"stationID": stationID, "channel": "47.72"}},
				"metadata": map[string]any{"lineup": lineupID},
			})
		case "/schedules/md5":
			var reqs []sd.ScheduleRequest
			if err := json.NewDecoder(r.Body).Decode(&reqs); err != nil || len(reqs) != 1 {
				t.Errorf("cross-midnight MD5 request = %+v err %v", reqs, err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			md5Calls.Add(1)
			entries := map[string]any{}
			if len(reqs[0].Dates) == 0 {
				entries[today] = map[string]any{"code": 0, "message": "OK", "md5": "today-md5"}
				entries[dayAfterTomorrow] = map[string]any{"code": 0, "message": "OK", "md5": "day-after-md5"}
				if !gapManifest.Load() {
					entries[tomorrow] = map[string]any{"code": 0, "message": "OK", "md5": "tomorrow-md5"}
				}
			} else {
				for _, date := range reqs[0].Dates {
					md5 := "previous-md5"
					if withdrawPrior.Load() {
						md5 = "previous-md5-withdrawn"
					}
					entries[date] = map[string]any{"code": 0, "message": "OK", "md5": md5}
				}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{stationID: entries})
		case "/schedules":
			scheduleCalls.Add(1)
			priorProgram := map[string]any{
				"programID": "EP047500000721", "airDateTime": previousStart.Format(time.RFC3339),
				"duration": int(todayStart.Sub(previousStart).Seconds()),
				"md5":      "previous-program-md5", "liveTapeDelay": "Live",
			}
			priorMD5 := "previous-md5"
			if withdrawPrior.Load() {
				priorProgram = map[string]any{
					"programID": "EP047500000720", "airDateTime": previous + "T00:00:00Z",
					"duration": 60, "md5": "expired-previous-program-md5",
				}
				priorMD5 = "previous-md5-withdrawn"
			}
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{
					"stationID": stationID,
					"programs":  []map[string]any{priorProgram},
					"metadata":  map[string]any{"md5": priorMD5, "startDate": previous},
				},
				{
					"stationID": stationID,
					"programs": []map[string]any{{
						"programID": "EP047500000722", "airDateTime": todayStart.Format(time.RFC3339),
						"duration": 60, "md5": "today-program-md5",
					}},
					"metadata": map[string]any{"md5": "today-md5", "startDate": today},
				},
				{
					"stationID": stationID,
					"programs": []map[string]any{{
						"programID": "EP047500000723", "airDateTime": tomorrowStart.Format(time.RFC3339),
						"duration": 3600, "md5": "tomorrow-program-md5",
					}},
					"metadata": map[string]any{"md5": "tomorrow-md5", "startDate": tomorrow},
				},
				{
					"stationID": stationID,
					"programs": []map[string]any{{
						"programID": "EP047500000724", "airDateTime": dayAfterTomorrowStart.Format(time.RFC3339),
						"duration": 3600, "md5": "day-after-program-md5",
					}},
					"metadata": map[string]any{"md5": "day-after-md5", "startDate": dayAfterTomorrow},
				},
			})
		case "/programs":
			programs := []map[string]any{
				{"programID": "EP047500000722", "md5": "today-program-md5", "titles": []map[string]string{{"title120": "Today programme"}}},
				{"programID": "EP047500000723", "md5": "tomorrow-program-md5", "titles": []map[string]string{{"title120": "Tomorrow programme"}}},
				{"programID": "EP047500000724", "md5": "day-after-program-md5", "titles": []map[string]string{{"title120": "Day-after programme"}}},
			}
			if !withdrawPrior.Load() {
				programs = append(programs, map[string]any{
					"programID": "EP047500000721", "md5": "previous-program-md5",
					"titles": []map[string]string{{"title120": "Cross-midnight live event"}},
				})
			}
			_ = json.NewEncoder(w).Encode(programs)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	if _, err := db.CreateSDLineup(ctx, lineupID, "FX475 cross-midnight lineup", "test", true); err != nil {
		t.Fatal(err)
	}
	channel, err := db.CreateChannel(ctx, store.Channel{
		Number: 9475.72, Name: "FX475 cross-midnight", CallSign: stationID,
		EpgChannelID: stationID, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	client := sd.New(server.URL, "sd-user", "password")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ingester := sd.NewIngester(logger, client, db, time.Hour)
	first := ingester.RunOnce(ctx)
	if first.LineupsOK != 1 || first.ProgramsAdded != 4 || first.ProgramsUnchanged != 0 || first.ProgramsDeleted != 0 || first.StationsFetched != 1 {
		t.Fatalf("cross-midnight initial stats = %+v", first)
	}
	var priorRows int
	if err := db.Pool.QueryRow(ctx, `
		SELECT count(*) FROM epg_program
		 WHERE channel_id=$1 AND start_at=$2 AND title='Cross-midnight live event'`, channel.ID, previousStart).Scan(&priorRows); err != nil {
		t.Fatal(err)
	}
	if priorRows != 1 || md5Calls.Load() != 2 || scheduleCalls.Load() != 1 {
		t.Fatalf("cross-midnight retention rows=%d md5=%d schedules=%d",
			priorRows, md5Calls.Load(), scheduleCalls.Load())
	}
	lastGoodWatermark, err := db.GetSDStationMD5(ctx, lineupID, stationID)
	if err != nil || lastGoodWatermark == "" {
		t.Fatalf("cross-midnight last-good watermark = %q err %v", lastGoodWatermark, err)
	}
	unchanged := ingester.RunOnce(ctx)
	if unchanged.LineupsOK != 1 || unchanged.StationsSkipped != 1 || scheduleCalls.Load() != 1 {
		t.Fatalf("cross-midnight unchanged pass = %+v schedules=%d", unchanged, scheduleCalls.Load())
	}

	withdrawPrior.Store(true)
	withdrawn := ingester.RunOnce(ctx)
	if withdrawn.LineupsOK != 1 || withdrawn.StationsFetched != 1 || withdrawn.ProgramsDeleted != 1 ||
		withdrawn.ProgramsUnchanged != 3 || scheduleCalls.Load() != 2 {
		t.Fatalf("cross-midnight withdrawal pass = %+v schedules=%d", withdrawn, scheduleCalls.Load())
	}
	var withdrawnRows int
	if err := db.Pool.QueryRow(ctx, `
		SELECT count(*) FROM epg_program
		 WHERE channel_id=$1 AND start_at=$2`, channel.ID, previousStart).Scan(&withdrawnRows); err != nil {
		t.Fatal(err)
	}
	if withdrawnRows != 0 {
		t.Fatalf("withdrawn cross-midnight row count = %d want 0", withdrawnRows)
	}
	withdrawnWatermark, err := db.GetSDStationMD5(ctx, lineupID, stationID)
	if err != nil || withdrawnWatermark == "" || withdrawnWatermark == lastGoodWatermark {
		t.Fatalf("withdrawn watermark = %q previous %q err %v", withdrawnWatermark, lastGoodWatermark, err)
	}

	gapManifest.Store(true)
	gapped := ingester.RunOnce(ctx)
	if gapped.LineupsFailed != 1 || gapped.LineupsOK != 0 || scheduleCalls.Load() != 2 {
		t.Fatalf("gapped manifest pass = %+v schedules=%d", gapped, scheduleCalls.Load())
	}
	afterGapWatermark, err := db.GetSDStationMD5(ctx, lineupID, stationID)
	if err != nil || afterGapWatermark != withdrawnWatermark {
		t.Fatalf("gapped manifest changed watermark = %q want %q err %v", afterGapWatermark, withdrawnWatermark, err)
	}
	var retainedRows int
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM epg_program WHERE channel_id=$1`, channel.ID).Scan(&retainedRows); err != nil {
		t.Fatal(err)
	}
	if retainedRows != 3 {
		t.Fatalf("gapped manifest changed last-good rows: got %d want 3", retainedRows)
	}
}

func TestIntegrationSDScheduleMetadataErrorRetainsPriorSnapshotAndWatermark(t *testing.T) {
	db, feed, ingester, channel := setupSDWorkerIntegration(t, false, 9475.76)
	ctx := context.Background()
	_, _, _, _, start, _, _, _, _, _, _, _ := feed.snapshot()
	first := ingester.RunOnce(ctx)
	if first.LineupsOK != 1 || first.ProgramsAdded != 1 {
		t.Fatalf("valid seed stats = %+v", first)
	}
	seedMD5, err := db.GetSDStationMD5(ctx, "FX475-LINEUP", "FX475-OLD")
	if err != nil {
		t.Fatal(err)
	}
	var seedTitle string
	if err := db.Pool.QueryRow(ctx, `
		SELECT title FROM epg_program
		 WHERE channel_id=$1 AND source_hash LIKE 'sd:%'`, channel.ID).Scan(&seedTitle); err != nil {
		t.Fatal(err)
	}

	feed.setProgram("provider-error-md5", "Must not replace prior snapshot", start, 3600, false, false)
	feed.setScheduleCode(5000)
	failed := ingester.RunOnce(ctx)
	if failed.LineupsFailed != 1 || failed.LineupsOK != 0 || failed.StationsFetched != 0 {
		t.Fatalf("metadata-error stats = %+v", failed)
	}
	afterMD5, err := db.GetSDStationMD5(ctx, "FX475-LINEUP", "FX475-OLD")
	if err != nil {
		t.Fatal(err)
	}
	var afterTitle, lineupStatus string
	if err := db.Pool.QueryRow(ctx, `
		SELECT title FROM epg_program
		 WHERE channel_id=$1 AND source_hash LIKE 'sd:%'`, channel.ID).Scan(&afterTitle); err != nil {
		t.Fatal(err)
	}
	if err := db.Pool.QueryRow(ctx, `SELECT last_status FROM sd_lineup WHERE sd_lineup_id='FX475-LINEUP'`).Scan(&lineupStatus); err != nil {
		t.Fatal(err)
	}
	if afterMD5 != seedMD5 || afterTitle != seedTitle {
		t.Fatalf("metadata error changed snapshot: md5 %q->%q title %q->%q",
			seedMD5, afterMD5, seedTitle, afterTitle)
	}
	if lineupStatus != "error: schedules response for station FX475-OLD has metadata code 5000" {
		t.Fatalf("metadata-error lineup status = %q", lineupStatus)
	}
}

func TestIntegrationSDProgramMetadataErrorRetainsPriorSnapshotAndWatermark(t *testing.T) {
	db, feed, ingester, channel := setupSDWorkerIntegration(t, false, 9475.77)
	ctx := context.Background()
	_, programID, _, _, start, _, _, _, _, _, _, _ := feed.snapshot()
	first := ingester.RunOnce(ctx)
	if first.LineupsOK != 1 || first.ProgramsAdded != 1 {
		t.Fatalf("valid seed stats = %+v", first)
	}
	seedMD5, err := db.GetSDStationMD5(ctx, "FX475-LINEUP", "FX475-OLD")
	if err != nil {
		t.Fatal(err)
	}
	var seedTitle string
	if err := db.Pool.QueryRow(ctx, `
		SELECT title FROM epg_program
		 WHERE channel_id=$1 AND source_hash LIKE 'sd:%'`, channel.ID).Scan(&seedTitle); err != nil {
		t.Fatal(err)
	}

	feed.setProgram("queued-program-md5", "Must not publish queued details", start, 3600, false, false)
	feed.setProgramError(6001, "PROGRAMID_QUEUED")
	failed := ingester.RunOnce(ctx)
	if failed.LineupsFailed != 1 || failed.LineupsOK != 0 || failed.StationsFetched != 1 {
		t.Fatalf("program-error stats = %+v", failed)
	}
	afterMD5, err := db.GetSDStationMD5(ctx, "FX475-LINEUP", "FX475-OLD")
	if err != nil {
		t.Fatal(err)
	}
	var afterTitle, lineupStatus string
	if err := db.Pool.QueryRow(ctx, `
		SELECT title FROM epg_program
		 WHERE channel_id=$1 AND source_hash LIKE 'sd:%'`, channel.ID).Scan(&afterTitle); err != nil {
		t.Fatal(err)
	}
	if err := db.Pool.QueryRow(ctx, `SELECT last_status FROM sd_lineup WHERE sd_lineup_id='FX475-LINEUP'`).Scan(&lineupStatus); err != nil {
		t.Fatal(err)
	}
	if afterMD5 != seedMD5 || afterTitle != seedTitle {
		t.Fatalf("program metadata error changed snapshot: md5 %q->%q title %q->%q",
			seedMD5, afterMD5, seedTitle, afterTitle)
	}
	if lineupStatus != "error: program details response for "+programID+" has code 6001" {
		t.Fatalf("program-error lineup status = %q", lineupStatus)
	}
}

func TestIntegrationSDProgramBlankTitleRetainsPriorSnapshotAndWatermark(t *testing.T) {
	db, feed, ingester, channel := setupSDWorkerIntegration(t, false, 9475.78)
	ctx := context.Background()
	_, programID, _, _, start, _, _, _, _, _, _, _ := feed.snapshot()
	first := ingester.RunOnce(ctx)
	if first.LineupsOK != 1 || first.ProgramsAdded != 1 {
		t.Fatalf("valid seed stats = %+v", first)
	}
	seedMD5, err := db.GetSDStationMD5(ctx, "FX475-LINEUP", "FX475-OLD")
	if err != nil {
		t.Fatal(err)
	}
	var seedTitle string
	if err := db.Pool.QueryRow(ctx, `
		SELECT title FROM epg_program
		 WHERE channel_id=$1 AND source_hash LIKE 'sd:%'`, channel.ID).Scan(&seedTitle); err != nil {
		t.Fatal(err)
	}

	feed.setProgram("blank-title-md5", "   ", start, 3600, false, false)
	failed := ingester.RunOnce(ctx)
	if failed.LineupsFailed != 1 || failed.LineupsOK != 0 || failed.StationsFetched != 1 {
		t.Fatalf("blank-title stats = %+v", failed)
	}
	afterMD5, err := db.GetSDStationMD5(ctx, "FX475-LINEUP", "FX475-OLD")
	if err != nil {
		t.Fatal(err)
	}
	var afterTitle, lineupStatus string
	if err := db.Pool.QueryRow(ctx, `
		SELECT title FROM epg_program
		 WHERE channel_id=$1 AND source_hash LIKE 'sd:%'`, channel.ID).Scan(&afterTitle); err != nil {
		t.Fatal(err)
	}
	if err := db.Pool.QueryRow(ctx, `SELECT last_status FROM sd_lineup WHERE sd_lineup_id='FX475-LINEUP'`).Scan(&lineupStatus); err != nil {
		t.Fatal(err)
	}
	if afterMD5 != seedMD5 || afterTitle != seedTitle {
		t.Fatalf("blank program title changed snapshot: md5 %q->%q title %q->%q",
			seedMD5, afterMD5, seedTitle, afterTitle)
	}
	if lineupStatus != "error: program details response for "+programID+" has no non-empty title120" {
		t.Fatalf("blank-title lineup status = %q", lineupStatus)
	}
}

func TestIntegrationSDInvalidOriginalAirDateRetainsPriorSnapshotAndWatermark(t *testing.T) {
	db, feed, ingester, channel := setupSDWorkerIntegration(t, false, 9475.79)
	ctx := context.Background()
	_, programID, _, _, start, _, _, _, _, _, _, _ := feed.snapshot()
	feed.setOriginalAirDate("2020-01-02")
	first := ingester.RunOnce(ctx)
	if first.LineupsOK != 1 || first.ProgramsAdded != 1 {
		t.Fatalf("valid OAD seed stats = %+v", first)
	}
	seedMD5, err := db.GetSDStationMD5(ctx, "FX475-LINEUP", "FX475-OLD")
	if err != nil {
		t.Fatal(err)
	}
	var seedTitle string
	var seedOAD time.Time
	if err := db.Pool.QueryRow(ctx, `
		SELECT title, original_air_date FROM epg_program
		 WHERE channel_id=$1 AND source_hash LIKE 'sd:%'`, channel.ID).Scan(&seedTitle, &seedOAD); err != nil {
		t.Fatal(err)
	}

	feed.setProgram("invalid-oad-md5", "Must not publish invalid OAD", start, 3600, false, false)
	feed.setOriginalAirDate("2020-02-30")
	failed := ingester.RunOnce(ctx)
	if failed.LineupsFailed != 1 || failed.LineupsOK != 0 || failed.StationsFetched != 1 {
		t.Fatalf("invalid-OAD stats = %+v", failed)
	}
	afterMD5, err := db.GetSDStationMD5(ctx, "FX475-LINEUP", "FX475-OLD")
	if err != nil {
		t.Fatal(err)
	}
	var afterTitle, lineupStatus string
	var afterOAD time.Time
	if err := db.Pool.QueryRow(ctx, `
		SELECT title, original_air_date FROM epg_program
		 WHERE channel_id=$1 AND source_hash LIKE 'sd:%'`, channel.ID).Scan(&afterTitle, &afterOAD); err != nil {
		t.Fatal(err)
	}
	if err := db.Pool.QueryRow(ctx, `SELECT last_status FROM sd_lineup WHERE sd_lineup_id='FX475-LINEUP'`).Scan(&lineupStatus); err != nil {
		t.Fatal(err)
	}
	if afterMD5 != seedMD5 || afterTitle != seedTitle || !afterOAD.Equal(seedOAD) {
		t.Fatalf("invalid OAD changed snapshot: md5 %q->%q title %q->%q OAD %s->%s",
			seedMD5, afterMD5, seedTitle, afterTitle, seedOAD, afterOAD)
	}
	if lineupStatus != `error: station FX475-OLD program `+programID+` has invalid original air date "2020-02-30"` {
		t.Fatalf("invalid-OAD lineup status = %q", lineupStatus)
	}
}

func TestIntegrationSDProgramDetailMD5ErrorsRetainPriorSnapshotAndWatermark(t *testing.T) {
	db, feed, ingester, channel := setupSDWorkerIntegration(t, false, 9475.8)
	ctx := context.Background()
	_, programID, _, _, start, _, _, _, _, _, _, _ := feed.snapshot()
	first := ingester.RunOnce(ctx)
	if first.LineupsOK != 1 || first.ProgramsAdded != 1 {
		t.Fatalf("valid detail-MD5 seed stats = %+v", first)
	}
	seedMD5, err := db.GetSDStationMD5(ctx, "FX475-LINEUP", "FX475-OLD")
	if err != nil {
		t.Fatal(err)
	}
	var seedTitle string
	if err := db.Pool.QueryRow(ctx, `
		SELECT title FROM epg_program
		 WHERE channel_id=$1 AND source_hash LIKE 'sd:%'`, channel.ID).Scan(&seedTitle); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name, manifestMD5, detailMD5, wantStatus string
	}{
		{
			name: "mismatch", manifestMD5: "detail-mismatch-manifest", detailMD5: "stale-detail-md5",
			wantStatus: "error: program details response for " + programID + " MD5 does not match schedule",
		},
		{
			name: "missing", manifestMD5: "detail-missing-manifest", detailMD5: "__omit__",
			wantStatus: "error: program details response for " + programID + " has an empty MD5",
		},
	}
	for _, testCase := range cases {
		feed.setProgram(testCase.manifestMD5, "Must not publish "+testCase.name+" detail", start, 3600, false, false)
		feed.setProgramMD5Override(testCase.detailMD5)
		failed := ingester.RunOnce(ctx)
		if failed.LineupsFailed != 1 || failed.LineupsOK != 0 || failed.StationsFetched != 1 {
			t.Fatalf("%s detail-MD5 stats = %+v", testCase.name, failed)
		}
		afterMD5, err := db.GetSDStationMD5(ctx, "FX475-LINEUP", "FX475-OLD")
		if err != nil {
			t.Fatal(err)
		}
		var afterTitle, lineupStatus string
		if err := db.Pool.QueryRow(ctx, `
			SELECT title FROM epg_program
			 WHERE channel_id=$1 AND source_hash LIKE 'sd:%'`, channel.ID).Scan(&afterTitle); err != nil {
			t.Fatal(err)
		}
		if err := db.Pool.QueryRow(ctx, `SELECT last_status FROM sd_lineup WHERE sd_lineup_id='FX475-LINEUP'`).Scan(&lineupStatus); err != nil {
			t.Fatal(err)
		}
		if afterMD5 != seedMD5 || afterTitle != seedTitle || lineupStatus != testCase.wantStatus {
			t.Fatalf("%s detail-MD5 changed last-good state: md5 %q->%q title %q->%q status %q",
				testCase.name, seedMD5, afterMD5, seedTitle, afterTitle, lineupStatus)
		}
	}
}

func TestIntegrationSDBlockedStationDoesNotStarveLaterStation(t *testing.T) {
	db := freshSDIntegrationDB(t)
	ctx := context.Background()
	start := time.Now().UTC().Truncate(time.Minute).Add(48 * time.Hour)
	scheduleDate := time.Now().UTC().Format("2006-01-02")

	const (
		lineupID        = "FX475-TWO-STATION-LINEUP"
		blockedStation  = "FX475-BLOCKED"
		restoredStation = "FX475-RESTORABLE"
		unmappedStation = "FX475-UNMAPPED-QUEUED"
		blockedProgram  = "EP047500000101"
		restoredProgram = "EP047500000102"
		blockedMD5      = "blocked-station-md5"
		restoredMD5     = "restorable-station-md5"
		restoredTitle   = "Later station programme"
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/token" {
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "token": "tok-two-station"})
			return
		}
		if r.Header.Get("token") != "tok-two-station" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		switch r.URL.Path {
		case "/lineups/" + lineupID:
			_ = json.NewEncoder(w).Encode(map[string]any{
				// Deliberately return the blocked station first: the regression is
				// that its typed publication conflict must not abort this lineup.
				"stations": []map[string]any{
					{"stationID": unmappedStation, "name": "Unmapped queued station", "callsign": unmappedStation},
					{"stationID": blockedStation, "name": "Blocked station", "callsign": blockedStation},
					{"stationID": restoredStation, "name": "Restorable station", "callsign": restoredStation},
				},
				"map": []map[string]any{
					{"stationID": unmappedStation, "channel": "47.50"},
					{"stationID": blockedStation, "channel": "47.51"},
					{"stationID": restoredStation, "channel": "47.52"},
				},
				"metadata": map[string]any{"lineup": lineupID, "modified": "2026-08-23T12:00:00Z"},
			})
		case "/schedules/md5":
			var reqs []sd.ScheduleRequest
			if err := json.NewDecoder(r.Body).Decode(&reqs); err != nil {
				t.Errorf("decode mapped MD5 requests: %v", err)
			}
			for _, req := range reqs {
				if req.StationID == unmappedStation {
					t.Error("unmapped station was sent to schedules/md5")
				}
			}
			manifest := map[string]any{}
			for _, req := range reqs {
				dates := req.Dates
				if len(dates) == 0 {
					dates = []string{scheduleDate}
				}
				md5 := blockedMD5
				if req.StationID == restoredStation {
					md5 = restoredMD5
				}
				entries := map[string]any{}
				for _, date := range dates {
					entries[date] = map[string]any{"code": 0, "message": "OK", "md5": md5}
				}
				manifest[req.StationID] = entries
			}
			_ = json.NewEncoder(w).Encode(manifest)
		case "/schedules":
			scheduleDay, _ := time.Parse("2006-01-02", scheduleDate)
			prior := scheduleDay.AddDate(0, 0, -1).Format("2006-01-02")
			priorResponse := func(stationID, md5 string) map[string]any {
				return map[string]any{
					"stationID": stationID,
					"programs": []map[string]any{{
						"programID": "EP000000000000", "airDateTime": prior + "T00:00:00Z",
						"duration": 60, "md5": "expired-prior-day-program-md5",
					}},
					"metadata": map[string]any{"md5": md5, "startDate": prior},
				}
			}
			_ = json.NewEncoder(w).Encode([]map[string]any{
				priorResponse(blockedStation, blockedMD5),
				{
					"stationID": blockedStation,
					"programs": []map[string]any{{
						"programID": blockedProgram, "airDateTime": start.Format(time.RFC3339),
						"duration": 3600, "md5": "blocked-airing-md5",
					}},
					"metadata": map[string]any{
						"modified": "2026-08-23T12:00:00Z", "md5": blockedMD5,
						"startDate": scheduleDate,
					},
				},
				priorResponse(restoredStation, restoredMD5),
				{
					"stationID": restoredStation,
					"programs": []map[string]any{{
						"programID": restoredProgram, "airDateTime": start.Format(time.RFC3339),
						"duration": 3600, "md5": "restorable-airing-md5",
					}},
					"metadata": map[string]any{
						"modified": "2026-08-23T12:00:00Z", "md5": restoredMD5,
						"startDate": scheduleDate,
					},
				},
			})
		case "/programs":
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{
					"programID": blockedProgram,
					"md5":       "blocked-airing-md5",
					"titles":    []map[string]string{{"title120": "Blocked station programme"}},
					"genres":    []string{"Series"},
				},
				{
					"programID": restoredProgram,
					"md5":       "restorable-airing-md5",
					"titles":    []map[string]string{{"title120": restoredTitle}},
					"genres":    []string{"Series"},
				},
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)

	if _, err := db.CreateSDLineup(ctx, lineupID, "FX475 two-station lineup", "test", true); err != nil {
		t.Fatal(err)
	}
	blockedChannel, err := db.CreateChannel(ctx, store.Channel{
		Number: 9475.8, Name: "Blocked SD", CallSign: "FX475BLOCKED",
		EpgChannelID: blockedStation, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	restoredChannel, err := db.CreateChannel(ctx, store.Channel{
		Number: 9475.9, Name: "Restorable SD", CallSign: "FX475RESTORE",
		EpgChannelID: restoredStation, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.UpsertEPGProgram(ctx, store.EPGProgram{
		ChannelID: blockedChannel.ID, StartAt: start, EndAt: start.Add(time.Hour),
		Title: "Stronger direct owner", SourceHash: "manual:strong-owner", SourcePriority: -20,
	}); err != nil {
		t.Fatal(err)
	}

	client := sd.New(server.URL, "sd-user", "password")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	stats := sd.NewIngester(logger, client, db, time.Hour).RunOnce(ctx)
	if stats.LineupsFailed != 0 || stats.LineupsOK != 1 ||
		stats.StationsTotal != 3 || stats.UnmappedStations != 1 || stats.StationsFetched != 2 || stats.StationsBlocked != 1 ||
		stats.ProgramsAdded != 1 {
		t.Fatalf("two-station pass stats = %+v", stats)
	}

	blockedWatermark, err := db.GetSDStationMD5(ctx, lineupID, blockedStation)
	if err != nil {
		t.Fatal(err)
	}
	restoredWatermark, err := db.GetSDStationMD5(ctx, lineupID, restoredStation)
	if err != nil {
		t.Fatal(err)
	}
	var blockedRows, restoredRows int
	var publishedTitle, lineupStatus string
	if err := db.Pool.QueryRow(ctx, `
		SELECT count(*) FROM epg_program
		 WHERE channel_id=$1 AND source_hash LIKE 'sd:%'`, blockedChannel.ID).Scan(&blockedRows); err != nil {
		t.Fatal(err)
	}
	if err := db.Pool.QueryRow(ctx, `
		SELECT count(*), min(title) FROM epg_program
		 WHERE channel_id=$1 AND source_hash LIKE 'sd:%'`, restoredChannel.ID).Scan(&restoredRows, &publishedTitle); err != nil {
		t.Fatal(err)
	}
	if err := db.Pool.QueryRow(ctx, `
		SELECT last_status FROM sd_lineup
		 WHERE sd_lineup_id=$1`, lineupID).Scan(&lineupStatus); err != nil {
		t.Fatal(err)
	}
	if blockedWatermark != "" || blockedRows != 0 {
		t.Fatalf("blocked station advanced state: md5=%q rows=%d", blockedWatermark, blockedRows)
	}
	if restoredWatermark == "" || restoredRows != 1 || publishedTitle != restoredTitle {
		t.Fatalf("later station state: md5=%q rows=%d title=%q", restoredWatermark, restoredRows, publishedTitle)
	}
	if lineupStatus != "partial: Schedules Direct candidate did not own its direct slot: 1 station snapshot blocked (FX475-BLOCKED)" {
		t.Fatalf("two-station lineup status = %q", lineupStatus)
	}
}
