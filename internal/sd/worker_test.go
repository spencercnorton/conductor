package sd

import (
	"reflect"
	"testing"
	"time"
)

func TestValidateLineupAuthority(t *testing.T) {
	const lineupID = "FX475-AUTHORITY"
	validStations := []Station{{StationID: "station-a"}, {StationID: "station-b"}}
	validMap := []ChannelMap{
		{StationID: "station-a", Channel: "1"},
		{StationID: "station-a", Channel: "101"}, // duplicate map ownership is valid
		{StationID: "station-b", Channel: "2"},
	}
	for _, test := range []struct {
		name   string
		lineup *LineupResp
	}{
		{
			name: "complete authority",
			lineup: &LineupResp{
				Stations: validStations, Map: validMap, Metadata: LineupMeta{Lineup: lineupID},
			},
		},
		{
			name: "explicit authoritative empty",
			lineup: &LineupResp{
				Stations: []Station{}, Map: []ChannelMap{}, Metadata: LineupMeta{Lineup: lineupID},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			stationIDs, err := validateLineupAuthority(lineupID, test.lineup)
			if err != nil {
				t.Fatal(err)
			}
			if len(stationIDs) != len(test.lineup.Stations) {
				t.Fatalf("station ids = %v, want %d entries", stationIDs, len(test.lineup.Stations))
			}
		})
	}

	for _, test := range []struct {
		name   string
		lineup *LineupResp
	}{
		{name: "null response", lineup: nil},
		{name: "empty error object", lineup: &LineupResp{}},
		{
			name: "wrong identity",
			lineup: &LineupResp{
				Stations: validStations, Map: validMap, Metadata: LineupMeta{Lineup: "wrong-lineup"},
			},
		},
		{
			name: "omitted stations",
			lineup: &LineupResp{
				Map: validMap, Metadata: LineupMeta{Lineup: lineupID},
			},
		},
		{
			name: "omitted map",
			lineup: &LineupResp{
				Stations: validStations, Metadata: LineupMeta{Lineup: lineupID},
			},
		},
		{
			name: "empty station id",
			lineup: &LineupResp{
				Stations: []Station{{StationID: " "}}, Map: []ChannelMap{{StationID: " "}},
				Metadata: LineupMeta{Lineup: lineupID},
			},
		},
		{
			name: "whitespace-padded station id",
			lineup: &LineupResp{
				Stations: []Station{{StationID: " station-a"}}, Map: []ChannelMap{{StationID: " station-a"}},
				Metadata: LineupMeta{Lineup: lineupID},
			},
		},
		{
			name: "duplicate station description",
			lineup: &LineupResp{
				Stations: []Station{{StationID: "station-a"}, {StationID: "station-a"}},
				Map:      []ChannelMap{{StationID: "station-a"}}, Metadata: LineupMeta{Lineup: lineupID},
			},
		},
		{
			name: "empty mapped station id",
			lineup: &LineupResp{
				Stations: []Station{}, Map: []ChannelMap{{StationID: " "}},
				Metadata: LineupMeta{Lineup: lineupID},
			},
		},
		{
			name: "whitespace-padded mapped station id",
			lineup: &LineupResp{
				Stations: []Station{{StationID: "station-a"}}, Map: []ChannelMap{{StationID: "station-a "}},
				Metadata: LineupMeta{Lineup: lineupID},
			},
		},
		{
			name: "station missing from map",
			lineup: &LineupResp{
				Stations: []Station{{StationID: "station-a"}}, Map: []ChannelMap{},
				Metadata: LineupMeta{Lineup: lineupID},
			},
		},
		{
			name: "map references undescribed station",
			lineup: &LineupResp{
				Stations: []Station{}, Map: []ChannelMap{{StationID: "station-a"}},
				Metadata: LineupMeta{Lineup: lineupID},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := validateLineupAuthority(lineupID, test.lineup); err == nil {
				t.Fatal("malformed lineup authority unexpectedly accepted")
			}
		})
	}
}

func TestPriorScheduleRequestsAtUTCMidnightRollover(t *testing.T) {
	reqs := []ScheduleRequest{{StationID: "station-a"}}
	finished := time.Date(2026, 8, 24, 0, 0, 1, 0, time.UTC)

	t.Run("default response already includes finished-day prior date", func(t *testing.T) {
		authorities := map[string]stationScheduleAuthority{
			"station-a": {Dates: []string{"2026-08-23", "2026-08-24", "2026-08-25"}},
		}
		if got := priorScheduleRequests(reqs, authorities, finished); len(got) != 0 {
			t.Fatalf("prior requests = %+v, want none", got)
		}
	})

	t.Run("default response begins on finished UTC date", func(t *testing.T) {
		authorities := map[string]stationScheduleAuthority{
			"station-a": {Dates: []string{"2026-08-24", "2026-08-25"}},
		}
		want := []ScheduleRequest{{StationID: "station-a", Dates: []string{"2026-08-23"}}}
		if got := priorScheduleRequests(reqs, authorities, finished); !reflect.DeepEqual(got, want) {
			t.Fatalf("prior requests = %+v, want %+v", got, want)
		}
	})
}
