// dvr_cancel_unwanted_integration_test.go — the reconciler's cancel of a
// booked recording nobody wants any more, against a real Postgres. Shares the
// pg bootstrap helpers in lease_integration_test.go; same gating:
//
//	CONDUCTOR_INT_TEST=1 go test -race -count=1 ./internal/store -run Integration
package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/spencercnorton/conductor/internal/store"
)

func TestIntegrationCancelUnstartedDVRRecordingSparesStartedAndImminentRows(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	ch, err := db.CreateChannel(ctx, store.Channel{
		Number: 41, Name: "Cancel Test", EpgChannelID: "cancel.test", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	book := func(title, by string, startsIn time.Duration) store.DVRRecording {
		t.Helper()
		start := now.Add(startsIn)
		rec, err := db.CreateDVRRecording(ctx, store.DVRRecording{
			ChannelID: ch.ID, Title: title, ScheduledStart: start, ScheduledEnd: start.Add(30 * time.Minute),
			RequestedBy: by, OutputPath: "/dvr/TV/" + title + ".ts",
		})
		if err != nil {
			t.Fatalf("book %s: %v", title, err)
		}
		return rec
	}
	later := book("Later", "reconcile:sonarr", 3*time.Hour)
	imminent := book("Imminent", "reconcile:sonarr", 5*time.Minute)
	manual := book("Manual", "plex", 3*time.Hour)
	movie := book("Movie", "reconcile:radarr", 4*time.Hour)
	running := book("Running", "reconcile:sonarr", 5*time.Hour)
	if ok, err := db.TryMarkDVRRecordingStarted(ctx, running.ID, running.OutputPath); err != nil || !ok {
		t.Fatalf("claim running: ok=%v err=%v", ok, err)
	}

	guard := now.Add(15 * time.Minute)
	rows, err := db.ListUnstartedDVRRecordingsByRequester(ctx, []string{"reconcile:sonarr"}, guard)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ID != later.ID {
		t.Fatalf("listed %d rows, want only %q (not the imminent, manual, radarr or running rows): %+v",
			len(rows), later.Title, rows)
	}
	rows, err = db.ListUnstartedDVRRecordingsByRequester(ctx, []string{"reconcile:sonarr", "reconcile:radarr"}, guard)
	if err != nil || len(rows) != 2 || rows[0].ID != later.ID || rows[1].ID != movie.ID {
		t.Fatalf("both sources listed %+v err=%v, want Later then Movie", rows, err)
	}

	reason := "cancelled: reconcile:sonarr no longer wants this recording"
	if ok, err := db.CancelUnstartedDVRRecording(ctx, later.ID, guard, reason); err != nil || !ok {
		t.Fatalf("cancel later: ok=%v err=%v", ok, err)
	}
	got, err := db.GetDVRRecording(ctx, later.ID)
	if err != nil || got.State != "cancelled" || got.Error != reason || got.CompletedAt == nil {
		t.Fatalf("later after cancel = %+v err=%v", got, err)
	}
	for _, r := range []store.DVRRecording{later, imminent, running} {
		ok, err := db.CancelUnstartedDVRRecording(ctx, r.ID, guard, reason)
		if err != nil || ok {
			t.Fatalf("cancel %s must be a no-op (already cancelled, inside the guard, or started): ok=%v err=%v",
				r.Title, ok, err)
		}
	}
	for _, r := range []store.DVRRecording{imminent, manual} {
		if got, _ := db.GetDVRRecording(ctx, r.ID); got.State != "scheduled" {
			t.Fatalf("%s state = %s, want scheduled", r.Title, got.State)
		}
	}
	if got, _ := db.GetDVRRecording(ctx, running.ID); got.State != "recording" {
		t.Fatalf("running state = %s, want recording", got.State)
	}
}
