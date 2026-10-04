// cancel_integration_test.go — a recording booked from Sonarr's wanted list is
// cancelled once Sonarr stops wanting the episode, against a real Postgres.
//
//	CONDUCTOR_INT_TEST=1 CONDUCTOR_TEST_DSN=postgres://…/conductor_test go test -race -count=1 ./internal/reconcile -run Integration
package reconcile

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spencercnorton/conductor/internal/store"
	"github.com/spencercnorton/conductor/internal/testdb"
)

func TestIntegrationReconcileCancelsBookingOnceSonarrHasTheEpisode(t *testing.T) {
	if os.Getenv("CONDUCTOR_INT_TEST") != "1" {
		t.Skip("integration test skipped (set CONDUCTOR_INT_TEST=1 to enable)")
	}
	base := os.Getenv("CONDUCTOR_TEST_DSN")
	if base == "" {
		t.Skip("CONDUCTOR_TEST_DSN is required for the reconcile integration test")
	}
	ctx := context.Background()
	db, err := store.Open(ctx, testdb.New(t, base))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if err := db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	ch, err := db.CreateChannel(ctx, store.Channel{
		Number: 103, Name: "Reconcile Test", EpgChannelID: "reconcile.test", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now().Add(3 * time.Hour).UTC().Truncate(time.Second)
	if _, err := db.Pool.Exec(ctx, `
		INSERT INTO epg_program (channel_id, start_at, end_at, title, sub_title, source_hash)
		VALUES ($1, $2, $3, 'Airport Security', 'High Heels', 'reconcile-cancel-test')`,
		ch.ID, start, start.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	// Sonarr: 1 = the episode is wanted, 0 = it has a file now, -1 = Sonarr is down.
	var wanted atomic.Int32
	wanted.Store(1)
	sonarr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v3/wanted/missing" {
			http.NotFound(w, r)
			return
		}
		page := sonarrMissingPage{Records: []sonarrEpisode{}}
		switch wanted.Load() {
		case -1:
			http.Error(w, "down", http.StatusInternalServerError)
			return
		case 1:
			page.Records = append(page.Records, sonarrEpisode{
				Title: "High Heels", SeasonNumber: 1, EpisodeNumber: 6, Monitored: true,
				Series: &sonarrSeries{Title: "Airport Security", Year: 2015},
			})
		}
		page.TotalRecords = len(page.Records)
		_ = json.NewEncoder(w).Encode(page)
	}))
	t.Cleanup(sonarr.Close)

	w := &Worker{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		DB:     db, Sonarr: NewSonarrClient(sonarr.URL, "test-key"), OutputDir: t.TempDir(),
	}
	state := func() store.DVRRecording {
		t.Helper()
		rows, err := db.ListDVRRecordings(ctx, "", 10)
		if err != nil || len(rows) != 1 {
			t.Fatalf("recordings = %+v err=%v, want exactly one", rows, err)
		}
		return rows[0]
	}

	w.reconcile(ctx)
	if got := state(); got.State != "scheduled" || got.RequestedBy != "reconcile:sonarr" {
		t.Fatalf("after the wanted cycle: %+v", got)
	}

	wanted.Store(0) // grabbed elsewhere
	w.reconcile(ctx)
	if got := state(); got.State != "scheduled" {
		t.Fatalf("one unwanted cycle must not cancel yet, state=%s", got.State)
	}
	wanted.Store(-1) // a failed wanted-list read judges nothing and cancels nothing
	w.reconcile(ctx)
	if got := state(); got.State != "scheduled" {
		t.Fatalf("a Sonarr failure cancelled the booking, state=%s", got.State)
	}
	wanted.Store(0)
	w.reconcile(ctx)
	got := state()
	if got.State != "cancelled" || !strings.Contains(got.Error, "no longer wants") {
		t.Fatalf("second consecutive unwanted cycle: state=%s error=%q, want cancelled", got.State, got.Error)
	}
}
