package api

import (
	"context"
	"errors"
	"github.com/spencercnorton/conductor/internal/testdb"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/dvr"
	"github.com/spencercnorton/conductor/internal/store"
	"github.com/spencercnorton/conductor/internal/stream"
)

func openAPICancelIntegrationDB(t *testing.T) *store.DB {
	t.Helper()
	if os.Getenv("CONDUCTOR_INT_TEST") != "1" {
		t.Skip("integration test skipped (set CONDUCTOR_INT_TEST=1 to enable)")
	}
	dsn := os.Getenv("CONDUCTOR_API_TEST_DSN")
	if dsn == "" {
		t.Skip("CONDUCTOR_API_TEST_DSN is required for API cancellation integration tests")
	}
	db, err := store.Open(context.Background(), testdb.New(t, dsn))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if err := db.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	return db
}

func waitForAPICancel(t *testing.T, what string, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// PostgreSQL can commit the cancellation UPDATE while its response is lost.
// Even though the handler must report that database error, it must still stop
// the process-local recorder; otherwise the cancelled row drops its active
// output-path guard while the old writer and provider lease remain live.
func TestIntegrationAmbiguousDVRCancellationStillStopsLocalWriter(t *testing.T) {
	db := openAPICancelIntegrationDB(t)
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	upstreamStopped := make(chan struct{})
	var stoppedOnce sync.Once
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		flusher, _ := w.(http.Flusher)
		block := make([]byte, 188*7)
		for {
			select {
			case <-req.Context().Done():
				stoppedOnce.Do(func() { close(upstreamStopped) })
				return
			case <-time.After(2 * time.Millisecond):
			}
			if _, err := w.Write(block); err != nil {
				stoppedOnce.Do(func() { close(upstreamStopped) })
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
	}))
	// Registered before scheduler cleanup so LIFO cleanup stops any writer
	// before Close waits for the server's active request to return.
	t.Cleanup(upstream.Close)

	provider, err := db.CreateProvider(ctx, store.Provider{
		Name: "api-cancel-" + uuid.NewString(), Kind: "m3u_xtream",
		BaseURL: upstream.URL, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateCredential(ctx, store.ProviderCredential{
		ProviderID: provider.ID, Username: "api-cancel", PasswordEnc: []byte{1},
		MaxStreams: 1, Priority: 100, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	var channelNumber float64
	if err := db.Pool.QueryRow(ctx,
		`SELECT (COALESCE(MAX(number), 9899) + 0.1)::float8 FROM channel`).Scan(&channelNumber); err != nil {
		t.Fatal(err)
	}
	channel, err := db.CreateChannel(ctx, store.Channel{
		Number: channelNumber, Name: "api-cancel-" + uuid.NewString(), Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateChannelSource(ctx, store.ChannelSource{
		ChannelID: channel.ID, ProviderID: provider.ID, UpstreamURL: upstream.URL,
		Priority: 0, HealthScore: 1, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}

	outputPath := t.TempDir() + "/ambiguous-cancel.ts"
	start := time.Now().Add(250 * time.Millisecond)
	recording, err := db.CreateDVRRecording(ctx, store.DVRRecording{
		ChannelID: channel.ID, Title: "ambiguous cancel " + uuid.NewString(),
		ScheduledStart: start, ScheduledEnd: start.Add(30 * time.Second),
		Priority: 100, RequestedBy: "api-cancel-test", OutputPath: outputPath,
	}, store.DVRAdmissionPolicy{LiveReserve: 0, StartSlack: time.Second, MaxOverrun: 0})
	if err != nil {
		t.Fatal(err)
	}

	pool := stream.NewPool(logger, db, store.PassthroughResolverForTest{})
	recorder := dvr.NewRecorder(logger, db, pool, nil)
	recorder.StartSlack = time.Second
	recorder.MaxOverrun = 0
	scheduler := dvr.NewScheduler(logger, db, recorder)
	scheduler.LiveReserve = 0
	scheduler.TickInterval = 30 * time.Second
	runCtx, stopScheduler := context.WithCancel(context.Background())
	go scheduler.Run(runCtx)
	t.Cleanup(func() {
		stopScheduler()
		waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = scheduler.Wait(waitCtx)
		cancel()
		pool.Close()
	})

	partial := outputPath + ".capture." + recording.ID.String() + ".partial"
	waitForAPICancel(t, "active DVR writer", 5*time.Second, func() bool {
		info, statErr := os.Stat(partial)
		if statErr != nil || info.Size() == 0 {
			return false
		}
		current, getErr := db.GetDVRRecording(ctx, recording.ID)
		return getErr == nil && current.State == "recording"
	})

	simulated := errors.New("simulated lost cancellation COMMIT response")
	a := &admin{
		d: AdminDeps{DB: db, DVRScheduler: scheduler},
		markDVRRecordingCancelled: func(callCtx context.Context, id uuid.UUID) error {
			if err := db.MarkDVRRecordingCancelled(callCtx, id); err != nil {
				return err
			}
			// Replace only the client-visible result after the real UPDATE has
			// committed, matching a lost network response at the boundary.
			return simulated
		},
	}
	req := httptest.NewRequest(http.MethodDelete,
		"/admin/dvr/recordings/"+recording.ID.String(), nil)
	req.SetPathValue("id", recording.ID.String())
	response := httptest.NewRecorder()
	a.cancelDVRRecording(response, req)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("ambiguous cancellation status=%d body=%s, want 500",
			response.Code, response.Body.String())
	}

	select {
	case <-upstreamStopped:
	case <-time.After(5 * time.Second):
		t.Fatal("ambiguous database response left the local DVR writer running")
	}
	waitForAPICancel(t, "provider lease release", 5*time.Second, func() bool {
		var active int
		err := db.Pool.QueryRow(ctx, `
			SELECT COUNT(*)::int FROM active_stream
			 WHERE client_count <> 0 OR state IN ('starting','running','draining')`).Scan(&active)
		return err == nil && active == 0
	})
	current, err := db.GetDVRRecording(ctx, recording.ID)
	if err != nil || current.State != "cancelled" {
		t.Fatalf("durable cancellation row=%+v err=%v", current, err)
	}
	if _, err := os.Stat(outputPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cancelled recording unexpectedly finalized: %v", err)
	}
	if info, err := os.Stat(partial); err != nil || info.Size() == 0 {
		t.Fatalf("cancelled recording partial missing: info=%v err=%v", info, err)
	}
}
