// pool_integration_test.go — Pool-level Phase 5 behavior against a real
// Postgres + live httptest upstreams: mid-stream source failover end-to-end
// (S1) and the headers-timeout 503 path (S4). Gated like the store suite:
//
//	CONDUCTOR_INT_TEST=1 go test -race -count=1 ./internal/stream -run Integration
//
// Spins its own throwaway pg container (distinct name/port from the store
// suite's — go test runs packages in parallel).
package stream_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/spencercnorton/conductor/internal/security"
	"github.com/spencercnorton/conductor/internal/store"
	"github.com/spencercnorton/conductor/internal/stream"
)

const (
	streamPGImage     = "postgres:16-alpine"
	streamPGContainer = "conductor-stream-test-pg"
	streamPGPort      = "55437"
	streamPGPassword  = "conductor-test"
)

func skipIfNoIntegration(t *testing.T) {
	t.Helper()
	if os.Getenv("CONDUCTOR_INT_TEST") != "1" {
		t.Skip("integration test skipped (set CONDUCTOR_INT_TEST=1 to enable)")
	}
}

func startTestPostgres(t *testing.T) string {
	t.Helper()
	if dsn := os.Getenv("CONDUCTOR_STREAM_TEST_DSN"); dsn != "" {
		return dsn
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not available and CONDUCTOR_STREAM_TEST_DSN not set")
	}
	_ = exec.Command("docker", "rm", "-f", "-v", streamPGContainer).Run()

	cmd := exec.Command("docker", "run", "-d",
		"--name", streamPGContainer,
		"-p", streamPGPort+":5432",
		"-e", "POSTGRES_PASSWORD="+streamPGPassword,
		"-e", "POSTGRES_DB=conductor_test",
		streamPGImage)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("docker run: %v: %s", err, out)
	}
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", "-v", streamPGContainer).Run() })

	dsn := fmt.Sprintf("postgres://postgres:%s@127.0.0.1:%s/conductor_test?sslmode=disable",
		streamPGPassword, streamPGPort)
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := pgx.Connect(context.Background(), dsn)
		if err == nil {
			_ = conn.Close(context.Background())
			return dsn
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatal("postgres container never became reachable")
	return ""
}

func freshStreamDB(t *testing.T) *store.DB {
	t.Helper()
	skipIfNoIntegration(t)
	dsn := startTestPostgres(t)
	db, err := store.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(db.Close)
	if err := db.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return db
}

// seedChannel creates provider + one credential (maxStreams slots) + a
// channel with one source per upstream URL at ascending priority.
func seedChannel(t *testing.T, db *store.DB, maxStreams int, urls ...string) (store.Channel, []store.ChannelSource) {
	t.Helper()
	ctx := context.Background()
	p, err := db.CreateProvider(ctx, store.Provider{
		Name: "stream-test-provider", Kind: "m3u_xtream",
		BaseURL: "http://provider.test", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	rawKey := make([]byte, 32)
	_, _ = rand.Read(rawKey)
	k, err := security.LoadCredKey(hex.EncodeToString(rawKey))
	if err != nil {
		t.Fatal(err)
	}
	enc, _ := k.Encrypt([]byte("password"))
	if _, err := db.CreateCredential(ctx, store.ProviderCredential{
		ProviderID: p.ID, Username: "user", PasswordEnc: enc,
		MaxStreams: maxStreams, Priority: 100, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	ch, err := db.CreateChannel(ctx, store.Channel{Number: 901.0, Name: "pool-int-test", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	var sources []store.ChannelSource
	for i, u := range urls {
		src, err := db.CreateChannelSource(ctx, store.ChannelSource{
			ChannelID: ch.ID, ProviderID: p.ID,
			UpstreamURL: u, Priority: i, HealthScore: 1.0, Enabled: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		sources = append(sources, src)
	}
	return ch, sources
}

// byteServer streams 1KB blocks until stopped.
func byteServer(t *testing.T) (*httptest.Server, func()) {
	t.Helper()
	stop := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		fl, _ := w.(http.Flusher)
		block := make([]byte, 1024)
		for {
			select {
			case <-stop:
				return
			case <-r.Context().Done():
				return
			case <-time.After(2 * time.Millisecond):
			}
			if _, err := w.Write(block); err != nil {
				return
			}
			if fl != nil {
				fl.Flush()
			}
		}
	}))
	var once atomic.Bool
	return srv, func() {
		if once.CompareAndSwap(false, true) {
			close(stop)
			srv.CloseClientConnections()
			srv.Close()
		}
	}
}

// trackedByteServer is a continuous MPEG-TS-like origin with observable
// request lifetime. It lets the held-reservation regression prove that the
// original pump has really exited before PostgreSQL is relocated.
func trackedByteServer(t *testing.T) (*httptest.Server, *atomic.Int64, *atomic.Int64) {
	t.Helper()
	var requests atomic.Int64
	var active atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		active.Add(1)
		defer active.Add(-1)
		w.Header().Set("Content-Type", "video/mp2t")
		flusher, _ := w.(http.Flusher)
		block := make([]byte, 1024)
		for {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(2 * time.Millisecond):
			}
			if _, err := w.Write(block); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
	}))
	t.Cleanup(func() {
		srv.CloseClientConnections()
		srv.Close()
	})
	return srv, &requests, &active
}

type countingWriter struct{ n atomic.Int64 }

func (c *countingWriter) Write(p []byte) (int, error) {
	c.n.Add(int64(len(p)))
	return len(p), nil
}

type blockAfterFirstWriter struct {
	calls   atomic.Int32
	blocked chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *blockAfterFirstWriter) Write(p []byte) (int, error) {
	if w.calls.Add(1) == 2 {
		w.once.Do(func() { close(w.blocked) })
		<-w.release
	}
	return len(p), nil
}

type timestampCountingWriter struct {
	n      atomic.Int64
	lastAt atomic.Int64
}

func (w *timestampCountingWriter) Write(p []byte) (int, error) {
	w.n.Add(int64(len(p)))
	w.lastAt.Store(time.Now().UnixNano())
	return len(p), nil
}

// A viewer can keep a shared pump healthy after a stalled DVR subscriber is
// disconnected. The recording's coverage boundary must come only from real
// chunks that this DVR sink actually wrote; pump-wide media from the viewer
// must never make the truncated recording look salvageable.
func TestIntegrationStalledDVRLastRealIsSubscriberSpecific(t *testing.T) {
	db := freshStreamDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	upstream, stop := byteServer(t)
	defer stop()
	ch, _ := seedChannel(t, db, 1, upstream.URL)

	pool := stream.NewPool(logger, db, store.PassthroughResolverForTest{})
	defer pool.Close()

	viewerCtx, cancelViewer := context.WithCancel(context.Background())
	defer cancelViewer()
	var viewer timestampCountingWriter
	viewerDone := make(chan error, 1)
	go func() {
		_, _, serveErr := pool.ServeWriter(viewerCtx, &viewer, ch.ID)
		viewerDone <- serveErr
	}()
	waitFor(t, "viewer media before DVR attaches", 5*time.Second, func() bool {
		return viewer.lastAt.Load() != 0
	})

	slowReservation, err := pool.ReserveWriter(context.Background(), ch.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	slowSink := &blockAfterFirstWriter{
		blocked: make(chan struct{}),
		release: make(chan struct{}),
	}
	type serveResult struct {
		bytes int64
		last  time.Time
		err   error
	}
	slowDone := make(chan serveResult, 1)
	go func() {
		bytesWritten, lastReal, serveErr := slowReservation.Serve(
			context.Background(), slowSink)
		slowDone <- serveResult{bytes: bytesWritten, last: lastReal, err: serveErr}
	}()
	select {
	case <-slowSink.blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("DVR sink did not block on its second write")
	}

	// the DVR worker may spend its entire five-second budget blocked,
	// but the live viewer must keep advancing throughout that interval. The
	// old sequential fanout froze this viewer until the DVR was dropped.
	// Include enough time for the 32-entry public handoff to fill at the
	// relay's 50 ms partial-flush cadence before the five-second queue-age
	// budget expires. Releasing the writer only after that boundary proves the
	// isolated worker, rather than this test, closed the DVR subscription.
	for sample := 0; sample < 10; sample++ {
		before := viewer.lastAt.Load()
		time.Sleep(750 * time.Millisecond)
		after := viewer.lastAt.Load()
		if after <= before {
			t.Fatalf("viewer stopped advancing during blocked DVR interval %d: before=%d after=%d",
				sample, before, after)
		}
	}
	viewerContinuedAt := time.Unix(0, viewer.lastAt.Load())
	close(slowSink.release)

	var result serveResult
	select {
	case result = <-slowDone:
	case <-time.After(3 * time.Second):
		t.Fatal("stalled DVR did not drain its closed subscriber queue")
	}
	if !errors.Is(result.err, stream.ErrStreamEnded) {
		t.Fatalf("stalled DVR error=%v, want ErrStreamEnded", result.err)
	}
	if result.bytes == 0 || result.last.IsZero() {
		t.Fatalf("stalled DVR bytes=%d last_real=%v, want written real media", result.bytes, result.last)
	}
	if gap := viewerContinuedAt.Sub(result.last); gap < 4*time.Second {
		t.Fatalf("DVR last_real=%v followed pump-wide viewer media at %v (gap %v); want subscriber-specific pre-drop boundary",
			result.last, viewerContinuedAt, gap)
	}

	cancelViewer()
	select {
	case <-viewerDone:
	case <-time.After(3 * time.Second):
		t.Fatal("viewer did not stop after cancellation")
	}
}

type countingResponseWriter struct {
	header http.Header
	status atomic.Int32
	n      atomic.Int64
}

func newCountingResponseWriter() *countingResponseWriter {
	return &countingResponseWriter{header: make(http.Header)}
}

func (w *countingResponseWriter) Header() http.Header  { return w.header }
func (w *countingResponseWriter) WriteHeader(code int) { w.status.Store(int32(code)) }
func (w *countingResponseWriter) Write(p []byte) (int, error) {
	w.n.Add(int64(len(p)))
	return len(p), nil
}
func (w *countingResponseWriter) Flush() {}

var errExactPayloadCaptured = errors.New("exact finite payload captured")

type exactPayloadWriter struct {
	mu       sync.Mutex
	expected []byte
	got      []byte
}

func newExactPayloadWriter(expected []byte) *exactPayloadWriter {
	return &exactPayloadWriter{expected: expected, got: make([]byte, 0, len(expected))}
}

func (w *exactPayloadWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.got)+len(p) > len(w.expected) {
		return 0, fmt.Errorf("received %d bytes beyond finite payload", len(w.got)+len(p)-len(w.expected))
	}
	w.got = append(w.got, p...)
	if len(w.got) == len(w.expected) {
		return len(p), errExactPayloadCaptured
	}
	return len(p), nil
}

func (w *exactPayloadWriter) bytes() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]byte(nil), w.got...)
}

type exactPayloadResponseWriter struct {
	*exactPayloadWriter
	header http.Header
	status int
}

func newExactPayloadResponseWriter(expected []byte) *exactPayloadResponseWriter {
	return &exactPayloadResponseWriter{
		exactPayloadWriter: newExactPayloadWriter(expected),
		header:             make(http.Header),
	}
}

func (w *exactPayloadResponseWriter) Header() http.Header  { return w.header }
func (w *exactPayloadResponseWriter) WriteHeader(code int) { w.status = code }
func (w *exactPayloadResponseWriter) Flush()               {}

type discardHTTPWriter struct {
	header http.Header
	n      atomic.Int64
}

func (w *discardHTTPWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}
func (w *discardHTTPWriter) WriteHeader(_ int) {}
func (w *discardHTTPWriter) Write(p []byte) (int, error) {
	w.n.Add(int64(len(p)))
	return len(p), nil
}

// A Plex/live Serve request and a DVR reservation for the same logical
// channel must share one upstream even when the configured live reserve would
// reject a new channel. This covers the Pool-level handoff, not just the store
// lease primitive.
func TestIntegrationViewerAndDVRShareOneUpstream(t *testing.T) {
	db := freshStreamDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv, stop := byteServer(t)
	defer stop()
	ch, _ := seedChannel(t, db, 1, srv.URL)

	pool := stream.NewPool(logger, db, store.PassthroughResolverForTest{})
	defer pool.Close()
	viewerCtx, cancelViewer := context.WithCancel(context.Background())
	defer cancelViewer()
	w := &discardHTTPWriter{}
	req := httptest.NewRequest(http.MethodGet, "/stream/901", nil).WithContext(viewerCtx)
	viewerDone := make(chan error, 1)
	go func() { viewerDone <- pool.Serve(viewerCtx, w, req, ch.ID) }()
	waitFor(t, "viewer bytes", 15*time.Second, func() bool { return w.n.Load() > 64*1024 })

	reservation, err := pool.ReserveWriter(context.Background(), ch.ID, 1)
	if err != nil {
		t.Fatalf("same-channel DVR reservation: %v", err)
	}
	dvrCtx, cancelDVR := context.WithCancel(context.Background())
	defer cancelDVR()
	dvrWriter := &countingWriter{}
	dvrDone := make(chan error, 1)
	go func() {
		_, _, err := reservation.Serve(dvrCtx, dvrWriter)
		dvrDone <- err
	}()
	waitFor(t, "DVR bytes", 5*time.Second, func() bool { return dvrWriter.n.Load() > 64*1024 })
	var streams, clients int
	if err := db.Pool.QueryRow(context.Background(), `
		SELECT COUNT(*)::int, COALESCE(SUM(client_count), 0)::int
		  FROM active_stream WHERE state IN ('starting','running')`).Scan(&streams, &clients); err != nil {
		t.Fatal(err)
	}
	if streams != 1 || clients != 2 {
		t.Fatalf("streams=%d clients=%d, want one shared upstream/two clients", streams, clients)
	}

	cancelDVR()
	select {
	case <-dvrDone:
	case <-time.After(5 * time.Second):
		t.Fatal("DVR subscriber did not stop")
	}
	cancelViewer()
	select {
	case <-viewerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("viewer did not stop")
	}
}

// Releasing a reservation before Serve transitions its zero-ref row to dead
// and emits an immediate scheduler wake hint. No pump exists to provide an
// OnExit edge in this path.
func TestIntegrationUnservedReservationReleaseWakesCapacity(t *testing.T) {
	db := freshStreamDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ch, _ := seedChannel(t, db, 1, "http://unused.test/stream")
	pool := stream.NewPool(logger, db, store.PassthroughResolverForTest{})
	defer pool.Close()

	reservation, err := pool.ReserveWriter(context.Background(), ch.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	var streamID uuid.UUID
	var tokens int
	if err := db.Pool.QueryRow(context.Background(), `
		SELECT a.id, (SELECT COUNT(*)::int FROM stream_client WHERE active_stream_id = a.id)
		  FROM active_stream a WHERE a.channel_id = $1`, ch.ID).Scan(&streamID, &tokens); err != nil {
		t.Fatal(err)
	}
	if tokens != 1 {
		t.Fatalf("reserved stream tokens=%d, want one durable client", tokens)
	}
	reservation.Release()
	select {
	case <-pool.CapacityChanges():
	case <-time.After(time.Second):
		t.Fatal("unserved reservation release emitted no capacity wake")
	}
	var state string
	var clients int
	if err := db.Pool.QueryRow(context.Background(), `
		SELECT state, client_count,
		       (SELECT COUNT(*)::int FROM stream_client WHERE active_stream_id = a.id)
		  FROM active_stream a WHERE id = $1`, streamID).
		Scan(&state, &clients, &tokens); err != nil {
		t.Fatal(err)
	}
	if state != "dead" || clients != 0 || tokens != 0 {
		t.Fatalf("released reservation row=%s/%d tokens=%d, want dead/0/0", state, clients, tokens)
	}
}

// A replay can observe an earlier zero-client teardown after it committed the
// active row to draining but before its pump-less MarkDead step succeeded. The
// reservation has no local pump to produce an OnExit edge, so replay cleanup
// must finish the conditional draining->dead transition immediately instead of
// withholding the provider slot until the watchdog's next sweep.
func TestIntegrationReplayReleaseFinalizesAlreadyDrainingPumpLessReservation(t *testing.T) {
	db := freshStreamDB(t)
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ch, _ := seedChannel(t, db, 1, "http://unused.test/draining-replay")
	pool := stream.NewPool(logger, db, store.PassthroughResolverForTest{})
	defer pool.Close()

	reservation, err := pool.ReserveWriter(ctx, ch.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	var streamID, clientID uuid.UUID
	if err := db.Pool.QueryRow(ctx, `
		SELECT a.id, sc.id
		  FROM active_stream a
		  JOIN stream_client sc ON sc.active_stream_id = a.id
		 WHERE a.channel_id = $1`, ch.ID).Scan(&streamID, &clientID); err != nil {
		t.Fatal(err)
	}

	// Model an earlier release whose exact-token DELETE committed, followed by
	// a drain transition whose process-local pump-less completion was missed.
	if clients, err := db.ReleaseLeaseClient(ctx, streamID, clientID); err != nil || clients != 0 {
		t.Fatalf("seed zero-client release clients=%d err=%v", clients, err)
	}
	if draining, err := db.MarkDrainingIfIdle(ctx, streamID); err != nil || !draining {
		t.Fatalf("seed draining replay state changed=%v err=%v", draining, err)
	}

	// WriterReservation.Release replays the now-missing exact token. No active or
	// exiting pump exists, so Pool must conditionally mark the draining row dead
	// and publish exactly the capacity edge that OnExit cannot provide.
	reservation.Release()
	select {
	case <-pool.CapacityChanges():
	case <-time.After(time.Second):
		t.Fatal("pump-less draining replay emitted no capacity wake")
	}
	select {
	case <-pool.CapacityChanges():
		t.Fatal("pump-less draining replay emitted duplicate capacity wake")
	default:
	}
	var state string
	var clients int
	if err := db.Pool.QueryRow(ctx, `
		SELECT state::text, client_count FROM active_stream WHERE id = $1`, streamID).
		Scan(&state, &clients); err != nil {
		t.Fatal(err)
	}
	if state != "dead" || clients != 0 {
		t.Fatalf("replayed pump-less row=%s/%d, want dead/0", state, clients)
	}

	// The one-slot provider is immediately reusable; admission must not depend
	// on the 30s watchdog cadence after the physical owner is already absent.
	retried, err := pool.ReserveWriter(ctx, ch.ID, 0)
	if err != nil {
		t.Fatalf("reserve after pump-less drain finalization: %v", err)
	}
	retried.Release()
}

// A DVR admission is a durable capacity owner even before it starts writing.
// If the viewer-created process-local pump idles out meanwhile, OnExit must not
// free the slot or deaden the row. A later Serve reloads DB truth so a relocation
// that happened during the gap starts source B rather than the stale source A.
func TestIntegrationHeldDVRReservationSurvivesPumpExitAndReloadsRelocation(t *testing.T) {
	db := freshStreamDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	sourceA, requestsA, activeA := trackedByteServer(t)
	sourceB, requestsB, _ := trackedByteServer(t)
	channel, sources := seedChannel(t, db, 1, sourceA.URL, sourceB.URL)

	pool := stream.NewPool(logger, db, store.PassthroughResolverForTest{})
	defer pool.Close()
	viewerCtx, cancelViewer := context.WithCancel(context.Background())
	viewerWriter := &discardHTTPWriter{}
	viewerDone := make(chan error, 1)
	go func() {
		req := httptest.NewRequest(http.MethodGet, "/stream/901", nil).WithContext(viewerCtx)
		viewerDone <- pool.Serve(viewerCtx, viewerWriter, req, channel.ID)
	}()
	waitFor(t, "viewer media from source A", 10*time.Second, func() bool {
		return viewerWriter.n.Load() > 64*1024 && requestsA.Load() == 1 && activeA.Load() == 1
	})

	reservation, err := pool.ReserveWriter(context.Background(), channel.ID, 0)
	if err != nil {
		t.Fatalf("same-channel DVR reserve: %v", err)
	}
	defer reservation.Release()
	var streamID uuid.UUID
	var clients, tokens int
	if err := db.Pool.QueryRow(context.Background(), `
		SELECT a.id, a.client_count,
		       (SELECT COUNT(*)::int FROM stream_client WHERE active_stream_id = a.id)
		  FROM active_stream a WHERE a.channel_id = $1
		    AND a.state IN ('starting','running')`, channel.ID).
		Scan(&streamID, &clients, &tokens); err != nil {
		t.Fatal(err)
	}
	if clients != 2 || tokens != 2 {
		t.Fatalf("shared admission clients=%d tokens=%d, want 2/2", clients, tokens)
	}
	for {
		select {
		case <-pool.CapacityChanges():
		default:
			goto capacityDrained
		}
	}

capacityDrained:
	cancelViewer()
	select {
	case <-viewerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("viewer did not stop")
	}
	// Streamer deliberately retains a no-subscriber pump for two seconds. Its
	// origin request ending proves teardown began; WaitForPumps gives a bounded
	// proof that closeAll, OnExit, map removal, and finishPump's DB CAS completed.
	waitFor(t, "viewer pump idle exit", 5*time.Second, func() bool { return activeA.Load() == 0 })
	pumpWaitCtx, cancelPumpWait := context.WithTimeout(context.Background(), 5*time.Second)
	if err := pool.WaitForPumps(pumpWaitCtx); err != nil {
		cancelPumpWait()
		t.Fatalf("wait for viewer pump teardown: %v", err)
	}
	cancelPumpWait()
	waitFor(t, "held reservation remains active", 2*time.Second, func() bool {
		var state string
		return db.Pool.QueryRow(context.Background(), `
			SELECT state::text, client_count,
			       (SELECT COUNT(*)::int FROM stream_client WHERE active_stream_id = a.id)
			  FROM active_stream a WHERE id = $1`, streamID).
			Scan(&state, &clients, &tokens) == nil &&
			(state == "starting" || state == "running") && clients == 1 && tokens == 1
	})
	select {
	case <-pool.CapacityChanges():
		t.Fatal("pump exit advertised capacity while a DVR token still owned the row")
	case <-time.After(200 * time.Millisecond):
	}

	var providerID uuid.UUID
	if err := db.Pool.QueryRow(context.Background(),
		`SELECT provider_id FROM channel_source WHERE id = $1`, sources[0].ID).
		Scan(&providerID); err != nil {
		t.Fatal(err)
	}
	competitor, err := db.CreateChannel(context.Background(), store.Channel{
		Number: 902, Name: "held-reservation-competitor", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateChannelSource(context.Background(), store.ChannelSource{
		ChannelID: competitor.ID, ProviderID: providerID,
		UpstreamURL: "http://competitor.test/stream", Priority: 0,
		HealthScore: 1, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	if competing, err := pool.ReserveWriter(context.Background(), competitor.ID, 0); !errors.Is(err, store.ErrNoSlot) {
		if competing != nil {
			competing.Release()
		}
		t.Fatalf("competing admission err=%v, want capacity held/ErrNoSlot", err)
	}

	// Hold the same channel advisory lock as a relocation whose COMMIT outcome
	// is not visible yet, and stage source B behind it. Reservation.Serve must
	// block at the definitive DB boundary rather than reading MVCC source A.
	relocateTx, err := db.Pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = relocateTx.Rollback(context.Background()) }()
	if _, err := relocateTx.Exec(context.Background(), `
		SELECT pg_advisory_xact_lock(
			hashtextextended('conductor:channel-lease:' || $1::text, 0)
		)`, channel.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := relocateTx.Exec(context.Background(), `
		UPDATE active_stream
		   SET channel_source_id=$2, upstream_url=$3, state='starting'
		 WHERE id=$1`, streamID, sources[1].ID, sourceB.URL); err != nil {
		t.Fatal(err)
	}

	dvrCtx, cancelDVR := context.WithCancel(context.Background())
	defer cancelDVR()
	dvrWriter := &countingWriter{}
	dvrDone := make(chan error, 1)
	go func() {
		_, _, err := reservation.Serve(dvrCtx, dvrWriter)
		dvrDone <- err
	}()
	waitFor(t, "reservation reload waits for relocation boundary", 3*time.Second, func() bool {
		var blocked bool
		err := db.Pool.QueryRow(context.Background(), `
			SELECT EXISTS (
				SELECT 1 FROM pg_stat_activity
				 WHERE datname=current_database()
				   AND wait_event_type='Lock'
				   AND query LIKE '%pg_advisory_xact_lock%'
			)`).Scan(&blocked)
		return err == nil && blocked
	})
	if dvrWriter.n.Load() != 0 || requestsB.Load() != 0 || requestsA.Load() != 1 {
		t.Fatalf("reload crossed uncommitted relocation: bytes=%d requests A/B=%d/%d",
			dvrWriter.n.Load(), requestsA.Load(), requestsB.Load())
	}
	if err := relocateTx.Commit(context.Background()); err != nil {
		t.Fatalf("publish source-B relocation: %v", err)
	}
	waitFor(t, "replacement pump media from source B", 10*time.Second, func() bool {
		return dvrWriter.n.Load() > 64*1024 && requestsB.Load() >= 1
	})
	if got := requestsA.Load(); got != 1 {
		t.Fatalf("replacement pump retried stale source A %d times; want one original request", got)
	}
	cancelDVR()
	select {
	case <-dvrDone:
	case <-time.After(5 * time.Second):
		t.Fatal("DVR replacement subscriber did not stop")
	}
}

// subscriberSet closes subscribers before invoking Pool's OnExit callback.
// Source-health persistence can then block for seconds, so the terminal pump
// must be removed from the process map before that I/O. Otherwise a held DVR
// token subscribes to the already-closed pump and misses its recording.
func TestIntegrationPumpExitUnpublishesBeforeBlockedSourceHealth(t *testing.T) {
	db := freshStreamDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	origin, requests, active := trackedByteServer(t)
	channel, sources := seedChannel(t, db, 1, origin.URL)
	pool := stream.NewPool(logger, db, store.PassthroughResolverForTest{})
	defer pool.Close()

	viewerCtx, cancelViewer := context.WithCancel(context.Background())
	viewerWriter := &discardHTTPWriter{}
	viewerDone := make(chan error, 1)
	go func() {
		req := httptest.NewRequest(http.MethodGet, "/stream/901", nil).WithContext(viewerCtx)
		viewerDone <- pool.Serve(viewerCtx, viewerWriter, req, channel.ID)
	}()
	waitFor(t, "initial viewer media", 10*time.Second, func() bool {
		return viewerWriter.n.Load() > 64*1024 && requests.Load() == 1 && active.Load() == 1
	})
	reservation, err := pool.ReserveWriter(context.Background(), channel.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer reservation.Release()
	// Source success now correctly requires one stable attempt. Keep this
	// teardown-order test beyond that threshold so OnExit performs the health
	// update we deliberately block below.
	time.Sleep(5200 * time.Millisecond)

	healthTx, err := db.Pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = healthTx.Rollback(context.Background()) }()
	if _, err := healthTx.Exec(context.Background(),
		`SELECT id FROM channel_source WHERE id=$1 FOR UPDATE`, sources[0].ID); err != nil {
		t.Fatal(err)
	}
	cancelViewer()
	select {
	case <-viewerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("viewer did not stop")
	}
	waitFor(t, "old origin request ended", 5*time.Second, func() bool { return active.Load() == 0 })
	waitFor(t, "pump exit blocked in source health update", 3*time.Second, func() bool {
		var blocked bool
		err := db.Pool.QueryRow(context.Background(), `
			SELECT EXISTS (
				SELECT 1 FROM pg_stat_activity
				 WHERE datname=current_database()
				   AND wait_event_type='Lock'
				   AND query LIKE '%UPDATE channel_source%'
			)`).Scan(&blocked)
		return err == nil && blocked
	})

	dvrCtx, cancelDVR := context.WithCancel(context.Background())
	defer cancelDVR()
	dvrWriter := &countingWriter{}
	dvrDone := make(chan error, 1)
	go func() {
		_, _, err := reservation.Serve(dvrCtx, dvrWriter)
		dvrDone <- err
	}()
	waitFor(t, "held reservation replacement media", 5*time.Second, func() bool {
		return requests.Load() == 2 && dvrWriter.n.Load() > 64*1024
	})
	if err := healthTx.Commit(context.Background()); err != nil {
		t.Fatalf("release blocked source-health update: %v", err)
	}
	cancelDVR()
	select {
	case <-dvrDone:
	case <-time.After(5 * time.Second):
		t.Fatal("replacement DVR subscriber did not stop")
	}
}

// Runtime ownership loss closes the Pool while requests may still be inside
// PostgreSQL. A lease that completes after Close must be torn down rather
// than registering a role or starting a new upstream pump.
func TestIntegrationPoolCloseRejectsBlockedAcquisition(t *testing.T) {
	db := freshStreamDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	var upstreamConnections atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamConnections.Add(1)
		<-r.Context().Done()
	}))
	defer srv.Close()
	ch, sources := seedChannel(t, db, 1, srv.URL)
	var providerID uuid.UUID
	if err := db.Pool.QueryRow(context.Background(),
		`SELECT provider_id FROM channel_source WHERE id = $1`, sources[0].ID).
		Scan(&providerID); err != nil {
		t.Fatal(err)
	}
	lockTx, err := db.Pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lockTx.Rollback(context.Background()) }()
	if _, err := lockTx.Exec(context.Background(),
		`SELECT 1 FROM provider WHERE id = $1 FOR UPDATE`, providerID); err != nil {
		t.Fatal(err)
	}

	pool := stream.NewPool(logger, db, store.PassthroughResolverForTest{})
	result := make(chan error, 1)
	go func() {
		_, err := pool.ReserveWriter(context.Background(), ch.ID, 0)
		result <- err
	}()
	waitFor(t, "reservation blocked on provider lock", 3*time.Second, func() bool {
		var blocked bool
		err := db.Pool.QueryRow(context.Background(), `
			SELECT EXISTS (
				SELECT 1 FROM pg_stat_activity
				 WHERE datname = current_database()
				   AND wait_event_type = 'Lock'
				   AND query LIKE '%SELECT capacity_domain%FROM provider%FOR UPDATE%'
			)`).Scan(&blocked)
		return err == nil && blocked
	})
	pool.Close()
	if err := lockTx.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, stream.ErrPoolClosed) {
			t.Fatalf("blocked acquisition after Close err=%v, want ErrPoolClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("blocked acquisition did not return after provider lock release")
	}
	var active int
	if err := db.Pool.QueryRow(context.Background(), `
		SELECT COUNT(*)::int FROM active_stream
		 WHERE state IN ('starting','running') OR client_count <> 0`).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if active != 0 {
		t.Fatalf("Pool.Close race left %d active/refcounted rows", active)
	}
	if got := upstreamConnections.Load(); got != 0 {
		t.Fatalf("Pool.Close race started %d upstream connections", got)
	}
}

type crossProviderFixture struct {
	channel        store.Channel
	sourceA        store.ChannelSource
	sourceB        store.ChannelSource
	stopA          func()
	stopB          func()
	providerBBlock store.Lease
}

// seedCrossProviderFixture leaves provider B with exactly one free slot out
// of two. A DVR reserve of one must reject that slot; a live viewer may use it.
func seedCrossProviderFixture(t *testing.T, db *store.DB, suffix string, number float64) crossProviderFixture {
	t.Helper()
	ctx := context.Background()
	srvA, stopA := byteServer(t)
	srvB, stopB := byteServer(t)
	t.Cleanup(stopA)
	t.Cleanup(stopB)
	seedProvider := func(name, baseURL string, maxStreams int) store.Provider {
		t.Helper()
		provider, err := db.CreateProvider(ctx, store.Provider{
			Name: name, Kind: "m3u_xtream", BaseURL: baseURL, Enabled: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.CreateCredential(ctx, store.ProviderCredential{
			ProviderID: provider.ID, Username: name, PasswordEnc: []byte{1},
			MaxStreams: maxStreams, Priority: 100, Enabled: true,
		}); err != nil {
			t.Fatal(err)
		}
		return provider
	}
	providerA := seedProvider("role-a-"+suffix, srvA.URL, 1)
	providerB := seedProvider("role-b-"+suffix, srvB.URL, 2)
	channel, err := db.CreateChannel(ctx, store.Channel{Number: number, Name: "role-main-" + suffix, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	sourceA, err := db.CreateChannelSource(ctx, store.ChannelSource{
		ChannelID: channel.ID, ProviderID: providerA.ID, UpstreamURL: srvA.URL,
		Priority: 0, HealthScore: 1, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	sourceB, err := db.CreateChannelSource(ctx, store.ChannelSource{
		ChannelID: channel.ID, ProviderID: providerB.ID, UpstreamURL: srvB.URL,
		Priority: 1, HealthScore: 1, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	blocker, err := db.CreateChannel(ctx, store.Channel{Number: number + .1, Name: "role-blocker-" + suffix, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateChannelSource(ctx, store.ChannelSource{
		ChannelID: blocker.ID, ProviderID: providerB.ID, UpstreamURL: srvB.URL,
		Priority: 0, HealthScore: 1, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	blockerLease, err := db.AcquireLease(ctx, blocker.ID, store.PassthroughResolverForTest{})
	if err != nil {
		t.Fatal(err)
	}
	return crossProviderFixture{
		channel: channel, sourceA: sourceA, sourceB: sourceB,
		stopA: stopA, stopB: stopB, providerBBlock: blockerLease,
	}
}

// An all-DVR pump must retain its configured live floor after the initial
// reservation. Killing provider A triggers relocation, but provider B's last
// slot remains reserved and the active row stays on A.
func TestIntegrationDVRPumpRelocationPreservesLiveReserve(t *testing.T) {
	db := freshStreamDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	fixture := seedCrossProviderFixture(t, db, "dvr-only", 911)
	pool := stream.NewPool(logger, db, store.PassthroughResolverForTest{})
	defer pool.Close()
	reservation, err := pool.ReserveWriter(context.Background(), fixture.channel.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	writer := &countingWriter{}
	serveCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, _, err := reservation.Serve(serveCtx, writer)
		done <- err
	}()
	waitFor(t, "DVR bytes before failover", 10*time.Second, func() bool { return writer.n.Load() > 64*1024 })
	fixture.stopA()
	waitFor(t, "DVR relocation attempt", 10*time.Second, func() bool {
		var failedAt *time.Time
		if err := db.Pool.QueryRow(context.Background(), `
			SELECT last_failure_at FROM channel_source WHERE id = $1`, fixture.sourceA.ID).Scan(&failedAt); err != nil {
			return false
		}
		return failedAt != nil
	})
	var currentSource uuid.UUID
	if err := db.Pool.QueryRow(context.Background(), `
		SELECT channel_source_id FROM active_stream
		 WHERE channel_id = $1 AND state IN ('starting','running')`, fixture.channel.ID).Scan(&currentSource); err != nil {
		t.Fatal(err)
	}
	if currentSource != fixture.sourceA.ID {
		t.Fatalf("DVR-only pump consumed live-reserved provider B slot: source=%s", currentSource)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("DVR-only serve did not stop")
	}
}

// A live viewer on the same pump takes precedence over the piggybacking DVR:
// provider B's reserved slot exists for exactly this viewer, so both clients
// continue through one cross-provider upstream after A fails.
func TestIntegrationViewerWinsSharedPumpRelocationPriority(t *testing.T) {
	db := freshStreamDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	fixture := seedCrossProviderFixture(t, db, "viewer-shared", 912)
	pool := stream.NewPool(logger, db, store.PassthroughResolverForTest{})
	defer pool.Close()

	viewerCtx, cancelViewer := context.WithCancel(context.Background())
	defer cancelViewer()
	viewerWriter := &discardHTTPWriter{}
	request := httptest.NewRequest(http.MethodGet, "/stream/912", nil).WithContext(viewerCtx)
	viewerDone := make(chan error, 1)
	go func() { viewerDone <- pool.Serve(viewerCtx, viewerWriter, request, fixture.channel.ID) }()
	waitFor(t, "viewer bytes before failover", 10*time.Second, func() bool { return viewerWriter.n.Load() > 64*1024 })

	reservation, err := pool.ReserveWriter(context.Background(), fixture.channel.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	dvrCtx, cancelDVR := context.WithCancel(context.Background())
	defer cancelDVR()
	dvrWriter := &countingWriter{}
	dvrDone := make(chan error, 1)
	go func() {
		_, _, err := reservation.Serve(dvrCtx, dvrWriter)
		dvrDone <- err
	}()
	waitFor(t, "shared DVR bytes before failover", 10*time.Second, func() bool { return dvrWriter.n.Load() > 64*1024 })
	viewerBefore := viewerWriter.n.Load()
	dvrBefore := dvrWriter.n.Load()
	fixture.stopA()
	waitFor(t, "shared pump provider-B relocation", 15*time.Second, func() bool {
		var sourceID uuid.UUID
		if err := db.Pool.QueryRow(context.Background(), `
			SELECT channel_source_id FROM active_stream
			 WHERE channel_id = $1 AND state IN ('starting','running')`, fixture.channel.ID).Scan(&sourceID); err != nil {
			return false
		}
		return sourceID == fixture.sourceB.ID &&
			viewerWriter.n.Load() > viewerBefore+64*1024 && dvrWriter.n.Load() > dvrBefore+64*1024
	})
	var streams int
	if err := db.Pool.QueryRow(context.Background(), `
		SELECT COUNT(*)::int FROM active_stream
		 WHERE channel_id = $1 AND state IN ('starting','running')`, fixture.channel.ID).Scan(&streams); err != nil {
		t.Fatal(err)
	}
	if streams != 1 {
		t.Fatalf("viewer+DVR failover created %d upstream rows, want one", streams)
	}

	cancelDVR()
	cancelViewer()
	select {
	case <-dvrDone:
	case <-time.After(5 * time.Second):
		t.Fatal("shared DVR did not stop")
	}
	select {
	case <-viewerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("shared viewer did not stop")
	}
}

func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func lockTranscodeProfileTable(t *testing.T, db *store.DB) pgx.Tx {
	t.Helper()
	tx, err := db.Pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(context.Background(),
		`LOCK TABLE transcode_profile IN ACCESS EXCLUSIVE MODE`); err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatal(err)
	}
	return tx
}

func blockedProfileLookups(db *store.DB) int {
	var count int
	_ = db.Pool.QueryRow(context.Background(), `
		SELECT COUNT(*)::int
		  FROM pg_stat_activity
		 WHERE pid <> pg_backend_pid()
		   AND datname = current_database()
		   AND state = 'active'
		   AND wait_event_type = 'Lock'
		   AND query LIKE '%FROM transcode_profile WHERE id%'`).Scan(&count)
	return count
}

func blockedSourceHealthUpdates(db *store.DB) int {
	var count int
	_ = db.Pool.QueryRow(context.Background(), `
		SELECT COUNT(*)::int
		  FROM pg_stat_activity
		 WHERE pid <> pg_backend_pid()
		   AND datname = current_database()
		   AND state = 'active'
		   AND wait_event_type = 'Lock'
		   AND query ILIKE '%UPDATE channel_source%'
		   AND query ILIKE '%last_failure_at%'`).Scan(&count)
	return count
}

// Source-health bookkeeping is advisory. A row lock on the failed source
// must not consume the independent 500ms pre-media relocation budget before
// the alternate is attempted inside Plex's startup deadline.
func TestIntegrationBlockedSourceHealthDoesNotDelayStartupRelocation(t *testing.T) {
	db := freshStreamDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	failedRequested := make(chan struct{})
	releaseFailure := make(chan struct{})
	var failedOnce sync.Once
	failed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		failedOnce.Do(func() { close(failedRequested) })
		select {
		case <-releaseFailure:
		case <-r.Context().Done():
			return
		}
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer failed.Close()
	var alternateRequests atomic.Int64
	alternate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		alternateRequests.Add(1)
		w.Header().Set("Content-Type", "video/mp2t")
		flusher, _ := w.(http.Flusher)
		block := bytes.Repeat([]byte{0xbc}, 1024)
		for {
			if _, err := w.Write(block); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
			select {
			case <-r.Context().Done():
				return
			case <-time.After(2 * time.Millisecond):
			}
		}
	}))
	defer alternate.Close()

	ch, sources := seedChannel(t, db, 1, failed.URL, alternate.URL)
	var healthLock pgx.Tx
	lockHeld := false
	defer func() {
		if lockHeld {
			_ = healthLock.Rollback(context.Background())
		}
	}()

	pool := stream.NewPool(logger, db, store.PassthroughResolverForTest{})
	defer pool.Close()
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	startupCtx := stream.WithStartupDeadline(parent, time.Now().Add(4*time.Second))
	served := make(chan error, 1)
	go func() {
		_, _, serveErr := pool.ServeWriter(startupCtx, io.Discard, ch.ID)
		served <- serveErr
	}()
	select {
	case <-failedRequested:
	case <-time.After(time.Second):
		t.Fatal("priority source was not requested before health lock")
	}
	var err error
	healthLock, err = db.Pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	lockHeld = true
	var lockedID uuid.UUID
	if err := healthLock.QueryRow(context.Background(),
		`SELECT id FROM channel_source WHERE id=$1 FOR UPDATE`, sources[0].ID).Scan(&lockedID); err != nil {
		t.Fatal(err)
	}
	close(releaseFailure)

	waitFor(t, "source-health update to block", time.Second, func() bool {
		return blockedSourceHealthUpdates(db) > 0
	})
	waitFor(t, "alternate request while source-health row stays locked", time.Second, func() bool {
		return alternateRequests.Load() > 0
	})
	if blockedSourceHealthUpdates(db) == 0 {
		t.Fatal("source-health update stopped blocking before alternate request proof")
	}

	cancel()
	pool.Close()
	select {
	case <-served:
	case <-time.After(3 * time.Second):
		t.Fatal("ServeWriter did not stop after alternate relocation test cancellation")
	}
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	waitErr := pool.WaitForPumps(waitCtx)
	waitCancel()
	if !errors.Is(waitErr, context.DeadlineExceeded) {
		t.Fatalf("WaitForPumps returned %v while advisory health update remained row-locked", waitErr)
	}
	if err := healthLock.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	lockHeld = false
	quiesceCtx, quiesceCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer quiesceCancel()
	if err := pool.WaitForPumps(quiesceCtx); err != nil {
		t.Fatalf("WaitForPumps after source-health unlock: %v", err)
	}
}

// TestIntegrationPoolFailsOverMidStream: a viewer (ServeWriter — same lease +
// Streamer machinery as Plex's Serve) keeps receiving bytes after the
// priority-0 upstream is killed mid-stream; the active_stream row repoints
// to the priority-1 source without changing identity (audit S1).
func TestIntegrationPoolFailsOverMidStream(t *testing.T) {
	db := freshStreamDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	srvA, stopA := byteServer(t)
	defer stopA()
	srvB, stopB := byteServer(t)
	defer stopB()

	ch, sources := seedChannel(t, db, 1, srvA.URL, srvB.URL)

	pool := stream.NewPool(logger, db, store.PassthroughResolverForTest{})
	defer pool.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var w countingWriter
	served := make(chan error, 1)
	go func() {
		_, _, err := pool.ServeWriter(ctx, &w, ch.ID)
		served <- err
	}()

	// Phase 1: bytes flowing from source A.
	waitFor(t, "first bytes", 15*time.Second, func() bool { return w.n.Load() > 64*1024 })

	rowSource := func() uuid.UUID {
		var id uuid.UUID
		err := db.Pool.QueryRow(context.Background(),
			`SELECT channel_source_id FROM active_stream LIMIT 1`).Scan(&id)
		if err != nil {
			return uuid.Nil
		}
		return id
	}
	if got := rowSource(); got != sources[0].ID {
		t.Fatalf("initial source = %s, want priority-0 %s", got, sources[0].ID)
	}

	// Kill A mid-stream. The pump must fail over to B while the viewer's
	// connection stays open.
	stopA()
	atKill := w.n.Load()

	waitFor(t, "post-failover bytes", 30*time.Second, func() bool {
		return w.n.Load() > atKill+256*1024
	})
	waitFor(t, "row repointed to source B", 10*time.Second, func() bool {
		return rowSource() == sources[1].ID
	})

	select {
	case err := <-served:
		t.Fatalf("ServeWriter returned mid-failover: %v", err)
	default:
	}

	cancel()
	select {
	case <-served:
	case <-time.After(10 * time.Second):
		t.Fatal("ServeWriter did not return after ctx cancel")
	}
}

// A second viewer joining while a shared pump is between sources must not use
// pump-wide historical MediaReady and receive 200-with-no-bytes. Its startup
// subscription has an empty (cleared) ring and waits for source-two real media.
func TestIntegrationLateAttachDuringReconnectWaitsForRealMedia(t *testing.T) {
	db := freshStreamDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	sourceOne, stopSourceOne := byteServer(t)
	defer stopSourceOne()

	sourceTwoStarted := make(chan struct{})
	releaseSourceTwo := make(chan struct{})
	var startedOnce sync.Once
	sourceTwo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		startedOnce.Do(func() { close(sourceTwoStarted) })
		select {
		case <-releaseSourceTwo:
		case <-r.Context().Done():
			return
		}
		flusher, _ := w.(http.Flusher)
		block := bytes.Repeat([]byte{0xbc}, 1024)
		for {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(2 * time.Millisecond):
			}
			if _, err := w.Write(block); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
	}))
	defer sourceTwo.Close()

	ch, _ := seedChannel(t, db, 1, sourceOne.URL, sourceTwo.URL)
	pool := stream.NewPool(logger, db, store.PassthroughResolverForTest{})
	defer pool.Close()

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	defer cancelFirst()
	var firstWriter countingWriter
	firstDone := make(chan error, 1)
	go func() {
		_, _, err := pool.ServeWriter(firstCtx, &firstWriter, ch.ID)
		firstDone <- err
	}()
	waitFor(t, "source-one media", 5*time.Second, func() bool { return firstWriter.n.Load() > 64*1024 })
	stopSourceOne()
	select {
	case <-sourceTwoStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("shared pump did not enter source-two reconnect attempt")
	}

	secondCtx, cancelSecond := context.WithCancel(context.Background())
	defer cancelSecond()
	req := httptest.NewRequest(http.MethodGet, "/stream/901", nil).WithContext(secondCtx)
	secondWriter := newCountingResponseWriter()
	secondDone := make(chan error, 1)
	go func() { secondDone <- pool.Serve(secondCtx, secondWriter, req, ch.ID) }()

	time.Sleep(200 * time.Millisecond)
	if status, n := secondWriter.status.Load(), secondWriter.n.Load(); status != 0 || n != 0 {
		t.Fatalf("late reconnect attach committed before real media: status=%d bytes=%d", status, n)
	}
	close(releaseSourceTwo)
	waitFor(t, "late subscriber source-two bytes", 3*time.Second, func() bool {
		return secondWriter.n.Load() > 0
	})
	if got := secondWriter.status.Load(); got != http.StatusOK {
		t.Fatalf("late subscriber status=%d, want 200 after real media", got)
	}

	cancelSecond()
	cancelFirst()
	select {
	case <-secondDone:
	case <-time.After(3 * time.Second):
		t.Fatal("late Serve did not return after cancellation")
	}
	select {
	case <-firstDone:
	case <-time.After(3 * time.Second):
		t.Fatal("first ServeWriter did not return after cancellation")
	}
}

// TestIntegrationStartupTimeoutRelocatesInsidePlexDeadline exercises the real
// lease row and Pool callbacks, not only the Streamer seam. Source one returns
// HTTP 200 but no media for its full four-second budget; source two must take
// over the same active_stream and deliver live HTTP bytes before ten seconds,
// with source one's request closed and no extra row/body retained.
func TestIntegrationStartupTimeoutRelocatesInsidePlexDeadline(t *testing.T) {
	db := freshStreamDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	sourceOneCanceled := make(chan struct{})
	var canceledOnce atomic.Bool
	sourceOne := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		<-r.Context().Done()
		if canceledOnce.CompareAndSwap(false, true) {
			close(sourceOneCanceled)
		}
	}))
	defer sourceOne.Close()
	sourceTwo, stopTwo := byteServer(t)
	defer stopTwo()

	ch, sources := seedChannel(t, db, 1, sourceOne.URL, sourceTwo.URL)
	pool := stream.NewPool(logger, db, store.PassthroughResolverForTest{})
	defer pool.Close()

	requestCtx, cancelRequest := context.WithCancel(context.Background())
	defer cancelRequest()
	req := httptest.NewRequest(http.MethodGet, "/stream/901", nil).WithContext(requestCtx)
	w := newCountingResponseWriter()
	served := make(chan error, 1)
	started := time.Now()
	go func() { served <- pool.Serve(requestCtx, w, req, ch.ID) }()

	waitFor(t, "source-two HTTP bytes", 9500*time.Millisecond, func() bool {
		return w.n.Load() > 0
	})
	if elapsed := time.Since(started); elapsed >= 9*time.Second {
		t.Fatalf("startup failover took %v, want <9s (<10s Plex deadline)", elapsed)
	}
	if got := w.status.Load(); got != http.StatusOK {
		t.Fatalf("HTTP status=%d, want 200 after validated alternate media", got)
	}
	select {
	case <-sourceOneCanceled:
	case <-time.After(time.Second):
		t.Fatal("source-one request/body remained open after relocation")
	}

	var rowSource uuid.UUID
	var state string
	var clients, rows int
	waitFor(t, "single running row on source two", 2*time.Second, func() bool {
		err := db.Pool.QueryRow(context.Background(), `
			SELECT channel_source_id, state, client_count
			  FROM active_stream LIMIT 1`).Scan(&rowSource, &state, &clients)
		return err == nil && rowSource == sources[1].ID && state == "running" && clients == 1
	})
	if err := db.Pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM active_stream`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("active_stream rows=%d, want one relocated row", rows)
	}

	cancelRequest()
	select {
	case <-served:
	case <-time.After(3 * time.Second):
		t.Fatal("Serve did not return after client cancellation")
	}
	waitFor(t, "released dead stream row", 2*time.Second, func() bool {
		err := db.Pool.QueryRow(context.Background(), `
			SELECT state, client_count FROM active_stream LIMIT 1`).Scan(&state, &clients)
		return err == nil && state == "dead" && clients == 0
	})
}

// The initiating subscriber must exist before a newly-created pump can run.
// Exercise both live and DVR Pool paths repeatedly with a finite accepted
// response; every byte, including the readiness chunk, must arrive exactly
// once even when the upstream can replay the complete body immediately.
func TestIntegrationFiniteAcceptedResponseRetainsExactBytes(t *testing.T) {
	db := freshStreamDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	payload := make([]byte, (1<<20)+188)
	for i := range payload {
		payload[i] = byte(i*31 + 7)
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		w.Header().Set("Content-Length", fmt.Sprint(len(payload)))
		_, _ = w.Write(payload)
	}))
	defer upstream.Close()

	ch, _ := seedChannel(t, db, 1, upstream.URL)
	pool := stream.NewPool(logger, db, store.PassthroughResolverForTest{})
	defer pool.Close()
	waitForPhysicalQuiescence := func(t *testing.T) {
		t.Helper()
		// This regression promises a newly-created pump on every iteration.
		// Release is authoritative before Serve returns, but physical socket exit
		// and its generation-fenced DB finalization remain intentionally async.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := pool.WaitForPumps(ctx); err != nil {
			t.Fatalf("wait for finite-response pump teardown: %v", err)
		}
	}

	for i := 0; i < 5; i++ {
		t.Run(fmt.Sprintf("live-%d", i), func(t *testing.T) {
			w := newExactPayloadResponseWriter(payload)
			req := httptest.NewRequest(http.MethodGet, "/stream/901", nil)
			err := pool.Serve(req.Context(), w, req, ch.ID)
			if !errors.Is(err, errExactPayloadCaptured) {
				t.Fatalf("Serve error=%v, want exact-payload sentinel", err)
			}
			if w.status != http.StatusOK {
				t.Fatalf("status=%d, want 200", w.status)
			}
			if got := w.bytes(); !bytes.Equal(got, payload) {
				t.Fatalf("live payload mismatch: got=%d want=%d", len(got), len(payload))
			}
		})
		waitForPhysicalQuiescence(t)

		t.Run(fmt.Sprintf("dvr-%d", i), func(t *testing.T) {
			w := newExactPayloadWriter(payload)
			written, lastReal, err := pool.ServeWriter(context.Background(), w, ch.ID)
			if !errors.Is(err, stream.ErrSinkWrite) || !errors.Is(err, errExactPayloadCaptured) {
				t.Fatalf("ServeWriter error=%v, want ErrSinkWrite wrapping exact-payload sentinel", err)
			}
			if written != int64(len(payload)) {
				t.Fatalf("bytesWritten=%d, want %d", written, len(payload))
			}
			if lastReal.IsZero() {
				t.Fatal("accepted finite replay did not record real-media delivery")
			}
			if got := w.bytes(); !bytes.Equal(got, payload) {
				t.Fatalf("DVR payload mismatch: got=%d want=%d", len(got), len(payload))
			}
		})
		waitForPhysicalQuiescence(t)
	}
}

// A deadline carried from the handler must include pre-Pool work, and a
// blocked teardown query must not extend the response. Holding FOR UPDATE on
// the stream row beyond one cleanup-attempt timeout proves Serve returns before
// the absolute cutoff and the idempotent background retry eventually releases.
func TestIntegrationAbsoluteStartupDeadlineIncludesPreludeAndAsyncCleanup(t *testing.T) {
	db := freshStreamDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	sourceCanceled := make(chan struct{})
	var canceled atomic.Bool
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		<-r.Context().Done()
		if canceled.CompareAndSwap(false, true) {
			close(sourceCanceled)
		}
	}))
	defer upstream.Close()

	ch, _ := seedChannel(t, db, 1, upstream.URL)
	pool := stream.NewPool(logger, db, store.PassthroughResolverForTest{})
	defer pool.Close()

	established := time.Now()
	deadline := established.Add(2 * time.Second)
	time.Sleep(500 * time.Millisecond) // handler/channel-lookup prelude
	requestCtx := stream.WithStartupDeadline(context.Background(), deadline)
	req := httptest.NewRequest(http.MethodGet, "/stream/901", nil).WithContext(requestCtx)
	rec := httptest.NewRecorder()
	served := make(chan error, 1)
	go func() { served <- pool.Serve(requestCtx, rec, req, ch.ID) }()

	var streamID uuid.UUID
	waitFor(t, "startup stream row", time.Second, func() bool {
		return db.Pool.QueryRow(context.Background(),
			`SELECT id FROM active_stream WHERE state IN ('starting','running') LIMIT 1`).Scan(&streamID) == nil
	})
	tx, err := db.Pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	locked := false
	defer func() {
		if locked {
			_ = tx.Rollback(context.Background())
		}
	}()
	if err := tx.QueryRow(context.Background(),
		`SELECT id FROM active_stream WHERE id=$1 FOR UPDATE`, streamID).Scan(&streamID); err != nil {
		t.Fatal(err)
	}
	locked = true

	select {
	case err := <-served:
		if !errors.Is(err, stream.ErrUpstreamNotReady) {
			t.Fatalf("Serve error=%v, want ErrUpstreamNotReady", err)
		}
	case <-time.After(time.Until(deadline) + 300*time.Millisecond):
		t.Fatal("Serve exceeded its absolute startup deadline while cleanup row was locked")
	}
	if elapsed := time.Since(established); elapsed >= 2300*time.Millisecond {
		t.Fatalf("end-to-end startup return took %v, want <2.3s", elapsed)
	}
	if rec.Body.Len() != 0 || rec.Flushed {
		t.Fatalf("Serve wrote before validated media (body=%d flushed=%v)", rec.Body.Len(), rec.Flushed)
	}

	// The result above arrived while ReleaseLeaseClient was unable to acquire
	// this row lock: cleanup is conclusively off the response path. Keep the
	// lock beyond the first background attempt, then let the bounded retry win.
	time.Sleep(1250 * time.Millisecond)
	if err := tx.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	locked = false
	select {
	case <-sourceCanceled:
	case <-time.After(time.Second):
		t.Fatal("startup request/body remained open after deadline")
	}
	waitFor(t, "retried asynchronous release", 5*time.Second, func() bool {
		var state string
		var clients int
		err := db.Pool.QueryRow(context.Background(),
			`SELECT state, client_count FROM active_stream WHERE id=$1`, streamID).Scan(&state, &clients)
		return errors.Is(err, pgx.ErrNoRows) || (err == nil && state == "dead" && clients == 0)
	})
	var durableClients int
	if err := db.Pool.QueryRow(context.Background(),
		`SELECT COUNT(*)::int FROM stream_client WHERE active_stream_id=$1`, streamID).
		Scan(&durableClients); err != nil {
		t.Fatal(err)
	}
	if durableClients != 0 {
		t.Fatalf("durable startup clients=%d after cleanup retry, want 0", durableClients)
	}
}

// EffectiveTranscodeProfileForLease performs database work after lease
// acquisition. A slow lookup must consume the request's carried startup
// deadline without holding Pool.mu: a second same-stream tune gets its own
// shorter bound, and Close linearizes promptly before either pump is inserted.
func TestIntegrationBlockedProfileLookupHonorsPerRequestDeadlineAndClose(t *testing.T) {
	db := freshStreamDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	var upstreamRequests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamRequests.Add(1)
		w.Header().Set("Content-Type", "video/mp2t")
		_, _ = w.Write(bytes.Repeat([]byte{0xab}, 1024))
	}))
	defer upstream.Close()

	ch, _ := seedChannel(t, db, 1, upstream.URL)
	profile, err := db.GetTranscodeProfileByName(context.Background(), "stabilize-cpu")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.SetChannelTranscodeProfile(context.Background(), ch.ID, &profile.ID); err != nil {
		t.Fatal(err)
	}

	profileLock := lockTranscodeProfileTable(t, db)
	lockHeld := true
	defer func() {
		if lockHeld {
			_ = profileLock.Rollback(context.Background())
		}
	}()

	pool := stream.NewPool(logger, db, store.PassthroughResolverForTest{})
	firstDeadline := time.Now().Add(5 * time.Second)
	firstCtx := stream.WithStartupDeadline(context.Background(), firstDeadline)
	firstDone := make(chan error, 1)
	go func() {
		_, _, serveErr := pool.ServeWriter(firstCtx, io.Discard, ch.ID)
		firstDone <- serveErr
	}()

	var streamID uuid.UUID
	waitFor(t, "first profile lookup to block", time.Second, func() bool {
		if blockedProfileLookups(db) < 1 {
			return false
		}
		return db.Pool.QueryRow(context.Background(), `
			SELECT id FROM active_stream
			 WHERE state = 'starting' AND client_count = 1
			 ORDER BY started_at DESC LIMIT 1`).Scan(&streamID) == nil
	})

	secondStarted := time.Now()
	secondDeadline := secondStarted.Add(900 * time.Millisecond)
	secondCtx := stream.WithStartupDeadline(context.Background(), secondDeadline)
	secondDone := make(chan error, 1)
	go func() {
		_, _, serveErr := pool.ServeWriter(secondCtx, io.Discard, ch.ID)
		secondDone <- serveErr
	}()
	waitFor(t, "second independent profile lookup to block", 500*time.Millisecond, func() bool {
		if blockedProfileLookups(db) < 2 {
			return false
		}
		var clients int
		return db.Pool.QueryRow(context.Background(),
			`SELECT client_count FROM active_stream WHERE id=$1`, streamID).Scan(&clients) == nil && clients == 2
	})

	select {
	case secondErr := <-secondDone:
		if !errors.Is(secondErr, stream.ErrUpstreamNotReady) ||
			!errors.Is(secondErr, store.ErrLeaseOperational) {
			t.Fatalf("short-deadline ServeWriter error=%v, want readiness and operational markers", secondErr)
		}
	case <-time.After(time.Until(secondDeadline) + 350*time.Millisecond):
		t.Fatal("second tune waited behind the first lookup instead of honoring its own deadline")
	}
	if elapsed := time.Since(secondStarted); elapsed >= 1250*time.Millisecond {
		t.Fatalf("short-deadline tune returned in %v, want <1.25s", elapsed)
	}
	waitFor(t, "short-deadline lease release", time.Second, func() bool {
		var clients int
		return db.Pool.QueryRow(context.Background(),
			`SELECT client_count FROM active_stream WHERE id=$1`, streamID).Scan(&clients) == nil && clients == 1
	})

	closeDone := make(chan struct{})
	closeStarted := time.Now()
	go func() {
		pool.Close()
		close(closeDone)
	}()
	select {
	case <-closeDone:
		if elapsed := time.Since(closeStarted); elapsed >= 500*time.Millisecond {
			t.Fatalf("Pool.Close returned in %v, want <500ms", elapsed)
		}
	case <-time.After(500 * time.Millisecond):
		// Release the database waiter before failing so an old mutex-holding
		// implementation cannot strand the test process.
		_ = profileLock.Rollback(context.Background())
		lockHeld = false
		<-closeDone
		t.Fatal("Pool.Close blocked behind transcode profile lookup")
	}

	if err := profileLock.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	lockHeld = false
	select {
	case firstErr := <-firstDone:
		if firstErr == nil || !strings.Contains(firstErr.Error(), "pool closed") {
			t.Fatalf("first blocked tune error=%v, want pool-closed rejection", firstErr)
		}
	case <-time.After(time.Until(firstDeadline) + 500*time.Millisecond):
		t.Fatal("first profile lookup did not unwind after Close")
	}
	if got := upstreamRequests.Load(); got != 0 {
		t.Fatalf("post-Close pump made %d upstream requests, want 0", got)
	}
	waitFor(t, "closed startup row dead with no clients", 3*time.Second, func() bool {
		var state string
		var clients int
		err := db.Pool.QueryRow(context.Background(),
			`SELECT state, client_count FROM active_stream WHERE id=$1`, streamID).Scan(&state, &clients)
		return errors.Is(err, pgx.ErrNoRows) || (err == nil && state == "dead" && clients == 0)
	})
}

// Profile lookup now runs outside Pool.mu, so two creators can finish that
// lookup together. The second map check must collapse them onto one pump,
// with both startup subscribers registered before its first real output.
func TestIntegrationConcurrentProfileLookupsCreateOnePump(t *testing.T) {
	db := freshStreamDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	releaseMedia := make(chan struct{})
	var upstreamRequests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests.Add(1)
		w.Header().Set("Content-Type", "video/mp2t")
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		select {
		case <-releaseMedia:
		case <-r.Context().Done():
			return
		}
		flusher, _ := w.(http.Flusher)
		block := bytes.Repeat([]byte{0xbd}, 1024)
		for {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(2 * time.Millisecond):
			}
			if _, err := w.Write(block); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
	}))
	defer upstream.Close()

	ch, _ := seedChannel(t, db, 1, upstream.URL)
	profile, err := db.GetTranscodeProfileByName(context.Background(), "stabilize-cpu")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.SetChannelTranscodeProfile(context.Background(), ch.ID, &profile.ID); err != nil {
		t.Fatal(err)
	}
	ffmpegStub := filepath.Join(t.TempDir(), "ffmpeg-cat")
	if err := os.WriteFile(ffmpegStub, []byte("#!/bin/sh\nexec cat\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	profileLock := lockTranscodeProfileTable(t, db)
	lockHeld := true
	defer func() {
		if lockHeld {
			_ = profileLock.Rollback(context.Background())
		}
	}()
	pool := stream.NewPool(logger, db, store.PassthroughResolverForTest{})
	pool.FFmpegBinary = ffmpegStub
	defer pool.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var writers [2]countingWriter
	done := make(chan error, 2)
	startViewer := func(i int) {
		go func() {
			_, _, serveErr := pool.ServeWriter(ctx, &writers[i], ch.ID)
			done <- serveErr
		}()
	}
	startViewer(0)
	var streamID uuid.UUID
	waitFor(t, "first creator profile lookup", time.Second, func() bool {
		if blockedProfileLookups(db) < 1 {
			return false
		}
		return db.Pool.QueryRow(context.Background(), `
			SELECT id FROM active_stream
			 WHERE state = 'starting' AND client_count = 1
			 ORDER BY started_at DESC LIMIT 1`).Scan(&streamID) == nil
	})
	startViewer(1)
	waitFor(t, "second creator profile lookup", time.Second, func() bool {
		if blockedProfileLookups(db) < 2 {
			return false
		}
		var clients int
		return db.Pool.QueryRow(context.Background(),
			`SELECT client_count FROM active_stream WHERE id=$1`, streamID).Scan(&clients) == nil && clients == 2
	})
	if err := profileLock.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	lockHeld = false
	waitFor(t, "single shared upstream request", 2*time.Second, func() bool {
		return upstreamRequests.Load() == 1
	})
	close(releaseMedia)
	waitFor(t, "both initiating subscribers to receive media", 2*time.Second, func() bool {
		return writers[0].n.Load() > 0 && writers[1].n.Load() > 0
	})
	if got := upstreamRequests.Load(); got != 1 {
		t.Fatalf("concurrent creators made %d upstream requests, want one shared pump", got)
	}
	var rows, clients int
	if err := db.Pool.QueryRow(context.Background(), `
		SELECT COUNT(*)::int, COALESCE(MAX(client_count), 0)::int
		  FROM active_stream WHERE state IN ('starting','running')`).Scan(&rows, &clients); err != nil {
		t.Fatal(err)
	}
	if rows != 1 || clients != 2 {
		t.Fatalf("active streams=%d clients=%d, want one row with two clients", rows, clients)
	}

	cancel()
	for range writers {
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("shared ServeWriter did not return after cancellation")
		}
	}
}

// A shared pump belongs to the active stream, not to the request that won
// creation. If that creator spent most of its absolute startup budget in the
// profile lookup, a later subscriber must still get a complete bounded source
// attempt and the alternate inside its own fresh deadline.
func TestIntegrationStaggeredSubscriberKeepsOwnStartupDeadline(t *testing.T) {
	db := freshStreamDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	var sourceOneRequests atomic.Int32
	sourceOne := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sourceOneRequests.Add(1)
		w.Header().Set("Content-Type", "video/mp2t")
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		<-r.Context().Done()
	}))
	defer sourceOne.Close()
	sourceTwo, stopSourceTwo := byteServer(t)
	defer stopSourceTwo()

	ch, _ := seedChannel(t, db, 1, sourceOne.URL, sourceTwo.URL)
	profile, err := db.GetTranscodeProfileByName(context.Background(), "stabilize-cpu")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.SetChannelTranscodeProfile(context.Background(), ch.ID, &profile.ID); err != nil {
		t.Fatal(err)
	}
	ffmpegStub := filepath.Join(t.TempDir(), "ffmpeg-cat")
	if err := os.WriteFile(ffmpegStub, []byte("#!/bin/sh\nexec cat\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	profileLock := lockTranscodeProfileTable(t, db)
	lockHeld := true
	defer func() {
		if lockHeld {
			_ = profileLock.Rollback(context.Background())
		}
	}()
	pool := stream.NewPool(logger, db, store.PassthroughResolverForTest{})
	pool.FFmpegBinary = ffmpegStub
	// The creator's expired deadline may skip the optional hold, but must not
	// shorten the later viewer's source attempt or prevent alternate media.
	pool.LiveStartupLead = 4 * time.Second
	defer pool.Close()

	creatorDeadline := time.Now().Add(4 * time.Second)
	creatorCtx := stream.WithStartupDeadline(context.Background(), creatorDeadline)
	var creatorWriter countingWriter
	creatorDone := make(chan error, 1)
	go func() {
		_, _, serveErr := pool.ServeWriter(creatorCtx, &creatorWriter, ch.ID)
		creatorDone <- serveErr
	}()

	var streamID uuid.UUID
	waitFor(t, "creator profile lookup", time.Second, func() bool {
		if blockedProfileLookups(db) < 1 {
			return false
		}
		return db.Pool.QueryRow(context.Background(), `
			SELECT id FROM active_stream
			 WHERE state = 'starting' AND client_count = 1
			 ORDER BY started_at DESC LIMIT 1`).Scan(&streamID) == nil
	})
	// Leave the creator less than one complete four-second source attempt. It
	// must time out, but its departure must not kill the pump once viewer two
	// has attached.
	if delay := time.Until(creatorDeadline.Add(-2 * time.Second)); delay > 0 {
		time.Sleep(delay)
	}
	if err := profileLock.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	lockHeld = false
	waitFor(t, "creator pump source-one request", time.Second, func() bool {
		return sourceOneRequests.Load() == 1
	})

	viewerCtx, cancelViewer := context.WithCancel(context.Background())
	defer cancelViewer()
	viewerStartup := stream.WithStartupDeadline(viewerCtx, time.Now().Add(7*time.Second))
	var viewerWriter countingWriter
	viewerDone := make(chan error, 1)
	go func() {
		_, _, serveErr := pool.ServeWriter(viewerStartup, &viewerWriter, ch.ID)
		viewerDone <- serveErr
	}()
	waitFor(t, "second subscriber lease attach", time.Second, func() bool {
		var clients int
		return db.Pool.QueryRow(context.Background(),
			`SELECT client_count FROM active_stream WHERE id=$1`, streamID).Scan(&clients) == nil && clients == 2
	})

	select {
	case creatorErr := <-creatorDone:
		if !errors.Is(creatorErr, stream.ErrUpstreamNotReady) {
			t.Fatalf("creator error=%v, want ErrUpstreamNotReady", creatorErr)
		}
	case <-time.After(time.Until(creatorDeadline) + 500*time.Millisecond):
		t.Fatal("creator did not stop at its own startup deadline")
	}
	if creatorWriter.n.Load() != 0 {
		t.Fatalf("creator received %d bytes after its deadline, want 0", creatorWriter.n.Load())
	}
	waitFor(t, "creator lease release while viewer remains", 2*time.Second, func() bool {
		var state string
		var clients int
		return db.Pool.QueryRow(context.Background(),
			`SELECT state, client_count FROM active_stream WHERE id=$1`, streamID).Scan(&state, &clients) == nil &&
			state != "dead" && clients == 1
	})

	waitFor(t, "fresh subscriber alternate media", 6*time.Second, func() bool {
		return viewerWriter.n.Load() > 0
	})
	if got := sourceOneRequests.Load(); got != 1 {
		t.Fatalf("shared pump made %d source-one requests, want 1", got)
	}

	cancelViewer()
	select {
	case viewerErr := <-viewerDone:
		if !errors.Is(viewerErr, context.Canceled) {
			t.Fatalf("viewer error=%v, want context cancellation", viewerErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("fresh subscriber did not stop after cancellation")
	}
	waitFor(t, "shared startup lease cleanup", 3*time.Second, func() bool {
		var state string
		var clients int
		err := db.Pool.QueryRow(context.Background(),
			`SELECT state, client_count FROM active_stream WHERE id=$1`, streamID).Scan(&state, &clients)
		return errors.Is(err, pgx.ErrNoRows) || (err == nil && state == "dead" && clients == 0)
	})
}

// Canceling a pump before it has established stable media is neutral. A clean
// context-driven OnExit must not slowly credit source health merely because no
// terminal provider error was recorded; exercise both production pump kinds.
func TestIntegrationPreMediaCancellationDoesNotCreditSource(t *testing.T) {
	db := freshStreamDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	requestStarted := make(chan struct{}, 2)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		requestStarted <- struct{}{}
		<-r.Context().Done()
	}))
	defer upstream.Close()

	ch, sources := seedChannel(t, db, 1, upstream.URL)
	const initialHealth = 0.4
	if _, err := db.Pool.Exec(context.Background(),
		`UPDATE channel_source SET health_score=$2, last_failure_at=NULL WHERE id=$1`,
		sources[0].ID, initialHealth); err != nil {
		t.Fatal(err)
	}
	pool := stream.NewPool(logger, db, store.PassthroughResolverForTest{})
	defer pool.Close()

	runCanceled := func(kind string) {
		t.Helper()
		ctx, cancel := context.WithCancel(context.Background())
		var output countingWriter
		done := make(chan error, 1)
		go func() {
			_, _, err := pool.ServeWriter(ctx, &output, ch.ID)
			done <- err
		}()
		select {
		case <-requestStarted:
		case <-time.After(3 * time.Second):
			cancel()
			t.Fatalf("%s pump never opened upstream", kind)
		}
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("%s cancellation error=%v, want context.Canceled", kind, err)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("%s ServeWriter did not return after cancellation", kind)
		}
		if got := output.n.Load(); got != 0 {
			t.Fatalf("%s cancellation wrote %d bytes before media", kind, got)
		}
		waitCtx, cancelWait := context.WithTimeout(context.Background(), 3*time.Second)
		if err := pool.WaitForPumps(waitCtx); err != nil {
			cancelWait()
			t.Fatalf("wait for %s cancellation cleanup: %v", kind, err)
		}
		cancelWait()
		var health float64
		var failedAt *time.Time
		if err := db.Pool.QueryRow(context.Background(),
			`SELECT health_score, last_failure_at FROM channel_source WHERE id=$1`,
			sources[0].ID).Scan(&health, &failedAt); err != nil {
			t.Fatal(err)
		}
		if math.Abs(health-initialHealth) > 1e-6 || failedAt != nil {
			t.Fatalf("%s pre-media cancellation changed source health: score=%v failure=%v",
				kind, health, failedAt)
		}
	}

	runCanceled("passthrough")
	profile, err := db.GetTranscodeProfileByName(context.Background(), "stabilize-cpu")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.SetChannelTranscodeProfile(context.Background(), ch.ID, &profile.ID); err != nil {
		t.Fatal(err)
	}
	ffmpegStub := filepath.Join(t.TempDir(), "ffmpeg-cat")
	if err := os.WriteFile(ffmpegStub, []byte("#!/bin/sh\nexec cat\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	pool.FFmpegBinary = ffmpegStub
	runCanceled("transcode")
}

// Pool wiring must preserve the same attribution as TranscodeStreamer: when
// a healthy source feeds a local transcoder that never emits its first byte,
// the absolute deadline returns a bounded startup failure without relocating
// or decrementing channel_source health after the deadline expires.
func TestIntegrationPoolTranscodeStartupTimeoutDoesNotPenalizeSource(t *testing.T) {
	db := freshStreamDB(t)
	var logs strings.Builder
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	upstream, stopUpstream := byteServer(t)
	defer stopUpstream()

	ch, sources := seedChannel(t, db, 1, upstream.URL)
	if _, err := db.Pool.Exec(context.Background(),
		`UPDATE channel_source SET health_score=0.4 WHERE id=$1`, sources[0].ID); err != nil {
		t.Fatal(err)
	}
	profile, err := db.GetTranscodeProfileByName(context.Background(), "stabilize-cpu")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.SetChannelTranscodeProfile(context.Background(), ch.ID, &profile.ID); err != nil {
		t.Fatal(err)
	}
	ffmpegStub := filepath.Join(t.TempDir(), "ffmpeg-no-output")
	if err := os.WriteFile(ffmpegStub, []byte("#!/bin/sh\nexec dd of=/dev/null bs=65536\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	pool := stream.NewPool(logger, db, store.PassthroughResolverForTest{})
	pool.FFmpegBinary = ffmpegStub
	defer pool.Close()

	established := time.Now()
	deadline := established.Add(1200 * time.Millisecond)
	ctx := stream.WithStartupDeadline(context.Background(), deadline)
	var w countingWriter
	_, _, err = pool.ServeWriter(ctx, &w, ch.ID)
	if !errors.Is(err, stream.ErrUpstreamNotReady) {
		t.Fatalf("ServeWriter error=%v, want ErrUpstreamNotReady", err)
	}
	if elapsed := time.Since(established); elapsed >= 1500*time.Millisecond {
		t.Fatalf("transcode startup failure took %v, want <1.5s", elapsed)
	}
	if got := w.n.Load(); got != 0 {
		t.Fatalf("local transcoder emitted %d bytes, want none", got)
	}

	waitFor(t, "local transcode pump exit", 3*time.Second, func() bool {
		var state string
		return db.Pool.QueryRow(context.Background(),
			`SELECT state FROM active_stream ORDER BY started_at DESC LIMIT 1`).Scan(&state) == nil && state == "dead"
	})
	var health float64
	var failedAt *time.Time
	if err := db.Pool.QueryRow(context.Background(),
		`SELECT health_score, last_failure_at FROM channel_source WHERE id=$1`, sources[0].ID).
		Scan(&health, &failedAt); err != nil {
		t.Fatal(err)
	}
	if math.Abs(health-0.4) > 1e-6 || failedAt != nil {
		t.Fatalf("healthy source was penalized: health=%v last_failure_at=%v", health, failedAt)
	}
	if !strings.Contains(logs.String(), "local transcode pipeline failed") {
		t.Fatalf("missing neutral local-pipeline diagnostic: %s", logs.String())
	}
	if strings.Contains(logs.String(), "upstream exited with error") {
		t.Fatalf("local transcode fault was described as upstream failure: %s", logs.String())
	}
}

// A fragment can be large enough to reach ffmpeg but still be stale and
// unusable. Pool must relocate that inconclusive source inside the one Plex
// deadline while leaving its health untouched; only sustained recent input
// is strong enough to attribute no output to the local transcoder.
func TestIntegrationPoolShortTranscodeFragmentRelocatesWithoutPenalty(t *testing.T) {
	db := freshStreamDB(t)
	var logs strings.Builder
	logger := slog.New(slog.NewTextHandler(&logs, nil))

	sourceOneCanceled := make(chan struct{})
	var canceledOnce atomic.Bool
	sourceOne := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		w.WriteHeader(http.StatusOK)
		// Deliberately exceed confirmedInputMinBytes. The fragment still is
		// not a recent sustained flow by the attempt deadline.
		_, _ = w.Write(bytes.Repeat([]byte{0xaa}, 96*1024))
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		<-r.Context().Done()
		if canceledOnce.CompareAndSwap(false, true) {
			close(sourceOneCanceled)
		}
	}))
	defer sourceOne.Close()

	var sourceTwoRequests atomic.Int32
	sourceTwo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sourceTwoRequests.Add(1)
		w.Header().Set("Content-Type", "video/mp2t")
		flusher, _ := w.(http.Flusher)
		block := bytes.Repeat([]byte{0xbc}, 1024)
		for {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(2 * time.Millisecond):
			}
			if _, err := w.Write(block); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
	}))
	defer sourceTwo.Close()

	ch, sources := seedChannel(t, db, 1, sourceOne.URL, sourceTwo.URL)
	profile, err := db.GetTranscodeProfileByName(context.Background(), "stabilize-cpu")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.SetChannelTranscodeProfile(context.Background(), ch.ID, &profile.ID); err != nil {
		t.Fatal(err)
	}
	ffmpegStub := filepath.Join(t.TempDir(), "ffmpeg-buffer-until-usable")
	stub := "#!/bin/sh\nexec python3 -c 'import shutil,sys; d=sys.stdin.buffer.read(131072); sys.stdout.buffer.write(d); sys.stdout.buffer.flush(); shutil.copyfileobj(sys.stdin.buffer, sys.stdout.buffer)'\n"
	if err := os.WriteFile(ffmpegStub, []byte(stub), 0o755); err != nil {
		t.Fatal(err)
	}

	pool := stream.NewPool(logger, db, store.PassthroughResolverForTest{})
	pool.FFmpegBinary = ffmpegStub
	defer pool.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var writer countingWriter
	served := make(chan error, 1)
	started := time.Now()
	go func() {
		_, _, serveErr := pool.ServeWriter(ctx, &writer, ch.ID)
		served <- serveErr
	}()

	waitFor(t, "alternate transcoded media", 8500*time.Millisecond, func() bool {
		return writer.n.Load() > 0
	})
	if elapsed := time.Since(started); elapsed >= 8500*time.Millisecond {
		t.Fatalf("inconclusive fragment failover took %v, want <8.5s", elapsed)
	}
	select {
	case <-sourceOneCanceled:
	case <-time.After(time.Second):
		t.Fatal("fragment source request remained open after relocation")
	}
	if got := sourceTwoRequests.Load(); got != 1 {
		t.Fatalf("source-two requests=%d, want one alternate attempt", got)
	}

	var rowSource uuid.UUID
	waitFor(t, "running row on alternate source", 2*time.Second, func() bool {
		var state string
		return db.Pool.QueryRow(context.Background(), `
			SELECT channel_source_id, state FROM active_stream
			 ORDER BY started_at DESC LIMIT 1`).Scan(&rowSource, &state) == nil &&
			rowSource == sources[1].ID && state == "running"
	})
	var health float64
	var failedAt *time.Time
	if err := db.Pool.QueryRow(context.Background(),
		`SELECT health_score, last_failure_at FROM channel_source WHERE id=$1`, sources[0].ID).
		Scan(&health, &failedAt); err != nil {
		t.Fatal(err)
	}
	if health != 1.0 || failedAt != nil {
		t.Fatalf("inconclusive fragment source was penalized: health=%v last_failure_at=%v", health, failedAt)
	}
	if !strings.Contains(logs.String(), "transcode_input_uncertain") {
		t.Fatalf("missing inconclusive-input diagnostic: %s", logs.String())
	}

	cancel()
	select {
	case <-served:
	case <-time.After(3 * time.Second):
		t.Fatal("ServeWriter did not return after cancellation")
	}
}

// TestIntegrationServeRejectsWhenUpstreamNeverStarts: Pool.Serve must give
// up with ErrUpstreamNotReady (handler → 503) when media never arrives,
// releasing the lease instead of committing a 200-with-empty-body (audit S4).
func TestIntegrationServeRejectsWhenUpstreamNeverStarts(t *testing.T) {
	db := freshStreamDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Accepts the connection, never sends headers.
	hang := make(chan struct{})
	defer close(hang)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-hang:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()

	ch, _ := seedChannel(t, db, 1, srv.URL)

	pool := stream.NewPool(logger, db, store.PassthroughResolverForTest{})
	defer pool.Close()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/stream/901", nil)

	start := time.Now()
	err := pool.Serve(req.Context(), rec, req, ch.ID)
	elapsed := time.Since(start)

	if !errors.Is(err, stream.ErrUpstreamNotReady) {
		t.Fatalf("Serve err = %v, want ErrUpstreamNotReady", err)
	}
	if elapsed >= 10*time.Second {
		t.Fatalf("Serve took %v, exceeded Plex's observed 10s deadline", elapsed)
	}
	if rec.Body.Len() != 0 || rec.Flushed {
		t.Fatalf("Serve wrote to the client before failing (body=%d bytes)", rec.Body.Len())
	}

	// Cleanup is deliberately asynchronous so it cannot extend the response.
	// The exact client token must disappear, while a real pump remains
	// capacity-counted as draining until its teardown callback marks it dead.
	waitFor(t, "startup lease release", 3*time.Second, func() bool {
		var state string
		var clients int
		qerr := db.Pool.QueryRow(context.Background(),
			`SELECT state, client_count FROM active_stream LIMIT 1`).Scan(&state, &clients)
		return errors.Is(qerr, pgx.ErrNoRows) ||
			(qerr == nil && clients == 0 && (state == "draining" || state == "dead"))
	})
	var state string
	select {
	case <-pool.CapacityChanges():
	case <-time.After(3 * time.Second):
		t.Fatal("startup-failure pump teardown emitted no capacity wake")
	}
	waitFor(t, "startup-failure row dead after teardown", 3*time.Second, func() bool {
		qerr := db.Pool.QueryRow(context.Background(),
			`SELECT state::text FROM active_stream LIMIT 1`).Scan(&state)
		return errors.Is(qerr, pgx.ErrNoRows) || (qerr == nil && state == "dead")
	})
}

// statusServer405 always answers 405 Method Not Allowed — a source that is up
// at the TCP level but refuses to stream (the real S2 topology).
func statusServer405(t *testing.T) (*httptest.Server, func()) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusMethodNotAllowed)
	}))
	var once atomic.Bool
	return srv, func() {
		if once.CompareAndSwap(false, true) {
			srv.CloseClientConnections()
			srv.Close()
		}
	}
}

// TestIntegrationAllSourcesUnhealthyTerminates (audit S2): when every source
// for a channel is unhealthy mid-stream (both 405), the pump must NOT
// ping-pong between the dead sources forever. Once the exclude-list covers
// all sources the failover loop holds on the slate and gives up at the
// reconnect window — so the active_stream row goes dead and ServeWriter
// returns within bounded time instead of running until process restart.
func TestIntegrationAllSourcesUnhealthyTerminates(t *testing.T) {
	db := freshStreamDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Source A streams healthily; B is 405 from the start. We tune A, then
	// kill A so the pump fails over to B (405) and, with A also 405 on retry,
	// every source is exhausted.
	srvA, stopA := byteServer(t)
	defer stopA()
	srvB, stopB := statusServer405(t)
	defer stopB()

	ch, _ := seedChannel(t, db, 1, srvA.URL, srvB.URL)

	pool := stream.NewPool(logger, db, store.PassthroughResolverForTest{})
	defer pool.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var w countingWriter
	served := make(chan error, 1)
	go func() {
		_, _, err := pool.ServeWriter(ctx, &w, ch.ID)
		served <- err
	}()

	// Phase 1: bytes flowing from source A.
	waitFor(t, "first bytes", 15*time.Second, func() bool { return w.n.Load() > 64*1024 })

	// Now make EVERY source unhealthy: A starts 405ing too (kill the healthy
	// upstream). The pump fails A→B(405)→A(405); once both are excluded it must
	// terminate, not loop.
	stopA()

	// ServeWriter must RETURN (row dead, subscribers closed) within the
	// reconnect window + slack. Pre-fix this never happened — the pump
	// relocated A↔B forever.
	select {
	case err := <-served:
		if err == nil {
			t.Fatal("ServeWriter returned nil; want give-up error after all sources failed")
		}
	case <-time.After(130 * time.Second): // defaultReconnectWindow (90s) + slack
		t.Fatal("ServeWriter never returned — pump is ping-ponging between dead sources")
	}

	// Row must be dead (sweeper fodder), not stuck running.
	waitFor(t, "row dead after all sources failed", 5*time.Second, func() bool {
		var state string
		err := db.Pool.QueryRow(context.Background(),
			`SELECT state FROM active_stream LIMIT 1`).Scan(&state)
		return err != nil || state == "dead"
	})
}

// Ordinary client teardown — not just an aborted startup — must reconcile an
// ambiguous release. Before the ordinary release path logged the first
// ReleaseLeaseClient error and returned, so a release that failed (or whose
// COMMIT response was lost) left client_count positive forever and the pump
// holding a real provider connection. On a deployment with two upstream slots
// that is a permanently stranded tuner.
//
// Teeth: hold FOR UPDATE on the stream row across the client's disconnect and
// past one cleanup-attempt timeout, then release. The old code never retries,
// so client_count stays 1 and this fails; the shared reconciler retries until
// the result is authoritative.
func TestIntegrationOrdinaryTeardownReconcilesAmbiguousRelease(t *testing.T) {
	db := freshStreamDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	releaseUpstream := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		w.WriteHeader(http.StatusOK)
		chunk := make([]byte, 188*8)
		for i := range chunk {
			chunk[i] = byte(i % 251)
		}
		for {
			if _, err := w.Write(chunk); err != nil {
				return
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			select {
			case <-r.Context().Done():
				return
			case <-releaseUpstream:
				return
			case <-time.After(20 * time.Millisecond):
			}
		}
	}))
	defer upstream.Close()
	defer close(releaseUpstream)

	ch, _ := seedChannel(t, db, 1, upstream.URL)
	pool := stream.NewPool(logger, db, store.PassthroughResolverForTest{})
	defer pool.Close()

	// A normal, fully-established client: it gets past startup and receives
	// real media, so teardown runs the ordinary path, not abortStartupLease.
	viewerCtx, cancelViewer := context.WithCancel(context.Background())
	viewerWriter := &countingWriter{}
	viewerDone := make(chan error, 1)
	go func() {
		_, _, err := pool.ServeWriter(viewerCtx, viewerWriter, ch.ID)
		viewerDone <- err
	}()
	waitFor(t, "viewer received real media", 10*time.Second, func() bool {
		return viewerWriter.n.Load() > 0
	})

	var streamID uuid.UUID
	waitFor(t, "active stream visible with one client", 5*time.Second, func() bool {
		return db.Pool.QueryRow(context.Background(),
			`SELECT id FROM active_stream WHERE client_count = 1`).Scan(&streamID) == nil
	})
	// Ignore any startup/coalescing hint so the assertions below bind only to
	// the teardown being exercised.
drainCapacity:
	for {
		select {
		case <-pool.CapacityChanges():
			continue
		default:
			break drainCapacity
		}
	}

	// Make the release ambiguous: the reconciler cannot take the row.
	tx, err := db.Pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	locked := false
	defer func() {
		if locked {
			_ = tx.Rollback(context.Background())
		}
	}()
	var lockedID uuid.UUID
	if err := tx.QueryRow(context.Background(),
		`SELECT id FROM active_stream WHERE id=$1 FOR UPDATE`, streamID).Scan(&lockedID); err != nil {
		t.Fatal(err)
	}
	locked = true

	cancelViewer()
	select {
	case verr := <-viewerDone:
		if !errors.Is(verr, context.Canceled) {
			t.Fatalf("viewer error=%v, want context cancellation", verr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("teardown blocked the caller on the locked cleanup row")
	}

	// Outlast one cleanup attempt so the first reconcile attempt definitively
	// fails, then let the retry win.
	time.Sleep(1250 * time.Millisecond)
	select {
	case <-pool.CapacityChanges():
		t.Fatal("capacity wake emitted while ordinary release row remained locked")
	default:
	}
	if err := tx.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	locked = false

	waitFor(t, "ordinary teardown retried to an authoritative result", 10*time.Second, func() bool {
		var state string
		var clients int
		qerr := db.Pool.QueryRow(context.Background(),
			`SELECT state, client_count FROM active_stream WHERE id=$1`, streamID).Scan(&state, &clients)
		return errors.Is(qerr, pgx.ErrNoRows) || (qerr == nil && clients == 0 && state == "dead")
	})

	var durableClients int
	if err := db.Pool.QueryRow(context.Background(),
		`SELECT COUNT(*)::int FROM stream_client WHERE active_stream_id=$1`, streamID).
		Scan(&durableClients); err != nil {
		t.Fatal(err)
	}
	if durableClients != 0 {
		t.Fatalf("durable clients=%d after teardown retry, want 0", durableClients)
	}
	select {
	case <-pool.CapacityChanges():
	case <-time.After(5 * time.Second):
		t.Fatal("ordinary teardown emitted no capacity wake after retry and physical exit")
	}
}
