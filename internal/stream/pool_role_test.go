package stream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/store"
)

// Plex startup gives each alternate source a sub-second budget. Contention on
// same-channel role serialization must return with that context instead of
// inheriting another acquire/release operation's multi-second DB timeout.
func TestContextGateHonorsRelocationDeadline(t *testing.T) {
	gate := newContextGate()
	if err := gate.Lock(context.Background()); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	started := time.Now()
	err := gate.Lock(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("contended gate err=%v, want context deadline", err)
	}
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("contended gate ignored bounded relocation context: %v", elapsed)
	}
	gate.Unlock()

	// The timed-out waiter must not consume or return the holder's token.
	freshCtx, freshCancel := context.WithTimeout(context.Background(), time.Second)
	defer freshCancel()
	if err := gate.Lock(freshCtx); err != nil {
		t.Fatalf("fresh waiter could not acquire after holder unlock: %v", err)
	}
	gate.Unlock()

	// A context canceled before Lock must not randomly win a ready-token
	// select and enter protected DB work, nor strand the token.
	canceledCtx, canceled := context.WithCancel(context.Background())
	canceled()
	if err := gate.Lock(canceledCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("ready gate with canceled context err=%v, want context.Canceled", err)
	}
	if err := gate.Lock(freshCtx); err != nil {
		t.Fatalf("canceled waiter consumed gate token: %v", err)
	}
	gate.Unlock()
}

func TestStartupPhaseErrorRetainsSafeTypedDeadlineCauses(t *testing.T) {
	for _, marker := range []error{
		store.ErrLeaseOperational,
		store.ErrAdmissionContention,
		store.ErrNoSlot,
		ErrPoolClosed,
		store.ErrRelocationStateUnknown,
	} {
		t.Run(marker.Error(), func(t *testing.T) {
			parent := context.Background()
			startup, cancel := context.WithDeadline(parent, time.Now().Add(-time.Millisecond))
			defer cancel()
			cause := fmt.Errorf("%w: bounded startup work: %w",
				marker, context.DeadlineExceeded)
			err := startupPhaseError(parent, startup, cause)
			if !errors.Is(err, ErrUpstreamNotReady) || !errors.Is(err, marker) {
				t.Fatalf("startupPhaseError=%v, want readiness and %v markers", err, marker)
			}
		})
	}
}

func TestPoolHealthySharedAttachUsesValidatedPreRoll(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	pool := &Pool{
		logger:    logger,
		streamers: make(map[uuid.UUID]pumpEntry),
	}
	streamID := uuid.New()
	pump := NewStreamer(streamID.String(), "http://unused.test", logger, nil)
	_, cancelPump := context.WithCancel(context.Background())
	defer cancelPump()
	pool.streamers[streamID] = pumpEntry{pump: pump, cancel: cancelPump}

	eligible := []byte("eligible-same-attempt-preroll")
	validation := []byte("next-live-chunk")
	// Model the first slice released by the pump's private media-prefix gate.
	// A retained marker makes this same-attempt ring snapshot safe without a
	// second ffprobe; an evicted marker is covered by the late-join tests.
	pump.fanoutValidatedStart(eligible)
	type startupResult struct {
		pump        streamPump
		chunks      <-chan startupChunk
		unsubscribe func()
		first       startupChunk
		err         error
	}
	result := make(chan startupResult, 1)
	go func() {
		s, chunks, unsubscribe, first, err := pool.subscribeForStartup(
			context.Background(), store.Lease{ActiveStreamID: streamID}, "dvr-client")
		result <- startupResult{
			pump: s, chunks: chunks, unsubscribe: unsubscribe, first: first, err: err,
		}
	}()
	deadline := time.Now().Add(time.Second)
	for pump.SubscriberCount() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if pump.SubscriberCount() != 1 {
		t.Fatal("Pool did not attach startup subscriber to healthy shared pump")
	}
	select {
	case got := <-result:
		if got.unsubscribe != nil {
			got.unsubscribe()
		}
		t.Fatalf("Pool exposed pre-roll before current-attempt validation: err=%v first=%q",
			got.err, got.first.data)
	default:
	}
	pump.fanout(validation)
	select {
	case got := <-result:
		if got.unsubscribe != nil {
			defer got.unsubscribe()
		}
		if got.err != nil {
			t.Fatalf("Pool startup attach: %v", got.err)
		}
		if got.pump != pump {
			t.Fatal("Pool did not retain the healthy shared pump")
		}
		if !bytes.Equal(got.first.data, eligible) {
			t.Fatalf("Pool first media=%q, want eligible pre-roll %q", got.first.data, eligible)
		}
		select {
		case chunk := <-got.chunks:
			if !bytes.Equal(chunk.data, validation) {
				t.Fatalf("post-pre-roll chunk=%q, want validation %q", chunk.data, validation)
			}
		case <-time.After(time.Second):
			t.Fatal("Pool did not queue validating live chunk after pre-roll")
		}
	case <-time.After(time.Second):
		t.Fatal("Pool startup attach did not complete after live validation")
	}
}

type stoppingPreRollPump struct {
	stale       []byte
	attached    chan struct{}
	attachOnce  sync.Once
	stopping    atomic.Bool
	subscribers atomic.Int32
}

func (p *stoppingPreRollPump) Run(ctx context.Context) { <-ctx.Done() }

func (p *stoppingPreRollPump) SubscribeForStartup(string) (<-chan startupChunk, <-chan startupChunk, <-chan struct{}, func()) {
	firstReal := make(chan startupChunk, 1)
	chunks := make(chan startupChunk)
	p.subscribers.Add(1)
	firstReal <- startupChunk{data: append([]byte(nil), p.stale...)}
	// Deterministically model the exact barrier: getOrStart already accepted
	// this mapped pump and its ring preload, then the pump commits to stopping
	// before OnStopping can detach it from Pool.
	p.stopping.Store(true)
	p.attachOnce.Do(func() { close(p.attached) })
	var once sync.Once
	return firstReal, chunks, make(chan struct{}), func() {
		once.Do(func() { p.subscribers.Add(-1) })
	}
}

func (p *stoppingPreRollPump) SubscriberCount() int          { return int(p.subscribers.Load()) }
func (p *stoppingPreRollPump) MediaReady() bool              { return true }
func (p *stoppingPreRollPump) MediaEpoch() uint64            { return 0 }
func (p *stoppingPreRollPump) Stopping() bool                { return p.stopping.Load() }
func (p *stoppingPreRollPump) ContentType() string           { return "video/mp2t" }
func (p *stoppingPreRollPump) TerminalError() error          { return nil }
func (p *stoppingPreRollPump) LastRealChunk() time.Time      { return time.Time{} }
func (p *stoppingPreRollPump) UpstreamInterruptions() uint64 { return 0 }

type signalingBuffer struct {
	bytes.Buffer
	wrote chan struct{}
	once  sync.Once
}

func TestPhysicalExitFenceFailsClosedAndHonorsDeadline(t *testing.T) {
	closed := make(chan struct{})
	close(closed)
	var canceled atomic.Int32
	if err := cancelAndWaitForPhysicalExit(context.Background(), []pumpEntry{{
		cancel: func() { canceled.Add(1) }, physicalExit: closed,
	}}); err != nil {
		t.Fatalf("closed physical exit: %v", err)
	}
	if canceled.Load() != 1 {
		t.Fatalf("closed physical exit cancel calls=%d, want 1", canceled.Load())
	}

	blocked := make(chan struct{})
	deadlineCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := cancelAndWaitForPhysicalExit(deadlineCtx, []pumpEntry{{physicalExit: blocked}})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked physical exit error=%v, want deadline", err)
	}
	if err := cancelAndWaitForPhysicalExit(context.Background(), []pumpEntry{{}}); err == nil {
		t.Fatal("missing physical-exit proof was accepted")
	}
}

func (w *signalingBuffer) Write(p []byte) (int, error) {
	n, err := w.Buffer.Write(p)
	if n > 0 {
		w.once.Do(func() { close(w.wrote) })
	}
	return n, err
}

// A ring-populated pump can enter its terminal state between the process-map
// lookup and startup subscription. The stale pre-roll must be discarded and
// the same durable DVR token must start a fresh generation; it must never be
// released/reacquired or written to the recording.
func TestIntegrationStoppingPumpPreRollIsDiscardedBeforeDVRWrite(t *testing.T) {
	if os.Getenv("CONDUCTOR_INT_TEST") != "1" {
		t.Skip("integration test skipped (set CONDUCTOR_INT_TEST=1 to enable)")
	}
	dsn := os.Getenv("CONDUCTOR_STREAM_TEST_DSN")
	if dsn == "" {
		t.Skip("CONDUCTOR_STREAM_TEST_DSN is required for internal stream integration test")
	}
	ctx := context.Background()
	db, err := store.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	realMedia := bytes.Repeat([]byte("REAL-MEDIA-188-BYTE-PACKET-"), 4096)
	originStarted := make(chan struct{})
	releaseMedia := make(chan struct{})
	var originOnce sync.Once
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		originOnce.Do(func() { close(originStarted) })
		select {
		case <-releaseMedia:
		case <-r.Context().Done():
			return
		}
		for {
			if _, err := w.Write(realMedia); err != nil {
				return
			}
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
			select {
			case <-r.Context().Done():
				return
			default:
			}
		}
	}))
	defer origin.Close()

	provider, err := db.CreateProvider(ctx, store.Provider{
		Name: "stopping-preroll-" + uuid.NewString(), Kind: "m3u_xtream", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateCredential(ctx, store.ProviderCredential{
		ProviderID: provider.ID, Username: "stopping", PasswordEnc: []byte{1},
		MaxStreams: 1, Priority: 100, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	channel, err := db.CreateChannel(ctx, store.Channel{
		Number: 8889, Name: "stopping-preroll", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateChannelSource(ctx, store.ChannelSource{
		ChannelID: channel.ID, ProviderID: provider.ID, UpstreamURL: origin.URL,
		Priority: 0, HealthScore: 1, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}

	pool := NewPool(slog.New(slog.NewTextHandler(io.Discard, nil)), db,
		store.PassthroughResolverForTest{})
	defer pool.Close()
	reservation, err := pool.ReserveWriter(ctx, channel.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer reservation.Release()

	stale := []byte("STALE-PRE-ROLL-MUST-NOT-BE-WRITTEN")
	fake := &stoppingPreRollPump{stale: stale, attached: make(chan struct{})}
	fakeCtx, cancelFake := context.WithCancel(context.Background())
	fakeCanceled := make(chan struct{})
	releaseFakeTeardown := make(chan struct{})
	fakePhysicalExit := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-releaseFakeTeardown:
		default:
			close(releaseFakeTeardown)
		}
	})
	go func() {
		<-fakeCtx.Done()
		close(fakeCanceled)
		<-releaseFakeTeardown
		close(fakePhysicalExit)
	}()
	pool.mu.Lock()
	pool.streamers[reservation.lease.ActiveStreamID] = pumpEntry{
		pump: fake, cancel: cancelFake, physicalExit: fakePhysicalExit,
	}
	pool.mu.Unlock()

	serveCtx, cancelServe := context.WithCancel(context.Background())
	writer := &signalingBuffer{wrote: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		_, _, serveErr := reservation.Serve(serveCtx, writer)
		done <- serveErr
	}()
	select {
	case <-fake.attached:
	case <-time.After(time.Second):
		t.Fatal("reservation did not attach to the ring-populated stopping pump")
	}
	select {
	case <-fakeCanceled:
	case <-time.After(time.Second):
		t.Fatal("stopping pump was not unpublished before replacement")
	}
	select {
	case <-originStarted:
		t.Fatal("replacement opened its provider before prior physical teardown")
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseFakeTeardown)
	select {
	case <-originStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("replacement generation did not start the authoritative origin")
	}

	var clients, tokens int
	var exactToken bool
	if err := db.Pool.QueryRow(ctx, `
		SELECT a.client_count,
		       (SELECT COUNT(*)::int FROM stream_client WHERE active_stream_id=a.id),
		       EXISTS(SELECT 1 FROM stream_client
		               WHERE active_stream_id=a.id AND id=$2)
		  FROM active_stream a WHERE a.id=$1`,
		reservation.lease.ActiveStreamID, reservation.lease.LeaseClientID).
		Scan(&clients, &tokens, &exactToken); err != nil {
		t.Fatal(err)
	}
	if clients != 1 || tokens != 1 || !exactToken {
		t.Fatalf("replacement crossed durable-token boundary: clients=%d tokens=%d exact=%v",
			clients, tokens, exactToken)
	}

	close(releaseMedia)
	select {
	case <-writer.wrote:
	case <-time.After(3 * time.Second):
		t.Fatal("replacement origin delivered no media")
	}
	cancelServe()
	select {
	case serveErr := <-done:
		if !errors.Is(serveErr, context.Canceled) {
			t.Fatalf("Serve error=%v, want context cancellation", serveErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("reservation Serve did not stop after cancellation")
	}
	got := writer.Bytes()
	if bytes.Contains(got, stale) {
		t.Fatalf("recording contains stale terminal pre-roll: %q", stale)
	}
	if !bytes.Contains(got, []byte("REAL-MEDIA-188-BYTE-PACKET-")) {
		t.Fatalf("recording contains no replacement media (%d bytes)", len(got))
	}
}

// Reload failure happens before any pump or response bytes exist. It must stay
// an operational startup error so the API can return its retryable 503, while
// exact-token abort cleanup releases the admitted DVR capacity.
func TestIntegrationReservationReloadFailureIsPreByteOperationalError(t *testing.T) {
	if os.Getenv("CONDUCTOR_INT_TEST") != "1" {
		t.Skip("integration test skipped (set CONDUCTOR_INT_TEST=1 to enable)")
	}
	dsn := os.Getenv("CONDUCTOR_STREAM_TEST_DSN")
	if dsn == "" {
		t.Skip("CONDUCTOR_STREAM_TEST_DSN is required for internal stream integration test")
	}
	ctx := context.Background()
	db, err := store.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	provider, err := db.CreateProvider(ctx, store.Provider{
		Name: "reservation-reload-" + uuid.NewString(), Kind: "m3u_xtream", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateCredential(ctx, store.ProviderCredential{
		ProviderID: provider.ID, Username: "reload", PasswordEnc: []byte{1},
		MaxStreams: 1, Priority: 100, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	channel, err := db.CreateChannel(ctx, store.Channel{
		Number: 8888, Name: "reservation-reload", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateChannelSource(ctx, store.ChannelSource{
		ChannelID: channel.ID, ProviderID: provider.ID,
		UpstreamURL: "http://must-not-open.test/stream",
		Priority:    0, HealthScore: 1, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	pool := NewPool(slog.New(slog.NewTextHandler(io.Discard, nil)), db,
		store.PassthroughResolverForTest{})
	defer pool.Close()
	reservation, err := pool.ReserveWriter(ctx, channel.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	streamID := reservation.lease.ActiveStreamID
	if _, err := db.Pool.Exec(ctx,
		`UPDATE active_stream SET state='dead' WHERE id=$1`, streamID); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	_, _, err = reservation.Serve(ctx, &output)
	if !errors.Is(err, store.ErrLeaseOperational) || !errors.Is(err, store.ErrStreamGone) {
		t.Fatalf("reservation Serve err=%v, want pre-byte operational/stream-gone error", err)
	}
	if output.Len() != 0 {
		t.Fatalf("reload failure wrote %d bytes before returning", output.Len())
	}
	cleanupCtx, cancelCleanup := context.WithTimeout(ctx, 2*time.Second)
	if err := pool.WaitForPumps(cleanupCtx); err != nil {
		cancelCleanup()
		t.Fatalf("wait for off-path abort cleanup: %v", err)
	}
	cancelCleanup()
	var clients, tokens int
	if err := db.Pool.QueryRow(ctx, `
		SELECT a.client_count,
		       (SELECT COUNT(*)::int FROM stream_client WHERE active_stream_id=a.id)
		  FROM active_stream a WHERE a.id=$1`, streamID).Scan(&clients, &tokens); err != nil {
		t.Fatal(err)
	}
	if clients != 0 || tokens != 0 {
		t.Fatalf("reload abort cleanup clients=%d tokens=%d, want 0/0", clients, tokens)
	}
}

// A durable reservation can reach its absolute Plex deadline while the
// authoritative pump-generation claim waits on the channel boundary. The
// response stays pre-byte/retryable, but the operational DB classification
// must survive alongside ErrUpstreamNotReady for diagnostics and scheduling.
func TestIntegrationBlockedPumpClaimRetainsOperationalDeadlineCause(t *testing.T) {
	if os.Getenv("CONDUCTOR_INT_TEST") != "1" {
		t.Skip("integration test skipped (set CONDUCTOR_INT_TEST=1 to enable)")
	}
	dsn := os.Getenv("CONDUCTOR_STREAM_TEST_DSN")
	if dsn == "" {
		t.Skip("CONDUCTOR_STREAM_TEST_DSN is required for internal stream integration test")
	}
	ctx := context.Background()
	db, err := store.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	provider, err := db.CreateProvider(ctx, store.Provider{
		Name: "blocked-claim-" + uuid.NewString(), Kind: "m3u_xtream", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateCredential(ctx, store.ProviderCredential{
		ProviderID: provider.ID, Username: "blocked-claim", PasswordEnc: []byte{1},
		MaxStreams: 1, Priority: 100, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	channel, err := db.CreateChannel(ctx, store.Channel{
		Number: 8890, Name: "blocked-claim", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateChannelSource(ctx, store.ChannelSource{
		ChannelID: channel.ID, ProviderID: provider.ID,
		UpstreamURL: "http://must-not-open.test/stream",
		Priority:    0, HealthScore: 1, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	pool := NewPool(slog.New(slog.NewTextHandler(io.Discard, nil)), db,
		store.PassthroughResolverForTest{})
	defer pool.Close()
	reservation, err := pool.ReserveWriter(ctx, channel.ID, 0)
	if err != nil {
		t.Fatal(err)
	}

	lockTx, err := db.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	lockHeld := true
	defer func() {
		if lockHeld {
			_ = lockTx.Rollback(context.Background())
		}
	}()
	if _, err := lockTx.Exec(ctx, `
		SELECT pg_advisory_xact_lock(
			hashtextextended('conductor:channel-lease:' || $1::text, 0)
		)`, channel.ID); err != nil {
		t.Fatal(err)
	}

	startupCtx := WithStartupDeadline(context.Background(), time.Now().Add(500*time.Millisecond))
	var output bytes.Buffer
	done := make(chan error, 1)
	go func() {
		_, _, serveErr := reservation.Serve(startupCtx, &output)
		done <- serveErr
	}()
	blocked := false
	for deadline := time.Now().Add(400 * time.Millisecond); time.Now().Before(deadline); {
		if err := db.Pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM pg_stat_activity
				 WHERE datname=current_database()
				   AND wait_event_type='Lock'
				   AND query LIKE '%pg_advisory_xact_lock%'
			)`).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !blocked {
		t.Fatal("pump-generation claim did not block on channel boundary")
	}
	select {
	case serveErr := <-done:
		if !errors.Is(serveErr, ErrUpstreamNotReady) ||
			!errors.Is(serveErr, store.ErrLeaseOperational) {
			t.Fatalf("blocked claim error=%v, want readiness + operational markers", serveErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked pump claim did not honor startup deadline")
	}
	if output.Len() != 0 {
		t.Fatalf("blocked claim wrote %d bytes before returning", output.Len())
	}
	if err := lockTx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	lockHeld = false
	cleanupCtx, cancelCleanup := context.WithTimeout(ctx, 3*time.Second)
	defer cancelCleanup()
	if err := pool.WaitForPumps(cleanupCtx); err != nil {
		t.Fatalf("wait for blocked-claim cleanup: %v", err)
	}
	var clients, tokens int
	if err := db.Pool.QueryRow(ctx, `
		SELECT a.client_count,
		       (SELECT COUNT(*)::int FROM stream_client WHERE active_stream_id=a.id)
		  FROM active_stream a WHERE a.id=$1`, reservation.lease.ActiveStreamID).
		Scan(&clients, &tokens); err != nil {
		t.Fatal(err)
	}
	if clients != 0 || tokens != 0 {
		t.Fatalf("blocked-claim cleanup clients=%d tokens=%d, want 0/0", clients, tokens)
	}
}

type cleanupBlockingPump struct {
	cancelSeen chan struct{}
	release    chan struct{}
}

func (p *cleanupBlockingPump) Run(ctx context.Context) {
	<-ctx.Done()
	close(p.cancelSeen)
	<-p.release
}
func (*cleanupBlockingPump) SubscribeForStartup(string) (<-chan startupChunk, <-chan startupChunk, <-chan struct{}, func()) {
	return make(chan startupChunk), make(chan startupChunk), make(chan struct{}), func() {}
}
func (*cleanupBlockingPump) SubscriberCount() int          { return 0 }
func (*cleanupBlockingPump) MediaReady() bool              { return false }
func (*cleanupBlockingPump) MediaEpoch() uint64            { return 0 }
func (*cleanupBlockingPump) Stopping() bool                { return false }
func (*cleanupBlockingPump) HeadersReady() bool            { return false }
func (*cleanupBlockingPump) ContentType() string           { return "" }
func (*cleanupBlockingPump) TerminalError() error          { return nil }
func (*cleanupBlockingPump) LastRealChunk() time.Time      { return time.Time{} }
func (*cleanupBlockingPump) UpstreamInterruptions() uint64 { return 0 }

// Close cancels pumps but runtime ownership may be released only after their
// Run/OnExit cleanup returns. WaitForPumps must therefore block (bounded by its
// caller's context) until that teardown is actually quiescent.
func TestPoolWaitForPumpsIncludesTeardown(t *testing.T) {
	p := &Pool{streamers: make(map[uuid.UUID]pumpEntry)}
	pump := &cleanupBlockingPump{
		cancelSeen: make(chan struct{}),
		release:    make(chan struct{}),
	}
	pumpCtx, cancelPump := context.WithCancel(context.Background())
	streamID := uuid.New()
	p.mu.Lock()
	p.streamers[streamID] = pumpEntry{pump: pump, cancel: cancelPump}
	p.pumpWG.Add(1)
	p.mu.Unlock()
	go p.runPump(pumpCtx, pump)

	p.Close()
	select {
	case <-pump.cancelSeen:
	case <-time.After(time.Second):
		t.Fatal("Pool.Close did not cancel tracked pump")
	}
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	err := p.WaitForPumps(waitCtx)
	waitCancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitForPumps returned %v while teardown remained blocked", err)
	}
	close(pump.release)
	quiesceCtx, quiesceCancel := context.WithTimeout(context.Background(), time.Second)
	defer quiesceCancel()
	if err := p.WaitForPumps(quiesceCtx); err != nil {
		t.Fatalf("WaitForPumps after teardown release: %v", err)
	}
}

type capacityDrainBlockingPump struct {
	cancelSeen chan struct{}
	release    chan struct{}
	exited     chan struct{}
	onStopping func()
	onExit     func()
}

type detachedExitBarrierPump struct {
	begin      chan struct{}
	stopped    chan struct{}
	finish     chan struct{}
	exited     chan struct{}
	stopping   atomic.Bool
	onStopping func()
	onExit     func()
}

func (p *detachedExitBarrierPump) Run(ctx context.Context) {
	select {
	case <-p.begin:
	case <-ctx.Done():
	}
	p.stopping.Store(true)
	if p.onStopping != nil {
		p.onStopping()
	}
	close(p.stopped)
	<-p.finish
	if p.onExit != nil {
		p.onExit()
	}
	close(p.exited)
}

func (*detachedExitBarrierPump) SubscribeForStartup(string) (<-chan startupChunk, <-chan startupChunk, <-chan struct{}, func()) {
	return make(chan startupChunk), make(chan startupChunk), make(chan struct{}), func() {}
}
func (*detachedExitBarrierPump) SubscriberCount() int          { return 0 }
func (*detachedExitBarrierPump) MediaReady() bool              { return true }
func (*detachedExitBarrierPump) MediaEpoch() uint64            { return 0 }
func (p *detachedExitBarrierPump) Stopping() bool              { return p.stopping.Load() }
func (*detachedExitBarrierPump) ContentType() string           { return "video/mp2t" }
func (*detachedExitBarrierPump) TerminalError() error          { return nil }
func (*detachedExitBarrierPump) LastRealChunk() time.Time      { return time.Time{} }
func (*detachedExitBarrierPump) UpstreamInterruptions() uint64 { return 0 }

func (p *capacityDrainBlockingPump) Run(ctx context.Context) {
	<-ctx.Done()
	close(p.cancelSeen)
	<-p.release
	if p.onStopping != nil {
		p.onStopping()
	}
	if p.onExit != nil {
		p.onExit()
	}
	close(p.exited)
}
func (*capacityDrainBlockingPump) SubscribeForStartup(string) (<-chan startupChunk, <-chan startupChunk, <-chan struct{}, func()) {
	return make(chan startupChunk), make(chan startupChunk), make(chan struct{}), func() {}
}
func (*capacityDrainBlockingPump) SubscriberCount() int          { return 0 }
func (*capacityDrainBlockingPump) MediaReady() bool              { return true }
func (*capacityDrainBlockingPump) MediaEpoch() uint64            { return 0 }
func (*capacityDrainBlockingPump) Stopping() bool                { return false }
func (*capacityDrainBlockingPump) HeadersReady() bool            { return true }
func (*capacityDrainBlockingPump) ContentType() string           { return "video/mp2t" }
func (*capacityDrainBlockingPump) TerminalError() error          { return nil }
func (*capacityDrainBlockingPump) LastRealChunk() time.Time      { return time.Time{} }
func (*capacityDrainBlockingPump) UpstreamInterruptions() uint64 { return 0 }

// The pump-less replay fallback must never broaden into cancellation. A client
// can attach and install a replacement after the first local-map check, while
// an exiting generation remains the physical capacity owner until OnExit.
func TestFinalizeDrainingIfPumpLessLeavesLocalOwnersUntouched(t *testing.T) {
	for _, tc := range []struct {
		name    string
		install func(*Pool, uuid.UUID, pumpEntry)
	}{
		{
			name: "intervening active client pump",
			install: func(pool *Pool, id uuid.UUID, entry pumpEntry) {
				pool.streamers[id] = entry
			},
		},
		{
			name: "tracked exiting pump",
			install: func(pool *Pool, id uuid.UUID, entry pumpEntry) {
				pool.exitingPumps[id] = []pumpEntry{entry}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			channelID := uuid.New()
			streamID := uuid.New()
			var cancels atomic.Int32
			pool := &Pool{
				streamers:    make(map[uuid.UUID]pumpEntry),
				exitingPumps: make(map[uuid.UUID][]pumpEntry),
			}
			entry := pumpEntry{
				pump: &capacityDrainBlockingPump{},
				cancel: func() {
					cancels.Add(1)
				},
			}
			tc.install(pool, streamID, entry)

			// DB is deliberately nil: local ownership must make the conditional
			// no-pump finalizer return before either DB mutation or cancellation.
			pool.finalizeDrainingIfPumpLess(channelID, streamID)
			if got := cancels.Load(); got != 0 {
				t.Fatalf("local owner cancel calls=%d, want zero", got)
			}
		})
	}
}

// Releasing the last client must keep a provider slot occupied while its pump
// is still tearing down. The next channel may acquire only after OnExit marks
// the draining row dead and publishes the capacity edge.
func TestIntegrationDrainingPumpHoldsCapacityUntilTeardown(t *testing.T) {
	if os.Getenv("CONDUCTOR_INT_TEST") != "1" {
		t.Skip("integration test skipped (set CONDUCTOR_INT_TEST=1 to enable)")
	}
	dsn := os.Getenv("CONDUCTOR_STREAM_TEST_DSN")
	if dsn == "" {
		t.Skip("CONDUCTOR_STREAM_TEST_DSN is required for internal stream integration test")
	}
	ctx := context.Background()
	db, err := store.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	provider, err := db.CreateProvider(ctx, store.Provider{
		Name: "draining-capacity", Kind: "m3u_xtream", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateCredential(ctx, store.ProviderCredential{
		ProviderID: provider.ID, Username: "draining", PasswordEnc: []byte{1},
		MaxStreams: 1, Priority: 100, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	seedChannel := func(number float64, name string) store.Channel {
		t.Helper()
		channel, err := db.CreateChannel(ctx, store.Channel{
			Number: number, Name: name, Enabled: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.CreateChannelSource(ctx, store.ChannelSource{
			ChannelID: channel.ID, ProviderID: provider.ID,
			UpstreamURL: "http://unused.test/" + name,
			Priority:    0, HealthScore: 1, Enabled: true,
		}); err != nil {
			t.Fatal(err)
		}
		return channel
	}
	first := seedChannel(881, "draining-first")
	second := seedChannel(882, "draining-second")
	pool := NewPool(slog.New(slog.NewTextHandler(io.Discard, nil)), db,
		store.PassthroughResolverForTest{})
	defer pool.Close()
	reservation, err := pool.ReserveWriter(ctx, first.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	reservation.lease, err = db.ClaimPumpLeaseForClient(
		ctx, reservation.lease.ActiveStreamID, reservation.lease.LeaseClientID)
	if err != nil {
		t.Fatal(err)
	}
	pump := &capacityDrainBlockingPump{
		cancelSeen: make(chan struct{}),
		release:    make(chan struct{}),
		exited:     make(chan struct{}),
	}
	pump.onStopping = func() { pool.detachPump(reservation.lease.ActiveStreamID, pump) }
	pump.onExit = func() {
		pool.finishPump(reservation.lease.ActiveStreamID,
			reservation.lease.PumpGeneration, pump)
	}
	pumpCtx, pumpCancel := context.WithCancel(context.Background())
	pool.mu.Lock()
	pool.streamers[reservation.lease.ActiveStreamID] = pumpEntry{pump: pump, cancel: pumpCancel}
	pool.pumpWG.Add(1)
	pool.mu.Unlock()
	go pool.runPump(pumpCtx, pump)

	reservation.Release()
	select {
	case <-pump.cancelSeen:
	case <-time.After(time.Second):
		t.Fatal("last-client release did not cancel pump")
	}
	var state string
	if err := db.Pool.QueryRow(ctx,
		`SELECT state::text FROM active_stream WHERE id = $1`, reservation.lease.ActiveStreamID).
		Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "draining" {
		t.Fatalf("released pump state=%q, want draining during teardown", state)
	}
	if active := pool.Active(ctx); active != 1 {
		t.Fatalf("admin active streams=%d, want draining slot visible", active)
	}
	if _, err := pool.ReserveWriter(ctx, second.ID, 0); !errors.Is(err, store.ErrNoSlot) {
		t.Fatalf("second channel admitted before teardown: %v", err)
	}
	select {
	case <-pool.CapacityChanges():
		t.Fatal("capacity wake emitted before pump teardown")
	default:
	}

	close(pump.release)
	select {
	case <-pump.exited:
	case <-time.After(time.Second):
		t.Fatal("pump teardown did not finish")
	}
	select {
	case <-pool.CapacityChanges():
	case <-time.After(time.Second):
		t.Fatal("pump OnExit emitted no capacity wake")
	}
	retried, err := pool.ReserveWriter(ctx, second.ID, 0)
	if err != nil {
		t.Fatalf("second channel retry after teardown: %v", err)
	}
	retried.Release()
}

// OnStopping must remove a terminal pump from attachment before subscribers
// close, but that detached generation still owns physical provider teardown.
// If the last durable client releases in the OnStopping→OnExit window, Pool
// must keep the row draining and withhold both admission and its capacity edge.
func TestIntegrationDetachedPumpHoldsCapacityUntilOnExit(t *testing.T) {
	if os.Getenv("CONDUCTOR_INT_TEST") != "1" {
		t.Skip("integration test skipped (set CONDUCTOR_INT_TEST=1 to enable)")
	}
	dsn := os.Getenv("CONDUCTOR_STREAM_TEST_DSN")
	if dsn == "" {
		t.Skip("CONDUCTOR_STREAM_TEST_DSN is required for internal stream integration test")
	}
	ctx := context.Background()
	db, err := store.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	provider, err := db.CreateProvider(ctx, store.Provider{
		Name: "detached-exit-capacity-" + uuid.NewString(),
		Kind: "m3u_xtream", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateCredential(ctx, store.ProviderCredential{
		ProviderID: provider.ID, Username: "detached-exit", PasswordEnc: []byte{1},
		MaxStreams: 1, Priority: 100, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	seedChannel := func(number float64, name string) store.Channel {
		t.Helper()
		channel, err := db.CreateChannel(ctx, store.Channel{
			Number: number, Name: name, Enabled: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.CreateChannelSource(ctx, store.ChannelSource{
			ChannelID: channel.ID, ProviderID: provider.ID,
			UpstreamURL: "http://unused.test/" + name,
			Priority:    0, HealthScore: 1, Enabled: true,
		}); err != nil {
			t.Fatal(err)
		}
		return channel
	}
	first := seedChannel(883, "detached-exit-first")
	second := seedChannel(884, "detached-exit-second")
	pool := NewPool(slog.New(slog.NewTextHandler(io.Discard, nil)), db,
		store.PassthroughResolverForTest{})
	defer pool.Close()
	markReturned := make(chan struct{})
	allowExitCompletion := make(chan struct{})
	pool.pumpExitAfterMarkHook = func() {
		close(markReturned)
		<-allowExitCompletion
	}
	reservation, err := pool.ReserveWriter(ctx, first.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	reservation.lease, err = db.ClaimPumpLeaseForClient(
		ctx, reservation.lease.ActiveStreamID, reservation.lease.LeaseClientID)
	if err != nil {
		t.Fatal(err)
	}
	pump := &detachedExitBarrierPump{
		begin:   make(chan struct{}),
		stopped: make(chan struct{}),
		finish:  make(chan struct{}),
		exited:  make(chan struct{}),
	}
	pump.onStopping = func() {
		pool.detachPump(reservation.lease.ActiveStreamID, pump)
	}
	pump.onExit = func() {
		pool.finishPump(reservation.lease.ActiveStreamID,
			reservation.lease.PumpGeneration, pump)
	}
	pumpCtx, pumpCancel := context.WithCancel(context.Background())
	pool.mu.Lock()
	pool.streamers[reservation.lease.ActiveStreamID] = pumpEntry{
		pump: pump, cancel: pumpCancel,
	}
	pool.pumpWG.Add(1)
	pool.mu.Unlock()
	go pool.runPump(pumpCtx, pump)

	close(pump.begin)
	select {
	case <-pump.stopped:
	case <-time.After(time.Second):
		t.Fatal("pump did not reach detached OnStopping barrier")
	}
	pool.mu.Lock()
	_, attachVisible := pool.streamers[reservation.lease.ActiveStreamID]
	exiting := len(pool.exitingPumps[reservation.lease.ActiveStreamID])
	pool.mu.Unlock()
	if attachVisible || exiting != 1 {
		t.Fatalf("terminal generation attach-visible=%v exiting=%d, want false/1",
			attachVisible, exiting)
	}

	// Let OnExit's first generation CAS observe the still-held token and return
	// capacityFreed=false, then pause before it removes the exiting generation.
	close(pump.finish)
	select {
	case <-markReturned:
	case <-time.After(time.Second):
		t.Fatal("OnExit did not reach post-MarkDead(false) barrier")
	}
	reservation.Release()
	var state string
	var clients, tokens int
	deadline := time.Now().Add(3 * time.Second)
	for {
		err := db.Pool.QueryRow(ctx, `
			SELECT a.state::text, a.client_count,
			       (SELECT COUNT(*)::int FROM stream_client sc
			         WHERE sc.active_stream_id = a.id)
			  FROM active_stream a
			 WHERE a.id = $1`, reservation.lease.ActiveStreamID).
			Scan(&state, &clients, &tokens)
		if err != nil {
			t.Fatal(err)
		}
		if state == "draining" && clients == 0 && tokens == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("detached release state=%q clients=%d tokens=%d, want draining/0/0",
				state, clients, tokens)
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case <-pool.CapacityChanges():
		t.Fatal("capacity wake emitted while detached pump still in OnExit")
	default:
	}
	if admitted, err := pool.ReserveWriter(ctx, second.ID, 0); !errors.Is(err, store.ErrNoSlot) {
		if admitted != nil {
			admitted.Release()
		}
		t.Fatalf("second channel admitted before detached OnExit: %v", err)
	}

	close(allowExitCompletion)
	select {
	case <-pump.exited:
	case <-time.After(time.Second):
		t.Fatal("detached pump OnExit did not finish")
	}
	select {
	case <-pool.CapacityChanges():
	case <-time.After(time.Second):
		t.Fatal("detached pump OnExit emitted no capacity wake")
	}
	retried, err := pool.ReserveWriter(ctx, second.ID, 0)
	if err != nil {
		t.Fatalf("second channel retry after detached OnExit: %v", err)
	}
	retried.Release()
}

func TestCleanupRejectsIncompleteLeaseIdentity(t *testing.T) {
	p := &Pool{}
	streamID := uuid.New()
	clientID := uuid.New()
	channelID := uuid.New()

	for _, lease := range []store.Lease{
		{},
		{ActiveStreamID: streamID},
		{LeaseClientID: clientID},
	} {
		p.releaseAndMaybeDrain(lease, nil)
		p.retryLeaseCleanup(lease, nil)
	}
	for _, lease := range []store.Lease{
		{},
		{ChannelID: channelID, ActiveStreamID: streamID},
		{ChannelID: channelID, LeaseClientID: clientID},
		{ActiveStreamID: streamID, LeaseClientID: clientID},
	} {
		p.retryAmbiguousLeaseCleanup(lease)
	}

	// Every entry point must return before registering background cleanup.
	// A missing guard would either panic on the nil DB or leave Wait blocked.
	done := make(chan struct{})
	go func() {
		p.cleanupWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("incomplete cleanup identity registered background work")
	}
}
