package stream

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/store"
)

type hardStaleTestPump struct {
	lastReal time.Time
	stopping atomic.Bool
}

func (*hardStaleTestPump) Run(context.Context) {}
func (*hardStaleTestPump) SubscribeForStartup(string) (<-chan startupChunk, <-chan startupChunk, <-chan struct{}, func()) {
	return make(chan startupChunk), make(chan startupChunk), make(chan struct{}), func() {}
}
func (*hardStaleTestPump) SubscriberCount() int            { return 1 }
func (p *hardStaleTestPump) LastRealChunk() time.Time      { return p.lastReal }
func (p *hardStaleTestPump) UpstreamInterruptions() uint64 { return 0 }
func (*hardStaleTestPump) MediaReady() bool                { return true }
func (*hardStaleTestPump) MediaEpoch() uint64              { return 1 }
func (p *hardStaleTestPump) Stopping() bool                { return p.stopping.Load() }
func (*hardStaleTestPump) ContentType() string             { return "video/mp2t" }
func (*hardStaleTestPump) TerminalError() error            { return nil }
func (p *hardStaleTestPump) tryTerminateHardStale(
	cutoff, installedAt time.Time,
) bool {
	lastActivity := p.lastReal
	if lastActivity.Before(installedAt) {
		lastActivity = installedAt
	}
	if lastActivity.IsZero() || lastActivity.After(cutoff) {
		return false
	}
	return p.stopping.CompareAndSwap(false, true)
}

func newHardStaleTestPool() *Pool {
	return &Pool{
		logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		streamers:       make(map[uuid.UUID]pumpEntry),
		exitingPumps:    make(map[uuid.UUID][]pumpEntry),
		clientRoles:     make(map[uuid.UUID]*streamClientRoles),
		capacityChanged: make(chan struct{}, 1),
	}
}

func TestDefaultStreamHardTimeoutExceedsReconnectWindow(t *testing.T) {
	if got := DefaultWatchdogConfig().HardTimeout; got <= defaultReconnectWindow {
		t.Fatalf("hard timeout=%s must exceed reconnect window=%s",
			got, defaultReconnectWindow)
	}
}

func TestHardStaleDBHeartbeatCannotKillLocallyActivePump(t *testing.T) {
	pool := newHardStaleTestPool()
	streamID, channelID, generation := uuid.New(), uuid.New(), uuid.New()
	cutoff := time.Now().Add(-3 * time.Minute)
	var cancels, durableCalls atomic.Int32
	pool.streamers[streamID] = pumpEntry{
		pump:        &hardStaleTestPump{lastReal: time.Now()},
		cancel:      func() { cancels.Add(1) },
		generation:  generation,
		installedAt: time.Now().Add(-10 * time.Minute),
	}
	pool.hardStaleFinalizeHook = func(
		context.Context,
		store.HardStaleStreamCandidate,
		time.Time,
		bool,
	) (bool, error) {
		durableCalls.Add(1)
		return true, nil
	}

	got := pool.ReapHardStaleStreams(context.Background(), []store.HardStaleStreamCandidate{{
		ID: streamID, ChannelID: channelID, PumpGeneration: generation,
	}}, cutoff)
	if got != 0 || durableCalls.Load() != 0 || cancels.Load() != 0 {
		t.Fatalf("fresh local pump reaped=%d durable_calls=%d cancels=%d, want 0/0/0",
			got, durableCalls.Load(), cancels.Load())
	}
	if _, ok := pool.streamers[streamID]; !ok {
		t.Fatal("fresh local pump was detached")
	}
}

func TestHardStalePreMediaPumpUsesInstallTimeAsLocalActivity(t *testing.T) {
	pool := newHardStaleTestPool()
	streamID, channelID, generation := uuid.New(), uuid.New(), uuid.New()
	cutoff := time.Now().Add(-3 * time.Minute)
	var durableCalls, cancels atomic.Int32
	pool.streamers[streamID] = pumpEntry{
		pump:        &hardStaleTestPump{}, // no real media has arrived yet
		cancel:      func() { cancels.Add(1) },
		generation:  generation,
		installedAt: time.Now().Add(-time.Minute),
	}
	pool.hardStaleFinalizeHook = func(
		context.Context,
		store.HardStaleStreamCandidate,
		time.Time,
		bool,
	) (bool, error) {
		durableCalls.Add(1)
		return true, nil
	}

	got := pool.ReapHardStaleStreams(context.Background(), []store.HardStaleStreamCandidate{{
		ID: streamID, ChannelID: channelID, PumpGeneration: generation,
	}}, cutoff)
	if got != 0 || durableCalls.Load() != 0 || cancels.Load() != 0 {
		t.Fatalf("recent pre-media pump reaped=%d durable_calls=%d cancels=%d, want 0/0/0",
			got, durableCalls.Load(), cancels.Load())
	}
}

func TestHardStaleExactGenerationStopsBeforeDurableCAS(t *testing.T) {
	pool := newHardStaleTestPool()
	streamID, channelID, generation := uuid.New(), uuid.New(), uuid.New()
	cutoff := time.Now().Add(-3 * time.Minute)
	var cancels atomic.Int32
	pool.streamers[streamID] = pumpEntry{
		pump:        &hardStaleTestPump{lastReal: time.Now().Add(-10 * time.Minute)},
		cancel:      func() { cancels.Add(1) },
		generation:  generation,
		installedAt: time.Now().Add(-10 * time.Minute),
	}
	pool.clientRoles[streamID] = &streamClientRoles{live: 1}
	pool.hardStaleFinalizeHook = func(
		_ context.Context,
		candidate store.HardStaleStreamCandidate,
		_ time.Time,
		hasLocalPump bool,
	) (bool, error) {
		if candidate.PumpGeneration != generation || !hasLocalPump {
			t.Fatalf("durable CAS candidate=%+v has_local=%v", candidate, hasLocalPump)
		}
		if cancels.Load() != 1 {
			t.Fatal("exact stale pump was not cancelled before durable finalization")
		}
		return true, nil
	}

	got := pool.ReapHardStaleStreams(context.Background(), []store.HardStaleStreamCandidate{{
		ID: streamID, ChannelID: channelID, PumpGeneration: generation,
	}}, cutoff)
	if got != 1 || cancels.Load() != 1 {
		t.Fatalf("stale exact generation reaped=%d cancels=%d, want 1/1",
			got, cancels.Load())
	}
	if _, ok := pool.streamers[streamID]; ok {
		t.Fatal("stale generation remained attachment-visible")
	}
	if exiting := pool.exitingPumps[streamID]; len(exiting) != 1 ||
		exiting[0].generation != generation {
		t.Fatalf("exiting generations=%+v, want exact %s", exiting, generation)
	}
	if _, ok := pool.clientRoles[streamID]; ok {
		t.Fatal("reaped generation retained local client roles")
	}
}

func TestHardStaleDecisionFailsClosedAcrossDurableCASOutcomes(t *testing.T) {
	simulated := errors.New("simulated finalization failure")
	for _, tc := range []struct {
		name       string
		finalized  bool
		err        error
		timeout    bool
		wantReaped int
	}{
		{name: "cas_rejected", wantReaped: 0},
		{name: "cas_committed", finalized: true, wantReaped: 1},
		{name: "cas_error", err: simulated, wantReaped: 0},
		{name: "cas_timeout", timeout: true, wantReaped: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := newHardStaleTestPool()
			streamID, channelID, generation := uuid.New(), uuid.New(), uuid.New()
			cutoff := time.Now().Add(-3 * time.Minute)
			lastReal := cutoff.Add(-time.Minute)
			pump := NewStreamer(streamID.String(), "", pool.logger, nil)
			pump.lastRealChunk.Store(lastReal.UnixNano())
			viewer, unsubscribe := pump.Subscribe("viewer")
			defer unsubscribe()
			pump.fanout([]byte{0x47, 0x00})
			select {
			case <-viewer:
			case <-time.After(time.Second):
				t.Fatal("baseline viewer media was not delivered")
			}
			pump.lastRealChunk.Store(lastReal.UnixNano())
			var cancels atomic.Int32
			physicalExit := make(chan struct{})
			pool.streamers[streamID] = pumpEntry{
				// Production cancellation only signals Run. Deliberately leave this
				// synthetic Run wedged: hard-stale terminalization itself must close
				// the real subscriber without fabricating an OnExit.
				pump: pump, cancel: func() { cancels.Add(1) }, physicalExit: physicalExit,
				generation: generation, installedAt: cutoff.Add(-2 * time.Minute),
			}

			finalizeEntered := make(chan struct{})
			allowFinalize := make(chan struct{})
			pool.hardStaleFinalizeHook = func(
				ctx context.Context,
				_ store.HardStaleStreamCandidate,
				_ time.Time,
				_ bool,
			) (bool, error) {
				close(finalizeEntered)
				if tc.timeout {
					<-ctx.Done()
					return false, ctx.Err()
				}
				<-allowFinalize
				return tc.finalized, tc.err
			}

			reapDone := make(chan int, 1)
			reapCtx := context.Background()
			var reapCancel context.CancelFunc
			if tc.timeout {
				reapCtx, reapCancel = context.WithTimeout(reapCtx, 100*time.Millisecond)
				defer reapCancel()
			}
			go func() {
				reapDone <- pool.ReapHardStaleStreams(
					reapCtx, []store.HardStaleStreamCandidate{{
						ID: streamID, ChannelID: channelID, PumpGeneration: generation,
					}}, cutoff)
			}()
			select {
			case <-finalizeEntered:
			case <-time.After(3 * time.Second):
				t.Fatal("durable hard-stale CAS was not reached")
			}

			if cancels.Load() != 1 || !pump.Stopping() {
				t.Fatalf("in-flight stale decision cancels=%d stopping=%v, want 1/true",
					cancels.Load(), pump.Stopping())
			}
			pool.mu.Lock()
			inFlightEntry, stillActive := pool.streamers[streamID]
			inFlightExiting := len(pool.exitingPumps[streamID])
			pool.mu.Unlock()
			if !stillActive || inFlightEntry.generation != generation || inFlightExiting != 0 {
				t.Fatalf("in-flight ownership active=%v entry=%+v exiting=%d, want exact active owner",
					stillActive, inFlightEntry, inFlightExiting)
			}
			select {
			case <-physicalExit:
				t.Fatal("subscriber terminalization fabricated physical pump exit")
			default:
			}
			select {
			case _, ok := <-viewer:
				if ok {
					t.Fatal("viewer received media after hard-stale terminal decision")
				}
			case <-time.After(time.Second):
				t.Fatal("viewer was not closed before database finalization completed")
			}
			mediaDone := make(chan struct{})
			go func() {
				pump.fanout([]byte{0x47, 0x01})
				close(mediaDone)
			}()
			select {
			case <-mediaDone:
			case <-time.After(time.Second):
				t.Fatal("post-stop fanout blocked on database finalization")
			}
			if got := pump.LastRealChunk(); !got.Equal(lastReal) {
				t.Fatalf("rejected recovery advanced activity %v -> %v", lastReal, got)
			}

			if !tc.timeout {
				close(allowFinalize)
			}
			var reaped int
			select {
			case reaped = <-reapDone:
			case <-time.After(3 * time.Second):
				t.Fatal("hard-stale reaper did not finish")
			}
			if reaped != tc.wantReaped || cancels.Load() != 1 || !pump.Stopping() {
				t.Fatalf("durable outcome reaped=%d cancels=%d stopping=%v, want %d/1/true",
					reaped, cancels.Load(), pump.Stopping(), tc.wantReaped)
			}
			pool.mu.Lock()
			_, activeAfter := pool.streamers[streamID]
			exitingAfter := append([]pumpEntry(nil), pool.exitingPumps[streamID]...)
			pool.mu.Unlock()
			if tc.finalized {
				if activeAfter || len(exitingAfter) != 1 || exitingAfter[0].generation != generation {
					t.Fatalf("committed ownership active=%v exiting=%+v, want exact exiting owner",
						activeAfter, exitingAfter)
				}
			} else if !activeAfter || len(exitingAfter) != 0 {
				t.Fatalf("uncommitted ownership active=%v exiting=%+v, want stopping active owner",
					activeAfter, exitingAfter)
			}
		})
	}
}

func TestHardStaleGenerationMismatchCannotCancelReplacement(t *testing.T) {
	pool := newHardStaleTestPool()
	streamID, channelID := uuid.New(), uuid.New()
	staleGeneration, replacementGeneration := uuid.New(), uuid.New()
	cutoff := time.Now().Add(-3 * time.Minute)
	var cancels atomic.Int32
	pool.streamers[streamID] = pumpEntry{
		pump:        &hardStaleTestPump{lastReal: time.Now().Add(-10 * time.Minute)},
		cancel:      func() { cancels.Add(1) },
		generation:  replacementGeneration,
		installedAt: time.Now().Add(-10 * time.Minute),
	}
	pool.hardStaleFinalizeHook = func(
		_ context.Context,
		_ store.HardStaleStreamCandidate,
		_ time.Time,
		hasLocalPump bool,
	) (bool, error) {
		if hasLocalPump {
			t.Fatal("replacement generation matched stale candidate")
		}
		return false, nil // durable generation CAS rejects the old snapshot
	}

	got := pool.ReapHardStaleStreams(context.Background(), []store.HardStaleStreamCandidate{{
		ID: streamID, ChannelID: channelID, PumpGeneration: staleGeneration,
	}}, cutoff)
	if got != 0 || cancels.Load() != 0 {
		t.Fatalf("replacement generation reaped=%d cancels=%d, want 0/0", got, cancels.Load())
	}
	if entry := pool.streamers[streamID]; entry.generation != replacementGeneration {
		t.Fatalf("replacement generation changed to %s", entry.generation)
	}
}

func TestHardStalePumpLessCandidateFreesCapacityImmediately(t *testing.T) {
	pool := newHardStaleTestPool()
	candidate := store.HardStaleStreamCandidate{
		ID: uuid.New(), ChannelID: uuid.New(), PumpGeneration: uuid.Nil,
	}
	pool.hardStaleFinalizeHook = func(
		_ context.Context,
		got store.HardStaleStreamCandidate,
		_ time.Time,
		hasLocalPump bool,
	) (bool, error) {
		if got != candidate || hasLocalPump {
			t.Fatalf("pump-less durable CAS candidate=%+v has_local=%v", got, hasLocalPump)
		}
		return true, nil
	}
	if got := pool.ReapHardStaleStreams(
		context.Background(), []store.HardStaleStreamCandidate{candidate}, time.Now()); got != 1 {
		t.Fatalf("pump-less reaped=%d, want 1", got)
	}
	select {
	case <-pool.CapacityChanges():
	default:
		t.Fatal("pump-less hard reap did not publish immediate capacity")
	}
}

func TestHardStaleRejectedDurableCASKeepsExactPumpFailClosed(t *testing.T) {
	pool := newHardStaleTestPool()
	streamID, generation := uuid.New(), uuid.New()
	var cancels atomic.Int32
	pool.streamers[streamID] = pumpEntry{
		pump:        &hardStaleTestPump{lastReal: time.Now().Add(-10 * time.Minute)},
		cancel:      func() { cancels.Add(1) },
		generation:  generation,
		installedAt: time.Now().Add(-10 * time.Minute),
	}
	pool.hardStaleFinalizeHook = func(
		context.Context,
		store.HardStaleStreamCandidate,
		time.Time,
		bool,
	) (bool, error) {
		return false, nil
	}
	candidate := store.HardStaleStreamCandidate{
		ID: streamID, ChannelID: uuid.New(), PumpGeneration: generation,
	}
	if got := pool.ReapHardStaleStreams(
		context.Background(), []store.HardStaleStreamCandidate{candidate},
		time.Now().Add(-3*time.Minute)); got != 0 {
		t.Fatalf("failed durable CAS reaped=%d, want 0", got)
	}
	if cancels.Load() != 1 {
		t.Fatalf("rejected durable CAS cancels=%d, want exact pump stopped once", cancels.Load())
	}
	entry, ok := pool.streamers[streamID]
	if !ok || entry.generation != generation || !entry.pump.Stopping() {
		t.Fatalf("rejected durable CAS entry=%+v present=%v, want exact stopping owner retained",
			entry, ok)
	}
}
