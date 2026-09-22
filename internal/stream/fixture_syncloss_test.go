package stream

// Replays the REAL mid-stream TS sync loss captured at byte 471412256 of
// tlc-commercial-break-20min.ts (the realigned tail sits off the original 188
// grid) through the fanout transform and the PCR pacer — the two layers that
// each independently tore the attempt down before the bounded resync existed.
// Env-gated like the other fixture tests; CI skips it.
//
//	CONDUCTOR_FIXTURE=$HOME/work/conductor-fixtures/tlc-commercial-break-20min.ts \
//	  go test ./internal/stream/ -run TestFixtureTransportResyncSurvivesRealSyncLoss -v

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/spencercnorton/conductor/internal/metrics"
)

func TestFixtureTransportResyncSurvivesRealSyncLoss(t *testing.T) {
	path := os.Getenv("CONDUCTOR_FIXTURE")
	if path == "" || !strings.Contains(path, "commercial-break") {
		t.Skip("set CONDUCTOR_FIXTURE to the tlc-commercial-break capture")
	}
	buf, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	const loss = 471412256
	if len(buf) < loss+400*1024 {
		t.Fatalf("capture shorter than expected: %d bytes", len(buf))
	}

	programs := scanTSProgramMaps(buf[:2*1024*1024], "")
	if len(programs) != 1 {
		t.Fatalf("programs=%d, want 1", len(programs))
	}
	sel := programs[0]
	program := avProgramPIDs{
		videoPID: sel.videoPID, pcrPID: sel.pcrPID,
		audioPIDs: sel.audioPIDs, audioCount: sel.audioCount,
		mapIdentity: sel.mapIdentity.clone(),
	}

	beforeResyncs := metrics.RelayTransportResyncs.Value()
	wall := time.Unix(1_700_000_000, 0)
	pacer := testTransportPCRPacer(false)
	pacer.ConfigureSelectedProgram(program, true)
	pacer.now = func() time.Time { return wall }
	pacer.wait = func(_ context.Context, delay time.Duration) error {
		wall = wall.Add(delay)
		return nil
	}

	window := buf[loss-400*1024 : loss+400*1024]
	emitted := 0
	for off := 0; off < len(window); off += chunkSize {
		end := min(off+chunkSize, len(window))
		arrival := wall
		media, err := pacer.TransformForFanoutAt(window[off:end], arrival)
		if err != nil {
			t.Fatalf("transform failed at byte offset %d relative to loss: %v", off-400*1024, err)
		}
		if len(media) > 0 {
			if err := pacer.Wait(context.Background(), media, arrival); err != nil {
				t.Fatalf("pacer failed at byte offset %d relative to loss: %v", off-400*1024, err)
			}
			emitted += len(media)
		}
		// The capture's delivery ran ~4.2 Mbps; advance the virtual clock at
		// roughly that rate so arrival and media clocks stay commensurate
		// across the ~6 s hole the loss spans.
		wall = wall.Add(125 * time.Millisecond)
	}
	if delta := metrics.RelayTransportResyncs.Value() - beforeResyncs; delta < 1 {
		t.Fatalf("resync metric delta=%d, want >=1 (the capture contains one real loss)", delta)
	}
	if emitted == 0 {
		t.Fatal("no media emitted across the loss window")
	}
	t.Logf("survived the real sync loss: %d bytes emitted across the window, resyncs=%d",
		emitted, metrics.RelayTransportResyncs.Value()-beforeResyncs)
}
