// transcode_streamer_test.go — S6 resilience tests for the transcode pump,
// using shell stubs in place of ffmpeg (`cat` = passthrough transcoder,
// `head -c N` = a transcoder that dies after N bytes). No real ffmpeg or
// GPU required.
package stream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/metrics"
	"github.com/spencercnorton/conductor/internal/store"
	"github.com/spencercnorton/conductor/internal/transcode"
)

func TestSourceFailureAttributionExcludesLocalAndUncertainTranscodeFaults(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "upstream", err: errors.New("provider connection reset"), want: true},
		{name: "local ffmpeg", err: localTranscodeFailure(errors.New("ffmpeg exited")), want: false},
		{name: "wrapped local ffmpeg", err: fmt.Errorf("reconnect callback: %w", localTranscodeFailure(errors.New("ffmpeg exited"))), want: false},
		{name: "uncertain input", err: fmt.Errorf("startup: %w", errUncertainTranscodeInput), want: false},
		{name: "classifier capacity", err: fmt.Errorf("startup: %w", errClassifierCapacity), want: false},
		{name: "classifier execution timeout", err: fmt.Errorf("startup: %w", errClassifierTimeout), want: false},
		{name: "uncertain transcode prefix", err: errors.Join(errUncertainMediaPrefix, errMediaPrefixNotReady), want: false},
		{name: "relay read-ahead full", err: errRelayReadAheadFull, want: false},
		{name: "stale relay chunk", err: errTransportStaleChunk, want: false},
		{name: "buffered relay backlog", err: errTransportRelayBacklog, want: false},
		{name: "transport clock boundary", err: errTransportAttemptBoundary, want: false},
		{name: "hung ffmpeg stdout", err: localTranscodeFailure(errTranscodeOutputIdle), want: false},
		{name: "unattributed recovery backlog", err: localTranscodeFailure(errTransportRecoveryBacklog), want: false},
		{name: "relocation state unknown", err: store.ErrRelocationStateUnknown, want: false},
		{name: "relocation operational terminal", err: errors.Join(store.ErrLeaseOperational, store.ErrRelocationStateUnknown), want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldMarkSourceFailure(tt.err); got != tt.want {
				t.Fatalf("shouldMarkSourceFailure(%v)=%v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestCheckedTranscodeInputWithholdsEverySplitOfUnsafeTransport(t *testing.T) {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
		audioPID = 0x0101
	)
	base := time.Unix(1_700_000_090, 0)
	prefix, _, _, audioCC := healthyAVTranscodePrefix(
		pmtPID, videoPID, audioPID, minPrefixDecisionBytes+2*tsPacketSize)
	bad := testTimestampedPESPacket(audioPID, 0xc0,
		4*60*60*transportPTSRate, audioCC)

	for split := 1; split < len(bad); split++ {
		checked := newCheckedTranscodeInput()
		initial, err := checked.push(prefix, base)
		if err != nil || len(initial) != len(prefix) {
			t.Fatalf("split %d initial checked bytes=%d err=%v, want %d",
				split, len(initial), err, len(prefix))
		}
		first, err := checked.push(bad[:split], base.Add(time.Second))
		if err != nil || len(first) != 0 {
			t.Fatalf("split %d wrote %d partial bad bytes before completion (err=%v)",
				split, len(first), err)
		}
		second, err := checked.push(bad[split:], base.Add(time.Second))
		var continuityErr *avContinuityError
		if len(second) != 0 || !errors.As(err, &continuityErr) ||
			continuityErr.kind != avFaultTransport {
			t.Fatalf("split %d completion=(bytes=%d err=%v), want unpublished transport fault",
				split, len(second), err)
		}
	}

	t.Run("PES optional header split across TS packets", func(t *testing.T) {
		checked := newCheckedTranscodeInput()
		if _, err := checked.push(prefix, base); err != nil {
			t.Fatal(err)
		}
		pes := slateTestPES(0xc0, transportPTSRate, 0, false)
		pes[9] = pes[9]&0x0f | 0x30 // invalid PTS prefix, known only after byte 9
		firstPacket := testExactPayloadPacket(audioPID, true, audioCC, pes[:8])
		secondPacket := testExactPayloadPacket(audioPID, false, (audioCC+1)&0x0f, pes[8:])
		if output, err := checked.push(firstPacket, base.Add(time.Second)); err != nil || len(output) != 0 {
			t.Fatalf("incomplete selected PES wrote %d bytes before header completion (err=%v)", len(output), err)
		}
		if output, err := checked.push(secondPacket, base.Add(time.Second)); err == nil || len(output) != 0 {
			t.Fatalf("malformed split PES completion=(bytes=%d err=%v), want withheld fault", len(output), err)
		}
	})

	for _, tc := range []struct {
		name   string
		mutate func([]byte)
	}{
		{name: "invalid PES start code", mutate: func(payload []byte) { payload[2] = 0x02 }},
		{name: "unsupported MPEG-1 layout", mutate: func(payload []byte) { payload[6] = 0x40 }},
		{name: "contradictory PES packet length", mutate: func(payload []byte) {
			payload[4], payload[5] = 0, 1
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			checked := newCheckedTranscodeInput()
			if _, err := checked.push(prefix, base); err != nil {
				t.Fatal(err)
			}
			bad := testTimestampedPESPacket(audioPID, 0xc0, transportPTSRate, audioCC)
			payload, _ := tsPayload(bad)
			tc.mutate(payload)
			out, err := checked.push(bad, base.Add(time.Second))
			var continuityErr *avContinuityError
			if len(out) != 0 || !errors.As(err, &continuityErr) ||
				continuityErr.kind != avFaultTransport {
				t.Fatalf("malformed selected PES=(bytes=%d err=%v), want zero bytes before FFmpeg stdin",
					len(out), err)
			}
		})
	}
}

func TestCheckedTranscodeInputRequiresExactProgramBeforeStdin(t *testing.T) {
	const (
		pmtPID     = 0x1000
		videoPID   = 0x0100
		audioPID   = 0x0101
		otherPMT   = 0x1001
		otherVideo = 0x0200
	)
	base := time.Unix(1_700_000_091, 0)
	pmtPacket := testAVPMTPacket(pmtPID, videoPID, audioPID)
	payload, ok := tsPayload(pmtPacket)
	if !ok {
		t.Fatal("PMT fixture has no payload")
	}
	pmtSection := append([]byte(nil), payload[1+int(payload[0]):]...)
	pmtSection = pmtSection[:3+(int(pmtSection[1]&0x0f)<<8|int(pmtSection[2]))]
	splitPMT := testPSISectionPackets(pmtPID, 0, bytes.Repeat([]byte{0}, 170), pmtSection)
	if len(splitPMT) != 2 {
		t.Fatalf("split PMT packets=%d, want 2", len(splitPMT))
	}
	badAudio := testTimestampedPESPacket(audioPID, 0xc0, transportPTSRate, 0)
	badPayload, _ := tsPayload(badAudio)
	badPayload[9] = badPayload[9]&0x0f | 0x30
	fixture := joinTSPackets(
		testPATPacket(pmtPID), splitPMT[0], splitPMT[1],
		testTimestampedPESPacket(videoPID, 0xe0, 0, 0), badAudio,
		pcrTestNonPCRPacket(0x1fff, false))
	checked := newCheckedTranscodeInput()
	out, err := checked.push(fixture, base)
	var continuityErr *avContinuityError
	if len(out) != 0 || !errors.As(err, &continuityErr) ||
		continuityErr.kind != avFaultTransport {
		t.Fatalf("split-map corrupt input=(bytes=%d err=%v), want unpublished transport fault", len(out), err)
	}

	t.Run("MPTS is rejected rather than guessed", func(t *testing.T) {
		checked := newCheckedTranscodeInput()
		mpts := joinTSPackets(
			testPATPacketForTable(1, 0, true, 0, 0,
				testPATProgram{programNumber: 1, pmtPID: pmtPID},
				testPATProgram{programNumber: 2, pmtPID: otherPMT}),
			testPMTPacketForProgram(1, pmtPID, videoPID, 0x1b),
			testPMTPacketForProgram(2, otherPMT, otherVideo, 0x1b),
			pcrTestNonPCRPacket(0x1fff, false),
			pcrTestNonPCRPacket(0x1ffe, false))
		out, err := checked.push(mpts, base)
		if len(out) != 0 || !errors.Is(err, errUncertainTranscodeInput) {
			t.Fatalf("MPTS input=(bytes=%d err=%v), want private unsupported-selection rejection", len(out), err)
		}
	})

	t.Run("selected audio before its PMT is dropped and cannot establish readiness", func(t *testing.T) {
		prePMTAudio := testPayloadOnlyTimestampedPESPacket(
			audioPID, 0xc0, 123*transportPTSRate, 3)
		checked := newCheckedTranscodeInput()
		out, err := checked.push(joinTSPackets(
			testPATPacket(pmtPID),
			prePMTAudio,
			testAVPMTPacket(pmtPID, videoPID, audioPID),
			testTimestampedPESPacket(videoPID, 0xe0, 0, 0),
			pcrTestNonPCRPacket(0x1fff, false),
		), base)
		if err != nil || len(out) != 0 || !checked.programBound {
			t.Fatalf("pre-PMT audio startup=(bytes=%d err=%v bound=%v), want private post-map establishment",
				len(out), err, checked.programBound)
		}
		if bytes.Contains(checked.pending, prePMTAudio) {
			t.Fatal("checked input retained audio consumed before FFmpeg could learn its PMT mapping")
		}
		postPMTAudio := testPayloadOnlyTimestampedPESPacket(audioPID, 0xc0, 0, 4)
		out, err = checked.push(postPMTAudio, base.Add(time.Millisecond))
		if err != nil || len(out) == 0 || bytes.Contains(out, prePMTAudio) ||
			!bytes.Contains(out, postPMTAudio) {
			t.Fatalf("post-PMT audio release=(bytes=%d err=%v pre=%v post=%v)",
				len(out), err, bytes.Contains(out, prePMTAudio), bytes.Contains(out, postPMTAudio))
		}
	})
}

func TestCheckedTranscodeInputBoundsIncompleteRuntimeProgramMap(t *testing.T) {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
		audioPID = 0x0101
	)
	base := time.Unix(1_700_000_121, 0)
	prefix, _, _, _ := healthyAVTranscodePrefix(
		pmtPID, videoPID, audioPID, minPrefixDecisionBytes+2*tsPacketSize)
	checked := newCheckedTranscodeInput()
	if out, err := checked.push(prefix, base); err != nil || len(out) != len(prefix) {
		t.Fatalf("safe startup=(bytes=%d err=%v), want %d", len(out), err, len(prefix))
	}
	incomplete := testExactPayloadPacket(pmtPID, true, 1,
		[]byte{0, 0x02, 0xb3, 0xfd, 0, 1, 0xc1, 0, 0})
	if out, err := checked.push(incomplete, base.Add(time.Second)); err != nil || len(out) != 0 {
		t.Fatalf("incomplete runtime PMT=(bytes=%d err=%v), want private", len(out), err)
	}
	out, err := checked.push(pcrTestNonPCRPacket(0x1ffe, false),
		base.Add(time.Second+maxPublicationAVGateHold+time.Millisecond))
	if len(out) != 0 || !errors.Is(err, errTransportProgramMapStall) {
		t.Fatalf("expired checked-input map=(bytes=%d err=%v), want no stdin bytes", len(out), err)
	}
	if isLocalRelayRetry(err) || !shouldMarkSourceFailure(err) {
		t.Fatalf("checked-input map stall attribution local=%v source=%v",
			isLocalRelayRetry(err), shouldMarkSourceFailure(err))
	}

	t.Run("high-rate byte cap remains provider map fault", func(t *testing.T) {
		checked := newCheckedTranscodeInput()
		if out, err := checked.push(prefix, base); err != nil || len(out) != len(prefix) {
			t.Fatalf("safe startup=(bytes=%d err=%v), want %d", len(out), err, len(prefix))
		}
		incomplete := testExactPayloadPacket(pmtPID, true, 1,
			[]byte{0, 0x02, 0xb3, 0xfd, 0, 1, 0xc1, 0, 0})
		if out, err := checked.push(incomplete, base.Add(time.Second)); err != nil || len(out) != 0 {
			t.Fatalf("incomplete runtime PMT=(bytes=%d err=%v), want private", len(out), err)
		}
		nullPacket := pcrTestNonPCRPacket(0x1ffe, false)
		count := maxCheckedTranscodeInputPending/tsPacketSize + 2
		burst := make([]byte, 0, count*tsPacketSize)
		for i := 0; i < count; i++ {
			burst = append(burst, nullPacket...)
		}
		out, err := checked.push(burst, base.Add(time.Second+100*time.Millisecond))
		if len(out) != 0 || !errors.Is(err, errTransportProgramMapStall) ||
			isLocalRelayRetry(err) || !shouldMarkSourceFailure(err) {
			t.Fatalf("checked-input map byte-cap=(bytes=%d err=%v local=%v source=%v)",
				len(out), err, isLocalRelayRetry(err), shouldMarkSourceFailure(err))
		}
	})
}

func TestTranscodeRuntimeInputProgramMapStallRelocatesProviderWithoutStdinLeak(t *testing.T) {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
		audioPID = 0x0101
	)
	prefix, _, _, _ := healthyAVTranscodePrefix(
		pmtPID, videoPID, audioPID, minPrefixDecisionBytes+2*tsPacketSize)
	releaseMap := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		if _, err := w.Write(prefix); err != nil {
			return
		}
		if flusher != nil {
			flusher.Flush()
		}
		select {
		case <-releaseMap:
		case <-r.Context().Done():
			return
		}
		incomplete := testExactPayloadPacket(pmtPID, true, 1,
			[]byte{0, 0x02, 0xb3, 0xfd, 0, 1, 0xc1, 0, 0})
		if _, err := w.Write(incomplete); err != nil {
			return
		}
		if flusher != nil {
			flusher.Flush()
		}
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case <-ticker.C:
				if _, err := w.Write(pcrTestNonPCRPacket(0x1ffe, false)); err != nil {
					return
				}
				if flusher != nil {
					flusher.Flush()
				}
			}
		}
	}))
	defer srv.Close()

	s := NewTranscodeStreamer("runtime-input-map-stall", srv.URL, stubProfile(),
		stubBinary(t, "exec cat"), testLogger(), nil)
	s.prefixValidator = &thresholdPrefixValidator{readyAt: minPrefixDecisionBytes}
	s.sourceStartupTimeout = 3 * time.Second
	s.idleReadTimeout = 3 * time.Second
	s.outputProgressTimeout = 3 * time.Second
	defer drainTranscodeTestSubscriber(s, "runtime-input-map-stall-viewer")()

	var (
		mapReleased   atomic.Bool
		postMapWrites atomic.Int32
		releaseOnce   sync.Once
		delivered     atomic.Int64
	)
	s.inputPumpHooks = &transcodeInputPumpTestHooks{onWriteStart: func() {
		if mapReleased.Load() {
			postMapWrites.Add(1)
		}
	}}
	s.OnBytes = func(n int) error {
		if delivered.Add(int64(n)) >= int64(len(prefix)) {
			releaseOnce.Do(func() {
				mapReleased.Store(true)
				close(releaseMap)
			})
		}
		return nil
	}

	gotBytes, upstreamFault, err := s.runOnce(context.Background())
	if !gotBytes || !upstreamFault || !errors.Is(err, errTransportProgramMapStall) {
		t.Fatalf("runtime input map stall=(bytes=%v upstream=%v err=%v), want provider relocation",
			gotBytes, upstreamFault, err)
	}
	if !shouldMarkSourceFailure(err) || isLocalRelayRetry(err) {
		t.Fatalf("runtime input map attribution source=%v local=%v err=%v",
			shouldMarkSourceFailure(err), isLocalRelayRetry(err), err)
	}
	if writes := postMapWrites.Load(); writes != 0 {
		t.Fatalf("checked input issued %d FFmpeg stdin writes after the incomplete PMT became pending", writes)
	}
}

func TestCheckedTranscodeInputProgramDiscoveryWorkIsGeometricallyBounded(t *testing.T) {
	const (
		pmtPID       = 0x1000
		videoPID     = 0x0100
		audioPID     = 0x0101
		discoveryAt  = 4096
		totalPackets = 5120
	)
	base := time.Unix(1_700_000_095, 0)
	checked := newCheckedTranscodeInput()
	var released []byte
	boundAt := -1
	for index := 0; index < totalPackets; index++ {
		var packet []byte
		switch index {
		case discoveryAt:
			packet = testAVPMTPacket(pmtPID, videoPID, audioPID)
		case discoveryAt + 1:
			packet = testTimestampedPESPacket(videoPID, 0xe0, 0, 0)
		case discoveryAt + 2:
			packet = testTimestampedPESPacket(audioPID, 0xc0, 0, 0)
		default:
			if index < discoveryAt {
				packet = testPATPacket(pmtPID)
				packet[3] = packet[3]&0xf0 | byte(index&0x0f)
			} else {
				packet = pcrTestNonPCRPacket(0x1fff, false)
			}
		}
		out, err := checked.push(packet, base.Add(time.Duration(index)*time.Microsecond))
		if err != nil {
			t.Fatalf("packet %d discovery error: %v", index, err)
		}
		released = append(released, out...)
		if checked.programBound && boundAt < 0 {
			boundAt = index
		}
	}
	wantBytes := totalPackets * tsPacketSize
	if len(released) != wantBytes || !checked.programBound {
		t.Fatalf("delayed map release=(bytes=%d bound=%v), want %d bytes", len(released), checked.programBound, wantBytes)
	}
	if checked.programScans > 32 {
		t.Fatalf("tiny-read program scans=%d, want bounded discovery", checked.programScans)
	}
	if checked.programScanBytes > 32*wantBytes {
		t.Fatalf("tiny-read discovery rescanned %d bytes for %d-byte prefix", checked.programScanBytes, wantBytes)
	}
	if boundAt < discoveryAt ||
		(boundAt-discoveryAt)*tsPacketSize > maxCheckedInputProgramScanSpacing+3*tsPacketSize {
		t.Fatalf("delayed PMT bound at packet %d after arrival %d (%d bytes), max spacing %d",
			boundAt, discoveryAt, (boundAt-discoveryAt)*tsPacketSize, maxCheckedInputProgramScanSpacing)
	}
}

func TestCheckedTranscodeInputAcceptsSparseVideoOnlyLivePrefix(t *testing.T) {
	chunks := pcrSparseTestChunks(delayedLivePrefixChunkCount, 25*time.Millisecond)
	checked := newCheckedTranscodeInput()
	var released int
	for index, chunk := range chunks {
		out, err := checked.push(chunk, time.Unix(1_700_000_098, int64(index)))
		if err != nil {
			t.Fatalf("sparse chunk %d: %v", index, err)
		}
		released += len(out)
	}
	rawBytes := delayedLivePrefixChunkCount * chunkSize
	tailOffset := rawBytes % tsPacketSize
	out, err := checked.push(pcrTestNonPCRPacket(0x1ffe, false)[tailOffset:], time.Unix(1_700_000_099, 0))
	if err != nil {
		t.Fatal(err)
	}
	released += len(out)
	if !checked.programBound || released == 0 {
		t.Fatalf("sparse video-only input=(bound=%v released=%d), want FFmpeg-ready data", checked.programBound, released)
	}
}

func TestCheckedTranscodeInputReestablishesAVPrivatelyAfterBoundary(t *testing.T) {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
		audioPID = 0x0101
	)
	base := time.Unix(1_700_000_092, 0)
	prefix, nextPTS, videoCC, audioCC := healthyAVTranscodePrefix(
		pmtPID, videoPID, audioPID, minPrefixDecisionBytes+2*tsPacketSize)
	checked := newCheckedTranscodeInput()
	if out, err := checked.push(prefix, base); err != nil || len(out) != len(prefix) {
		t.Fatalf("initial checked prefix=(bytes=%d err=%v), want %d", len(out), err, len(prefix))
	}
	boundary := testAdaptationOnlyDiscontinuity(audioPID, audioCC)
	if out, err := checked.push(boundary, base.Add(time.Second)); err != nil || len(out) != 0 {
		t.Fatalf("selected boundary=(bytes=%d err=%v), want privately held", len(out), err)
	}
	grossAudio := testTimestampedPESPacket(audioPID, 0xc0,
		nextPTS+9*60*transportPTSRate, (audioCC+1)&0x0f)
	if out, err := checked.push(grossAudio, base.Add(time.Second)); err != nil || len(out) != 0 {
		t.Fatalf("lone gross audio=(bytes=%d err=%v), want privately held", len(out), err)
	}
	out, err := checked.push(testTimestampedPESPacket(
		videoPID, 0xe0, nextPTS, videoCC), base.Add(time.Second))
	var continuityErr *avContinuityError
	if len(out) != 0 || !errors.As(err, &continuityErr) ||
		continuityErr.kind != avFaultTimelineSkew {
		t.Fatalf("post-boundary re-establishment=(bytes=%d err=%v), want unpublished gross skew", len(out), err)
	}

	t.Run("ordinary provider marker opens a shared epoch", func(t *testing.T) {
		prefix, nextPTS, videoCC, audioCC := healthyAVTranscodePrefix(
			pmtPID, videoPID, audioPID, minPrefixDecisionBytes+2*tsPacketSize)
		checked := newCheckedTranscodeInput()
		if out, err := checked.push(prefix, base); err != nil || len(out) != len(prefix) {
			t.Fatalf("initial checked prefix=(bytes=%d err=%v), want %d", len(out), err, len(prefix))
		}
		boundary := testAdaptationOnlyDiscontinuity(audioPID, audioCC)
		if out, err := checked.push(boundary, base.Add(time.Second)); err != nil || len(out) != 0 {
			t.Fatalf("selected boundary=(bytes=%d err=%v), want private shared reset", len(out), err)
		}
		audio := testTimestampedPESPacket(audioPID, 0xc0,
			nextPTS+5*transportPTSRate, (audioCC+1)&0x0f)
		if out, err := checked.push(audio, base.Add(time.Second)); err != nil || len(out) != 0 {
			t.Fatalf("new-epoch audio=(bytes=%d err=%v), want held for video", len(out), err)
		}
		video := testTimestampedPESPacket(videoPID, 0xe0,
			nextPTS+5*transportPTSRate, videoCC)
		out, err := checked.push(video, base.Add(time.Second))
		want := joinTSPackets(boundary, audio, video)
		if err != nil || !bytes.Equal(out, want) {
			t.Fatalf("aligned new epoch=(bytes=%d err=%v), want atomic %d-byte release", len(out), err, len(want))
		}
	})
}

func collectTranscodeSubscriber(t *testing.T, ch <-chan []byte) []byte {
	t.Helper()
	var received []byte
	idle := time.NewTimer(40 * time.Millisecond)
	defer idle.Stop()
	for {
		select {
		case chunk := <-ch:
			received = append(received, chunk...)
			if !idle.Stop() {
				select {
				case <-idle.C:
				default:
				}
			}
			idle.Reset(40 * time.Millisecond)
		case <-idle.C:
			return received
		}
	}
}

func TestTranscodeInputTransportFaultNeverReachesFFmpegOrFanout(t *testing.T) {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
		audioPID = 0x0101
	)
	prefix, _, _, audioCC := healthyAVTranscodePrefix(
		pmtPID, videoPID, audioPID, minPrefixDecisionBytes+2*tsPacketSize)
	badPTS := uint64(4 * 60 * 60 * transportPTSRate)
	badThenSane := joinTSPackets(
		testTimestampedPESPacket(audioPID, 0xc0, badPTS, audioCC),
		testTimestampedPESPacket(audioPID, 0xc0, transportPTSRate, (audioCC+1)&0x0f))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		flusher, _ := w.(http.Flusher)
		_, _ = w.Write(prefix)
		if flusher != nil {
			flusher.Flush()
		}
		select {
		case <-r.Context().Done():
			return
		case <-time.After(500 * time.Millisecond):
		}
		_, _ = w.Write(badThenSane)
		if flusher != nil {
			flusher.Flush()
		}
		<-r.Context().Done()
	}))
	defer srv.Close()

	s := NewTranscodeStreamer("input-transport-safety", srv.URL, stubProfile(),
		stubBinary(t, "exec cat"), testLogger(), nil)
	s.prefixValidator = &thresholdPrefixValidator{
		readyAt: minPrefixDecisionBytes, videoPID: videoPID, audioCount: 1,
		pcrPID: videoPID, havePCRPID: true, haveProgram: true,
	}
	s.prefixValidator.(*thresholdPrefixValidator).audioPIDs[0] = audioPID
	s.sourceStartupTimeout = 2 * time.Second
	viewer, unsubscribe := s.Subscribe("plex-input-safety")
	defer unsubscribe()
	gotBytes, upstreamFault, err := s.runOnce(context.Background())
	var continuityErr *avContinuityError
	if !gotBytes || !upstreamFault || !errors.As(err, &continuityErr) ||
		continuityErr.kind != avFaultTransport {
		t.Fatalf("input safety=(bytes=%v upstream=%v err=%v), want provider transport relocation",
			gotBytes, upstreamFault, err)
	}
	received := collectTranscodeSubscriber(t, viewer)
	if testStreamContainsPTS(received, audioPID, badPTS) {
		t.Fatal("unsafe input PTS crossed FFmpeg stdin and reached fanout")
	}
	if !shouldMarkSourceFailure(err) {
		t.Fatalf("proven input transport damage would not decay provider health: %v", err)
	}
}

func TestTranscodeOutputTransportFaultIsWithheldAndRetriedLocally(t *testing.T) {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
		audioPID = 0x0101
	)
	prefix, _, _, audioCC := healthyAVTranscodePrefix(
		pmtPID, videoPID, audioPID, minPrefixDecisionBytes+2*tsPacketSize)
	badPTS := uint64(4 * 60 * 60 * transportPTSRate)
	badThenSane := joinTSPackets(
		testTimestampedPESPacket(audioPID, 0xc0, badPTS, audioCC),
		testTimestampedPESPacket(audioPID, 0xc0, transportPTSRate, (audioCC+1)&0x0f))
	fixture := append(append([]byte(nil), prefix...), badThenSane...)
	fixturePath := filepath.Join(t.TempDir(), "output.ts")
	if err := os.WriteFile(fixturePath, fixture, 0o600); err != nil {
		t.Fatal(err)
	}
	quotedPath := strings.ReplaceAll(fixturePath, "'", "'\"'\"'")
	script := fmt.Sprintf(
		`exec python3 -c 'import sys,time; d=open(sys.argv[1],"rb").read(); n=int(sys.argv[2]); sys.stdout.buffer.write(d[:n]); sys.stdout.buffer.flush(); time.sleep(.1); sys.stdout.buffer.write(d[n:]); sys.stdout.buffer.flush(); sys.stdin.buffer.read()' '%s' '%d'`,
		quotedPath, len(prefix))

	input, _, _, _ := healthyAVTranscodeInputServer(t)
	s := NewTranscodeStreamer("output-transport-safety", input.URL, stubProfile(),
		stubBinary(t, script), testLogger(), nil)
	validator := &thresholdPrefixValidator{
		readyAt: minPrefixDecisionBytes, videoPID: videoPID, audioCount: 1,
		pcrPID: videoPID, havePCRPID: true, haveProgram: true,
	}
	for i := range validator.audioPIDs {
		validator.audioPIDs[i] = -1
	}
	validator.audioPIDs[0] = audioPID
	s.prefixValidator = validator
	s.sourceStartupTimeout = 2 * time.Second
	viewer, unsubscribe := s.Subscribe("plex-output-safety")
	defer unsubscribe()
	gotBytes, upstreamFault, err := s.runOnce(context.Background())
	var continuityErr *avContinuityError
	if !gotBytes || upstreamFault || !errors.As(err, &continuityErr) ||
		continuityErr.kind != avFaultTransport || !errors.Is(err, errLocalTranscodeFailure) {
		t.Fatalf("output safety=(bytes=%v upstream=%v err=%v), want local transport restart",
			gotBytes, upstreamFault, err)
	}
	received := collectTranscodeSubscriber(t, viewer)
	if testStreamContainsPTS(received, audioPID, badPTS) {
		t.Fatal("unsafe FFmpeg output PTS reached fanout")
	}
	if shouldMarkSourceFailure(err) {
		t.Fatalf("output-only transport damage would decay provider health: %v", err)
	}
}

type transcodeInputRequestEvent struct {
	number int32
	at     time.Time
}

func healthyAVTranscodePrefix(
	pmtPID, videoPID, audioPID, minimumBytes int,
) (prefix []byte, nextPTS uint64, nextVideoCC, nextAudioCC byte) {
	packets := [][]byte{testPATPacket(pmtPID), testAVPMTPacket(pmtPID, videoPID, audioPID)}
	for len(packets)*tsPacketSize < minimumBytes {
		packets = append(packets,
			testTimestampedPESPacket(videoPID, 0xe0, nextPTS, nextVideoCC),
			testTimestampedPESPacket(audioPID, 0xc0, nextPTS, nextAudioCC))
		nextVideoCC, nextAudioCC = (nextVideoCC+1)&0x0f, (nextAudioCC+1)&0x0f
		nextPTS += transportPTSRate / 50
	}
	return joinTSPackets(packets...), nextPTS, nextVideoCC, nextAudioCC
}

func burstyAVPCRTestChunks(count int, step time.Duration) [][]byte {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
		audioPID = 0x0101
	)
	totalBytes := count * chunkSize
	transportBytes := ((totalBytes + tsPacketSize - 1) / tsPacketSize) * tsPacketSize
	stream := make([]byte, transportBytes)
	nullPacket := pcrTestNonPCRPacket(0x1ffe, false)
	for offset := 0; offset < len(stream); offset += tsPacketSize {
		copy(stream[offset:offset+tsPacketSize], nullPacket)
	}
	copy(stream[:tsPacketSize], testPATPacket(pmtPID))
	copy(stream[tsPacketSize:2*tsPacketSize], testAVPMTPacket(pmtPID, videoPID, audioPID))
	for i := 0; i < count; i++ {
		chunkStart := i * chunkSize
		packetOffset := ((chunkStart + tsPacketSize - 1) / tsPacketSize) * tsPacketSize
		if packetOffset < 2*tsPacketSize {
			packetOffset = 2 * tsPacketSize
		}
		pcrPacket := bytes.Repeat([]byte{0xff}, tsPacketSize)
		pcrPacket[0] = 0x47
		pcrPacket[1] = byte(videoPID>>8) & 0x1f
		pcrPacket[2] = byte(videoPID & 0xff)
		pcrCC := byte(0)
		if i > 0 {
			pcrCC = byte((i - 1) & 0x0f)
		}
		pcrPacket[3] = 0x20 | pcrCC // adaptation only: retains prior payload continuity
		pcrPacket[4] = tsPacketSize - 5
		pcrPacket[5] = 0x10
		copy(pcrPacket[6:12], slateTestPCR(durationTicks(time.Duration(i)*step, transportClockRate)))
		pts := durationTicks(time.Duration(i)*step, transportPTSRate)
		copy(stream[packetOffset:packetOffset+tsPacketSize], pcrPacket)
		copy(stream[packetOffset+tsPacketSize:packetOffset+2*tsPacketSize],
			testTimestampedPESPacket(videoPID, 0xe0, pts, byte(i&0x0f)))
		copy(stream[packetOffset+2*tsPacketSize:packetOffset+3*tsPacketSize],
			testTimestampedPESPacket(audioPID, 0xc0, pts, byte(i&0x0f)))
	}
	chunks := make([][]byte, count)
	for i := range chunks {
		chunks[i] = stream[i*chunkSize : (i+1)*chunkSize]
	}
	return chunks
}

func healthyAVTranscodeInputServer(t *testing.T) (
	*httptest.Server,
	*transportAVTimeline,
	*atomic.Int32,
	<-chan transcodeInputRequestEvent,
) {
	t.Helper()
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
		audioPID = 0x0101
	)
	timeline := &transportAVTimeline{}
	postPrefixWrites := &atomic.Int32{}
	requestStarted := make(chan transcodeInputRequestEvent, 8)
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := requests.Add(1)
		select {
		case requestStarted <- transcodeInputRequestEvent{number: request, at: time.Now()}:
		default:
		}
		w.Header().Set("Content-Type", "video/mp2t")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)

		prefix, pts, videoCC, audioCC := healthyAVTranscodePrefix(
			pmtPID, videoPID, audioPID, minPrefixDecisionBytes+2*tsPacketSize)
		if request == 1 {
			timeline.feedAt(prefix, time.Now())
		}
		if _, err := w.Write(prefix); err != nil {
			return
		}
		if flusher != nil {
			flusher.Flush()
		}
		// Push more than a typical local stdin pipe immediately after FFmpeg's
		// one published prefix. A fixture whose child stops reading stdin will
		// therefore enter a real blocked Write rather than merely waiting for
		// enough low-rate test bytes to fill the pipe.
		burstPackets := make([][]byte, 0, 2*chunkSize/tsPacketSize+2)
		for len(burstPackets)*tsPacketSize < 2*chunkSize {
			burstPackets = append(burstPackets,
				testTimestampedPESPacket(videoPID, 0xe0, pts, videoCC),
				testTimestampedPESPacket(audioPID, 0xc0, pts, audioCC))
			videoCC, audioCC = (videoCC+1)&0x0f, (audioCC+1)&0x0f
			pts += transportPTSRate / 50
		}
		burst := joinTSPackets(burstPackets...)
		if request == 1 {
			timeline.feedAt(burst, time.Now())
			postPrefixWrites.Add(1)
		}
		if _, err := w.Write(burst); err != nil {
			return
		}
		if flusher != nil {
			flusher.Flush()
		}

		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case now := <-ticker.C:
				chunk := joinTSPackets(
					testTimestampedPESPacket(videoPID, 0xe0, pts, videoCC),
					testTimestampedPESPacket(audioPID, 0xc0, pts, audioCC))
				videoCC, audioCC = (videoCC+1)&0x0f, (audioCC+1)&0x0f
				pts += transportPTSRate / 50
				if request == 1 {
					timeline.feedAt(chunk, now)
					postPrefixWrites.Add(1)
				}
				if _, err := w.Write(chunk); err != nil {
					return
				}
				if flusher != nil {
					flusher.Flush()
				}
			}
		}
	}))
	t.Cleanup(srv.Close)
	return srv, timeline, postPrefixWrites, requestStarted
}

func hungStdoutTranscoder(t *testing.T) string {
	t.Helper()
	return stubBinary(t,
		`exec python3 -c 'import sys; d=sys.stdin.buffer.read(65536); sys.stdout.buffer.write(d); sys.stdout.buffer.flush(); sys.stdin.buffer.read()'`)
}

func blockedStdinAndStdoutTranscoder(t *testing.T) string {
	t.Helper()
	return stubBinary(t,
		`exec python3 -c 'import sys,time; d=sys.stdin.buffer.read(65536); sys.stdout.buffer.write(d); sys.stdout.buffer.flush(); time.sleep(3600)'`)
}

func drainTranscodeTestSubscriber(s *TranscodeStreamer, id string) func() {
	viewer, unsubscribe := s.Subscribe(id)
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-viewer:
			case <-stop:
				return
			}
		}
	}()
	return func() {
		close(stop)
		unsubscribe()
	}
}

func configureHungOutputTestStreamer(t *testing.T, upstream string) *TranscodeStreamer {
	t.Helper()
	s := NewTranscodeStreamer("healthy-input-hung-output", upstream, stubProfile(),
		hungStdoutTranscoder(t), testLogger(), nil)
	s.prefixValidator = &thresholdPrefixValidator{readyAt: minPrefixDecisionBytes}
	s.sourceStartupTimeout = 2 * time.Second
	s.idleReadTimeout = 2 * time.Second
	s.outputProgressTimeout = 350 * time.Millisecond
	return s
}

func TestTranscodeOutputProgressWatchdogClassifiesHungStdoutLocal(t *testing.T) {
	srv, inputTimeline, postPrefixWrites, _ := healthyAVTranscodeInputServer(t)
	s := configureHungOutputTestStreamer(t, srv.URL)
	defer drainTranscodeTestSubscriber(s, "hung-output-attribution")()
	beforeStalls := metrics.TranscodeOutputStalls.Value()
	started := time.Now()
	gotBytes, upstreamFault, err := s.runOnce(context.Background())
	if !gotBytes {
		t.Fatalf("hung stdout never established validated output: %v", err)
	}
	if upstreamFault {
		t.Fatalf("healthy live input + hung stdout requested provider relocation: %v", err)
	}
	if !errors.Is(err, errTranscodeOutputIdle) || !errors.Is(err, errLocalTranscodeFailure) {
		t.Fatalf("hung stdout error=%v, want typed local output-progress failure", err)
	}
	if shouldMarkSourceFailure(err) {
		t.Fatalf("hung local stdout would decay provider health: %v", err)
	}
	if elapsed := time.Since(started); elapsed > 1500*time.Millisecond {
		t.Fatalf("hung stdout took %s, want bounded local watchdog", elapsed)
	}
	if writes := postPrefixWrites.Load(); writes < 10 {
		t.Fatalf("provider made only %d post-prefix writes; input liveness not proven", writes)
	}
	now := time.Now()
	if snapshot := inputTimeline.snapshotAt(now); !snapshot.valid || snapshot.drift != 0 ||
		snapshot.finalSkew != 0 {
		t.Fatalf("provider input was not independently A/V-healthy: %+v", snapshot)
	}
	if fault := inputTimeline.continuityFault(now, defaultAVContinuityPolicy()); fault != nil {
		t.Fatalf("provider input developed a continuity fault while stdout hung: %v", fault)
	}
	if delta := metrics.TranscodeOutputStalls.Value() - beforeStalls; delta != 1 {
		t.Fatalf("transcode output-stall metric delta=%d, want 1", delta)
	}
}

func TestTranscodeBlockedStdinAndStdoutRemainsLocalAndBounded(t *testing.T) {
	srv, _, postPrefixWrites, _ := healthyAVTranscodeInputServer(t)
	s := NewTranscodeStreamer("blocked-local-ffmpeg", srv.URL, stubProfile(),
		blockedStdinAndStdoutTranscoder(t), testLogger(), nil)
	s.prefixValidator = &thresholdPrefixValidator{readyAt: minPrefixDecisionBytes}
	s.sourceStartupTimeout = 2 * time.Second
	// If the provider watchdog leaked across Stdin.Write this shorter deadline
	// would win and falsely request provider relocation before stdout's timer.
	s.idleReadTimeout = 200 * time.Millisecond
	s.outputProgressTimeout = 450 * time.Millisecond
	defer drainTranscodeTestSubscriber(s, "blocked-stdin-attribution")()

	started := time.Now()
	gotBytes, upstreamFault, err := s.runOnce(context.Background())
	if !gotBytes || upstreamFault || !errors.Is(err, errTranscodeOutputIdle) ||
		!errors.Is(err, errLocalTranscodeFailure) {
		t.Fatalf("blocked local pipeline result=(bytes=%v upstream=%v err=%v), want local output idle",
			gotBytes, upstreamFault, err)
	}
	if shouldMarkSourceFailure(err) {
		t.Fatalf("blocked local stdin/stdout would decay provider health: %v", err)
	}
	if elapsed := time.Since(started); elapsed > 1500*time.Millisecond {
		t.Fatalf("blocked stdin/stdout teardown took %s, want bounded cancellation", elapsed)
	}
	if writes := postPrefixWrites.Load(); writes == 0 {
		t.Fatal("fixture never supplied healthy post-prefix provider media")
	}
}

func TestTranscodeRunJoinsBlockedInputPumpBeforeSameSourceRetry(t *testing.T) {
	srv, _, _, requests := healthyAVTranscodeInputServer(t)
	s := NewTranscodeStreamer("blocked-pump-retry-order", srv.URL, stubProfile(),
		blockedStdinAndStdoutTranscoder(t), testLogger(), nil)
	s.prefixValidator = &thresholdPrefixValidator{readyAt: minPrefixDecisionBytes}
	s.sourceStartupTimeout = defaultSourceStartupTimeout
	s.idleReadTimeout = defaultIdleReadTimeout
	s.outputProgressTimeout = defaultTranscodeOutputProgressTimeout
	s.reconnectWindow = 12 * time.Second
	s.SlateFn = func() *slate {
		return &slate{data: bytes.Repeat([]byte{0xab}, 16*1024), duration: 100 * time.Millisecond}
	}

	firstPumpAtExit := make(chan struct{})
	releaseFirstPump := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseFirstPump) }) }
	defer release()
	var pumpExits atomic.Int32
	var firstPumpExitedUnixNS atomic.Int64
	outputIdleObserved := make(chan struct{})
	var outputIdleOnce sync.Once
	var writeBlockedAtOutputIdle atomic.Bool
	s.inputPumpHooks = &transcodeInputPumpTestHooks{
		onOutputIdle: func(writeInFlight bool) {
			outputIdleOnce.Do(func() {
				writeBlockedAtOutputIdle.Store(writeInFlight)
				close(outputIdleObserved)
			})
		},
		onPumpExit: func() {
			if pumpExits.Add(1) != 1 {
				return
			}
			close(firstPumpAtExit)
			<-releaseFirstPump
			firstPumpExitedUnixNS.Store(time.Now().UnixNano())
		},
	}
	var upstreamFailures atomic.Int32
	runningEvents := make(chan time.Time, 4)
	s.OnRunning = func() { runningEvents <- time.Now() }
	s.OnUpstreamDown = func(_ context.Context, _ error) (string, bool, error) {
		upstreamFailures.Add(1)
		return "", true, nil
	}

	defer drainTranscodeTestSubscriber(s, "blocked-pump-retry-order")()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.Run(ctx)
		close(done)
	}()
	defer func() {
		cancel()
		release()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("transcode Run did not stop after blocked-pump retry test")
		}
	}()

	var firstRequest transcodeInputRequestEvent
	select {
	case request := <-requests:
		if request.number != 1 {
			t.Fatalf("first provider request=%d, want 1", request.number)
		}
		firstRequest = request
	case <-time.After(3 * time.Second):
		t.Fatal("first provider request did not start")
	}
	select {
	case <-firstPumpAtExit:
	case <-time.After(7 * time.Second):
		t.Fatal("blocked stdin pump did not reach physical exit")
	}
	var firstRunning time.Time
	select {
	case firstRunning = <-runningEvents:
	default:
		t.Fatal("first attempt reached output-idle without validated running media")
	}
	select {
	case <-outputIdleObserved:
		if !writeBlockedAtOutputIdle.Load() {
			t.Fatal("fixture did not hold FFmpeg stdin Write across output-idle cancellation")
		}
	default:
		t.Fatal("blocked input pump exited without an observed output-idle deadline")
	}

	// Hold the old attempt inside its final exit hook for longer than the local
	// 250ms retry/slate backoff. A runOnce that merely reads buffered upErr but
	// does not join inputPumpDone can open request #2 while this goroutine still
	// owns the old FFmpeg stdin lifetime.
	hold := time.NewTimer(2*reconnectInitialBackoff + 300*time.Millisecond)
	defer hold.Stop()
	select {
	case request := <-requests:
		t.Fatalf("provider request #%d started before old input pump exited", request.number)
	case <-hold.C:
	}

	release()
	var second transcodeInputRequestEvent
	select {
	case second = <-requests:
		if second.number != 2 {
			t.Fatalf("retry provider request=%d, want 2", second.number)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("same-source retry did not start after input pump exit")
	}
	exitedNS := firstPumpExitedUnixNS.Load()
	if exited := time.Unix(0, exitedNS); exitedNS <= 0 || second.at.Before(exited) {
		t.Fatalf("request #2 started at %v before input pump exit at %v", second.at, exited)
	}
	var secondRunning time.Time
	select {
	case secondRunning = <-runningEvents:
	case <-time.After(defaultSourceStartupTimeout):
		t.Fatal("second attempt did not establish validated output")
	}
	if elapsed := secondRunning.Sub(firstRunning); elapsed >= ClientStartupBudget {
		t.Fatalf("physical-exit join expanded default recovery to %s; want <%s (first request %v)",
			elapsed, ClientStartupBudget, firstRequest.at)
	}
	if got := upstreamFailures.Load(); got != 0 {
		t.Fatalf("OnUpstreamDown fired %d times for blocked local FFmpeg", got)
	}
}

func TestTranscodeProviderReadTimeoutPrecedesSilentStdout(t *testing.T) {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
		audioPID = 0x0101
	)
	prefix, _, _, _ := healthyAVTranscodePrefix(
		pmtPID, videoPID, audioPID, minPrefixDecisionBytes+2*tsPacketSize)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(prefix)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		<-r.Context().Done()
	}))
	defer srv.Close()

	s := NewTranscodeStreamer("provider-read-precedence", srv.URL, stubProfile(),
		stubBinary(t, "exec cat"), testLogger(), nil)
	s.prefixValidator = &thresholdPrefixValidator{readyAt: minPrefixDecisionBytes}
	s.sourceStartupTimeout = 2 * time.Second
	s.idleReadTimeout = 200 * time.Millisecond
	s.stallTolerance = s.idleReadTimeout
	s.outputProgressTimeout = s.idleReadTimeout + 150*time.Millisecond
	defer drainTranscodeTestSubscriber(s, "provider-read-precedence")()

	gotBytes, upstreamFault, err := s.runOnce(context.Background())
	if !gotBytes || !upstreamFault || !errors.Is(err, errUpstreamIdle) {
		t.Fatalf("simultaneous silent input/output result=(bytes=%v upstream=%v err=%v), want provider Read timeout",
			gotBytes, upstreamFault, err)
	}
	if errors.Is(err, errTranscodeOutputIdle) {
		t.Fatalf("provider Read timeout lost deterministic precedence: %v", err)
	}
}

func TestTranscodeProviderReadDeadlineWinnerCannotReadOrWriteTail(t *testing.T) {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
		audioPID = 0x0101
	)
	prefix, pts, videoCC, audioCC := healthyAVTranscodePrefix(
		pmtPID, videoPID, audioPID, minPrefixDecisionBytes+2*tsPacketSize)
	tail := joinTSPackets(
		testTimestampedPESPacket(videoPID, 0xe0, pts, videoCC),
		testTimestampedPESPacket(audioPID, 0xc0, pts, audioCC))
	timeoutWon := make(chan struct{})
	releaseWatcher := make(chan struct{})
	timeoutObserved := make(chan struct{})
	releaseReader := make(chan struct{})
	inputPumpExited := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseWatcher) }) }
	defer release()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write(prefix); err != nil {
			return
		}
		flusher, _ := w.(http.Flusher)
		if flusher != nil {
			flusher.Flush()
		}
		select {
		case <-timeoutWon:
		case <-r.Context().Done():
			return
		}
		// The watcher owns the phase CAS but is deliberately held before it
		// publishes idleKilled/cancellation. Return a valid buffered tail into
		// that window; the reader's losing CAS must discard it and terminate.
		if _, err := w.Write(tail); err != nil {
			return
		}
		if flusher != nil {
			flusher.Flush()
		}
		<-r.Context().Done()
	}))
	defer srv.Close()

	s := NewTranscodeStreamer("provider-read-cas-winner", srv.URL, stubProfile(),
		stubBinary(t, "exec cat"), testLogger(), nil)
	s.prefixValidator = &thresholdPrefixValidator{readyAt: minPrefixDecisionBytes}
	s.sourceStartupTimeout = 2 * time.Second
	s.idleReadTimeout = 150 * time.Millisecond
	s.stallTolerance = s.idleReadTimeout
	s.outputProgressTimeout = 750 * time.Millisecond
	defer drainTranscodeTestSubscriber(s, "provider-read-cas-winner")()
	var readStarts, writeStarts atomic.Int32
	var readsAtWin, writesAtWin atomic.Int32
	s.inputPumpHooks = &transcodeInputPumpTestHooks{
		onReadStart:  func() { readStarts.Add(1) },
		onWriteStart: func() { writeStarts.Add(1) },
		afterTimeoutWin: func() {
			readsAtWin.Store(readStarts.Load())
			writesAtWin.Store(writeStarts.Load())
			close(timeoutWon)
			<-releaseWatcher
		},
		onTimeoutObserved: func() {
			close(timeoutObserved)
			<-releaseReader
		},
		onPumpExit: func() { close(inputPumpExited) },
	}

	type result struct {
		gotBytes      bool
		upstreamFault bool
		err           error
	}
	resultCh := make(chan result, 1)
	go func() {
		gotBytes, upstreamFault, err := s.runOnce(context.Background())
		resultCh <- result{gotBytes: gotBytes, upstreamFault: upstreamFault, err: err}
	}()
	select {
	case <-timeoutObserved:
	case <-time.After(2 * time.Second):
		release()
		t.Fatal("reader never observed the watchdog's winning phase CAS")
	}
	// Let the CAS-losing reader proceed while the winning watcher remains held
	// before idleKilled/context publication. The losing CAS must be a terminal,
	// typed timeout by itself; an implementation that consults ctx.Err() here
	// can see nil and enter a second Body.Read, so it cannot signal pump exit.
	close(releaseReader)
	select {
	case <-inputPumpExited:
	case <-time.After(2 * time.Second):
		release()
		t.Fatal("CAS-losing reader did not exit before watcher cancellation publication")
	}
	if got, want := readStarts.Load(), readsAtWin.Load(); got != want {
		release()
		t.Fatalf("reader started %d reads after timeout winner; total=%d at_win=%d",
			got-want, got, want)
	}
	if got, want := writeStarts.Load(), writesAtWin.Load(); got != want {
		release()
		t.Fatalf("reader wrote %d tails after timeout winner; total=%d at_win=%d",
			got-want, got, want)
	}
	release()
	select {
	case got := <-resultCh:
		if !got.gotBytes || !got.upstreamFault || !errors.Is(got.err, errUpstreamIdle) {
			t.Fatalf("CAS-winner result=(bytes=%v upstream=%v err=%v), want typed provider timeout",
				got.gotBytes, got.upstreamFault, got.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("CAS-winner runOnce did not terminate after watcher release")
	}
}

func TestTranscodeSilentOutputStillRelocatesForProvenInputAudioStarvation(t *testing.T) {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
		audioPID = 0x0101
	)
	prefix, pts, videoCC, _ := healthyAVTranscodePrefix(
		pmtPID, videoPID, audioPID, minPrefixDecisionBytes+2*tsPacketSize)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write(prefix); err != nil {
			return
		}
		flusher, _ := w.(http.Flusher)
		if flusher != nil {
			flusher.Flush()
		}
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case <-ticker.C:
				packet := testTimestampedPESPacket(videoPID, 0xe0, pts, videoCC)
				pts += transportPTSRate / 50
				videoCC = (videoCC + 1) & 0x0f
				if _, err := w.Write(packet); err != nil {
					return
				}
				if flusher != nil {
					flusher.Flush()
				}
			}
		}
	}))
	defer srv.Close()

	s := NewTranscodeStreamer("input-audio-starvation", srv.URL, stubProfile(),
		hungStdoutTranscoder(t), testLogger(), nil)
	s.prefixValidator = &thresholdPrefixValidator{readyAt: minPrefixDecisionBytes}
	s.sourceStartupTimeout = 2 * time.Second
	s.idleReadTimeout = 2 * time.Second
	s.outputProgressTimeout = 1200 * time.Millisecond
	s.avContinuityPolicy = avContinuityPolicy{
		starvationThreshold: 600 * time.Millisecond,
		driftThreshold:      time.Second,
		skewThreshold:       2 * time.Second,
		confirmation:        50 * time.Millisecond,
		minSamples:          4,
	}
	defer drainTranscodeTestSubscriber(s, "input-audio-starvation")()
	beforeOutputStalls := metrics.TranscodeOutputStalls.Value()

	gotBytes, upstreamFault, err := s.runOnce(context.Background())
	var continuityErr *avContinuityError
	if !gotBytes || !upstreamFault || !errors.As(err, &continuityErr) ||
		continuityErr.kind != avFaultAudioStarvation {
		t.Fatalf("silent output with proven input starvation=(bytes=%v upstream=%v err=%v), want input audio relocation",
			gotBytes, upstreamFault, err)
	}
	if !shouldMarkSourceFailure(err) {
		t.Fatalf("proven flowing-input audio starvation would not decay that source: %v", err)
	}
	if delta := metrics.TranscodeOutputStalls.Value() - beforeOutputStalls; delta != 0 {
		t.Fatalf("input starvation incremented local output-stall metric by %d", delta)
	}
}

func TestTranscodeOutputStallFeedsSlateBeforeSameSourceRestart(t *testing.T) {
	srv, _, _, requests := healthyAVTranscodeInputServer(t)
	s := configureHungOutputTestStreamer(t, srv.URL)
	s.reconnectWindow = 2 * time.Second
	s.SlateFn = func() *slate {
		return &slate{data: bytes.Repeat([]byte{0xab}, 16*1024), duration: 100 * time.Millisecond}
	}
	var upstreamFailures atomic.Int32
	s.OnUpstreamDown = func(_ context.Context, _ error) (string, bool, error) {
		upstreamFailures.Add(1)
		return "", true, nil
	}

	viewer, unsubscribe := s.Subscribe("hung-output-viewer")
	defer unsubscribe()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.Run(ctx)
		close(done)
	}()

	var firstRealAt, slateAt time.Time
	requestCount := int32(0)
	var secondRequestAt time.Time
	deadline := time.After(3 * time.Second)
	for slateAt.IsZero() || requestCount < 2 {
		select {
		case request := <-requests:
			if request.number > requestCount {
				requestCount = request.number
			}
			if request.number == 2 {
				secondRequestAt = request.at
			}
		case chunk, ok := <-viewer:
			if !ok {
				cancel()
				t.Fatal("viewer closed during local stdout recovery")
			}
			if len(chunk) == 0 {
				continue
			}
			if chunk[0] == 0xab {
				if slateAt.IsZero() {
					slateAt = time.Now()
				}
			} else if firstRealAt.IsZero() {
				firstRealAt = time.Now()
			}
		case <-deadline:
			cancel()
			t.Fatalf("local output recovery incomplete: slate=%v requests=%d",
				!slateAt.IsZero(), requestCount)
		}
	}
	if firstRealAt.IsZero() || !slateAt.After(firstRealAt) {
		cancel()
		t.Fatalf("slate ordering real=%v slate=%v", firstRealAt, slateAt)
	}
	if elapsed := slateAt.Sub(firstRealAt); elapsed > s.outputProgressTimeout+300*time.Millisecond {
		cancel()
		t.Fatalf("immediate continuity slate took %s after last real output, want <=%s",
			elapsed, s.outputProgressTimeout+300*time.Millisecond)
	}
	if got := upstreamFailures.Load(); got != 0 {
		cancel()
		t.Fatalf("OnUpstreamDown fired %d times for local stdout stall", got)
	}
	if secondRequestAt.IsZero() || !secondRequestAt.After(slateAt) {
		cancel()
		t.Fatalf("same-source retry started at %v before continuity slate at %v",
			secondRequestAt, slateAt)
	}
	if lead := secondRequestAt.Sub(slateAt); lead < reconnectInitialBackoff-75*time.Millisecond {
		cancel()
		t.Fatalf("same-source retry followed slate after %s, want the 250ms slate backoff first", lead)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("transcode streamer did not stop after hung-output recovery test")
	}
}

func TestSourceSuccessAttributionRequiresValidatedMedia(t *testing.T) {
	current := uuid.New()
	tests := []struct {
		name      string
		err       error
		delivered bool
		want      bool
	}{
		{name: "clean after media", delivered: true, want: true},
		{name: "clean startup abort", delivered: false, want: false},
		{name: "error after media", err: errors.New("upstream reset"), delivered: true, want: false},
		{name: "error before media", err: errors.New("startup failed"), delivered: false, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldMarkSourceSuccess(tt.err, tt.delivered, current, current); got != tt.want {
				t.Fatalf("shouldMarkSourceSuccess(err=%v, delivered=%v)=%v, want %v",
					tt.err, tt.delivered, got, tt.want)
			}
		})
	}
}

func TestTranscodeAVContinuityAttributionUsesBothSidesOfPipeline(t *testing.T) {
	audioMissing := &avContinuityError{kind: avFaultAudioStarvation}
	drift := &avContinuityError{kind: avFaultTimelineDrift}
	videoMissing := &avContinuityError{kind: avFaultVideoStarvation}
	transportDamage := &avContinuityError{kind: avFaultTransport}

	if fault, input := selectTranscodeContinuityFault(audioMissing, nil); fault != audioMissing || !input {
		t.Fatalf("proven input audio starvation selected fault=%v input=%v", fault, input)
	}
	if fault, input := selectTranscodeContinuityFault(drift, nil); fault != nil || input {
		t.Fatalf("repairable input drift selected fault=%v input=%v", fault, input)
	}
	if fault, input := selectTranscodeContinuityFault(nil, videoMissing); fault != videoMissing || input {
		t.Fatalf("output-only video starvation selected fault=%v input=%v", fault, input)
	}
	if fault, input := selectTranscodeContinuityFault(drift, &avContinuityError{kind: avFaultTimelineDrift}); fault != drift || !input {
		t.Fatalf("matching input/output drift selected fault=%v input=%v", fault, input)
	}
	if fault, input := selectTranscodeContinuityFault(transportDamage, audioMissing); fault != transportDamage || !input {
		t.Fatalf("input transport damage + output audio loss selected fault=%v input=%v", fault, input)
	}
	if fault, input := selectTranscodeContinuityFault(transportDamage, nil); fault != nil || input {
		t.Fatalf("repaired transient input transport fence selected fault=%v input=%v", fault, input)
	}

	if !shouldMarkSourceFailure(fmt.Errorf("input media: %w", audioMissing)) {
		t.Fatal("proven input starvation would not decay source health")
	}
	if shouldMarkSourceFailure(localTranscodeFailure(videoMissing)) {
		t.Fatal("output-only continuity fault would decay source health")
	}
}

func TestTranscodeRelayBufferFailureRetriesSameSource(t *testing.T) {
	attempt := newUpstreamAttempt(context.Background(), "http://provider.test/live",
		"relay-buffer", testLogger(), time.Minute)
	defer attempt.Close()
	upstreamFault, err := transcodeRelayBufferFailure(attempt, errRelayReadAheadFull)
	if upstreamFault {
		t.Fatal("local ffmpeg-output queue exhaustion requested provider relocation")
	}
	if !errors.Is(err, errRelayReadAheadFull) {
		t.Fatalf("relay error=%v, want typed read-ahead exhaustion", err)
	}
	if shouldMarkSourceFailure(err) {
		t.Fatalf("relay output exhaustion would decay provider health: %v", err)
	}
}

func TestTranscodePacingFailureRequiresRecentProviderGap(t *testing.T) {
	for _, tc := range []struct {
		name          string
		cause         error
		gapAge        time.Duration
		upstreamFault bool
		localFault    bool
		markSource    bool
	}{
		{name: "recent provider gap", cause: errTransportRecoveryBacklog, gapAge: time.Second, upstreamFault: true, markSource: true},
		{name: "stale provider gap", cause: errTransportRecoveryBacklog, gapAge: transcodeRecoveryReadGapWindow + time.Millisecond, localFault: true},
		{name: "no provider gap", cause: errTransportRecoveryBacklog, localFault: true},
		{name: "stale relay chunk", cause: errTransportStaleChunk, localFault: true},
		{name: "buffered relay backlog", cause: errTransportRelayBacklog, upstreamFault: true},
		{name: "transport clock boundary", cause: errTransportAttemptBoundary, localFault: true},
		{name: "ffmpeg output program map stall", cause: errTransportProgramMapStall, localFault: true},
		{name: "canceled wait", cause: context.Canceled, markSource: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			attempt := newUpstreamAttempt(context.Background(), "http://provider.test/live",
				"relay-pacing", testLogger(), time.Minute)
			defer attempt.Close()
			if tc.gapAge > 0 {
				attempt.diag.readGaps.recordAt(diagnosticGapThreshold,
					time.Now().Add(-tc.gapAge))
			}
			upstreamFault, err := transcodePacingFailure(attempt, tc.cause)
			if upstreamFault != tc.upstreamFault {
				t.Fatalf("upstreamFault=%v, want %v", upstreamFault, tc.upstreamFault)
			}
			if !errors.Is(err, tc.cause) {
				t.Fatalf("pacing error=%v, want wrapped %v", err, tc.cause)
			}
			if got := errors.Is(err, errLocalTranscodeFailure); got != tc.localFault {
				t.Fatalf("local fault=%v, want %v: %v", got, tc.localFault, err)
			}
			if got := shouldMarkSourceFailure(err); got != tc.markSource {
				t.Fatalf("source health failure=%v, want %v: %v", got, tc.markSource, err)
			}
		})
	}
}

func delayedTranscodePrefixLiveStreamServer(t *testing.T) (*httptest.Server, int) {
	t.Helper()
	// A 25 ms PCR step still proves cadence (>15 ms without startup
	// acceleration) while leaving deterministic scheduler headroom below the
	// one-second live arrival-age ceiling.
	chunks := pcrSparseTestChunks(delayedLivePrefixChunkCount, 25*time.Millisecond)
	rawBytes := delayedLivePrefixChunkCount * chunkSize
	tailOffset := rawBytes % tsPacketSize
	tail := pcrTestNonPCRPacket(0x1ffe, false)[tailOffset:]
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		flusher, _ := w.(http.Flusher)
		for _, chunk := range chunks {
			_, _ = w.Write(chunk)
		}
		_, _ = w.Write(tail)
		if flusher != nil {
			flusher.Flush()
		}
		<-r.Context().Done()
	}))
	return srv, rawBytes + len(tail)
}

func TestTranscodeStartsPacingAtValidatedPrefixRelease(t *testing.T) {
	srv, _ := delayedTranscodePrefixLiveStreamServer(t)
	defer srv.Close()
	expectedPrefixBytes := delayedLivePrefixChunkCount * chunkSize
	s := NewTranscodeStreamer("transcode-prefix-release", srv.URL, stubProfile(),
		stubBinary(t, "exec cat"), testLogger(), nil)
	s.prefixValidator = &thresholdPrefixValidator{
		readyAt: delayedLivePrefixChunkCount * chunkSize, mediaDuration: 1600 * time.Millisecond,
		pcrPID: 0x100, havePCRPID: true, delay: 450 * time.Millisecond,
	}
	// Keep the input live through validation so shell scheduling cannot turn a
	// successful cat passthrough into finite-input EOF attribution under -race.
	s.sourceStartupTimeout = 5 * time.Second
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	delivered := 0
	s.OnBytes = func(n int) error {
		delivered += n
		if delivered == expectedPrefixBytes {
			cancel()
		}
		return nil
	}
	_, unsubscribe := s.Subscribe("transcode-prefix-release-test")
	defer unsubscribe()
	gotBytes, upstreamFault, err := s.runOnce(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("valid private transcode prefix failed after validation delay: %v", err)
	}
	if !gotBytes {
		t.Fatalf("transcode prefix upstreamFault=%v gotBytes=false err=%v", upstreamFault, err)
	}
	if got := s.bytesOut.Load(); got != int64(expectedPrefixBytes) {
		t.Fatalf("transcode validated prefix bytes=%d, want %d",
			got, expectedPrefixBytes)
	}
}

func TestTranscodeBackpressuresBurstyOutputWithoutRestartOrAVDrift(t *testing.T) {
	const (
		chunkCount = 201
		mediaStep  = 10 * time.Millisecond
	)
	chunks := burstyAVPCRTestChunks(chunkCount, mediaStep)
	rawBytes := chunkCount * chunkSize
	tailOffset := rawBytes % tsPacketSize
	tail := pcrTestNonPCRPacket(0x1ffe, false)[tailOffset:]
	declareSyntheticTimestampPESLengths(append(chunks, tail)...)
	inputBytes := rawBytes + len(tail)
	wantBytes := inputBytes + 2*tsPacketSize // initial video/audio markers
	checked := newCheckedTranscodeInput()
	checkedBytes := 0
	for i, chunk := range chunks {
		out, checkedErr := checked.push(chunk, time.Now())
		checkedBytes += len(out)
		if checkedErr != nil {
			t.Fatalf("bursty fixture chunk %d input safety=(bytes=%d err=%v)", i, checkedBytes, checkedErr)
		}
	}
	checkedTail, checkedErr := checked.push(tail, time.Now())
	checkedBytes += len(checkedTail)
	if checkedErr != nil {
		t.Fatalf("bursty fixture tail input safety=(bytes=%d err=%v)", checkedBytes, checkedErr)
	}
	if checkedBytes != inputBytes {
		t.Fatalf("bursty fixture input safety bytes=%d, want %d", checkedBytes, inputBytes)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		flusher, _ := w.(http.Flusher)
		// Deliberately write the complete two-second A/V window as fast as the
		// transport permits. The 24x/15x rate boundaries are pinned by the
		// deterministic pacer test; this end-to-end case is the stronger
		// unbounded producer that caused v0.44.0 to restart `cat`/FFmpeg.
		for _, chunk := range chunks {
			if _, err := w.Write(chunk); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
		if _, err := w.Write(tail); err != nil {
			return
		}
		if flusher != nil {
			flusher.Flush()
		}
		<-r.Context().Done()
	}))
	defer srv.Close()

	s := NewTranscodeStreamer("transcode-bursty-output", srv.URL, stubProfile(),
		stubBinary(t, "exec cat"), testLogger(), nil)
	s.prefixValidator = exactProgramMapTestValidator{readyAt: chunkSize}
	s.sourceStartupTimeout = 5 * time.Second
	viewer, unsubscribe := s.Subscribe("plex-bursty-output")
	defer unsubscribe()
	receivedDone := make(chan []byte, 1)
	go func() {
		received := make([]byte, 0, wantBytes)
		for len(received) < wantBytes {
			chunk, ok := <-viewer
			if !ok {
				break
			}
			received = append(received, chunk...)
		}
		receivedDone <- received
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	var (
		delivered int
		fanoutAt  []time.Time
	)
	s.OnBytes = func(n int) error {
		delivered += n
		fanoutAt = append(fanoutAt, time.Now())
		if delivered >= wantBytes {
			cancel()
		}
		return nil
	}
	gotBytes, upstreamFault, err := s.runOnce(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("bursty transcode output restarted: bytes=%v delivered=%d fanout=%d upstream=%v err=%v",
			gotBytes, delivered, len(fanoutAt), upstreamFault, err)
	}
	if !gotBytes || upstreamFault || delivered != wantBytes {
		t.Fatalf("bursty transcode result=(bytes=%v upstream=%v delivered=%d err=%v), want %d bytes without relocation",
			gotBytes, upstreamFault, delivered, err, wantBytes)
	}
	wantSpan := time.Duration(chunkCount-1) * mediaStep
	if span := fanoutAt[len(fanoutAt)-1].Sub(fanoutAt[0]); span < wantSpan-100*time.Millisecond {
		t.Fatalf("bursty transcode compressed %s of media into %s", wantSpan, span)
	}

	var received []byte
	select {
	case received = <-receivedDone:
	case <-time.After(2 * time.Second):
		t.Fatal("bursty transcode subscriber did not receive the paced tail")
	}
	if len(received) != wantBytes {
		t.Fatalf("bursty subscriber bytes=%d, want %d", len(received), wantBytes)
	}
	var timeline transportAVTimeline
	timeline.requireFirstAudio = true
	timeline.feedAt(received, time.Now())
	snapshot := timeline.snapshot()
	if !snapshot.valid || snapshot.videoSamples < chunkCount || snapshot.audioSamples < chunkCount ||
		snapshot.drift != 0 || snapshot.initialSkew != 0 || snapshot.finalSkew != 0 {
		t.Fatalf("paced burst changed A/V continuity: %+v", snapshot)
	}
}

func TestTranscodeFiniteReplayDrainsSubscriberTailBeforeAttemptBoundary(t *testing.T) {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
		audioPID = 0x0101
	)
	input, _, _, _ := healthyAVTranscodePrefix(
		pmtPID, videoPID, audioPID, int(minFinitePreflightBytes)+tsPacketSize)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		w.Header().Set("Content-Length", fmt.Sprint(len(input)))
		_, _ = w.Write(input)
	}))
	defer upstream.Close()

	s := NewTranscodeStreamer("transcode-finite-tail", upstream.URL, stubProfile(),
		// Close stdout before process exit so this fixture presents the clean pipe
		// EOF required by the finite-output drain contract. A bare `exec cat` lets
		// os/exec's concurrent Wait close StdoutPipe first on some schedules.
		stubBinary(t, "cat; exec 1>&-; sleep 0.2"), testLogger(), nil)
	s.classifier = &staticFiniteClassifier{result: classificationResult{
		kind: classificationFiniteAccepted, reason: "test_finite_accepted",
	}}
	// The test targets the subscriber boundary, not media-prefix rewriting.
	// Valid packet-aligned input passes checkedTranscodeInput unchanged; disabling
	// the output validator makes cat's expected byte stream exact.
	s.prefixValidator = nil
	s.sourceStartupTimeout = 5 * time.Second
	chunks, unsubscribe := s.Subscribe("transcode-finite-tail-client")
	defer unsubscribe()

	boundaryQueued := make(chan struct{})
	var boundaryOnce sync.Once
	s.onDeliveryBoundaryQueued = func() {
		boundaryOnce.Do(func() { close(boundaryQueued) })
	}
	type runResult struct {
		gotBytes      bool
		upstreamFault bool
		err           error
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	runDone := make(chan runResult, 1)
	go func() {
		gotBytes, upstreamFault, err := s.runOnce(ctx)
		runDone <- runResult{gotBytes: gotBytes, upstreamFault: upstreamFault, err: err}
	}()
	select {
	case <-boundaryQueued:
	case result := <-runDone:
		t.Fatalf("finite transcode attempt returned before queuing its delivery boundary: %+v",
			result)
	case <-time.After(5 * time.Second):
		t.Fatal("finite transcode output did not reach its delivery boundary")
	}
	expectedBytes := s.bytesOut.Load()
	if expectedBytes <= 0 || expectedBytes > int64(len(input)) {
		t.Fatalf("finite transcode queued bytes=%d, want within 1..%d",
			expectedBytes, len(input))
	}
	select {
	case result := <-runDone:
		t.Fatalf("finite transcode attempt crossed EOF before subscriber drain: %+v", result)
	case <-time.After(25 * time.Millisecond):
	}

	delivered := make([]byte, 0, expectedBytes)
	for int64(len(delivered)) < expectedBytes {
		select {
		case chunk, ok := <-chunks:
			if !ok {
				t.Fatalf("finite transcode subscriber closed at %d/%d bytes",
					len(delivered), expectedBytes)
			}
			delivered = append(delivered, chunk...)
		case <-time.After(5 * time.Second):
			t.Fatalf("finite transcode subscriber stalled at %d/%d bytes",
				len(delivered), expectedBytes)
		}
	}
	if int64(len(delivered)) != expectedBytes || !bytes.Equal(delivered, input[:expectedBytes]) {
		t.Fatalf("finite transcode output mismatch: got=%d want=%d",
			len(delivered), expectedBytes)
	}
	select {
	case result := <-runDone:
		if !result.gotBytes || !result.upstreamFault ||
			!errors.Is(result.err, io.ErrUnexpectedEOF) {
			t.Fatalf("finite transcode completion=%+v, want delivered upstream EOF", result)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("finite transcode attempt did not cross the drained boundary")
	}
}

func TestTranscodePacesPrefixAfterSlowOnRunning(t *testing.T) {
	srv, _ := delayedTranscodePrefixLiveStreamServer(t)
	defer srv.Close()
	expectedPrefixBytes := delayedLivePrefixChunkCount * chunkSize
	s := NewTranscodeStreamer("transcode-slow-running", srv.URL, stubProfile(),
		stubBinary(t, "exec cat"), testLogger(), nil)
	s.prefixValidator = &thresholdPrefixValidator{
		readyAt: delayedLivePrefixChunkCount * chunkSize, mediaDuration: 1600 * time.Millisecond,
		pcrPID: 0x100, havePCRPID: true,
	}
	// This regression targets the post-validation release edge, not the
	// production startup budget. Leave headroom for race/CI instrumentation to
	// move 2 MiB through the cat pipeline before validation.
	s.sourceStartupTimeout = 5 * time.Second
	s.OnRunning = func() { time.Sleep(maxTransportAddedLatency + 50*time.Millisecond) }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var (
		delivered int
		fanoutAt  []time.Time
	)
	s.OnBytes = func(n int) error {
		fanoutAt = append(fanoutAt, time.Now())
		delivered += n
		if delivered == expectedPrefixBytes {
			cancel()
		}
		return nil
	}
	_, unsubscribe := s.Subscribe("transcode-slow-running-test")
	defer unsubscribe()
	gotBytes, upstreamFault, err := s.runOnce(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("slow transcode OnRunning prefix failed: %v", err)
	}
	if !gotBytes || s.bytesOut.Load() != int64(expectedPrefixBytes) {
		t.Fatalf("slow transcode OnRunning upstreamFault=%v gotBytes=%v bytes=%d want=%d err=%v",
			upstreamFault, gotBytes, s.bytesOut.Load(), expectedPrefixBytes, err)
	}
	if len(fanoutAt) < 2 {
		t.Fatalf("slow transcode OnRunning fanout callbacks=%d, want at least 2", len(fanoutAt))
	}
	if interval := fanoutAt[1].Sub(fanoutAt[0]); interval < 15*time.Millisecond {
		t.Fatalf("first two transcode prefix chunks were %s apart; callback time replaced pacing cadence", interval)
	}
}

func TestSourceSuccessRequiresStableCurrentSource(t *testing.T) {
	current := uuid.New()
	other := uuid.New()
	tests := []struct {
		name   string
		err    error
		stable uuid.UUID
		want   bool
	}{
		{name: "stable current", stable: current, want: true},
		{name: "no stable media", stable: uuid.Nil, want: false},
		{name: "prior source stable", stable: other, want: false},
		{name: "terminal error", err: context.Canceled, stable: current, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldMarkSourceSuccess(tt.err, true, tt.stable, current); got != tt.want {
				t.Fatalf("shouldMarkSourceSuccess(%v, %s, %s)=%v, want %v",
					tt.err, tt.stable, current, got, tt.want)
			}
		})
	}
}

func stubBinary(t *testing.T, script string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "fake-ffmpeg")
	if err := os.WriteFile(p, []byte("#!/bin/sh\n"+script+"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	return p
}

func stubProfile() *transcode.Profile {
	return &transcode.Profile{Name: "stub", Kind: transcode.Kind("test-cpu")}
}

type markerOutputPrefixValidator struct{}

func (markerOutputPrefixValidator) Validate(_ context.Context, data []byte) (prefixDecision, error) {
	if len(data) > 0 && data[0] == 0xbc {
		return prefixDecision{kind: prefixReady, reason: "test_alternate_ready"}, nil
	}
	return prefixDecision{kind: prefixNeedMore, reason: "test_invalid_transcode_output"}, nil
}

// fastByteServer streams 1KB blocks every 2ms (~500KB/s) until stopped.
func fastByteServer(t *testing.T) (*httptest.Server, func()) {
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

// S6: a "transcoder" that exits mid-stream under a healthy upstream must be
// respawned on the same source — bytes keep flowing across generations and
// the restart counter moves; the failover callback stays untouched.
func TestTranscodeRespawnsAfterFFmpegDeath(t *testing.T) {
	srv, stopSrv := fastByteServer(t)
	defer stopSrv()

	// Dies after passing 256KB through.
	bin := stubBinary(t, "head -c 262144")

	s := NewTranscodeStreamer("test", srv.URL, stubProfile(), bin, testLogger(), nil)
	var failovers atomic.Int32
	s.OnUpstreamDown = func(_ context.Context, _ error) (string, bool, error) {
		failovers.Add(1)
		return "", true, nil
	}

	ch, unsub := s.Subscribe("viewer")
	defer unsub()

	restartsBefore := metrics.FFmpegRestarts.Value()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()

	// Drain and count: passing 700KB total requires at least three
	// 256KB-limited transcoder generations → at least two respawns.
	var received int64
	deadline := time.After(30 * time.Second)
	for received < 700*1024 {
		select {
		case <-deadline:
			t.Fatalf("only %d bytes through after 30s (restarts: %d)",
				received, metrics.FFmpegRestarts.Value()-restartsBefore)
		case chunk, ok := <-ch:
			if !ok {
				t.Fatalf("subscriber closed after %d bytes — pump gave up instead of respawning", received)
			}
			received += int64(len(chunk))
		}
	}

	if d := metrics.FFmpegRestarts.Value() - restartsBefore; d < 1 {
		t.Fatalf("FFmpegRestarts delta = %d, want >= 1", d)
	}
	if f := failovers.Load(); f != 0 {
		t.Fatalf("OnUpstreamDown fired %d times for an ffmpeg-side death", f)
	}

	cancel()
	<-done
}

// S6: an upstream death under a healthy "transcoder" must take the failover
// path (OnUpstreamDown → new source) with the subscriber held open.
func TestTranscodeFailsOverOnUpstreamDeath(t *testing.T) {
	srvA, stopA := fastByteServer(t)
	defer stopA()
	srvB, stopB := fastByteServer(t)
	defer stopB()

	bin := stubBinary(t, "exec cat")

	s := NewTranscodeStreamer("test", srvA.URL, stubProfile(), bin, testLogger(), nil)
	var failovers atomic.Int32
	s.OnUpstreamDown = func(_ context.Context, cause error) (string, bool, error) {
		if cause == nil {
			t.Error("OnUpstreamDown got nil cause")
		}
		failovers.Add(1)
		return srvB.URL, true, nil
	}

	ch, unsub := s.Subscribe("viewer")
	defer unsub()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()

	// Phase 1: bytes through cat from server A.
	var received int64
	deadline := time.After(15 * time.Second)
	for received < 64*1024 {
		select {
		case <-deadline:
			t.Fatalf("phase 1: only %d bytes", received)
		case chunk, ok := <-ch:
			if !ok {
				t.Fatal("subscriber closed in phase 1")
			}
			received += int64(len(chunk))
		}
	}

	stopA()

	// Phase 2: failover must fire and bytes must keep arriving on the SAME
	// subscriber channel.
	deadline = time.After(30 * time.Second)
	var post int64
	for post < 64*1024 {
		select {
		case <-deadline:
			t.Fatalf("phase 2: only %d bytes after upstream kill (failovers: %d)", post, failovers.Load())
		case chunk, ok := <-ch:
			if !ok {
				t.Fatal("subscriber closed across the failover gap")
			}
			if failovers.Load() > 0 {
				post += int64(len(chunk))
			}
		}
	}
	if failovers.Load() < 1 {
		t.Fatal("OnUpstreamDown never fired")
	}

	cancel()
	<-done
}

// A no-output ffmpeg startup is bounded by the same source attempt timer, but
// once upstream bytes reached stdin it is a local pipeline fault. It must
// retry the same source without calling OnUpstreamDown (which would penalize
// provider health and exclude a healthy source).
func TestTranscodeNoFirstOutputDoesNotPenalizeUpstream(t *testing.T) {
	srv, stopSrv := fastByteServer(t)
	defer stopSrv()

	s := NewTranscodeStreamer("slow-local-output", srv.URL, stubProfile(),
		stubBinary(t, "exec dd of=/dev/null bs=65536"), testLogger(), nil)
	s.sourceStartupTimeout = 700 * time.Millisecond
	s.reconnectWindow = 1500 * time.Millisecond
	var upstreamFailures atomic.Int32
	s.OnUpstreamDown = func(_ context.Context, _ error) (string, bool, error) {
		upstreamFailures.Add(1)
		return "", true, nil
	}

	viewer, unsubscribe := s.Subscribe("viewer")
	defer unsubscribe()
	go func() {
		for range viewer {
		}
	}()

	var exitErr error
	exited := make(chan struct{})
	s.OnExit = func(err error) {
		exitErr = err
		close(exited)
	}
	started := time.Now()
	go s.Run(context.Background())

	select {
	case <-exited:
	case <-time.After(4 * time.Second):
		t.Fatal("local no-output startup failure exceeded bounded reconnect window")
	}
	if elapsed := time.Since(started); elapsed >= 4*time.Second {
		t.Fatalf("local no-output failure took %v, want <4s", elapsed)
	}
	if !errors.Is(exitErr, errTranscodeStartupTimeout) {
		t.Fatalf("exit error=%v, want local transcode startup timeout", exitErr)
	}
	if got := upstreamFailures.Load(); got != 0 {
		t.Fatalf("OnUpstreamDown fired %d times for local ffmpeg startup", got)
	}
}

// Spawn/bad-binary errors are also explicitly local. The terminal wrapper is
// what lets Pool avoid logging or scoring them as provider failures even when
// the local reconnect window is eventually exhausted.
func TestTranscodeSpawnFailureIsClassifiedLocal(t *testing.T) {
	srv, stopSrv := fastByteServer(t)
	defer stopSrv()

	s := NewTranscodeStreamer("missing-local-ffmpeg", srv.URL, stubProfile(),
		filepath.Join(t.TempDir(), "missing-ffmpeg"), testLogger(), nil)
	s.reconnectWindow = 400 * time.Millisecond
	var upstreamFailures atomic.Int32
	s.OnUpstreamDown = func(_ context.Context, _ error) (string, bool, error) {
		upstreamFailures.Add(1)
		return "", true, nil
	}
	_, unsubscribe := s.Subscribe("viewer")
	defer unsubscribe()

	var exitErr error
	exited := make(chan struct{})
	s.OnExit = func(err error) {
		exitErr = err
		close(exited)
	}
	go s.Run(context.Background())

	select {
	case <-exited:
	case <-time.After(2 * time.Second):
		t.Fatal("missing ffmpeg did not terminate within local reconnect window")
	}
	if !errors.Is(exitErr, errLocalTranscodeFailure) {
		t.Fatalf("exit error=%v, want local transcode classification", exitErr)
	}
	if got := upstreamFailures.Load(); got != 0 {
		t.Fatalf("OnUpstreamDown fired %d times for missing local ffmpeg", got)
	}
}

// Source one returns headers but no bytes for the complete four-second
// attempt. A deterministic cat stub must then produce source-two transcoded
// output with clear margin inside Plex's observed ten-second cutoff.
func TestTranscodeStartupTimeoutTriesAlternateBeforePlexDeadline(t *testing.T) {
	var sourceOneRequests atomic.Int32
	sourceOneCanceled := make(chan struct{})
	var canceled atomic.Bool
	sourceOne := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sourceOneRequests.Add(1)
		w.Header().Set("Content-Type", "video/mp2t")
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		<-r.Context().Done()
		if canceled.CompareAndSwap(false, true) {
			close(sourceOneCanceled)
		}
	}))
	defer sourceOne.Close()
	sourceTwo := markerStreamServer(t, 0xbc)
	defer sourceTwo.Close()

	s := NewTranscodeStreamer("transcode-startup-failover", sourceOne.URL,
		stubProfile(), stubBinary(t, "exec cat"), testLogger(), nil)
	s.initialStartupDeadline = time.Now().Add(ClientStartupBudget)
	var failovers atomic.Int32
	s.OnUpstreamDown = func(_ context.Context, cause error) (string, bool, error) {
		if !errors.Is(cause, errSourceStartupTimeout) {
			t.Errorf("source-one cause=%v, want startup timeout", cause)
		}
		failovers.Add(1)
		return sourceTwo.URL, true, nil
	}

	chunks, unsubscribe := s.Subscribe("viewer")
	defer unsubscribe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	started := time.Now()
	go func() {
		s.Run(ctx)
		close(done)
	}()

	select {
	case chunk, ok := <-chunks:
		if !ok || len(chunk) == 0 {
			t.Fatal("transcode subscriber closed before alternate output")
		}
	case <-time.After(8500 * time.Millisecond):
		t.Fatal("source-two transcoded output missed Plex startup margin")
	}
	if elapsed := time.Since(started); elapsed >= 8500*time.Millisecond {
		t.Fatalf("transcode failover readiness took %v, want <8.5s", elapsed)
	}
	if sourceOneRequests.Load() != 1 || failovers.Load() != 1 {
		t.Fatalf("source-one requests=%d failovers=%d, want 1/1",
			sourceOneRequests.Load(), failovers.Load())
	}
	select {
	case <-sourceOneCanceled:
	case <-time.After(time.Second):
		t.Fatal("source-one request remained open after transcode failover")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("transcode streamer did not exit after cancellation")
	}
}

func TestTranscodeIdleWatchdogCauseWinsStartupCancellation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		<-r.Context().Done()
	}))
	defer srv.Close()

	s := NewTranscodeStreamer("transcode-idle-cause", srv.URL, stubProfile(),
		stubBinary(t, "exec cat"), testLogger(), nil)
	s.idleReadTimeout = 150 * time.Millisecond
	s.sourceStartupTimeout = 2 * time.Second
	started := time.Now()
	gotBytes, upstreamFault, runErr := s.runOnce(context.Background())
	if gotBytes {
		t.Fatal("idle source unexpectedly published transcode output")
	}
	if !upstreamFault {
		t.Fatal("independently observed upstream idle was classified as local")
	}
	if !errors.Is(runErr, errUpstreamIdle) {
		t.Fatalf("transcode idle error=%v, want errUpstreamIdle", runErr)
	}
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("transcode idle attribution took %s, want watchdog bound", elapsed)
	}
}

func TestTranscodeInvalidOutputPrefixWithheldThenAlternateSucceeds(t *testing.T) {
	sourceOne := markerStreamServer(t, 0xaa)
	defer sourceOne.Close()
	sourceTwo := markerStreamServer(t, 0xbc)
	defer sourceTwo.Close()

	s := NewTranscodeStreamer("transcode-prefix-failover", sourceOne.URL, stubProfile(),
		stubBinary(t, "exec cat"), testLogger(), nil)
	s.prefixValidator = markerOutputPrefixValidator{}
	s.sourceStartupTimeout = 400 * time.Millisecond
	s.initialStartupDeadline = time.Now().Add(2 * time.Second)
	var failovers atomic.Int32
	causeCh := make(chan error, 1)
	s.OnUpstreamDown = func(_ context.Context, cause error) (string, bool, error) {
		if failovers.Add(1) == 1 {
			causeCh <- cause
		}
		return sourceTwo.URL, true, nil
	}

	viewer, unsubscribe := s.Subscribe("viewer")
	defer unsubscribe()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.Run(ctx)
		close(done)
	}()

	select {
	case chunk, ok := <-viewer:
		if !ok || len(chunk) == 0 {
			cancel()
			t.Fatal("transcode subscriber closed before alternate output")
		}
		if chunk[0] != 0xbc {
			cancel()
			t.Fatalf("invalid source-one transcode prefix leaked marker %#x", chunk[0])
		}
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("invalid transcode output did not relocate inside Plex startup budget")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("transcode streamer did not exit after cancellation")
	}
	var observed error
	select {
	case observed = <-causeCh:
	default:
		t.Fatal("transcode prefix relocation cause was not captured")
	}
	if failovers.Load() != 1 {
		t.Fatalf("transcode prefix failovers=%d, want 1", failovers.Load())
	}
	if !errors.Is(observed, errUncertainMediaPrefix) {
		t.Fatalf("transcode prefix cause=%v, want neutral uncertainty", observed)
	}
	if shouldMarkSourceFailure(observed) {
		t.Fatalf("invalid transcode output would decay source health: %v", observed)
	}
}

// A short fragment that reaches ffmpeg and then stalls is not proof of a
// healthy source. The no-output timeout must be marked inconclusive (so Pool
// does not penalize health) while still taking the alternate-source path.
func TestTranscodeShortFragmentThenStallTriesAlternate(t *testing.T) {
	sourceOneCanceled := make(chan struct{})
	var canceled atomic.Bool
	sourceOne := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		w.WriteHeader(http.StatusOK)
		// Exceed the minimum byte threshold deliberately: staleness and lack
		// of a sustained multi-write streak, not volume alone, must keep this
		// fragment from being blamed on the local transcoder.
		_, _ = w.Write(make([]byte, 96*1024))
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		<-r.Context().Done()
		if canceled.CompareAndSwap(false, true) {
			close(sourceOneCanceled)
		}
	}))
	defer sourceOne.Close()
	sourceTwo := markerStreamServer(t, 0xbc)
	defer sourceTwo.Close()

	bufferUntilUseful := stubBinary(t,
		`exec python3 -c 'import sys; d=sys.stdin.buffer.read(131072); sys.stdout.buffer.write(d) if len(d) == 131072 else None'`)
	s := NewTranscodeStreamer("fragment-stall-failover", sourceOne.URL,
		stubProfile(), bufferUntilUseful, testLogger(), nil)
	s.sourceStartupTimeout = 650 * time.Millisecond
	s.initialStartupDeadline = time.Now().Add(3 * time.Second)
	var failovers atomic.Int32
	s.OnUpstreamDown = func(_ context.Context, cause error) (string, bool, error) {
		if !errors.Is(cause, errUncertainTranscodeInput) {
			t.Errorf("fragment-stall cause=%v, want inconclusive input evidence", cause)
		}
		failovers.Add(1)
		return sourceTwo.URL, true, nil
	}

	chunks, unsubscribe := s.Subscribe("viewer")
	defer unsubscribe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	started := time.Now()
	go func() {
		s.Run(ctx)
		close(done)
	}()

	select {
	case chunk, ok := <-chunks:
		if !ok {
			t.Fatal("transcode subscriber closed before alternate output")
		}
		if len(chunk) == 0 {
			t.Fatal("alternate emitted an empty output chunk")
		}
		if chunk[0] != 0xbc {
			t.Fatalf("alternate output marker=%x, want source-two media", chunk[0])
		}
	case <-time.After(2500 * time.Millisecond):
		t.Fatal("short fragment did not relocate to alternate inside Plex margin")
	}
	if elapsed := time.Since(started); elapsed >= 2500*time.Millisecond {
		t.Fatalf("fragment-stall failover took %v, want <2.5s", elapsed)
	}
	if got := failovers.Load(); got != 1 {
		t.Fatalf("failovers=%d, want 1", got)
	}
	select {
	case <-sourceOneCanceled:
	case <-time.After(time.Second):
		t.Fatal("fragment source request remained open after relocation")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("transcode streamer did not exit after cancellation")
	}
}

func TestTranscodeShortFragmentEOFTriesAlternateWithoutSourceBlame(t *testing.T) {
	sourceOne := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(make([]byte, 96*1024))
	}))
	defer sourceOne.Close()
	sourceTwo := markerStreamServer(t, 0xbc)
	defer sourceTwo.Close()

	bufferUntilUseful := stubBinary(t,
		`exec python3 -c 'import sys; d=sys.stdin.buffer.read(131072); sys.stdout.buffer.write(d) if len(d) == 131072 else None'`)
	s := NewTranscodeStreamer("fragment-eof-failover", sourceOne.URL,
		stubProfile(), bufferUntilUseful, testLogger(), nil)
	s.sourceStartupTimeout = time.Second
	s.initialStartupDeadline = time.Now().Add(3 * time.Second)
	var failovers atomic.Int32
	s.OnUpstreamDown = func(_ context.Context, cause error) (string, bool, error) {
		if !errors.Is(cause, errUncertainTranscodeInput) {
			t.Errorf("fragment-EOF cause=%v, want inconclusive input evidence", cause)
		}
		failovers.Add(1)
		return sourceTwo.URL, true, nil
	}

	chunks, unsubscribe := s.Subscribe("viewer")
	defer unsubscribe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		s.Run(ctx)
		close(done)
	}()

	select {
	case chunk, ok := <-chunks:
		if !ok || len(chunk) == 0 {
			t.Fatal("transcode subscriber closed before alternate output")
		}
		if chunk[0] != 0xbc {
			t.Fatalf("alternate output marker=%x, want source-two media", chunk[0])
		}
	case <-time.After(2500 * time.Millisecond):
		t.Fatal("short EOF fragment did not relocate inside Plex margin")
	}
	if got := failovers.Load(); got != 1 {
		t.Fatalf("failovers=%d, want 1", got)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("transcode streamer did not exit after cancellation")
	}
}

// The Hallmark incident exercised the transcode path: the provider returned
// HTTP 200, emitted a short fragment, then EOFed on every reconnect. Those
// fragments must not reset the continuous-outage window indefinitely.
func TestTranscodeFragmentedEOFDoesNotResetReconnectWindow(t *testing.T) {
	srv, requests := fragmentEOFServer(t, 20*time.Millisecond)
	s := NewTranscodeStreamer("test", srv.URL, stubProfile(),
		stubBinary(t, "exec cat"), testLogger(), nil)
	s.stableDuration = 150 * time.Millisecond
	s.reconnectWindow = 900 * time.Millisecond
	s.OnUpstreamDown = func(_ context.Context, _ error) (string, bool, error) {
		return "", true, nil
	}
	var stable atomic.Int32
	s.OnStable = func() { stable.Add(1) }

	viewer, unsub := s.Subscribe("viewer")
	defer unsub()
	go func() {
		for range viewer {
		}
	}()

	var exitErr error
	exited := make(chan struct{})
	s.OnExit = func(err error) {
		exitErr = err
		close(exited)
	}
	go s.Run(context.Background())

	select {
	case <-exited:
	case <-time.After(6 * time.Second):
		t.Fatal("transcode fragments reset the reconnect window")
	}
	if exitErr == nil {
		t.Fatal("OnExit err = nil, want continuous-outage give-up error")
	}
	if !errors.Is(s.TerminalError(), exitErr) {
		t.Fatalf("TerminalError = %v, want %v", s.TerminalError(), exitErr)
	}
	if got := requests.Load(); got < 2 {
		t.Fatalf("requests = %d, want multiple reconnect attempts", got)
	} else if epoch := s.MediaEpoch(); epoch < uint64(got) {
		t.Fatalf("media epoch=%d for %d fragment attempts; ring was not invalidated per attempt",
			epoch, got)
	}
	if got := stable.Load(); got != 0 {
		t.Fatalf("OnStable fired %d times for sub-threshold fragments", got)
	}
}

// The transcode provider-read watchdog evaluates its deadline on every tick,
// so streamer-lifetime readiness leaking into a reconnect attempt showed here
// most directly: a replacement source that trickles sub-prefix bytes and then
// goes silent must still die at the startup timeout (review finding).
func TestTranscodeReconnectAttemptKeepsStartupIdleTimeout(t *testing.T) {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
		audioPID = 0x0101
	)
	prefix, _, _, _ := healthyAVTranscodePrefix(
		pmtPID, videoPID, audioPID, minPrefixDecisionBytes+2*tsPacketSize)
	hang := make(chan struct{})
	defer close(hang)
	// Flush-and-hold keeps both responses on the LIVE path; a write-and-close
	// body is finite and routes through the classifier instead.
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(prefix)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		select {
		case <-hang:
		case <-r.Context().Done():
		}
	}))
	defer healthy.Close()
	silent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(make([]byte, 16))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		select {
		case <-hang:
		case <-r.Context().Done():
		}
	}))
	defer silent.Close()

	s := NewTranscodeStreamer("transcode-reconnect-startup", healthy.URL, stubProfile(),
		stubBinary(t, "exec cat"), testLogger(), nil)
	s.prefixValidator = &thresholdPrefixValidator{readyAt: minPrefixDecisionBytes}
	s.sourceStartupTimeout = 2 * time.Second
	defer drainTranscodeTestSubscriber(s, "transcode-reconnect-startup")()

	// Attempt 1 proves media, then ends via its own watchdog; equal phase
	// timeouts make its ending independent of the fix under test. Only the
	// reconnect attempt runs with the tight/loose pair.
	s.idleReadTimeout = 800 * time.Millisecond
	s.stallTolerance = 800 * time.Millisecond
	if gotBytes, upstreamFault, ferr := s.runOnce(context.Background()); !gotBytes {
		t.Fatalf("first attempt delivered no bytes (upstream=%v err=%v)", upstreamFault, ferr)
	}
	if !s.MediaReady() {
		t.Fatal("streamer-lifetime readiness not established by the first attempt")
	}

	s.idleReadTimeout = 150 * time.Millisecond
	s.stallTolerance = 10 * time.Second
	s.upstream = silent.URL
	started := time.Now()
	_, _, err := s.runOnce(context.Background())
	elapsed := time.Since(started)
	if !errors.Is(err, errUpstreamIdle) {
		t.Fatalf("silent reconnect result=%v after %s, want %v", err, elapsed, errUpstreamIdle)
	}
	if elapsed >= 1500*time.Millisecond {
		t.Fatalf("silent reconnect detected in %s — the live tolerance leaked into a startup phase", elapsed)
	}
}

// Transcode twin of TestStreamerStartupLeadHoldSurvivesOriginPause: the
// provider read watchdog polls attemptIdleTimeout, which must already be the
// live tolerance while the startup hold runs. This is the exact shape of the
// 2026-09-02 transcode-start failures (attempt bytes == the connect lead, a
// provider read in flight for idleReadTimeout, result canceled).
func TestTranscodeStartupLeadHoldSurvivesOriginPause(t *testing.T) {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
		audioPID = 0x0101
	)
	prefix, pts, videoCC, audioCC := healthyAVTranscodePrefix(
		pmtPID, videoPID, audioPID, minPrefixDecisionBytes+2*tsPacketSize)
	tail := joinTSPackets(
		testTimestampedPESPacket(videoPID, 0xe0, pts, videoCC),
		testTimestampedPESPacket(audioPID, 0xc0, pts, audioCC))
	const startupIdle = 200 * time.Millisecond
	const pause = 3 * startupIdle
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(prefix)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		select {
		case <-time.After(pause):
		case <-r.Context().Done():
			return
		}
		_, _ = w.Write(tail)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	}))
	defer srv.Close()

	s := NewTranscodeStreamer("hold-origin-pause", srv.URL, stubProfile(),
		stubBinary(t, "exec cat"), testLogger(), nil)
	s.prefixValidator = &thresholdPrefixValidator{readyAt: minPrefixDecisionBytes}
	s.sourceStartupTimeout = 4 * time.Second
	s.idleReadTimeout = startupIdle
	s.stallTolerance = 5 * time.Second
	s.outputProgressTimeout = 5 * time.Second
	s.startupLead = 2 * startupIdle
	defer drainTranscodeTestSubscriber(s, "hold-origin-pause")()

	gotBytes, _, err := s.runOnce(context.Background())
	if !gotBytes {
		t.Fatalf("attempt delivered no bytes: %v", err)
	}
	if errors.Is(err, errUpstreamIdle) {
		t.Fatalf("provider read watchdog killed the origin's burst pause during the startup hold: %v", err)
	}
}
