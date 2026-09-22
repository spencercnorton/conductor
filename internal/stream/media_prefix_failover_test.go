package stream

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

type prefixFailoverObserver struct {
	inner           mediaPrefixValidator
	videoPID        int
	invalidBytes    atomic.Int64
	invalidAccepted atomic.Bool
}

func (v *prefixFailoverObserver) Validate(ctx context.Context, data []byte) (prefixDecision, error) {
	decision, err := v.inner.Validate(ctx, data)
	if _, isTS := findTSSyncOffset(data); isTS && !hasTSPID(data, v.videoPID) {
		v.invalidBytes.Store(int64(len(data)))
		if decision.kind == prefixReady {
			v.invalidAccepted.Store(true)
		}
	}
	return decision, err
}

// A finite HTTP 200 can carry structurally recognizable MPEG-TS while still
// containing no decodable video. Its private prefix must not reach Plex, and
// a PAT/PMT/config/keyframe-capable alternate must become visible with clear
// margin inside Plex's observed ten-second tuner deadline.
func TestUndecodableTSPrefixRelocatesWithoutLeakingBeforePlexDeadline(t *testing.T) {
	ffmpeg, err := exec.LookPath("ffmpeg")
	if err != nil {
		if os.Getenv("CI") != "" {
			t.Fatal("ffmpeg missing from supported CI image")
		}
		t.Skip("ffmpeg not on PATH")
	}
	ffprobe, err := exec.LookPath("ffprobe")
	if err != nil {
		if os.Getenv("CI") != "" {
			t.Fatal("ffprobe missing from supported CI image")
		}
		t.Skip("ffprobe not on PATH")
	}

	cmd := exec.Command(ffmpeg,
		"-hide_banner", "-loglevel", "error", "-nostdin",
		"-f", "lavfi", "-i", "testsrc2=size=640x360:rate=30:duration=4",
		"-f", "lavfi", "-i", "sine=sample_rate=48000:duration=4",
		"-c:v", "libx264", "-preset", "ultrafast", "-tune", "zerolatency",
		"-g", "30", "-keyint_min", "30", "-sc_threshold", "0",
		"-b:v", "2M", "-maxrate", "2M", "-bufsize", "4M",
		"-c:a", "aac", "-f", "mpegts", "pipe:1")
	validMedia, err := cmd.Output()
	if err != nil {
		t.Fatalf("generate alternate MPEG-TS fixture: %v", err)
	}
	videoPID, ok := videoPIDFromTS(validMedia)
	if !ok {
		t.Fatal("generated fixture has no parseable video PID")
	}

	undecodable := append([]byte(nil), validMedia...)
	removed := replaceTSPID(undecodable, videoPID, 0x1fff)
	if removed == 0 {
		t.Fatalf("generated fixture contains no packets for video PID %#x", videoPID)
	}
	const invalidTargetBytes = 512 * 1024
	if len(undecodable) < invalidTargetBytes {
		t.Fatalf("generated fixture too small: got=%d want>=%d", len(undecodable), invalidTargetBytes)
	}
	invalidLength := invalidTargetBytes - invalidTargetBytes%tsPacketSize
	undecodable = undecodable[:invalidLength]
	if _, isTS := findTSSyncOffset(undecodable); !isTS {
		t.Fatal("video-stripped source one is not recognized as MPEG-TS")
	}

	realValidator := &ffprobeMediaPrefixValidator{ffprobeBinary: ffprobe, available: true}
	probeCtx, probeCancel := context.WithTimeout(context.Background(), 2*defaultSourceStartupTimeout)
	defer probeCancel()
	invalidDecision, err := realValidator.Validate(probeCtx, undecodable)
	if err != nil {
		t.Fatalf("validate source-one fixture: %v", err)
	}
	if invalidDecision.kind == prefixReady {
		t.Fatalf("video-stripped MPEG-TS was accepted: reason=%s", invalidDecision.reason)
	}
	validDecision, err := realValidator.Validate(probeCtx, validMedia)
	if err != nil {
		t.Fatalf("validate source-two fixture: %v", err)
	}
	if validDecision.kind != prefixReady {
		t.Fatalf("alternate prefix decision=%v reason=%s, want ready", validDecision.kind, validDecision.reason)
	}

	var sourceOneRequests atomic.Int32
	sourceOne := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sourceOneRequests.Add(1)
		w.Header().Set("Content-Type", "video/mp2t")
		w.Header().Set("Content-Length", strconv.Itoa(len(undecodable)))
		w.WriteHeader(http.StatusOK)
		writePacedTSPrefix(w, r, undecodable, 90*time.Millisecond)
	}))
	defer sourceOne.Close()

	var sourceTwoRequests atomic.Int32
	sourceTwoRequested := make(chan struct{}, 1)
	releaseSourceTwo := make(chan struct{})
	sourceTwo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sourceTwoRequests.Add(1)
		select {
		case sourceTwoRequested <- struct{}{}:
		default:
		}
		select {
		case <-releaseSourceTwo:
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Content-Type", "video/mp2t")
		w.WriteHeader(http.StatusOK)
		writePacedTSPrefix(w, r, validMedia, 90*time.Millisecond)
	}))
	defer sourceTwo.Close()

	observer := &prefixFailoverObserver{inner: realValidator, videoPID: videoPID}
	s := NewStreamer("prefix-failover", sourceOne.URL, testLogger(), nil)
	s.prefixValidator = observer
	s.sourceStartupTimeout = defaultSourceStartupTimeout
	s.initialStartupDeadline = time.Now().Add(ClientStartupBudget)
	var running atomic.Int32
	var failovers atomic.Int32
	failoverCause := make(chan error, 1)
	s.OnRunning = func() { running.Add(1) }
	s.OnUpstreamDown = func(_ context.Context, cause error) (string, bool, error) {
		if failovers.Add(1) == 1 {
			failoverCause <- cause
			return sourceTwo.URL, true, nil
		}
		return "", false, nil
	}

	chunks, unsubscribe := s.Subscribe("plex-viewer")
	defer unsubscribe()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	started := time.Now()
	go func() {
		s.Run(ctx)
		close(done)
	}()

	select {
	case <-sourceTwoRequested:
	case <-time.After(4 * time.Second):
		cancel()
		t.Fatal("recognized but undecodable source did not relocate promptly")
	}
	if got := observer.invalidBytes.Load(); got < minTSProbeBytes {
		cancel()
		t.Fatalf("source-one validator inspected only %d bytes, want >=%d", got, minTSProbeBytes)
	}
	if observer.invalidAccepted.Load() {
		cancel()
		t.Fatal("source-one undecodable prefix was accepted during startup")
	}
	select {
	case chunk, ok := <-chunks:
		cancel()
		if !ok {
			t.Fatal("subscriber closed before alternate request")
		}
		t.Fatalf("source-one prefix leaked %d bytes before source two was released", len(chunk))
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseSourceTwo)

	var first []byte
	select {
	case chunk, ok := <-chunks:
		if !ok {
			cancel()
			t.Fatal("subscriber closed before alternate prefix became ready")
		}
		first = chunk
	case <-time.After(6 * time.Second):
		cancel()
		t.Fatal("valid alternate missed bounded Plex-compatible startup deadline")
	}
	elapsed := time.Since(started)
	if elapsed >= 6*time.Second {
		cancel()
		t.Fatalf("alternate startup took %v, want <6s (<10s Plex deadline)", elapsed)
	}
	if len(first) < tsPacketSize || first[0] != 0x47 {
		cancel()
		t.Fatalf("first published alternate chunk is not MPEG-TS: %x", first[:min(len(first), 8)])
	}
	if pid := int(first[1]&0x1f)<<8 | int(first[2]); pid != 0 {
		cancel()
		t.Fatalf("first published alternate packet PID=%#x, want PAT PID 0", pid)
	}
	if !s.MediaReady() {
		cancel()
		t.Fatal("MediaReady false after validated alternate prefix")
	}
	if sourceOneRequests.Load() != 1 || sourceTwoRequests.Load() != 1 || failovers.Load() != 1 {
		cancel()
		t.Fatalf("requests/failovers source1=%d source2=%d failovers=%d, want 1/1/1",
			sourceOneRequests.Load(), sourceTwoRequests.Load(), failovers.Load())
	}
	if running.Load() != 1 {
		cancel()
		t.Fatalf("OnRunning calls=%d, want only the validated alternate", running.Load())
	}
	select {
	case cause := <-failoverCause:
		if !errors.Is(cause, io.ErrUnexpectedEOF) {
			cancel()
			t.Fatalf("source-one relocation cause=%v, want finite unexpected EOF", cause)
		}
	default:
		cancel()
		t.Fatal("source-one relocation cause was not recorded")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("streamer did not exit after cancellation")
	}
}

func writePacedTSPrefix(w http.ResponseWriter, r *http.Request, data []byte, interval time.Duration) {
	flusher, _ := w.(http.Flusher)
	for offset := 0; offset < len(data); {
		end := min(offset+chunkSize, len(data))
		if _, err := w.Write(data[offset:end]); err != nil {
			return
		}
		if flusher != nil {
			flusher.Flush()
		}
		offset = end
		if offset == len(data) {
			return
		}
		timer := time.NewTimer(interval)
		select {
		case <-timer.C:
		case <-r.Context().Done():
			timer.Stop()
			return
		}
	}
}

func hasTSPID(data []byte, want int) bool {
	syncOffset, ok := findTSSyncOffset(data)
	if !ok {
		return false
	}
	for pos := syncOffset; pos+tsPacketSize <= len(data); pos += tsPacketSize {
		packet := data[pos : pos+tsPacketSize]
		pid := int(packet[1]&0x1f)<<8 | int(packet[2])
		if pid == want {
			return true
		}
	}
	return false
}

func replaceTSPID(data []byte, oldPID, newPID int) int {
	syncOffset, ok := findTSSyncOffset(data)
	if !ok {
		return 0
	}
	replaced := 0
	for pos := syncOffset; pos+tsPacketSize <= len(data); pos += tsPacketSize {
		packet := data[pos : pos+tsPacketSize]
		pid := int(packet[1]&0x1f)<<8 | int(packet[2])
		if pid != oldPID {
			continue
		}
		packet[1] = (packet[1] & 0xe0) | byte((newPID>>8)&0x1f)
		packet[2] = byte(newPID)
		replaced++
	}
	return replaced
}
