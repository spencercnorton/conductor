package stream

import (
	"bytes"
	"errors"
	"sync"
	"time"

	"github.com/spencercnorton/conductor/internal/metrics"
	"github.com/spencercnorton/conductor/internal/transcode"
)

const (
	transportPTSRate           = uint64(90_000)
	maxAVTimelineAudioPIDs     = 8
	maxTransportPTSForwardStep = 30 * time.Second
	maxTransportPTSReorder     = 3 * time.Second
	// A normal encoder lead/lag still uses the sustained continuity policy.
	// Incident-scale offsets are different: one sane sample per track proves a
	// 30-second fixed skew, while two prove a 30-second slope divergence. Once
	// either exists inside an unpublished slice, releasing even the first packet
	// can poison Plex's queues for minutes.
	immediateGrossAVOffset          = 30 * time.Second
	immediateGrossAVDriftMinSamples = uint64(2)
)

var errAVContinuity = errors.New("audio/video transport continuity failed")

type avContinuityFaultKind string

const (
	avFaultAudioStarvation avContinuityFaultKind = "audio_starvation"
	avFaultVideoStarvation avContinuityFaultKind = "video_starvation"
	avFaultMediaStarvation avContinuityFaultKind = "av_clock_starvation"
	avFaultTimelineDrift   avContinuityFaultKind = "timeline_drift"
	avFaultTimelineSkew    avContinuityFaultKind = "timeline_skew"
	avFaultTransport       avContinuityFaultKind = "transport_corruption"
)

// avContinuityPolicy is deliberately conservative: ordinary silence still
// carries timestamped audio frames, so the starvation guard keys on missing
// PES clock activity rather than decoded loudness. A fault also has to remain
// observable across a confirmation window. This keeps one bursty mux read or
// normal audio/video packet interleave from forcing a source change.
type avContinuityPolicy struct {
	starvationThreshold time.Duration
	driftThreshold      time.Duration
	skewThreshold       time.Duration
	confirmation        time.Duration
	minSamples          uint64
}

func defaultAVContinuityPolicy() avContinuityPolicy {
	// Starvation must clear the origin's real delivery rhythm, not an ideal
	// one. Bursty cable re-encodes pause 3-14s between bursts (measured
	// p99=11s, max=14s over 20 minutes) while averaging healthy realtime, and
	// on resume the first burst chunk refreshes the video clock one or two
	// chunks before the sparser audio PES cadence (~215ms on those muxes)
	// lands. A 2s threshold plus a backdated onset therefore declared a
	// one-sided starvation on every resume. 18s clears the worst measured
	// pause end-to-end; a genuinely dead track still fails over, just after
	// the tolerance a viewer keeps their session through instead of before.
	return avContinuityPolicy{
		starvationThreshold: 18 * time.Second,
		driftThreshold:      time.Second,
		skewThreshold:       2 * time.Second,
		confirmation:        2 * time.Second,
		minSamples:          4,
	}
}

type avContinuityError struct {
	kind     avContinuityFaultKind
	wallSkew time.Duration
	drift    time.Duration
	skew     time.Duration
}

func (e *avContinuityError) Error() string {
	return "audio/video transport continuity failed: " + string(e.kind)
}

func (e *avContinuityError) Unwrap() error { return errAVContinuity }

type ptsTimelineTrack struct {
	have          bool
	lastRaw       uint64
	lastUnwrapped int64
	minUnwrapped  int64
	maxUnwrapped  int64
	minRaw        uint64
	maxRaw        uint64
	samples       uint64
	firstObserved time.Time
	lastObserved  time.Time
	recoveryHave  bool
	recoveryRaw   uint64
	recoveryAt    time.Time
	stepHave      bool
	minStepTicks  int64
	maxStepTicks  int64
}

func (t *ptsTimelineTrack) reset() {
	*t = ptsTimelineTrack{}
}

// fence discards the comparison extrema after corrupt transport while keeping
// the last independently sane clock as a recovery anchor. A one-packet PTS
// mutation can then heal when the prior epoch resumes, while a sustained
// undeclared new epoch remains corrupt until an explicit discontinuity opens a
// fresh attempt/epoch.
func (t *ptsTimelineTrack) fence() {
	recoveryHave, recoveryRaw, recoveryAt := t.recoveryHave, t.recoveryRaw, t.recoveryAt
	if t.have {
		recoveryHave, recoveryRaw, recoveryAt = true, t.lastRaw, t.lastObserved
	}
	*t = ptsTimelineTrack{recoveryHave: recoveryHave, recoveryRaw: recoveryRaw, recoveryAt: recoveryAt}
}

func (t *ptsTimelineTrack) record(raw uint64) {
	_ = t.recordAt(raw, time.Now())
}

// recordAt validates a nearest-wrap transport-order step before mutating any
// timestamp, extrema, or progress clock. It permits bounded B-frame reorder
// and ordinary PES cadence, including the incident-scale synthetic spans used
// by diagnostics, but rejects an hours-away valid-marker PTS as corruption.
func (t *ptsTimelineTrack) recordAt(raw uint64, observedAt time.Time) bool {
	raw %= ptsModulus
	if observedAt.IsZero() {
		observedAt = time.Now()
	}
	if !t.have {
		if t.recoveryHave && !plausibleTransportPTSStep(t.recoveryRaw, raw) {
			// An anchor whose clock has been silent longer than the maximum
			// plausible forward step can no longer validate anything: any
			// legitimate wake PTS is beyond it by construction. Measured on a
			// TLC-class mux, a declared track sleeps for minutes and then
			// resumes; punishing that resume as corruption re-fenced the track
			// forever. Establish fresh instead.
			if t.recoveryAt.IsZero() ||
				observedAt.Sub(t.recoveryAt) <= maxTransportPTSForwardStep {
				return false
			}
		}
		t.have = true
		t.recoveryHave = false
		t.lastRaw = raw
		t.lastUnwrapped = int64(raw)
		t.minUnwrapped = int64(raw)
		t.maxUnwrapped = int64(raw)
		t.minRaw = raw
		t.maxRaw = raw
		t.samples = 1
		t.firstObserved = observedAt
		t.lastObserved = observedAt
		return true
	}
	// PES packets arrive in decode/transport order, so B-frames can carry a
	// PTS slightly behind the preceding packet. Unwrap each step to its nearest
	// signed 33-bit epoch, then measure the presentation extrema instead of
	// interpreting ordinary reordering as a discontinuity. Only an explicit
	// transport discontinuity or PID change resets the diagnostic epoch.
	delta := nearestPTSDelta(t.lastRaw, raw)
	if !plausibleTransportPTSDelta(delta) {
		return false
	}
	if !t.stepHave {
		t.stepHave = true
		t.minStepTicks = delta
		t.maxStepTicks = delta
	} else {
		t.minStepTicks = min(t.minStepTicks, delta)
		t.maxStepTicks = max(t.maxStepTicks, delta)
	}
	unwrapped := t.lastUnwrapped + delta
	t.lastRaw = raw
	t.lastUnwrapped = unwrapped
	t.lastObserved = observedAt
	if unwrapped < t.minUnwrapped {
		t.minUnwrapped = unwrapped
		t.minRaw = raw
	}
	if unwrapped > t.maxUnwrapped {
		t.maxUnwrapped = unwrapped
		t.maxRaw = raw
	}
	t.samples++
	return true
}

func nearestPTSDelta(previous, current uint64) int64 {
	delta := int64((current + ptsModulus - previous) % ptsModulus)
	if delta > int64(ptsModulus/2) {
		delta -= int64(ptsModulus)
	}
	return delta
}

func plausibleTransportPTSStep(previous, current uint64) bool {
	return plausibleTransportPTSDelta(nearestPTSDelta(previous, current))
}

func plausibleTransportPTSDelta(delta int64) bool {
	minimum := -int64(durationTicks(maxTransportPTSReorder, transportPTSRate))
	maximum := int64(durationTicks(maxTransportPTSForwardStep, transportPTSRate))
	return delta >= minimum && delta <= maximum
}

func (t *ptsTimelineTrack) spanTicks() int64 {
	return t.maxUnwrapped - t.minUnwrapped
}

type avTimelineSnapshot struct {
	valid                 bool
	videoSamples          uint64
	audioSamples          uint64
	audioTracks           int
	continuityEnforced    bool
	continuityReason      string
	transportCorruptions  uint64
	initialSkew           time.Duration
	finalSkew             time.Duration
	drift                 time.Duration
	videoLastGap          time.Duration
	audioLastGap          time.Duration
	audioOriginCorrection time.Duration
	sharedBoundaries      uint64
}

// avProgressComparison compares clocks over jointly observed media. The first
// sample of one track can precede the other's by an ordinary mux/startup lead;
// those unequal initial spans are not evidence of a continuing clock slope.
// The origin is immutable until one of the compared tracks is reset/fenced.
type avProgressComparison struct {
	have                       bool
	videoOrigin, audioOrigin   int64
	videoEnd, audioEnd         int64
	videoSamples, audioSamples uint64
}

type tsPayloadContinuity struct {
	have       bool
	lastCC     byte
	lastPacket [tsPacketSize]byte
}

type pesPTSParseStatus uint8

const (
	pesPTSIncomplete pesPTSParseStatus = iota
	pesPTSAbsent
	pesPTSPresent
	pesPTSMalformed
)

// pesPTSAssembler retains only the bounded MPEG-2 PES optional-header prefix
// needed for PTS/DTS validation. A legitimate header may straddle TS packets;
// incomplete first-packet views are neither progress nor corruption.
type pesPTSAssembler struct {
	// MPEG-2 header_data_length is one byte, so the complete optional header is
	// bounded by its nine-byte prefix plus 255 declared bytes. Retain one
	// elementary-stream byte beyond that header as well: a timestamp in a PES
	// containing no media is not decoder progress and must not establish an A/V
	// publication epoch.
	buffer [9 + 255 + 1]byte
	count  int
	active bool
}

func (a *pesPTSAssembler) reset() { *a = pesPTSAssembler{} }

func (a *pesPTSAssembler) feed(payload []byte, start bool) (uint64, pesPTSParseStatus) {
	supersededIncomplete := start && a.active
	if start {
		a.reset()
		a.active = true
	}
	if !a.active {
		return 0, pesPTSAbsent
	}
	if remaining := len(a.buffer) - a.count; remaining > 0 {
		take := min(remaining, len(payload))
		copy(a.buffer[a.count:], payload[:take])
		a.count += take
	}
	pts, status := parsePESPTSStatus(a.buffer[:a.count])
	if status != pesPTSIncomplete {
		a.active = false
	}
	if supersededIncomplete {
		// A new selected PES start cannot make a previously published truncated
		// optional header safe. Keep any incomplete replacement assembled for
		// diagnostics, but fence the exact feed so neither header reaches Plex or
		// FFmpeg. Recovery begins only in a fresh private attempt/epoch.
		return 0, pesPTSMalformed
	}
	return pts, status
}

func (c *tsPayloadContinuity) reset() { *c = tsPayloadContinuity{} }

// accept validates payload continuity before a packet can contribute a PTS.
// An identical same-counter retransmission is legal, but is not new media
// progress. A same-counter mutation or skipped counter is corruption and must
// neither refresh lastObserved nor poison presentation extrema.
func (c *tsPayloadContinuity) accept(packet []byte) (accepted, duplicate, corrupt bool) {
	adaptationControl := (packet[3] >> 4) & 0x03
	if adaptationControl != 1 && adaptationControl != 3 {
		return false, false, false
	}
	payload, ok := tsPayload(packet)
	if !ok || len(payload) == 0 {
		return false, false, false
	}
	cc := packet[3] & 0x0f
	if !c.have {
		c.have = true
		c.lastCC = cc
		copy(c.lastPacket[:], packet)
		return true, false, false
	}
	if cc == c.lastCC {
		if bytes.Equal(c.lastPacket[:], packet) {
			return false, true, false
		}
		return false, false, true
	}
	if cc != (c.lastCC+1)&0x0f {
		return false, false, true
	}
	c.lastCC = cc
	copy(c.lastPacket[:], packet)
	return true, false, false
}

type avProgramPIDs struct {
	videoPID    int
	pcrPID      int
	audioPIDs   [maxAVTimelineAudioPIDs]int
	audioCount  int
	mapIdentity selectedProgramMapIdentity
}

func (p avProgramPIDs) firstAudioPID() int {
	if p.audioCount == 0 {
		return -1
	}
	return p.audioPIDs[0]
}

func (p avProgramPIDs) equal(other avProgramPIDs) bool {
	if p.videoPID != other.videoPID || p.pcrPID != other.pcrPID || p.audioCount != other.audioCount {
		return false
	}
	for i := 0; i < min(p.audioCount, maxAVTimelineAudioPIDs); i++ {
		if p.audioPIDs[i] != other.audioPIDs[i] {
			return false
		}
	}
	return true
}

// transportAVTimeline is a bounded, allocation-reusing MPEG-TS diagnostic and
// media-progress guard. It never decodes pictures or sound: dark, still, and
// silent programme material remains healthy while its PES clocks advance.
// Unsupported split/multi-section maps fail open with an explicit diagnostic
// reason instead of binding enforcement to a guessed or stale PID map.
type transportAVTimeline struct {
	mu                         sync.Mutex
	initialized                bool
	buffer                     []byte
	synced                     bool
	pmtPID                     int
	videoPID                   int
	audioPID                   int // first mapped track, retained for diagnostics
	pcrPID                     int
	audioPIDs                  [maxAVTimelineAudioPIDs]int
	audioTrackCount            int
	requireFirstAudio          bool
	firstAudioOriginCorrection time.Duration
	fixedProgram               bool
	continuityReason           string
	epochStarted               time.Time
	video                      ptsTimelineTrack
	audio                      [maxAVTimelineAudioPIDs]ptsTimelineTrack
	audioProgress              [maxAVTimelineAudioPIDs]avProgressComparison
	videoCC                    tsPayloadContinuity
	audioCC                    [maxAVTimelineAudioPIDs]tsPayloadContinuity
	videoPES                   pesPTSAssembler
	audioPES                   [maxAVTimelineAudioPIDs]pesPTSAssembler
	selectedDiscontinuitySeen  [0x2000]bool
	boundaryGathering          bool
	faultKind                  avContinuityFaultKind
	faultSince                 time.Time
	transportFaultSince        time.Time
	transportCorruptions       uint64
	unsafeCurrentFeed          bool
	unsafeCurrentAVFault       *avContinuityError
	sharedBoundaries           uint64
}

func (m *transportAVTimeline) resetTracks() {
	m.audioProgress = [maxAVTimelineAudioPIDs]avProgressComparison{}
	m.video.reset()
	m.videoCC.reset()
	m.videoPES.reset()
	for i := range m.audio {
		m.audio[i].reset()
		m.audioCC[i].reset()
		m.audioPES[i].reset()
	}
	m.faultKind = ""
	m.faultSince = time.Time{}
}

func (m *transportAVTimeline) fenceTracks() {
	m.audioProgress = [maxAVTimelineAudioPIDs]avProgressComparison{}
	m.video.fence()
	m.videoCC.reset()
	m.videoPES.reset()
	for i := range m.audio {
		m.audio[i].fence()
		m.audioCC[i].reset()
		m.audioPES[i].reset()
	}
	m.faultKind = ""
	m.faultSince = time.Time{}
}

func (m *transportAVTimeline) beginEpoch(observedAt time.Time) {
	m.resetTracks()
	m.epochStarted = observedAt
	m.transportFaultSince = time.Time{}
}

func (m *transportAVTimeline) clearSelectedProgram(reason string, observedAt time.Time) {
	m.beginEpoch(observedAt)
	m.selectedDiscontinuitySeen = [0x2000]bool{}
	m.boundaryGathering = false
	m.videoPID = -1
	m.audioPID = -1
	m.pcrPID = -1
	m.audioTrackCount = 0
	for i := range m.audioPIDs {
		m.audioPIDs[i] = -1
	}
	m.continuityReason = reason
}

func (m *transportAVTimeline) noteTransportCorruption(observedAt time.Time) {
	m.unsafeCurrentFeed = true
	since := m.transportFaultSince
	if since.IsZero() {
		since = observedAt
	}
	m.transportCorruptions++
	// Fence the shared comparison epoch, but keep its establishment clock and
	// the first corruption time. Repeated malformed packets therefore cannot
	// indefinitely defer the declared-audio progress guard.
	m.fenceTracks()
	m.transportFaultSince = since
}

func (m *transportAVTimeline) noteAudioTransportCorruption(index int, observedAt time.Time) {
	if index < 0 || index >= min(m.audioTrackCount, maxAVTimelineAudioPIDs) {
		return
	}
	// Plex passthrough can select any declared language, so corruption on every
	// audio PID is publication-unsafe. FFmpeg explicitly maps only 0:a:0;
	// damage on an unmapped secondary track remains telemetry/fencing but must
	// not poison or relocate the selected transcode pipeline.
	selectedPipeline := !m.requireFirstAudio || index == 0
	if selectedPipeline {
		m.unsafeCurrentFeed = true
	}
	if m.audioTrackCount == 1 || (m.requireFirstAudio && index == 0) {
		m.noteTransportCorruption(observedAt)
		return
	}
	// Passthrough damage was already marked publication-unsafe above. For an
	// unmapped transcode commentary/language track, fence only that track and
	// expose telemetry; FFmpeg's selected 0:a:0 pipeline remains eligible.
	m.transportCorruptions++
	m.audio[index].fence()
	m.audioProgress[index] = avProgressComparison{}
	m.audioCC[index].reset()
	m.audioPES[index].reset()
}

func (m *transportAVTimeline) selectedAudioIndex(pid int) int {
	for i := 0; i < min(m.audioTrackCount, maxAVTimelineAudioPIDs); i++ {
		if m.audioPIDs[i] == pid {
			return i
		}
	}
	return -1
}

// bindSelectedProgram gives an exact pre-publication inspector the same
// programme identity proved by the startup gate. Diagnostic timelines may
// still discover maps at the raw edge, but safety decisions never guess the
// first PAT programme or wait for a later repetition.
func (m *transportAVTimeline) bindSelectedProgram(program avProgramPIDs, observedAt time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if observedAt.IsZero() {
		observedAt = time.Now()
	}
	m.initialized = true
	m.fixedProgram = true
	m.beginEpoch(observedAt)
	m.selectedDiscontinuitySeen = [0x2000]bool{}
	m.boundaryGathering = false
	m.videoPID = program.videoPID
	m.pcrPID = program.pcrPID
	m.audioPIDs = program.audioPIDs
	m.audioTrackCount = program.audioCount
	m.audioPID = program.firstAudioPID()
	m.pmtPID = program.mapIdentity.pmtPID
	if !program.mapIdentity.valid() || m.pmtPID < 0 || m.pmtPID >= 0x1fff {
		m.pmtPID = -1
	}
	switch {
	case program.audioCount == 0:
		m.continuityReason = "audio_not_declared"
	case program.audioCount > maxAVTimelineAudioPIDs:
		m.continuityReason = "too_many_audio_tracks"
	case program.audioCount > 1 && !m.requireFirstAudio:
		m.continuityReason = "multi_audio_any_track"
	case program.audioCount > 1:
		m.continuityReason = "multi_audio_first_mapped"
	default:
		m.continuityReason = "single_audio"
	}
}

// setFirstAudioOriginCorrection records the attempt-local correction that
// FFmpeg applies to mapped stream 0:a:0. Raw snapshots remain unchanged for
// diagnostics; only safety comparisons use the residual skew after this
// independently proven constant origin is removed.
func (m *transportAVTimeline) setFirstAudioOriginCorrection(correction time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.firstAudioOriginCorrection = correction
	m.faultKind = ""
	m.faultSince = time.Time{}
}

func (m *transportAVTimeline) safetySkewLocked(raw time.Duration) time.Duration {
	if m.requireFirstAudio {
		return raw + m.firstAudioOriginCorrection
	}
	return raw
}

func (m *transportAVTimeline) resetSelectedTrackForBoundary(pid int) {
	if pid == m.videoPID {
		m.audioProgress = [maxAVTimelineAudioPIDs]avProgressComparison{}
		m.video.reset()
		m.videoCC.reset()
		m.videoPES.reset()
		return
	}
	if index := m.selectedAudioIndex(pid); index >= 0 {
		m.audioProgress[index] = avProgressComparison{}
		m.audio[index].reset()
		m.audioCC[index].reset()
		m.audioPES[index].reset()
	}
}

func (m *transportAVTimeline) recordJointProgressLocked() {
	if !m.video.have {
		return
	}
	for i := 0; i < min(m.audioTrackCount, maxAVTimelineAudioPIDs); i++ {
		audio, progress := &m.audio[i], &m.audioProgress[i]
		if !audio.have {
			continue
		}
		if !progress.have {
			*progress = avProgressComparison{
				have:        true,
				videoOrigin: m.video.maxUnwrapped, audioOrigin: audio.maxUnwrapped,
				videoEnd: m.video.maxUnwrapped, audioEnd: audio.maxUnwrapped,
				videoSamples: m.video.samples, audioSamples: audio.samples,
			}
			continue
		}
		if m.video.samples <= progress.videoSamples || audio.samples <= progress.audioSamples {
			// Do not charge an unmatched finite tail as clock drift. Missing
			// partners retain the independent starvation guard. Sample counts
			// (not positive PTS steps) keep repeated/frozen clocks observable.
			continue
		}
		progress.videoEnd, progress.audioEnd = m.video.maxUnwrapped, audio.maxUnwrapped
		progress.videoSamples, progress.audioSamples = m.video.samples, audio.samples
	}
}

func (m *transportAVTimeline) sustainedDriftLocked(audio *ptsTimelineTrack) time.Duration {
	for i := 0; i < min(m.audioTrackCount, maxAVTimelineAudioPIDs); i++ {
		if audio != &m.audio[i] {
			continue
		}
		progress := m.audioProgress[i]
		if progress.have {
			return ticksSignedDuration((progress.audioEnd - progress.audioOrigin) -
				(progress.videoEnd - progress.videoOrigin))
		}
		break
	}
	// Direct diagnostic callers may supply tracks without the packet feeder.
	// Preserve their conservative historical result instead of inventing an
	// origin at query time and erasing drift already present in one burst.
	return ticksSignedDuration(audio.spanTicks() - m.video.spanTicks())
}

func (m *transportAVTimeline) noteSelectedDiscontinuity(pid int, observedAt time.Time) {
	firstForPID := !m.selectedDiscontinuitySeen[pid]
	m.selectedDiscontinuitySeen[pid] = true
	if firstForPID || m.boundaryGathering {
		// Conductor inserts a marker immediately before each selected PID's first
		// packet. Those one-time markers initialize the PID without declaring a
		// succession of shared epochs: resetting the other tracks here could erase
		// a pathological clock before it is compared. The scanner marks a PID
		// active on its first packet even when the provider supplied the marker, so
		// a later provider marker cannot masquerade as initialization.
		//
		// After a real shared boundary, broadcasters commonly emit one marker per
		// selected PID. While that boundary is gathering, preserve tracks already
		// re-established and reset only the PID named by each additional marker.
		m.resetSelectedTrackForBoundary(pid)
		return
	}
	m.sharedBoundaries++
	m.beginEpoch(observedAt)
	m.boundaryGathering = true
}

func (m *transportAVTimeline) finishBoundaryGatheringIfReady() {
	if !m.boundaryGathering || m.video.samples == 0 {
		return
	}
	if !m.continuityEnforcedLocked() || m.anyRequiredAudioSamplesLocked() {
		m.boundaryGathering = false
	}
}

func (m *transportAVTimeline) feed(data []byte) {
	m.feedAt(data, time.Now())
}

func (m *transportAVTimeline) feedAt(data []byte, observedAt time.Time) {
	if len(data) == 0 {
		return
	}
	if observedAt.IsZero() {
		observedAt = time.Now()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.unsafeCurrentFeed = false
	m.unsafeCurrentAVFault = nil
	m.feedLocked(data, observedAt)
}

// feedAtAndReportFault binds corruption evidence to the exact byte slice the
// caller is deciding whether to publish. Raw-edge diagnostic timelines may run
// many megabytes ahead of a relay consumer, so a later global snapshot cannot
// safely identify which queued result contained the bad packet. Incident-scale
// skew/drift is also final on first observation: confirmation is useful for
// normal encoder offsets, but a private slice already proving a 30-second clock
// split must never be allowed to seed a decoder queue.
func (m *transportAVTimeline) feedAtAndReportFault(
	data []byte,
	observedAt time.Time,
) *avContinuityError {
	if len(data) == 0 {
		return nil
	}
	if observedAt.IsZero() {
		observedAt = time.Now()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.unsafeCurrentFeed = false
	m.unsafeCurrentAVFault = nil
	m.feedLocked(data, observedAt)
	if m.unsafeCurrentFeed {
		return &avContinuityError{kind: avFaultTransport}
	}
	if m.unsafeCurrentAVFault != nil {
		return m.unsafeCurrentAVFault
	}
	return m.immediateGrossFaultLocked()
}

func (m *transportAVTimeline) immediateGrossFaultLocked() *avContinuityError {
	if !m.continuityEnforcedLocked() || m.video.samples == 0 {
		return nil
	}
	audioTracks := 1
	if m.audioTrackCount > 1 {
		// Any declared track at an incident-scale offset is unsafe regardless
		// of which one a client renders.
		audioTracks = m.audioTrackCount
	}
	staleBound := defaultAVContinuityPolicy().starvationThreshold
	for index := 0; index < audioTracks; index++ {
		audio := &m.audio[index]
		if audio.samples == 0 {
			continue
		}
		// A sleeping track's frozen clock is absence, not gross misalignment:
		// its skew against a progressing video grows without bound while it
		// carries no renderable media. Judge only clocks that are actually
		// progressing alongside the video.
		if m.video.lastObserved.Sub(audio.lastObserved) >= staleBound {
			continue
		}
		skew := ptsSignedDuration(audio.maxRaw, m.video.maxRaw)
		if m.requireFirstAudio && index == 0 {
			skew = m.safetySkewLocked(skew)
		}
		if absoluteDuration(skew) >= immediateGrossAVOffset {
			return &avContinuityError{kind: avFaultTimelineSkew, skew: skew}
		}
		if m.video.samples < immediateGrossAVDriftMinSamples ||
			audio.samples < immediateGrossAVDriftMinSamples {
			continue
		}
		drift := ticksSignedDuration(audio.spanTicks() - m.video.spanTicks())
		if absoluteDuration(drift) >= immediateGrossAVOffset {
			return &avContinuityError{kind: avFaultTimelineDrift, drift: drift, skew: skew}
		}
	}
	return nil
}

func (m *transportAVTimeline) latchImmediateGrossFaultLocked() {
	if m.unsafeCurrentAVFault != nil {
		return
	}
	if fault := m.immediateGrossFaultLocked(); fault != nil {
		m.unsafeCurrentAVFault = fault
	}
}

func (m *transportAVTimeline) feedAtAndReportUnsafe(data []byte, observedAt time.Time) bool {
	return m.feedAtAndReportFault(data, observedAt) != nil
}

func (m *transportAVTimeline) hasIncompleteSelectedPES() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.videoPES.active {
		return true
	}
	for i := 0; i < min(m.audioTrackCount, maxAVTimelineAudioPIDs); i++ {
		if m.audioPES[i].active {
			return true
		}
	}
	return false
}

func (m *transportAVTimeline) publicationEpochReady() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.publicationEpochReadyLocked(false)
}

func (m *transportAVTimeline) publicationEpochReadyLocked(allowEstablishedDrift bool) bool {
	// Validators that cannot provide an exact programme retain the legacy
	// fail-open behavior; there is no selected PID set on which to establish an
	// A/V epoch. A bound audio-less programme is also immediately publishable.
	if !m.fixedProgram {
		return true
	}
	if m.videoPES.active {
		return false
	}
	for i := 0; i < min(m.audioTrackCount, maxAVTimelineAudioPIDs); i++ {
		if m.audioPES[i].active {
			return false
		}
	}
	if !m.continuityEnforcedLocked() {
		return true
	}
	if m.video.samples == 0 || !m.anyRequiredAudioSamplesLocked() {
		return false
	}
	fault := m.publicationCandidateFaultLocked()
	return fault == nil || (allowEstablishedDrift && fault.kind == avFaultTimelineDrift)
}

func (m *transportAVTimeline) publicationCandidateFaultLocked() *avContinuityError {
	if !m.continuityEnforcedLocked() || m.video.samples == 0 ||
		m.audioTrackCount > 1 {
		return nil
	}
	audio := m.requiredAudioTrackLocked()
	if audio == nil || audio.samples == 0 {
		return nil
	}
	policy := defaultAVContinuityPolicy()
	skew := m.safetySkewLocked(ptsSignedDuration(audio.maxRaw, m.video.maxRaw))
	if m.video.samples >= 2 && audio.samples >= 2 {
		drift := ticksSignedDuration(audio.spanTicks() - m.video.spanTicks())
		initialSkew := m.safetySkewLocked(ptsSignedDuration(audio.minRaw, m.video.minRaw))
		if absoluteDuration(drift) >= policy.driftThreshold &&
			absoluteDuration(skew) >= absoluteDuration(initialSkew) {
			return &avContinuityError{kind: avFaultTimelineDrift, drift: drift, skew: skew}
		}
	}
	if absoluteDuration(skew) >= policy.skewThreshold {
		return &avContinuityError{kind: avFaultTimelineSkew, skew: skew}
	}
	return nil
}

const (
	maxPublicationAVGateBytes = maxMediaPrefixBytes
	// Bounds how long an unestablished epoch may stay private. This is the
	// gate's own latency budget and is spent before the PCR pacer sees a byte;
	// push therefore releases at the release instant so the two budgets are not
	// charged to one timestamp. Worst-case added latency is consequently this
	// hold plus maxTransportAddedLatency, not maxTransportAddedLatency alone.
	// Missing selected clocks at this point are a media fault, not a backlog.
	maxPublicationAVGateHold = maxTransportAddedLatency - transportWakeReserve
)

type publicationAVGateMode uint8

const (
	publicationGateClient publicationAVGateMode = iota
	publicationGateRepairableInput
)

// publicationAVGate makes selected-program epoch establishment private. A
// synthetic late-PID marker or an upstream discontinuity resets timeline
// samples; the marker and all following bytes stay withheld until required A/V
// clocks coexist and the immediate gross-skew/drift fence accepts them. This is
// essential because a lone +minutes audio timestamp cannot be recalled after
// it enters Plex, even if the next video packet proves the fault milliseconds
// later.
type publicationAVGate struct {
	timeline           transportAVTimeline
	pending            []byte
	pendingArrival     time.Time
	mode               publicationAVGateMode
	repairReady        bool
	repairEpoch        uint64
	repairVideoMinStep int64
	repairVideoMaxStep int64
}

func (g *publicationAVGate) bindSelectedProgram(
	program avProgramPIDs,
	observedAt time.Time,
	requireFirstAudio bool,
) {
	g.timeline.requireFirstAudio = requireFirstAudio
	g.timeline.bindSelectedProgram(program, observedAt)
	g.repairReady = false
}

// epochReady keeps ordinary client publication strict. An explicitly eligible
// raw-input gate may forward later drift only after the current epoch passed
// the same establishment checks. FFmpeg's encoded-audio clock filter can then
// prove repair; the separate output gate and input/output arbitration still
// decide whether any repaired bytes may reach a viewer.
func (g *publicationAVGate) epochReady() bool {
	if g.mode != publicationGateRepairableInput {
		return g.timeline.publicationEpochReady()
	}
	m := &g.timeline
	m.mu.Lock()
	defer m.mu.Unlock()
	if g.repairEpoch != m.sharedBoundaries {
		g.repairReady = false
		g.repairEpoch = m.sharedBoundaries
	}
	strictReady := m.publicationEpochReadyLocked(false)
	// Freeze cadence only after the original startup gate and the ordinary
	// continuity policy's minimum samples have established this exact epoch.
	// This narrow mode supports a single selected audio track; it does not
	// infer which clock a multi-audio demuxer will eventually output.
	if strictReady && !g.repairReady && m.fixedProgram && m.audioTrackCount == 1 &&
		m.video.samples >= defaultAVContinuityPolicy().minSamples &&
		m.audio[0].samples >= defaultAVContinuityPolicy().minSamples &&
		m.video.stepHave && m.audio[0].stepHave {
		g.repairReady = true
		g.repairVideoMinStep = m.video.minStepTicks
		g.repairVideoMaxStep = m.video.maxStepTicks
	}
	if strictReady {
		return true
	}
	// Do not ask an audio filter to repair a newly changed video cadence or an
	// abrupt/backward audio step. Extrema accumulate for the whole epoch, so a
	// refused step cannot quietly become a new baseline on the next packet.
	// The complete PES bound is stricter than the filter's per-frame gap test.
	return g.repairReady && m.audioTrackCount == 1 &&
		m.video.minStepTicks >= g.repairVideoMinStep &&
		m.video.maxStepTicks <= g.repairVideoMaxStep &&
		m.audio[0].minStepTicks > 0 &&
		m.audio[0].maxStepTicks <= int64(durationTicks(transcode.AudioClockGapThreshold, transportPTSRate)) &&
		m.publicationEpochReadyLocked(true)
}

func (g *publicationAVGate) push(
	data []byte,
	observedAt time.Time,
) ([]byte, time.Time, *avContinuityError) {
	if observedAt.IsZero() {
		observedAt = time.Now()
	}
	if fault := g.pendingAgeFault(observedAt); fault != nil {
		return nil, observedAt, fault
	}
	if len(data) == 0 {
		return nil, observedAt, nil
	}
	wasReady := g.epochReady()
	if fault := g.timeline.feedAtAndReportFault(data, observedAt); fault != nil {
		g.pending = nil
		g.pendingArrival = time.Time{}
		return nil, observedAt, fault
	}
	ready := g.epochReady()
	if wasReady && ready && len(g.pending) == 0 {
		return data, observedAt, nil
	}
	if len(g.pending) == 0 {
		g.pendingArrival = observedAt
	}
	if len(data) > maxPublicationAVGateBytes-len(g.pending) {
		g.pending = nil
		g.pendingArrival = time.Time{}
		return nil, observedAt, &avContinuityError{kind: avFaultMediaStarvation}
	}
	g.pending = append(g.pending, data...)
	if !ready {
		return nil, g.pendingArrival, nil
	}
	out := g.pending
	// Release carries the release instant, not the first held byte's arrival.
	// The hold above is already local relay latency. Passing the first byte's
	// arrival onward can leave only the wake reserve in the pacer's independent
	// one-second deadline; near the gate boundary, even a maximum-rate positive
	// interval from a clocked multi-chunk release then exceeds paceLimit and is
	// rejected as errTransportRecoveryBacklog. Switching sources cannot reclaim
	// time already spent validating this epoch. pendingArrival still bounds the
	// hold itself in pendingAgeFault.
	g.pending = nil
	g.pendingArrival = time.Time{}
	return out, observedAt, nil
}

func (g *publicationAVGate) pendingAgeFault(now time.Time) *avContinuityError {
	if len(g.pending) == 0 || g.pendingArrival.IsZero() || now.Before(g.pendingArrival) ||
		now.Sub(g.pendingArrival) < maxPublicationAVGateHold {
		return nil
	}
	g.timeline.mu.Lock()
	candidate := g.timeline.publicationCandidateFaultLocked()
	kind := avFaultMediaStarvation
	videoEstablished := g.timeline.video.samples > 0
	audioEstablished := g.timeline.anyRequiredAudioSamplesLocked()
	if g.timeline.continuityEnforcedLocked() {
		switch {
		case videoEstablished && !audioEstablished:
			kind = avFaultAudioStarvation
		case !videoEstablished && audioEstablished:
			kind = avFaultVideoStarvation
		}
	}
	g.timeline.mu.Unlock()
	g.pending = nil
	g.pendingArrival = time.Time{}
	if candidate != nil {
		return candidate
	}
	return &avContinuityError{kind: kind}
}

func (g *publicationAVGate) continuityFault(
	now time.Time,
	policy avContinuityPolicy,
) *avContinuityError {
	if fault := g.pendingAgeFault(now); fault != nil {
		return fault
	}
	return g.timeline.continuityFault(now, policy)
}

func (m *transportAVTimeline) feedLocked(data []byte, observedAt time.Time) {
	if !m.initialized {
		m.initialized = true
		m.pmtPID = -1
		m.videoPID = -1
		m.audioPID = -1
		m.pcrPID = -1
		m.continuityReason = "program_map_unavailable"
		for i := range m.audioPIDs {
			m.audioPIDs[i] = -1
		}
	}
	m.buffer = append(m.buffer, data...)
	for {
		if !m.synced {
			offset, ok := findTSSyncOffset(m.buffer)
			if !ok {
				if len(m.buffer) > tsPacketSize*transportSyncPacketCount {
					keep := tsPacketSize*transportSyncPacketCount - 1
					copy(m.buffer, m.buffer[len(m.buffer)-keep:])
					m.buffer = m.buffer[:keep]
				}
				return
			}
			if offset > 0 {
				copy(m.buffer, m.buffer[offset:])
				m.buffer = m.buffer[:len(m.buffer)-offset]
			}
			m.synced = true
		}

		consumed := 0
		for consumed+tsPacketSize <= len(m.buffer) {
			packet := m.buffer[consumed : consumed+tsPacketSize]
			if packet[0] != 0x47 {
				consumed++
				m.synced = false
				break
			}
			pid := int(packet[1]&0x1f)<<8 | int(packet[2])
			adaptationControl := (packet[3] >> 4) & 0x03
			transportInvalid := packet[1]&0x80 != 0 || packet[3]&0xc0 != 0 ||
				!tsPacketStructureValid(packet)
			if transportInvalid {
				if pid == m.videoPID || pid == m.pcrPID {
					m.noteTransportCorruption(observedAt)
				} else if audioIndex := m.selectedAudioIndex(pid); audioIndex >= 0 {
					m.noteAudioTransportCorruption(audioIndex, observedAt)
				}
				// Never derive a PID map from TEI, scrambled, or reserved-control
				// PSI. If it could supersede the active map, fail enforcement open
				// until a fresh CRC-valid single-section table arrives.
				if packet[1]&0x40 != 0 && (pid == 0 || pid == m.pmtPID) {
					m.unsafeCurrentFeed = true
					m.clearSelectedProgram("corrupt_program_map", observedAt)
					if pid == 0 {
						m.pmtPID = -1
					}
				}
				consumed += tsPacketSize
				continue
			}
			if !m.fixedProgram && pid == 0 && packet[1]&0x40 != 0 {
				if pmtPID, reason, ok := parseTimelinePATPMTPID(packet); ok {
					if pmtPID != m.pmtPID {
						// A PAT rebind is a program boundary. Stop sampling the
						// prior elementary streams immediately: packets from the old
						// program may continue until the new PMT arrives, but they do
						// not belong in the next A/V comparison epoch.
						m.clearSelectedProgram("awaiting_program_map", observedAt)
						m.pmtPID = pmtPID
					}
				} else {
					m.clearSelectedProgram(reason, observedAt)
					m.pmtPID = -1
				}
			}
			if !m.fixedProgram && pid == m.pmtPID && packet[1]&0x40 != 0 {
				if program, ok := parsePMTAVProgram(packet); ok {
					current := avProgramPIDs{
						videoPID:   m.videoPID,
						pcrPID:     m.pcrPID,
						audioPIDs:  m.audioPIDs,
						audioCount: m.audioTrackCount,
					}
					if !program.equal(current) {
						// Drift is meaningful only when both tracks cover the same
						// program epoch. A one-sided PID or clock change therefore
						// resets the pair, not only the changed stream.
						m.beginEpoch(observedAt)
					}
					m.videoPID, m.pcrPID = program.videoPID, program.pcrPID
					m.audioPIDs, m.audioTrackCount = program.audioPIDs, program.audioCount
					m.audioPID = program.firstAudioPID()
					switch {
					case program.audioCount == 0:
						m.continuityReason = "audio_not_declared"
					case program.audioCount > maxAVTimelineAudioPIDs:
						m.continuityReason = "too_many_audio_tracks"
					case program.audioCount > 1 && !m.requireFirstAudio:
						m.continuityReason = "multi_audio_any_track"
					case program.audioCount > 1:
						m.continuityReason = "multi_audio_first_mapped"
					default:
						m.continuityReason = "single_audio"
					}
				} else {
					m.clearSelectedProgram("pmt_unassembled_or_invalid", observedAt)
				}
			}

			declaredDiscontinuity := (adaptationControl == 2 || adaptationControl == 3) &&
				packet[4] >= 1 && packet[5]&0x80 != 0
			selectedPID := pid == m.videoPID || m.selectedAudioIndex(pid) >= 0 || pid == m.pcrPID
			if selectedPID {
				if declaredDiscontinuity {
					m.noteSelectedDiscontinuity(pid, observedAt)
				} else {
					// The raw transcode-input guard sees provider packets before the
					// final boundary transform adds synthetic initialization markers.
					// Record any ordinary first selected packet as initialized so a
					// later real marker opens a shared epoch rather than masquerading
					// as that PID's one-time initialization fence.
					m.selectedDiscontinuitySeen[pid] = true
				}
			}
			track := (*ptsTimelineTrack)(nil)
			continuity := (*tsPayloadContinuity)(nil)
			pes := (*pesPTSAssembler)(nil)
			if pid == m.videoPID {
				track = &m.video
				continuity = &m.videoCC
				pes = &m.videoPES
			} else if audioIndex := m.selectedAudioIndex(pid); audioIndex >= 0 {
				track = &m.audio[audioIndex]
				continuity = &m.audioCC[audioIndex]
				pes = &m.audioPES[audioIndex]
			}
			if track != nil {
				accepted, duplicate, corrupt := continuity.accept(packet)
				if corrupt {
					if audioIndex := m.selectedAudioIndex(pid); audioIndex >= 0 {
						m.noteAudioTransportCorruption(audioIndex, observedAt)
					} else {
						m.noteTransportCorruption(observedAt)
					}
				} else if accepted && !duplicate {
					if payload, ok := tsPayload(packet); ok {
						pts, status := pes.feed(payload, packet[1]&0x40 != 0)
						switch status {
						case pesPTSPresent:
							if !track.recordAt(pts, observedAt) {
								if audioIndex := m.selectedAudioIndex(pid); audioIndex >= 0 {
									m.noteAudioTransportCorruption(audioIndex, observedAt)
								} else {
									m.noteTransportCorruption(observedAt)
								}
							} else {
								// Capture establishment at the exact PES edge, before
								// another track in this same feed can advance further.
								m.recordJointProgressLocked()
								if m.transportRecoveryCompleteLocked() {
									m.transportFaultSince = time.Time{}
								}
							}
							m.latchImmediateGrossFaultLocked()
							m.finishBoundaryGatheringIfReady()
						case pesPTSMalformed:
							if audioIndex := m.selectedAudioIndex(pid); audioIndex >= 0 {
								m.noteAudioTransportCorruption(audioIndex, observedAt)
							} else {
								m.noteTransportCorruption(observedAt)
							}
						}
					}
				}
			}
			consumed += tsPacketSize
		}
		if consumed > 0 {
			copy(m.buffer, m.buffer[consumed:])
			m.buffer = m.buffer[:len(m.buffer)-consumed]
		}
		if m.synced {
			return
		}
	}
}

func (m *transportAVTimeline) continuityEnforcedLocked() bool {
	return m.videoPID >= 0 && m.audioTrackCount > 0 &&
		m.audioTrackCount <= maxAVTimelineAudioPIDs
}

func (m *transportAVTimeline) requiredAudioTrackLocked() *ptsTimelineTrack {
	if !m.continuityEnforcedLocked() {
		return nil
	}
	if m.audioTrackCount == 1 {
		return &m.audio[0]
	}
	// A multi-audio origin may legitimately stop emitting PES on one declared
	// track for minutes at a time (measured on a TLC-class mux: the first
	// declared track slept 280s while the second stayed continuous at a 215ms
	// cadence). That silence is that track's normal behavior, not source
	// death, and failing over lands on the same origin with the same sleeping
	// track. Liveness verdicts therefore consult the freshest declared clock in
	// both passthrough and transcode modes, and total audio starvation means
	// every declared track stopped. Track-0-specific policy (origin correction,
	// slope/skew) applies only while track 0 is that freshest clock.
	selected := &m.audio[0]
	for i := 1; i < m.audioTrackCount; i++ {
		if m.audio[i].lastObserved.After(selected.lastObserved) ||
			(m.audio[i].lastObserved.Equal(selected.lastObserved) && m.audio[i].samples > selected.samples) {
			selected = &m.audio[i]
		}
	}
	return selected
}

func (m *transportAVTimeline) anyRequiredAudioSamplesLocked() bool {
	track := m.requiredAudioTrackLocked()
	return track != nil && track.samples > 0
}

func (m *transportAVTimeline) transportRecoveryCompleteLocked() bool {
	if m.video.samples == 0 || m.video.recoveryHave {
		return false
	}
	if !m.continuityEnforcedLocked() {
		return true
	}
	if m.audioTrackCount == 1 {
		return m.audio[0].samples > 0 && !m.audio[0].recoveryHave
	}
	// A multi-audio program recovers when any declared audio track returns to
	// its prior sane epoch: a declared track that is legitimately sleeping
	// (measured: 280s on a TLC-class mux) must not hold the whole feed in a
	// corruption state that no failover can clear.
	for i := 0; i < m.audioTrackCount; i++ {
		if m.audio[i].samples > 0 && !m.audio[i].recoveryHave {
			return true
		}
	}
	return false
}

func (m *transportAVTimeline) snapshot() avTimelineSnapshot {
	return m.snapshotAt(time.Now())
}

func (m *transportAVTimeline) snapshotAt(now time.Time) avTimelineSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snapshotLocked(now)
}

func (m *transportAVTimeline) snapshotLocked(now time.Time) avTimelineSnapshot {
	audio := m.requiredAudioTrackLocked()
	snapshot := avTimelineSnapshot{
		videoSamples:          m.video.samples,
		audioTracks:           m.audioTrackCount,
		continuityEnforced:    m.continuityEnforcedLocked(),
		continuityReason:      m.continuityReason,
		transportCorruptions:  m.transportCorruptions,
		videoLastGap:          nonNegativeAge(now, m.video.lastObserved),
		audioOriginCorrection: m.firstAudioOriginCorrection,
		sharedBoundaries:      m.sharedBoundaries,
	}
	if audio == nil {
		return snapshot
	}
	snapshot.audioSamples = audio.samples
	snapshot.audioLastGap = nonNegativeAge(now, audio.lastObserved)
	if m.video.samples < 2 || audio.samples < 2 {
		return snapshot
	}
	snapshot.valid = true
	snapshot.initialSkew = ptsSignedDuration(audio.minRaw, m.video.minRaw)
	snapshot.finalSkew = ptsSignedDuration(audio.maxRaw, m.video.maxRaw)
	snapshot.drift = ticksSignedDuration(audio.spanTicks() - m.video.spanTicks())
	return snapshot
}

// continuityFault reports only a sustained, independently measured transport
// failure. It never decodes audio or judges dark/quiet programme content. A
// valid PMT starts an establishment deadline, so declared audio that never
// begins (or fails to resume after a clock boundary) cannot remain invisible.
// One candidate must persist across the confirmation window before restart.
func (m *transportAVTimeline) continuityFault(now time.Time, policy avContinuityPolicy) *avContinuityError {
	if now.IsZero() {
		now = time.Now()
	}
	if policy.starvationThreshold <= 0 || policy.driftThreshold <= 0 ||
		policy.skewThreshold <= 0 ||
		policy.confirmation < 0 || policy.minSamples < 2 {
		policy = defaultAVContinuityPolicy()
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	kind, wallSkew, drift, skew := m.currentContinuityFaultLocked(now, policy)
	if kind == "" {
		m.faultKind = ""
		m.faultSince = time.Time{}
		return nil
	}
	if kind != m.faultKind {
		m.faultKind = kind
		m.faultSince = m.continuityFaultOnsetLocked(kind, now, policy)
	}
	if m.faultSince.IsZero() {
		m.faultSince = m.continuityFaultOnsetLocked(kind, now, policy)
	}
	if now.Sub(m.faultSince) < policy.confirmation {
		return nil
	}
	return &avContinuityError{kind: kind, wallSkew: wallSkew, drift: drift, skew: skew}
}

func (m *transportAVTimeline) continuityFaultOnsetLocked(
	kind avContinuityFaultKind,
	now time.Time,
	policy avContinuityPolicy,
) time.Time {
	// Starvation onset is independently knowable from the last useful PES
	// clock. Backdate it so a sparse null/PSI keepalive arriving every 3.9s
	// cannot defer a 2.5s continuity decision until a second keepalive outside
	// Plex's survival window. Drift/skew require repeated observations, so their
	// candidate begins now.
	switch kind {
	case avFaultAudioStarvation:
		audio := m.requiredAudioTrackLocked()
		if audio != nil && !audio.lastObserved.IsZero() {
			return audio.lastObserved.Add(policy.starvationThreshold)
		}
		if !m.video.firstObserved.IsZero() {
			return m.video.firstObserved.Add(policy.starvationThreshold)
		}
		return m.epochStarted.Add(policy.starvationThreshold)
	case avFaultVideoStarvation:
		if !m.video.lastObserved.IsZero() {
			return m.video.lastObserved.Add(policy.starvationThreshold)
		}
		audio := m.requiredAudioTrackLocked()
		if audio != nil && !audio.firstObserved.IsZero() {
			return audio.firstObserved.Add(policy.starvationThreshold)
		}
		return m.epochStarted.Add(policy.starvationThreshold)
	case avFaultMediaStarvation:
		latest := m.video.lastObserved
		if audio := m.requiredAudioTrackLocked(); audio != nil && audio.lastObserved.After(latest) {
			latest = audio.lastObserved
		}
		if latest.IsZero() {
			latest = m.epochStarted
		}
		return latest.Add(policy.starvationThreshold)
	case avFaultTransport:
		return m.transportFaultSince
	default:
		return now
	}
}

func (m *transportAVTimeline) currentContinuityFaultLocked(
	now time.Time,
	policy avContinuityPolicy,
) (avContinuityFaultKind, time.Duration, time.Duration, time.Duration) {
	if !m.continuityEnforcedLocked() || m.epochStarted.IsZero() {
		return "", 0, 0, 0
	}
	if !m.transportFaultSince.IsZero() {
		return avFaultTransport, 0, 0, 0
	}
	audio := m.requiredAudioTrackLocked()
	videoEstablished := m.video.samples > 0 && !m.video.lastObserved.IsZero()
	audioEstablished := audio != nil && audio.samples > 0 && !audio.lastObserved.IsZero()
	if !videoEstablished && !audioEstablished {
		if !now.Before(m.epochStarted) && now.Sub(m.epochStarted) >= policy.starvationThreshold {
			return avFaultMediaStarvation, 0, 0, 0
		}
		return "", 0, 0, 0
	}
	if videoEstablished && !audioEstablished {
		onset := m.video.firstObserved.Add(policy.starvationThreshold)
		if !now.Before(onset) {
			return avFaultAudioStarvation, 0, 0, 0
		}
		return "", 0, 0, 0
	}
	if !videoEstablished && audioEstablished {
		onset := audio.firstObserved.Add(policy.starvationThreshold)
		if !now.Before(onset) {
			return avFaultVideoStarvation, 0, 0, 0
		}
		return "", 0, 0, 0
	}

	audioIsFirst := audio == &m.audio[0]
	wallSkew := m.video.lastObserved.Sub(audio.lastObserved)
	drift := m.sustainedDriftLocked(audio)
	skew := ptsSignedDuration(audio.maxRaw, m.video.maxRaw)
	if audioIsFirst {
		// The FFmpeg origin correction is proven against 0:a:0 only.
		skew = m.safetySkewLocked(skew)
	}
	latest := m.video.lastObserved
	if audio.lastObserved.After(latest) {
		latest = audio.lastObserved
	}
	// Transport bytes can keep the HTTP connection busy even after both
	// selected elementary clocks stop. Treat sustained null/PSI/other-program
	// traffic without either established A/V PES clock as useful-media
	// starvation; quiet, dark, or still content continues timestamped PES.
	if !now.Before(latest) && now.Sub(latest) >= policy.starvationThreshold {
		return avFaultMediaStarvation, wallSkew, drift, skew
	}
	if wallSkew >= policy.starvationThreshold {
		return avFaultAudioStarvation, wallSkew, drift, skew
	}
	if wallSkew <= -policy.starvationThreshold {
		return avFaultVideoStarvation, wallSkew, drift, skew
	}
	// With multiple audio tracks, slope/skew is enforced only while the
	// evaluated (freshest) clock is the FFmpeg-mapped first track: Plex's
	// selected passthrough language is not visible here, and judging the
	// mapped pipeline by an unmapped commentary track's clock would relocate
	// for an alignment nobody is rendering.
	if m.video.samples < policy.minSamples || audio.samples < policy.minSamples ||
		(m.audioTrackCount > 1 && !(m.requireFirstAudio && audioIsFirst)) {
		return "", wallSkew, drift, skew
	}
	if absoluteDuration(drift) >= policy.driftThreshold {
		return avFaultTimelineDrift, wallSkew, drift, skew
	}
	// Equal A/V spans can still be unusable when an undeclared programme
	// transition establishes the tracks at a pathological fixed offset. Keep
	// normal encoder lead/lag (including the existing one-second regression),
	// but fail a sustained two-second-or-larger absolute presentation skew.
	if absoluteDuration(skew) >= policy.skewThreshold {
		return avFaultTimelineSkew, wallSkew, drift, skew
	}
	return "", wallSkew, drift, skew
}

func nonNegativeAge(now, observed time.Time) time.Duration {
	if now.IsZero() || observed.IsZero() || now.Before(observed) {
		return 0
	}
	return now.Sub(observed)
}

func parsePESPTS(payload []byte) (uint64, bool) {
	pts, status := parsePESPTSStatus(payload)
	return pts, status == pesPTSPresent
}

func parsePESPTSStatus(payload []byte) (uint64, pesPTSParseStatus) {
	if len(payload) < 3 {
		return 0, pesPTSIncomplete
	}
	if payload[0] != 0 || payload[1] != 0 || payload[2] != 1 {
		return 0, pesPTSMalformed
	}
	if len(payload) < 9 {
		return 0, pesPTSIncomplete
	}
	if !pesHasOptionalHeader(payload[3]) {
		return 0, pesPTSAbsent
	}
	// Conductor's selected-clock safety parser deliberately accepts the
	// MPEG-2 PES optional-header form only (leading marker bits `10`). Legacy
	// MPEG-1 PES stuffing/STD layouts are not interpreted as timestamp absence:
	// doing so would let malformed modern audio refresh neither progress nor a
	// fault. Such a programme is privately rejected as unsupported transport.
	if payload[6]&0xc0 != 0x80 {
		return 0, pesPTSMalformed
	}
	packetLength := int(payload[4])<<8 | int(payload[5])
	headerLength := int(payload[8])
	// Some live providers emit zero-length audio PES despite the narrower
	// MPEG stream-id convention; retain that established unbounded-length
	// compatibility. A nonzero length, however, must at least contain the three
	// MPEG-2 optional-header control bytes plus its declared header_data_length.
	if packetLength != 0 && packetLength < 3+headerLength {
		return 0, pesPTSMalformed
	}
	// Withhold the complete declared MPEG-2 optional header for every selected
	// PES, including flags=00. Otherwise a long no-PTS header can be published
	// before a later packet proves that it was truncated or superseded.
	if len(payload) < 9+headerLength {
		return 0, pesPTSIncomplete
	}
	flags := payload[7] >> 6
	if flags == 1 {
		return 0, pesPTSMalformed // reserved PTS_DTS_flags value
	}
	requiredHeaderBytes := 0
	expectedPTSPrefix := byte(0)
	if flags == 2 {
		requiredHeaderBytes = 5
		expectedPTSPrefix = 0x2
	} else if flags == 3 {
		requiredHeaderBytes = 10
		expectedPTSPrefix = 0x3
	}
	if headerLength < requiredHeaderBytes {
		return 0, pesPTSMalformed
	}
	// A nonzero PES_packet_length that ends exactly with its optional header is
	// a legal empty PES, but it is not useful media progress. For an unbounded
	// or payload-bearing PES, require the first elementary-stream byte before a
	// PTS can refresh clocks (and before flags=00 can be released as absence).
	if packetLength != 0 && packetLength == 3+headerLength {
		return 0, pesPTSAbsent
	}
	if len(payload) < 9+headerLength+1 {
		return 0, pesPTSIncomplete
	}
	if flags == 0 {
		return 0, pesPTSAbsent
	}
	field := payload[9:14]
	if field[0]>>4 != expectedPTSPrefix || !validMPEGTimestampMarkers(field) {
		return 0, pesPTSMalformed
	}
	pts := decodeMPEGTimestamp(field)
	if flags == 3 {
		dts := payload[14:19]
		if dts[0]>>4 != 0x1 || !validMPEGTimestampMarkers(dts) {
			return 0, pesPTSMalformed
		}
		// Decode-order reordering is normal, including across the 33-bit wrap.
		// An incident-scale split is not: a DTS minutes away from an aligned PTS
		// can poison a decoder queue even though all marker bits are valid.
		if absoluteDuration(ptsSignedDuration(pts, decodeMPEGTimestamp(dts))) >= immediateGrossAVOffset {
			return 0, pesPTSMalformed
		}
	}
	return pts, pesPTSPresent
}

func decodeMPEGTimestamp(field []byte) uint64 {
	return uint64(field[0]&0x0e)<<29 |
		uint64(field[1])<<22 |
		uint64(field[2]&0xfe)<<14 |
		uint64(field[3])<<7 |
		uint64(field[4]>>1)
}

func validMPEGTimestampMarkers(field []byte) bool {
	return len(field) >= 5 && field[0]&0x01 != 0 &&
		field[2]&0x01 != 0 && field[4]&0x01 != 0
}

func parseTimelinePATPMTPID(packet []byte) (int, string, bool) {
	payload, ok := tsPayload(packet)
	if !ok || len(payload) < 13 {
		return -1, "pat_unassembled_or_invalid", false
	}
	pointer := int(payload[0])
	if 1+pointer+12 > len(payload) {
		return -1, "pat_unassembled_or_invalid", false
	}
	parsed, ok := parsePATSection(payload[1+pointer:])
	if !ok || !parsed.current || parsed.sectionNumber != 0 || parsed.lastSectionNumber != 0 {
		return -1, "pat_unassembled_or_invalid", false
	}
	if len(parsed.programs) != 1 {
		// A true MPTS is ambiguous here: ffprobe/startup may select a program
		// other than PAT order zero. Until the exact startup-selected map is
		// plumbed into this low-cost observer, fail enforcement open rather than
		// relocating against an arbitrary programme.
		return -1, "ambiguous_multi_program_pat", false
	}
	return parsed.programs[0].pmtPID, "", true
}

func parsePMTAVPIDs(packet []byte) (videoPID, audioPID, pcrPID int, ok bool) {
	program, ok := parsePMTAVProgram(packet)
	if !ok {
		return -1, -1, -1, false
	}
	return program.videoPID, program.firstAudioPID(), program.pcrPID, true
}

func parsePMTAVProgram(packet []byte) (avProgramPIDs, bool) {
	payload, payloadOK := tsPayload(packet)
	if !payloadOK || len(payload) < 13 {
		return avProgramPIDs{}, false
	}
	pointer := int(payload[0])
	if 1+pointer+12 > len(payload) {
		return avProgramPIDs{}, false
	}
	section := payload[1+pointer:]
	if len(section) < 12 || section[0] != 0x02 {
		return avProgramPIDs{}, false
	}
	sectionLength := int(section[1]&0x0f)<<8 | int(section[2])
	total := 3 + sectionLength
	end := total - 4
	if end > len(section) || end < 12 {
		return avProgramPIDs{}, false
	}
	// Diagnostics must not bind A/V timelines to a table that the startup
	// state machine would reject. This single-packet helper therefore shares
	// the same CRC/current-section boundary even though it does not assemble
	// split PMTs.
	if total > len(section) || !mpeg2PSICRCValid(section[:total]) ||
		section[5]&0x01 == 0 || section[6] != 0 || section[7] != 0 {
		return avProgramPIDs{}, false
	}
	program := avProgramPIDs{videoPID: -1}
	for i := range program.audioPIDs {
		program.audioPIDs[i] = -1
	}
	program.pcrPID = int(section[8]&0x1f)<<8 | int(section[9])
	if program.pcrPID == 0x1fff {
		program.pcrPID = -1
	}
	programInfoLength := int(section[10]&0x0f)<<8 | int(section[11])
	pos := 12 + programInfoLength
	if pos > end {
		return avProgramPIDs{}, false
	}
	for pos+5 <= end {
		streamType := section[pos]
		pid := int(section[pos+1]&0x1f)<<8 | int(section[pos+2])
		esInfoLength := int(section[pos+3]&0x0f)<<8 | int(section[pos+4])
		if pos+5+esInfoLength > end {
			return avProgramPIDs{}, false
		}
		if program.videoPID < 0 && isVideoStreamType(streamType) {
			program.videoPID = pid
		} else if isAudioStreamType(streamType, section[pos+5:pos+5+esInfoLength]) {
			if program.audioCount < maxAVTimelineAudioPIDs {
				program.audioPIDs[program.audioCount] = pid
			}
			program.audioCount++
		}
		pos += 5 + esInfoLength
	}
	if pos != end {
		return avProgramPIDs{}, false
	}
	return program, program.videoPID >= 0
}

func isVideoStreamType(streamType byte) bool {
	switch streamType {
	case 0x01, 0x02, 0x10, 0x1b, 0x24:
		return true
	default:
		return false
	}
}

func isAudioStreamType(streamType byte, descriptors []byte) bool {
	switch streamType {
	case 0x03, 0x04, 0x0f, 0x11, 0x81, 0x87:
		return true
	case 0x06:
		for pos := 0; pos+2 <= len(descriptors); {
			length := int(descriptors[pos+1])
			if pos+2+length > len(descriptors) {
				break
			}
			switch descriptors[pos] {
			case 0x6a, 0x7a, 0x7c: // AC-3, enhanced AC-3, AAC
				return true
			}
			pos += 2 + length
		}
	}
	return false
}

func ptsSignedDuration(a, b uint64) time.Duration {
	delta := int64((a + ptsModulus - b) % ptsModulus)
	if delta > int64(ptsModulus/2) {
		delta -= int64(ptsModulus)
	}
	return ticksSignedDuration(delta)
}

func ticksSignedDuration(ticks int64) time.Duration {
	seconds := ticks / int64(transportPTSRate)
	remainder := ticks % int64(transportPTSRate)
	return time.Duration(seconds)*time.Second +
		time.Duration(remainder)*time.Second/time.Duration(transportPTSRate)
}

func recordAVTimelineMetrics(input, output avTimelineSnapshot) {
	const eventThreshold = 250 * time.Millisecond
	if input.valid && absoluteDuration(input.drift) >= eventThreshold {
		metrics.InputAVDriftEvents.Inc()
		metrics.InputAVDriftMilliseconds.Add(uint64(absoluteDuration(input.drift) / time.Millisecond))
	}
	if output.valid && absoluteDuration(output.drift) >= eventThreshold {
		metrics.OutputAVDriftEvents.Inc()
		metrics.OutputAVDriftMilliseconds.Add(uint64(absoluteDuration(output.drift) / time.Millisecond))
	}
}

func recordAVContinuityFailure(input bool, fault *avContinuityError) {
	if fault == nil {
		return
	}
	if input {
		switch fault.kind {
		case avFaultAudioStarvation:
			metrics.InputAudioStarvationEvents.Inc()
		case avFaultVideoStarvation:
			metrics.InputVideoStarvationEvents.Inc()
		case avFaultMediaStarvation:
			metrics.InputAVClockStarvationEvents.Inc()
		case avFaultTimelineDrift:
			metrics.InputAVContinuityDriftEvents.Inc()
		case avFaultTimelineSkew:
			metrics.InputAVContinuitySkewEvents.Inc()
		case avFaultTransport:
			metrics.InputTransportCorruptionEvents.Inc()
		}
		return
	}
	switch fault.kind {
	case avFaultAudioStarvation:
		metrics.OutputAudioStarvationEvents.Inc()
	case avFaultVideoStarvation:
		metrics.OutputVideoStarvationEvents.Inc()
	case avFaultMediaStarvation:
		metrics.OutputAVClockStarvationEvents.Inc()
	case avFaultTimelineDrift:
		metrics.OutputAVContinuityDriftEvents.Inc()
	case avFaultTimelineSkew:
		metrics.OutputAVContinuitySkewEvents.Inc()
	case avFaultTransport:
		metrics.OutputTransportCorruptionEvents.Inc()
	}
}

func absoluteDuration(value time.Duration) time.Duration {
	if value < 0 {
		return -value
	}
	return value
}
