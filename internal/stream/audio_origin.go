package stream

import (
	"fmt"
	"io"
	"sort"
	"sync/atomic"
	"time"

	"github.com/spencercnorton/conductor/internal/transcode"
)

const (
	// A normal encoder/mux lead strictly below two seconds is preserved exactly.
	// The checked-input safety gate rejects a two-second skew, so that exact
	// boundary must be independently proved and corrected rather than described
	// as preserved while a later gate rejects it.
	maxPreservedAudioOrigin = 2 * time.Second
	// Fox Business' primary English clock has been observed between roughly
	// 4.2s and 4.7s ahead of video while its secondary Spanish clock is aligned.
	// Five seconds remains a narrow defect envelope while covering that live
	// provider variance. Anything beyond it is still left private and rejected.
	maxCorrectedAudioOrigin = 5 * time.Second

	// A normal sub-two-second origin never needs correction; four clocks are
	// enough to replay it into the existing checked-input/publication gates.
	// Corrected origins need stronger private evidence: ignore the first two
	// mux-boundary pairings, then require a stable six-pair window.
	minPreservedAudioOriginSamples = uint64(4)
	minAudioOriginSamples          = uint64(8)
	audioOriginPairWindowSamples   = 6
	audioOriginPairRingSamples     = 16
	minAudioOriginSpan             = 200 * time.Millisecond
	maxAudioOriginDrift            = 100 * time.Millisecond
	maxAudioOriginInitialAgreement = 500 * time.Millisecond
	maxAudioOriginStep             = 500 * time.Millisecond
	maxVideoOriginFold             = 250 * time.Millisecond

	// Keep the private probe smaller than checkedTranscodeInput's two-megabyte
	// ceiling while allowing a high-bitrate, sparse-audio mux to contribute eight
	// PES clocks. The wall cap still leaves the explicit FFmpeg reserve inside
	// the four-second per-source startup deadline.
	maxAudioOriginProbeBytes = 1536 * 1024
	maxAudioOriginProbeWall  = 1500 * time.Millisecond
	minFFmpegStartupReserve  = 2 * time.Second
)

// audioOriginRefusalError is errUncertainTranscodeInput with a stable slug
// naming which constraint refused, so the attempt diagnostic can count refusal
// paths per channel in LogsQL (measured 2026-09-01: 79 of 214 upstream
// attempts refused here with no way to tell the byte cap from a slow start).
// The message may embed a wrapped read error and stays out of the diagnostic;
// only the slug travels.
type audioOriginRefusalError struct {
	slug    string
	message string
}

func (e *audioOriginRefusalError) Error() string {
	return errUncertainTranscodeInput.Error() + ": " + e.message
}

func (e *audioOriginRefusalError) Unwrap() error { return errUncertainTranscodeInput }

func audioOriginRefusal(slug, message string) error {
	return &audioOriginRefusalError{slug: slug, message: message}
}

// audioOriginProbe inspects only the single MPEG-TS programme that FFmpeg can
// map safely and only its first declared audio PID (the existing 0:a:0
// contract). All bytes remain private and are replayed unchanged into the
// ordinary checked-input gate after a decision.
type audioOriginProbe struct {
	buffer   []byte
	bound    bool
	program  avProgramPIDs
	timeline transportAVTimeline
	pairs    audioOriginPairRing
}

// audioOriginPairRing records primary-audio PTS against the latest video PTS
// at the same transport edge. Raw track extrema remain untouched for
// diagnostics and the eventual correction; only this bounded evidence window
// decides whether sparse interleave is stable enough to trust them.
type audioOriginPairRing struct {
	values [audioOriginPairRingSamples]time.Duration
	next   int
	count  int
}

func (r *audioOriginPairRing) record(skew time.Duration) {
	r.values[r.next] = skew
	r.next = (r.next + 1) % len(r.values)
	if r.count < len(r.values) {
		r.count++
	}
}

func medianDuration(values []time.Duration) time.Duration {
	ordered := append([]time.Duration(nil), values...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	middle := len(ordered) / 2
	if len(ordered)%2 != 0 {
		return ordered[middle]
	}
	return time.Duration((int64(ordered[middle-1]) + int64(ordered[middle])) / 2)
}

func (r *audioOriginPairRing) stableMedian(initial time.Duration) (time.Duration, bool) {
	if r.count < int(minAudioOriginSamples) {
		return 0, false
	}
	window := make([]time.Duration, audioOriginPairWindowSamples)
	start := (r.next - len(window) + len(r.values)) % len(r.values)
	for i := range window {
		window[i] = r.values[(start+i)%len(r.values)]
	}
	minimum, maximum := window[0], window[0]
	nondecreasing, nonincreasing := true, true
	for _, value := range window[1:] {
		minimum = min(minimum, value)
		maximum = max(maximum, value)
	}
	if maximum-minimum > maxAudioOriginDrift {
		return 0, false
	}
	for i := 1; i < len(window); i++ {
		if window[i] < window[i-1] {
			nondecreasing = false
		}
		if window[i] > window[i-1] {
			nonincreasing = false
		}
	}
	if nondecreasing || nonincreasing {
		// A six-pair slice of a real clock-rate mismatch can remain inside the
		// instantaneous 100 ms spread. Project that consistent direction over
		// the eight-sample proof horizon so +15 ms/sample (75 ms observed,
		// 105 ms projected) cannot masquerade as a constant origin. Legitimate
		// mux-interleave sawtooth has a reversal; a truly constant window has
		// zero excursion and remains eligible.
		excursion := absoluteDuration(window[len(window)-1] - window[0])
		if excursion*time.Duration(minAudioOriginSamples-1) >
			maxAudioOriginDrift*time.Duration(len(window)-1) {
			return 0, false
		}
	}
	median := medianDuration(window)
	if absoluteDuration(median-initial) > maxAudioOriginInitialAgreement {
		return 0, false
	}
	return median, true
}

func (p *audioOriginProbe) feedTimeline(data []byte, observedAt time.Time) {
	for len(data) > 0 {
		take := min(len(data), tsPacketSize)
		part := data[:take]
		p.timeline.mu.Lock()
		audioBefore := p.timeline.audio[0].samples
		p.timeline.mu.Unlock()

		p.timeline.feedAt(part, observedAt)

		p.timeline.mu.Lock()
		if p.timeline.audio[0].samples > audioBefore && p.timeline.video.have {
			p.pairs.record(ptsSignedDuration(
				p.timeline.audio[0].lastRaw, p.timeline.video.maxRaw))
		}
		p.timeline.mu.Unlock()
		data = data[take:]
	}
}

func (p *audioOriginProbe) push(data []byte, observedAt time.Time) (bool, time.Duration, error) {
	if len(data) == 0 {
		return false, 0, nil
	}
	p.buffer = append(p.buffer, data...)
	if !p.bound && len(p.buffer) > tsPacketSize*(transportSyncPacketCount+1) {
		if _, transportSynced := findTSSyncOffset(p.buffer); !transportSynced {
			// Preserve checkedTranscodeInput's established configured non-TS
			// compatibility. Five correctly spaced sync bytes make the input TS
			// or ambiguous and keep it on the fail-closed proof path; only a
			// clearly non-TS prefix bypasses correction and is replayed exactly.
			return true, 0, nil
		}
	}
	if len(p.buffer) > maxAudioOriginProbeBytes {
		return false, 0, audioOriginRefusal("byte_cap",
			fmt.Sprintf("audio-origin evidence exceeded %d bytes", maxAudioOriginProbeBytes))
	}
	if !p.bound {
		selection, ready, err := selectCheckedInputProgram(p.buffer)
		if err != nil {
			return false, 0, err
		}
		if ready {
			if selection.program.videoPID < 0 || selection.program.audioCount == 0 ||
				selection.program.firstAudioPID() < 0 {
				return false, 0, audioOriginRefusal("no_av_clocks",
					"selected programme has no required A/V clocks")
			}
			p.bound = true
			p.program = selection.program
			// The FFmpeg contract maps exactly 0:a:0. Bind the private proof to
			// the selected programme's video/PCR and first declared audio PID so
			// corruption or discontinuities on an unmapped language track cannot
			// veto an otherwise safe primary-audio correction. PCR remains bound
			// independently even when the provider carries it on another PID.
			proofProgram := selection.program
			for i := range proofProgram.audioPIDs {
				proofProgram.audioPIDs[i] = -1
			}
			proofProgram.audioPIDs[0] = selection.program.firstAudioPID()
			proofProgram.audioCount = 1
			p.timeline.requireFirstAudio = true
			p.timeline.bindSelectedProgram(proofProgram, observedAt)
			p.feedTimeline(p.buffer, observedAt)
		}
	} else {
		p.feedTimeline(data, observedAt)
	}

	if p.bound {
		ready, correction, err := p.decision()
		if ready || err != nil {
			return ready, correction, err
		}
	}
	if len(p.buffer) >= maxAudioOriginProbeBytes {
		return false, 0, audioOriginRefusal("byte_cap",
			fmt.Sprintf("audio-origin evidence exceeded %d bytes", maxAudioOriginProbeBytes))
	}
	return false, 0, nil
}

func (p *audioOriginProbe) decision() (bool, time.Duration, error) {
	p.timeline.mu.Lock()
	defer p.timeline.mu.Unlock()

	if p.timeline.transportCorruptions != 0 || p.timeline.sharedBoundaries != 0 ||
		p.timeline.boundaryGathering {
		return false, 0, audioOriginRefusal("evidence_corrupt",
			"corrupt or discontinuous audio-origin evidence")
	}
	video := &p.timeline.video
	audio := &p.timeline.audio[0]
	if video.samples < minPreservedAudioOriginSamples ||
		audio.samples < minPreservedAudioOriginSamples ||
		ticksSignedDuration(video.spanTicks()) < minAudioOriginSpan ||
		ticksSignedDuration(audio.spanTicks()) < minAudioOriginSpan {
		return false, 0, nil
	}
	if p.timeline.videoPES.active || p.timeline.audioPES[0].active {
		return false, 0, nil
	}
	if !video.stepHave || !audio.stepHave ||
		ticksSignedDuration(video.maxStepTicks) > maxAudioOriginStep ||
		ticksSignedDuration(audio.maxStepTicks) > maxAudioOriginStep ||
		ticksSignedDuration(video.minStepTicks) < -maxVideoOriginFold ||
		audio.minStepTicks < 0 {
		return false, 0, audioOriginRefusal("clock_gap",
			"audio-origin evidence contains a gap or clock fold")
	}

	initial := ptsSignedDuration(audio.minRaw, video.minRaw)
	final := ptsSignedDuration(audio.maxRaw, video.maxRaw)
	drift := ticksSignedDuration(audio.spanTicks() - video.spanTicks())
	if absoluteDuration(initial) < maxPreservedAudioOrigin &&
		absoluteDuration(final) < maxPreservedAudioOrigin &&
		absoluteDuration(drift) < defaultAVContinuityPolicy().driftThreshold {
		// This path changes no timestamp. The exact bytes remain private until
		// checkedTranscodeInput replays them through the selected-program
		// publication gate, whose zero-correction skew/drift policy remains
		// authoritative. Requiring the correction-grade eight sparse clocks
		// here rejected a valid low-bitrate TLC mux before FFmpeg could start.
		return true, 0, nil
	}
	if video.samples < minAudioOriginSamples || audio.samples < minAudioOriginSamples {
		return false, 0, nil
	}
	pairedMedian, stable := p.pairs.stableMedian(initial)
	if !stable {
		return false, 0, nil
	}

	initialAbs, medianAbs := absoluteDuration(initial), absoluteDuration(pairedMedian)
	if initialAbs > maxCorrectedAudioOrigin || medianAbs > maxCorrectedAudioOrigin {
		return false, 0, audioOriginRefusal("outside_window",
			"audio origin lies outside the correction window")
	}
	if initialAbs < maxPreservedAudioOrigin || medianAbs < maxPreservedAudioOrigin ||
		(initial < 0) != (pairedMedian < 0) {
		return false, 0, nil
	}

	return true, -initial, nil
}

func audioOriginProbeBudget(total, elapsed time.Duration) time.Duration {
	remaining := total - elapsed - minFFmpegStartupReserve
	if remaining <= 0 {
		return 0
	}
	return min(remaining, maxAudioOriginProbeWall)
}

func preflightTranscodeAudioOrigin(
	attempt *upstreamAttempt,
	profile *transcode.Profile,
	budget time.Duration,
) ([]byte, time.Duration, error) {
	if profile == nil || !profile.SupportsAudioOriginCorrection() {
		return nil, 0, nil
	}
	if budget <= 0 {
		return nil, 0, audioOriginRefusal("no_budget",
			"no startup budget remains for audio-origin proof")
	}
	// Refusal paths below return the bytes examined so far alongside the
	// error: the caller persists them as the gate's diagnostic fixture.

	var timedOut atomic.Bool
	timer := time.AfterFunc(budget, func() {
		timedOut.Store(true)
		attempt.Close()
	})
	defer timer.Stop()

	probe := &audioOriginProbe{}
	buf := make([]byte, chunkSize)
	for {
		n, readErr := attempt.Read(buf)
		if n > 0 {
			ready, correction, probeErr := probe.push(buf[:n], time.Now())
			if probeErr != nil {
				return probe.buffer, 0, probeErr
			}
			if ready {
				if timedOut.Load() || !timer.Stop() {
					return probe.buffer, 0, audioOriginRefusal("wall_budget",
						"audio-origin proof exceeded its startup budget")
				}
				return probe.buffer, correction, nil
			}
		}
		if timedOut.Load() {
			return probe.buffer, 0, audioOriginRefusal("wall_budget",
				"audio-origin proof exceeded its startup budget")
		}
		if readErr != nil {
			if readErr == io.EOF {
				readErr = io.ErrUnexpectedEOF
			}
			return probe.buffer, 0, audioOriginRefusal("ended_early",
				fmt.Sprintf("audio-origin proof ended before stable clocks: %v", readErr))
		}
	}
}
