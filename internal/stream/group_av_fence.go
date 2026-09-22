package stream

import "time"

// completeGroupAVFence observes only complete groups about to be published.
// The upstream publication gate has already observed an entire read, including
// later groups that the pacer may reject. Their private video clocks cannot
// justify exposing a group with a large audio-only lead (or the converse).
//
// This is an endpoint fence, not a new sustained-drift policy or a proof of
// continuous media inside a group. Preserve finite replay tail behavior and
// the existing multi-audio selection policy; do not guess which language a
// downstream client will render. The normal input/output guards remain active.
type completeGroupAVFence struct {
	timeline transportAVTimeline
	enabled  bool
}

func (f *completeGroupAVFence) configure(program avProgramPIDs, bound, replay bool, at time.Time) {
	f.enabled = bound && !replay && program.audioCount == 1
	if f.enabled {
		f.timeline.bindSelectedProgram(program, at)
	}
}

func (f *completeGroupAVFence) inspect(group []byte, at time.Time) *avContinuityError {
	if !f.enabled || len(group) == 0 {
		return nil
	}
	// Transport and gross-clock faults are already checked before grouping;
	// retain those same protections if this observer encounters one too.
	if fault := f.timeline.feedAtAndReportFault(group, at); fault != nil {
		return fault
	}
	m := &f.timeline
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.continuityEnforcedLocked() || m.video.samples == 0 || m.audio[0].samples == 0 {
		return nil
	}
	audio := &m.audio[0]
	skew := ptsSignedDuration(audio.maxRaw, m.video.maxRaw)
	initialSkew := ptsSignedDuration(audio.minRaw, m.video.minRaw)
	// Unequal track establishment or a supported constant origin must not
	// become new drift merely because this group ends on one track. The
	// existing 2s skew ceiling still fences a newly enlarged exposed endpoint;
	// the 1s sustained-drift rule continues to use jointly established progress.
	if absoluteDuration(skew) >= defaultAVContinuityPolicy().skewThreshold &&
		absoluteDuration(skew) > absoluteDuration(initialSkew) {
		return &avContinuityError{kind: avFaultTimelineSkew, skew: skew}
	}
	return nil
}
