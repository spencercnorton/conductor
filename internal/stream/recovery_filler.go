package stream

import (
	"context"
	"time"

	"github.com/spencercnorton/conductor/internal/metrics"
)

type recoveryFiller struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func (ss *subscriberSet) startRecoveryFillerForFailure(ctx context.Context, cause error, downSince time.Time) {
	elapsed := time.Since(downSince)
	if requiresImmediateContinuityFiller(cause) {
		elapsed = max(elapsed, ss.slateAfter)
	}
	ss.startRecoveryFiller(ctx, elapsed)
}

func nextRecoveryFillerDeadline(scheduled, emitted time.Time, duration time.Duration) time.Time {
	next := scheduled.Add(duration)
	if next.Before(emitted) {
		// A long scheduler/fanout stall must not dump an entire backlog of
		// fallback groups. Keep the media grid for ordinary timer jitter;
		// rebase once a whole group cadence is already overdue.
		next = emitted.Add(duration)
	}
	return next
}

// startRecoveryFiller gives one worker exclusive ownership of the synthetic
// cursor throughout backoff, source connection and private startup validation.
// Initial tunes never acquire provider readiness from fallback. The first real
// publication joins this worker before resetting the cursor or delivering media.
func (ss *subscriberSet) startRecoveryFiller(ctx context.Context, gapElapsed time.Duration) {
	ss.recoveryMu.Lock()
	defer ss.recoveryMu.Unlock()
	if ss.stopping.Load() || ctx.Err() != nil || ss.lastRealChunk.Load() == 0 || ss.SlateFn == nil {
		return
	}
	if ss.recoveryFill != nil {
		select {
		case <-ss.recoveryFill.done:
			ss.recoveryFill = nil
		default:
			return
		}
	}
	fillerCtx, cancel := context.WithCancel(ctx)
	filler := &recoveryFiller{cancel: cancel, done: make(chan struct{})}
	ss.recoveryFill = filler
	delay := max(time.Duration(0), ss.slateAfter-gapElapsed)
	go func() {
		defer close(filler.done)
		if delay > 0 && waitTransportDelay(fillerCtx, delay) != nil {
			return
		}
		sl := ss.SlateFn()
		if sl == nil || len(sl.data) == 0 || sl.duration <= 0 {
			return
		}
		if ss.slateFeed == nil || ss.slateFeed.slate != sl {
			ss.slateFeed = newSlateLoopCursor(sl)
			ss.slateClockEpoch = nil
			ss.slatePending, ss.slatePendingDuration = nil, 0
		}
		deadline := time.Now()
		clockEpoch := ss.slateClockEpoch
		for fillerCtx.Err() == nil && !ss.stopping.Load() && ss.SubscriberCount() > 0 {
			if wait := time.Until(deadline); wait > 0 && waitTransportDelay(fillerCtx, wait) != nil {
				return
			}
			if len(ss.slatePending) == 0 {
				chunk, duration, err := ss.slateFeed.nextComplete()
				if err != nil {
					ss.logger.Warn("recovery slate cannot supply complete media", "stream", ss.streamID, "err", err)
					return
				}
				ss.slatePending, ss.slatePendingDuration = chunk, duration
				if ss.onFillerGroupFetched != nil {
					ss.onFillerGroupFetched(fillerCtx)
				}
			}
			chunk, duration := ss.slatePending, ss.slatePendingDuration
			if len(chunk) == 0 || fillerCtx.Err() != nil || ss.stopping.Load() {
				return
			}
			if clockEpoch == nil {
				selection, ready, selectionErr := selectCheckedInputProgram(ss.slateFeed.loop)
				clockEpoch = newOutputClockEpoch(selection.program, ready && selectionErr == nil, false, ss.slateFeed.completeChunks)
				ss.slateClockEpoch = clockEpoch
			}
			if err := ss.deliverClocked(chunk, false, false, clockEpoch); err != nil {
				if fillerCtx.Err() == nil && !ss.stopping.Load() {
					ss.logger.Warn("recovery slate cannot maintain output clock", "stream", ss.streamID, "err", err)
				}
				return
			}
			ss.slatePending, ss.slatePendingDuration = nil, 0
			metrics.SlateBytes.Add(uint64(len(chunk)))
			// Pace the rendered media clock, not its variable byte rate. A
			// large IDR must not create a long sleep after only one frame.
			deadline = nextRecoveryFillerDeadline(deadline, time.Now(), duration)
		}
	}()
}

// Retain the handle under recoveryMu until the worker has joined: concurrent
// real takeover and hard terminalization must both wait for the last complete
// synthetic item. Never call while holding epochDeliveryMu or the subscriber
// map lock; the worker may be completing a fanout using those locks.
func (ss *subscriberSet) stopRecoveryFiller() {
	ss.recoveryMu.Lock()
	defer ss.recoveryMu.Unlock()
	if filler := ss.recoveryFill; filler != nil {
		filler.cancel()
		<-filler.done
		ss.recoveryFill = nil
	}
}
