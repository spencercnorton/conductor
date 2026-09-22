package stream

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"time"
)

// Accepted finite preflight bodies can be replayed at memory speed. Keep the
// downstream queue coherent with that existing 32 MiB contract, including a
// separate terminal EOF result after an exact chunk-multiple body. Live bodies
// use the much smaller backpressured policy below.
const relayReadAheadChunks = int((maxFinitePreflightBytes+chunkSize-1)/chunkSize) + 1

// Live bodies use a bounded reservoir, not a minimal handoff. Measured on the
// bursty cable-re-encode origins (TLC-class, 2026-08-28, 20-minute 1Hz sample):
// delivery is 2-5 MB bursts separated by pauses of p50=4s p90=7s p99=11s
// max=14s while averaging a healthy realtime 3.7 Mbps. Consumers restamp
// arrival at dequeue, so the pacer's own added-latency contract still measures
// only pacer-added delay and a queued burst is never misread as stale relay
// media.
//
// Size the reservoir in TIME, not bytes. A fixed byte budget buys a span
// inversely proportional to bitrate, so the channels that need the most cover
// got the least: the previous 64 chunks (4 MiB) was sized for a 4 Mbps stream
// (~9s), but the lineup's heaviest channel (CBS Denver, 1080p59.94, 8.46 Mbps)
// received only ~4s. Both sit under the measured p99 pause of 11s, and under
// the ~10s after which Plex/tvOS abandons a Live TV input (see
// defaultIdleReadTimeout), so an ordinary burst pause drained the reservoir and
// starved the client into a teardown while the pump was still willing to wait
// defaultLiveStallTolerance for the next burst. Observed 2026-09-01 on A&E:
// the Apple TV reported bufferedTime=0 and stopped while conductor recorded
// zero upstream read gaps and dropped nothing.
//
// Sizing for the heaviest channel at the pump's own stall tolerance covers
// every lighter channel a fortiori. Cost is bounded: one reservoir per live
// upstream, and credential slots cap those at single digits.
const heaviestLineupBitrateBitsPerSec = 8_460_000

const liveReservoirBytes = heaviestLineupBitrateBitsPerSec / 8 *
	int(defaultLiveStallTolerance/time.Second)

const liveRelayReadAheadChunks = (liveReservoirBytes + chunkSize - 1) / chunkSize

// Once the validated startup prefix is released, every pacer wait is itself
// bounded by the one-second live-age contract. A producer that still cannot
// hand off its next chunk or observe validated publication within that window
// has a local consumer stall and retains the existing typed retry path.
const liveRelayConsumerStallTimeout = maxTransportAddedLatency

// relayCoalesceMaxDelay bounds the latency added while turning arbitrary
// network/pipe reads into byte-accounted chunks. It is well below both the
// prefix probe cadence and Plex's per-source startup allowance.
const relayCoalesceMaxDelay = 50 * time.Millisecond

var errRelayReadAheadFull = errors.New("bounded relay read-ahead queue exhausted")

type relayReadResult struct {
	data      []byte
	err       error
	arrivedAt time.Time
}

type rawRelayRead struct {
	data      []byte
	err       error
	arrivedAt time.Time
}

// privateReleaseArrival moves only media that was already read while startup
// validation/OnRunning was private to the common release edge. The first read
// completed after that edge is authoritative evidence that the source is live
// again; preserve it exactly and end release-backlog rebasing for the attempt.
func privateReleaseArrival(arrivedAt time.Time, releaseAt *time.Time) time.Time {
	if releaseAt == nil || releaseAt.IsZero() {
		return arrivedAt
	}
	if arrivedAt.After(*releaseAt) {
		*releaseAt = time.Time{}
		return arrivedAt
	}
	return *releaseAt
}

// relayReaderCloser adapts a reader and the operation that interrupts a
// blocked Read. Closing a relay source must not necessarily cancel the wider
// attempt: buffered terminal media still needs to drain after a normal EOF.
type relayReaderCloser struct {
	io.Reader
	close func()
}

func (r relayReaderCloser) Close() error {
	if r.close != nil {
		r.close()
	}
	return nil
}

// startRelayReadAhead continuously drains a source/stdout into one bounded
// queue. Short underlying reads are coalesced into chunkSize buffers so the
// entry bound is also the documented byte bound: an HTTP origin writing
// seven-packet (1316-byte) TS bursts or ffmpeg returning 8 KiB pipe reads must
// not consume 8-50x more queue entries than an ordinary full read. A terminal
// partial buffer is retained with its read error.
//
// Arrival diagnostics still run for every underlying read, at the
// provider/ffmpeg edge, rather than inheriting time the downstream normalizer
// deliberately spends probing or pacing. A full queue is a typed failure;
// bytes are never dropped to catch up.
func startRelayReadAhead(
	ctx context.Context,
	reader io.ReadCloser,
	onRead func([]byte, time.Time, time.Time),
	onDone func(),
) (<-chan relayReadResult, <-chan error, <-chan struct{}) {
	return startRelayReadAheadWithPolicy(
		ctx, reader, onRead, onDone, relayReadAheadChunks, nil, 0, nil)
}

// startLiveRelayReadAhead uses bounded blocking backpressure instead of
// eagerly failing when its byte-bounded reservoir is occupied. Before release is
// closed, the attempt's absolute startup context remains the bound: private
// prefix validation legitimately takes longer than the post-release pacing
// window. After release, dequeuing input or publishing an already-validated
// slice demonstrates consumer progress. A full queue without either for one
// full live window fails with errRelayReadAheadFull.
func startLiveRelayReadAhead(
	ctx context.Context,
	reader io.ReadCloser,
	onRead func([]byte, time.Time, time.Time),
	onDone func(),
	release <-chan struct{},
	publication *atomic.Int64,
) (<-chan relayReadResult, <-chan error, <-chan struct{}) {
	return startRelayReadAheadWithPolicy(
		ctx, reader, onRead, onDone, liveRelayReadAheadChunks,
		release, liveRelayConsumerStallTimeout, publication)
}

func startRelayReadAheadWithPolicy(
	ctx context.Context,
	reader io.ReadCloser,
	onRead func([]byte, time.Time, time.Time),
	onDone func(),
	queueChunks int,
	release <-chan struct{},
	consumerStallTimeout time.Duration,
	publication *atomic.Int64,
) (<-chan relayReadResult, <-chan error, <-chan struct{}) {
	results := make(chan relayReadResult, queueChunks)
	overflow := make(chan error, 1)
	done := make(chan struct{})
	rawReads := make(chan rawRelayRead)
	rawAck := make(chan struct{}, 1)
	rawStop := make(chan struct{})
	rawDone := make(chan struct{})

	// The raw worker is the sole caller of Read. Its buffer may be reused only
	// after the coalescer acknowledges that it copied the exposed view.
	go func() {
		defer close(rawDone)
		buf := make([]byte, chunkSize)
		for {
			started := time.Now()
			n, readErr := reader.Read(buf)
			ended := time.Now()
			if n == 0 && readErr == nil {
				select {
				case <-ctx.Done():
					return
				case <-rawStop:
					return
				default:
				}
				continue
			}
			if n > 0 {
				if onRead != nil {
					onRead(buf[:n], started, ended)
				}
			}
			select {
			case rawReads <- rawRelayRead{data: buf[:n:n], err: readErr, arrivedAt: ended}:
			case <-ctx.Done():
				return
			case <-rawStop:
				return
			}
			select {
			case <-rawAck:
			case <-ctx.Done():
				return
			case <-rawStop:
				return
			}
			if readErr != nil {
				return
			}
		}
	}()

	// The coalescer is the sole producer of results. It flushes on a full
	// chunk, terminal read, or bounded age, so small live bursts remain timely
	// without making entry-count bounds depend on raw Read behavior.
	go func() {
		timer := time.NewTimer(time.Hour)
		if !timer.Stop() {
			<-timer.C
		}
		var timerC <-chan time.Time
		timerActive := false
		consumerTimer := time.NewTimer(time.Hour)
		if !consumerTimer.Stop() {
			<-consumerTimer.C
		}
		var pending []byte
		var pendingArrival time.Time

		stopTimer := func() {
			if !timerActive {
				return
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timerActive = false
			timerC = nil
		}
		startTimer := func() {
			if timerActive {
				return
			}
			timer.Reset(relayCoalesceMaxDelay)
			timerActive = true
			timerC = timer.C
		}
		stopConsumerTimer := func() {
			if !consumerTimer.Stop() {
				select {
				case <-consumerTimer.C:
				default:
				}
			}
		}
		sendResult := func(result relayReadResult) bool {
			select {
			case results <- result:
				return true
			default:
			}
			if consumerStallTimeout <= 0 {
				// The finite-replay/default policy retains fail-fast queue
				// exhaustion. Its full accepted body is already bounded to 32 MiB.
				overflow <- errRelayReadAheadFull
				return false
			}

			// Startup validation and its running-state transition are private and
			// remain bounded by the attempt's absolute startup context. Do not let
			// that expected work consume the post-release live pacing window.
			select {
			case <-release:
			case results <- result:
				return true
			case <-ctx.Done():
				return false
			}

			stopConsumerTimer()
			blockedAt := time.Now()
			consumerTimer.Reset(consumerStallTimeout)
			for {
				select {
				case results <- result:
					stopConsumerTimer()
					return true
				case <-consumerTimer.C:
					// The consumer may still be publishing a validated multi-chunk
					// slice while this byte-bounded queue stays full. Only actual
					// fanout progress renews the same no-progress bound; reads,
					// validation and a producer blocked on this send never renew it.
					if publication != nil {
						last := publication.Load()
						if last > blockedAt.UnixNano() {
							remaining := consumerStallTimeout - time.Since(time.Unix(0, last))
							if remaining > 0 && remaining <= consumerStallTimeout {
								consumerTimer.Reset(remaining)
								continue
							}
						}
					}
					overflow <- errRelayReadAheadFull
					return false
				case <-ctx.Done():
					stopConsumerTimer()
					return false
				}
			}
		}
		emit := func(readErr error, arrivedAt time.Time) bool {
			if len(pending) == 0 && readErr == nil {
				return true
			}
			result := relayReadResult{err: readErr, arrivedAt: arrivedAt}
			if len(pending) > 0 {
				result.data = compactRelayReadBuffer(pending)
				result.arrivedAt = pendingArrival
			}
			pending = nil
			pendingArrival = time.Time{}
			stopTimer()
			return sendResult(result)
		}
		appendRead := func(data []byte, readErr error, arrivedAt time.Time) bool {
			for len(data) > 0 {
				if len(pending) == 0 {
					pending = make([]byte, 0, chunkSize)
					// Freshness is a contract on the oldest media byte in the
					// coalesced chunk. Later short reads must not make already-
					// buffered bytes appear younger than their source arrival.
					pendingArrival = arrivedAt
					startTimer()
				}
				take := min(chunkSize-len(pending), len(data))
				pending = append(pending, data[:take]...)
				data = data[take:]
				if len(pending) == chunkSize {
					emitErr := error(nil)
					if len(data) == 0 && readErr != nil {
						emitErr = readErr
						readErr = nil
					}
					if !emit(emitErr, arrivedAt) {
						return false
					}
				}
			}
			if readErr != nil {
				return emit(readErr, arrivedAt)
			}
			return true
		}

		defer func() {
			stopTimer()
			stopConsumerTimer()
			close(rawStop)
			// Context cancellation alone cannot interrupt an arbitrary blocked
			// Read. The read-closer contract makes teardown explicit; close it
			// before joining the sole raw worker.
			_ = reader.Close()
			<-rawDone
			if onDone != nil {
				onDone()
			}
			close(results)
			close(done)
		}()

		for {
			select {
			case raw := <-rawReads:
				terminal := raw.err != nil
				ok := appendRead(raw.data, raw.err, raw.arrivedAt)
				if !ok || terminal {
					// Deferred rawStop releases the worker without letting it race
					// into another blocking Read after a terminal/overflow decision.
					return
				}
				// appendRead copied the reusable raw buffer before this ack.
				rawAck <- struct{}{}
			case <-timerC:
				timerActive = false
				timerC = nil
				if !emit(nil, time.Time{}) {
					return
				}
			case <-ctx.Done():
				return
			case <-rawDone:
				return
			}
		}
	}()
	return results, overflow, done
}

func compactRelayReadBuffer(pending []byte) []byte {
	if len(pending) == chunkSize {
		// Full batches already account for their complete backing allocation;
		// transfer ownership without another copy.
		return pending[:len(pending):len(pending)]
	}
	// Timer and terminal flushes can be only a few KiB. Detach those bytes
	// from the 64 KiB coalescer allocation before the pre-roll ring or a
	// subscriber queue retains the slice.
	compacted := make([]byte, len(pending))
	copy(compacted, pending)
	return compacted
}

func takeRelayReadAheadError(terminal <-chan error) error {
	select {
	case err := <-terminal:
		return err
	default:
		return nil
	}
}
