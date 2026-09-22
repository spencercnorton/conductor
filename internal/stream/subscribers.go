// subscribers.go — the fanout half shared by Streamer and TranscodeStreamer.
//
// Delivery contract (spec Phase 5 / audit S2): mid-stream bytes are NEVER
// silently dropped. Dropping arbitrary 64KB chunks out of an MPEG-TS stream
// guarantees decoder discontinuities — the "picture pixelates, then buffers"
// failure mode. Each subscriber therefore owns a delivery worker and bounded
// queue. A slow sink can fill only its own queue; it is then disconnected so
// it can re-tune cleanly. The upstream pump and every healthy viewer keep
// moving independently.
//
// Channel ownership is structural: the pump is the only closer of the private
// delivery queue, while the subscriber worker is the only closer of the
// public data/readiness channels. Handlers and the slow-subscriber path only
// close done. No producer can therefore race a send against a close.
package stream

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spencercnorton/conductor/internal/metrics"
)

// defaultStallBudget is how long one subscriber worker will wait on its sink
// before disconnecting it. Plex's transcoder routinely pauses reads ~1s
// during startup analysis; 5s is comfortably past normal jitter. This wait is
// never paid by the shared upstream pump or another subscriber.
const defaultStallBudget = 5 * time.Second

// Variable complete-PES publications must not multiply the historical 64 MiB
// private jitter budget. Account pre-roll, queued items and the worker's pending
// handoff together; an ordinary synchronous writer can hold one bounded item
// outside that private budget. Its own startup/media buffers are separate.
const subscriberQueueMaxBytes = 64 * 1024 * 1024
const maxSubscriberChunkBytes = 2 * 1024 * 1024

// ringCapBytes bounds the pre-roll ring (spec Phase 5 "ring buffer"): the
// most recent real upstream bytes, replayed to new subscribers so a second
// client tuning a shared channel gets PAT/PMT + recent data immediately
// instead of waiting for the live edge. ~2MB ≈ 0.5–1s of HD MPEG-TS.
// Synthetic (placeholder) bytes never enter the ring, and the ring is
// cleared at the start of a reconnect gap so pre-roll never spans a
// discontinuity.
const ringCapBytes = 2 * 1024 * 1024

// preloadMaxChunks caps how many ring chunks a new subscriber is seeded
// with, leaving headroom in its channel (capacity subscriberBuffer) so the
// first live fanout after Subscribe doesn't immediately hit the stall path
// while the handler is still spinning up.
const preloadMaxChunks = subscriberBuffer - 8

// defaultSlateAfter is how long a reconnect gap must last before the
// "Source Unavailable" slate starts. Short blips reconnect invisibly;
// flashing a slate card for a sub-3s hiccup would be worse than the
// momentary freeze.
const defaultSlateAfter = 3 * time.Second

// subscriber is one attached client.
type subscriber struct {
	id           string
	ch           chan []byte
	startupCh    chan startupChunk
	firstReal    chan startupChunk // startup-only, owns validated first media
	queue        chan queuedChunk  // pump ingress; drained only by this subscriber's worker
	done         chan struct{}     // closed by handler departure or isolated slow-sink drop
	workerDone   chan struct{}     // closed after the worker closes every public channel
	once         sync.Once         // guards close(done)
	queueOnce    sync.Once         // pump-side close(queue), including terminal startup
	reset        chan subscriberEpochReset
	epoch        atomic.Uint64
	resetPending atomic.Bool
	epochSendMu  sync.Mutex

	// Serialize enqueue admission with leave/close and final queue disposal.
	// queuedBytes includes startupPreRoll and the worker-held item until its
	// whole-item handoff. No network write holds queueMu.
	queueMu     sync.Mutex
	queueClosed bool
	queuedBytes atomic.Int64

	// startupPreRoll is a private snapshot of the current-attempt ring. It is
	// exposed only when a subsequent real chunk validates that the attempt is
	// still live; clearRing retracts it atomically at a reconnect boundary.
	// Protected by queueMu, including terminal worker cleanup.
	startupPreRoll []startupChunk

	// Startup subscribers discard synthetic slate until a real chunk can be
	// routed exclusively through firstReal. Only the pump transitions true to
	// false; atomic keeps the closed/preload path race-safe.
	awaitingReal atomic.Bool
}

type startupChunk struct {
	data           []byte
	epoch          uint64
	realAt         time.Time
	validatedStart bool
}

type queuedChunk struct {
	chunk         startupChunk
	first         bool
	enqueuedAt    time.Time
	boundary      chan struct{}
	retainedBytes int64
}

type subscriberEpochReset struct {
	epoch       uint64
	oldEpoch    uint64
	notifyFirst bool
	ack         chan struct{}
}

type deliveryResult uint8

const (
	deliveryComplete deliveryResult = iota
	deliveryStopped
	deliveryReset
)

// leave returns true only to the caller that actually stopped this
// subscriber. The slow-sink path uses the result to avoid counting a normal
// handler departure as a drop.
func (sub *subscriber) leave() bool {
	left := false
	sub.once.Do(func() {
		sub.queueMu.Lock()
		defer sub.queueMu.Unlock()
		left = true
		close(sub.done)
	})
	return left
}

func (sub *subscriber) closeByPump() {
	sub.queueOnce.Do(func() {
		sub.queueMu.Lock()
		defer sub.queueMu.Unlock()
		sub.queueClosed = true
		close(sub.queue)
	})
}

// tryQueue never waits for a client. A false/full result means the bounded
// per-subscriber jitter queue is exhausted; the caller disconnects only that
// sink rather than stalling the shared upstream pump.
func (sub *subscriber) tryQueue(chunk startupChunk, first bool, enqueuedAt time.Time) (queued, left bool) {
	return sub.tryQueueDelivery(queuedChunk{chunk: chunk, first: first, enqueuedAt: enqueuedAt})
}

func (sub *subscriber) tryQueueDelivery(item queuedChunk) (queued, left bool) {
	sub.queueMu.Lock()
	defer sub.queueMu.Unlock()
	if sub.stopped() || sub.queueClosed {
		return false, true
	}
	if !sub.retainBytesLocked(len(item.chunk.data)) {
		return false, false
	}
	item.retainedBytes = int64(len(item.chunk.data))
	select {
	case sub.queue <- item:
		return true, false
	default:
		sub.releaseDelivery(item)
		return false, false
	}
}

func (sub *subscriber) retainBytesLocked(size int) bool {
	if size > maxSubscriberChunkBytes || int64(size) > subscriberQueueMaxBytes-sub.queuedBytes.Load() {
		return false
	}
	sub.queuedBytes.Add(int64(size))
	return true
}

func (sub *subscriber) releaseDelivery(item queuedChunk) {
	if item.retainedBytes != 0 {
		sub.queuedBytes.Add(-item.retainedBytes)
	}
}

func (sub *subscriber) setStartupPreRoll(chunks []startupChunk) bool {
	sub.queueMu.Lock()
	defer sub.queueMu.Unlock()
	if sub.stopped() || sub.queueClosed {
		return false
	}
	for _, chunk := range chunks {
		if !sub.retainBytesLocked(len(chunk.data)) {
			sub.clearStartupPreRollLocked()
			return false
		}
		sub.startupPreRoll = append(sub.startupPreRoll, chunk)
	}
	return true
}

func (sub *subscriber) clearStartupPreRollLocked() {
	for _, chunk := range sub.startupPreRoll {
		sub.queuedBytes.Add(-int64(len(chunk.data)))
	}
	sub.startupPreRoll = nil
}

func (sub *subscriber) clearStartupPreRoll() {
	sub.queueMu.Lock()
	defer sub.queueMu.Unlock()
	sub.clearStartupPreRollLocked()
}

// queueStartup transfers the already-reserved ring references, then admits the
// validating real item. Preflight the complete handoff before exposing any item.
func (sub *subscriber) queueStartup(chunk startupChunk, enqueuedAt time.Time) bool {
	sub.queueMu.Lock()
	defer sub.queueMu.Unlock()
	if sub.stopped() || sub.queueClosed || len(sub.startupPreRoll)+1 > cap(sub.queue)-len(sub.queue) {
		sub.clearStartupPreRollLocked()
		return false
	}
	if !sub.retainBytesLocked(len(chunk.data)) {
		sub.clearStartupPreRollLocked()
		return false
	}
	first := true
	for _, preRoll := range sub.startupPreRoll {
		sub.queue <- queuedChunk{chunk: preRoll, first: first, enqueuedAt: enqueuedAt, retainedBytes: int64(len(preRoll.data))}
		first = false
	}
	sub.startupPreRoll = nil // reservations now belong to the queued items
	sub.queue <- queuedChunk{chunk: chunk, first: first, enqueuedAt: enqueuedAt, retainedBytes: int64(len(chunk.data))}
	return true
}

func (sub *subscriber) discardPrivateQueue() {
	sub.queueMu.Lock()
	defer sub.queueMu.Unlock()
	// The worker is terminal even if a caller supplied no stall callback.
	// Close admission before disposal; the pump still owns channel closure.
	sub.queueClosed = true
	sub.clearStartupPreRollLocked()
	for {
		select {
		case item, ok := <-sub.queue:
			if !ok {
				return
			}
			sub.releaseDelivery(item)
		default:
			return
		}
	}
}

func (sub *subscriber) stopped() bool {
	select {
	case <-sub.done:
		return true
	default:
		return false
	}
}

// runDelivery is the sole writer/closer for the channels exposed to handlers.
// Its bounded wait is subscriber-local: a blocked disk writer or Plex socket
// never appears in the pump's call stack.
func (sub *subscriber) runDelivery(
	stallBudget time.Duration,
	onBlockStart func(),
	onBlocked func(time.Duration, bool),
	onDelivered func(time.Duration),
	onStalled func(),
) {
	defer func() {
		sub.discardPrivateQueue()
		if sub.startupCh != nil {
			close(sub.startupCh)
		} else {
			close(sub.ch)
		}
		if sub.firstReal != nil {
			close(sub.firstReal)
		}
		close(sub.workerDone)
	}()

	for {
		if sub.stopped() {
			return
		}
		if sub.resetPending.Load() {
			select {
			case reset := <-sub.reset:
				sub.applyEpochReset(reset, nil)
				continue
			case <-sub.done:
				return
			}
		}
		select {
		case <-sub.done:
			return
		case reset := <-sub.reset:
			sub.applyEpochReset(reset, nil)
			continue
		case delivery, ok := <-sub.queue:
			if !ok {
				return
			}
			// Cancellation may have raced the queue receive above. Recheck before
			// a public send so closeAll abandons private backlog deterministically.
			if sub.stopped() {
				sub.releaseDelivery(delivery)
				return
			}
			if delivery.boundary != nil {
				close(delivery.boundary)
				sub.releaseDelivery(delivery)
				continue
			}
			if delivery.chunk.epoch != sub.epoch.Load() {
				sub.releaseDelivery(delivery)
				continue
			}
			result := sub.deliverOne(delivery, stallBudget, onBlockStart, onBlocked, onDelivered)
			sub.releaseDelivery(delivery)
			switch result {
			case deliveryComplete, deliveryReset:
				continue
			case deliveryStopped:
				if onStalled != nil {
					onStalled()
				}
				return
			}
		}
	}
}

func (sub *subscriber) deliverOne(
	delivery queuedChunk,
	stallBudget time.Duration,
	onBlockStart func(),
	onBlocked func(time.Duration, bool),
	onDelivered func(time.Duration),
) deliveryResult {
	if sub.stopped() {
		return deliveryStopped
	}
	if delivery.chunk.epoch != sub.epoch.Load() {
		return deliveryComplete
	}
	if delivery.enqueuedAt.IsZero() {
		delivery.enqueuedAt = time.Now()
	}
	if time.Since(delivery.enqueuedAt) >= stallBudget {
		return deliveryStopped
	}
	delivered := func() {
		if onDelivered != nil {
			onDelivered(time.Since(delivery.enqueuedAt))
		}
	}
	trySend := func() (sent, current bool) {
		if sub.stopped() {
			return false, false
		}
		sub.epochSendMu.Lock()
		defer sub.epochSendMu.Unlock()
		if delivery.chunk.epoch != sub.epoch.Load() {
			return false, false
		}
		if delivery.first {
			select {
			case sub.firstReal <- delivery.chunk:
				delivered()
				return true, true
			default:
				return false, true
			}
		}
		if sub.startupCh != nil {
			select {
			case sub.startupCh <- delivery.chunk:
				delivered()
				return true, true
			default:
				return false, true
			}
		}
		select {
		case sub.ch <- delivery.chunk.data:
			delivered()
			return true, true
		default:
			return false, true
		}
	}
	if sent, current := trySend(); sent {
		return deliveryComplete
	} else if !current {
		return deliveryReset
	}

	blockedAt := time.Now()
	if onBlockStart != nil {
		onBlockStart()
	}
	remaining := stallBudget - time.Since(delivery.enqueuedAt)
	if remaining <= 0 {
		return deliveryStopped
	}
	timer := time.NewTimer(remaining)
	defer func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}()
	// The slow path never blocks inside a public-channel send. It retries the
	// same non-blocking send under epochSendMu, so clearRing either linearizes
	// after a completed old send or publishes the new epoch first and prevents
	// that send. This removes select's random ready-case priority from the
	// attempt-boundary contract.
	// A healthy unbuffered HTTP handoff is ready again almost immediately after
	// each receive. Poll well below one transport-chunk interval so a queued
	// burst does not accumulate one millisecond of artificial latency per chunk;
	// every attempt still takes epochSendMu and remains preemptible by reset.
	ticker := time.NewTicker(50 * time.Microsecond)
	defer ticker.Stop()
	for {
		if sent, current := trySend(); sent {
			if onBlocked != nil {
				onBlocked(time.Since(blockedAt), true)
			}
			return deliveryComplete
		} else if !current {
			return deliveryReset
		}
		select {
		case <-sub.done:
			return deliveryStopped
		case reset := <-sub.reset:
			pendingFirst := (*startupChunk)(nil)
			if delivery.first {
				pendingFirst = &delivery.chunk
			}
			sub.applyEpochReset(reset, pendingFirst)
			return deliveryReset
		case <-timer.C:
			if onBlocked != nil {
				onBlocked(time.Since(blockedAt), false)
			}
			return deliveryStopped
		case <-ticker.C:
		}
	}
}

// waitForDeliveryBoundary places a FIFO control item behind every subscriber's
// current private backlog and waits until healthy workers have handed all
// preceding chunks to their public channels. It is used only for a bounded,
// already-complete finite replay: normal live attempt boundaries still purge
// queued old-epoch media immediately. A departed, stalled, or full subscriber
// never extends the wait beyond within and remains subject to clearRing's
// ordinary epoch reset.
func (ss *subscriberSet) waitForDeliveryBoundary(ctx context.Context, within time.Duration) bool {
	if within <= 0 {
		return false
	}
	type waiter struct {
		ack        <-chan struct{}
		done       <-chan struct{}
		workerDone <-chan struct{}
	}

	allQueued := true
	// Match the established delivery/reset lock order. The epoch fence makes
	// barrier insertion one structural boundary with fanout, clearRing, and
	// closeAll; neither lock is held while waiting for acknowledgments.
	ss.epochDeliveryMu.Lock()
	ss.mu.Lock()
	if ss.closed {
		ss.mu.Unlock()
		ss.epochDeliveryMu.Unlock()
		return false
	}
	waiters := make([]waiter, 0, len(ss.subs))
	for _, sub := range ss.subs {
		ack := make(chan struct{})
		if queued, _ := sub.tryQueueDelivery(queuedChunk{boundary: ack}); queued {
			waiters = append(waiters, waiter{ack: ack, done: sub.done, workerDone: sub.workerDone})
		} else {
			allQueued = false
		}
	}
	ss.mu.Unlock()
	ss.epochDeliveryMu.Unlock()
	if queued := ss.onDeliveryBoundaryQueued; queued != nil {
		queued()
	}

	waitCtx, cancel := context.WithTimeout(ctx, within)
	defer cancel()
	for _, pending := range waiters {
		select {
		case <-pending.ack:
		case <-pending.done:
			allQueued = false
		case <-pending.workerDone:
			allQueued = false
		case <-waitCtx.Done():
			return false
		}
	}
	return allQueued
}

func (ss *subscriberSet) deliveryBoundaryTimeout() time.Duration {
	if ss.stallBudget <= 0 || ss.stallBudget > defaultStallBudget {
		return defaultStallBudget
	}
	return ss.stallBudget
}

// applyEpochReset runs only on the delivery worker. It retracts every
// undispatched old-attempt item from both the private jitter queue and the
// already-buffered public handoff before acknowledging the pump. Bytes already
// consumed by a socket writer cannot be recalled; the acknowledgement is the
// precise boundary after which slate/new-attempt bytes cannot sit behind stale
// media in Conductor.
func (sub *subscriber) applyEpochReset(reset subscriberEpochReset, pendingFirst *startupChunk) {
	sub.epoch.Store(reset.epoch)
	preserveFirst := func(chunk startupChunk) {
		if sub.firstReal == nil {
			return
		}
		select {
		case sub.firstReal <- chunk:
		default:
		}
	}
	if pendingFirst != nil {
		preserveFirst(*pendingFirst)
	}
	finish := func() {
		if reset.notifyFirst {
			// firstReal has one writer and one closer: this delivery worker. If
			// the actual validating item was already consumed or was dropped from
			// the old private queue, an old-epoch empty item makes startup retry
			// instead of timing out. A buffered actual item wins naturally.
			preserveFirst(startupChunk{epoch: reset.oldEpoch})
		}
		sub.resetPending.Store(false)
		close(reset.ack)
	}
	for {
		select {
		case delivery, ok := <-sub.queue:
			if !ok {
				finish()
				return
			}
			sub.releaseDelivery(delivery)
			if delivery.first {
				// Startup readiness stays on firstReal with its old epoch so Pool
				// observes a retryable discontinuity instead of timing out. It is
				// never part of the HTTP writer's ordinary media channel.
				preserveFirst(delivery.chunk)
			}
		default:
			goto public
		}
	}

public:
	if sub.startupCh != nil {
		for {
			select {
			case <-sub.startupCh:
			default:
				finish()
				return
			}
		}
	}
	for {
		select {
		case <-sub.ch:
		default:
			finish()
			return
		}
	}
}

// subscriberSet manages the subscriber map and fanout for a pump. Embedded
// by value in Streamer and TranscodeStreamer; call init before use.
type subscriberSet struct {
	streamID    string
	logger      *slog.Logger
	stallBudget time.Duration

	// SlateFn returns the "Source Unavailable" placeholder clip (nil when
	// unavailable). Wired by the Pool to a lazy, process-cached ffmpeg
	// render. Production startup requires this process-cached clip to exist;
	// the callback is consulted once a reconnect gap outlasts slateAfter.
	SlateFn func() *slate

	// slateAfter is how long a gap must last before the slate starts —
	// brief blips reconnect invisibly without flashing a slate card.
	slateAfter           time.Duration
	slateFeed            *slateLoopCursor  // recovery worker owns it until synchronous real takeover
	slateClockEpoch      *outputClockEpoch // same cursor identity survives failed real takeover
	slatePending         []byte            // at most one complete bounded group; consumed only on accepted delivery
	slatePendingDuration time.Duration
	onFillerGroupFetched func(context.Context) // optional test barrier, configured before starting a worker
	recoveryMu           sync.Mutex
	recoveryFill         *recoveryFiller

	mu       sync.Mutex
	subs     map[string]*subscriber
	closed   bool
	stopping atomic.Bool
	// fanoutWG covers the lock-free tail of deliver after it snapshots
	// subscribers. Hard-stale terminalization closes admission under mu, waits
	// for that tail, and only then closes private queues, so a watchdog cannot
	// race a send against close even when Run itself is wedged.
	fanoutWG sync.WaitGroup
	// closeOnce makes terminalization completion-idempotent. Concurrent callers
	// must all wait for the winning closeAll invocation to finish draining the
	// fanout tail and joining every subscriber worker before they may proceed to
	// durable recovery or successor admission.
	closeOnce sync.Once
	// terminalErr records why the pump closed all subscriber channels. It is
	// written before closeAll and read by Pool after a subscriber observes the
	// closed channel, so DVR can distinguish a completed recording window from
	// an upstream that exhausted its reconnect budget.
	terminalErr error

	// lastRealChunk is a pump-wide diagnostic timestamp. Recording coverage is
	// deliberately computed from each subscriber's tagged chunks after they are
	// written, because another healthy viewer must not advance a stalled DVR.
	lastRealChunk atomic.Int64

	// upstreamInterruptions counts upstream attempts that delivered bytes and
	// then ended while pumping continued — each one is a splice point in every
	// subscriber's byte stream where the next connection's H.264 can join
	// mid-GOP and decode corrupt until the following IDR, with packet clocks
	// staying perfectly continuous. DVR samples the delta across its serve
	// window as capture provenance.
	upstreamInterruptions atomic.Uint64
	mediaEpoch            atomic.Uint64
	// epochDeliveryMu makes the pump-owned clear/deliver ordering structural,
	// including tests or future callers that invoke them from different
	// goroutines. A reset drains only while no new-epoch slate/source chunk can
	// enter any subscriber queue; clearRing returns after every worker acks.
	epochDeliveryMu sync.Mutex
	outputClock     pumpOutputClock // same publication lock; deliberately survives clearRing

	// ring holds the most recent real chunks (refs; chunks are immutable
	// after fanout) up to ringBytes ≤ ringCap. Guarded by mu: appends happen
	// in fanout under the same lock as the snapshot, preloads in Subscribe —
	// so a joining subscriber sees each chunk exactly once (either from the
	// ring or from the in-flight fanout, never both).
	ring      []startupChunk
	ringBytes int
	ringCap   int

	stalls   atomic.Int64 // sends that hit a full buffer but recovered in budget
	subDrops atomic.Int64 // subscribers disconnected for exceeding the budget
	// onDeliveryBlockStart is an optional slow-path observer configured before
	// subscribers are admitted. Tests use it as a precise barrier; production
	// leaves it nil, so healthy delivery retains no additional callback work.
	onDeliveryBlockStart func(string)
	// onSubscribeBeforeAdmission is a test-only race barrier invoked after the
	// subscriber exists but before the set mutex linearizes its epoch/admission.
	// Production leaves it nil.
	onSubscribeBeforeAdmission func()
	// onDeliveryBoundaryQueued is a test-only observer invoked after every FIFO
	// boundary is enqueued and all delivery/reset locks have been released.
	onDeliveryBoundaryQueued func()

	// scratch is the per-fanout snapshot buffer. Only the pump goroutine
	// touches it; reusing it keeps the hot loop allocation-free.
	scratch []*subscriber
}

func (ss *subscriberSet) init(streamID string, logger *slog.Logger) {
	ss.streamID = streamID
	ss.logger = logger
	ss.stallBudget = defaultStallBudget
	ss.slateAfter = defaultSlateAfter
	ss.ringCap = ringCapBytes
	ss.subs = make(map[string]*subscriber)
}

// waitBeforeRetry preserves the reconnect backoff while a separately owned
// filler continues through the next source's private startup. Real publication
// and terminalization synchronously join that worker before changing ownership.
func (ss *subscriberSet) waitBeforeRetry(ctx context.Context, window, gapElapsed time.Duration) bool {
	ss.startRecoveryFiller(ctx, gapElapsed)
	return waitTransportDelay(ctx, window) == nil
}

// Subscribe returns a channel of chunks and a cleanup func. The cleanup func
// MUST be called when the subscriber disconnects (deferred in the handler).
// The returned channel is closed by the pump when the stream ends or the
// subscriber is disconnected for stalling — never by the cleanup func.
func (ss *subscriberSet) Subscribe(clientID string) (<-chan []byte, func()) {
	sub, cleanup := ss.subscribe(clientID, false)
	return sub.ch, cleanup
}

// SubscribeForStartup withholds its startup handoff until the next live real
// chunk validates the current attempt. It then routes eligible pre-roll first
// (followed by the validating chunk on the ordinary channel), or routes that
// live chunk itself when no ring exists. Synthetic slate is never readiness.
func (ss *subscriberSet) SubscribeForStartup(clientID string) (
	<-chan startupChunk,
	<-chan startupChunk,
	<-chan struct{},
	func(),
) {
	sub, cleanup := ss.subscribe(clientID, true)
	// done is the authoritative subscriber-stop decision: unsubscribe,
	// queue exhaustion, delivery timeout, and pump terminalization all close it
	// synchronously before the worker later closes its public channels.
	return sub.firstReal, sub.startupCh, sub.done, cleanup
}

func (ss *subscriberSet) subscribe(clientID string, startup bool) (*subscriber, func()) {
	sub := &subscriber{
		id: clientID,
		// Keep the HTTP-facing handoff unbuffered. The private 1024-entry jitter
		// queue already isolates network writes from the shared pump; a second
		// public buffer cannot be epoch-purged atomically against its receiver and
		// would let old-attempt bytes escape after a recovery boundary begins.
		ch:         make(chan []byte),
		queue:      make(chan queuedChunk, subscriberQueueBuffer),
		done:       make(chan struct{}),
		workerDone: make(chan struct{}),
		reset:      make(chan subscriberEpochReset),
	}
	if startup {
		sub.ch = nil
		sub.startupCh = make(chan startupChunk)
		sub.firstReal = make(chan startupChunk, 1)
		sub.awaitingReal.Store(true)
	}
	startWorker := func() {
		var notifyBlockStart func()
		if onBlockStart := ss.onDeliveryBlockStart; onBlockStart != nil {
			notifyBlockStart = func() { onBlockStart(sub.id) }
		}
		go sub.runDelivery(ss.stallBudget,
			notifyBlockStart,
			func(stall time.Duration, recovered bool) {
				metrics.SubscriberStallMilliseconds.Add(uint64(stall / time.Millisecond))
				if recovered {
					ss.stalls.Add(1)
					metrics.SubscriberStalls.Inc()
				}
			},
			func(age time.Duration) {
				metrics.SubscriberDeliveries.Inc()
				metrics.SubscriberDeliveryMillis.Add(uint64(age / time.Millisecond))
			},
			func() { ss.dropSlow(sub, "delivery_age_or_timeout") },
		)
	}
	if beforeAdmission := ss.onSubscribeBeforeAdmission; beforeAdmission != nil {
		beforeAdmission()
	}
	ss.mu.Lock()
	_, duplicate := ss.subs[clientID]
	if ss.closed || ss.stopping.Load() || duplicate {
		ss.mu.Unlock()
		startWorker()
		sub.closeByPump()
		return sub, func() {}
	}
	// Epoch initialization and map/ring admission are one critical section with
	// clearRing's epoch increment and subscriber snapshot. A subscriber that
	// loses that race therefore joins the new epoch; it can never be admitted
	// after the snapshot while retaining an old epoch that drops all future data.
	sub.epoch.Store(ss.mediaEpoch.Load())
	start := 0
	if len(ss.ring) > preloadMaxChunks {
		start = len(ss.ring) - preloadMaxChunks
	}
	preloadReady := true
	if startup {
		// Keep the snapshot private until the next real fanout proves this same
		// attempt is still live. Slice-copy the references so clearRing can
		// replace the shared ring without changing this pending view; clearRing
		// also retracts the pending view under this same mutex.
		preloadReady = sub.setStartupPreRoll(ss.ring[start:])
	} else {
		// Ordinary internal subscribers may consume recent pre-roll. Startup
		// subscribers validate their snapshot before it becomes visible.
		enqueuedAt := time.Now()
		for _, chunk := range ss.ring[start:] {
			// preloadMaxChunks is strictly below queue capacity, so this cannot
			// block while the subscriber-set mutex is held.
			if queued, _ := sub.tryQueue(chunk, false, enqueuedAt); !queued {
				preloadReady = false
				break
			}
		}
	}
	if !preloadReady {
		ss.mu.Unlock()
		sub.leave()
		startWorker()
		sub.closeByPump()
		return sub, func() {}
	}
	ss.subs[clientID] = sub
	ss.mu.Unlock()
	startWorker()

	return sub, func() {
		ss.remove(sub)
		_ = sub.leave()
	}
}

// markStopping closes admission before a terminal pump is unpublished. The
// mutex serializes the transition with SubscribeForStartup's ring preload; a
// post-subscribe Stopping check catches the subscriber that won immediately
// before this transition without allowing stale pre-roll to reach its sink.
func (ss *subscriberSet) markStopping() {
	ss.mu.Lock()
	ss.stopping.Store(true)
	ss.mu.Unlock()
}

// tryMarkStoppingIfIdle combines the no-subscriber decision with the stopping
// transition. A tune cannot attach between SubscriberCount==0 and shutdown.
func (ss *subscriberSet) tryMarkStoppingIfIdle(eligible bool) bool {
	if !eligible {
		return false
	}
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if len(ss.subs) != 0 {
		return false
	}
	ss.stopping.Store(true)
	return true
}

// tryTerminateHardStale linearizes the watchdog's local activity proof with
// real-media publication. Once both local and durable signals qualify as hard
// stale, the exact generation is terminal even if its PostgreSQL CAS is later
// rejected or outcome-ambiguous. Subscriber closure is independent of Run
// returning, so a wedged source/relay/ffmpeg still gives Plex a prompt EOF and
// clean retry while Pool continues tracking physical ownership until OnExit.
func (ss *subscriberSet) tryTerminateHardStale(
	cutoff, installedAt time.Time,
) bool {
	ss.mu.Lock()
	if ss.closed || ss.stopping.Load() {
		ss.mu.Unlock()
		return false
	}

	lastActivity := ss.LastRealChunk()
	if lastActivity.Before(installedAt) {
		lastActivity = installedAt
	}
	// A zero activity timestamp cannot independently corroborate the stale
	// durable heartbeat. Production entries always carry installedAt; this
	// conservative branch also keeps legacy/test entries fail-safe.
	if lastActivity.IsZero() || lastActivity.After(cutoff) {
		ss.mu.Unlock()
		return false
	}
	ss.stopping.Store(true)
	ss.mu.Unlock()
	ss.closeAll()
	return true
}

func (ss *subscriberSet) Stopping() bool { return ss.stopping.Load() }

func (ss *subscriberSet) MediaEpoch() uint64 { return ss.mediaEpoch.Load() }

// SubscriberCount returns how many clients are currently attached.
func (ss *subscriberSet) SubscriberCount() int {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return len(ss.subs)
}

// noteUpstreamInterruption records one mid-pump upstream splice. Called only
// from the pump goroutine at the moment it commits to retrying after an
// attempt that fanned out bytes.
func (ss *subscriberSet) noteUpstreamInterruption() {
	ss.upstreamInterruptions.Add(1)
}

// UpstreamInterruptions returns the monotonic count of mid-pump upstream
// splices since this pump started. Failed reconnect attempts that never
// delivered bytes do not add a second splice to the same gap.
func (ss *subscriberSet) UpstreamInterruptions() uint64 {
	return ss.upstreamInterruptions.Load()
}

// LastRealChunk returns when real upstream content was last delivered. Zero
// means none has been delivered yet. Synthetic filler never advances it.
func (ss *subscriberSet) LastRealChunk() time.Time {
	ns := ss.lastRealChunk.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

// TerminalError returns the pump's terminal failure, if any. A nil value
// means the pump stopped cleanly (usually cancellation or no subscribers).
func (ss *subscriberSet) TerminalError() error {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return ss.terminalErr
}

// setTerminalError must run before closeAll so a receiver unblocked by a
// closed subscriber channel can always observe the corresponding failure.
func (ss *subscriberSet) setTerminalError(err error) {
	ss.mu.Lock()
	ss.terminalErr = err
	ss.mu.Unlock()
}

// Stalls returns how many subscriber-sink deliveries blocked but recovered.
func (ss *subscriberSet) Stalls() int64 { return ss.stalls.Load() }

// SubDrops returns how many subscribers were disconnected for stalling.
func (ss *subscriberSet) SubDrops() int64 { return ss.subDrops.Load() }

func (ss *subscriberSet) remove(sub *subscriber) {
	ss.mu.Lock()
	if cur, ok := ss.subs[sub.id]; ok && cur == sub {
		delete(ss.subs, sub.id)
	}
	ss.mu.Unlock()
}

// closeAll rejects future subscribes and stops every worker. Public-channel
// bytes already delivered remain readable, while private backlog is abandoned
// so a dead sink cannot delay pump teardown. Run and the hard-stale watchdog
// may call this concurrently; sync.Once makes every caller wait for the same
// complete terminalization. The delivery-epoch fence also prevents a reset
// sentinel or public-channel send from racing the winning terminalizer.
func (ss *subscriberSet) closeAll() {
	ss.closeOnce.Do(func() {
		ss.stopping.Store(true)
		ss.stopRecoveryFiller()
		ss.epochDeliveryMu.Lock()
		defer ss.epochDeliveryMu.Unlock()

		// First close admission under the same mutex used by deliver's WaitGroup
		// Add. Once closed is visible, no new fanout can increment the group; Wait
		// is therefore race-safe even when this terminalizer is not the pump
		// goroutine.
		ss.mu.Lock()
		ss.closed = true
		ss.stopping.Store(true)
		ss.mu.Unlock()
		ss.fanoutWG.Wait()

		ss.mu.Lock()
		clear(ss.scratch)
		ss.scratch = nil
		subs := make([]*subscriber, 0, len(ss.subs))
		for id, sub := range ss.subs {
			delete(ss.subs, id)
			subs = append(subs, sub)
		}
		ss.mu.Unlock()
		for _, sub := range subs {
			// Terminal close preserves anything already buffered on the public
			// channel but abandons private backlog, matching the old immediate
			// close semantics and keeping pump teardown independent of a dead sink.
			_ = sub.leave()
			sub.closeByPump()
			<-sub.workerDone
		}
	})
}

func (ss *subscriberSet) dropSlow(sub *subscriber, reason string) {
	if !sub.leave() {
		return
	}
	ss.remove(sub)
	metrics.SubscriberDrops.Inc()
	drops := ss.subDrops.Add(1)
	if ss.logger != nil {
		ss.logger.Warn("subscriber stalled past budget; disconnected",
			"stream", ss.streamID, "subscriber", sub.id,
			"reason", reason, "stall_budget", ss.stallBudget,
			"queue_capacity", cap(sub.queue),
			"stalls_total", ss.stalls.Load(), "sub_drops_total", drops)
	}
}

// fanout delivers a real upstream chunk to every subscriber and records it
// in the pre-roll ring. Every queue send is non-blocking and allocation-free
// on this hot path; queue exhaustion disconnects only that subscriber.
func (ss *subscriberSet) fanout(chunk []byte) {
	ss.stopRecoveryFiller()
	// Any real packet ends the current synthetic clock epoch. If this source
	// later drops, its first slate loop must retain the muxer's discontinuity
	// marker instead of resuming an old outage cursor with that marker cleared.
	ss.slateFeed = nil
	ss.slateClockEpoch = nil
	ss.slatePending, ss.slatePendingDuration = nil, 0
	ss.deliver(chunk, true, false)
}

// fanoutValidatedStart records the first published slice of an attempt whose
// complete private prefix has already passed startupMediaGate. A subscriber
// whose ring snapshot still begins at this slice can accept it without running
// the same expensive probe again. Once ring eviction removes this marker, the
// subscriber must establish a fresh decoder-safe boundary from later media.
func (ss *subscriberSet) fanoutValidatedStart(chunk []byte) {
	ss.stopRecoveryFiller()
	ss.slateFeed = nil
	ss.slateClockEpoch = nil
	ss.slatePending, ss.slatePendingDuration = nil, 0
	ss.deliver(chunk, true, true)
}

// fanoutSynthetic delivers filler bytes (the "Source Unavailable" slate)
// without recording them in the ring — pre-roll stays real-content-only.
func (ss *subscriberSet) fanoutSynthetic(chunk []byte) {
	ss.deliver(chunk, false, false)
}

// The final real clock is chosen only after the recovery worker joins. It may
// have published another complete group during the caller's private pacing.
func (ss *subscriberSet) fanoutClockedReal(chunk []byte, validated bool, epoch *outputClockEpoch) error {
	ss.stopRecoveryFiller()
	if err := ss.deliverClocked(chunk, true, validated, epoch); err != nil {
		return err
	}
	ss.slateFeed = nil
	ss.slateClockEpoch = nil
	ss.slatePending, ss.slatePendingDuration = nil, 0
	return nil
}

func (ss *subscriberSet) deliverClocked(chunk []byte, real, validated bool, epoch *outputClockEpoch) error {
	ss.epochDeliveryMu.Lock()
	defer ss.epochDeliveryMu.Unlock()
	ss.mu.Lock()
	denied := ss.closed || ss.stopping.Load()
	ss.mu.Unlock()
	if denied {
		return context.Canceled
	}
	out, next, err := ss.outputClock.prepare(chunk, epoch, real)
	if err != nil {
		return err
	}
	if !ss.deliverLocked(out, real, validated) {
		return context.Canceled
	}
	ss.outputClock = next
	return nil
}

// clearRing drops the pre-roll buffer. Called at the start of a reconnect
// gap: ring contents must never span an upstream discontinuity.
func (ss *subscriberSet) clearRing() {
	ss.epochDeliveryMu.Lock()
	defer ss.epochDeliveryMu.Unlock()
	ss.mu.Lock()
	epoch := ss.mediaEpoch.Add(1)
	ss.ring = nil
	ss.ringBytes = 0
	subs := make([]*subscriber, 0, len(ss.subs))
	for _, sub := range ss.subs {
		if sub.awaitingReal.Load() {
			sub.clearStartupPreRoll()
		}
		subs = append(subs, sub)
	}
	ss.mu.Unlock()
	for _, sub := range subs {
		// Publish invalidation under the same lock every public send checks.
		// Once this store completes, no old-epoch fast or slow send can win;
		// the reset handshake below only drains already-buffered bytes.
		sub.epochSendMu.Lock()
		oldEpoch := sub.epoch.Load()
		notifyFirst := sub.firstReal != nil && !sub.awaitingReal.Load()
		sub.epoch.Store(epoch)
		sub.resetPending.Store(true)
		sub.epochSendMu.Unlock()
		// The delivery worker owns every handler-facing send and close. Carry
		// startup invalidation through its reset message instead of sending to
		// firstReal here; ordinary unsubscribe may otherwise close firstReal
		// between this subscriber snapshot and a pump-side sentinel send.
		reset := subscriberEpochReset{
			epoch:       epoch,
			oldEpoch:    oldEpoch,
			notifyFirst: notifyFirst,
			ack:         make(chan struct{}),
		}
		select {
		case sub.reset <- reset:
		case <-sub.done:
			continue
		case <-sub.workerDone:
			continue
		}
		select {
		case <-reset.ack:
		case <-sub.done:
		case <-sub.workerDone:
		}
	}
}

func (ss *subscriberSet) deliver(chunk []byte, recordRing, validatedStart bool) {
	ss.epochDeliveryMu.Lock()
	defer ss.epochDeliveryMu.Unlock()
	ss.deliverLocked(chunk, recordRing, validatedStart)
}

// Caller owns epochDeliveryMu, including preparation and clock commit when
// enabled. False means admission was denied and must not advance that clock.
func (ss *subscriberSet) deliverLocked(chunk []byte, recordRing, validatedStart bool) bool {
	delivered := startupChunk{data: chunk, validatedStart: validatedStart}
	var failedStartup []*subscriber // allocated only on exceptional admission failure
	enqueuedAt := time.Now()
	ss.mu.Lock()
	if ss.closed || ss.stopping.Load() {
		ss.mu.Unlock()
		return false
	}
	ss.fanoutWG.Add(1)
	if recordRing {
		// recordRing is true exactly for real upstream content (fanout) and
		// false for synthetic filler (fanoutSynthetic) — the same distinction
		// the pre-roll ring needs.
		delivered.realAt = time.Now()
		ss.lastRealChunk.Store(delivered.realAt.UnixNano())
	}
	delivered.epoch = ss.mediaEpoch.Load()
	if recordRing && ss.ringCap > 0 {
		ss.ring = append(ss.ring, delivered)
		ss.ringBytes += len(chunk)
		evict := 0
		for ss.ringBytes > ss.ringCap && evict < len(ss.ring) {
			ss.ringBytes -= len(ss.ring[evict].data)
			evict++
		}
		if evict > 0 {
			// Slide instead of re-slicing so the backing array doesn't pin
			// evicted chunks alive (and doesn't grow unboundedly).
			n := copy(ss.ring, ss.ring[evict:])
			for i := n; i < len(ss.ring); i++ {
				ss.ring[i] = startupChunk{}
			}
			ss.ring = ss.ring[:n]
		}
	}
	// Clear prior entries before reusing the snapshot buffer. A removed slow
	// subscriber may own a deep private queue; retaining its pointer in the
	// backing array would pin that backlog until the entire pump is collected.
	clear(ss.scratch)
	ss.scratch = ss.scratch[:0]
	for _, sub := range ss.subs {
		if sub.awaitingReal.Load() {
			if !recordRing {
				// A new tune must not treat historical reconnect slate as
				// provider readiness. It begins at the next real media chunk.
				continue
			}
			if sub.awaitingReal.CompareAndSwap(true, false) {
				if !sub.queueStartup(delivered, enqueuedAt) {
					failedStartup = append(failedStartup, sub)
				}
				continue // current chunk already queued exactly once above
			}
		}
		ss.scratch = append(ss.scratch, sub)
	}
	ss.mu.Unlock()
	defer ss.fanoutWG.Done()

	for _, sub := range failedStartup {
		ss.dropSlow(sub, "queue_full")
	}
	for _, sub := range ss.scratch {
		queued, left := sub.tryQueue(delivered, false, enqueuedAt)
		if queued {
			continue
		}
		if left {
			// Handler already left; its cleanup func races us to the map.
			ss.remove(sub)
			continue
		}

		// Queue exhaustion is already a bounded stall: disconnect only this
		// sink. Waiting here would recreate the cross-subscriber head-of-line
		// blocking this worker layer exists to remove.
		ss.dropSlow(sub, "queue_full")
	}
	return true
}
