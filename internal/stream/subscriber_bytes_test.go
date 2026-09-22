package stream

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func byteBudgetTestSubscriber() *subscriber {
	return &subscriber{queue: make(chan queuedChunk, subscriberQueueBuffer), done: make(chan struct{}), workerDone: make(chan struct{})}
}

func TestSubscriberByteBudgetBoundsMixedSizesAndOversize(t *testing.T) {
	sub := byteBudgetTestSubscriber()
	oneMiB := make([]byte, 1024*1024)
	for i := 0; i < 63; i++ {
		if queued, _ := sub.tryQueue(startupChunk{data: oneMiB}, false, time.Now()); !queued {
			t.Fatalf("item %d rejected before byte budget", i)
		}
	}
	for i := 0; i < 2; i++ {
		if queued, _ := sub.tryQueue(startupChunk{data: oneMiB[:len(oneMiB)/2]}, false, time.Now()); !queued {
			t.Fatal("mixed-size exact fit rejected")
		}
	}
	if sub.queuedBytes.Load() != subscriberQueueMaxBytes || len(sub.queue) != 65 {
		t.Fatalf("bytes=%d entries=%d", sub.queuedBytes.Load(), len(sub.queue))
	}
	if queued, left := sub.tryQueue(startupChunk{data: []byte{1}}, false, time.Now()); queued || left {
		t.Fatalf("byte overflow queued=%v left=%v", queued, left)
	}
	held := <-sub.queue
	if sub.queuedBytes.Load() != subscriberQueueMaxBytes {
		t.Fatal("worker-held item escaped accounting before public handoff")
	}
	sub.releaseDelivery(held)
	if queued, _ := sub.tryQueue(startupChunk{data: oneMiB}, false, time.Now()); !queued {
		t.Fatal("released handoff did not free its exact bytes")
	}
	sub.leave()
	sub.discardPrivateQueue()
	if sub.queuedBytes.Load() != 0 {
		t.Fatalf("discard retained %d bytes", sub.queuedBytes.Load())
	}

	oversized := byteBudgetTestSubscriber()
	if queued, left := oversized.tryQueue(startupChunk{data: make([]byte, maxSubscriberChunkBytes+1)}, false, time.Now()); queued || left {
		t.Fatalf("oversize queued=%v left=%v", queued, left)
	}
	if oversized.queuedBytes.Load() != 0 || len(oversized.queue) != 0 {
		t.Fatal("oversize changed queue/accounting")
	}
	oversized.closeByPump()
	if queued, left := oversized.tryQueue(startupChunk{data: []byte{1}}, false, time.Now()); queued || !left {
		t.Fatalf("closed queue admission queued=%v left=%v", queued, left)
	}
}

func TestSubscriberByteBudgetRetainsEntryLimit(t *testing.T) {
	sub := byteBudgetTestSubscriber()
	for i := 0; i < subscriberQueueBuffer; i++ {
		if queued, _ := sub.tryQueue(startupChunk{data: []byte{1}}, false, time.Now()); !queued {
			t.Fatalf("entry %d rejected", i)
		}
	}
	if queued, left := sub.tryQueue(startupChunk{data: []byte{1}}, false, time.Now()); queued || left {
		t.Fatalf("item overflow queued=%v left=%v", queued, left)
	}
	if sub.queuedBytes.Load() != subscriberQueueBuffer {
		t.Fatalf("failed entry admission retained bytes: %d", sub.queuedBytes.Load())
	}
	sub.leave()
	sub.discardPrivateQueue()
	if sub.queuedBytes.Load() != 0 {
		t.Fatal("entry-limited queue did not drain")
	}
}

func TestSubscriberByteBudgetTransfersStartupPreRollExactlyOnce(t *testing.T) {
	sub := byteBudgetTestSubscriber()
	block := make([]byte, 1024*1024)
	prefix := []startupChunk{{data: block}, {data: block}}
	if !sub.setStartupPreRoll(prefix) || sub.queuedBytes.Load() != 2*int64(len(block)) {
		t.Fatal("pre-roll not reserved")
	}
	if !sub.queueStartup(startupChunk{data: block}, time.Now()) {
		t.Fatal("startup handoff rejected")
	}
	if sub.queuedBytes.Load() != 3*int64(len(block)) {
		t.Fatalf("pre-roll counted twice: %d", sub.queuedBytes.Load())
	}
	for i := 0; i < 3; i++ {
		item := <-sub.queue
		if item.first != (i == 0) {
			t.Fatalf("item %d first=%v", i, item.first)
		}
		sub.releaseDelivery(item)
	}
	if sub.queuedBytes.Load() != 0 {
		t.Fatal("startup transfer leaked reservation")
	}
	if !sub.setStartupPreRoll(prefix) {
		t.Fatal("second pre-roll rejected")
	}
	if sub.queueStartup(startupChunk{data: make([]byte, maxSubscriberChunkBytes+1)}, time.Now()) {
		t.Fatal("oversized validating item admitted")
	}
	if len(sub.queue) != 0 || sub.queuedBytes.Load() != 0 {
		t.Fatal("failed startup exposed partial handoff or retained bytes")
	}
}

func TestSubscriberByteBudgetDropsOnlyOversubscribedSink(t *testing.T) {
	var set subscriberSet
	set.init("byte-budget", testLogger())
	set.ringCap = 0
	blocked := make(chan struct{})
	var once sync.Once
	set.onDeliveryBlockStart = func(id string) {
		if id == "stalled" {
			once.Do(func() { close(blocked) })
		}
	}
	slow, leaveSlow := set.subscribe("stalled", false)
	defer leaveSlow()
	fast, leaveFast := set.subscribe("healthy", false)
	defer leaveFast()
	defer set.closeAll()
	const want = subscriberQueueMaxBytes + 1
	done := make(chan struct{})
	fastDone := make(chan struct{})
	fastProgress := make(chan int64, subscriberQueueMaxBytes/maxSubscriberChunkBytes+1)
	var received atomic.Int64
	go func() {
		defer close(fastDone)
		for chunk := range fast.ch {
			total := received.Add(int64(len(chunk)))
			fastProgress <- total
			if total == want {
				close(done)
			}
		}
	}()
	waitForHealthy := func(want int64) {
		t.Helper()
		select {
		case got := <-fastProgress:
			if got != want {
				t.Fatalf("healthy progress=%d want=%d", got, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("healthy subscriber received %d/%d", received.Load(), want)
		}
	}
	group := make([]byte, maxSubscriberChunkBytes)
	set.fanout(group)
	select {
	case <-blocked:
	case <-time.After(time.Second):
		t.Fatal("slow worker did not hold first item")
	}
	// Establish that this sink actually consumes each group. An unpaced burst
	// may legitimately fill both queues before either worker gets scheduled.
	waitForHealthy(int64(len(group)))
	for i := 1; i < subscriberQueueMaxBytes/maxSubscriberChunkBytes; i++ {
		set.fanout(group)
		waitForHealthy(int64((i + 1) * len(group)))
	}
	if slow.queuedBytes.Load() != subscriberQueueMaxBytes || set.SubDrops() != 0 {
		t.Fatalf("premature drop or incorrect bytes=%d drops=%d", slow.queuedBytes.Load(), set.SubDrops())
	}
	set.fanout([]byte{1})
	select {
	case <-slow.workerDone:
	case <-time.After(time.Second):
		t.Fatal("byte overflow did not stop isolated worker")
	}
	if slow.queuedBytes.Load() != 0 || set.SubDrops() != 1 || set.SubscriberCount() != 1 {
		t.Fatalf("cleanup bytes=%d drops=%d subscribers=%d", slow.queuedBytes.Load(), set.SubDrops(), set.SubscriberCount())
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("healthy subscriber received %d/%d", received.Load(), want)
	}
	set.closeAll()
	<-fastDone
}

func TestSubscriberByteBudgetReleasesEpochResetAndDeparture(t *testing.T) {
	for _, startup := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary", true: "startup_preroll"}[startup], func(t *testing.T) {
			var set subscriberSet
			set.init("byte-reset", testLogger())
			block := make([]byte, 1024*1024)
			if startup {
				set.fanout(block)
			}
			sub, unsubscribe := set.subscribe("reset-viewer", startup)
			defer unsubscribe()
			defer set.closeAll()
			if !startup {
				set.fanout(block)
				set.fanout(block)
			}
			if sub.queuedBytes.Load() == 0 {
				t.Fatal("fixture retained no private media")
			}
			set.clearRing()
			// A fresh handoff proves the worker passed any held old delivery;
			// the epoch reset itself acknowledges before that stack unwinds.
			received := make(chan struct{})
			go func() {
				if startup {
					<-sub.firstReal
				} else {
					<-sub.ch
				}
				close(received)
			}()
			set.fanout([]byte{1})
			select {
			case <-received:
			case <-time.After(time.Second):
				t.Fatal("fresh epoch did not deliver")
			}
			unsubscribe()
			select {
			case <-sub.workerDone:
			case <-time.After(time.Second):
				t.Fatal("departure did not finish worker")
			}
			if sub.queuedBytes.Load() != 0 {
				t.Fatalf("reset/departure retained %d bytes", sub.queuedBytes.Load())
			}
		})
	}
}

func TestSubscriberByteBudgetConcurrentLeaveResetAndFanout(t *testing.T) {
	for i := 0; i < 20; i++ {
		var set subscriberSet
		set.init("byte-race", testLogger())
		sub, unsubscribe := set.subscribe("racing", i%2 == 0)
		block := make([]byte, 128*1024)
		var wg sync.WaitGroup
		wg.Add(3)
		go func() {
			defer wg.Done()
			for n := 0; n < 20; n++ {
				set.fanout(block)
			}
		}()
		go func() { defer wg.Done(); set.clearRing() }()
		go func() { defer wg.Done(); unsubscribe() }()
		wg.Wait()
		set.closeAll()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		select {
		case <-sub.workerDone:
		case <-ctx.Done():
			t.Fatal("racing worker did not finish")
		}
		cancel()
		if sub.queuedBytes.Load() != 0 {
			t.Fatalf("iteration %d retained %d bytes", i, sub.queuedBytes.Load())
		}
	}
}
