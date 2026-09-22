package stream

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestPumpPersistenceCoalescesBlockedHeartbeatsAndStopsBoundedly(t *testing.T) {
	started := make(chan struct{})
	var calls atomic.Int64
	persistence := newPumpPersistence(func(ctx context.Context, _ int64) {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-ctx.Done()
	})
	persistence.AddBytes(1)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("heartbeat worker did not start")
	}

	// A blocked database receives one coalesced signal, not one goroutine per
	// callback. Stop cancels that single in-flight operation immediately.
	for i := 0; i < 10_000; i++ {
		persistence.AddBytes(1)
	}
	startedStop := time.Now()
	if !persistence.Stop(100 * time.Millisecond) {
		t.Fatal("blocked heartbeat worker exceeded bounded stop")
	}
	if elapsed := time.Since(startedStop); elapsed > 100*time.Millisecond {
		t.Fatalf("blocked heartbeat stop took %s", elapsed)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("blocked heartbeat calls=%d, want one coalesced operation", got)
	}
}
