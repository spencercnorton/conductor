package stream

import (
	"testing"
	"time"
)

// The reservoir is the only thing keeping output flowing while a bursty origin
// pauses between bursts. Plex/tvOS abandons a Live TV input after roughly ten
// seconds without progress, so a reservoir shallower than the pump's own stall
// tolerance lets a pause the pump is still willing to wait out starve the
// client into a teardown first. Guard the worst case: the lineup's heaviest
// channel, where a fixed byte budget buys the least time.
func TestLiveRelayReservoirCoversPumpStallTolerance(t *testing.T) {
	bytesPerSec := heaviestLineupBitrateBitsPerSec / 8
	covered := time.Duration(liveRelayReadAheadChunks*chunkSize) * time.Second /
		time.Duration(bytesPerSec)

	if covered < defaultLiveStallTolerance {
		t.Fatalf("live reservoir covers %s at %d bps (%d chunks); want >= %s so the "+
			"pump never waits longer for a burst than the reservoir can bridge",
			covered, heaviestLineupBitrateBitsPerSec, liveRelayReadAheadChunks,
			defaultLiveStallTolerance)
	}

	// A reservoir must also swallow one whole burst; the measured origins send
	// 2-5 MB at a time and a queue smaller than that can never get ahead.
	const largestMeasuredBurstBytes = 5 << 20
	if liveRelayReadAheadChunks*chunkSize < largestMeasuredBurstBytes {
		t.Fatalf("live reservoir is %d bytes, smaller than the largest measured "+
			"burst of %d bytes", liveRelayReadAheadChunks*chunkSize, largestMeasuredBurstBytes)
	}
}
