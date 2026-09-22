package stream

// Timed replay: the captured TLC stream, delivered on its REAL measured
// schedule (1Hz byte counts), through the new live reservoir + dequeue
// restamp + PCR pacer + input A/V timeline, on a virtual clock. This is the
// validation for the burst-tolerance change: this exact delivery pattern tore
// the source down 137 times in 20 minutes under the previous constants.
//
//	CONDUCTOR_FIXTURE=.../tlc-commercial-break-20min.ts \
//	CONDUCTOR_FIXTURE_CSV=.../tlc-delivery-1hz.csv \
//	go test ./internal/stream/ -run TestFixtureTimedReplaySurvivesBurstDelivery -v
//
// The first CONDUCTOR_FIXTURE_PREFIX_CHUNKS chunks (default 16, ~2s at 4 Mbps)
// are released the way the streamer releases a validated startup prefix: one
// PrepareBufferedPrefix profile, then paced against a single release arrival.
// This is the path that decided the live-edge policy until 2026-09-02, so a
// harness that skips it cannot see the difference.
//
// CONDUCTOR_FIXTURE_HOLD (optional, e.g. 4s) emulates the initial attempt's
// startup lead hold between prefix validation and release, exactly as
// holdStartupLead does in the read loops.
//
// The client model at the end is the measured Plex/tvOS behaviour on live TV
// (2026-09-02, ): the app starts ~3s behind the newest segment, plays at
// media rate, and starves whenever the segmenter has not yet published the
// media it needs. Plex's segmenter publishes exactly what the relay emits and
// when (verified: identical video packet counts through PMS and stock ffmpeg),
// so relay emission time is segment availability time.

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestFixtureTimedReplaySurvivesBurstDelivery(t *testing.T) {
	tsPath := os.Getenv("CONDUCTOR_FIXTURE")
	csvPath := os.Getenv("CONDUCTOR_FIXTURE_CSV")
	if tsPath == "" || csvPath == "" {
		t.Skip("CONDUCTOR_FIXTURE / CONDUCTOR_FIXTURE_CSV not set")
	}
	data, err := os.ReadFile(tsPath)
	if err != nil {
		t.Fatal(err)
	}
	cf, err := os.Open(csvPath)
	if err != nil {
		t.Fatal(err)
	}
	defer cf.Close()
	rows, err := csv.NewReader(cf).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	// schedule[i] = bytes delivered during second i (skip header).
	var schedule []int
	for _, r := range rows[1:] {
		b, err := strconv.Atoi(r[1])
		if err != nil {
			t.Fatal(err)
		}
		schedule = append(schedule, b)
	}
	csvTotal := 0
	for _, b := range schedule {
		csvTotal += b
	}
	// The sampler started after the capture; simulate the tail it covers.
	const simChunk = 348 * tsPacketSize // 64KB-aligned to whole TS packets
	start := len(data) - csvTotal
	if start < 0 {
		start = 0
	}
	start = (start / tsPacketSize) * tsPacketSize
	body := data[start:]
	// A raw capture may lose 188-alignment mid-file (origin-side reconnect
	// artifact); everything past that point is unparseable at stride and would
	// only measure the harness, not the relay. Simulate the clean region.
	for off := 0; off+tsPacketSize <= len(body); off += tsPacketSize {
		if body[off] != 0x47 {
			body = body[:off]
			break
		}
	}
	nChunks := len(body) / simChunk

	// Nominal delivery time for each chunk from the cumulative schedule.
	nominal := make([]time.Duration, nChunks)
	sec, cum := 0, 0
	for i := 0; i < nChunks; i++ {
		endOff := (i + 1) * simChunk
		for sec < len(schedule) && cum+schedule[sec] < endOff {
			cum += schedule[sec]
			sec++
		}
		nominal[i] = time.Duration(sec) * time.Second
	}

	base := time.Unix(1_700_000_000, 0)
	wall := base
	var hold time.Duration
	if v := os.Getenv("CONDUCTOR_FIXTURE_HOLD"); v != "" {
		parsed, err := time.ParseDuration(v)
		if err != nil {
			t.Fatal(err)
		}
		hold = parsed
	}
	prefixChunks := 16
	if v := os.Getenv("CONDUCTOR_FIXTURE_PREFIX_CHUNKS"); v != "" {
		parsed, err := strconv.Atoi(v)
		if err != nil {
			t.Fatal(err)
		}
		prefixChunks = parsed
	}
	pacer := testTransportPCRPacer(false)
	pacer.now = func() time.Time { return wall }
	pacer.wait = func(_ context.Context, d time.Duration) error {
		wall = wall.Add(d)
		return nil
	}

	program := avProgramPIDs{videoPID: 0x100, pcrPID: 0x100, audioCount: 2}
	for i := range program.audioPIDs {
		program.audioPIDs[i] = -1
	}
	program.audioPIDs[0] = 0x101
	program.audioPIDs[1] = 0x102
	var timeline transportAVTimeline
	timeline.requireFirstAudio = true // transcode input configuration
	timeline.bindSelectedProgram(program, base)
	policy := defaultAVContinuityPolicy()

	type queued struct{ idx int }
	var queue []queued
	next := 0
	faultKinds := map[avContinuityFaultKind]int{}
	var (
		inputFaults  []string
		pacerErrs    []string
		maxSocketGap time.Duration
		maxEmitGap   time.Duration
		lastEnqueue  = base
		lastEmit     time.Time
		emissions    int
		// Emission timeline for the client model: wall time and media clock
		// (selected-program PCR) of every emitted chunk.
		emitWall  []time.Time
		emitMedia []time.Duration
		mediaScan pcrClockScanner
		mediaOrig uint64
		haveMedia bool
		minQueue  = -1
	)
	mediaScan.selectPID(program.pcrPID)
	held := false
	prefixReleased := prefixChunks <= 0
	var prefixArrival time.Time
	prefixLeft := 0
	enqueue := func(at time.Time) {
		c := body[next*simChunk : (next+1)*simChunk]
		if gap := at.Sub(lastEnqueue); gap > maxSocketGap && next > 0 {
			maxSocketGap = gap
		}
		lastEnqueue = at
		timeline.feedAt(c, at)
		if f := timeline.continuityFault(at, policy); f != nil {
			faultKinds[f.kind]++
			// Production tears the source down for starvation AND confirmed
			// transport corruption (inputFaultProvesUpstreamWithoutOutput).
			if isAVStarvationFault(f.kind) || f.kind == avFaultTransport {
				inputFaults = append(inputFaults,
					fmt.Sprintf("chunk %d at %s: %s", next, at.Sub(base), f.kind))
			}
		}
		queue = append(queue, queued{idx: next})
		next++
	}
	for next < nChunks || len(queue) > 0 {
		// Producer: top the reservoir up with everything already delivered.
		for next < nChunks && len(queue) < liveRelayReadAheadChunks &&
			!wall.Before(base.Add(nominal[next])) {
			enqueue(base.Add(nominal[next]))
		}
		if len(queue) == 0 {
			// Reservoir drained mid-pause: jump to the next delivery.
			wall = base.Add(nominal[next])
			enqueue(wall)
			continue
		}
		if !prefixReleased {
			if len(queue) < prefixChunks && next < nChunks {
				// The startup gate is still buffering its private prefix.
				wall = base.Add(nominal[next])
				continue
			}
			take := min(prefixChunks, len(queue))
			var prefix []byte
			for _, q := range queue[:take] {
				prefix = append(prefix, body[q.idx*simChunk:(q.idx+1)*simChunk]...)
			}
			if err := pacer.PrepareBufferedPrefix(prefix, chunkSize); err != nil {
				t.Fatalf("prepare validated prefix: %v", err)
			}
			if hold > 0 && !held {
				// holdStartupLead: release waits while the producer keeps
				// filling the reservoir.
				held = true
				wall = wall.Add(hold)
				for next < nChunks && len(queue) < liveRelayReadAheadChunks &&
					!wall.Before(base.Add(nominal[next])) {
					enqueue(base.Add(nominal[next]))
				}
			}
			prefixReleased = true
			prefixArrival = wall
			prefixLeft = take
		}
		head := queue[0]
		queue = queue[1:]
		if emissions > 60 && (minQueue < 0 || len(queue) < minQueue) {
			minQueue = len(queue)
		}
		// Dequeue is the live edge: restamped arrival, exactly as the
		// streamer read loops now do. The validated prefix shares its single
		// release arrival instead.
		arrival := wall
		if prefixLeft > 0 {
			arrival = prefixArrival
			prefixLeft--
		}
		c := body[head.idx*simChunk : (head.idx+1)*simChunk]
		pacer.NoteBacklog(len(queue))
		if err := pacer.Wait(context.Background(), c, arrival); err != nil {
			pacerErrs = append(pacerErrs,
				fmt.Sprintf("chunk %d at %s: %v", head.idx, wall.Sub(base), err))
		}
		if !lastEmit.IsZero() {
			if gap := wall.Sub(lastEmit); gap > maxEmitGap {
				maxEmitGap = gap
				if os.Getenv("CONDUCTOR_FIXTURE_TRACE") != "" {
					fmt.Printf("max_gap so far=%s at wall=%s emit=%d queue=%d\n", gap, wall.Sub(base), emissions, len(queue))
				}
			}
		}
		lastEmit = wall
		emissions++
		if os.Getenv("CONDUCTOR_FIXTURE_TRACE") != "" && (emissions <= 40 || emissions%50 == 0) {
			fmt.Printf("trace emit=%d wall=%s queue=%d rate=%d next=%d\n",
				emissions, wall.Sub(base), len(queue), pacer.recoveryRate, next)
		}
		if clock, observed, _ := mediaScan.feed(c); observed {
			if !haveMedia {
				haveMedia = true
				mediaOrig = clock
			}
			if clock >= mediaOrig {
				emitWall = append(emitWall, wall)
				emitMedia = append(emitMedia, pcrTicksDuration(clock-mediaOrig))
			}
		}
	}
	// Client model: Plex/tvOS starts clientStartBehind behind the newest
	// published media and consumes at media rate.
	const clientStartBehind = 3 * time.Second
	var clientStarves int
	var clientStarveTotal, clientStarveMax time.Duration
	if len(emitMedia) > 0 && emitMedia[len(emitMedia)-1] > clientStartBehind {
		startIdx := 0
		for startIdx < len(emitMedia) && emitMedia[startIdx] < clientStartBehind {
			startIdx++
		}
		// The client begins playing media 0 at the wall time the segmenter
		// published clientStartBehind of media.
		clientWall := emitWall[startIdx]
		prevMedia := time.Duration(0)
		for i := 0; i < len(emitMedia); i++ {
			need := clientWall.Add(emitMedia[i] - prevMedia)
			if emitWall[i].After(need) {
				starve := emitWall[i].Sub(need)
				if os.Getenv("CONDUCTOR_FIXTURE_TRACE") != "" {
					fmt.Printf("starve at wall=%s media=%s prev=%s starve=%s\n",
						emitWall[i].Sub(base), emitMedia[i], prevMedia, starve)
				}
				clientStarves++
				clientStarveTotal += starve
				if starve > clientStarveMax {
					clientStarveMax = starve
				}
				clientWall = emitWall[i]
			} else {
				clientWall = need
			}
			prevMedia = emitMedia[i]
		}
	}

	fmt.Printf("fault_kinds=%v\n", faultKinds)
	snap := timeline.snapshotAt(wall)
	fmt.Printf("chunks=%d emissions=%d sim_span=%s\n", nChunks, emissions, wall.Sub(base))
	fmt.Printf("video_samples=%d audio_samples=%d corruptions=%d\n",
		snap.videoSamples, snap.audioSamples, snap.transportCorruptions)
	fmt.Printf("max_socket_gap=%s (tolerance %s)\n", maxSocketGap, defaultLiveStallTolerance)
	fmt.Printf("max_output_gap=%s (Plex patience ~10s)\n", maxEmitGap)
	fmt.Printf("hold=%s min_reservoir_after_startup=%d chunks\n", hold, minQueue)
	fmt.Printf("client3s_starves=%d client3s_starve_total=%s client3s_starve_max=%s\n",
		clientStarves, clientStarveTotal, clientStarveMax)
	fmt.Printf("input_starvation_faults=%d pacer_errors=%d\n", len(inputFaults), len(pacerErrs))
	for _, f := range inputFaults[:min(len(inputFaults), 5)] {
		fmt.Println("  fault:", f)
	}
	for _, e := range pacerErrs[:min(len(pacerErrs), 5)] {
		fmt.Println("  pacer:", e)
	}

	if len(inputFaults) > 0 {
		t.Fatalf("%d input starvation faults on a healthy bursty stream", len(inputFaults))
	}
	if len(pacerErrs) > 0 {
		t.Fatalf("%d pacer errors on a healthy bursty stream", len(pacerErrs))
	}
	if maxSocketGap >= defaultLiveStallTolerance {
		t.Fatalf("socket gap %s would trip the live stall watchdog %s",
			maxSocketGap, defaultLiveStallTolerance)
	}
	if maxEmitGap >= 10*time.Second {
		t.Fatalf("output gap %s exceeds Plex's ~10s patience", maxEmitGap)
	}
}
