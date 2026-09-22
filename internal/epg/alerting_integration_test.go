// alerting_integration_test.go — DB-side checks for the EPG freshness
// alarms (audit 2026-06-09, E5): the consecutive-failure counter on
// epg_source and the guide-horizon metric. Same harness as
// ingest_integration_test.go (CONDUCTOR_INT_TEST=1 + docker postgres).
package epg_test

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/spencercnorton/conductor/internal/store"
)

func TestIntegrationPollStateFailureCounter(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()

	src, err := db.CreateEPGSource(ctx, store.EPGSource{
		Name: "flaky", URL: "https://example.com/guide.xml", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Three consecutive failures: streak counts 0→1→2→3.
	wantPairs := [][2]int{{0, 1}, {1, 2}, {2, 3}}
	for i, want := range wantPairs {
		startedAt, err := db.BeginEPGSourcePollAttempt(ctx, src)
		if err != nil {
			t.Fatal(err)
		}
		prev, cur, err := db.UpdateEPGSourcePollState(ctx, src, startedAt, "error:502", false)
		if err != nil {
			t.Fatal(err)
		}
		if prev != want[0] || cur != want[1] {
			t.Fatalf("failure %d: got (prev=%d cur=%d), want %v", i+1, prev, cur, want)
		}
	}

	// last_ok_at still unset after failures only.
	srcs, err := db.ListEPGSources(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if srcs[0].LastOKAt != nil {
		t.Fatalf("last_ok_at should be nil after failures, got %v", srcs[0].LastOKAt)
	}
	if srcs[0].ConsecutiveFailures != 3 {
		t.Fatalf("expected streak 3 in list, got %d", srcs[0].ConsecutiveFailures)
	}

	// Success resets the streak and reports the prior depth for the
	// recovery alert.
	startedAt, err := db.BeginEPGSourcePollAttempt(ctx, src)
	if err != nil {
		t.Fatal(err)
	}
	prev, cur, err := db.UpdateEPGSourcePollState(ctx, src, startedAt, "ok", true)
	if err != nil {
		t.Fatal(err)
	}
	if prev != 3 || cur != 0 {
		t.Fatalf("recovery: got (prev=%d cur=%d), want (3, 0)", prev, cur)
	}
	srcs, err = db.ListEPGSources(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if srcs[0].LastOKAt == nil {
		t.Fatal("last_ok_at should be set after a success")
	}
}

func TestIntegrationGuideHorizonDays(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()

	// No channels at all → 0 channels, callers skip alerting.
	days, channels, err := db.EPGGuideHorizonDays(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if channels != 0 || days != 0 {
		t.Fatalf("empty DB: got days=%v channels=%d, want 0/0", days, channels)
	}

	covered, err := db.CreateChannel(ctx, store.Channel{
		Number: 100, Name: "Covered", CallSign: "COV", EpgChannelID: "cov", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateChannel(ctx, store.Channel{
		Number: 101, Name: "Dark", CallSign: "DRK", EpgChannelID: "drk", Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	// Disabled channel with far-future EPG must not skew the metric.
	ignored, err := db.CreateChannel(ctx, store.Channel{
		Number: 102, Name: "Disabled", CallSign: "DIS", EpgChannelID: "dis", Enabled: false,
	})
	if err != nil {
		t.Fatal(err)
	}

	// A PPV slot ppvsync has populated: enabled, dark, and by nature only ever
	// carries today's events. It must not count as a dark scheduled channel.
	ppv, err := db.CreateChannel(ctx, store.Channel{
		Number: 301, Name: "NFL PPV 1", CallSign: "PPV1", EpgChannelID: "ppv1", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool.Exec(ctx,
		`INSERT INTO ppv_sync_channel_state (channel_id, observed_at) VALUES ($1, now())`, ppv.ID); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	mkProg := func(ch store.Channel, end time.Time, hash string) {
		t.Helper()
		if _, err := db.UpsertEPGProgram(ctx, store.EPGProgram{
			ChannelID:  ch.ID,
			StartAt:    end.Add(-time.Hour),
			EndAt:      end,
			Title:      "prog-" + hash,
			Category:   []string{"Series"},
			SourceHash: hash,
		}); err != nil {
			t.Fatal(err)
		}
	}
	mkProg(covered, now.Add(6*24*time.Hour), "h1")  // 6 days out
	mkProg(ignored, now.Add(60*24*time.Hour), "h2") // disabled: ignored

	days, channels, err = db.EPGGuideHorizonDays(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if channels != 2 {
		t.Fatalf("expected 2 scheduled channels (PPV slot excluded), got %d", channels)
	}
	// Median of {6 days, 0 days} interpolates to ~3 days; with the dark PPV
	// slot wrongly counted it would be {6, 0, 0} → 0.
	if math.Abs(days-3.0) > 0.1 {
		t.Fatalf("expected median horizon ≈3.0 days, got %v", days)
	}
}

// An enabled lineup made entirely of ppvsync-managed slots has no scheduled
// channel to measure. The query must report 0 channels / 0 days without error
// (percentile_cont over no rows is NULL; the COALESCE keeps the scan valid) so
// callers skip horizon alerting exactly as they do on a fresh install.
func TestIntegrationGuideHorizonDaysAllPPV(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	ppv, err := db.CreateChannel(ctx, store.Channel{
		Number: 301, Name: "NFL PPV 1", CallSign: "PPV1", EpgChannelID: "ppv1", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool.Exec(ctx,
		`INSERT INTO ppv_sync_channel_state (channel_id, observed_at) VALUES ($1, now())`, ppv.ID); err != nil {
		t.Fatal(err)
	}
	days, channels, err := db.EPGGuideHorizonDays(ctx)
	if err != nil {
		t.Fatalf("all-PPV lineup must not error: %v", err)
	}
	if channels != 0 || days != 0 {
		t.Fatalf("all-PPV lineup: got days=%v channels=%d, want 0/0", days, channels)
	}
}
