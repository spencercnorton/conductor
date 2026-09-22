// metrics_gauges.go — DB-backed gauges for /metrics (audit Q1). Evaluated
// at scrape time with a short per-query context; on error the sample is
// simply omitted (Prometheus treats absence as "no data", which is more
// honest than a stale or zero value).
package main

import (
	"context"
	"time"

	"github.com/spencercnorton/conductor/internal/metrics"
	"github.com/spencercnorton/conductor/internal/store"
)

const gaugeQueryTimeout = 2 * time.Second

func registerDBGauges(db *store.DB) {
	q := func(fn func(ctx context.Context) (float64, error)) func() (float64, bool) {
		return func() (float64, bool) {
			ctx, cancel := context.WithTimeout(context.Background(), gaugeQueryTimeout)
			defer cancel()
			v, err := fn(ctx)
			if err != nil {
				return 0, false
			}
			return v, true
		}
	}

	metrics.RegisterGauge("conductor_active_streams",
		"Upstream slots in use (active_stream rows in starting/running).",
		q(func(ctx context.Context) (float64, error) {
			n, err := db.CountActiveStreams(ctx)
			return float64(n), err
		}))

	metrics.RegisterGauge("conductor_credential_slots_total",
		"Total tuner capacity (sum of enabled credentials' max_streams).",
		q(func(ctx context.Context) (float64, error) {
			n, err := db.CountTuners(ctx)
			return float64(n), err
		}))

	metrics.RegisterGauge("conductor_epg_horizon_days",
		"Days of future guide data remaining (0 = guide has run dry).",
		q(func(ctx context.Context) (float64, error) {
			return db.EPGHorizonDays(ctx)
		}))

	metrics.RegisterGauge("conductor_epg_programs_total",
		"Canonical programme rows visible to guide consumers.",
		q(func(ctx context.Context) (float64, error) {
			n, err := db.CountEPGPrograms(ctx)
			return float64(n), err
		}))

	metrics.RegisterGauge("conductor_epg_canonical_overlaps",
		"Overlapping canonical programme pairs (must remain zero).",
		q(func(ctx context.Context) (float64, error) {
			_, _, overlaps, err := db.EPGCanonicalHealth(ctx)
			return float64(overlaps), err
		}))

	metrics.RegisterGauge("conductor_epg_suppressed_candidates",
		"Retained non-canonical programme candidates available for fallback promotion.",
		q(func(ctx context.Context) (float64, error) {
			_, suppressed, _, err := db.EPGCanonicalHealth(ctx)
			return float64(suppressed), err
		}))

	metrics.RegisterGaugeVec("conductor_epg_source_last_ok_age_seconds",
		"Seconds since each enabled EPG source last fetched successfully (-1 = never).",
		func() []metrics.Sample {
			ctx, cancel := context.WithTimeout(context.Background(), gaugeQueryTimeout)
			defer cancel()
			rows, err := db.ListEPGSourceFreshness(ctx)
			if err != nil {
				return nil
			}
			out := make([]metrics.Sample, 0, len(rows))
			for _, r := range rows {
				age := -1.0
				if r.LastOKAt != nil {
					age = time.Since(*r.LastOKAt).Seconds()
				}
				out = append(out, metrics.Sample{
					Labels: metrics.Label("source", r.Name),
					Value:  age,
				})
			}
			return out
		})
}
