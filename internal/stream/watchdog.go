// watchdog.go — orphan upstream sweeper.
//
// Spec §4.2 calls for three independent mechanisms to teardown orphans:
//
//  1. TCP close detection — handled inline in Pool.Serve (write error →
//     ReleaseClient → MarkDead if last subscriber).
//  2. Heartbeat watchdog — Phase 1 deferred (no per-client heartbeat row;
//     we track per-stream heartbeat which the sweeper uses).
//  3. Orphan upstream sweeper — implemented here.
//
// The sweeper runs every SweepInterval. Stale zero-client rows first become
// capacity-counted `draining` rows and their local pumps are cancelled; only
// rows already marked dead after teardown are deleted.
package stream

import (
	"context"
	"log/slog"
	"time"

	"github.com/spencercnorton/conductor/internal/store"
)

type WatchdogConfig struct {
	SweepInterval time.Duration // how often to scan
	IdleTimeout   time.Duration // grace period before reaping idle streams
	// HardTimeout is deliberately longer than the complete reconnect window.
	// A positive durable-heartbeat age alone never kills a local pump: Pool
	// independently requires the matching generation's real-media activity to
	// exceed this ceiling too.
	HardTimeout time.Duration
}

const DefaultStreamHardTimeout = 3 * time.Minute

func DefaultWatchdogConfig() WatchdogConfig {
	return WatchdogConfig{
		SweepInterval: 30 * time.Second,
		IdleTimeout:   10 * time.Second,
		HardTimeout:   DefaultStreamHardTimeout,
	}
}

// Watchdog runs the orphan sweeper until ctx is cancelled. pool may be nil
// (tests); when set, pumps belonging to draining rows are cancelled before
// their DB capacity is released (audit S3).
func Watchdog(ctx context.Context, db *store.DB, logger *slog.Logger, cfg WatchdogConfig, pool *Pool) {
	t := time.NewTicker(cfg.SweepInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			swept, err := db.SweepOrphans(ctx, int(cfg.IdleTimeout/time.Second))
			if err != nil {
				logger.Warn("sweep orphans", "err", err)
			} else if len(swept) > 0 {
				if pool != nil {
					pool.CancelPumps(swept)
				}
				logger.Info("advanced orphan teardown", "count", len(swept))
			}

			// The runtime singleton makes the process-local registry authoritative.
			// Without it (pool=nil in repository tests), a stale DB heartbeat is
			// insufficient evidence to consume live client tokens.
			if pool == nil {
				continue
			}
			hardTimeout := cfg.HardTimeout
			if hardTimeout <= 0 {
				hardTimeout = DefaultStreamHardTimeout
			}
			cutoff := time.Now().Add(-hardTimeout)
			candidates, err := db.ListHardStaleStreams(ctx, cutoff)
			if err != nil {
				logger.Warn("list hard-stale streams", "err", err)
				continue
			}
			if reaped := pool.ReapHardStaleStreams(ctx, candidates, cutoff); reaped > 0 {
				logger.Warn("advanced hard-stale stream teardown", "count", reaped)
			}
		}
	}
}
