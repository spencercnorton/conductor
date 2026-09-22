package enrich

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
)

// PendingProgram is what the worker reads from the DB.
type PendingProgram struct {
	ID                 uuid.UUID
	ChannelID          uuid.UUID
	Title              string
	OriginalAirDate    *time.Time
	IsMovie            bool
	EpisodeNumOnscreen string
	EnrichmentInputKey string
}

// EnrichmentStore is the contract the worker depends on for reading
// pending rows and persisting results.
type EnrichmentStore interface {
	ListPendingEnrichment(ctx context.Context, limit int) ([]PendingProgram, error)
	UpsertEnrichmentResult(ctx context.Context, programID uuid.UUID, inputKey string, result *Enrichment) error
	MarkEnrichmentFailed(ctx context.Context, programID uuid.UUID, inputKey, reason string) error
	DeferEnrichment(ctx context.Context, programID uuid.UUID, inputKey string, delay time.Duration) error
	ManualLookup
}

// Worker drains epg_program rows whose enrichment_status='pending'.
// Runs every Interval; processes up to BatchSize rows per pass; rate-limits
// outbound TMDb calls via a small sleep between requests.
type Worker struct {
	Logger         *slog.Logger
	Enricher       *Enricher
	Store          EnrichmentStore
	Interval       time.Duration // default 5m
	BatchSize      int           // default 50 per pass
	BetweenRequest time.Duration // default 200ms ≈ 5 req/s
}

func NewWorker(logger *slog.Logger, enricher *Enricher, store EnrichmentStore) *Worker {
	return &Worker{
		Logger: logger, Enricher: enricher, Store: store,
		Interval: 5 * time.Minute, BatchSize: 50, BetweenRequest: 200 * time.Millisecond,
	}
}

// Run blocks until ctx is cancelled. Safe to call exactly once per Worker.
func (w *Worker) Run(ctx context.Context) {
	w.Logger.Info("enrichment worker starting",
		"interval", w.Interval, "batch_size", w.BatchSize)

	w.runOnce(ctx)

	t := time.NewTicker(w.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			w.Logger.Info("enrichment worker stopped")
			return
		case <-t.C:
			w.runOnce(ctx)
		}
	}
}

// RunOnce runs a single pass; useful for the manual-trigger admin endpoint.
func (w *Worker) RunOnce(ctx context.Context) (matched, failed, skipped int) {
	return w.runOnce(ctx)
}

func (w *Worker) runOnce(ctx context.Context) (matched, failed, skipped int) {
	pending, err := w.Store.ListPendingEnrichment(ctx, w.BatchSize)
	if err != nil {
		w.Logger.Warn("enrichment list failed", "err", err)
		return 0, 0, 0
	}
	if len(pending) == 0 {
		return 0, 0, 0
	}

	for _, p := range pending {
		select {
		case <-ctx.Done():
			break
		default:
		}

		result, err := w.Enricher.Enrich(ctx, EnrichInput{
			ProgramID:          p.ID,
			ChannelID:          p.ChannelID,
			Title:              p.Title,
			OriginalAirDate:    p.OriginalAirDate,
			IsMovie:            p.IsMovie,
			EpisodeNumOnscreen: p.EpisodeNumOnscreen,
		})
		switch {
		case err != nil:
			w.Logger.Warn("enrichment errored",
				"program", p.ID, "title", p.Title, "err", err)
			// HTTP/rate-limit/provider errors are not proof that no artwork
			// exists. Defer instead of failing: the row remains ineligible for
			// generated fallback art, and moving it out 30 minutes prevents a
			// provider outage from starving every later guide row.
			_ = w.Store.DeferEnrichment(ctx, p.ID, p.EnrichmentInputKey, 30*time.Minute)
			failed++
		case result == nil:
			// No match found. Mark failed so we don't keep re-trying;
			// operator can clear via manual override.
			_ = w.Store.MarkEnrichmentFailed(ctx, p.ID, p.EnrichmentInputKey, "no acceptable match")
			skipped++
		default:
			if err := w.Store.UpsertEnrichmentResult(ctx, p.ID, p.EnrichmentInputKey, result); err != nil {
				w.Logger.Warn("enrichment persist failed",
					"program", p.ID, "err", err)
				failed++
				continue
			}
			matched++
		}

		// Rate limit between requests so TMDb's free tier is happy.
		select {
		case <-ctx.Done():
			break
		case <-time.After(w.BetweenRequest):
		}
	}

	w.Logger.Info("enrichment pass complete",
		"pending_attempted", len(pending),
		"matched", matched, "failed", failed, "skipped", skipped)
	return
}
