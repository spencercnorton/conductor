package enrich

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/store"
)

// StoreAdapter wraps *store.DB to satisfy enrich.EnrichmentStore +
// enrich.TMDbCache + enrich.ManualLookup. Lives in the enrich package
// so the store package stays free of any enrich-specific types.
type StoreAdapter struct {
	DB *store.DB
}

// ─────────── EnrichmentStore (worker side) ───────────

func (a *StoreAdapter) ListPendingEnrichment(ctx context.Context, limit int) ([]PendingProgram, error) {
	rows, err := a.DB.ListPendingEnrichment(ctx, limit)
	if err != nil {
		return nil, err
	}
	out := make([]PendingProgram, 0, len(rows))
	for _, r := range rows {
		out = append(out, PendingProgram{
			ID: r.ID, ChannelID: r.ChannelID,
			Title:           r.Title,
			OriginalAirDate: r.OriginalAirDate,
			IsMovie:         r.IsMovie, EpisodeNumOnscreen: r.EpisodeNumOnscreen,
			EnrichmentInputKey: r.EnrichmentInputKey,
		})
	}
	return out, nil
}

func (a *StoreAdapter) UpsertEnrichmentResult(ctx context.Context, programID uuid.UUID, inputKey string, result *Enrichment) error {
	if result == nil {
		return nil
	}
	return a.DB.UpsertEnrichmentResultGuarded(ctx, programID, inputKey, store.EnrichmentResult{
		TMDbID:        result.TMDbID,
		TVDBID:        result.TVDBID,
		IMDbID:        result.IMDbID,
		PosterURL:     result.PosterURL,
		BackdropURL:   result.BackdropURL,
		Overview:      result.Overview,
		Confidence:    result.Confidence,
		MatchedSource: result.MatchedSource,
	})
}

func (a *StoreAdapter) MarkEnrichmentFailed(ctx context.Context, programID uuid.UUID, inputKey, _ string) error {
	return a.DB.MarkEnrichmentFailedGuarded(ctx, programID, inputKey)
}

func (a *StoreAdapter) DeferEnrichment(ctx context.Context, programID uuid.UUID, inputKey string, delay time.Duration) error {
	return a.DB.DeferEnrichmentGuarded(ctx, programID, inputKey, delay)
}

// ─────────── ManualLookup (used inside enricher) ───────────

func (a *StoreAdapter) LookupManualMatch(
	ctx context.Context, channelID uuid.UUID, normalizedTitle string, isMovie bool,
) (int, int, string, bool, error) {
	return a.DB.LookupManualMatch(ctx, channelID, normalizedTitle, isMovie)
}

// ─────────── TMDbCache (passed to TMDbClient) ───────────

func (a *StoreAdapter) Get(ctx context.Context, kind, queryHash string) (json.RawMessage, bool, error) {
	return a.DB.GetTMDbCache(ctx, kind, queryHash)
}

func (a *StoreAdapter) Put(ctx context.Context, kind, queryHash, queryLabel string, response json.RawMessage) error {
	return a.DB.PutTMDbCache(ctx, kind, queryHash, queryLabel, response)
}

// NormalizeForMatch is the public hook callers use to compute the
// title_norm field consistently — both the worker (when looking up
// overrides) and the admin endpoint (when storing them).
func NormalizeForMatch(s string) string { return normalizeForMatch(s) }
