// admin_sports.go — admin handlers for sports schedule mappings
// (Phase 5b). Mirrors the existing patterns in admin.go.
package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/store"
)

// GET /admin/sports/mappings — list all enabled channel→league mappings.
func (a *admin) listSportMappings(w http.ResponseWriter, r *http.Request) {
	mappings, err := a.d.DB.ListChannelSportMappings(r.Context())
	if err != nil {
		http.Error(w, "list mappings: "+err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, mappings)
}

// POST /admin/sports/mappings — upsert one mapping. Body shape:
//
//	{
//	  "channel_id": "...",
//	  "league": "nfl",
//	  "team_name": "",                  // optional
//	  "broadcaster_filter": "",         // optional
//	  "enabled": true,                  // optional, default true
//	  "note": ""                        // optional
//	}
//
// Returns the persisted row (with id assigned for new entries).
func (a *admin) upsertSportMapping(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ChannelID         uuid.UUID `json:"channel_id"`
		League            string    `json:"league"`
		TeamName          string    `json:"team_name"`
		BroadcasterFilter string    `json:"broadcaster_filter"`
		Enabled           *bool     `json:"enabled,omitempty"`
		Note              string    `json:"note"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.ChannelID == uuid.Nil {
		http.Error(w, "channel_id required", http.StatusBadRequest)
		return
	}
	if req.League == "" {
		http.Error(w, "league required", http.StatusBadRequest)
		return
	}
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	m := store.ChannelSportMapping{
		ChannelID:         req.ChannelID,
		League:            req.League,
		TeamName:          req.TeamName,
		BroadcasterFilter: req.BroadcasterFilter,
		Enabled:           enabled,
		Note:              req.Note,
	}
	out, err := a.d.DB.UpsertChannelSportMapping(r.Context(), m)
	if err != nil {
		http.Error(w, "upsert: "+err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, out)
}

// DELETE /admin/sports/mappings/{id} — remove one mapping. The
// schedule worker stops generating EPG entries for this mapping on
// its next pass; existing synthetic epg_program rows stay until
// PurgeOldPrograms reaps them.
func (a *admin) deleteSportMapping(w http.ResponseWriter, r *http.Request) {
	rawID := r.PathValue("id")
	id, err := uuid.Parse(rawID)
	if err != nil {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}
	if err := a.d.DB.DeleteChannelSportMapping(r.Context(), id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			http.NotFound(w, r)
			return
		}
		http.Error(w, "delete: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// POST /admin/sports/refresh — kick the sports worker for an
// out-of-band sync. The handler returns immediately; the actual
// refresh runs in the worker's goroutine. Useful when the operator
// has just added new mappings and wants Plex's guide updated without
// waiting for the next 30-min tick.
//
// We don't expose direct worker handle here (avoids a dep cycle
// admin↔sports); instead this endpoint returns 204 and the operator
// can hit /admin/epg/refresh or simply wait. Reserved for the case
// when we wire a per-worker `Trigger()` channel later.
func (a *admin) refreshSports(w http.ResponseWriter, r *http.Request) {
	// No-op for now — placeholder so the route exists. Implementation
	// will land when the worker exposes a Trigger channel.
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(`{"status":"queued","note":"sports worker runs every 30m; out-of-band trigger not yet wired"}`))
}
