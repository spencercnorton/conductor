// admin.go — minimal JSON CRUD for providers/credentials/channels/sources/lineups.
//
// Phase 1 ergonomics: just enough surface to seed a working install via
// curl/httpie. Phase 6 layers a React UI onto these same endpoints.
//
// Auth: MountAdmin wraps the complete surface with delegated SSO or the static
// admin Bearer key and fails closed in DB mode when auth is disabled.
package api

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/alerts"
	"github.com/spencercnorton/conductor/internal/auth"
	"github.com/spencercnorton/conductor/internal/dvr"
	"github.com/spencercnorton/conductor/internal/enrich"
	"github.com/spencercnorton/conductor/internal/epg"
	"github.com/spencercnorton/conductor/internal/reconcile"
	"github.com/spencercnorton/conductor/internal/security"
	"github.com/spencercnorton/conductor/internal/store"
)

type AdminDeps struct {
	Logger        *slog.Logger
	DB            *store.DB
	CredKey       *security.CredKey
	Auth          auth.Config           // injected from main; pass-through when disabled
	EPG           *epg.Worker           // optional; when set POST /admin/epg/refresh works
	DVRSchedule   *dvr.ScheduleHandler  // optional; authenticated explicit re-record target binding
	DVRScheduler  *dvr.Scheduler        // optional; when set DELETE /admin/dvr/recordings/{id} cancels in-flight
	Reconciler    *reconcile.Worker     // optional; when set GET /admin/dvr/reconcile/preview works
	EnrichWorker  *enrich.Worker        // optional; when set POST /admin/enrich/refresh works
	PosterWorker  *enrich.PosterWorker  // optional; poster override + generation queue
	PPVMonitor    *alerts.WorkerMonitor // optional; surfaced by GET /admin/epg/health
	SportsMonitor *alerts.WorkerMonitor // optional; surfaced by GET /admin/epg/health
}

// MountAdmin attaches the /admin/* routes to mux. All admin routes are
// wrapped in the auth middleware. Routes register on a private sub-mux so
// the wrapping is local — leaving healthchecks, /xmltv.xml, and HDHomeRun
// surfaces unauthenticated (Plex can't speak JWT).
func MountAdmin(mux *http.ServeMux, d AdminDeps) {
	a := &admin{d: d}
	sub := http.NewServeMux()

	// Providers
	sub.HandleFunc("GET /admin/providers", a.listProviders)
	sub.HandleFunc("POST /admin/providers", a.createProvider)
	sub.HandleFunc("PATCH /admin/providers/{provider_id}", a.updateProvider)

	// Credentials
	sub.HandleFunc("GET /admin/providers/{provider_id}/credentials", a.listCredentials)
	sub.HandleFunc("POST /admin/providers/{provider_id}/credentials", a.createCredential)

	// Channels
	sub.HandleFunc("GET /admin/channels", a.listChannels)
	sub.HandleFunc("POST /admin/channels", a.createChannel)
	sub.HandleFunc("PATCH /admin/channels/{channel_id}", a.updateChannel)
	sub.HandleFunc("GET /admin/channels/{channel_id}/sources", a.listChannelSources)
	sub.HandleFunc("POST /admin/channels/{channel_id}/sources", a.createChannelSource)
	sub.HandleFunc("PATCH /admin/channels/{channel_id}/sources/{source_id}", a.updateChannelSource)

	// Lineups
	sub.HandleFunc("POST /admin/lineups", a.createLineup)
	sub.HandleFunc("POST /admin/lineups/{lineup_id}/channels", a.addChannelToLineup)

	// Active streams (read-only for now; kill button comes in Phase 6)
	sub.HandleFunc("GET /admin/streams", a.listActiveStreams)

	// EPG admin
	sub.HandleFunc("GET /admin/epg/sources", a.listEPGSources)
	sub.HandleFunc("POST /admin/epg/sources", a.createEPGSource)
	sub.HandleFunc("PATCH /admin/epg/sources/{source_id}", a.updateEPGSource)
	sub.HandleFunc("DELETE /admin/epg/sources/{source_id}", a.deleteEPGSource)
	sub.HandleFunc("POST /admin/epg/refresh", a.refreshEPG)
	sub.HandleFunc("GET /admin/epg/health", a.epgHealth)

	// DVR admin (Phase 3a)
	sub.HandleFunc("GET /admin/dvr/recordings", a.listDVRRecordings)
	sub.HandleFunc("GET /admin/dvr/recordings/{id}", a.getDVRRecording)
	sub.HandleFunc("DELETE /admin/dvr/recordings/{id}", a.cancelDVRRecording)
	sub.HandleFunc("POST /admin/dvr/recordings/{id}/rerecord", a.scheduleDVRRerecord)
	sub.HandleFunc("GET /admin/dvr/reconcile/preview", a.reconcilePreview)

	// Transcoding admin (Phase 4)
	sub.HandleFunc("GET /admin/transcode/profiles", a.listTranscodeProfiles)
	sub.HandleFunc("POST /admin/transcode/profiles", a.createTranscodeProfile)
	sub.HandleFunc("GET /admin/transcode/profiles/{id}", a.getTranscodeProfile)
	sub.HandleFunc("PUT /admin/channels/{channel_id}/transcode", a.assignChannelTranscode)
	sub.HandleFunc("PUT /admin/channels/{channel_id}/sources/{source_id}/transcode", a.assignSourceTranscode)

	// Enrichment admin (Phase 3)
	sub.HandleFunc("POST /admin/enrich/refresh", a.refreshEnrichment)
	sub.HandleFunc("GET /admin/enrich/manual-matches", a.listManualMatches)
	sub.HandleFunc("POST /admin/enrich/manual-matches", a.createManualMatch)
	sub.HandleFunc("GET /admin/posters/overrides", a.listPosterOverrides)
	sub.HandleFunc("POST /admin/posters/overrides", a.createPosterOverride)
	sub.HandleFunc("POST /admin/posters/refresh", a.refreshPosters)

	// Sports schedule admin (Phase 5b)
	sub.HandleFunc("GET /admin/sports/mappings", a.listSportMappings)
	sub.HandleFunc("POST /admin/sports/mappings", a.upsertSportMapping)
	sub.HandleFunc("DELETE /admin/sports/mappings/{id}", a.deleteSportMapping)
	sub.HandleFunc("POST /admin/sports/refresh", a.refreshSports)

	// Schedules Direct admin (Phase 3b)
	sub.HandleFunc("GET /admin/sd/credentials", a.getSDCredentials)
	sub.HandleFunc("POST /admin/sd/credentials", a.upsertSDCredentials)
	sub.HandleFunc("GET /admin/sd/lineups", a.listSDLineups)
	sub.HandleFunc("POST /admin/sd/lineups", a.createSDLineup)

	// Fail-closed safety: in DB mode (admin endpoints exposing channel/
	// credential CRUD), refuse to mount /admin if auth is disabled. The
	// operator gets a clear 503 instead of an open surface — better than
	// silently degrading to "anyone can call it" because someone set
	// CONDUCTOR_AUTH_ENABLED=false.
	mw := auth.Middleware(d.Auth)
	if !d.Auth.Enabled && d.DB != nil {
		mux.Handle("/admin/", failClosedHandler())
		return
	}
	mux.Handle("/admin/", mw(sub))
}

// failClosedHandler returns 503 with a clear setup hint. Ensures admin
// surface never serves unauthenticated requests in DB mode.
func failClosedHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeErr(w, http.StatusServiceUnavailable,
			"admin disabled: auth is off (CONDUCTOR_AUTH_ENABLED=false). Re-enable it (and set CONDUCTOR_ADMIN_API_KEY) to expose /admin in DB mode")
	})
}

type admin struct {
	d AdminDeps

	// markDVRRecordingCancelled is a narrow test seam for the network-only
	// boundary where PostgreSQL commits cancellation but its response is lost.
	// Production handlers use DB.MarkDVRRecordingCancelled directly.
	markDVRRecordingCancelled func(context.Context, uuid.UUID) error
}

// ─────────────────────── providers ───────────────────────

type createProviderReq struct {
	Name           string `json:"name"`
	Kind           string `json:"kind"`
	BaseURL        string `json:"base_url"`
	CapacityDomain string `json:"capacity_domain"`
	Notes          string `json:"notes"`
	Enabled        *bool  `json:"enabled"`
}

func (a *admin) createProvider(w http.ResponseWriter, r *http.Request) {
	var req createProviderReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if req.Name == "" || req.Kind == "" || req.BaseURL == "" {
		writeErr(w, http.StatusBadRequest, "name, kind, base_url are required")
		return
	}
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	p, err := a.d.DB.CreateProvider(r.Context(), store.Provider{
		Name: req.Name, Kind: req.Kind, BaseURL: req.BaseURL,
		CapacityDomain: req.CapacityDomain, Notes: req.Notes, Enabled: enabled,
	})
	if errors.Is(err, store.ErrInvalidProviderCapacityDomain) {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, p)
}

func (a *admin) listProviders(w http.ResponseWriter, r *http.Request) {
	ps, err := a.d.DB.ListProviders(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, ps)
}

// updateProviderReq is intentionally narrow: quota grouping is the only
// provider mutation currently safe to expose without changing credentials or
// source ownership. A pointer distinguishes an omitted sparse field from the
// explicit empty string that restores isolated-provider semantics.
type updateProviderReq struct {
	CapacityDomain *string `json:"capacity_domain"`
}

func (a *admin) updateProvider(w http.ResponseWriter, r *http.Request) {
	providerID, err := uuid.Parse(r.PathValue("provider_id"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid provider_id")
		return
	}
	var req updateProviderReq
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeErr(w, http.StatusBadRequest, "invalid json: exactly one object is required")
		return
	}
	if req.CapacityDomain == nil {
		writeErr(w, http.StatusBadRequest, "capacity_domain is required")
		return
	}

	p, err := a.d.DB.UpdateProviderCapacityDomain(
		r.Context(), providerID, *req.CapacityDomain)
	switch {
	case errors.Is(err, store.ErrInvalidProviderCapacityDomain):
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	case errors.Is(err, store.ErrNotFound):
		writeErr(w, http.StatusNotFound, "provider not found")
		return
	case errors.Is(err, store.ErrProviderCapacityDomainBusy):
		writeErr(w, http.StatusConflict,
			"capacity domain change requires both affected domains to be idle")
		return
	case err != nil:
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}

	// The boundary mutation is durable before forecast refresh. Await one full
	// recomputation so schedule-time conflicts converge immediately. A failed
	// observation is reported honestly; an exact retry is idempotent and retries
	// only the forecast because the requested domain is already installed.
	if a.d.DVRScheduler != nil {
		err = a.d.DVRScheduler.RefreshAdmissionForecast(r.Context())
	} else {
		err = a.d.DB.RefreshDVRAdmissionForecast(r.Context(), store.DVRAdmissionPolicy{
			LiveReserve: dvr.DefaultLiveReserve,
			StartSlack:  dvr.DefaultStartSlack,
			MaxOverrun:  dvr.DefaultMaxOverrun,
		})
	}
	if err != nil {
		if a.d.Logger != nil {
			a.d.Logger.Error("provider capacity domain updated but DVR forecast refresh failed",
				"provider_id", providerID, "err", err)
		}
		writeErr(w, http.StatusServiceUnavailable,
			"capacity domain updated; DVR admission forecast refresh failed, retry the same PATCH")
		return
	}
	writeJSON(w, p)
}

// ─────────────────────── credentials ───────────────────────

type createCredentialReq struct {
	Username   string `json:"username"`
	Password   string `json:"password"` // cleartext; encrypted before persist
	MaxStreams int    `json:"max_streams"`
	Priority   int    `json:"priority"`
	Notes      string `json:"notes"`
	Enabled    *bool  `json:"enabled"`
}

type credentialView struct {
	ID         uuid.UUID `json:"id"`
	ProviderID uuid.UUID `json:"provider_id"`
	Username   string    `json:"username"`
	MaxStreams int       `json:"max_streams"`
	Priority   int       `json:"priority"`
	Notes      string    `json:"notes"`
	Enabled    bool      `json:"enabled"`
	// PasswordEnc deliberately omitted from API.
}

func toCredentialView(c store.ProviderCredential) credentialView {
	return credentialView{
		ID: c.ID, ProviderID: c.ProviderID, Username: c.Username,
		MaxStreams: c.MaxStreams, Priority: c.Priority, Notes: c.Notes, Enabled: c.Enabled,
	}
}

func (a *admin) createCredential(w http.ResponseWriter, r *http.Request) {
	provID, err := uuid.Parse(r.PathValue("provider_id"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid provider_id")
		return
	}
	var req createCredentialReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if req.Username == "" || req.MaxStreams <= 0 {
		writeErr(w, http.StatusBadRequest, "username and max_streams required")
		return
	}
	if a.d.CredKey == nil {
		writeErr(w, http.StatusServiceUnavailable, "CONDUCTOR_CRED_KEY not configured")
		return
	}
	enc, err := a.d.CredKey.Encrypt([]byte(req.Password))
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "encrypt: "+err.Error())
		return
	}
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	c, err := a.d.DB.CreateCredential(r.Context(), store.ProviderCredential{
		ProviderID: provID, Username: req.Username, PasswordEnc: enc,
		MaxStreams: req.MaxStreams, Priority: req.Priority, Notes: req.Notes, Enabled: enabled,
	})
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, toCredentialView(c))
}

func (a *admin) listCredentials(w http.ResponseWriter, r *http.Request) {
	provID, err := uuid.Parse(r.PathValue("provider_id"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid provider_id")
		return
	}
	cs, err := a.d.DB.ListCredentials(r.Context(), provID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	out := make([]credentialView, 0, len(cs))
	for _, c := range cs {
		out = append(out, toCredentialView(c))
	}
	writeJSON(w, out)
}

// ─────────────────────── channels ───────────────────────

type createChannelReq struct {
	Number       float64 `json:"number"`
	Name         string  `json:"name"`
	CallSign     string  `json:"call_sign"`
	LogoURL      string  `json:"logo_url"`
	GroupTag     string  `json:"group_tag"`
	Enabled      *bool   `json:"enabled"`
	EpgChannelID string  `json:"epg_channel_id"`
}

func (a *admin) createChannel(w http.ResponseWriter, r *http.Request) {
	var req createChannelReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if req.Number == 0 || req.Name == "" {
		writeErr(w, http.StatusBadRequest, "number and name required")
		return
	}
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	c, err := a.d.DB.CreateChannel(r.Context(), store.Channel{
		Number: req.Number, Name: req.Name, CallSign: req.CallSign,
		LogoURL: req.LogoURL, GroupTag: req.GroupTag, Enabled: enabled,
		EpgChannelID: req.EpgChannelID,
	})
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, c)
}

func (a *admin) listChannels(w http.ResponseWriter, r *http.Request) {
	var (
		cs  []store.Channel
		err error
	)
	if include, _ := strconv.ParseBool(r.URL.Query().Get("include_disabled")); include {
		cs, err = a.d.DB.ListAllChannels(r.Context())
	} else {
		cs, err = a.d.DB.ListChannels(r.Context())
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, cs)
}

// listChannelSources returns every channel_source for one channel, in
// priority+health order — the same ordering AcquireLease walks. Exposes
// source id, upstream_url, priority, health_score, last_failure_at and
// enabled so an operator (or repair tooling / the future UI) can see which
// upstreams a channel has and patch a stale one via PATCH .../sources/{id}.
func (a *admin) listChannelSources(w http.ResponseWriter, r *http.Request) {
	chID, err := uuid.Parse(r.PathValue("channel_id"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid channel_id")
		return
	}
	srcs, err := a.d.DB.ListSourcesForChannel(r.Context(), chID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, srcs)
}

type createChannelSourceReq struct {
	ProviderID  uuid.UUID `json:"provider_id"`
	UpstreamURL string    `json:"upstream_url"`
	Priority    int       `json:"priority"`
	Enabled     *bool     `json:"enabled"`
}

func (a *admin) createChannelSource(w http.ResponseWriter, r *http.Request) {
	chID, err := uuid.Parse(r.PathValue("channel_id"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid channel_id")
		return
	}
	var req createChannelSourceReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if req.ProviderID == uuid.Nil || req.UpstreamURL == "" {
		writeErr(w, http.StatusBadRequest, "provider_id and upstream_url required")
		return
	}
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	s, err := a.d.DB.CreateChannelSource(r.Context(), store.ChannelSource{
		ChannelID: chID, ProviderID: req.ProviderID, UpstreamURL: req.UpstreamURL,
		Priority: req.Priority, HealthScore: 1.0, Enabled: enabled,
	})
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, s)
}

// updateChannelSourceReq is a sparse PATCH body — every field is optional.
// Omit a field to leave it unchanged. Use reset_failure=true to clear
// last_failure_at after manually fixing an upstream URL.
type updateChannelSourceReq struct {
	UpstreamURL  *string    `json:"upstream_url,omitempty"`
	Priority     *int       `json:"priority,omitempty"`
	Enabled      *bool      `json:"enabled,omitempty"`
	HealthScore  *float64   `json:"health_score,omitempty"`
	ProviderID   *uuid.UUID `json:"provider_id,omitempty"`
	ResetFailure bool       `json:"reset_failure,omitempty"`
}

func (a *admin) updateChannelSource(w http.ResponseWriter, r *http.Request) {
	chID, err := uuid.Parse(r.PathValue("channel_id"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid channel_id")
		return
	}
	srcID, err := uuid.Parse(r.PathValue("source_id"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid source_id")
		return
	}
	var req updateChannelSourceReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if req.UpstreamURL != nil && *req.UpstreamURL == "" {
		writeErr(w, http.StatusBadRequest, "upstream_url cannot be empty")
		return
	}
	if req.Priority != nil && *req.Priority < 0 {
		writeErr(w, http.StatusBadRequest, "priority must be >= 0")
		return
	}
	if req.HealthScore != nil && (*req.HealthScore < 0.0 || *req.HealthScore > 1.0) {
		writeErr(w, http.StatusBadRequest, "health_score must be between 0.0 and 1.0")
		return
	}
	if req.ProviderID != nil && *req.ProviderID == uuid.Nil {
		writeErr(w, http.StatusBadRequest, "provider_id cannot be the zero UUID")
		return
	}
	s, err := a.d.DB.UpdateChannelSource(r.Context(), chID, srcID, store.ChannelSourceUpdate{
		UpstreamURL:  req.UpstreamURL,
		Priority:     req.Priority,
		Enabled:      req.Enabled,
		HealthScore:  req.HealthScore,
		ProviderID:   req.ProviderID,
		ResetFailure: req.ResetFailure,
	})
	if errors.Is(err, store.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "no source with that id under that channel")
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, s)
}

// ─────────────────────── lineups ───────────────────────

type createLineupReq struct {
	Name       string `json:"name"`
	DeviceUUID string `json:"device_uuid"`
}

func (a *admin) createLineup(w http.ResponseWriter, r *http.Request) {
	var req createLineupReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if req.Name == "" || req.DeviceUUID == "" {
		writeErr(w, http.StatusBadRequest, "name and device_uuid required")
		return
	}
	l, err := a.d.DB.CreateLineup(r.Context(), store.Lineup{Name: req.Name, DeviceUUID: req.DeviceUUID})
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, l)
}

type addChannelToLineupReq struct {
	ChannelID uuid.UUID `json:"channel_id"`
	Number    float64   `json:"number"`
	Position  int       `json:"position"`
}

func (a *admin) addChannelToLineup(w http.ResponseWriter, r *http.Request) {
	lineupID, err := uuid.Parse(r.PathValue("lineup_id"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid lineup_id")
		return
	}
	var req addChannelToLineupReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if req.ChannelID == uuid.Nil || req.Number == 0 {
		writeErr(w, http.StatusBadRequest, "channel_id and number required")
		return
	}
	err = a.d.DB.AddChannelToLineup(r.Context(), store.LineupChannel{
		LineupID: lineupID, ChannelID: req.ChannelID, Number: req.Number, Position: req.Position,
	})
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, map[string]any{"ok": true})
}

// ─────────────────────── active streams (dashboard) ───────────────────

// activeStreamView is the safe-for-API view of an active_stream row.
// active_stream.upstream_url contains the RESOLVED URL with credential
// password substituted in (per spec §4.1) — never expose that via API.
// The rest of the fields are operator-useful runtime state.
type activeStreamView struct {
	ID              string `json:"id"`
	ChannelID       string `json:"channel_id"`
	ChannelSourceID string `json:"channel_source_id"`
	CredentialID    string `json:"credential_id"`
	StartedAt       string `json:"started_at"`
	LastHeartbeat   string `json:"last_heartbeat"`
	ClientCount     int    `json:"client_count"`
	State           string `json:"state"`
	BytesOut        int64  `json:"bytes_out"`
}

func (a *admin) listActiveStreams(w http.ResponseWriter, r *http.Request) {
	rows, err := a.d.DB.ListActiveStreams(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	out := make([]activeStreamView, 0, len(rows))
	for _, s := range rows {
		out = append(out, activeStreamView{
			ID:              s.ID.String(),
			ChannelID:       s.ChannelID.String(),
			ChannelSourceID: s.ChannelSourceID.String(),
			CredentialID:    s.CredentialID.String(),
			StartedAt:       s.StartedAt.Format(time.RFC3339),
			LastHeartbeat:   s.LastHeartbeat.Format(time.RFC3339),
			ClientCount:     s.ClientCount,
			State:           s.State,
			BytesOut:        s.BytesOut,
		})
	}
	writeJSON(w, out)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// ─────────────────────── EPG sources ───────────────────────

type createEPGSourceReq struct {
	Name       string `json:"name"`
	URL        string `json:"url"`
	AuthHeader string `json:"auth_header"`
	Priority   int    `json:"priority"`
	Enabled    *bool  `json:"enabled"`
}

func (a *admin) createEPGSource(w http.ResponseWriter, r *http.Request) {
	var req createEPGSourceReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if req.Name == "" || req.URL == "" {
		writeErr(w, http.StatusBadRequest, "name and url required")
		return
	}
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	s, err := a.d.DB.CreateEPGSource(r.Context(), store.EPGSource{
		Name: req.Name, URL: req.URL, AuthHeader: req.AuthHeader,
		Priority: req.Priority, Enabled: enabled,
	})
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, s)
}

func (a *admin) listEPGSources(w http.ResponseWriter, r *http.Request) {
	ss, err := a.d.DB.ListEPGSources(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, ss)
}

type updateChannelReq struct {
	Name         *string `json:"name"`
	CallSign     *string `json:"call_sign"`
	LogoURL      *string `json:"logo_url"`
	GroupTag     *string `json:"group_tag"`
	EpgChannelID *string `json:"epg_channel_id"`
	Enabled      *bool   `json:"enabled"`
}

func (a *admin) updateChannel(w http.ResponseWriter, r *http.Request) {
	chID, err := uuid.Parse(r.PathValue("channel_id"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid channel_id")
		return
	}
	var req updateChannelReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if req.Name != nil && *req.Name == "" {
		writeErr(w, http.StatusBadRequest, "name cannot be empty")
		return
	}
	c, err := a.d.DB.UpdateChannel(r.Context(), chID, store.ChannelUpdate{
		Name:         req.Name,
		CallSign:     req.CallSign,
		LogoURL:      req.LogoURL,
		GroupTag:     req.GroupTag,
		EpgChannelID: req.EpgChannelID,
		Enabled:      req.Enabled,
	})
	if errors.Is(err, store.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "no channel with that id")
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, c)
}

type updateEPGSourceReq struct {
	Name       *string `json:"name"`
	URL        *string `json:"url"`
	AuthHeader *string `json:"auth_header"`
	Priority   *int    `json:"priority"`
	Enabled    *bool   `json:"enabled"`
}

func (a *admin) updateEPGSource(w http.ResponseWriter, r *http.Request) {
	srcID, err := uuid.Parse(r.PathValue("source_id"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid source_id")
		return
	}
	var req updateEPGSourceReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if req.Name != nil && *req.Name == "" {
		writeErr(w, http.StatusBadRequest, "name cannot be empty")
		return
	}
	if req.URL != nil && *req.URL == "" {
		writeErr(w, http.StatusBadRequest, "url cannot be empty")
		return
	}
	if req.Priority != nil && *req.Priority < 0 {
		writeErr(w, http.StatusBadRequest, "priority must be >= 0")
		return
	}
	s, err := a.d.DB.UpdateEPGSource(r.Context(), srcID, store.EPGSourceUpdate{
		Name:       req.Name,
		URL:        req.URL,
		AuthHeader: req.AuthHeader,
		Priority:   req.Priority,
		Enabled:    req.Enabled,
	})
	if errors.Is(err, store.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "no epg source with that id")
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, s)
}

func (a *admin) deleteEPGSource(w http.ResponseWriter, r *http.Request) {
	srcID, err := uuid.Parse(r.PathValue("source_id"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid source_id")
		return
	}
	if err := a.d.DB.DeleteEPGSource(r.Context(), srcID); errors.Is(err, store.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "no epg source with that id")
		return
	} else if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *admin) refreshEPG(w http.ResponseWriter, r *http.Request) {
	if a.d.EPG == nil {
		writeErr(w, http.StatusServiceUnavailable, "EPG worker not configured")
		return
	}
	stats := a.d.EPG.RunOnce(r.Context())
	writeJSON(w, stats)
}

// epgHealth is the one-stop EPG freshness view (audit 2026-06-09, E5):
// per-source poll state + failure streaks, the guide-horizon metric the
// worker alerts on, and ppvsync/sports worker liveness. Designed for
// ops dashboards to poll.
func (a *admin) epgHealth(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	sources, err := a.d.DB.ListEPGSources(ctx)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}

	type sourceHealth struct {
		Name                string     `json:"name"`
		Enabled             bool       `json:"enabled"`
		LastStatus          string     `json:"last_status,omitempty"`
		LastFetchedAt       *time.Time `json:"last_fetched_at,omitempty"`
		LastOKAt            *time.Time `json:"last_ok_at,omitempty"`
		ConsecutiveFailures int        `json:"consecutive_failures"`
		SnapshotGeneration  int64      `json:"snapshot_generation"`
		SnapshotAppliedAt   *time.Time `json:"snapshot_applied_at,omitempty"`
		Failing             bool       `json:"failing"`
	}

	threshold := epg.DefaultFailureAlertThreshold
	horizonAlertDays := epg.DefaultHorizonAlertDays
	if a.d.EPG != nil {
		threshold = a.d.EPG.FailureAlertThreshold
		horizonAlertDays = a.d.EPG.HorizonAlertDays
	}

	out := struct {
		Sources            []sourceHealth           `json:"sources"`
		HorizonDays        float64                  `json:"horizon_days"`
		HorizonAlertDays   float64                  `json:"horizon_alert_days"`
		HorizonChannels    int                      `json:"horizon_channels"`
		CanonicalPrograms  int64                    `json:"canonical_programs"`
		SuppressedPrograms int64                    `json:"suppressed_programs"`
		CanonicalOverlaps  int64                    `json:"canonical_overlaps"`
		CanonicalHealthy   bool                     `json:"canonical_healthy"`
		Workers            []alerts.MonitorSnapshot `json:"workers"`
	}{
		Sources:          make([]sourceHealth, 0, len(sources)),
		HorizonAlertDays: horizonAlertDays,
		Workers:          make([]alerts.MonitorSnapshot, 0, 2),
	}

	for _, s := range sources {
		out.Sources = append(out.Sources, sourceHealth{
			Name:                s.Name,
			Enabled:             s.Enabled,
			LastStatus:          s.LastStatus,
			LastFetchedAt:       s.LastFetchedAt,
			LastOKAt:            s.LastOKAt,
			ConsecutiveFailures: s.ConsecutiveFailures,
			SnapshotGeneration:  s.SnapshotGeneration,
			SnapshotAppliedAt:   s.SnapshotAppliedAt,
			Failing:             s.Enabled && threshold > 0 && s.ConsecutiveFailures >= threshold,
		})
	}

	days, channels, err := a.d.DB.EPGGuideHorizonDays(ctx)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	out.HorizonDays = days
	// Scheduled (non-PPV) enabled channels behind horizon_days; PPV slots are
	// excluded from the median, see store.EPGGuideHorizonDays.
	out.HorizonChannels = channels
	canonical, suppressed, overlaps, err := a.d.DB.EPGCanonicalHealth(ctx)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	out.CanonicalPrograms = canonical
	out.SuppressedPrograms = suppressed
	out.CanonicalOverlaps = overlaps
	out.CanonicalHealthy = overlaps == 0

	if a.d.PPVMonitor != nil {
		out.Workers = append(out.Workers, a.d.PPVMonitor.Snapshot())
	}
	if a.d.SportsMonitor != nil {
		out.Workers = append(out.Workers, a.d.SportsMonitor.Snapshot())
	}

	if overlaps != 0 {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(out)
		return
	}
	writeJSON(w, out)
}

// ─────────────────────── DVR recordings ───────────────────────

func (a *admin) listDVRRecordings(w http.ResponseWriter, r *http.Request) {
	state := r.URL.Query().Get("state") // optional filter
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	rs, err := a.d.DB.ListDVRRecordings(r.Context(), state, limit)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, rs)
}

func (a *admin) getDVRRecording(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid id")
		return
	}
	rec, err := a.d.DB.GetDVRRecording(r.Context(), id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeErr(w, http.StatusNotFound, "not found")
			return
		}
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, rec)
}

func (a *admin) cancelDVRRecording(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid id")
		return
	}
	// Mark cancelled in DB regardless of whether it's in-flight — covers
	// both "not yet started" (just cancel the schedule) and "in flight"
	// (DB cancel + ask scheduler to abort the goroutine).
	markCancelled := a.markDVRRecordingCancelled
	if markCancelled == nil {
		markCancelled = a.d.DB.MarkDVRRecordingCancelled
	}
	cancelErr := markCancelled(r.Context(), id)
	// A PostgreSQL UPDATE can commit while the client loses its response. Stop
	// the local writer after every database attempt, not only a confirmed one;
	// Recorder's detached cancellation CAS safely retries either outcome. This
	// also keeps a durably-cancelled row from dropping its output-path guard
	// while the old process still writes the same .partial file.
	cancelled := false
	if a.d.DVRScheduler != nil {
		cancelled = a.d.DVRScheduler.Cancel(id)
	}
	if cancelErr != nil {
		writeErr(w, http.StatusInternalServerError, cancelErr.Error())
		return
	}
	writeJSON(w, map[string]any{"ok": true, "in_flight_cancelled": cancelled})
}

type scheduleDVRRerecordRequest struct {
	ProgramID uuid.UUID `json:"program_id"`
}

// scheduleDVRRerecord creates one successor immediately, binding operator
// authorization to both the current validated owner and a specific canonical
// future EPG programme. There is deliberately no loose "arm next airing" bit.
func (a *admin) scheduleDVRRerecord(w http.ResponseWriter, r *http.Request) {
	if a.d.DVRSchedule == nil {
		writeErr(w, http.StatusServiceUnavailable, "DVR scheduler is not configured")
		return
	}
	ownerID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid recording id")
		return
	}
	var req scheduleDVRRerecordRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil || req.ProgramID == uuid.Nil {
		writeErr(w, http.StatusBadRequest, "program_id is required")
		return
	}
	rec, err := a.d.DVRSchedule.ScheduleReplacement(
		r.Context(), ownerID, req.ProgramID, "admin:explicit-rerecord")
	if err != nil {
		switch {
		case errors.Is(err, store.ErrNotFound):
			writeErr(w, http.StatusNotFound, "recording or replacement programme not found")
		case errors.Is(err, dvr.ErrReplacementTargetNotFuture):
			writeErr(w, http.StatusUnprocessableEntity, err.Error())
		case errors.Is(err, store.ErrArtifactNotReplaceable):
			writeErr(w, http.StatusConflict, err.Error())
		case errors.Is(err, store.ErrArtifactReplacementMismatch),
			errors.Is(err, store.ErrOutputPathBusy),
			errors.Is(err, store.ErrDVRArtifactPublishInProgress):
			writeErr(w, http.StatusConflict, err.Error())
		default:
			writeErr(w, http.StatusInternalServerError, err.Error())
		}
		return
	}
	w.Header().Set("Location", "/admin/dvr/recordings/"+rec.ID.String())
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(rec)
}

// reconcilePreview runs the reconciler's match pass on demand and returns
// exactly what the next cycle would act on — without scheduling anything.
// Lets an operator validate matches before flipping CONDUCTOR_RECONCILE_DRY_RUN
// off. 503 when the reconciler isn't configured (no Sonarr/Radarr wired).
func (a *admin) reconcilePreview(w http.ResponseWriter, r *http.Request) {
	if a.d.Reconciler == nil {
		writeErr(w, http.StatusServiceUnavailable,
			"reconciler not enabled (set CONDUCTOR_SONARR_URL/_API_KEY and/or CONDUCTOR_RADARR_URL/_API_KEY)")
		return
	}
	report, err := a.d.Reconciler.Preview(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, report)
}

// ─────────────────────── Transcode profiles (Phase 4) ───────────────

type createTranscodeProfileReq struct {
	Name                string   `json:"name"`
	Description         string   `json:"description"`
	Kind                string   `json:"kind"`
	VideoCodec          string   `json:"video_codec"`
	VideoBitrateKbps    int      `json:"video_bitrate_kbps"`
	VideoMaxBitrateKbps int      `json:"video_max_bitrate_kbps"`
	VideoHeight         int      `json:"video_height"`
	VideoPreset         string   `json:"video_preset"`
	VideoTune           string   `json:"video_tune"`
	VideoKeyint         int      `json:"video_keyint"`
	Deinterlace         bool     `json:"deinterlace"`
	HDRtoSDR            bool     `json:"hdr_to_sdr"`
	AudioCodec          string   `json:"audio_codec"`
	AudioBitrateKbps    int      `json:"audio_bitrate_kbps"`
	AudioChannels       int      `json:"audio_channels"`
	AudioNormalize      bool     `json:"audio_normalize"`
	UseNVENC            bool     `json:"use_nvenc"`
	UseNVDEC            bool     `json:"use_nvdec"`
	FixTimestamps       bool     `json:"fix_timestamps"`
	LowLatency          bool     `json:"low_latency"`
	FFmpegArgs          []string `json:"ffmpeg_args"`
	Notes               string   `json:"notes"`
	Enabled             *bool    `json:"enabled"`
}

func (a *admin) createTranscodeProfile(w http.ResponseWriter, r *http.Request) {
	var req createTranscodeProfileReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if req.Name == "" || req.Kind == "" {
		writeErr(w, http.StatusBadRequest, "name and kind required")
		return
	}
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	p, err := a.d.DB.CreateTranscodeProfile(r.Context(), store.TranscodeProfile{
		Name: req.Name, Description: req.Description, Kind: req.Kind,
		VideoCodec: req.VideoCodec, VideoBitrateKbps: req.VideoBitrateKbps,
		VideoMaxBitrateKbps: req.VideoMaxBitrateKbps, VideoHeight: req.VideoHeight,
		VideoPreset: req.VideoPreset, VideoTune: req.VideoTune, VideoKeyint: req.VideoKeyint,
		Deinterlace: req.Deinterlace, HDRtoSDR: req.HDRtoSDR,
		AudioCodec: req.AudioCodec, AudioBitrateKbps: req.AudioBitrateKbps,
		AudioChannels: req.AudioChannels, AudioNormalize: req.AudioNormalize,
		UseNVENC: req.UseNVENC, UseNVDEC: req.UseNVDEC,
		FixTimestamps: req.FixTimestamps, LowLatency: req.LowLatency,
		FFmpegArgs: req.FFmpegArgs, Notes: req.Notes, Enabled: enabled,
	})
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, p)
}

func (a *admin) listTranscodeProfiles(w http.ResponseWriter, r *http.Request) {
	ps, err := a.d.DB.ListTranscodeProfiles(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, ps)
}

func (a *admin) getTranscodeProfile(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid id")
		return
	}
	p, err := a.d.DB.GetTranscodeProfile(r.Context(), id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeErr(w, http.StatusNotFound, "not found")
			return
		}
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, p)
}

type assignTranscodeReq struct {
	// ProfileID null = clear (revert to inheritance / passthrough).
	ProfileID *uuid.UUID `json:"profile_id"`
}

func (a *admin) assignChannelTranscode(w http.ResponseWriter, r *http.Request) {
	chID, err := uuid.Parse(r.PathValue("channel_id"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid channel_id")
		return
	}
	var req assignTranscodeReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if err := a.d.DB.SetChannelTranscodeProfile(r.Context(), chID, req.ProfileID); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, map[string]any{"ok": true, "channel_id": chID, "profile_id": req.ProfileID})
}

func (a *admin) assignSourceTranscode(w http.ResponseWriter, r *http.Request) {
	srcID, err := uuid.Parse(r.PathValue("source_id"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid source_id")
		return
	}
	var req assignTranscodeReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if err := a.d.DB.SetChannelSourceTranscodeProfile(r.Context(), srcID, req.ProfileID); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, map[string]any{"ok": true, "source_id": srcID, "profile_id": req.ProfileID})
}

// ─────────────────────── Enrichment (Phase 3) ───────────────────────

func (a *admin) refreshEnrichment(w http.ResponseWriter, r *http.Request) {
	if a.d.EnrichWorker == nil {
		writeErr(w, http.StatusServiceUnavailable, "enrichment worker not configured (set CONDUCTOR_TMDB_API_KEY)")
		return
	}
	matched, failed, skipped := a.d.EnrichWorker.RunOnce(r.Context())
	writeJSON(w, map[string]any{
		"matched": matched, "failed": failed, "skipped": skipped,
	})
}

func (a *admin) listManualMatches(w http.ResponseWriter, r *http.Request) {
	ms, err := a.d.DB.ListManualMatches(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, ms)
}

type createManualMatchReq struct {
	ChannelID uuid.UUID `json:"channel_id"`
	Title     string    `json:"title"` // raw, normalized server-side
	IsMovie   bool      `json:"is_movie"`
	TMDbID    int       `json:"tmdb_id"`
	TVDBID    int       `json:"tvdb_id"`
	IMDbID    string    `json:"imdb_id"`
	Note      string    `json:"note"`
}

func (a *admin) createManualMatch(w http.ResponseWriter, r *http.Request) {
	var req createManualMatchReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if req.ChannelID == uuid.Nil || req.Title == "" {
		writeErr(w, http.StatusBadRequest, "channel_id and title required")
		return
	}
	if req.TMDbID == 0 && req.TVDBID == 0 && req.IMDbID == "" {
		writeErr(w, http.StatusBadRequest, "must supply at least one of tmdb_id, tvdb_id, imdb_id")
		return
	}
	m, err := a.d.DB.CreateManualMatch(r.Context(), store.ManualEnrichmentMatch{
		ChannelID: req.ChannelID,
		TitleNorm: enrich.NormalizeForMatch(req.Title),
		IsMovie:   req.IsMovie,
		TMDbID:    req.TMDbID, TVDBID: req.TVDBID, IMDbID: req.IMDbID,
		Note: req.Note,
	})
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, m)
}

// ─────────────────────── Durable poster overrides ───────────────────────

func (a *admin) listPosterOverrides(w http.ResponseWriter, r *http.Request) {
	overrides, err := a.d.DB.ListPosterOverrides(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, overrides)
}

type createPosterOverrideReq struct {
	ChannelID *uuid.UUID `json:"channel_id,omitempty"`
	Title     string     `json:"title"`
	SubTitle  string     `json:"subtitle,omitempty"`
	IsMovie   bool       `json:"is_movie"`
	SourceURL string     `json:"source_url"`
	Note      string     `json:"note"`
}

func (a *admin) createPosterOverride(w http.ResponseWriter, r *http.Request) {
	var req createPosterOverrideReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if req.Title == "" || req.SourceURL == "" || (req.ChannelID != nil && *req.ChannelID == uuid.Nil) {
		writeErr(w, http.StatusBadRequest, "title and source_url required; channel_id may be omitted for a global override")
		return
	}
	u, err := url.Parse(req.SourceURL)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil {
		writeErr(w, http.StatusBadRequest, "source_url must be a public HTTPS URL")
		return
	}
	assetKey := enrich.ResearchedAssetKey(req.SourceURL)
	o, err := a.d.DB.UpsertPosterOverride(r.Context(), store.PosterOverride{
		ChannelID:    req.ChannelID,
		TitleNorm:    enrich.NormalizeForMatch(req.Title),
		SubTitleNorm: enrich.NormalizeForMatch(req.SubTitle),
		IsMovie:      req.IsMovie,
		AssetKey:     assetKey,
		Note:         req.Note,
	}, req.SourceURL)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if a.d.PosterWorker != nil {
		a.d.PosterWorker.Wake()
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(o)
}

func (a *admin) refreshPosters(w http.ResponseWriter, r *http.Request) {
	if a.d.PosterWorker == nil {
		writeErr(w, http.StatusServiceUnavailable, "poster worker not configured")
		return
	}
	// Generation can legitimately wait for Plex GPU activity to clear. Queue
	// the pass instead of tying an admin HTTP request to that lifecycle.
	a.d.PosterWorker.Wake()
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]bool{"queued": true})
}

// ─────────────────────── Schedules Direct (Phase 3b) ───────────────

type sdCredentialsView struct {
	Username      string `json:"username"`
	Enabled       bool   `json:"enabled"`
	HasToken      bool   `json:"has_cached_token"`
	LastSuccessAt any    `json:"last_success_at"`
	LastError     string `json:"last_error,omitempty"`
}

func (a *admin) getSDCredentials(w http.ResponseWriter, r *http.Request) {
	c, err := a.d.DB.GetSDCredentials(r.Context())
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeJSON(w, sdCredentialsView{}) // empty = not configured
			return
		}
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, sdCredentialsView{
		Username: c.Username, Enabled: c.Enabled,
		HasToken: c.CachedToken != "", LastSuccessAt: c.LastSuccessAt,
		LastError: c.LastError,
	})
}

type upsertSDCredsReq struct {
	Username string `json:"username"`
	Password string `json:"password"` // cleartext; SHA-1'd before persist
	Enabled  *bool  `json:"enabled"`
}

func (a *admin) upsertSDCredentials(w http.ResponseWriter, r *http.Request) {
	var req upsertSDCredsReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if req.Username == "" || req.Password == "" {
		writeErr(w, http.StatusBadRequest, "username and password required")
		return
	}
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	// SD wants the SHA-1 of the password as its "password" field. We hash
	// once and persist only the hash; cleartext never touches the DB.
	h := sha1.Sum([]byte(req.Password))
	hashed := hex.EncodeToString(h[:])
	if err := a.d.DB.UpsertSDCredentials(r.Context(), req.Username, hashed, enabled); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, map[string]any{"ok": true, "username": req.Username, "enabled": enabled})
}

func (a *admin) listSDLineups(w http.ResponseWriter, r *http.Request) {
	ls, err := a.d.DB.ListEnabledSDLineups(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, ls)
}

type createSDLineupReq struct {
	SDLineupID string `json:"sd_lineup_id"`
	Name       string `json:"name"`
	Location   string `json:"location"`
	Enabled    *bool  `json:"enabled"`
}

func (a *admin) createSDLineup(w http.ResponseWriter, r *http.Request) {
	var req createSDLineupReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if req.SDLineupID == "" || req.Name == "" {
		writeErr(w, http.StatusBadRequest, "sd_lineup_id and name required")
		return
	}
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	l, err := a.d.DB.CreateSDLineup(r.Context(), req.SDLineupID, req.Name, req.Location, enabled)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, l)
}

// asserterr keeps imports that may briefly become unused as we refactor.
var _ = errors.New
var _ = strconv.Itoa
