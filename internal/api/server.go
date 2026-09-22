// Package api wires HTTP routes for Conductor's HDHomeRun emulator,
// stream endpoints, healthchecks, admin CRUD, and (later) /xmltv.xml + the
// React UI.
package api

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/spencercnorton/conductor/internal/alerts"
	"github.com/spencercnorton/conductor/internal/auth"
	"github.com/spencercnorton/conductor/internal/dvr"
	"github.com/spencercnorton/conductor/internal/enrich"
	"github.com/spencercnorton/conductor/internal/epg"
	"github.com/spencercnorton/conductor/internal/hdhr"
	"github.com/spencercnorton/conductor/internal/logo"
	"github.com/spencercnorton/conductor/internal/metrics"
	"github.com/spencercnorton/conductor/internal/poster"
	"github.com/spencercnorton/conductor/internal/reconcile"
	"github.com/spencercnorton/conductor/internal/security"
	"github.com/spencercnorton/conductor/internal/store"
	"github.com/spencercnorton/conductor/internal/stream"
	"github.com/spencercnorton/conductor/internal/version"
)

// Deps is the bag of dependencies the router needs. Either DB+Pool are both
// set (Phase 1+ runtime), or neither is set and the router runs in
// "discovery-only" mode (Phase 0 boot before a DB is configured — useful
// for tests + initial pairing).
type Deps struct {
	Logger         *slog.Logger
	Device         hdhr.Device
	DB             *store.DB    // optional; nil = discovery-only mode
	Pool           *stream.Pool // optional; required iff DB is set
	CredKey        *security.CredKey
	Auth           auth.Config           // applied to /admin/*; pass-through when disabled
	Alerts         *alerts.Client        // optional; passed through to admin handlers
	EPGWorker      *epg.Worker           // optional; required for POST /admin/epg/refresh
	Torznab        *dvr.Torznab          // optional; mounts /indexer/torznab/api when set
	DVRSchedule    *dvr.ScheduleHandler  // optional; mounts /dvr/schedule.torrent
	DVRScheduler   *dvr.Scheduler        // optional; admin /admin/dvr/recordings/{id} cancel
	Reconciler     *reconcile.Worker     // optional; GET /admin/dvr/reconcile/preview
	EnrichWorker   *enrich.Worker        // optional; required for POST /admin/enrich/refresh
	PosterWorker   *enrich.PosterWorker  // optional; researched/generated poster queue
	PPVMonitor     *alerts.WorkerMonitor // optional; surfaced by GET /admin/epg/health
	SportsMonitor  *alerts.WorkerMonitor // optional; surfaced by GET /admin/epg/health
	LogosDir       string                // optional; when set, mounts GET /logos/{filename}
	PosterCacheDir string                // optional; when set, mounts GET /posters/branded/{channel_id}.jpg
	// SeedLineup is used when DB is nil (Phase 0 / dev convenience).
	SeedLineup []hdhr.Channel
}

func NewRouter(d Deps) http.Handler {
	mux := http.NewServeMux()

	hh := hdhr.New(d.Device, lineupFunc(d))

	// HDHomeRun emulator surface.
	mux.HandleFunc("GET /discover.json", hh.Discover)
	mux.HandleFunc("GET /lineup.json", hh.LineupJSON)
	mux.HandleFunc("GET /lineup.xml", hh.LineupXML)
	mux.HandleFunc("GET /lineup_status.json", hh.LineupStatus)
	mux.HandleFunc("GET /device.xml", hh.DeviceXML)

	// Plex hits `/auto/v<num>`. Go's ServeMux does not support partial-segment
	// wildcards, so we register `/auto/{vch}` and strip the `v` prefix in
	// the handler. The URL Plex sees from /lineup.json is unchanged.
	mux.HandleFunc("GET /auto/{vch}", streamHandler(d))

	// Admin CRUD (Phase 1+). Wraps in auth middleware (or no-op when disabled).
	if d.DB != nil {
		MountAdmin(mux, AdminDeps{
			Logger: d.Logger, DB: d.DB, CredKey: d.CredKey,
			Auth: d.Auth, EPG: d.EPGWorker, DVRSchedule: d.DVRSchedule,
			DVRScheduler: d.DVRScheduler,
			Reconciler:   d.Reconciler,
			EnrichWorker: d.EnrichWorker,
			PosterWorker: d.PosterWorker,
			PPVMonitor:   d.PPVMonitor, SportsMonitor: d.SportsMonitor,
		})
	}

	// EPG output for Plex (Phase 2). Public — Plex can't speak JWT.
	if d.DB != nil {
		eo := epg.NewOutput(d.DB, d.Device.BaseURL)
		// Gzip: 13K+ programmes serialize to multi-MB XML and Plex
		// re-fetches every ~12h; XMLTV compresses ~10× (audit Q5).
		mux.HandleFunc("GET /xmltv.xml", gzipped(eo.ServeHTTP))
		mux.HandleFunc("GET /epg.xml", gzipped(eo.ServeHTTP))
	}

	// Prometheus exposition (audit Q1). Unauthenticated like /healthz —
	// Conductor is Tailscale/LAN-only and the payload is operational only.
	mux.Handle("GET /metrics", metrics.Handler())

	// DVR-as-Indexer (Phase 3a). Torznab indexer + grab handler.
	// Auth is via apikey query param (Prowlarr convention); NOT the
	// admin Bearer key — Prowlarr stores indexer keys in plaintext.
	if d.Torznab != nil {
		mux.HandleFunc("GET /indexer/torznab/api", d.Torznab.ServeHTTP)
	}
	if d.DVRSchedule != nil {
		mux.HandleFunc("GET /dvr/schedule.torrent", d.DVRSchedule.ServeHTTP)
	}

	// Channel logos. Conductor self-hosts logos so we don't depend on
	// the source EPG provider's HTTP endpoints. Public — channel art is
	// not sensitive, and Plex/iOS/etc. fetch unauth'd.
	if d.LogosDir != "" {
		ls := logo.New(d.LogosDir)
		mux.HandleFunc("GET /logos/{filename}", ls.ServeHTTP)
	}

	// Branded fallback posters. Public (Plex/iOS fetch unauth'd, like
	// channel logos). Renders a per-channel, per-category 600×900 JPEG
	// when a programme has no third-party (TMDb/TVDb) match. Cached
	// on disk under PosterCacheDir/branded/.
	if d.PosterCacheDir != "" && d.LogosDir != "" && d.DB != nil {
		ps := poster.New(d.PosterCacheDir, d.LogosDir, d.DB)
		mux.HandleFunc("GET /posters/branded/{channel_id}", ps.ServeHTTP)
	}
	if d.PosterCacheDir != "" {
		as := poster.NewAssetServer(d.PosterCacheDir)
		mux.HandleFunc("GET /posters/assets/{asset_path...}", as.ServeHTTP)
	}

	// Health.
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{
			"status":  "ok",
			"version": version.Version,
		})
	})
	mux.HandleFunc("GET /readyz", readyzHandler(d))

	// Root: tiny landing page. Use /{$} (Go 1.22 mux syntax) so this handler
	// matches exactly "/" and doesn't shadow other registered prefixes.
	mux.HandleFunc("GET /{$}", rootHandler(d))

	return logging(d.Logger, mux)
}

// lineupFunc returns the right LineupFunc for the deployment mode.
// DB-backed when DB+SeedLineup is configured; static otherwise.
func lineupFunc(d Deps) hdhr.LineupFunc {
	if d.DB == nil {
		seed := d.SeedLineup
		return func(context.Context) ([]hdhr.Channel, error) { return seed, nil }
	}
	return func(ctx context.Context) ([]hdhr.Channel, error) {
		channels, err := d.DB.ListChannels(ctx)
		if err != nil {
			return nil, err
		}
		out := make([]hdhr.Channel, 0, len(channels))
		for _, c := range channels {
			out = append(out, hdhr.Channel{
				GuideNumber: trimZero(c.Number),
				GuideName:   c.Name,
				URL:         fmt.Sprintf("%s/auto/v%s", d.Device.BaseURL, trimZero(c.Number)),
				HD:          1,
			})
		}
		// If no channels yet but we have a seed (dev mode), surface the seed
		// so initial pairing in Plex still has something to show.
		if len(out) == 0 {
			return d.SeedLineup, nil
		}
		return out, nil
	}
}

// streamHandler routes /auto/v<num> to the Pool (DB mode) or to a stub
// upstream (no-DB mode).
func streamHandler(d Deps) http.HandlerFunc {
	const phase0TestUpstream = "https://commondatastorage.googleapis.com/gtv-videos-bucket/sample/BigBuckBunny.mp4"
	return func(w http.ResponseWriter, r *http.Request) {
		ch := strings.TrimPrefix(r.PathValue("vch"), "v")
		started := time.Now()
		startupDeadline := started.Add(stream.ClientStartupBudget)
		serveCtx := stream.WithStartupDeadline(r.Context(), startupDeadline)

		// No-DB mode: serve a single hardcoded test stream so Plex pairing
		// can complete on a fresh install.
		if d.DB == nil || d.Pool == nil {
			err := stubServe(r.Context(), w, phase0TestUpstream)
			d.Logger.Info("stream (no-DB stub)",
				"channel", ch, "remote", r.RemoteAddr, "dur", time.Since(started), "err", err)
			return
		}

		// DB mode: resolve channel by number, lease, serve via Pool.
		num, err := strconv.ParseFloat(ch, 64)
		if err != nil {
			http.Error(w, "invalid channel number: "+ch, http.StatusBadRequest)
			return
		}
		lookupCtx, cancelLookup := context.WithDeadline(serveCtx, startupDeadline)
		channel, err := d.DB.GetChannelByNumber(lookupCtx, num)
		cancelLookup()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				http.Error(w, "upstream did not start", http.StatusServiceUnavailable)
				return
			}
			if errors.Is(err, store.ErrNotFound) {
				http.Error(w, "channel not found", http.StatusNotFound)
				return
			}
			d.Logger.Warn("channel lookup", "channel", ch, "err", err)
			http.Error(w, "channel lookup failed", http.StatusInternalServerError)
			return
		}
		if err := d.Pool.Serve(serveCtx, w, r, channel.ID); err != nil {
			if !responseCommitted(w) {
				logStreamStartupError(d.Logger, ch, r.RemoteAddr, time.Since(started), err)
				if writeStreamStartupErrorIfUncommitted(w, err) {
					return
				}
			}
			d.Logger.Warn("stream ended with error",
				"channel", ch, "remote", r.RemoteAddr, "dur", time.Since(started), "err", err)
			return
		}
		d.Logger.Info("stream ended ok",
			"channel", ch, "remote", r.RemoteAddr, "dur", time.Since(started))
	}
}

func logStreamStartupError(logger *slog.Logger, channel, remote string, elapsed time.Duration, err error) {
	switch {
	case errors.Is(err, store.ErrLeaseOperational):
		logger.Warn("stream startup admission failed",
			"channel", channel, "remote", remote, "dur", elapsed, "err", err)
	case errors.Is(err, store.ErrAdmissionContention):
		logger.Info("stream startup admission contended",
			"channel", channel, "remote", remote, "dur", elapsed, "err", err)
	case errors.Is(err, stream.ErrPoolClosed):
		logger.Info("stream startup unavailable during shutdown",
			"channel", channel, "remote", remote, "dur", elapsed, "err", err)
	}
}

// writeStreamStartupErrorIfUncommitted prevents a shutdown or other typed
// startup marker from appending plaintext after Pool has already committed an
// MPEG-TS response. The logging middleware supplies the commitment tracker.
func writeStreamStartupErrorIfUncommitted(w http.ResponseWriter, err error) bool {
	if responseCommitted(w) {
		return false
	}
	return writeStreamStartupError(w, err)
}

func responseCommitted(w http.ResponseWriter) bool {
	type commitmentReporter interface {
		ResponseCommitted() bool
	}
	type responseUnwrapper interface {
		Unwrap() http.ResponseWriter
	}
	// Middleware may place capability-preserving wrappers between the stream
	// handler and the access-log status writer. Walk the same Unwrap convention
	// used by net/http's response controller so a committed transport response
	// can never be mistaken for an uncommitted startup failure. Bound the walk
	// to fail closed on a malformed cyclic wrapper chain.
	for depth := 0; w != nil; depth++ {
		if depth >= 64 {
			// Wrapper cycles and unreasonable depth make commitment state
			// unknowable. Treat that uncertainty as committed so the only
			// possible mistake is omitting a startup error, never corrupting
			// an existing MPEG-TS response with plaintext.
			return true
		}
		if reporter, ok := w.(commitmentReporter); ok && reporter.ResponseCommitted() {
			return true
		}
		unwrapper, ok := w.(responseUnwrapper)
		if !ok {
			return false
		}
		w = unwrapper.Unwrap()
	}
	return false
}

// writeStreamStartupError maps errors that Pool can only return before
// committing response bytes. In particular, a bounded wait on Conductor's own
// admission locks must be a retryable 503 for Plex, never an implicit
// 200-with-empty-body. Other errors may describe a stream that ended after a
// 200 was already written, so the caller only logs those.
func writeStreamStartupError(w http.ResponseWriter, err error) bool {
	switch {
	case errors.Is(err, store.ErrNoSlot):
		http.Error(w, "no upstream slots available", http.StatusServiceUnavailable)
		return true
	case errors.Is(err, store.ErrAdmissionContention):
		http.Error(w, "upstream admission busy; retry", http.StatusServiceUnavailable)
		return true
	case errors.Is(err, store.ErrLeaseOperational):
		// AcquireLease adds this marker only to pre-pump source/DB/resolver
		// failures. Midstream relocation consumes its errors inside the pump.
		http.Error(w, "upstream admission failed; retry", http.StatusServiceUnavailable)
		return true
	case errors.Is(err, stream.ErrPoolClosed):
		// Pool.Close can win after lease acquisition but before the first byte.
		// Plex must see a retryable startup failure, never implicit empty 200.
		http.Error(w, "stream temporarily unavailable; retry", http.StatusServiceUnavailable)
		return true
	case errors.Is(err, stream.ErrUpstreamNotReady):
		http.Error(w, "upstream did not start", http.StatusServiceUnavailable)
		return true
	default:
		return false
	}
}

// stubServe is the no-DB pairing-test fallback. Phase 0 lived here originally;
// Phase 1+ uses d.Pool.Serve instead.
func stubServe(ctx context.Context, w http.ResponseWriter, upstream string) error {
	hc := &http.Client{Timeout: 0}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, upstream, nil)
	if err != nil {
		return err
	}
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		http.Error(w, "upstream "+http.StatusText(resp.StatusCode), http.StatusBadGateway)
		return nil
	}
	if ct := resp.Header.Get("Content-Type"); ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	buf := make([]byte, 64*1024)
	for {
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return werr
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
		if rerr != nil {
			return nil
		}
	}
}

func readyzHandler(d Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		out := map[string]any{
			"status":         "ready",
			"version":        version.Version,
			"hdhr_device_id": d.Device.DeviceID,
			"tuner_count":    d.Device.TunerCount(),
		}
		if d.DB != nil {
			if err := d.DB.Ping(r.Context()); err != nil {
				out["status"] = "degraded"
				out["db_error"] = err.Error()
				w.Header().Set("Content-Type", "application/json; charset=utf-8")
				w.Header().Set("Cache-Control", "no-store")
				w.WriteHeader(http.StatusServiceUnavailable)
				writeJSON(w, out)
				return
			}
			out["db"] = "ok"
			canonical, suppressed, overlaps, err := d.DB.EPGCanonicalHealth(r.Context())
			if err != nil || overlaps != 0 {
				out["status"] = "degraded"
				out["epg_canonical_programs"] = canonical
				out["epg_suppressed_programs"] = suppressed
				out["epg_canonical_overlaps"] = overlaps
				if err != nil {
					out["epg_error"] = err.Error()
				}
				w.Header().Set("Content-Type", "application/json; charset=utf-8")
				w.Header().Set("Cache-Control", "no-store")
				w.WriteHeader(http.StatusServiceUnavailable)
				writeJSON(w, out)
				return
			}
			out["epg_canonical_overlaps"] = overlaps
		}
		if d.Pool != nil {
			out["active_streams"] = d.Pool.Active(r.Context())
		}
		writeJSON(w, out)
	}
}

func rootHandler(d Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprintf(w, "Conductor %s — HDHomeRun emulator for Plex\n", version.Version)
		fmt.Fprintf(w, "Device: %s (%s)\n", d.Device.FriendlyName, d.Device.DeviceID)
		fmt.Fprintf(w, "Tuners: %d\n", d.Device.TunerCount())
		mode := "discovery-only (no DB configured)"
		if d.DB != nil {
			mode = "full (DB-backed)"
		}
		fmt.Fprintf(w, "Mode: %s\n", mode)
		fmt.Fprintf(w, "Endpoints: /discover.json /lineup.json /lineup.xml /lineup_status.json /device.xml /auto/v<ch> /healthz /readyz")
		if d.DB != nil {
			fmt.Fprintf(w, " /admin/*")
		}
		fmt.Fprintf(w, "\n")
	}
}

func logging(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t0 := time.Now()
		sw := &statusWriter{ResponseWriter: w, code: 200}
		next.ServeHTTP(sw, r)
		// Skip noisy stream paths — they log themselves via the handler.
		if strings.HasPrefix(r.URL.Path, "/auto/v") {
			return
		}
		// Healthy healthchecks are ~3K INFO lines/day of noise (audit
		// Q6); failures still log.
		if r.URL.Path == "/healthz" && sw.code == http.StatusOK {
			return
		}
		logger.Info("http",
			"method", r.Method,
			"path", r.URL.Path,
			"status", sw.code,
			"dur_ms", time.Since(t0).Milliseconds(),
			"remote", r.RemoteAddr,
		)
	})
}

type statusWriter struct {
	http.ResponseWriter
	code        int
	wroteHeader bool
}

func (s *statusWriter) WriteHeader(c int) {
	if s.wroteHeader {
		return
	}
	s.wroteHeader = true
	s.code = c
	s.ResponseWriter.WriteHeader(c)
}

func (s *statusWriter) Write(p []byte) (int, error) {
	if !s.wroteHeader {
		s.WriteHeader(http.StatusOK)
	}
	return s.ResponseWriter.Write(p)
}

// Flush preserves the streaming capability of net/http's concrete writer;
// otherwise wrapping it for access logs hides http.Flusher from Pool and lets
// live transport data sit in the server buffer.
func (s *statusWriter) Flush() {
	if !s.wroteHeader {
		s.WriteHeader(http.StatusOK)
	}
	if flusher, ok := s.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (s *statusWriter) ResponseCommitted() bool { return s.wroteHeader }

// Unwrap lets http.ResponseController reach optional capabilities on the
// concrete response writer without teaching this middleware every interface.
func (s *statusWriter) Unwrap() http.ResponseWriter { return s.ResponseWriter }

// gzipped wraps a handler with on-the-fly gzip when the client asks for
// it. The writer is engaged lazily on the first body Write so bodyless
// responses (304 Not Modified — Plex's common case via ETag) pass through
// without a spurious gzip header or trailer.
func gzipped(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			next(w, r)
			return
		}
		gw := &gzipResponseWriter{ResponseWriter: w}
		defer gw.close()
		next(gw, r)
	}
}

type gzipResponseWriter struct {
	http.ResponseWriter
	gz          *gzip.Writer
	wroteHeader bool
	compress    bool
}

// Unwrap preserves response capabilities and commitment tracking supplied by
// outer middleware.
func (g *gzipResponseWriter) Unwrap() http.ResponseWriter { return g.ResponseWriter }

func (g *gzipResponseWriter) WriteHeader(code int) {
	if g.wroteHeader {
		return
	}
	g.wroteHeader = true
	// Only compress 200s with a body; 304/4xx/5xx pass through unchanged.
	if code == http.StatusOK {
		g.compress = true
		g.Header().Set("Content-Encoding", "gzip")
		g.Header().Del("Content-Length")
		// Vary so caches keep compressed + identity variants apart.
		g.Header().Add("Vary", "Accept-Encoding")
	}
	g.ResponseWriter.WriteHeader(code)
}

func (g *gzipResponseWriter) Write(b []byte) (int, error) {
	if !g.wroteHeader {
		g.WriteHeader(http.StatusOK)
	}
	if !g.compress {
		return g.ResponseWriter.Write(b)
	}
	if g.gz == nil {
		g.gz = gzip.NewWriter(g.ResponseWriter)
	}
	return g.gz.Write(b)
}

func (g *gzipResponseWriter) close() {
	if g.gz != nil {
		_ = g.gz.Close()
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(v)
}

// trimZero formats a numeric channel as "100" or "100.1" (no trailing .0).
func trimZero(n float64) string {
	s := strconv.FormatFloat(n, 'f', -1, 64)
	return s
}
