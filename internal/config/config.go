// Package config loads runtime configuration from environment variables.
//
// Precedence (highest to lowest):
// 1. Process environment (set directly, by your orchestrator, or by a
// secret manager injecting them at container start).
// 2. .env file in CWD (dev convenience only; never committed).
// 3. Hardcoded defaults below.
//
// Secrets should come from a secret manager in production. The container
// entrypoint can fetch them and re-exec conductor.
package config

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	// HTTP listen address (host:port). Default :8409 (HDHomeRun convention is :80
	// but we run behind NPM / Tailscale).
	ListenAddr string

	// BaseURL Plex will use to reach this device. Must be set explicitly when
	// running behind a reverse proxy or on a non-default port. Falls back to
	// http://<first-non-loopback-ip>:<port>.
	BaseURL string

	// Stable HDHomeRun device UUID. Plex hashes this to track the tuner across
	// restarts. Generated once at first boot if blank, persisted to ConfigDir.
	DeviceID string

	// TunerCount advertised in /discover.json. In Phase 1+ this becomes the sum
	// of enabled credentials' max_streams; for Phase 0 it's a literal config.
	TunerCount int

	// Postgres DSN. Empty disables DB mode (discovery-only, no streaming).
	PostgresDSN string

	// AES-GCM key (32 bytes hex) for encrypting provider-credential
	// passwords at rest. Empty disables credential persistence (admin
	// returns 503 on credential creation).
	CredKeyHex string

	// Filesystem dir for persistent state Conductor manages itself
	// (device UUID file, cached logos, raw XMLTV snapshots).
	DataDir string

	// ops bot webhook for alert delivery (optional).
	// See internal/alerts/plex_ops_bot.go.
	OpsBotWebhookURL string
	OpsBotSecret     string

	// EPG ingest cadence. Default 6h matches the spec recommendation
	// (XMLTV providers update every few hours; nightly is too coarse).
	// Set CONDUCTOR_EPG_INTERVAL=15m for testing.
	EPGRefreshInterval time.Duration

	// DVR-as-Indexer (Phase 3a) — Conductor exposes a Torznab indexer that
	// schedules a DVR recording when *arr "grabs" a result.

	// CONDUCTOR_DVR_INDEXER_KEY: Torznab apikey shared with Prowlarr.
	// Empty disables the indexer surface entirely.
	DVRIndexerKey string

	// CONDUCTOR_DVR_OUTPUT_DIR: root dir for finished DVR recordings.
	// Mounted into Conductor + visible to Sonarr/Radarr's library scan.
	// Default /var/lib/conductor/dvr.
	DVROutputDir string

	// CONDUCTOR_DVR_INDEXER_NAME: friendly name shown to Prowlarr in the
	// caps response. Default "Conductor DVR".
	DVRIndexerName string

	// CONDUCTOR_DVR_START_SLACK / CONDUCTOR_DVR_MAX_OVERRUN: pre-roll before
	// scheduled_start and post-roll after scheduled_end. A recording holds a
	// provider stream slot for its whole padded window, so their sum is how
	// long adjacent recordings fight each other for the (very few) slots.
	// Zero = unset; dvr.NewRecorder's defaults apply (30s / 90s).
	DVRStartSlack time.Duration
	DVRMaxOverrun time.Duration

	// CONDUCTOR_DVR_LIVE_RESERVE: provider slots retained for live viewers
	// before a new distinct-channel DVR upstream may start. Default 1; clamped
	// by the allocator so a one-slot provider can still record.
	DVRLiveReserve int

	// CONDUCTOR_DVR_DEFAULT_PRIORITY: lower values win deterministic capacity
	// conflicts. Default 100.
	DVRDefaultPriority int

	// CONDUCTOR_LIVE_STARTUP_LEAD: how long a live pump's initial attempt
	// holds its validated prefix so the reservoir fills with lead before the
	// first byte reaches the client. The pacer keeps that lead for the life of
	// the attempt (a source pause is absorbed from it instead of reaching
	// Plex), so this trades tune-in latency for stability on origins that
	// never deliver ahead of realtime. Default 4s; 0 disables; capped at 10s,
	// which is what the reservoir holds at the lineup's heaviest bitrate, and
	// always clipped to the initiating client's startup budget.
	LiveStartupLead time.Duration

	// CONDUCTOR_DVR_SCAN_MAX_DECODE_ERRORS: tolerance for the finalization
	// bitstream scan, which fully decodes each normalized recording's video
	// and rejects the artifact when decoder corruption events exceed this.
	// Default 3. Measured 2026-09-02 over 14 days in production: the 45
	// rejections were 21 with one event, 6 with two, 1 with three, then
	// nothing until 15, and 17 with 15-286 (mid-GOP reconnect splices). One
	// event is a single macroblock row on a single frame; the band from 4 to
	// 14 is empty, so 3 keeps 62% of the rejected full-length recordings and
	// still rejects every splice-damaged one. 0 restores the original
	// any-glitch rule; -1 disables the scan (escape hatch if a future ffmpeg's
	// decoder logging changes and clean recordings start mass-failing).
	DVRScanMaxDecodeErrors int

	// CONDUCTOR_DVR_CAPACITY_RETRY_INTERVAL / _WINDOW: fallback retry cadence
	// and maximum lateness measured from padded start. The scheduler also wakes
	// on slot release, and never retries beyond scheduled_end.
	DVRCapacityRetryInterval time.Duration
	DVRCapacityRetryWindow   time.Duration

	// Phase 4: transcoding + post-processing.

	// CONDUCTOR_FFMPEG_BINARY: path to ffmpeg. Empty = "ffmpeg" on PATH.
	// Set in containers where ffmpeg lives at a non-standard location.
	FFmpegBinary string
	// DiagDir receives bounded refusal-head dumps from stream startup gates
	// when set (CONDUCTOR_DIAG_DIR). Empty disables dumping.
	DiagDir string

	// CONDUCTOR_COMMSKIP_URL: base URL of the commskip-auto service.
	// Empty = post-processing disabled (recordings land as-is).
	CommskipURL string

	// CONDUCTOR_COMMSKIP_API_KEY: Bearer key for commskip-auto.
	CommskipAPIKey string

	// Phase 3: enrichment.

	// CONDUCTOR_TMDB_API_KEY: TMDb v3 API key. Empty = enrichment disabled.
	TMDbAPIKey string

	// CONDUCTOR_TMDB_BASE_URL: override for tests / proxy. Defaults to
	// https://api.themoviedb.org/3.
	TMDbBaseURL string

	// CONDUCTOR_ENRICH_INTERVAL: how often the enrichment worker drains
	// pending epg_program rows. Default 5m.
	EnrichInterval time.Duration

	// Phase 3b: TVDB fallback + Schedules Direct.

	// CONDUCTOR_TVDB_API_KEY: TheTVDB v4 project key. When set + TMDb
	// misses, the enricher falls back to TVDB.
	TVDbAPIKey string
	// CONDUCTOR_TVDB_PIN: optional TVDB subscriber PIN. Empty for free tier.
	TVDbPIN string
	// CONDUCTOR_TVDB_BASE_URL: override for tests.
	TVDbBaseURL string

	// CONDUCTOR_TVMAZE_ENABLED: when true (default), the enricher uses
	// TVmaze as a third fallback after TMDb and TVDb. TVmaze is free,
	// no-auth, TV-only, and rate-limited to 20 req/s. Set to "false" /
	// "0" / "no" to disable.
	TVmazeEnabled bool
	// CONDUCTOR_TVMAZE_BASE_URL: override for tests.
	TVmazeBaseURL string

	// Schedules Direct ($25/yr broadcast EPG). When credentials are set,
	// a separate ingester populates epg_program from SD; runs on the
	// same cadence as the XMLTV worker.
	SDUsername string
	SDPassword string // cleartext from env; SHA-1'd on the way to the API
	SDBaseURL  string

	// CONDUCTOR_LOGOS_DIR: directory holding self-hosted channel logo
	// files. Conductor mounts GET /logos/{filename} and channels
	// can use that URL for their logo_url instead of an upstream provider.
	// Defaults to ${CONDUCTOR_DATA_DIR}/logos.
	LogosDir string

	// CONDUCTOR_LOGO_BASE_URL: optional origin for self-hosted channel logos
	// in the XMLTV guide. Newer Plex apps fetch guide images themselves and
	// will not load plain-HTTP LAN URLs, so point this at an HTTPS proxy
	// that serves GET /logos/. Empty keeps logos on BaseURL.
	LogoBaseURL string

	// CONDUCTOR_UPSTREAM_PROXY: optional HTTP proxy URL (credentials, if any,
	// in its userinfo) for provider traffic only — the panel request, the
	// origin it redirects to, and the catalogue read — so all of it leaves
	// from one IP. Empty keeps provider traffic direct.
	UpstreamProxy *url.URL

	// CONDUCTOR_SPORTS_API_KEY / CONDUCTOR_SPORTS_INTERVAL: sports
	// schedule ingestion (Phase 5b). API key defaults to TheSportsDB's
	// free key when unset. Interval defaults to 30m. Set to 0 to keep
	// the default. Worker is always running when the DB is available
	// (even with no mappings — cheap no-op on each pass).
	SportsAPIKey   string
	SportsInterval time.Duration

	// CONDUCTOR_PPVSYNC_ENABLED: gates the PPV stream-name sync worker
	// (Phase 5d / audit E2). Default true; the worker self-disables at
	// runtime when no enabled m3u_xtream provider with a usable
	// credential exists, so the flag exists only to switch ppvsync off
	// on deploys that have providers but don't want PPV EPG.
	PPVSyncEnabled  bool
	PPVSyncInterval time.Duration
	// CONDUCTOR_PPV_DISPLAY_TIMEZONE controls the local time embedded in
	// pre-event Plex titles such as "Next 6:15 PM MDT — Event". It is an
	// IANA location name; the Norvi home-lab default is America/Denver.
	PPVDisplayTimezone string

	// CONDUCTOR_POSTER_CACHE_DIR: directory used by the branded
	// fallback poster generator to cache rendered JPEGs. The package
	// writes to ${PosterCacheDir}/branded/. When unset, branded poster
	// rendering is disabled (programmes with no enrichment match emit
	// no poster URL — the existing behavior). Recommend pointing at
	// the Conductor data volume so the cache survives restarts.
	PosterCacheDir string

	// Image generation is an optional, last-resort sports poster
	// source. Both URL and key must be set; researched poster caching still
	// works when generation is disabled.
	ImageGenURL    string
	ImageGenAPIKey string
	PosterInterval time.Duration

	// Proactive DVR reconciler (internal/reconcile). Pulls Sonarr/Radarr
	// wanted-lists, matches them against the upcoming EPG, and schedules
	// recordings for hits. The worker only starts when the DVR indexer is
	// enabled (CONDUCTOR_DVR_INDEXER_KEY set) AND at least one of the
	// Sonarr/Radarr URL+key pairs is configured.

	// CONDUCTOR_SONARR_URL / _API_KEY: read-only Sonarr v3 access. Use the
	// INTERNAL url (http://192.0.2.10:8989), never an auth-gated
	// public host — an auth gate returns HTML and breaks JSON.
	SonarrURL    string
	SonarrAPIKey string

	// CONDUCTOR_RADARR_URL / _API_KEY: read-only Radarr v3 access. Same
	// internal-url rule as Sonarr.
	RadarrURL    string
	RadarrAPIKey string

	// CONDUCTOR_RECONCILE_INTERVAL: reconcile cadence. Default 1h.
	ReconcileInterval time.Duration
	// CONDUCTOR_RECONCILE_WINDOW: how far ahead to scan the EPG. Default 14d.
	ReconcileWindow time.Duration
	// CONDUCTOR_RECONCILE_THRESHOLD: min match confidence (0..1) to
	// auto-record. Below it a candidate is logged only. Default 0.80.
	ReconcileThreshold float64
	// CONDUCTOR_RECONCILE_MAX_PER_CYCLE: cap on auto-records scheduled per
	// cycle (back-pressure on the first run). Default 20.
	ReconcileMaxPerCycle int
	// CONDUCTOR_RECONCILE_DRY_RUN: when true (the default), log would-record
	// matches but insert nothing. Flip to false to arm recording.
	ReconcileDryRun bool

	// CONDUCTOR_ARR_IMPORT_TRIGGER: when true (default) AND a Sonarr/Radarr
	// URL+key is configured, a post-record stage asks the matching *arr to
	// scan-and-import the finished recording's folder. Without it,
	// reconcile-initiated recordings only import on the *arr's next library
	// scan. Independent of commskip — the import stage runs whenever an
	// *arr is wired, even with no commskip pipeline.
	ArrImportTrigger bool

	// CONDUCTOR_PLEX_URL / _TOKEN: read-only Plex access for the owned-but-
	// incomplete gap diff — find episodes of series you own in Plex but are
	// missing (and Sonarr isn't monitoring). Use the internal url
	// (http://192.0.2.10:32400). Empty disables the gap diff.
	PlexURL   string
	PlexToken string

	// CONDUCTOR_RECONCILE_GAP_AUTORECORD: when false (the default), Plex gap
	// matches are preview-only — surfaced in /admin/dvr/reconcile/preview but
	// never auto-scheduled (they're inferred, not on a monitored wanted-list).
	// Set true to let them schedule like wanted-list matches.
	ReconcileGapAutoRecord bool
}

func Load() (Config, error) {
	listen := envDefault("CONDUCTOR_LISTEN", ":8409")
	dataDir := envDefault("CONDUCTOR_DATA_DIR", "/var/lib/conductor")

	tunerCount, err := strconv.Atoi(envDefault("CONDUCTOR_TUNER_COUNT", "8"))
	if err != nil {
		return Config{}, fmt.Errorf("CONDUCTOR_TUNER_COUNT: %w", err)
	}

	upstreamProxy, err := parseUpstreamProxy(os.Getenv("CONDUCTOR_UPSTREAM_PROXY"))
	if err != nil {
		return Config{}, fmt.Errorf("CONDUCTOR_UPSTREAM_PROXY: %w", err)
	}

	epgInterval := 6 * time.Hour
	if v := os.Getenv("CONDUCTOR_EPG_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return Config{}, fmt.Errorf("CONDUCTOR_EPG_INTERVAL: %w", err)
		}
		epgInterval = d
	}

	enrichInterval := 5 * time.Minute
	if v := os.Getenv("CONDUCTOR_ENRICH_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return Config{}, fmt.Errorf("CONDUCTOR_ENRICH_INTERVAL: %w", err)
		}
		enrichInterval = d
	}

	// Zero means "unset" — the recorder keeps its own default. Avoids
	// duplicating the default in two packages.
	dvrStartSlack, err := envDuration("CONDUCTOR_DVR_START_SLACK")
	if err != nil {
		return Config{}, err
	}
	dvrMaxOverrun, err := envDuration("CONDUCTOR_DVR_MAX_OVERRUN")
	if err != nil {
		return Config{}, err
	}
	dvrLiveReserve, err := strconv.Atoi(envDefault("CONDUCTOR_DVR_LIVE_RESERVE", "1"))
	if err != nil || dvrLiveReserve < 0 {
		return Config{}, fmt.Errorf("CONDUCTOR_DVR_LIVE_RESERVE: must be a non-negative integer")
	}
	dvrDefaultPriority, err := strconv.Atoi(envDefault("CONDUCTOR_DVR_DEFAULT_PRIORITY", "100"))
	if err != nil || dvrDefaultPriority <= 0 {
		return Config{}, fmt.Errorf("CONDUCTOR_DVR_DEFAULT_PRIORITY: must be a positive integer")
	}
	liveStartupLead, err := time.ParseDuration(envDefault("CONDUCTOR_LIVE_STARTUP_LEAD", "4s"))
	if err != nil || liveStartupLead < 0 || liveStartupLead > 10*time.Second {
		return Config{}, fmt.Errorf("CONDUCTOR_LIVE_STARTUP_LEAD: must be a duration between 0 and 10s")
	}
	dvrScanMaxDecodeErrors, err := strconv.Atoi(envDefault("CONDUCTOR_DVR_SCAN_MAX_DECODE_ERRORS", "3"))
	if err != nil || dvrScanMaxDecodeErrors < -1 {
		return Config{}, fmt.Errorf("CONDUCTOR_DVR_SCAN_MAX_DECODE_ERRORS: must be an integer >= -1 (-1 disables the scan)")
	}
	dvrCapacityRetryInterval, err := envDuration("CONDUCTOR_DVR_CAPACITY_RETRY_INTERVAL")
	if err != nil {
		return Config{}, err
	}
	dvrCapacityRetryWindow, err := envDuration("CONDUCTOR_DVR_CAPACITY_RETRY_WINDOW")
	if err != nil {
		return Config{}, err
	}

	ppvDisplayTimezone := envDefault("CONDUCTOR_PPV_DISPLAY_TIMEZONE", "America/Denver")
	if _, err := time.LoadLocation(ppvDisplayTimezone); err != nil {
		return Config{}, fmt.Errorf("CONDUCTOR_PPV_DISPLAY_TIMEZONE: %w", err)
	}

	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return Config{}, fmt.Errorf("data dir: %w", err)
	}

	deviceID := os.Getenv("CONDUCTOR_DEVICE_ID")
	if deviceID == "" {
		deviceID, err = loadOrCreateDeviceID(dataDir)
		if err != nil {
			return Config{}, fmt.Errorf("device id: %w", err)
		}
	}

	baseURL := os.Getenv("CONDUCTOR_BASE_URL")
	if baseURL == "" {
		baseURL = guessBaseURL(listen)
	}

	return Config{
		ListenAddr:               listen,
		BaseURL:                  baseURL,
		DeviceID:                 deviceID,
		TunerCount:               tunerCount,
		PostgresDSN:              os.Getenv("CONDUCTOR_POSTGRES_DSN"),
		CredKeyHex:               os.Getenv("CONDUCTOR_CRED_KEY"),
		DataDir:                  dataDir,
		OpsBotWebhookURL:         os.Getenv("CONDUCTOR_OPSBOT_WEBHOOK"),
		OpsBotSecret:             os.Getenv("CONDUCTOR_OPSBOT_SECRET"),
		EPGRefreshInterval:       epgInterval,
		DVRIndexerKey:            os.Getenv("CONDUCTOR_DVR_INDEXER_KEY"),
		DVROutputDir:             envDefault("CONDUCTOR_DVR_OUTPUT_DIR", dataDir+"/dvr"),
		DVRIndexerName:           envDefault("CONDUCTOR_DVR_INDEXER_NAME", "Conductor DVR"),
		DVRStartSlack:            dvrStartSlack,
		DVRMaxOverrun:            dvrMaxOverrun,
		DVRLiveReserve:           dvrLiveReserve,
		DVRDefaultPriority:       dvrDefaultPriority,
		DVRScanMaxDecodeErrors:   dvrScanMaxDecodeErrors,
		LiveStartupLead:          liveStartupLead,
		DVRCapacityRetryInterval: dvrCapacityRetryInterval,
		DVRCapacityRetryWindow:   dvrCapacityRetryWindow,
		FFmpegBinary:             os.Getenv("CONDUCTOR_FFMPEG_BINARY"),
		DiagDir:                  os.Getenv("CONDUCTOR_DIAG_DIR"),
		CommskipURL:              os.Getenv("CONDUCTOR_COMMSKIP_URL"),
		CommskipAPIKey:           os.Getenv("CONDUCTOR_COMMSKIP_API_KEY"),
		TMDbAPIKey:               os.Getenv("CONDUCTOR_TMDB_API_KEY"),
		TMDbBaseURL:              os.Getenv("CONDUCTOR_TMDB_BASE_URL"),
		EnrichInterval:           enrichInterval,
		TVDbAPIKey:               os.Getenv("CONDUCTOR_TVDB_API_KEY"),
		TVDbPIN:                  os.Getenv("CONDUCTOR_TVDB_PIN"),
		TVDbBaseURL:              os.Getenv("CONDUCTOR_TVDB_BASE_URL"),
		TVmazeEnabled:            parseBoolDefault(os.Getenv("CONDUCTOR_TVMAZE_ENABLED"), true),
		TVmazeBaseURL:            os.Getenv("CONDUCTOR_TVMAZE_BASE_URL"),
		SDUsername:               os.Getenv("CONDUCTOR_SD_USERNAME"),
		SDPassword:               os.Getenv("CONDUCTOR_SD_PASSWORD"),
		SDBaseURL:                os.Getenv("CONDUCTOR_SD_BASE_URL"),
		LogosDir:                 logosDirDefault(dataDir),
		LogoBaseURL:              os.Getenv("CONDUCTOR_LOGO_BASE_URL"),
		UpstreamProxy:            upstreamProxy,
		SportsAPIKey:             os.Getenv("CONDUCTOR_SPORTS_API_KEY"),
		SportsInterval:           parseDurationDefault(os.Getenv("CONDUCTOR_SPORTS_INTERVAL"), 0),
		PPVSyncEnabled:           parseBoolDefault(os.Getenv("CONDUCTOR_PPVSYNC_ENABLED"), true),
		PPVSyncInterval:          parseDurationDefault(os.Getenv("CONDUCTOR_PPV_SYNC_INTERVAL"), 0),
		PPVDisplayTimezone:       ppvDisplayTimezone,
		PosterCacheDir:           posterCacheDirDefault(dataDir),
		ImageGenURL:              os.Getenv("CONDUCTOR_IMAGE_GEN_URL"),
		ImageGenAPIKey:           os.Getenv("CONDUCTOR_IMAGE_GEN_API_KEY"),
		PosterInterval:           parseDurationDefault(os.Getenv("CONDUCTOR_POSTER_INTERVAL"), 10*time.Minute),
		SonarrURL:                os.Getenv("CONDUCTOR_SONARR_URL"),
		SonarrAPIKey:             os.Getenv("CONDUCTOR_SONARR_API_KEY"),
		RadarrURL:                os.Getenv("CONDUCTOR_RADARR_URL"),
		RadarrAPIKey:             os.Getenv("CONDUCTOR_RADARR_API_KEY"),
		ReconcileInterval:        parseDurationDefault(os.Getenv("CONDUCTOR_RECONCILE_INTERVAL"), time.Hour),
		ReconcileWindow:          parseDurationDefault(os.Getenv("CONDUCTOR_RECONCILE_WINDOW"), 14*24*time.Hour),
		ReconcileThreshold:       parseFloatDefault(os.Getenv("CONDUCTOR_RECONCILE_THRESHOLD"), 0.80),
		ReconcileMaxPerCycle:     parseIntDefault(os.Getenv("CONDUCTOR_RECONCILE_MAX_PER_CYCLE"), 20),
		// Dry-run defaults TRUE: a fresh deploy observes before it records.
		ReconcileDryRun:        parseBoolDefault(os.Getenv("CONDUCTOR_RECONCILE_DRY_RUN"), true),
		ArrImportTrigger:       parseBoolDefault(os.Getenv("CONDUCTOR_ARR_IMPORT_TRIGGER"), true),
		PlexURL:                os.Getenv("CONDUCTOR_PLEX_URL"),
		PlexToken:              os.Getenv("CONDUCTOR_PLEX_TOKEN"),
		ReconcileGapAutoRecord: parseBoolDefault(os.Getenv("CONDUCTOR_RECONCILE_GAP_AUTORECORD"), false),
	}, nil
}

// parseFloatDefault parses a float64 env, returning fallback when empty or
// unparseable so a typo can't silently zero a threshold.
func parseFloatDefault(s string, fallback float64) float64 {
	if s == "" {
		return fallback
	}
	if f, err := strconv.ParseFloat(strings.TrimSpace(s), 64); err == nil {
		return f
	}
	return fallback
}

// parseIntDefault parses an int env, returning fallback when empty or
// unparseable.
func parseIntDefault(s string, fallback int) int {
	if s == "" {
		return fallback
	}
	if n, err := strconv.Atoi(strings.TrimSpace(s)); err == nil {
		return n
	}
	return fallback
}

// envDuration reads a time.Duration env var. Unset returns 0 so the caller can
// tell "not configured" from an explicit value. Unparseable is a hard error —
// a mistyped padding value silently reverting to a default is exactly the kind
// of thing nobody notices until a recording is lost.
func envDuration(name string) (time.Duration, error) {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", name, err)
	}
	if d < 0 {
		return 0, fmt.Errorf("%s: must not be negative, got %s", name, d)
	}
	return d, nil
}

// parseBoolDefault parses common boolean shorthands and returns
// fallback when s is empty. Accepts true/yes/on/1 and false/no/off/0
// (case-insensitive). Unparseable values fall back so a misspelled env
// can't silently disable a feature.
func parseBoolDefault(s string, fallback bool) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "":
		return fallback
	case "true", "yes", "on", "1":
		return true
	case "false", "no", "off", "0":
		return false
	default:
		return fallback
	}
}

// parseDurationDefault: returns the parsed duration, or fallback when
// s is empty or unparseable. Single-purpose helper for env knobs whose
// default lives in the consuming worker, not config.
func parseDurationDefault(s string, fallback time.Duration) time.Duration {
	if s == "" {
		return fallback
	}
	if d, err := time.ParseDuration(s); err == nil {
		return d
	}
	return fallback
}

// posterCacheDirDefault: explicit env wins, otherwise fall back to a
// subfolder under the data dir so a default Conductor deploy gets
// branded posters working out of the box.
func posterCacheDirDefault(dataDir string) string {
	if v := os.Getenv("CONDUCTOR_POSTER_CACHE_DIR"); v != "" {
		return v
	}
	if dataDir == "" {
		return ""
	}
	return dataDir + "/posters"
}

// logosDirDefault keeps self-hosted logos on the persistent data volume unless
// an operator explicitly places them elsewhere.
func logosDirDefault(dataDir string) string {
	if v := os.Getenv("CONDUCTOR_LOGOS_DIR"); v != "" {
		return v
	}
	if dataDir == "" {
		return ""
	}
	return dataDir + "/logos"
}

func envDefault(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func loadOrCreateDeviceID(dataDir string) (string, error) {
	path := dataDir + "/device-id"
	if b, err := os.ReadFile(path); err == nil {
		s := strings.TrimSpace(string(b))
		if s != "" {
			return s, nil
		}
	}
	buf := make([]byte, 4)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	id := strings.ToUpper(hex.EncodeToString(buf)) // 8 hex chars, HDHomeRun shape
	if err := os.WriteFile(path, []byte(id+"\n"), 0o600); err != nil {
		return "", err
	}
	return id, nil
}

// guessBaseURL is a best-effort fallback. Production deploys MUST set
// CONDUCTOR_BASE_URL explicitly so Plex receives a stable, reachable URL.
func guessBaseURL(listen string) string {
	port := strings.TrimPrefix(listen, ":")
	if i := strings.LastIndex(listen, ":"); i >= 0 {
		port = listen[i+1:]
	}
	ifaces, err := net.InterfaceAddrs()
	if err != nil {
		return "http://127.0.0.1:" + port
	}
	for _, a := range ifaces {
		ipnet, ok := a.(*net.IPNet)
		if !ok || ipnet.IP.IsLoopback() {
			continue
		}
		v4 := ipnet.IP.To4()
		if v4 == nil {
			continue
		}
		return fmt.Sprintf("http://%s:%s", v4.String(), port)
	}
	return "http://127.0.0.1:" + port
}

// parseUpstreamProxy validates CONDUCTOR_UPSTREAM_PROXY. Errors never echo the
// value: it carries the proxy password.
func parseUpstreamProxy(raw string) (*url.URL, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.Port() == "" {
		return nil, errors.New("want an http(s) URL with an explicit host and port")
	}
	return u, nil
}
