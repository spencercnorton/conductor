// Command conductor is the entrypoint binary for the Conductor service:
// HDHomeRun emulator + multi-credential stream pool + EPG enrichment for
// Plex Live TV.
//
// Two run modes:
//
//   - Discovery-only (no CONDUCTOR_POSTGRES_DSN): the binary boots the
//     HDHomeRun emulator with one hardcoded test channel so a fresh deploy
//     can complete pairing in Plex without any provider configuration.
//   - Full (CONDUCTOR_POSTGRES_DSN set): boots Postgres pool, runs migrations,
//     starts the orphan upstream sweeper, mounts the admin CRUD API, and
//     serves real channels via the slot allocator (spec §4.1).
//
// The transition between modes is just env: set the DSN and restart.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	// Embed the IANA tz database in the binary so America/New_York,
	// Europe/London, etc. always load even on a scratch image without
	// system tzdata — otherwise ppvparse silently falls back to fixed
	// offsets and mis-times events across DST. Belt-and-braces: the prod
	// image currently ships tzdata, this guarantees it regardless.
	_ "time/tzdata"

	"github.com/spencercnorton/conductor/internal/alerts"
	"github.com/spencercnorton/conductor/internal/api"
	"github.com/spencercnorton/conductor/internal/auth"
	"github.com/spencercnorton/conductor/internal/config"
	"github.com/spencercnorton/conductor/internal/dvr"
	"github.com/spencercnorton/conductor/internal/enrich"
	"github.com/spencercnorton/conductor/internal/epg"
	"github.com/spencercnorton/conductor/internal/hdhr"
	"github.com/spencercnorton/conductor/internal/poster"
	"github.com/spencercnorton/conductor/internal/postprocess"
	"github.com/spencercnorton/conductor/internal/ppvsync"
	"github.com/spencercnorton/conductor/internal/reconcile"
	"github.com/spencercnorton/conductor/internal/sd"
	"github.com/spencercnorton/conductor/internal/security"
	"github.com/spencercnorton/conductor/internal/sports"
	"github.com/spencercnorton/conductor/internal/store"
	"github.com/spencercnorton/conductor/internal/stream"
	"github.com/spencercnorton/conductor/internal/version"
)

func main() {
	level, levelErr := logLevelFromEnv(os.Getenv("CONDUCTOR_LOG_LEVEL"))
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)
	if levelErr != nil {
		logger.Warn("log level not applied; staying at info", "err", levelErr)
	}

	cfg, err := config.Load()
	if err != nil {
		logger.Error("config load failed", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Default TunerCount: env-configured. Replaced below in DB mode with a
	// live-querying callback so /discover.json reflects the current sum
	// without requiring a Conductor restart.
	device := hdhr.Device{
		FriendlyName:    "Conductor (Default)",
		DeviceID:        cfg.DeviceID,
		DeviceAuth:      "conductor",
		ModelNumber:     "HDTC-2US",
		FirmwareName:    "hdhomeruntc_atsc",
		FirmwareVersion: "20200225",
		TunerCount:      hdhr.StaticTunerCount(cfg.TunerCount),
		BaseURL:         cfg.BaseURL,
	}

	credKey, err := security.LoadCredKey(cfg.CredKeyHex)
	if err != nil {
		logger.Error("cred key load failed", "err", err)
		os.Exit(1)
	}

	authCfg := auth.LoadFromEnv(os.Getenv)
	logger.Info("auth configured",
		"enabled", authCfg.Enabled,
		"api_key_set", authCfg.AdminAPIKey != "",
		"auth_portal_url", authCfg.AuthPortalURL,
		"allowed_principals", len(authCfg.AllowedPrincipals),
	)

	alertClient := alerts.New(cfg.OpsBotWebhookURL, cfg.OpsBotSecret, logger)
	// Continuous stream-health canary: watches conductor's own counters and
	// pages on teardown-class bursts (failovers, slate holds, starvation,
	// ffmpeg respawn storms). Send is a no-op without a webhook URL.
	go alerts.NewSentinel(alertClient, logger).Run(ctx)

	deps := api.Deps{
		Logger:         logger,
		Device:         device,
		CredKey:        credKey,
		Auth:           authCfg,
		Alerts:         alertClient,
		LogosDir:       cfg.LogosDir,
		LogoBaseURL:    cfg.LogoBaseURL,
		PosterCacheDir: cfg.PosterCacheDir,
		SeedLineup:     hdhr.SeedLineup(cfg.BaseURL),
	}

	// DB mode: open pool, run migrations, derive TunerCount from credentials,
	// start the orphan sweeper and Pool.
	var db *store.DB
	var runtimePool *stream.Pool
	var runtimeScheduler *dvr.Scheduler
	runtimeFailure := make(chan error, 1)
	if cfg.PostgresDSN != "" {
		db, err = store.Open(ctx, cfg.PostgresDSN)
		if err != nil {
			logger.Error("postgres open failed", "err", err)
			os.Exit(1)
		}
		defer db.Close()

		// Stream pumps and their in-memory fan-out registry are process-local,
		// while startup reconciliation mutates every active_stream row. Own a
		// dedicated PostgreSQL session lock before migrations or reconciliation
		// so overlapping boots cannot race schema changes or undercount real
		// upstreams.
		runtimeOwner, err := db.AcquireRuntimeSingleton(ctx)
		if err != nil {
			logger.Error("conductor runtime ownership failed", "err", err)
			os.Exit(1)
		}
		defer func() {
			closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := runtimeOwner.Close(closeCtx); err != nil {
				logger.Error("conductor runtime ownership release failed", "err", err)
			}
		}()
		go func() {
			err := runtimeOwner.Monitor(ctx, store.DefaultRuntimeSingletonCheckInterval)
			if ctx.Err() != nil && errors.Is(err, context.Canceled) {
				return
			}
			if err == nil {
				err = errors.New("runtime ownership monitor stopped unexpectedly")
			}
			runtimeFailure <- err
			stop()
		}()
		logger.Info("conductor runtime ownership acquired")

		if err := db.Migrate(ctx); err != nil {
			logger.Error("postgres migrate failed", "err", err)
			os.Exit(1)
		}
		logger.Info("postgres migrations applied")

		// Startup reconcile: no stream pump survives a restart, so any
		// active_stream row from a previous process is a zombie holding a
		// leased credential slot that SweepOrphans can never reclaim
		// (state='running'/client_count=1). Mark them dead before the Pool
		// and watchdog start so the sweeper reaps them (incident 2026-07-12:
		// a leaked slot blocked PPV failover). The runtime singleton proves no
		// legitimate foreign pump exists, making this cleanup load-bearing for
		// allocator truth; fail closed if PostgreSQL cannot perform it.
		if recovered, err := db.ReconcileStartup(ctx); err != nil {
			logger.Error("startup runtime reconcile failed", "err", err)
			os.Exit(1)
		} else if recovered.ActiveStreams > 0 || recovered.DVRRecordings > 0 {
			logger.Warn("startup reconcile recovered interrupted runtime state",
				"active_streams_marked_dead", recovered.ActiveStreams,
				"recordings_marked_failed", recovered.DVRRecordings)
		}
		if recovered, err := dvr.RecoverInterruptedArtifactPublishes(ctx, db, logger); err != nil {
			logger.Error("startup DVR artifact publish recovery failed", "err", err)
			os.Exit(1)
		} else if recovered > 0 {
			logger.Warn("startup recovered interrupted DVR artifact publishes", "count", recovered)
		}
		if reconciled, err := dvr.ReconcileCurrentDVRArtifacts(ctx, db, logger); err != nil {
			logger.Error("startup DVR artifact ownership reconcile failed", "err", err)
			os.Exit(1)
		} else if reconciled.Demoted > 0 || reconciled.Indeterminate > 0 {
			logger.Warn("startup reconciled current DVR artifact ownership",
				"demoted", reconciled.Demoted,
				"retained_indeterminate", reconciled.Indeterminate,
				"inspection_budget_exhausted", reconciled.InspectionBudgetExhausted,
				"inspection_worker_may_remain", reconciled.InspectionWorkerMayRemain)
		}

		// Live tuner count: query sum(max_streams) on every /discover.json hit.
		// Plex doesn't poll often (once at setup, then on manual rescan), so
		// the per-call cost is negligible. Falls back to the env value when
		// no credentials are configured.
		device.TunerCount = func() int {
			lookupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if n, err := db.CountTuners(lookupCtx); err == nil && n > 0 {
				return n
			}
			return cfg.TunerCount
		}
		deps.Device = device

		resolver := security.NewResolver(credKey)
		pool := stream.NewPool(logger, db, resolver)
		runtimePool = pool
		pool.Alerts = alertClient
		pool.FFmpegBinary = cfg.FFmpegBinary
		pool.DiagDir = cfg.DiagDir
		pool.LiveStartupLead = cfg.LiveStartupLead
		if cfg.UpstreamProxy != nil {
			pool.SetUpstreamProxy(cfg.UpstreamProxy)
			logger.Info("provider traffic via upstream proxy", "proxy", cfg.UpstreamProxy.Host)
		}
		defer pool.Close()
		// Render the continuity slate before Plex can open a tuner. A lazy
		// first render during a live outage would consume the same sub-ten-second
		// survival budget the slate is meant to protect.
		if err := requireContinuitySlate(ctx, pool); err != nil {
			logger.Error("required source-unavailable slate preparation failed", "err", err)
			os.Exit(1)
		}

		go stream.Watchdog(ctx, db, logger, stream.DefaultWatchdogConfig(), pool)

		registerDBGauges(db)

		// EPG: scheduled ingest worker. Alerts on per-source consecutive
		// fetch failures and on the guide horizon dropping below the
		// threshold (audit 2026-06-09, E5).
		epgWorker := epg.NewWorker(logger, db, cfg.EPGRefreshInterval)
		epgWorker.Alerts = alertClient
		go epgWorker.Run(ctx)

		// Enrichment worker (Phase 3). Only started when at least one
		// enrichment source is configured (TMDb / TVDb / TVmaze).
		var enrichWorker *enrich.Worker
		if cfg.TMDbAPIKey != "" || cfg.TVDbAPIKey != "" || cfg.TVmazeEnabled {
			adapter := &enrich.StoreAdapter{DB: db}
			var tmdb *enrich.TMDbClient
			var tvdb *enrich.TVDbClient
			var tvmaze *enrich.TVmazeClient

			if cfg.TMDbAPIKey != "" {
				tmdb = enrich.NewTMDbClient(cfg.TMDbBaseURL, cfg.TMDbAPIKey)
				tmdb.Cache = adapter
			}
			enricher := enrich.NewEnricher(tmdb, adapter)
			if cfg.TVDbAPIKey != "" {
				tvdb = enrich.NewTVDbClient(cfg.TVDbBaseURL, cfg.TVDbAPIKey, cfg.TVDbPIN)
				// TVDb cache uses the same DB-backed adapter shape as TMDb.
				tvdb.Cache = &tvdbCacheAdapter{DB: db}
				enricher = enricher.WithTVDb(tvdb)
			}
			if cfg.TVmazeEnabled {
				tvmaze = enrich.NewTVmazeClient(cfg.TVmazeBaseURL)
				// TVmaze cache shares the same backing table as TVDb —
				// the `kind` column namespaces the entries
				// (tvdbCacheAdapter satisfies the structurally-identical
				// enrich.TVmazeCache interface).
				tvmaze.Cache = &tvdbCacheAdapter{DB: db}
				enricher = enricher.WithTVmaze(tvmaze)
			}
			enrichWorker = enrich.NewWorker(logger, enricher, adapter)
			enrichWorker.Interval = cfg.EnrichInterval
			go enrichWorker.Run(ctx)
			logger.Info("enrichment worker enabled",
				"interval", cfg.EnrichInterval,
				"tmdb", tmdb != nil, "tvdb", tvdb != nil, "tvmaze", tvmaze != nil)
		}

		// Sports schedule worker (Phase 5b). Boots whenever the binary
		// has migrations applied — even with no mappings yet, the
		// worker is cheap (skips early when ListChannelSportMappings
		// returns nothing). API key defaults to TheSportsDB's free key.
		sportsClient := sports.NewClient(cfg.SportsAPIKey)
		sportsWorker := sports.New(db, sportsClient, logger)
		if cfg.SportsInterval > 0 {
			sportsWorker.Interval = cfg.SportsInterval
		}
		sportsMonitor := alerts.NewWorkerMonitor("sports", 0, alertClient) // StallAfter defaults to 3×Interval in Run
		sportsWorker.Monitor = sportsMonitor
		sportsWorker.Alerts = alertClient // ESPN breaker open/recovered events
		deps.SportsMonitor = sportsMonitor
		go sportsWorker.Run(ctx)
		logger.Info("sports worker enabled",
			"interval", sportsWorker.Interval,
			"api_key_set", cfg.SportsAPIKey != "")

		// PPV stream-name sync worker (Phase 5d / audit E2). The upstream
		// IPTV provider dynamically renames PPV streams as games are
		// scheduled; ppvsync polls the provider's player_api stream
		// catalogue (the same Xtream panel the channel sources point at),
		// parses names via internal/ppvparse, and injects matching events
		// as epg_program rows. v2 (2026-06-10) reads the provider
		// directly — the Dispatcharr-Postgres hop and the cross-stack
		// docker network it required are gone.
		var ppvMonitor *alerts.WorkerMonitor
		if cfg.PPVSyncEnabled {
			ppvMonitor = alerts.NewWorkerMonitor("ppvsync", 0, alertClient) // StallAfter defaults to 3×Interval in Run
			deps.PPVMonitor = ppvMonitor
			ppvDisplayLocation, err := time.LoadLocation(cfg.PPVDisplayTimezone)
			if err != nil {
				// Config.Load validates this before any workers start; keep
				// the guard local so a future alternate Config constructor
				// cannot silently render UTC times into Plex titles.
				logger.Error("ppvsync display timezone invalid",
					"timezone", cfg.PPVDisplayTimezone, "err", err)
				ppvMonitor.InitFailed(ctx, err.Error())
			} else {
				// Init + run in one goroutine so a slow provider catalogue
				// can't block conductor startup (Plex tuner discovery depends
				// on the HTTP listener coming up promptly).
				go func() {
					lookup, err := ppvsync.NewXtreamLookupFromDB(ctx, db, credKey, catalogueClient(cfg), logger)
					if err != nil {
						logger.Error("ppvsync init failed: no usable xtream provider", "err", err)
						ppvMonitor.InitFailed(ctx, err.Error())
						return
					}
					defer lookup.Close()

					ppvWorker := ppvsync.New(db, lookup, logger)
					if cfg.PPVSyncInterval > 0 {
						ppvWorker.Interval = cfg.PPVSyncInterval
					}
					ppvWorker.DisplayLocation = ppvDisplayLocation
					ppvWorker.Monitor = ppvMonitor
					logger.Info("ppvsync worker enabled",
						"interval", ppvWorker.Interval,
						"display_timezone", ppvDisplayLocation.String(),
						"mode", "xtream")
					ppvWorker.Run(ctx)
				}()
			}
		}

		// Schedules Direct ingester (Phase 3b). Only started when SD
		// credentials are set + at least one sd_lineup is enabled.
		if cfg.SDUsername != "" && cfg.SDPassword != "" {
			sdClient := sd.New(cfg.SDBaseURL, cfg.SDUsername, cfg.SDPassword)
			sdIngester := sd.NewIngester(logger, sdClient, db, cfg.EPGRefreshInterval)
			go sdIngester.Run(ctx)
			logger.Info("schedules direct ingester enabled",
				"interval", cfg.EPGRefreshInterval, "username", cfg.SDUsername)
		}

		deps.DB = db
		deps.Pool = pool
		deps.EPGWorker = epgWorker
		deps.EnrichWorker = enrichWorker

		// Durable poster worker: researched overrides are always available;
		// Image generation activates only when both URL + API key exist. It
		// runs entirely off the guide/stream request path.
		if cfg.PosterCacheDir != "" {
			var imageGenerator enrich.PosterImageGenerator
			if cfg.ImageGenURL != "" && cfg.ImageGenAPIKey != "" {
				imageGenerator = enrich.NewImageGen(cfg.ImageGenURL, cfg.ImageGenAPIKey)
			}
			posterWorker := enrich.NewPosterWorker(
				logger, db, poster.NewAssetStore(cfg.PosterCacheDir), imageGenerator,
			)
			posterWorker.Interval = cfg.PosterInterval
			deps.PosterWorker = posterWorker
			go posterWorker.Run(ctx)
			logger.Info("poster worker enabled",
				"interval", cfg.PosterInterval,
				"generation_enabled", imageGenerator != nil)
		}

		// DVR-as-Indexer (Phase 3a). Only enabled when an indexer key is set —
		// the indexer + grab handler + scheduler all activate together.
		if cfg.DVRIndexerKey != "" {
			torznab := dvr.New(db, cfg.BaseURL, cfg.DVRIndexerKey, cfg.DVRIndexerName)
			startSlack := cfg.DVRStartSlack
			if startSlack <= 0 {
				startSlack = dvr.DefaultStartSlack
			}
			maxOverrun := cfg.DVRMaxOverrun
			if maxOverrun <= 0 {
				maxOverrun = dvr.DefaultMaxOverrun
			}
			admissionPolicy := store.DVRAdmissionPolicy{
				LiveReserve: cfg.DVRLiveReserve,
				StartSlack:  startSlack,
				MaxOverrun:  maxOverrun,
			}
			schedule := &dvr.ScheduleHandler{
				DB: db, Torznab: torznab,
				OutputDir: cfg.DVROutputDir, IndexerKey: cfg.DVRIndexerKey,
				Priority: cfg.DVRDefaultPriority, AdmissionPolicy: &admissionPolicy,
			}
			recorder := dvr.NewRecorder(logger, db, pool, alertClient)
			recorder.StartSlack = startSlack
			recorder.MaxOverrun = maxOverrun
			recorder.MediaProcessor = dvr.NewFFmpegMediaProcessor(
				pool.FFmpegBinary, cfg.DVRScanMaxDecodeErrors)
			logger.Info("dvr recorder padding",
				"start_slack", recorder.StartSlack, "max_overrun", recorder.MaxOverrun,
				"scan_max_decode_errors", cfg.DVRScanMaxDecodeErrors)

			// Phase 4b: post-record pipeline (commskip-auto + Whisper + LLM)
			// followed by an *arr import-scan trigger. The import stage runs
			// last so a commskip cut (dry run off) is written before *arr imports.
			var stages []postprocess.Stage
			var stageNames []string
			if cfg.CommskipURL != "" {
				stages = append(stages,
					postprocess.NewCommskipStage(cfg.CommskipURL, cfg.CommskipAPIKey, cfg.CommskipDryRun, logger))
				stageNames = append(stageNames, "commskip-auto")
			}
			arrConfigured := (cfg.SonarrURL != "" && cfg.SonarrAPIKey != "") ||
				(cfg.RadarrURL != "" && cfg.RadarrAPIKey != "")
			if cfg.ArrImportTrigger && arrConfigured {
				stages = append(stages, postprocess.NewArrImportStage(
					postprocess.ArrTarget{BaseURL: cfg.SonarrURL, APIKey: cfg.SonarrAPIKey},
					postprocess.ArrTarget{BaseURL: cfg.RadarrURL, APIKey: cfg.RadarrAPIKey},
				))
				stageNames = append(stageNames, "arr-import")
			}
			if len(stages) > 0 {
				recorder.PostProcess = &postprocess.Pipeline{Logger: logger, Stages: stages}
				logger.Info("postprocess pipeline enabled", "stages", stageNames)
			}

			scheduler := dvr.NewScheduler(logger, db, recorder)
			runtimeScheduler = scheduler
			scheduler.LiveReserve = cfg.DVRLiveReserve
			if cfg.DVRCapacityRetryInterval > 0 {
				scheduler.CapacityRetryInterval = cfg.DVRCapacityRetryInterval
			}
			if cfg.DVRCapacityRetryWindow > 0 {
				scheduler.CapacityRetryWindow = cfg.DVRCapacityRetryWindow
			}
			go scheduler.Run(ctx)

			deps.Torznab = torznab
			deps.DVRSchedule = schedule
			deps.DVRScheduler = scheduler

			logger.Info("dvr-as-indexer enabled",
				"output_dir", cfg.DVROutputDir,
				"indexer_name", cfg.DVRIndexerName)

			// Proactive reconciler (internal/reconcile). Inverts the
			// passive-indexer flow: pulls Sonarr/Radarr wanted-lists,
			// matches them against the upcoming EPG, and schedules
			// recordings the existing scheduler above then records.
			// Only starts when at least one *arr is configured.
			var sonarrClient *reconcile.SonarrClient
			var radarrClient *reconcile.RadarrClient
			if cfg.SonarrURL != "" && cfg.SonarrAPIKey != "" {
				sonarrClient = reconcile.NewSonarrClient(cfg.SonarrURL, cfg.SonarrAPIKey)
			}
			if cfg.RadarrURL != "" && cfg.RadarrAPIKey != "" {
				radarrClient = reconcile.NewRadarrClient(cfg.RadarrURL, cfg.RadarrAPIKey)
			}
			var plexClient *reconcile.PlexClient
			if cfg.PlexURL != "" && cfg.PlexToken != "" {
				plexClient = reconcile.NewPlexClient(cfg.PlexURL, cfg.PlexToken)
			}
			if sonarrClient != nil || radarrClient != nil || plexClient != nil {
				rec := &reconcile.Worker{
					Logger:          logger,
					DB:              db,
					Sonarr:          sonarrClient,
					Radarr:          radarrClient,
					Plex:            plexClient,
					GapAutoRecord:   cfg.ReconcileGapAutoRecord,
					Interval:        cfg.ReconcileInterval,
					Window:          cfg.ReconcileWindow,
					Threshold:       cfg.ReconcileThreshold,
					MaxPerCycle:     cfg.ReconcileMaxPerCycle,
					DryRun:          cfg.ReconcileDryRun,
					OutputDir:       cfg.DVROutputDir,
					Priority:        cfg.DVRDefaultPriority,
					AdmissionPolicy: &admissionPolicy,
				}
				go rec.Run(ctx)
				deps.Reconciler = rec
				logger.Info("dvr reconciler enabled",
					"sonarr", sonarrClient != nil,
					"radarr", radarrClient != nil,
					"plex_gap", plexClient != nil,
					"dry_run", cfg.ReconcileDryRun)
			}
		}
		logger.Info("db mode enabled", "dsn_host", maskDSN(cfg.PostgresDSN))
	} else {
		logger.Warn("running in discovery-only mode (CONDUCTOR_POSTGRES_DSN not set)")
	}

	logger.Info("conductor starting",
		"version", version.Version,
		"commit", version.Commit,
		"listen", cfg.ListenAddr,
		"base_url", cfg.BaseURL,
		"logo_base_url", cfg.LogoBaseURL,
		"tuner_count", deps.Device.TunerCount(),
	)

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           api.NewRouter(deps),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       5 * time.Minute,
	}

	go func() {
		logger.Info("http listening", "addr", cfg.ListenAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http listen failed", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	var ownershipErr error
	select {
	case ownershipErr = <-runtimeFailure:
		logger.Error("conductor runtime ownership lost; terminating", "err", ownershipErr)
	default:
	}
	// Stop admission and join recorder cleanup while Pool and PostgreSQL remain
	// available: reservation release, partial-file close, and the durable
	// recording→cancelled transition all precede runtime ownership handoff.
	if runtimeScheduler != nil {
		runtimeScheduler.Close()
	}
	// As soon as the bounded singleton monitor reports loss, stop live pumps
	// before waiting for recorder cleanup; the scheduler still joins below.
	if ownershipErr != nil && runtimePool != nil {
		runtimePool.Close()
	}
	if runtimeScheduler != nil {
		schedulerCtx, schedulerCancel := context.WithTimeout(context.Background(), 15*time.Second)
		err := runtimeScheduler.Wait(schedulerCtx)
		schedulerCancel()
		if err != nil {
			if runtimePool != nil {
				runtimePool.Close()
			}
			logger.Error("dvr scheduler did not quiesce before runtime handoff", "err", err)
			os.Exit(1)
		}
	}
	// Pump contexts intentionally outlive individual HTTP requests. Reject new
	// work and cancel every pump before the graceful HTTP wait. On ordinary
	// shutdown the singleton stays held until WaitForPumps below proves their
	// upstream/ffmpeg teardown is complete; ownership loss still closes them at
	// the earliest detected moment.
	if runtimePool != nil {
		runtimePool.Close()
	}
	logger.Info("shutdown requested")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("http shutdown failed", "err", err)
		os.Exit(1)
	}
	if runtimePool != nil {
		quiesceCtx, quiesceCancel := context.WithTimeout(context.Background(), 15*time.Second)
		err := runtimePool.WaitForPumps(quiesceCtx)
		quiesceCancel()
		if err != nil {
			// Do not run the singleton unlock defer on a teardown timeout. Process
			// death closes the provider pumps and dedicated DB lock session as one
			// fail-closed boundary.
			logger.Error("stream pumps did not quiesce before runtime handoff", "err", err)
			os.Exit(1)
		}
	}
	logger.Info("conductor stopped")
	if ownershipErr != nil {
		os.Exit(1)
	}
}

type continuitySlatePreparer interface {
	PrepareSlate(context.Context) error
}

func requireContinuitySlate(ctx context.Context, preparer continuitySlatePreparer) error {
	slateCtx, cancel := context.WithTimeout(ctx, stream.ClientStartupBudget)
	defer cancel()
	if err := preparer.PrepareSlate(slateCtx); err != nil {
		return fmt.Errorf("prepare continuity slate: %w", err)
	}
	return nil
}

// tvdbCacheAdapter wraps store.DB to satisfy enrich.TVDbCache. Lives here
// (not in enrich.StoreAdapter) so the existing TMDb cache stays cleanly
// separated — different DB tables, different kind enums.
type tvdbCacheAdapter struct{ DB *store.DB }

func (a *tvdbCacheAdapter) Get(ctx context.Context, kind, queryHash string) (json.RawMessage, bool, error) {
	return a.DB.GetTVDbCache(ctx, kind, queryHash)
}
func (a *tvdbCacheAdapter) Put(ctx context.Context, kind, queryHash, queryLabel string, response json.RawMessage) error {
	return a.DB.PutTVDbCache(ctx, kind, queryHash, queryLabel, response)
}

// maskDSN trims the password from a postgres DSN so it can be safely logged.
// Returns the host:port portion only.
func maskDSN(dsn string) string {
	// Cheap heuristic — find "@" and "/db" markers, log the host slice.
	at := -1
	slash := -1
	for i, c := range dsn {
		if c == '@' && at < 0 {
			at = i
		}
		if c == '/' && i > at && slash < 0 {
			slash = i
		}
	}
	if at >= 0 && slash > at {
		return dsn[at+1 : slash]
	}
	return "<dsn>"
}

// catalogueClient is ppvsync's catalogue client: nil (xtream's direct default)
// unless CONDUCTOR_UPSTREAM_PROXY routes provider traffic.
func catalogueClient(cfg config.Config) *http.Client {
	if cfg.UpstreamProxy == nil {
		return nil
	}
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.Proxy = http.ProxyURL(cfg.UpstreamProxy)
	return &http.Client{Timeout: 120 * time.Second, Transport: t}
}
