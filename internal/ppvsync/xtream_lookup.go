// xtream_lookup.go — StreamLookup backed by the provider's own Xtream
// player_api catalogue (ppvsync v2, audit E2 2026-06-10).
//
// v1 read stream names out of Dispatcharr's internal Postgres, which made
// Conductor's PPV EPG depend on a second tuner system, a cross-stack
// docker network, and a credentialed DSN — and silently died twice when
// Dispatcharr did (2026-05-26, 2026-06-07). v2 asks the panel directly:
// one get_live_streams fetch per TTL answers every per-source lookup from
// an in-memory map keyed by stream id (the trailing integer of the
// /live/<user>/<pass>/<id>.ts source URLs).
package ppvsync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/spencercnorton/conductor/internal/security"
	"github.com/spencercnorton/conductor/internal/store"
	"github.com/spencercnorton/conductor/internal/xtream"
)

// XtreamLookup implements StreamLookup against one or more Xtream panel
// clients. Clients are tried in order per refresh — typically they are
// the same panel under different credentials, so the first success wins;
// later clients are failover.
type XtreamLookup struct {
	Clients []*xtream.Client
	Logger  *slog.Logger

	// TTL is how long a fetched catalogue answers lookups before a
	// refresh. Default 10m — comfortably under the worker's 15m pass so
	// each pass triggers at most one panel fetch.
	TTL time.Duration

	// MaxStale bounds how long a stale catalogue keeps serving when every
	// refresh attempt fails. Within the window lookups degrade gracefully
	// (names just lag); past it they error so the worker's monitor sees
	// the outage instead of an eternally-quiet "success".
	MaxStale time.Duration

	// FailureBackoff suppresses another panel fetch after every configured
	// client failed. LookupBySource is called once per channel source; without
	// a negative cache, a DNS outage turns one failed refresh into hundreds of
	// sequential timeout-length requests during a single ppvsync pass.
	FailureBackoff time.Duration

	now func() time.Time // test hook

	mu        sync.Mutex
	catalogue map[int64]string // stream_id → current provider name
	fetchedAt time.Time
	failedAt  time.Time
	failedErr error

	// A worker pass pins exactly one catalogue map + generation. Without this,
	// crossing TTL between redundant source lookups can combine T1 and T2 names
	// and falsely stamp a selected T1 state with T2 authority.
	passActive    bool
	passCatalogue map[int64]string
	passFetchedAt time.Time
}

// NewXtreamLookupFromDB builds an XtreamLookup from the enabled
// m3u_xtream providers + their first enabled credential. Returns an error
// when no usable provider/credential pair exists — callers treat that as
// "ppvsync cannot run on this deploy".
func NewXtreamLookupFromDB(ctx context.Context, db *store.DB, key *security.CredKey, logger *slog.Logger) (*XtreamLookup, error) {
	providers, err := db.ListProviders(ctx)
	if err != nil {
		return nil, fmt.Errorf("list providers: %w", err)
	}
	var clients []*xtream.Client
	for _, p := range providers {
		if !p.Enabled || p.Kind != "m3u_xtream" {
			continue
		}
		creds, err := db.ListCredentials(ctx, p.ID)
		if err != nil {
			return nil, fmt.Errorf("list credentials for %s: %w", p.Name, err)
		}
		for _, c := range creds {
			if !c.Enabled {
				continue
			}
			pw, err := key.Decrypt(c.PasswordEnc)
			if err != nil {
				if logger != nil {
					logger.Warn("ppvsync: credential decrypt failed; skipping",
						"provider", p.Name, "username", c.Username, "err", err)
				}
				continue
			}
			clients = append(clients, xtream.New(p.BaseURL, c.Username, string(pw), nil))
			break // one credential per provider is enough for catalogue reads
		}
	}
	if len(clients) == 0 {
		return nil, errors.New("no enabled m3u_xtream provider with a usable credential")
	}
	return NewXtreamLookup(clients, logger), nil
}

// NewXtreamLookup wires an XtreamLookup with defaults.
func NewXtreamLookup(clients []*xtream.Client, logger *slog.Logger) *XtreamLookup {
	return &XtreamLookup{
		Clients:        clients,
		Logger:         logger,
		TTL:            10 * time.Minute,
		MaxStale:       45 * time.Minute,
		FailureBackoff: 5 * time.Minute,
		now:            time.Now,
	}
}

// streamIDRe matches the trailing integer of Xtream live URLs:
// .../live/<user>/<pass>/<id>.ts, .../<id>.m3u8, or a bare /<id>.
var streamIDRe = regexp.MustCompile(`/(\d+)(?:\.(?:ts|m3u8))?$`)

// BeginPass pins one catalogue generation for all source lookups in a worker
// pass. The database pass lease ensures refreshes are also ordered across
// processes; this method prevents a TTL boundary from mixing generations
// within one process.
func (l *XtreamLookup) BeginPass(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.passActive {
		return errors.New("xtream catalogue pass already active")
	}
	cat, fetchedAt, err := l.currentCatalogueLocked(ctx)
	if err != nil {
		return err
	}
	l.passActive = true
	l.passCatalogue = cat
	l.passFetchedAt = fetchedAt
	return nil
}

// EndPass releases the pinned catalogue generation.
func (l *XtreamLookup) EndPass() {
	l.mu.Lock()
	l.passActive = false
	l.passCatalogue = nil
	l.passFetchedAt = time.Time{}
	l.mu.Unlock()
}

// StreamIDFromURL extracts the Xtream stream id from a channel_source
// upstream URL. ok=false when the URL doesn't look like an Xtream live
// stream (plain-M3U sources etc.).
func StreamIDFromURL(upstreamURL string) (int64, bool) {
	m := streamIDRe.FindStringSubmatch(upstreamURL)
	if m == nil {
		return 0, false
	}
	id, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil || id <= 0 {
		return 0, false
	}
	return id, true
}

// LookupBySource resolves the provider's current name for the stream
// behind src. ErrNotFound when the URL has no stream id or the id isn't
// in the catalogue.
func (l *XtreamLookup) LookupBySource(ctx context.Context, src store.ChannelSource) (StreamRow, error) {
	id, ok := StreamIDFromURL(src.UpstreamURL)
	if !ok {
		return StreamRow{}, ErrNotFound
	}
	l.mu.Lock()
	cat, fetchedAt, pinned := l.passCatalogue, l.passFetchedAt, l.passActive
	l.mu.Unlock()
	if !pinned {
		var err error
		cat, fetchedAt, err = l.currentCatalogue(ctx)
		if err != nil {
			return StreamRow{}, err
		}
	}
	name, ok := cat[id]
	if !ok {
		// Preserve the catalogue generation even for absence: remembered guide
		// continuity may publish placeholders, and every mutation must be bound to
		// the concrete snapshot that authorised it.
		return StreamRow{ID: id, UpdatedAt: fetchedAt}, ErrNotFound
	}
	return StreamRow{ID: id, Name: name, UpdatedAt: fetchedAt}, nil
}

// Close satisfies StreamLookup; HTTP clients hold no resources to release.
func (l *XtreamLookup) Close() {}

// currentCatalogue returns a fresh-enough catalogue, refreshing from the
// panel when the TTL has lapsed. On refresh failure a stale catalogue
// keeps serving up to MaxStale before lookups start erroring.
func (l *XtreamLookup) currentCatalogue(ctx context.Context) (map[int64]string, time.Time, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.currentCatalogueLocked(ctx)
}

func (l *XtreamLookup) currentCatalogueLocked(ctx context.Context) (map[int64]string, time.Time, error) {
	now := l.now()
	age := now.Sub(l.fetchedAt)
	if l.catalogue != nil && age < l.TTL {
		return l.catalogue, l.fetchedAt, nil
	}
	if l.failedErr != nil && now.Sub(l.failedAt) < l.FailureBackoff {
		if l.catalogue != nil && age < l.MaxStale {
			return l.catalogue, l.fetchedAt, nil
		}
		return nil, time.Time{}, l.failedErr
	}

	var lastErr error
	for _, c := range l.Clients {
		streams, err := c.GetLiveStreams(ctx)
		if err != nil {
			lastErr = err
			if l.Logger != nil {
				l.Logger.Warn("ppvsync catalogue fetch failed",
					"panel", c.BaseURL(), "err", err)
			}
			continue
		}
		cat := make(map[int64]string, len(streams))
		for _, s := range streams {
			cat[s.StreamID] = s.Name
		}
		l.catalogue = cat
		l.fetchedAt = l.now()
		l.failedAt = time.Time{}
		l.failedErr = nil
		if l.Logger != nil {
			l.Logger.Info("ppvsync catalogue refreshed",
				"panel", c.BaseURL(), "streams", len(cat))
		}
		return l.catalogue, l.fetchedAt, nil
	}

	if lastErr == nil {
		lastErr = errors.New("no xtream clients configured")
	}
	l.failedAt = l.now()
	l.failedErr = lastErr
	if l.catalogue != nil && age < l.MaxStale {
		if l.Logger != nil {
			l.Logger.Warn("ppvsync serving stale catalogue",
				"age", age.Round(time.Second), "err", lastErr)
		}
		return l.catalogue, l.fetchedAt, nil
	}
	return nil, time.Time{}, lastErr
}
