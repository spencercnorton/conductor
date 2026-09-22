package enrich

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/poster"
	"github.com/spencercnorton/conductor/internal/store"
)

type PosterWorkerStore interface {
	ListPosterCandidates(context.Context, int) ([]store.PosterCandidate, error)
	EnsurePosterAsset(context.Context, store.PosterAsset) (store.PosterAsset, error)
	GetPosterAsset(context.Context, string) (store.PosterAsset, error)
	MarkPosterAssetReady(context.Context, string, string, string) error
	MarkPosterAssetFailed(context.Context, string, string, time.Time) error
	MarkPosterAssetSubmitted(context.Context, string, string) error
	ClearPosterAssetRemoteJob(context.Context, string) error
	ListPosterOverrides(context.Context) ([]store.PosterOverride, error)
	ApplyPosterToProgram(context.Context, uuid.UUID, string, string, bool) (bool, error)
	ClearGeneratedPosterForProgram(context.Context, uuid.UUID, string) error
}

type PosterImageGenerator interface {
	Submit(context.Context, GenerateRequest) (string, error)
	Wait(context.Context, string) ([]byte, error)
}

type PosterWorker struct {
	Logger                *slog.Logger
	Store                 PosterWorkerStore
	Assets                *poster.AssetStore
	Generator             PosterImageGenerator
	RemoteHTTP            *http.Client
	Interval              time.Duration
	BatchSize             int
	MaxGenerationsPerPass int
	wake                  chan struct{}
	runMu                 sync.Mutex
}

type PosterWorkerStats struct {
	Candidates       int `json:"candidates"`
	ResearchedStored int `json:"researched_stored"`
	ResearchedFailed int `json:"researched_failed"`
	OverridesApplied int `json:"overrides_applied"`
	GeneratedApplied int `json:"generated_applied"`
	Generations      int `json:"generations"`
	Failures         int `json:"failures"`
	Skipped          int `json:"skipped"`
}

func NewPosterWorker(logger *slog.Logger, s PosterWorkerStore, assets *poster.AssetStore, generator PosterImageGenerator) *PosterWorker {
	return &PosterWorker{
		Logger: logger, Store: s, Assets: assets, Generator: generator,
		RemoteHTTP: newPublicPosterHTTPClient(),
		// Scan the full normal 14-day guide window. A small LIMIT can permanently
		// starve later researched matches behind recurring ad/idle rows.
		Interval: 10 * time.Minute, BatchSize: 20000, MaxGenerationsPerPass: 1,
		wake: make(chan struct{}, 1),
	}
}

func (w *PosterWorker) Run(ctx context.Context) {
	interval := w.Interval
	if interval <= 0 {
		interval = 10 * time.Minute
	}
	w.Logger.Info("poster worker starting", "interval", interval,
		"generation_enabled", w.Generator != nil)
	w.runOnce(ctx)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			w.Logger.Info("poster worker stopped")
			return
		case <-t.C:
			w.runOnce(ctx)
		case <-w.wake:
			w.runOnce(ctx)
		}
	}
}

func (w *PosterWorker) Wake() {
	if w == nil || w.wake == nil {
		return
	}
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

func (w *PosterWorker) RunOnce(ctx context.Context) PosterWorkerStats { return w.runOnce(ctx) }

func (w *PosterWorker) runOnce(ctx context.Context) PosterWorkerStats {
	var stats PosterWorkerStats
	if w == nil || w.Store == nil || w.Assets == nil {
		return stats
	}
	w.runMu.Lock()
	defer w.runMu.Unlock()
	defer func() { w.logPass(stats) }()

	// Materialize every researched override independently of the missing-art
	// candidate list. A current provider/source poster may outrank the override,
	// but the researched fallback still needs to be validated and durably cached
	// now so it does not depend on the remote URL remaining available later.
	overrides, err := w.Store.ListPosterOverrides(ctx)
	if err != nil {
		w.Logger.Warn("poster overrides failed", "err", err)
		stats.Failures++
		return stats
	}
	resolved := map[string]store.PosterAsset{}
	seenAssets := make(map[string]struct{}, len(overrides))
	for _, o := range overrides {
		if ctx.Err() != nil {
			break
		}
		if _, seen := seenAssets[o.AssetKey]; seen {
			continue
		}
		seenAssets[o.AssetKey] = struct{}{}
		wasReady := o.Asset.Status == "ready" && w.Assets.Exists(o.Asset.LocalPath)
		a, ready, err := w.ensureResearched(ctx, o.Asset, resolved)
		resolved[o.AssetKey] = a
		if err != nil {
			stats.ResearchedFailed++
			stats.Failures++
			continue
		}
		if ready && !wasReady {
			stats.ResearchedStored++
		}
	}

	candidates, err := w.Store.ListPosterCandidates(ctx, w.BatchSize)
	if err != nil {
		w.Logger.Warn("poster candidates failed", "err", err)
		stats.Failures++
		return stats
	}
	stats.Candidates = len(candidates)
	if len(candidates) == 0 {
		return stats
	}
	byScope := make(map[string]store.PosterOverride, len(overrides))
	for _, o := range overrides {
		byScope[posterOverrideScope(o.ChannelID, o.TitleNorm, o.SubTitleNorm, o.IsMovie)] = o
	}

	failedGeneration := map[string]bool{}
	generationBudget := w.MaxGenerationsPerPass
	for _, c := range candidates {
		if ctx.Err() != nil {
			break
		}
		titleNorm := normalizeForMatch(c.Title)
		subTitleNorm := normalizeForMatch(c.SubTitle)
		o, ok := byScope[posterOverrideScope(&c.ChannelID, titleNorm, subTitleNorm, c.IsMovie)]
		if !ok {
			o, ok = byScope[posterOverrideScope(nil, titleNorm, subTitleNorm, c.IsMovie)]
		}
		if !ok && subTitleNorm != "" {
			o, ok = byScope[posterOverrideScope(&c.ChannelID, titleNorm, "", c.IsMovie)]
		}
		if !ok && subTitleNorm != "" {
			o, ok = byScope[posterOverrideScope(nil, titleNorm, "", c.IsMovie)]
		}
		if ok {
			a, ready, err := w.ensureResearched(ctx, o.Asset, resolved)
			resolved[o.AssetKey] = a
			if err != nil {
				stats.Failures++
				continue
			}
			if ready && w.apply(ctx, c.ProgramID, c.SourceHash, a, true) {
				stats.OverridesApplied++
			} else if !ready {
				stats.Skipped++
			}
			continue
		}
		if c.GeneratedPosterURL != "" {
			if w.Assets.Exists(poster.RelativePathFromLocalURL(c.GeneratedPosterURL)) {
				stats.Skipped++
				continue
			}
			if err := w.Store.ClearGeneratedPosterForProgram(ctx, c.ProgramID, c.SourceHash); err != nil {
				stats.Failures++
				continue
			}
			c.GeneratedPosterURL = ""
		}
		if c.EnrichmentState == "manual" {
			stats.Skipped++
			continue
		}

		// Conventional TMDb/TVmaze/source artwork always gets first chance.
		if c.EnrichmentState == "pending" || !GeneratedPosterCandidate(c) || w.Generator == nil {
			stats.Skipped++
			continue
		}
		key, prompt, league := generatedPosterIdentity(c)
		if failedGeneration[key] {
			// A repeated airing or simulcast shares the same remote job. After a
			// transient failure, resume it on the next worker pass instead of
			// multiplying a long GPU hold or hammering a throttled result once
			// per programme row in this pass.
			stats.Skipped++
			continue
		}
		a, ready, generated, err := w.ensureGenerated(ctx, key, prompt, league, resolved, generationBudget > 0)
		resolved[key] = a
		if generated {
			generationBudget--
			stats.Generations++
		}
		if err != nil {
			failedGeneration[key] = true
			if a.RemoteJobID != "" {
				// A queued/running remote job already owns this pass's generation
				// lane; do not submit a second job after a transient poll timeout.
				generationBudget = 0
			}
			stats.Failures++
			continue
		}
		if ready && w.apply(ctx, c.ProgramID, c.SourceHash, a, false) {
			stats.GeneratedApplied++
		} else if !ready {
			stats.Skipped++
		}
	}
	return stats
}

func (w *PosterWorker) logPass(stats PosterWorkerStats) {
	if stats.ResearchedStored+stats.ResearchedFailed+stats.OverridesApplied+
		stats.GeneratedApplied+stats.Generations+stats.Failures == 0 {
		return
	}
	w.Logger.Info("poster pass complete",
		"candidates", stats.Candidates,
		"researched_stored", stats.ResearchedStored,
		"researched_failed", stats.ResearchedFailed,
		"overrides_applied", stats.OverridesApplied,
		"generated_applied", stats.GeneratedApplied,
		"generations", stats.Generations,
		"failures", stats.Failures)
}

func (w *PosterWorker) ensureResearched(ctx context.Context, a store.PosterAsset, resolved map[string]store.PosterAsset) (store.PosterAsset, bool, error) {
	if cached, ok := resolved[a.IdentityKey]; ok {
		return cached, cached.Status == "ready" && w.Assets.Exists(cached.LocalPath), nil
	}
	if a.Status == "ready" && w.Assets.Exists(a.LocalPath) {
		return a, true, nil
	}
	if a.Status == "failed" && time.Now().Before(a.RetryAt) {
		return a, false, nil
	}
	u, err := url.Parse(a.SourceURL)
	if err != nil {
		err = fmt.Errorf("researched poster URL must be public HTTPS")
		w.failAsset(ctx, a, err)
		return a, false, err
	}
	if err = validatePublicPosterURL(ctx, u); err != nil {
		w.failAsset(ctx, a, err)
		return a, false, err
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	req.Header.Set("User-Agent", "Conductor/PosterBackfill")
	resp, err := w.remoteClient().Do(req)
	if err != nil {
		w.failAsset(ctx, a, err)
		return a, false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("researched poster HTTP %d", resp.StatusCode)
		w.failAsset(ctx, a, err)
		return a, false, err
	}
	if ct := strings.ToLower(resp.Header.Get("Content-Type")); ct != "" && !strings.HasPrefix(ct, "image/") {
		err = fmt.Errorf("researched poster content-type %q", ct)
		w.failAsset(ctx, a, err)
		return a, false, err
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, (20<<20)+1))
	if err != nil || len(b) > 20<<20 {
		if err == nil {
			err = fmt.Errorf("researched poster exceeds 20 MiB")
		}
		w.failAsset(ctx, a, err)
		return a, false, err
	}
	stored, err := w.Assets.PutResearchedImage(b)
	if err != nil {
		w.failAsset(ctx, a, err)
		return a, false, err
	}
	if err := w.Store.MarkPosterAssetReady(ctx, a.IdentityKey, stored.RelativePath, stored.SHA256); err != nil {
		return a, false, err
	}
	a.Status, a.LocalPath, a.ContentSHA256, a.RemoteJobID = "ready", stored.RelativePath, stored.SHA256, ""
	return a, true, nil
}

func (w *PosterWorker) ensureGenerated(ctx context.Context, key, prompt, league string, resolved map[string]store.PosterAsset, mayGenerate bool) (store.PosterAsset, bool, bool, error) {
	a, ok := resolved[key]
	if !ok {
		var err error
		a, err = w.Store.EnsurePosterAsset(ctx, store.PosterAsset{
			IdentityKey: key, Kind: "generated", Prompt: prompt, Model: "schnell",
		})
		if err != nil {
			return a, false, false, err
		}
	}
	if a.Status == "ready" && w.Assets.Exists(a.LocalPath) {
		return a, true, false, nil
	}
	if a.Status == "failed" && time.Now().Before(a.RetryAt) {
		return a, false, false, nil
	}
	newSubmission := false
	if a.RemoteJobID == "" {
		if !mayGenerate {
			return a, false, false, nil
		}
		jobID, err := w.Generator.Submit(ctx, GenerateRequest{
			Prompt: prompt, NegativePrompt: "text, letters, numbers, words, logos, watermark, landscape, horizontal",
			Width: 1024, Height: 1536, Steps: 4, Model: "schnell", BatchSize: 1,
		})
		if err != nil {
			w.failAsset(ctx, a, fmt.Errorf("%s generation submit: %w", league, err))
			return a, false, true, err
		}
		if err := w.Store.MarkPosterAssetSubmitted(ctx, key, jobID); err != nil {
			return a, false, true, err
		}
		a.RemoteJobID = jobID
		newSubmission = true
	}
	b, err := w.Generator.Wait(ctx, a.RemoteJobID)
	if err != nil {
		if IsTerminalGenerationError(err) {
			_ = w.Store.ClearPosterAssetRemoteJob(ctx, key)
			a.RemoteJobID = ""
		}
		w.failAsset(ctx, a, fmt.Errorf("%s generation wait: %w", league, err))
		return a, false, newSubmission, err
	}
	stored, err := w.Assets.PutGeneratedPNG(b)
	if err != nil {
		w.failAsset(ctx, a, err)
		return a, false, newSubmission, err
	}
	if err := w.Store.MarkPosterAssetReady(ctx, key, stored.RelativePath, stored.SHA256); err != nil {
		return a, false, newSubmission, err
	}
	a.Status, a.LocalPath, a.ContentSHA256, a.RemoteJobID = "ready", stored.RelativePath, stored.SHA256, ""
	return a, true, newSubmission, nil
}

func (w *PosterWorker) apply(ctx context.Context, programID uuid.UUID, sourceHash string, a store.PosterAsset, manual bool) bool {
	localURL := poster.LocalAssetURL(a.LocalPath)
	if localURL == "" {
		return false
	}
	applied, err := w.Store.ApplyPosterToProgram(ctx, programID, sourceHash, localURL, manual)
	if err != nil {
		w.Logger.Warn("apply poster failed", "program", programID, "asset", a.IdentityKey, "err", err)
		return false
	}
	return applied
}

func (w *PosterWorker) failAsset(ctx context.Context, a store.PosterAsset, err error) {
	if err == nil {
		return
	}
	backoff := 30 * time.Second
	for i := 0; i < a.Attempts && backoff < 30*time.Minute; i++ {
		backoff *= 2
	}
	if backoff > 30*time.Minute {
		backoff = 30 * time.Minute
	}
	_ = w.Store.MarkPosterAssetFailed(ctx, a.IdentityKey, err.Error(), time.Now().Add(backoff))
	w.Logger.Warn("poster asset failed", "asset", a.IdentityKey, "kind", a.Kind, "retry_in", backoff, "err", err)
}

func (w *PosterWorker) remoteClient() *http.Client {
	if w.RemoteHTTP != nil {
		return w.RemoteHTTP
	}
	return newPublicPosterHTTPClient()
}

func validatePublicPosterURL(ctx context.Context, u *url.URL) error {
	if u == nil || u.Scheme != "https" || u.Host == "" || u.User != nil {
		return fmt.Errorf("researched poster URL must be public HTTPS")
	}
	_, err := resolvePublicPosterIPs(ctx, u.Hostname())
	return err
}

func resolvePublicPosterIPs(ctx context.Context, hostname string) ([]net.IP, error) {
	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, hostname)
	if err != nil {
		return nil, fmt.Errorf("resolve researched poster host: %w", err)
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("resolve researched poster host: no addresses")
	}
	ips := make([]net.IP, 0, len(addrs))
	for _, addr := range addrs {
		ip := addr.IP
		if !isPublicPosterIP(ip) {
			return nil, fmt.Errorf("researched poster host resolves to a non-public address")
		}
		ips = append(ips, ip)
	}
	return ips, nil
}

var nonPublicPosterPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.31.196.0/24"),
	netip.MustParsePrefix("192.52.193.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"),
	netip.MustParsePrefix("192.175.48.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("64:ff9b::/96"),
	netip.MustParsePrefix("64:ff9b:1::/48"),
	netip.MustParsePrefix("100::/64"),
	netip.MustParsePrefix("100:0:0:1::/64"),
	netip.MustParsePrefix("2001::/23"),
	netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("2002::/16"),
	netip.MustParsePrefix("3fff::/20"),
	netip.MustParsePrefix("5f00::/16"),
}

func isPublicPosterIP(ip net.IP) bool {
	addr, ok := netip.AddrFromSlice(ip)
	if !ok {
		return false
	}
	addr = addr.Unmap()
	if !addr.IsGlobalUnicast() || addr.IsPrivate() || addr.IsLoopback() ||
		addr.IsUnspecified() || addr.IsLinkLocalUnicast() || addr.IsMulticast() {
		return false
	}
	for _, prefix := range nonPublicPosterPrefixes {
		if prefix.Contains(addr) {
			return false
		}
	}
	return true
}

// newPublicPosterHTTPClient validates and then dials the exact resolved IP,
// closing the DNS-rebinding gap between the SSRF check and net/http's dial.
func newPublicPosterHTTPClient() *http.Client {
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, fmt.Errorf("parse poster dial address: %w", err)
		}
		ips, err := resolvePublicPosterIPs(ctx, host)
		if err != nil {
			return nil, err
		}
		var lastErr error
		for _, ip := range ips {
			conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
			if err == nil {
				return conn, nil
			}
			lastErr = err
		}
		return nil, fmt.Errorf("dial researched poster host: %w", lastErr)
	}
	return &http.Client{
		Transport: transport,
		Timeout:   45 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 3 {
				return fmt.Errorf("unsafe poster redirect")
			}
			return validatePublicPosterURL(req.Context(), req.URL)
		},
	}
}

func posterOverrideScope(channelID *uuid.UUID, titleNorm, subTitleNorm string, isMovie bool) string {
	channel := "*"
	if channelID != nil && *channelID != uuid.Nil {
		channel = channelID.String()
	}
	return fmt.Sprintf("%s|%t|%s|%s", channel, isMovie,
		strings.TrimSpace(titleNorm), strings.TrimSpace(subTitleNorm))
}

// GeneratedPosterCandidate is deliberately narrow: only known injected PPV /
// sports events or explicit matchup-style live listings. Sports talk, generic
// league shows, idle slots, and ads must never spend GPU time.
func GeneratedPosterCandidate(c store.PosterCandidate) bool {
	if !hasSportsCategory(c.Category) {
		return false
	}
	combined := strings.ToLower(strings.TrimSpace(c.Title + " " + c.SubTitle))
	for _, reject := range []string{
		"no event scheduled", "no game today", "next game", "off-season",
		"off season", "channel off air", "signing off", "paid programming",
		"to be determined", "to be announced", "to be confirmed",
		"unknown opponent",
	} {
		if strings.Contains(combined, reject) {
			return false
		}
	}
	for _, token := range strings.Fields(normalizeForMatch(combined)) {
		if token == "tbd" || token == "tba" || token == "tbc" {
			return false
		}
	}
	titleNorm := normalizeForMatch(c.Title)
	leagueNorm := normalizeForMatch(generatedPosterLeague(c))
	if titleNorm == "sports event" || titleNorm == "live sports event" ||
		(leagueNorm != "" && titleNorm == leagueNorm+" event") {
		return false
	}
	if strings.HasPrefix(c.SourceHash, "sports:") || strings.HasPrefix(c.SourceHash, "ppv-parse:") {
		return true
	}
	if !c.IsLive {
		return false
	}
	for _, marker := range []string{" @ ", " vs ", " vs. ", " versus ", " v "} {
		if strings.Contains(" "+combined+" ", marker) {
			return true
		}
	}
	return false
}

func hasSportsCategory(categories []string) bool {
	for _, c := range categories {
		c = strings.ToLower(strings.TrimSpace(c))
		if strings.HasPrefix(c, "sport") || c == "baseball" || c == "basketball" ||
			c == "football" || c == "hockey" || c == "soccer" || c == "mma" ||
			c == "mixed martial arts" || c == "boxing" {
			return true
		}
	}
	return false
}

func generatedPosterIdentity(c store.PosterCandidate) (identity, prompt, league string) {
	league = generatedPosterLeague(c)
	canonical := strings.Join([]string{
		"generated-sports-v1", "schnell", league,
		normalizeForMatch(c.Title), normalizeForMatch(c.SubTitle),
	}, "|")
	sum := sha256.Sum256([]byte(canonical))
	identity = "generated:sports:v1:" + hex.EncodeToString(sum[:])
	prompt = SportsMatchupPrompt(c.Title, c.SubTitle, league)
	return
}

func generatedPosterLeague(c store.PosterCandidate) string {
	if strings.HasPrefix(c.SourceHash, "sports:") {
		parts := strings.SplitN(c.SourceHash, ":", 3)
		if len(parts) >= 2 && parts[1] != "" {
			return strings.ToUpper(parts[1])
		}
	}
	for _, category := range c.Category {
		cat := strings.TrimSpace(category)
		lower := strings.ToLower(cat)
		if lower != "sports" && lower != "series" && !strings.HasPrefix(lower, "sport") && cat != "" {
			return cat
		}
	}
	return "live sports"
}

func ResearchedAssetKey(sourceURL string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(sourceURL)))
	return "researched:v1:" + hex.EncodeToString(sum[:])
}
