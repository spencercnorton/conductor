package enrich

import (
	"bytes"
	"context"
	"errors"
	"image"
	"image/png"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/poster"
	"github.com/spencercnorton/conductor/internal/store"
)

func TestGeneratedPosterCandidateNarrowSportsOnly(t *testing.T) {
	base := store.PosterCandidate{
		Category: []string{"Sports", "MLB"}, IsLive: true,
		Title: "New York Mets @ Philadelphia Phillies",
	}
	if !GeneratedPosterCandidate(base) {
		t.Fatal("explicit live matchup should generate")
	}
	base.Title = "SportsCenter"
	if GeneratedPosterCandidate(base) {
		t.Fatal("generic sports show must not generate")
	}
	base.Title = "NHL — No Event Scheduled"
	base.SourceHash = "sports:nhl:idle"
	if GeneratedPosterCandidate(base) {
		t.Fatal("idle sports slot must not generate")
	}
	base.Title = "UFC 400 Main Card"
	base.SourceHash = "ppv-parse:provider:400"
	if !GeneratedPosterCandidate(base) {
		t.Fatal("known PPV parser event should generate")
	}
	base.Title = "MLB Event"
	base.SourceHash = "sports:mlb:placeholder"
	if GeneratedPosterCandidate(base) {
		t.Fatal("generic league event must not generate")
	}
	base.Title = "TBD @ New York Mets"
	if GeneratedPosterCandidate(base) {
		t.Fatal("TBD matchup must not generate")
	}
	for _, title := range []string{"TBA @ New York Mets", "New York Mets @ TBC", "Opponent to be announced @ New York Mets"} {
		base.Title = title
		if GeneratedPosterCandidate(base) {
			t.Fatalf("placeholder matchup must not generate: %q", title)
		}
	}
	base.Title = "Basketball: New York Mets @ Philadelphia Phillies"
	if !GeneratedPosterCandidate(base) {
		t.Fatal("TBA token check must not reject the word basketball")
	}
	base.Category = []string{"News"}
	if GeneratedPosterCandidate(base) {
		t.Fatal("non-sports programme must not generate")
	}
}

type fakePosterStore struct {
	mu         sync.Mutex
	candidates []store.PosterCandidate
	overrides  []store.PosterOverride
	assets     map[string]store.PosterAsset
	applied    []uuid.UUID
}

func (f *fakePosterStore) ListPosterCandidates(context.Context, int) ([]store.PosterCandidate, error) {
	return f.candidates, nil
}
func (f *fakePosterStore) EnsurePosterAsset(_ context.Context, a store.PosterAsset) (store.PosterAsset, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if got, ok := f.assets[a.IdentityKey]; ok {
		return got, nil
	}
	a.Status, a.RetryAt = "pending", time.Now()
	f.assets[a.IdentityKey] = a
	return a, nil
}
func (f *fakePosterStore) GetPosterAsset(_ context.Context, key string) (store.PosterAsset, error) {
	return f.assets[key], nil
}
func (f *fakePosterStore) MarkPosterAssetReady(_ context.Context, key, path, sha string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	a := f.assets[key]
	a.Status, a.LocalPath, a.ContentSHA256, a.RemoteJobID = "ready", path, sha, ""
	f.assets[key] = a
	return nil
}

func (f *fakePosterStore) MarkPosterAssetFailed(_ context.Context, key, _ string, _ time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	a := f.assets[key]
	a.Status, a.RetryAt, a.Attempts = "failed", time.Now().Add(-time.Second), a.Attempts+1
	f.assets[key] = a
	return nil
}
func (f *fakePosterStore) MarkPosterAssetSubmitted(_ context.Context, key, jobID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	a := f.assets[key]
	a.Status, a.RemoteJobID = "pending", jobID
	f.assets[key] = a
	return nil
}
func (f *fakePosterStore) ClearPosterAssetRemoteJob(_ context.Context, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	a := f.assets[key]
	a.RemoteJobID = ""
	f.assets[key] = a
	return nil
}
func (f *fakePosterStore) ListPosterOverrides(context.Context) ([]store.PosterOverride, error) {
	return f.overrides, nil
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func researchedPosterClient(t *testing.T) *http.Client {
	t.Helper()
	var b bytes.Buffer
	if err := png.Encode(&b, image.NewRGBA(image.Rect(0, 0, 200, 300))); err != nil {
		t.Fatal(err)
	}
	body := b.Bytes()
	return &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"image/png"}},
			Body:       io.NopCloser(bytes.NewReader(body)),
			Request:    req,
		}, nil
	})}
}

func TestPosterWorkerEagerlyStoresOverrideWithoutCandidates(t *testing.T) {
	asset := store.PosterAsset{
		IdentityKey: "researched:eager", Kind: "researched", Status: "pending",
		SourceURL: "https://8.8.8.8/poster.png",
	}
	f := &fakePosterStore{
		overrides: []store.PosterOverride{{
			TitleNorm: "already covered show", AssetKey: asset.IdentityKey, Asset: asset,
		}},
		assets: map[string]store.PosterAsset{asset.IdentityKey: asset},
	}
	assets := poster.NewAssetStore(t.TempDir())
	var logs bytes.Buffer
	w := NewPosterWorker(slog.New(slog.NewTextHandler(&logs, nil)), f, assets, nil)
	w.RemoteHTTP = researchedPosterClient(t)

	stats := w.RunOnce(context.Background())
	stored := f.assets[asset.IdentityKey]
	if stats.Candidates != 0 || stats.ResearchedStored != 1 || stats.ResearchedFailed != 0 || stats.Failures != 0 {
		t.Fatalf("override was not eagerly materialized without candidates: %+v", stats)
	}
	if stored.Status != "ready" || stored.LocalPath == "" || !assets.Exists(stored.LocalPath) {
		t.Fatalf("researched asset was not stored locally: %+v", stored)
	}
	if len(f.applied) != 0 {
		t.Fatalf("materialization must not apply over higher-precedence art: %v", f.applied)
	}
	if got := logs.String(); !strings.Contains(got, "candidates=0") || !strings.Contains(got, "researched_stored=1") {
		t.Fatalf("eager materialization was not logged: %s", got)
	}
}

func TestPosterWorkerEagerResearchContinuesAfterFailure(t *testing.T) {
	bad := store.PosterAsset{
		IdentityKey: "researched:bad", Kind: "researched", Status: "pending",
		SourceURL: "https://127.0.0.1/private.png",
	}
	good := store.PosterAsset{
		IdentityKey: "researched:good", Kind: "researched", Status: "pending",
		SourceURL: "https://8.8.8.8/poster.png",
	}
	f := &fakePosterStore{
		overrides: []store.PosterOverride{
			{TitleNorm: "bad", AssetKey: bad.IdentityKey, Asset: bad},
			{TitleNorm: "good", AssetKey: good.IdentityKey, Asset: good},
		},
		assets: map[string]store.PosterAsset{bad.IdentityKey: bad, good.IdentityKey: good},
	}
	w := NewPosterWorker(slog.New(slog.NewTextHandler(io.Discard, nil)), f,
		poster.NewAssetStore(t.TempDir()), nil)
	w.RemoteHTTP = researchedPosterClient(t)

	stats := w.RunOnce(context.Background())
	if stats.ResearchedStored != 1 || stats.ResearchedFailed != 1 || stats.Failures != 1 {
		t.Fatalf("individual research failure stopped the eager pass: %+v", stats)
	}
	if f.assets[bad.IdentityKey].Status != "failed" {
		t.Fatalf("unsafe asset was not failed: %+v", f.assets[bad.IdentityKey])
	}
	if f.assets[good.IdentityKey].Status != "ready" {
		t.Fatalf("later valid asset was not stored: %+v", f.assets[good.IdentityKey])
	}
}

func TestPosterWorkerOverrideMatchesSubtitle(t *testing.T) {
	channelID := uuid.New()
	wantedID, otherID := uuid.New(), uuid.New()
	assets := poster.NewAssetStore(t.TempDir())
	stored, err := assets.PutResearchedImage(func() []byte {
		var b bytes.Buffer
		if err := png.Encode(&b, image.NewRGBA(image.Rect(0, 0, 200, 300))); err != nil {
			t.Fatal(err)
		}
		return b.Bytes()
	}())
	if err != nil {
		t.Fatal(err)
	}
	asset := store.PosterAsset{
		IdentityKey: "researched:test", Kind: "researched", Status: "ready",
		LocalPath: stored.RelativePath,
	}
	f := &fakePosterStore{
		candidates: []store.PosterCandidate{
			{ProgramID: wantedID, ChannelID: channelID, Title: "Miraculous", SubTitle: "Tales of Ladybug and Cat Noir"},
			{ProgramID: otherID, ChannelID: channelID, Title: "Miraculous", SubTitle: "Tales of Ladybug and Cat Noir Chibi"},
		},
		overrides: []store.PosterOverride{{
			ChannelID: &channelID, TitleNorm: normalizeForMatch("Miraculous"),
			SubTitleNorm: normalizeForMatch("Tales of Ladybug and Cat Noir"),
			AssetKey:     asset.IdentityKey, Asset: asset,
		}},
		assets: map[string]store.PosterAsset{asset.IdentityKey: asset},
	}
	w := NewPosterWorker(slog.New(slog.NewTextHandler(io.Discard, nil)), f, assets, nil)
	stats := w.RunOnce(context.Background())
	if stats.OverridesApplied != 1 || len(f.applied) != 1 || f.applied[0] != wantedID {
		t.Fatalf("subtitle-scoped override leaked: stats=%+v applied=%v", stats, f.applied)
	}
}

func TestValidatePublicPosterURLRejectsPrivateHost(t *testing.T) {
	for _, raw := range []string{
		"https://127.0.0.1/poster.jpg",
		"https://100.100.100.100/poster.jpg", // Tailscale/CGNAT range
	} {
		u, err := url.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		if err := validatePublicPosterURL(context.Background(), u); err == nil {
			t.Fatalf("non-public poster host should be rejected: %s", raw)
		}
	}
}

func TestPublicPosterIPRejectsSpecialUseRanges(t *testing.T) {
	for _, raw := range []string{
		"0.1.2.3", "198.18.0.1", "192.0.2.1", "203.0.113.1", "240.0.0.1",
		"64:ff9b::1", "100:0:0:1::1", "2001:db8::1", "3fff::1", "5f00::1",
	} {
		if isPublicPosterIP(net.ParseIP(raw)) {
			t.Fatalf("special-use address accepted: %s", raw)
		}
	}
	for _, raw := range []string{"8.8.8.8", "2606:4700:4700::1111"} {
		if !isPublicPosterIP(net.ParseIP(raw)) {
			t.Fatalf("public address rejected: %s", raw)
		}
	}
}
func (f *fakePosterStore) ApplyPosterToProgram(_ context.Context, id uuid.UUID, _, _ string, _ bool) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applied = append(f.applied, id)
	return true, nil
}
func (f *fakePosterStore) ClearGeneratedPosterForProgram(context.Context, uuid.UUID, string) error {
	return nil
}

type fakeGenerator struct {
	mu         sync.Mutex
	submits    int
	waits      int
	body       []byte
	waitErrors []error
}

func (g *fakeGenerator) Submit(context.Context, GenerateRequest) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.submits++
	return "job-1", nil
}

func (g *fakeGenerator) Wait(context.Context, string) ([]byte, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.waits++
	if len(g.waitErrors) > 0 {
		err := g.waitErrors[0]
		g.waitErrors = g.waitErrors[1:]
		return nil, err
	}
	return g.body, nil
}

func TestPosterWorkerGeneratesOnceForSimulcasts(t *testing.T) {
	ch1, ch2 := uuid.New(), uuid.New()
	p1, p2 := uuid.New(), uuid.New()
	candidate := store.PosterCandidate{
		ProgramID: p1, ChannelID: ch1,
		Title:    "New York Mets @ Philadelphia Phillies",
		Category: []string{"Sports", "MLB"}, IsLive: true,
		SourceHash: "sports:mlb:espn-1", EnrichmentState: "failed",
	}
	candidate2 := candidate
	candidate2.ProgramID, candidate2.ChannelID = p2, ch2
	f := &fakePosterStore{
		candidates: []store.PosterCandidate{candidate, candidate2},
		assets:     map[string]store.PosterAsset{},
	}
	var b bytes.Buffer
	if err := png.Encode(&b, image.NewRGBA(image.Rect(0, 0, 200, 300))); err != nil {
		t.Fatal(err)
	}
	g := &fakeGenerator{body: b.Bytes()}
	w := NewPosterWorker(
		slog.New(slog.NewTextHandler(io.Discard, nil)), f,
		poster.NewAssetStore(t.TempDir()), g,
	)
	stats := w.RunOnce(context.Background())
	if g.submits != 1 || g.waits != 1 || stats.Generations != 1 || stats.GeneratedApplied != 2 || len(f.applied) != 2 {
		t.Fatalf("expected one generation reused twice; submits=%d waits=%d stats=%+v applied=%d", g.submits, g.waits, stats, len(f.applied))
	}
}

func TestPosterWorkerResumesPersistedRemoteJobAfterWaitError(t *testing.T) {
	p := uuid.New()
	candidate := store.PosterCandidate{
		ProgramID: p, ChannelID: uuid.New(), Title: "Mets @ Phillies",
		Category: []string{"Sports", "MLB"}, IsLive: true,
		SourceHash: "sports:mlb:resume", EnrichmentState: "failed",
	}
	f := &fakePosterStore{
		candidates: []store.PosterCandidate{candidate},
		assets:     map[string]store.PosterAsset{},
	}
	var b bytes.Buffer
	if err := png.Encode(&b, image.NewRGBA(image.Rect(0, 0, 200, 300))); err != nil {
		t.Fatal(err)
	}
	g := &fakeGenerator{body: b.Bytes(), waitErrors: []error{errors.New("temporary poll failure")}}
	w := NewPosterWorker(slog.New(slog.NewTextHandler(io.Discard, nil)), f,
		poster.NewAssetStore(t.TempDir()), g)
	first := w.RunOnce(context.Background())
	second := w.RunOnce(context.Background())
	if g.submits != 1 || g.waits != 2 || first.Generations != 1 || second.Generations != 0 || second.GeneratedApplied != 1 {
		t.Fatalf("remote job was not resumed: submits=%d waits=%d first=%+v second=%+v", g.submits, g.waits, first, second)
	}
}

func TestPosterWorkerPollsSharedRemoteJobOncePerPassAfterError(t *testing.T) {
	candidate := store.PosterCandidate{
		ProgramID: uuid.New(), ChannelID: uuid.New(), Title: "Mets @ Phillies",
		Category: []string{"Sports", "MLB"}, IsLive: true,
		SourceHash: "sports:mlb:shared-1", EnrichmentState: "failed",
	}
	repeat := candidate
	repeat.ProgramID, repeat.ChannelID, repeat.SourceHash = uuid.New(), uuid.New(), "sports:mlb:shared-2"
	f := &fakePosterStore{
		candidates: []store.PosterCandidate{candidate, repeat},
		assets:     map[string]store.PosterAsset{},
	}
	g := &fakeGenerator{waitErrors: []error{errors.New("temporary poll failure")}}
	w := NewPosterWorker(slog.New(slog.NewTextHandler(io.Discard, nil)), f,
		poster.NewAssetStore(t.TempDir()), g)
	stats := w.RunOnce(context.Background())
	if g.submits != 1 || g.waits != 1 || stats.Failures != 1 || stats.Skipped != 1 {
		t.Fatalf("shared failed job was retried in one pass: submits=%d waits=%d stats=%+v", g.submits, g.waits, stats)
	}
}
