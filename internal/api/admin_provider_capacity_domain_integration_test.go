package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/auth"
	"github.com/spencercnorton/conductor/internal/store"
)

func patchProviderCapacityDomain(
	t *testing.T,
	handler http.Handler,
	adminKey string,
	providerID uuid.UUID,
	body string,
) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPatch,
		"/admin/providers/"+providerID.String(), bytes.NewBufferString(body))
	if adminKey != "" {
		req.Header.Set("Authorization", "Bearer "+adminKey)
	}
	req.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	return response
}

func TestIntegrationAdminProviderCapacityDomainIsAuthenticatedSafeAndConvergent(t *testing.T) {
	db := openAPICancelIntegrationDB(t)
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	domain := "api-" + strings.ReplaceAll(uuid.NewString(), "-", "")[:16]

	seedProvider := func(name string) store.Provider {
		t.Helper()
		provider, err := db.CreateProvider(ctx, store.Provider{
			Name: name + "-" + uuid.NewString(), Kind: "m3u_xtream",
			BaseURL: "http://provider.test", Enabled: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.CreateCredential(ctx, store.ProviderCredential{
			ProviderID: provider.ID, Username: name, PasswordEnc: []byte{1},
			MaxStreams: 1, Priority: 100, Enabled: true,
		}); err != nil {
			t.Fatal(err)
		}
		return provider
	}
	providerA := seedProvider("api-domain-a")
	providerB := seedProvider("api-domain-b")
	seedChannel := func(provider store.Provider, name string) store.Channel {
		t.Helper()
		var number float64
		if err := db.Pool.QueryRow(ctx,
			`SELECT (COALESCE(MAX(number), 9800) + 0.1)::float8 FROM channel`).Scan(&number); err != nil {
			t.Fatal(err)
		}
		channel, err := db.CreateChannel(ctx, store.Channel{
			Number: number, Name: name + "-" + uuid.NewString(), Enabled: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.CreateChannelSource(ctx, store.ChannelSource{
			ChannelID: channel.ID, ProviderID: provider.ID,
			UpstreamURL: "http://provider.test/" + channel.ID.String(),
			Priority:    0, HealthScore: 1, Enabled: true,
		}); err != nil {
			t.Fatal(err)
		}
		return channel
	}
	channelA := seedChannel(providerA, "api-domain-a")
	channelB := seedChannel(providerB, "api-domain-b")
	start := time.Now().Add(time.Hour).UTC()
	recordings := make([]store.DVRRecording, 0, 2)
	for i, channel := range []store.Channel{channelA, channelB} {
		recording, err := db.CreateDVRRecording(ctx, store.DVRRecording{
			ChannelID: channel.ID, Title: "api-domain-" + uuid.NewString(), Priority: 100,
			ScheduledStart: start, ScheduledEnd: start.Add(time.Hour),
			RequestedBy: "api-domain-test",
			OutputPath:  "/tmp/api-domain-" + uuid.NewString() + ".ts",
		}, store.DVRAdmissionPolicy{LiveReserve: 1, StartSlack: 30 * time.Second, MaxOverrun: 90 * time.Second})
		if err != nil {
			t.Fatalf("create recording %d: %v", i, err)
		}
		recordings = append(recordings, recording)
	}

	const adminKey = "op473-capacity-domain-admin-key"
	mux := http.NewServeMux()
	MountAdmin(mux, AdminDeps{
		Logger: logger, DB: db,
		Auth: auth.Config{Enabled: true, AdminAPIKey: adminKey},
	})
	body := func(value string) string {
		encoded, err := json.Marshal(map[string]string{"capacity_domain": value})
		if err != nil {
			t.Fatal(err)
		}
		return string(encoded)
	}

	unauthorized := patchProviderCapacityDomain(t, mux, "", providerA.ID, body(domain))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status=%d body=%s", unauthorized.Code, unauthorized.Body.String())
	}
	invalid := patchProviderCapacityDomain(t, mux, adminKey, providerA.ID,
		`{"capacity_domain":"Invalid Domain"}`)
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid slug status=%d body=%s", invalid.Code, invalid.Body.String())
	}
	unknown := patchProviderCapacityDomain(t, mux, adminKey, providerA.ID,
		`{"capacity_domain":"`+domain+`","credentials":true}`)
	if unknown.Code != http.StatusBadRequest {
		t.Fatalf("unknown field status=%d body=%s", unknown.Code, unknown.Body.String())
	}
	trailing := patchProviderCapacityDomain(t, mux, adminKey, providerA.ID,
		body(domain)+` {"capacity_domain":"other"}`)
	if trailing.Code != http.StatusBadRequest {
		t.Fatalf("trailing object status=%d body=%s", trailing.Code, trailing.Body.String())
	}
	omitted := patchProviderCapacityDomain(t, mux, adminKey, providerA.ID, `{}`)
	if omitted.Code != http.StatusBadRequest {
		t.Fatalf("omitted sparse field status=%d body=%s", omitted.Code, omitted.Body.String())
	}

	createdDomain := domain + "-created"
	createBody, err := json.Marshal(map[string]any{
		"name": "api-domain-created-" + uuid.NewString(), "kind": "m3u_xtream",
		"base_url": "http://provider.test", "capacity_domain": createdDomain,
	})
	if err != nil {
		t.Fatal(err)
	}
	createReq := httptest.NewRequest(http.MethodPost, "/admin/providers", bytes.NewReader(createBody))
	createReq.Header.Set("Authorization", "Bearer "+adminKey)
	createdResponse := httptest.NewRecorder()
	mux.ServeHTTP(createdResponse, createReq)
	if createdResponse.Code != http.StatusOK {
		t.Fatalf("create provider with domain status=%d body=%s",
			createdResponse.Code, createdResponse.Body.String())
	}
	var createdProvider store.Provider
	if err := json.NewDecoder(createdResponse.Body).Decode(&createdProvider); err != nil {
		t.Fatal(err)
	}
	if createdProvider.CapacityDomain != createdDomain {
		t.Fatalf("created provider domain=%q, want %q", createdProvider.CapacityDomain, createdDomain)
	}

	for _, provider := range []store.Provider{providerA, providerB} {
		response := patchProviderCapacityDomain(t, mux, adminKey, provider.ID, body(domain))
		if response.Code != http.StatusOK {
			t.Fatalf("group provider %s status=%d body=%s",
				provider.ID, response.Code, response.Body.String())
		}
		var updated store.Provider
		if err := json.NewDecoder(response.Body).Decode(&updated); err != nil {
			t.Fatal(err)
		}
		if updated.CapacityDomain != domain {
			t.Fatalf("updated provider domain=%q, want %q", updated.CapacityDomain, domain)
		}
	}
	listReq := httptest.NewRequest(http.MethodGet, "/admin/providers", nil)
	listReq.Header.Set("Authorization", "Bearer "+adminKey)
	listResponse := httptest.NewRecorder()
	mux.ServeHTTP(listResponse, listReq)
	if listResponse.Code != http.StatusOK {
		t.Fatalf("list providers status=%d body=%s", listResponse.Code, listResponse.Body.String())
	}
	var providers []store.Provider
	if err := json.NewDecoder(listResponse.Body).Decode(&providers); err != nil {
		t.Fatal(err)
	}
	listed := make(map[uuid.UUID]string, len(providers))
	for _, provider := range providers {
		listed[provider.ID] = provider.CapacityDomain
	}
	if listed[providerA.ID] != domain || listed[providerB.ID] != domain ||
		listed[createdProvider.ID] != createdDomain {
		t.Fatalf("list did not expose capacity domains: a=%q b=%q created=%q",
			listed[providerA.ID], listed[providerB.ID], listed[createdProvider.ID])
	}
	first, err := db.GetDVRRecording(ctx, recordings[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	second, err := db.GetDVRRecording(ctx, recordings[1].ID)
	if err != nil {
		t.Fatal(err)
	}
	if first.AdmissionState != "pending" || second.AdmissionState != "queued" {
		t.Fatalf("post-PATCH forecast first=%q second=%q, want pending/queued",
			first.AdmissionState, second.AdmissionState)
	}

	lease, err := db.AcquireLease(ctx, channelA.ID, store.PassthroughResolverForTest{})
	if err != nil {
		t.Fatal(err)
	}
	busy := patchProviderCapacityDomain(t, mux, adminKey, providerA.ID, body(""))
	if busy.Code != http.StatusConflict {
		t.Fatalf("busy move status=%d body=%s, want 409", busy.Code, busy.Body.String())
	}
	idempotent := patchProviderCapacityDomain(t, mux, adminKey, providerA.ID, body(domain))
	if idempotent.Code != http.StatusOK {
		t.Fatalf("busy idempotent replay status=%d body=%s, want 200",
			idempotent.Code, idempotent.Body.String())
	}
	if _, err := db.Pool.Exec(ctx, `DELETE FROM active_stream WHERE id=$1`, lease.ActiveStreamID); err != nil {
		t.Fatal(err)
	}
	ungrouped := patchProviderCapacityDomain(t, mux, adminKey, providerA.ID, body(""))
	if ungrouped.Code != http.StatusOK {
		t.Fatalf("idle ungroup status=%d body=%s", ungrouped.Code, ungrouped.Body.String())
	}
	second, err = db.GetDVRRecording(ctx, recordings[1].ID)
	if err != nil {
		t.Fatal(err)
	}
	if second.AdmissionState != "pending" {
		t.Fatalf("forecast did not converge after ungroup: %+v", second)
	}
}
