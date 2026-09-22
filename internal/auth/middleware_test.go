package auth_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/spencercnorton/conductor/internal/auth"
)

// mockPortal stands in for telegram-auth-portal's GET /api/auth/me.
func mockPortal(t *testing.T, status int, body string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/auth/me" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(status)
		_, _ = fmt.Fprint(w, body)
	}))
}

func handlerEcho() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		uc := auth.FromContext(r.Context())
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(uc.Source))
	})
}

// doCookie drives one request carrying a ge_sso cookie through the middleware.
func doCookie(t *testing.T, cfg auth.Config, cookieVal string) *httptest.ResponseRecorder {
	t.Helper()
	srv := auth.Middleware(cfg)(handlerEcho())
	req := httptest.NewRequest(http.MethodGet, "/admin/x", nil)
	req.AddCookie(&http.Cookie{Name: "ge_sso", Value: cookieVal})
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	return rr
}

func TestAuthDisabledPassesThrough(t *testing.T) {
	cfg := auth.Config{Enabled: false}
	srv := auth.Middleware(cfg)(handlerEcho())
	req := httptest.NewRequest(http.MethodGet, "/admin/whatever", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("disabled middleware should pass through; got %d", rr.Code)
	}
}

func TestAdminAPIKeyAccepted(t *testing.T) {
	cfg := auth.Config{Enabled: true, AdminAPIKey: "secret-key"}
	srv := auth.Middleware(cfg)(handlerEcho())
	req := httptest.NewRequest(http.MethodGet, "/admin/x", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("valid api key: got %d want 200", rr.Code)
	}
	if rr.Body.String() != "api_key" {
		t.Errorf("source: got %q want api_key", rr.Body.String())
	}
}

func TestAdminAPIKeyRejected(t *testing.T) {
	cfg := auth.Config{Enabled: true, AdminAPIKey: "secret-key"}
	srv := auth.Middleware(cfg)(handlerEcho())
	req := httptest.NewRequest(http.MethodGet, "/admin/x", nil)
	req.Header.Set("Authorization", "Bearer wrong-key")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("bad api key: got %d want 401", rr.Code)
	}
	if rr.Header().Get("WWW-Authenticate") == "" {
		t.Errorf("expected WWW-Authenticate header on 401")
	}
}

func TestCookieAcceptedViaPortal(t *testing.T) {
	portal := mockPortal(t, http.StatusOK, `{"principal_id": 4242, "user": {"id": 123456789}, "priv": "OWNER"}`)
	defer portal.Close()
	cfg := auth.Config{Enabled: true, AuthPortalURL: portal.URL, CookieName: "ge_sso"}

	rr := doCookie(t, cfg, "valid-session-aaa")
	if rr.Code != http.StatusOK {
		t.Errorf("valid cookie: got %d want 200", rr.Code)
	}
	if rr.Body.String() != "jwt" {
		t.Errorf("source: got %q want jwt", rr.Body.String())
	}
}

func TestCookieRejectedByPortal(t *testing.T) {
	// Expired / forged / unknown cookies are rejected by the portal (the
	// single validator); Conductor just surfaces the 401.
	portal := mockPortal(t, http.StatusUnauthorized, `{"detail": "no session"}`)
	defer portal.Close()
	cfg := auth.Config{Enabled: true, AuthPortalURL: portal.URL, CookieName: "ge_sso"}

	rr := doCookie(t, cfg, "rejected-session-bbb")
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("rejected cookie: got %d want 401", rr.Code)
	}
}

func TestPortalUnreachableFailsClosed(t *testing.T) {
	// Point at a server we immediately close so the request errors.
	portal := mockPortal(t, http.StatusOK, `{"principal_id": 1}`)
	url := portal.URL
	portal.Close()
	cfg := auth.Config{Enabled: true, AuthPortalURL: url, CookieName: "ge_sso"}

	rr := doCookie(t, cfg, "unreachable-session-ccc")
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("portal unreachable must fail closed: got %d want 401", rr.Code)
	}
}

func TestPrincipalAllowlist(t *testing.T) {
	allowed := mockPortal(t, http.StatusOK, `{"principal_id": 4242}`)
	defer allowed.Close()
	cfgAllowed := auth.Config{
		Enabled: true, AuthPortalURL: allowed.URL, CookieName: "ge_sso",
		AllowedPrincipals: []int64{4242},
	}
	if rr := doCookie(t, cfgAllowed, "allow-session-ddd"); rr.Code != http.StatusOK {
		t.Errorf("allowed principal: got %d want 200", rr.Code)
	}

	stranger := mockPortal(t, http.StatusOK, `{"principal_id": 99999}`)
	defer stranger.Close()
	cfgDenied := auth.Config{
		Enabled: true, AuthPortalURL: stranger.URL, CookieName: "ge_sso",
		AllowedPrincipals: []int64{4242},
	}
	if rr := doCookie(t, cfgDenied, "deny-session-eee"); rr.Code != http.StatusUnauthorized {
		t.Errorf("disallowed principal: got %d want 401", rr.Code)
	}
}

func TestNoCredentialsRejected(t *testing.T) {
	cfg := auth.Config{Enabled: true, AdminAPIKey: "secret-key", AuthPortalURL: "https://portal.invalid"}
	srv := auth.Middleware(cfg)(handlerEcho())
	req := httptest.NewRequest(http.MethodGet, "/admin/x", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("no creds: got %d want 401", rr.Code)
	}
}

func TestLoadFromEnv(t *testing.T) {
	env := map[string]string{
		"CONDUCTOR_AUTH_PORTAL_URL": "https://auth.example.com",
		"CONDUCTOR_ADMIN_API_KEY":   "key",
		"CONDUCTOR_AUTH_PRINCIPALS": "1,2, 123456789  ,bogus",
		"CONDUCTOR_SSO_COOKIE_NAME": "ge_sso",
	}
	cfg := auth.LoadFromEnv(func(k string) string { return env[k] })
	if !cfg.Enabled {
		t.Error("expected Enabled=true by default")
	}
	if cfg.AuthPortalURL != "https://auth.example.com" {
		t.Errorf("portal url: %q", cfg.AuthPortalURL)
	}
	if cfg.AdminAPIKey != "key" {
		t.Errorf("api key: %q", cfg.AdminAPIKey)
	}
	if len(cfg.AllowedPrincipals) != 3 {
		t.Errorf("allowlist parse: %v", cfg.AllowedPrincipals)
	}
}

func TestAuthDisabledViaEnv(t *testing.T) {
	env := map[string]string{
		"CONDUCTOR_ADMIN_API_KEY": "key",
		"CONDUCTOR_AUTH_ENABLED":  "false",
	}
	cfg := auth.LoadFromEnv(func(k string) string { return env[k] })
	if cfg.Enabled {
		t.Error("CONDUCTOR_AUTH_ENABLED=false should disable auth")
	}
}
