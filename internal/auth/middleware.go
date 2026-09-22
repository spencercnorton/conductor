// Package auth integrates Conductor's admin surface with our home-lab SSO.
//
// Two accepted credentials:
//
//  1. ge_sso cookie — the session cookie minted by the SSO portal
//     when a user signs in. Conductor no longer decodes the cookie itself
//     (no shared HMAC secret); it VALIDATES the cookie by calling the portal's
//     /api/auth/me (telegram-auth-portal is the single token validator). A
//     forged cookie is rejected by the portal, so delegating is not spoofable.
//     (Auth standardization: the bespoke HS256 verify + the shared
//     CONDUCTOR_SSO_HMAC_SECRET were removed here — the portal validates.)
//
//  2. Authorization: Bearer <ADMIN_API_KEY> — static long-lived key for
//     CLI / service callers (the same shape gif-repo uses).
//
// If CONDUCTOR_AUTH_ENABLED=false the middleware no-ops — useful for
// fresh-install bring-up before things are wired.
//
// The middleware writes a UserContext into the request context so handlers
// can query who's calling. Use auth.FromContext(r.Context()) to read it.
package auth

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// UserContext is what handlers see after passing through Verify.
type UserContext struct {
	// Source tells you which credential was accepted: "jwt" or "api_key" or "none".
	Source string

	// PrincipalID is the auth-portal principal_id when Source==jwt.
	// Zero when Source==api_key (no per-user identity for shared keys).
	PrincipalID int64

	// Providers is the list of providers the user signed in via. Populated
	// only when known; empty via the delegated path (/api/auth/me does not
	// return it and nothing in Conductor consumes it).
	Providers []string

	// Subject is the JWT sub claim if present. Empty via the delegated path.
	Subject string

	// Expires is the JWT exp claim. Zero for api_key and the delegated path.
	Expires time.Time
}

type ctxKey struct{}

// FromContext retrieves the UserContext attached by Verify. Returns the
// zero value if no auth ran (i.e., middleware disabled).
func FromContext(ctx context.Context) UserContext {
	if v, ok := ctx.Value(ctxKey{}).(UserContext); ok {
		return v
	}
	return UserContext{}
}

// Config controls Verify.
type Config struct {
	// Enabled gates the entire middleware. When false, all requests pass
	// through with an empty UserContext.
	Enabled bool

	// AuthPortalURL is the base URL of telegram-auth-portal. The ge_sso cookie
	// is validated by GET {AuthPortalURL}/api/auth/me — no shared secret.
	AuthPortalURL string

	// CookieName the session cookie comes in. "ge_sso" by default.
	CookieName string

	// AdminAPIKey is the static Bearer fallback. Compared with constant-time.
	AdminAPIKey string

	// AllowedPrincipals is the allowlist of principal_ids permitted to call
	// /admin. Empty = allow any signed-in user (appropriate for a
	// single-tenant home-lab).
	AllowedPrincipals []int64
}

// LoadFromEnv builds a Config from CONDUCTOR_* env vars. Safe to call from
// main; never panics.
func LoadFromEnv(getenv func(string) string) Config {
	cfg := Config{
		AuthPortalURL: getenvOr(getenv, "CONDUCTOR_AUTH_PORTAL_URL", ""),
		CookieName:    getenvOr(getenv, "CONDUCTOR_SSO_COOKIE_NAME", "ge_sso"),
		AdminAPIKey:   getenv("CONDUCTOR_ADMIN_API_KEY"),
	}
	for _, p := range strings.Split(getenv("CONDUCTOR_AUTH_PRINCIPALS"), ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if id, err := strconv.ParseInt(p, 10, 64); err == nil {
			cfg.AllowedPrincipals = append(cfg.AllowedPrincipals, id)
		}
	}
	// Auth is on by default (fail-closed): the ge_sso path validates via the
	// portal and the Bearer path via the admin key — neither needs a local
	// secret. Force-disable for fresh-install bring-up with
	// CONDUCTOR_AUTH_ENABLED=false.
	cfg.Enabled = true
	if v := strings.ToLower(strings.TrimSpace(getenv("CONDUCTOR_AUTH_ENABLED"))); v != "" {
		cfg.Enabled = v == "true" || v == "1" || v == "yes"
	}
	return cfg
}

func getenvOr(get func(string) string, k, def string) string {
	if v := get(k); v != "" {
		return v
	}
	return def
}

// Middleware returns an http.Handler middleware that verifies credentials
// and writes a UserContext into the request context. Unauthenticated
// requests get a 401 unless Config.Enabled is false (then pass-through).
func Middleware(cfg Config) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		if !cfg.Enabled {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			uc, err := authenticate(cfg, r)
			if err != nil {
				writeAuthErr(w, http.StatusUnauthorized, err.Error())
				return
			}
			r = r.WithContext(context.WithValue(r.Context(), ctxKey{}, uc))
			next.ServeHTTP(w, r)
		})
	}
}

func authenticate(cfg Config, r *http.Request) (UserContext, error) {
	// 1. Authorization: Bearer <ADMIN_API_KEY>
	if cfg.AdminAPIKey != "" {
		ah := r.Header.Get("Authorization")
		if strings.HasPrefix(ah, "Bearer ") {
			presented := strings.TrimPrefix(ah, "Bearer ")
			if subtle.ConstantTimeCompare([]byte(presented), []byte(cfg.AdminAPIKey)) == 1 {
				return UserContext{Source: "api_key"}, nil
			}
			// Don't reveal whether the key is the wrong shape vs. wrong value.
			return UserContext{}, errors.New("invalid bearer token")
		}
	}

	// 2. ge_sso cookie — validated by the portal (no local secret).
	if cfg.AuthPortalURL != "" {
		c, err := r.Cookie(cfg.CookieName)
		if err == nil && c.Value != "" {
			return verifyViaPortal(cfg, c.Value)
		}
	}

	return UserContext{}, errors.New("missing credentials")
}

// --- portal-delegated cookie validation -------------------------------------

// portalHTTPClient is the client used for the /api/auth/me call. A short
// timeout keeps a wedged portal from hanging admin requests.
var portalHTTPClient = &http.Client{Timeout: 8 * time.Second}

const portalCacheTTL = 30 * time.Second

type portalCacheEntry struct {
	uc      UserContext
	valid   bool
	expires time.Time
}

var (
	portalCacheMu sync.Mutex
	portalCache   = map[string]portalCacheEntry{}
)

// portalMe mirrors the fields we read from telegram-auth-portal's
// GET /api/auth/me response: {"user": {"id": ...}, "principal_id": ...}.
type portalMe struct {
	PrincipalID *int64 `json:"principal_id"`
	User        struct {
		ID *int64 `json:"id"`
	} `json:"user"`
}

// verifyViaPortal validates the cookie by calling the portal's /api/auth/me
// and applies the principal allowlist. Verdicts are cached briefly per-cookie
// so admin bursts don't round-trip every request.
func verifyViaPortal(cfg Config, cookie string) (UserContext, error) {
	now := time.Now()

	portalCacheMu.Lock()
	if e, ok := portalCache[cookie]; ok && e.expires.After(now) {
		portalCacheMu.Unlock()
		if !e.valid {
			return UserContext{}, errors.New("invalid session")
		}
		return e.uc, nil
	}
	portalCacheMu.Unlock()

	req, err := http.NewRequest(http.MethodGet, cfg.AuthPortalURL+"/api/auth/me", nil)
	if err != nil {
		return UserContext{}, err
	}
	req.Header.Set("Cookie", cfg.CookieName+"="+cookie)

	resp, err := portalHTTPClient.Do(req)
	if err != nil {
		// Transient portal/network error — fail closed, do not cache.
		return UserContext{}, fmt.Errorf("portal auth unreachable: %w", err)
	}
	defer resp.Body.Close()

	uc := UserContext{}
	valid := false
	if resp.StatusCode == http.StatusOK {
		var me portalMe
		if err := json.NewDecoder(resp.Body).Decode(&me); err == nil {
			var pid int64
			switch {
			case me.PrincipalID != nil:
				pid = *me.PrincipalID
			case me.User.ID != nil:
				pid = *me.User.ID
			}
			uc = UserContext{Source: "jwt", PrincipalID: pid}
			valid = principalAllowed(cfg, pid)
			if !valid {
				uc = UserContext{}
			}
		}
	}

	// Cache the verdict (valid or not) to throttle the portal; a fresh login
	// issues a new cookie value, so a real sign-in is never blocked by a
	// cached negative.
	portalCacheMu.Lock()
	portalCache[cookie] = portalCacheEntry{uc: uc, valid: valid, expires: now.Add(portalCacheTTL)}
	if len(portalCache) > 512 {
		for k, e := range portalCache {
			if e.expires.Before(now) {
				delete(portalCache, k)
			}
		}
	}
	portalCacheMu.Unlock()

	if !valid {
		return UserContext{}, errors.New("invalid session")
	}
	return uc, nil
}

func principalAllowed(cfg Config, pid int64) bool {
	if len(cfg.AllowedPrincipals) == 0 {
		return true
	}
	for _, id := range cfg.AllowedPrincipals {
		if id == pid {
			return true
		}
	}
	return false
}

func writeAuthErr(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("WWW-Authenticate", `Bearer realm="conductor-admin", error="invalid_token"`)
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
