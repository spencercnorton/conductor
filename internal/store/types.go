package store

import (
	"time"

	"github.com/google/uuid"
)

// Provider is one upstream IPTV provider (Xtream, plain M3U, Stalker).
type Provider struct {
	ID             uuid.UUID
	Name           string
	Kind           string // "m3u_xtream" | "m3u_plain" | "stalker"
	BaseURL        string
	CapacityDomain string // empty = isolated quota boundary for this provider
	Notes          string
	Enabled        bool
	CreatedAt      time.Time
}

// ProviderCredential is one (username, password, slot-budget) tuple under
// a Provider. Phase 1's structural fix: credentials are first-class
// entities, leased at request time per spec §3.1.
//
// PasswordEnc is AES-GCM ciphertext over the cleartext password using the
// key from CONDUCTOR_CRED_KEY. The cleartext is only ever decrypted at
// request time, in memory, when substituting into the upstream URL template.
type ProviderCredential struct {
	ID          uuid.UUID
	ProviderID  uuid.UUID
	Username    string
	PasswordEnc []byte
	MaxStreams  int
	Priority    int
	Notes       string
	Enabled     bool
	CreatedAt   time.Time
}

// Channel is the consumer-facing concept (a "TV channel"), decoupled from
// the upstream sources that can serve it.
type Channel struct {
	ID           uuid.UUID
	Number       float64 // numeric(6,1) → "100" or "100.1"
	Name         string
	CallSign     string
	LogoURL      string
	GroupTag     string
	Enabled      bool
	EpgChannelID string
}

// ChannelSource binds a Channel to a (Provider, upstream URL) at a priority.
// A Channel with multiple Sources can fail over between them.
//
// UpstreamURL may contain template placeholders ${USER} and ${PASS} that are
// substituted from the leased ProviderCredential at request time.
type ChannelSource struct {
	ID            uuid.UUID
	ChannelID     uuid.UUID
	ProviderID    uuid.UUID
	UpstreamURL   string
	Priority      int     // 0 = primary, higher = failover
	HealthScore   float64 // 0.0–1.0, sliding-window error rate
	LastFailureAt *time.Time
	Enabled       bool
}

// Lineup is what Plex sees as one HDHomeRun device. Multiple lineups =
// multiple distinct device UUIDs, exposed via separate /discover.json
// responses (Phase 6 multi-tenant; Phase 1 supports a single Default lineup).
type Lineup struct {
	ID         uuid.UUID
	Name       string
	DeviceUUID string
}

type LineupChannel struct {
	LineupID  uuid.UUID
	ChannelID uuid.UUID
	Number    float64
	Position  int
}

// ActiveStream is one in-flight upstream connection. Refcounted by N
// StreamClient rows.
type ActiveStream struct {
	ID              uuid.UUID
	ChannelID       uuid.UUID
	ChannelSourceID uuid.UUID
	CredentialID    uuid.UUID
	UpstreamURL     string // resolved (creds substituted)
	StartedAt       time.Time
	LastHeartbeat   time.Time
	ClientCount     int
	State           string // "starting" | "running" | "draining" | "dead"
	BytesOut        int64
}

type StreamClient struct {
	ID             uuid.UUID
	ActiveStreamID uuid.UUID
	RemoteAddr     string
	UserAgent      string
	JoinedAt       time.Time
	LastSeen       time.Time
	BytesSent      int64
}
