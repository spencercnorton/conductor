package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ErrNotFound is returned by typed lookups when no row matches.
var ErrNotFound = errors.New("not found")

// ─────────────────────── Provider ───────────────────────

func (db *DB) CreateProvider(ctx context.Context, p Provider) (Provider, error) {
	if err := ValidateProviderCapacityDomain(p.CapacityDomain); err != nil {
		return Provider{}, err
	}
	if p.ID == uuid.Nil {
		p.ID = uuid.New()
	}
	row := db.Pool.QueryRow(ctx, `
		INSERT INTO provider (id, name, kind, base_url, capacity_domain, notes, enabled)
		VALUES ($1, $2, $3::provider_kind, $4, $5, $6, $7)
		RETURNING created_at`,
		p.ID, p.Name, p.Kind, p.BaseURL, p.CapacityDomain, p.Notes, p.Enabled)
	if err := row.Scan(&p.CreatedAt); err != nil {
		return Provider{}, fmt.Errorf("create provider: %w", err)
	}
	return p, nil
}

func (db *DB) ListProviders(ctx context.Context) ([]Provider, error) {
	rows, err := db.Pool.Query(ctx, `
		SELECT id, name, kind::text, base_url, capacity_domain, notes, enabled, created_at
		  FROM provider
		 ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Provider
	for rows.Next() {
		var p Provider
		if err := rows.Scan(&p.ID, &p.Name, &p.Kind, &p.BaseURL, &p.CapacityDomain,
			&p.Notes, &p.Enabled, &p.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (db *DB) GetProvider(ctx context.Context, id uuid.UUID) (Provider, error) {
	var p Provider
	row := db.Pool.QueryRow(ctx, `
		SELECT id, name, kind::text, base_url, capacity_domain, notes, enabled, created_at
		  FROM provider WHERE id = $1`, id)
	err := row.Scan(&p.ID, &p.Name, &p.Kind, &p.BaseURL, &p.CapacityDomain,
		&p.Notes, &p.Enabled, &p.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Provider{}, ErrNotFound
	}
	return p, err
}

// ─────────────────────── Credentials ───────────────────────

func (db *DB) CreateCredential(ctx context.Context, c ProviderCredential) (ProviderCredential, error) {
	if c.ID == uuid.Nil {
		c.ID = uuid.New()
	}
	row := db.Pool.QueryRow(ctx, `
		INSERT INTO provider_credential (id, provider_id, username, password_enc, max_streams, priority, notes, enabled)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING created_at`,
		c.ID, c.ProviderID, c.Username, c.PasswordEnc, c.MaxStreams, c.Priority, c.Notes, c.Enabled)
	if err := row.Scan(&c.CreatedAt); err != nil {
		return ProviderCredential{}, fmt.Errorf("create credential: %w", err)
	}
	return c, nil
}

func (db *DB) ListCredentials(ctx context.Context, providerID uuid.UUID) ([]ProviderCredential, error) {
	rows, err := db.Pool.Query(ctx, `
		SELECT id, provider_id, username, password_enc, max_streams, priority, notes, enabled, created_at
		  FROM provider_credential
		 WHERE provider_id = $1
		 ORDER BY priority, username`, providerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ProviderCredential
	for rows.Next() {
		var c ProviderCredential
		if err := rows.Scan(&c.ID, &c.ProviderID, &c.Username, &c.PasswordEnc, &c.MaxStreams, &c.Priority, &c.Notes, &c.Enabled, &c.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// ─────────────────────── Channels ───────────────────────

func (db *DB) CreateChannel(ctx context.Context, c Channel) (Channel, error) {
	if c.ID == uuid.Nil {
		c.ID = uuid.New()
	}
	// Every channel creation can make a previously-unmapped XMLTV identifier
	// resolvable: explicit epg_channel_id is only the first lookup; numeric feed
	// IDs also fall back to channel.number.  Join the same source-set/SD authority
	// as a remap so an accepted cached representation cannot remain hidden behind
	// a future 304 after the new binding appears.
	releaseAuthority, err := db.acquireSDAuthority(ctx)
	if err != nil {
		return Channel{}, fmt.Errorf("create channel: %w", err)
	}
	defer releaseAuthority()
	err = db.InTx(ctx, func(tx pgx.Tx) error {
		if err := lockSDIngestPass(ctx, tx); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `
			SELECT id
			  FROM epg_source
			 ORDER BY id
			 FOR UPDATE`)
		if err != nil {
			return err
		}
		var lockedSourceIDs []uuid.UUID
		for rows.Next() {
			var sourceID uuid.UUID
			if err := rows.Scan(&sourceID); err != nil {
				rows.Close()
				return err
			}
			lockedSourceIDs = append(lockedSourceIDs, sourceID)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()

		if _, err := tx.Exec(ctx, `
			INSERT INTO channel (id, number, name, call_sign, logo_url, group_tag, enabled, epg_channel_id)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			c.ID, c.Number, c.Name, c.CallSign, c.LogoURL, c.GroupTag, c.Enabled, c.EpgChannelID); err != nil {
			return err
		}
		if len(lockedSourceIDs) > 0 {
			if _, err := tx.Exec(ctx, `
				UPDATE epg_source
				   SET snapshot_generation = snapshot_generation + 1,
				       last_etag = '', last_modified = ''
				 WHERE id = ANY($1::uuid[])`, lockedSourceIDs); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, `UPDATE sd_station_state SET last_md5 = ''`); err != nil {
			return err
		}
		return bumpEPGRevision(ctx, tx)
	})
	if err != nil {
		return Channel{}, fmt.Errorf("create channel: %w", err)
	}
	return c, nil
}

func (db *DB) ListChannels(ctx context.Context) ([]Channel, error) {
	return db.listChannels(ctx, false)
}

// ListAllChannels includes disabled rows for admin repair/provisioning tools.
// Public lineup and tuning paths must keep using ListChannels so a disabled
// partial provision never leaks into Plex.
func (db *DB) ListAllChannels(ctx context.Context) ([]Channel, error) {
	return db.listChannels(ctx, true)
}

func (db *DB) listChannels(ctx context.Context, includeDisabled bool) ([]Channel, error) {
	rows, err := db.Pool.Query(ctx, `
		SELECT id, number, name, call_sign, logo_url, group_tag, enabled, epg_channel_id
		  FROM channel
		 WHERE $1::boolean OR enabled
		 ORDER BY number`, includeDisabled)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Channel
	for rows.Next() {
		var c Channel
		if err := rows.Scan(&c.ID, &c.Number, &c.Name, &c.CallSign, &c.LogoURL, &c.GroupTag, &c.Enabled, &c.EpgChannelID); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// GetChannelLogoURL returns the stored logo URL for one channel. Empty
// string + nil is the "channel exists but has no logo" case; ErrNotFound
// is the "channel id is unknown" case. Used by the branded-poster
// fallback to find the local logo file.
func (db *DB) GetChannelLogoURL(ctx context.Context, channelID uuid.UUID) (string, error) {
	var url string
	err := db.Pool.QueryRow(ctx,
		`SELECT logo_url FROM channel WHERE id = $1`, channelID).Scan(&url)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrNotFound
	}
	return url, err
}

// GetChannelByNumber resolves the channel Plex requested via /auto/v<num>.
// Plex sends the GuideNumber from /lineup.json verbatim.
func (db *DB) GetChannelByNumber(ctx context.Context, number float64) (Channel, error) {
	var c Channel
	row := db.Pool.QueryRow(ctx, `
		SELECT id, number, name, call_sign, logo_url, group_tag, enabled, epg_channel_id
		  FROM channel WHERE number = $1`, number)
	err := row.Scan(&c.ID, &c.Number, &c.Name, &c.CallSign, &c.LogoURL, &c.GroupTag, &c.Enabled, &c.EpgChannelID)
	if errors.Is(err, pgx.ErrNoRows) {
		return Channel{}, ErrNotFound
	}
	return c, err
}

// ─────────────────────── Channel Sources ───────────────────────

// ChannelUpdate carries optional field changes for UpdateChannel.
// nil pointer = leave unchanged. EpgChannelID may be set to the empty
// string to clear a mapping (pointer-to-"" is distinct from nil).
type ChannelUpdate struct {
	Name         *string
	CallSign     *string
	LogoURL      *string
	GroupTag     *string
	EpgChannelID *string
	Enabled      *bool
}

// UpdateChannel applies a partial update to one channel row.
func (db *DB) UpdateChannel(ctx context.Context, id uuid.UUID, u ChannelUpdate) (Channel, error) {
	if u.EpgChannelID != nil {
		releaseAuthority, err := db.acquireSDAuthority(ctx)
		if err != nil {
			return Channel{}, fmt.Errorf("update channel: %w", err)
		}
		defer releaseAuthority()
	}
	var c Channel
	err := db.InTx(ctx, func(tx pgx.Tx) error {
		var lockedSourceIDs []uuid.UUID
		// A possible mapping edit takes global and source authority before the
		// channel row. We cannot know whether the mapping really
		// changes until the channel row is locked, so conservatively lock the
		// currently visible source set first. Sources inserted after this scan have
		// no candidates or validators yet; their publication is fenced by the
		// binding recheck in ReplaceEPGSourceSnapshot.
		if u.EpgChannelID != nil {
			if err := lockSDIngestPass(ctx, tx); err != nil {
				return err
			}
			rows, err := tx.Query(ctx, `
				SELECT id
				  FROM epg_source
				 ORDER BY id
				 FOR UPDATE`)
			if err != nil {
				return err
			}
			for rows.Next() {
				var sourceID uuid.UUID
				if err := rows.Scan(&sourceID); err != nil {
					rows.Close()
					return err
				}
				lockedSourceIDs = append(lockedSourceIDs, sourceID)
			}
			if err := rows.Err(); err != nil {
				rows.Close()
				return err
			}
			rows.Close()
		}
		var oldEPGID string
		if err := tx.QueryRow(ctx, `
			SELECT epg_channel_id
			  FROM channel
			 WHERE id = $1
			 FOR UPDATE`, id).Scan(&oldEPGID); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		mappingChanged := u.EpgChannelID != nil && *u.EpgChannelID != oldEPGID
		if mappingChanged {
			// Invalidate only the exact source rows locked by the scan, and do it
			// without a later unqualified scan. A source created concurrently starts
			// at generation zero with empty validators and is fenced by its mapping
			// recheck if it attempts to publish the superseded binding.
			if len(lockedSourceIDs) > 0 {
				if _, err := tx.Exec(ctx, `
					UPDATE epg_source
					   SET snapshot_generation = snapshot_generation + 1,
					       last_etag = '', last_modified = ''
					 WHERE id = ANY($1::uuid[])`, lockedSourceIDs); err != nil {
					return err
				}
			}
			if err := lockEPGChannel(ctx, tx, id); err != nil {
				return err
			}
		}

		row := tx.QueryRow(ctx, `
			UPDATE channel
			   SET name           = COALESCE($2, name),
			       call_sign      = COALESCE($3, call_sign),
			       logo_url       = COALESCE($4, logo_url),
			       group_tag      = COALESCE($5, group_tag),
			       epg_channel_id = COALESCE($6, epg_channel_id),
			       enabled        = COALESCE($7, enabled)
			 WHERE id = $1
			 RETURNING id, number, name, call_sign, logo_url, group_tag, enabled, epg_channel_id`,
			id, u.Name, u.CallSign, u.LogoURL, u.GroupTag, u.EpgChannelID, u.Enabled)
		if err := row.Scan(&c.ID, &c.Number, &c.Name, &c.CallSign, &c.LogoURL, &c.GroupTag, &c.Enabled, &c.EpgChannelID); err != nil {
			return err
		}

		outputChanged := u.Name != nil || u.LogoURL != nil || u.Enabled != nil || mappingChanged
		if mappingChanged {
			// The old mapping's current/future guide is no longer authoritative.
			// Remove source-backed, explicit SD, and pre-provenance rows now, then
			// force every
			// source's next request to be unconditional so a cached 304 cannot
			// bypass reprocessing under the new mapping.
			if _, err := tx.Exec(ctx, `
				DELETE FROM epg_program
				 WHERE channel_id = $1
				   AND end_at > now()
				   AND (epg_source_id IS NOT NULL OR is_legacy OR source_hash LIKE 'sd:%')`, id); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `
				UPDATE sd_station_state
				   SET last_md5 = '',
				       channel_id = CASE WHEN channel_id = $1 THEN NULL ELSE channel_id END`, id); err != nil {
				return err
			}
			if _, _, err := canonicalizeEPGChannel(ctx, tx, id); err != nil {
				return err
			}
		}
		if outputChanged {
			return bumpEPGRevision(ctx, tx)
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return Channel{}, ErrNotFound
		}
		return Channel{}, fmt.Errorf("update channel: %w", err)
	}
	return c, nil
}

func (db *DB) CreateChannelSource(ctx context.Context, s ChannelSource) (ChannelSource, error) {
	if s.ID == uuid.Nil {
		s.ID = uuid.New()
	}
	_, err := db.Pool.Exec(ctx, `
		INSERT INTO channel_source (id, channel_id, provider_id, upstream_url, priority, health_score, enabled)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		s.ID, s.ChannelID, s.ProviderID, s.UpstreamURL, s.Priority, s.HealthScore, s.Enabled)
	if err != nil {
		return ChannelSource{}, fmt.Errorf("create channel_source: %w", err)
	}
	return s, nil
}

// ListChannelSources returns every channel_source row regardless of
// channel — used by background workers (e.g. ppvsync) that walk the
// full source set on each pass. Includes disabled sources so callers
// can filter on their own criteria.
func (db *DB) ListChannelSources(ctx context.Context) ([]ChannelSource, error) {
	rows, err := db.Pool.Query(ctx, `
		SELECT id, channel_id, provider_id, upstream_url, priority,
		       health_score, last_failure_at, enabled
		  FROM channel_source
		 ORDER BY channel_id, priority`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ChannelSource
	for rows.Next() {
		var s ChannelSource
		if err := rows.Scan(&s.ID, &s.ChannelID, &s.ProviderID, &s.UpstreamURL,
			&s.Priority, &s.HealthScore, &s.LastFailureAt, &s.Enabled); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// ListSourcesForChannel returns enabled sources in the exact stable order the
// DVR forecast uses: priority, health, provider, then source identity.
func (db *DB) ListSourcesForChannel(ctx context.Context, channelID uuid.UUID) ([]ChannelSource, error) {
	rows, err := db.Pool.Query(ctx, `
		SELECT id, channel_id, provider_id, upstream_url, priority, health_score, last_failure_at, enabled
		  FROM channel_source
		 WHERE channel_id = $1 AND enabled
		 ORDER BY priority ASC, health_score DESC, provider_id ASC, id ASC`, channelID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ChannelSource
	for rows.Next() {
		var s ChannelSource
		if err := rows.Scan(&s.ID, &s.ChannelID, &s.ProviderID, &s.UpstreamURL, &s.Priority, &s.HealthScore, &s.LastFailureAt, &s.Enabled); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// CountEnabledSourcesForChannel returns how many enabled sources a channel
// has. Used by the failover loop to decide when the exclude-list (sources
// already tried-and-failed this reconnect gap) covers every source — at
// which point there is nothing left to fail over to and the pump terminates
// to the Source-Unavailable slate instead of ping-ponging between dead
// sources (audit S2).
func (db *DB) CountEnabledSourcesForChannel(ctx context.Context, channelID uuid.UUID) (int, error) {
	var n int
	err := db.Pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM channel_source
		 WHERE channel_id = $1 AND enabled`, channelID).Scan(&n)
	return n, err
}

// MarkSourceFailure decrements health_score (sliding-window approximation)
// and sets last_failure_at. Called by the proxy when an upstream errors.
func (db *DB) MarkSourceFailure(ctx context.Context, sourceID uuid.UUID) error {
	_, err := db.Pool.Exec(ctx, `
		UPDATE channel_source
		   SET health_score = GREATEST(0.0, health_score - 0.2),
		       last_failure_at = now()
		 WHERE id = $1`, sourceID)
	return err
}

// MarkSourceSuccess recovers health_score after a pump that delivered stable
// media exits. Recovery is proportional to the remaining headroom: from 0.0 a
// source reaches 0.25, 0.44, 0.58, 0.68, 0.76 over five clean sessions and
// 0.9 after eight. The previous flat +0.05 needed twenty clean sessions to undo
// one boundary storm's five -0.2 failures, and success is credited once per
// pump lifetime (a channel watched once a day recovered 0.05 a day), which
// pinned working sources at 0.0 for weeks (measured 2026-08-10).
func (db *DB) MarkSourceSuccess(ctx context.Context, sourceID uuid.UUID) error {
	_, err := db.Pool.Exec(ctx, `
		UPDATE channel_source
		   SET health_score = LEAST(1.0, health_score + 0.25 * (1.0 - health_score))
		 WHERE id = $1`, sourceID)
	return err
}

// ChannelSourceUpdate is a sparse update — nil fields are left unchanged.
// ResetFailure clears last_failure_at when true (useful after manually fixing
// an upstream so the next tune doesn't get penalized by the health-score
// recovery curve).
type ChannelSourceUpdate struct {
	UpstreamURL  *string
	Priority     *int
	Enabled      *bool
	HealthScore  *float64
	ProviderID   *uuid.UUID
	ResetFailure bool
}

// UpdateChannelSource applies a sparse update to one channel_source row,
// scoped to (channel_id, source_id) so callers can't accidentally mutate a
// row from a different channel by guessing source IDs. Returns ErrNotFound
// if no row matches both ids.
func (db *DB) UpdateChannelSource(ctx context.Context, channelID, sourceID uuid.UUID, u ChannelSourceUpdate) (ChannelSource, error) {
	row := db.Pool.QueryRow(ctx, `
		UPDATE channel_source
		   SET upstream_url    = COALESCE($3, upstream_url),
		       priority        = COALESCE($4, priority),
		       enabled         = COALESCE($5, enabled),
		       health_score    = COALESCE($6, health_score),
		       provider_id     = COALESCE($7, provider_id),
		       last_failure_at = CASE WHEN $8::bool THEN NULL ELSE last_failure_at END
		 WHERE id = $1 AND channel_id = $2
		 RETURNING id, channel_id, provider_id, upstream_url, priority, health_score, last_failure_at, enabled`,
		sourceID, channelID, u.UpstreamURL, u.Priority, u.Enabled, u.HealthScore, u.ProviderID, u.ResetFailure)
	var s ChannelSource
	err := row.Scan(&s.ID, &s.ChannelID, &s.ProviderID, &s.UpstreamURL, &s.Priority, &s.HealthScore, &s.LastFailureAt, &s.Enabled)
	if errors.Is(err, pgx.ErrNoRows) {
		return ChannelSource{}, ErrNotFound
	}
	if err != nil {
		return ChannelSource{}, fmt.Errorf("update channel_source: %w", err)
	}
	return s, nil
}

// ─────────────────────── Lineups ───────────────────────

func (db *DB) CreateLineup(ctx context.Context, l Lineup) (Lineup, error) {
	if l.ID == uuid.Nil {
		l.ID = uuid.New()
	}
	_, err := db.Pool.Exec(ctx, `
		INSERT INTO lineup (id, name, device_uuid) VALUES ($1, $2, $3)`,
		l.ID, l.Name, l.DeviceUUID)
	return l, err
}

// ListLineupChannels returns the channels in a lineup, ordered by position.
// Used by the HDHomeRun /lineup.json handler.
type LineupChannelRow struct {
	Number   float64
	Name     string
	CallSign string
	LogoURL  string
	Enabled  bool
}

func (db *DB) ListLineupChannels(ctx context.Context, lineupID uuid.UUID) ([]LineupChannelRow, error) {
	rows, err := db.Pool.Query(ctx, `
		SELECT lc.number, c.name, c.call_sign, c.logo_url, c.enabled
		  FROM lineup_channel lc
		  JOIN channel c ON c.id = lc.channel_id
		 WHERE lc.lineup_id = $1 AND c.enabled
		 ORDER BY lc.position ASC, lc.number ASC`, lineupID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []LineupChannelRow
	for rows.Next() {
		var r LineupChannelRow
		if err := rows.Scan(&r.Number, &r.Name, &r.CallSign, &r.LogoURL, &r.Enabled); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (db *DB) AddChannelToLineup(ctx context.Context, lc LineupChannel) error {
	_, err := db.Pool.Exec(ctx, `
		INSERT INTO lineup_channel (lineup_id, channel_id, number, position)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (lineup_id, channel_id) DO UPDATE
		   SET number = EXCLUDED.number, position = EXCLUDED.position`,
		lc.LineupID, lc.ChannelID, lc.Number, lc.Position)
	return err
}

// CountTuners returns the sum of max_streams across all enabled credentials.
// This is the TunerCount Plex sees in /discover.json — the system-wide
// concurrency the device claims to support.
func (db *DB) CountTuners(ctx context.Context) (int, error) {
	var n int
	row := db.Pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(max_streams), 0)::int
		  FROM provider_credential WHERE enabled`)
	err := row.Scan(&n)
	return n, err
}

// ─────────────────────── Active streams (state queries) ───────────────────

// ListActiveStreams returns every provider slot that is still occupied.
// Draining rows stay visible until the pump's upstream/ffmpeg teardown exits.
func (db *DB) ListActiveStreams(ctx context.Context) ([]ActiveStream, error) {
	rows, err := db.Pool.Query(ctx, `
		SELECT id, channel_id, channel_source_id, credential_id, upstream_url,
		       started_at, last_heartbeat, client_count, state::text, bytes_out
		  FROM active_stream
		 WHERE state IN ('starting', 'running', 'draining')
		 ORDER BY started_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ActiveStream
	for rows.Next() {
		var a ActiveStream
		if err := rows.Scan(&a.ID, &a.ChannelID, &a.ChannelSourceID, &a.CredentialID,
			&a.UpstreamURL, &a.StartedAt, &a.LastHeartbeat, &a.ClientCount, &a.State, &a.BytesOut); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}
