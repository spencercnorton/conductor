-- retain source provenance, make New/repeat history canonical and
-- cross-channel, and expose one effective classification to every consumer.

-- The legacy table was never written by Conductor and its channel-scoped key
-- cannot represent an episode repeated on another network.  Refuse to discard
-- operator or out-of-band data silently if production differs from that audit.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM channel_episode_seen LIMIT 1) THEN
        RAISE EXCEPTION
            'channel_episode_seen is nonempty; migrate its data explicitly before ';
    END IF;
END
$$;

DROP TABLE channel_episode_seen;

ALTER TABLE epg_program
    -- is_new remains the normalized source-row candidate. These fields retain
    -- why it was asserted so future policy changes do not require re-fetching.
    ADD COLUMN new_explicit boolean NOT NULL DEFAULT false,
    ADD COLUMN previously_shown_explicit boolean NOT NULL DEFAULT false,
    -- Only episode-scoped provider IDs are stored (for example SD EP ids).
    ADD COLUMN provider_episode_id text NOT NULL DEFAULT '',
    -- Canonicalization may borrow a validated provider identity from one
    -- exact-slot donor. Keep that provenance distinct from the winner's source.
    ADD COLUMN provider_episode_id_supplemented boolean NOT NULL DEFAULT false,
    -- timestamptz preserves an instant, not the provider's calendar date. Keep
    -- the source civil date separately so future OADs stay rejected after a DB
    -- roundtrip. NULL means a legacy/manual row whose civil date is unknown.
    ADD COLUMN airing_civil_date date;

-- Program hashes and provenance policy changed in this migration. Force every
-- conditional source through one complete post-upgrade publish; otherwise a
-- 304 or unchanged SD watermark could retain pre-FX477 New values indefinitely.
UPDATE epg_source SET last_etag = '', last_modified = '';
UPDATE sd_station_state SET last_md5 = '';

CREATE FUNCTION epg_series_identity(value text)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    normalized text := lower(COALESCE(value, ''));
BEGIN
    -- Provider-controlled titles must never become an oversized expression-
    -- index key.  Real programme titles fit comfortably inside this bound;
    -- fail closed on pathological input instead of truncating two series into
    -- the same history identity.
    IF octet_length(normalized) > 512 THEN
        RETURN '';
    END IF;
    -- A provider may decorate only the live airing's title. Do not let that
    -- presentation badge split one series into unrelated history identities.
    normalized := regexp_replace(
        normalized,
        '^[[:space:]]*live[[:space:]]+(coverage|broadcast)[[:space:]]*[:–—-]?[[:space:]]*',
        '', 'i'
    );
    normalized := regexp_replace(
        normalized,
        '^[[:space:]]*live[[:space:]]*[:–—-][[:space:]]*',
        '', 'i'
    );
    normalized := regexp_replace(
        normalized,
        '[[:space:]]*[\[(][[:space:]]*live[[:space:]]*[\])][[:space:]]*$',
        '', 'i'
    );
    IF btrim(normalized) = 'live' THEN
        RETURN '';
    END IF;
    RETURN regexp_replace(normalized, '[^[:alnum:]]+', '', 'g');
END
$$;

-- One airing may expose a stable provider id, a formal season/episode number,
-- or both.  Keep every independently useful alias: a provider+formal airing is
-- the bridge that lets provider-only and formal-only reruns converge.  The
-- caller always scopes these aliases by series identity, so common S01E01
-- numbers cannot collide across unrelated shows.
CREATE FUNCTION epg_episode_identities(
    onscreen text,
    xmltv_ns text,
    provider_id text
)
RETURNS text[]
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    parts text[];
    provider_identity text := '';
    formal_identity text := '';
    identities text[] := ARRAY[]::text[];
BEGIN
    IF octet_length(btrim(COALESCE(provider_id, ''))) <= 128
       AND upper(btrim(COALESCE(provider_id, ''))) ~ '^EP[0-9A-Z]+$' THEN
        provider_identity := 'provider:' || lower(btrim(provider_id));
        identities := array_append(identities, provider_identity);
    END IF;

    parts := regexp_match(upper(btrim(COALESCE(onscreen, ''))), '^S([0-9]+)E([0-9]+)$');
    IF parts IS NOT NULL
       AND length(parts[1]) <= 9
       AND length(parts[2]) <= 9 THEN
        -- Formal metadata is provider-controlled text. Bound both components
        -- before casting so one oversized value cannot abort migration seeding
        -- or every read of epg_program_effective. Nine digits also leave room
        -- for xmltv_ns's one-based conversion below.
        formal_identity := 's' || parts[1]::integer || 'e' || parts[2]::integer;
    END IF;
    IF formal_identity = '' THEN
        parts := regexp_match(btrim(COALESCE(xmltv_ns, '')), '^([0-9]+)[.]([0-9]+)[.]?$');
    END IF;
    IF formal_identity = ''
       AND parts IS NOT NULL
       AND length(parts[1]) <= 9
       AND length(parts[2]) <= 9 THEN
        -- xmltv_ns is zero based; converge it with onscreen SxxEyy identity.
        formal_identity := 's' || (parts[1]::integer + 1) || 'e' || (parts[2]::integer + 1);
    END IF;
    IF formal_identity <> '' AND formal_identity <> provider_identity THEN
        identities := array_append(identities, formal_identity);
    END IF;

    RETURN identities;
END
$$;

CREATE FUNCTION epg_episode_identity(
    onscreen text,
    xmltv_ns text,
    provider_id text
)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    -- Preserve the provider-preferred public identity while history matching
    -- consumes the complete alias set above.
    SELECT COALESCE((epg_episode_identities(onscreen, xmltv_ns, provider_id))[1], '')
$$;

CREATE TABLE episode_airing_history (
    program_id        uuid NOT NULL,
    series_identity  text NOT NULL,
    episode_identity text NOT NULL,
    first_aired_at   timestamptz,
    repeat_proven    boolean NOT NULL DEFAULT false,
    first_observed_at timestamptz NOT NULL DEFAULT now(),
    last_observed_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (program_id, series_identity, episode_identity),
    CHECK (first_aired_at IS NOT NULL OR repeat_proven)
);

CREATE INDEX episode_airing_history_lookup_idx
    ON episode_airing_history (series_identity, episode_identity)
    INCLUDE (first_aired_at, repeat_proven);

-- Alias overlap is evaluated only after narrowing to one series and an earlier
-- start. Keep those two correlated-view predicates adjacent in the B-tree.
CREATE INDEX epg_program_effective_episode_idx
    ON epg_program (
        epg_series_identity(title),
        start_at
    )
    WHERE is_canonical AND NOT is_movie;

-- Seed only visible airings whose wall-clock start has arrived. Merely
-- receiving a future schedule must not poison history if it moves or vanishes.
INSERT INTO episode_airing_history (
    program_id, series_identity, episode_identity, first_aired_at
)
SELECT id,
       epg_series_identity(title),
       identity.episode_identity,
       min(start_at)
  FROM epg_program
 CROSS JOIN LATERAL unnest(epg_episode_identities(
     episode_num_onscreen, episode_num_xmltv, provider_episode_id
 )) AS identity(episode_identity)
 WHERE is_canonical
   AND NOT is_movie
   AND start_at <= clock_timestamp()
   AND epg_series_identity(title) <> ''
 GROUP BY id,
          epg_series_identity(title),
          identity.episode_identity;

-- Archive a canonical row when it is published, changed, demoted, or deleted.
-- Started rows prove their exact airing time; an explicit repeat proves a prior
-- airing even when the provider does not supply that historical timestamp.
CREATE FUNCTION epg_record_airing_history(
    row_program_id uuid,
    row_title text,
    row_onscreen text,
    row_xmltv_ns text,
    row_provider_id text,
    row_start_at timestamptz,
    row_is_movie boolean,
    row_is_canonical boolean,
    row_previously_shown_explicit boolean
)
RETURNS void
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    series_id text;
    episode_ids text[];
    episode_id text;
    aired_at timestamptz;
BEGIN
    IF NOT row_is_canonical OR row_is_movie THEN
        RETURN;
    END IF;
    IF row_start_at > clock_timestamp() AND NOT row_previously_shown_explicit THEN
        RETURN;
    END IF;

    series_id := epg_series_identity(row_title);
    episode_ids := epg_episode_identities(row_onscreen, row_xmltv_ns, row_provider_id);
    IF series_id = '' OR cardinality(episode_ids) = 0 THEN
        RETURN;
    END IF;

    IF row_start_at <= clock_timestamp() THEN
        aired_at := row_start_at;
    END IF;

    FOREACH episode_id IN ARRAY episode_ids LOOP
        INSERT INTO episode_airing_history (
            program_id, series_identity, episode_identity, first_aired_at, repeat_proven
        ) VALUES (
            row_program_id, series_id, episode_id, aired_at, row_previously_shown_explicit
        )
        -- History rows are scoped by the durable programme UUID while the
        -- lookup index remains series/episode-wide. Distinct guide rows never
        -- contend on one unique key, so opposite alias orders cannot deadlock;
        -- replay of one row still compacts into its exact provenance record.
        ON CONFLICT (program_id, series_identity, episode_identity) DO UPDATE
           SET first_aired_at = CASE
                   WHEN episode_airing_history.first_aired_at IS NULL
                       THEN EXCLUDED.first_aired_at
                   WHEN EXCLUDED.first_aired_at IS NULL
                       THEN episode_airing_history.first_aired_at
                   ELSE LEAST(episode_airing_history.first_aired_at, EXCLUDED.first_aired_at)
               END,
               repeat_proven = episode_airing_history.repeat_proven OR EXCLUDED.repeat_proven,
               last_observed_at = clock_timestamp();
    END LOOP;
END
$$;

CREATE FUNCTION epg_program_history_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'UPDATE'
       AND ROW(
           OLD.title,
           OLD.episode_num_onscreen,
           OLD.episode_num_xmltv,
           OLD.provider_episode_id,
           OLD.start_at,
           OLD.is_movie,
           OLD.is_canonical,
           OLD.previously_shown_explicit
       ) IS NOT DISTINCT FROM ROW(
           NEW.title,
           NEW.episode_num_onscreen,
           NEW.episode_num_xmltv,
           NEW.provider_episode_id,
           NEW.start_at,
           NEW.is_movie,
           NEW.is_canonical,
           NEW.previously_shown_explicit
       ) THEN
        -- Description, artwork, enrichment, and other non-identity updates do
        -- not change historical evidence and must not serialize snapshots or
        -- churn last_observed_at.
        RETURN NEW;
    END IF;
    IF TG_OP <> 'INSERT' THEN
        PERFORM epg_record_airing_history(
            OLD.id, OLD.title, OLD.episode_num_onscreen, OLD.episode_num_xmltv,
            OLD.provider_episode_id, OLD.start_at, OLD.is_movie,
            OLD.is_canonical, OLD.previously_shown_explicit
        );
    END IF;
    IF TG_OP <> 'DELETE' THEN
        PERFORM epg_record_airing_history(
            NEW.id, NEW.title, NEW.episode_num_onscreen, NEW.episode_num_xmltv,
            NEW.provider_episode_id, NEW.start_at, NEW.is_movie,
            NEW.is_canonical, NEW.previously_shown_explicit
        );
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER epg_program_history_capture
AFTER INSERT OR UPDATE OR DELETE ON epg_program
FOR EACH ROW EXECUTE FUNCTION epg_program_history_trigger();

-- All metadata consumers use this view. Effective New requires credible
-- source evidence, a recognized final canonical episode identity, no explicit
-- repeat evidence, and the earliest actual canonical airing across channels.
-- Equal-time simulcasts share the same earliest airing instant.
CREATE VIEW epg_program_effective AS
SELECT p.*,
       identity.series_identity,
       identity.episode_identity,
       (
           p.is_canonical
           AND NOT p.is_movie
           AND p.is_new
           AND NOT p.previously_shown_explicit
           AND identity.series_identity <> ''
           AND identity.episode_identity <> ''
           AND cardinality(identity.episode_identities) > 0
           AND NOT EXISTS (
               SELECT 1
                 FROM epg_program earlier
                WHERE earlier.is_canonical
                  AND NOT earlier.is_movie
                  AND epg_series_identity(earlier.title) = identity.series_identity
                  AND epg_episode_identities(
                          earlier.episode_num_onscreen,
                          earlier.episode_num_xmltv,
                          earlier.provider_episode_id
                      ) && identity.episode_identities
                  AND earlier.start_at < p.start_at
           )
           AND NOT EXISTS (
               SELECT 1
                 FROM episode_airing_history history
                WHERE history.series_identity = identity.series_identity
                  AND history.episode_identity = ANY(identity.episode_identities)
                  AND (
                      history.repeat_proven
                      OR history.first_aired_at < p.start_at
                  )
           )
       ) AS effective_is_new
  FROM epg_program p
 CROSS JOIN LATERAL (
     SELECT epg_series_identity(p.title) AS series_identity,
            epg_episode_identity(
                p.episode_num_onscreen,
                p.episode_num_xmltv,
                p.provider_episode_id
            ) AS episode_identity,
            epg_episode_identities(
                p.episode_num_onscreen,
                p.episode_num_xmltv,
                p.provider_episode_id
            ) AS episode_identities
 ) identity;
