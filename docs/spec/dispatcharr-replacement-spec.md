# Dispatcharr Replacement: Technical Specification

**Context:** a Plex-first self-hosted setup with Tunarr already in use, where
Dispatcharr is the weak link. This document is the specification the
implementation was driven from; each section is meant to be self-sufficient
enough to execute against.

---

## 1. Why rebuild instead of patch

Dispatcharr's pain points fall into three buckets, and the third is structural enough that patching upstream is a worse use of time than a clean reimplementation:

1. **Stream slot accounting is unreliable.** Slots leak on channel change, and the fix has been to run multiple instances per credential set rather than treat credentials as a first-class concept inside one process. This is a data model problem, not a bug.
2. **EPG is thin.** No poster/show enrichment, no sports awareness, weak live/new tagging — Plex's Live TV UX falls flat as a result.
3. **Plex integration is brittle.** HDHomeRun emulation works at the surface level but the XMLTV output doesn't hit the specific tags Plex parses, so the experience inside Plex is downgraded vs. what the data could support.

Goal: a drop-in HDHomeRun replacement that Plex sees as a clean, well-behaved tuner, with rich EPG and proper multi-credential connection pooling.

**Out of scope (deliberately):**
- Virtual channels — Tunarr is good, leave it alone. Optionally expose an integration so Tunarr can pull from this tool's stream pool instead of going direct to providers.
- DVR — Plex DVR is sufficient.
- Transcoding farm — pass through; let Plex transcode if it needs to.
- Mobile/standalone player — this is infrastructure, not a viewer.

---

## 2. Architecture

### 2.1 Language / runtime

**Recommendation: Go.** Reasons:
- Stream proxying is a long-lived TCP/HTTP problem with high concurrency; goroutines map cleanly to per-stream and per-client work.
- Single static binary deploys easily into a Linux/KVM host or container.
- Strong stdlib for HTTP/2, TLS, and io.Pipe for buffer chains.
- FFmpeg invocation when needed via os/exec is fine.

**Alternative: Python (FastAPI + asyncio + uvloop).** Acceptable if Claude Code or future contributors prefer it; the streaming hot path needs careful attention to avoid GIL contention. If chosen, isolate the stream proxy as a separate process from the API/UI server.

**Frontend: React + TypeScript + Vite.** TanStack Query for server state. Tailwind for speed.

### 2.2 Process layout

One binary, multiple internal services:

```
┌─────────────────────────────────────────────────────────────┐
│                      conductor (single binary)               │
├──────────────┬──────────────┬──────────────┬────────────────┤
│ HTTP API +   │ Stream Proxy │ EPG Worker   │ Enrichment     │
│ HDHomeRun    │ (per-stream  │ (XMLTV       │ Worker         │
│ Emulator     │  goroutines) │  fetch +     │ (TMDb / TVDB / │
│              │              │  parse)      │  TheSportsDB)  │
└──────┬───────┴──────┬───────┴──────┬───────┴────────┬───────┘
       │              │              │                │
       └──────────────┴──────────────┴────────────────┘
                              │
                  ┌───────────┴───────────┐
                  │  PostgreSQL (state)   │
                  │  Redis (ephemeral)    │
                  └───────────────────────┘
```

- **PostgreSQL** for everything durable: providers, channels, EPG, enrichment cache, audit.
- **Redis** for ephemeral, hot-path state: active stream registry, provider slot leases, distributed locks. Falls back to in-memory if running single-node (which is the default).
- **Single process** is the default deploy. The internal split is a logical one to keep concerns isolated; goroutines / asyncio tasks share the binary.

### 2.3 Storage choices

- **Postgres**, not SQLite, even for single-user. The active stream tracking and concurrent EPG ingestion benefit from row-level locks. Embed Postgres via container in the deploy if "no external service" is a hard requirement, but don't roll back to SQLite.
- **Bolt or BadgerDB** as a fallback if you really want zero external deps for a hobby deploy. Note this in deploy options, don't make it the default.
- **Filesystem** for: channel logos, cached posters, raw XMLTV snapshots. Use a content-addressable layout (`/cache/sha256/<hash>`) to dedupe.

---

## 3. Data Model

Core tables. Names are illustrative; the relationships are the point.

### 3.1 Providers and credentials

```
provider
  id              uuid pk
  name            text
  kind            enum [m3u_xtream, m3u_plain, stalker]
  base_url        text
  notes           text
  enabled         bool
  created_at      timestamptz

provider_credential
  id              uuid pk
  provider_id     uuid fk
  username        text
  password        text (encrypted at rest)
  max_streams     int             -- THE concurrency limit for this credential
  priority        int             -- lower = preferred
  notes           text             -- "shared with mom", "primary", etc.
  enabled         bool
```

**This is the structural fix for the multi-instance problem.** A provider can have N credential sets, each carrying its own slot budget. The stream allocator picks credentials at request time based on current load, not at process boot.

### 3.2 Channels and sources

```
channel
  id              uuid pk
  number          numeric(6,1) unique     -- "100", "100.1"
  name            text
  call_sign       text                     -- for XMLTV channel id
  logo_url        text
  group_tag       text                     -- "Sports", "News", etc.
  enabled         bool
  epg_channel_id  text                     -- maps to xmltv <channel id="...">

channel_source
  id              uuid pk
  channel_id      uuid fk
  provider_id     uuid fk
  upstream_url    text                     -- full URL or template
  priority        int                      -- 0 = primary, 1+ = failover
  health_score    float                    -- maintained by stream worker
  last_failure_at timestamptz
  enabled         bool
```

Channel decoupled from source so a channel can have N upstreams across providers and failover is a first-class operation.

### 3.3 Active stream registry

```
active_stream
  id                uuid pk
  channel_id        uuid fk
  channel_source_id uuid fk
  credential_id     uuid fk          -- which credential is holding the slot
  upstream_url      text             -- resolved URL with creds substituted
  started_at        timestamptz
  last_heartbeat    timestamptz
  client_count      int              -- refcount; multiple Plex clients can share one upstream
  state             enum [starting, running, draining, dead]
  bytes_out         bigint
  upstream_pid      int              -- if FFmpeg, the child pid

stream_client
  id                uuid pk
  active_stream_id  uuid fk
  remote_addr       inet
  user_agent        text
  joined_at         timestamptz
  last_seen         timestamptz
  bytes_sent        bigint
```

Two-level model: one upstream connection can fan out to many client connections. This is critical — it both protects credential slots and reduces upstream load when multiple Plex clients (or Plex + a phone) tune the same channel.

### 3.4 EPG

```
epg_program
  id                  uuid pk
  channel_id          uuid fk
  start_at            timestamptz
  end_at              timestamptz
  title               text
  sub_title           text
  description         text
  category            text[]
  episode_num_xmltv   text              -- "0.1.0/1"
  episode_num_onscreen text             -- "S01E02"
  provider_episode_id text              -- validated provider EP id, when present
  provider_episode_id_supplemented bool -- canonical value borrowed from one exact-slot donor
  original_air_date   date
  airing_civil_date   date nullable     -- source timestamp's date before timestamptz conversion
  is_movie            bool
  is_live             bool
  is_new              bool              -- normalized source-row candidate
  new_explicit        bool              -- source asserted New
  previously_shown_explicit bool        -- source asserted repeat
  is_premiere         bool
  is_finale           bool
  rating              text
  source_hash         text              -- content hash of source XMLTV entry
  epg_source_id       uuid fk nullable  -- XMLTV provenance; null for synthetic workers
  source_generation   bigint nullable   -- successful source snapshot that observed this row
  source_priority     int               -- lower value wins interval admission
  is_canonical        bool              -- the only schedule visible to consumers
  enrichment_status   enum [pending, matched, failed, manual]

epg_program_enrichment
  program_id          uuid pk fk
  tmdb_id             int
  tvdb_id             int
  imdb_id             text
  poster_url          text
  backdrop_url        text
  overview            text              -- enriched, fuller than XMLTV desc
  cast_json           jsonb
  sports_event_json   jsonb             -- TheSportsDB payload if matched
  match_confidence    float
  matched_at          timestamptz
```

Separating enrichment from the source EPG row matters: re-ingestion of XMLTV shouldn't blow away enrichment work.

### 3.5 Lineups

```
lineup
  id          uuid pk
  name        text                -- "Default", "Kids", "Sports Only"
  device_uuid text                -- HDHomeRun device UUID exposed to Plex

lineup_channel
  lineup_id   uuid fk
  channel_id  uuid fk
  number      numeric(6,1)        -- can override channel.number per lineup
  position    int
```

Multiple lineups = multiple HDHomeRun devices visible to Plex. Useful for splitting a kids' Plex profile to a different lineup, or for testing.

---

## 4. Stream Management (the big one)

This is the part Dispatcharr gets wrong. Spec it out carefully.

### 4.1 Slot allocation algorithm

When a client requests a channel:

```
1.  Look up channel and its sources (ordered by priority, filtered by health).
2.  For each candidate source in order:
2a.   Find an active_stream already serving this source. If found and healthy,
       attach the client (refcount++, return shared TS pipe). DONE.
2b.   Otherwise, attempt to lease a slot:
       - SELECT credential FROM provider_credential
         WHERE provider_id = source.provider_id AND enabled
         ORDER BY (active_count / max_streams), priority
         FOR UPDATE SKIP LOCKED
         LIMIT 1
       - Verify (active_count < max_streams) under the lock.
       - INSERT active_stream with state=starting.
       - COMMIT.
2c.   Start upstream connection (HTTP GET, FFmpeg if needed).
2d.   On success: state=running. Attach client. DONE.
2e.   On failure: mark source health_score down, try next source.
3.  If all sources exhausted: 503 to client, log diagnostic.
```

**Key invariants:**
- Slot lease and `active_stream` insertion happen in one transaction. No insert without a lease.
- Lease release happens *only* when `active_stream.state` transitions to `dead` or refcount hits zero AND idle timeout passes.
- The DB is the source of truth, not in-memory state. A process restart should be able to reconstruct active leases by reading `active_stream`.

### 4.2 Refcount discipline

The reason Dispatcharr leaks slots: clients disconnect without cleanup, channel changes spawn a new stream before the old one tears down, and the bookkeeping desyncs. Fix this with three independent mechanisms:

1. **TCP close detection.** `stream_client.last_seen` updated on every chunk written. Goroutine detects write errors, decrements refcount immediately.
2. **Heartbeat watchdog.** Every 5s, scan `stream_client` rows. Any row with `last_seen > 30s ago` is force-closed. Refcount decremented.
3. **Orphan upstream sweeper.** Every 30s, scan `active_stream` rows. Any row with `client_count == 0 AND last_heartbeat > idle_timeout` (default 10s) is torn down: kill upstream, release slot, mark dead.

Idle timeout is short on purpose. Plex sometimes opens a probe stream and closes it; you want that slot back fast. If it reopens within a beat, the credential is right there.

### 4.3 Channel-change burst handling

Plex aggressively probes channels when a user surfs. Without care, this burns through slots.

**Pre-emptive draining.** When a client switches from channel A to channel B:
- The new B request sees the same `remote_addr` + `user_agent` already on A.
- Treat the A connection as draining (give it 3s grace, then close cleanly).
- Don't wait for A to fully drain before starting B; do them in parallel but cap parallel-per-client at 2.

**Upstream warmup pool.** Optional: keep N upstream connections warm to popular channels so channel changes are sub-second. Tunable per channel. Costs slots, so off by default.

### 4.4 Buffering and stability

The intermittent buffering you see is almost always upstream-side: packet loss, segment delay, or provider hiccups. Mitigations:

- **Ring buffer between upstream and client.** 8MB default, tunable per channel. Smooths jitter.
- **PCR/PTS-aware rewriting** for MPEG-TS to avoid timestamp discontinuities causing Plex to stall.
- **Segment prefetch** for HLS sources — pull N+1 segment while serving N.
- **Transparent reconnect** on upstream drop. Buffer holds the client while reconnect attempts. If reconnect succeeds within 3s, client never notices. If not, fail over to next source. If that also fails, send a "Source Unavailable" placeholder TS (a 5s loop with text).
- **Health scoring.** Each source has a sliding-window error rate. Sources with rate > threshold are demoted in priority for 5min cooldown, then re-tried.

### 4.5 FFmpeg vs. direct passthrough

Default to direct HTTP passthrough — copy bytes, don't transcode. Invoke FFmpeg only when:
- Source is HLS and client wants MPEG-TS (Plex strongly prefers TS for live).
- Source has audio sync issues (configurable per-source flag).
- User explicitly enables remuxing for a given source.

When FFmpeg is used, prefer `-c copy` (no re-encode), only `-c:a aac` if audio codec is incompatible. Keep one FFmpeg per upstream, fanout happens in Go after FFmpeg's stdout.

---

## 5. HDHomeRun Emulation

Plex's Live TV thinks it's talking to an HDHomeRun. Mimic the real device closely.

### 5.1 Required endpoints

| Endpoint | Method | Returns |
|---|---|---|
| `/discover.json` | GET | Device descriptor |
| `/lineup.json` | GET | Channel list with stream URLs |
| `/lineup.xml` | GET | Same as JSON, XML format (some clients prefer) |
| `/lineup_status.json` | GET | Scan status (always "Idle" for us) |
| `/device.xml` | GET | UPnP descriptor |
| `/auto/v<num>` | GET | Stream endpoint Plex will hit |

### 5.2 discover.json shape

```json
{
  "FriendlyName": "Conductor (Default)",
  "Manufacturer": "Conductor",
  "ModelNumber": "HDTC-2US",
  "FirmwareName": "hdhomeruntc_atsc",
  "TunerCount": 8,
  "FirmwareVersion": "20200225",
  "DeviceID": "<lineup.device_uuid>",
  "DeviceAuth": "conductor",
  "BaseURL": "http://<host>:<port>",
  "LineupURL": "http://<host>:<port>/lineup.json"
}
```

**TunerCount** is the lie that makes multi-credential work. Expose the *sum* of all enabled credentials' max_streams across all providers — that's how many concurrent streams the system can actually serve. Plex respects this number.

### 5.3 Per-lineup devices

Each `lineup` row gets a unique device UUID and is exposed at a different port (or path prefix). Plex sees them as separate tuners. This lets you give different Plex servers / profiles different channel sets without restarting anything.

### 5.4 SSDP / UPnP discovery

Optional but nice: implement SSDP responder so Plex auto-discovers the tuner instead of needing manual IP entry. Listen on `239.255.255.250:1900`. Respond to `M-SEARCH` with the device descriptor URL.

Disable by default in container deploys (multicast and Docker fight); enable behind a flag.

---

## 6. EPG Pipeline

Three stages: ingest, normalize, enrich.

### 6.1 Ingest

Sources, in order of trust:
1. **XMLTV from M3U providers** (most providers ship one alongside the playlist).
2. **Schedules Direct** — paid, $25/yr, gold standard for US/CA. Worth integrating.
3. **External XMLTV URLs** — user-supplied.

Fetch on schedule (default every 6h). An HTTP 200 is normalized in full before
publication, then replaces that source's mapped candidate generation in one
transaction. Rows absent from the successful snapshot are deleted; a parse,
mapping, or database failure leaves the prior generation intact. A conditional
304 leaves it unchanged. Channel remaps remove the old mapping's current/future
source rows and clear every source validator so the next pass must reprocess a
payload under the new mapping.

Sources retain independent candidates, including suppressed fallbacks. The
canonical resolver processes lower numeric priority first, then stable source
identity. Within one source it uses weighted interval selection: maximize total
covered duration, then programme count, then stable row identity. Lower-priority
rows are admitted only when their complete half-open interval fits a remaining
gap. Rows are never clipped or split: a partial overlap remains suppressed as a
whole, and any uncovered tail is reported as a real guide gap instead of being
published with fabricated title/episode boundaries. This policy is deterministic
under provider ordering changes and prevents one malformed overlapping row from
silently discarding a longer valid schedule.

Only `is_canonical` rows may feed XMLTV, DVR/indexer/reconcile, enrichment,
poster, horizon, or programme-count consumers. PostgreSQL enforces zero overlap
among those rows with a partial GiST exclusion constraint. `/xmltv.xml`,
`/epg.xml`, `/readyz`, and `/admin/epg/health` fail closed if an independent
overlap check ever finds that invariant broken.

### 6.2 Normalize

XMLTV is a wild format. Normalize aggressively:
- Strip HTML from descriptions.
- Normalize whitespace, unicode (NFC), curly quotes.
- Detect and split combined fields ("Show Name: Episode Title" → title + sub_title).
- Parse `episode-num` in all the variants providers emit.
- Detect movies via category, runtime > 80min + no episode num, or year-in-title.
- Pre-fill `is_live`, `is_new`, etc. from XMLTV flags where present.

### 6.3 Enrich

For each program:

**Movies:**
- Query TMDb by title + year. Take top match if `popularity > threshold` AND title fuzzy-match > 0.85.
- Pull poster, backdrop, overview, runtime, IMDb id, cast.
- Cache TMDb response by tmdb_id; refresh every 30 days.

**Series episodes:**
- Query TMDb TV by series title.
- Match episode by season+episode num if available, else by airdate.
- If TMDb fails, try TVDB.
- Pull series poster, episode still, episode overview.

**Sports:**
- Detect via category=Sports OR title contains team names from a maintained team list.
- Query TheSportsDB for the event by date + teams.
- Pull team logos, league, venue if available.
- Inject team logos into the program icon (composite if both teams).
- For ongoing series of games (NFL Sunday, NBA tip-off windows), pre-build a sports schedule cache for the week.

**News, talk shows, kids:**
- TMDb TV usually has these. Same pipeline as series.

**Manual override table.** For programs the matcher repeatedly gets wrong, allow user-set TMDb/TVDB ids that lock the match.

### 6.4 "Live" detection

XMLTV's current DTD has no `<live/>` element, and a July 2026 live-provider
audit confirmed Plex's custom XMLTV importer does not turn that extension into
its Live badge. Conductor retains `<live/>` for compatible consumers, using
only explicit upstream/event state or an unambiguous title marker. Category
alone is insufficient: sports replays, talk shows, news recordings, and idle
placeholders commonly carry Sports/News categories. Conductor projects a stable
`LIVE broadcast — ` descriptor for a verified scheduled-live airing without
changing the stored guide. It means **scheduled to air live**, including when
viewing a future guide slot; it is not a real-time playback or native Plex badge.
Event titles carry the descriptor. Recognized episode titles/subtitles have
provider-added live decorations removed consistently across all airings while
keeping underlying names and formal identifiers. Plex shares that metadata;
only the compatibility `<live/>` remains for eligible episodes, with
no native Plex badge promised. Explicit repeats, known earlier actual episode
airings, replay/encore annotations and idle/PPV placeholders suppress live claims.
Missing New evidence or a future repeat alone does not prove a rerun. A past
event airing retains its broadcast description.

The descriptor must not depend on when Plex fetches XMLTV. Plex imports and
caches a whole schedule, so changing an ETag at kickoff cannot update a title
that Plex fetched earlier without another import. The former fetch-time-only
`LIVE — ` cue missed seven current live airings in a September 2026 readback.
See [current evidence and post-deploy checks](../epg-live-new.md).

### 6.5 "New" detection

For recognized episodes, Plex uses the absence of `<previously-shown>` as its
effective New signal; `<new/>` itself is not required by the observed importer.
Logic:
- If `original_air_date` is within 7 days of `start_at`: NEW.
- If an earlier canonical airing of the same normalized series plus episode
  identity exists on any channel: NOT NEW.
- If XMLTV says `<previously-shown>` explicitly: NOT NEW.
- If we have neither airdate nor history: conservatively emit
  `<previously-shown/>`; otherwise Plex may infer New for an episode-shaped row.

Keep durable history by normalized series plus formal/provider episode
identity. Only canonical rows contribute. A future row is not committed as an
airing until its start arrives, so a provider move/deletion before airtime can
promote the next scheduled airing. Explicit repeat evidence is durable even
when its historical timestamp is unknown. Original-air-date inference compares
the source timestamp's persisted civil date, accepts only `0..7` days, and
rejects future dates. A legacy or manual row whose source civil date is unknown
may expose an original-air date only when it is strictly earlier than the UTC
start date; equality is ambiguous for late-evening negative-offset airings and
therefore fails closed. Migration 0025 clears XMLTV `ETag`/`Last-Modified`
validators and Schedules Direct station MD5 watermarks once so pre-migration
rows are republished with this provenance.

### 6.6 "Premiere" / "Finale"

XMLTV defines `<premiere>`, which Conductor passes through when supplied.
`<finale>` is not part of the standard XMLTV DTD; Conductor parses and emits it
only as a compatibility extension for clients that understand it. Premiere and
finale remain independent of New/repeat state, and Conductor does not claim
that Plex will display the non-standard finale extension.

---

## 7. XMLTV Output for Plex (the gotchas)

This is where most tools fall short. Plex parses XMLTV strictly and quietly drops fields it doesn't recognize.

### 7.1 Channel block

```xml
<channel id="ch.espn.us">
  <display-name>ESPN</display-name>
  <display-name>206</display-name>
  <icon src="https://conductor.local/logos/espn.png"/>
</channel>
```

- `id` must be stable across runs. Plex hashes it for matching to existing recordings/watch history.
- Two `<display-name>` elements: one name, one channel number. Plex uses both.
- `<icon src>` must be a fully-qualified URL Plex can fetch directly. Plex caches aggressively — if you change the logo, the URL must change too (cache-bust with `?v=hash`).

### 7.2 Programme block (movies)

```xml
<programme channel="ch.amc.us" start="20260506200000 -0600" stop="20260506223000 -0600">
  <title>Heat</title>
  <desc>A career thief plans one last big score before retirement.</desc>
  <date>1995</date>
  <category>Movie</category>
  <category>Action</category>
  <category>Crime</category>
  <icon src="https://image.tmdb.org/t/p/w500/.../poster.jpg"/>
  <rating system="MPAA"><value>R</value></rating>
  <length units="minutes">170</length>
  <previously-shown/>
</programme>
```

- Include `<date>` for the year — Plex uses this with title for matching.
- `<category>Movie</category>` triggers Plex's movie classification.
- Multiple `<category>` for genre, in addition to Movie.

### 7.3 Programme block (episodes)

```xml
<programme channel="ch.fx.us" start="..." stop="...">
  <title>The Bear</title>
  <sub-title>System</sub-title>
  <desc>Carmy struggles with the new restaurant's chaotic launch.</desc>
  <category>Series</category>
  <category>Drama</category>
  <episode-num system="xmltv_ns">2.6.</episode-num>
  <episode-num system="onscreen">S03E07</episode-num>
  <icon src="https://image.tmdb.org/t/p/w500/.../still.jpg"/>
  <new/>
</programme>
```

- Both `xmltv_ns` and `onscreen` episode-num formats. Plex prefers `onscreen` for display, `xmltv_ns` for matching.
- Episode poster (`<icon>`) is the episode still, not the series poster, when available.
- `<new/>` is empty self-closing.
- `<previously-shown/>` without a start = "this aired before, don't know when".

### 7.4 Programme block (sports)

```xml
<programme channel="ch.espn.us" start="..." stop="...">
  <title>Denver Broncos at Kansas City Chiefs</title>
  <sub-title>NFL Football</sub-title>
  <desc>Week 9 AFC West matchup at Arrowhead Stadium.</desc>
  <category>Sports</category>
  <category>Football</category>
  <icon src="https://conductor.local/cache/sports/broncos-vs-chiefs.png"/>
  <live/>
  <previously-shown/>
</programme>
```

- `<live/>` is a non-standard compatibility extension; Plex custom XMLTV
  currently ignores it for the Live badge.
- Live-broadcast state is independent from first-run/repeat state. Do not mark
  every sports airing New; emit `<previously-shown/>` unless the source
  independently establishes that the airing is new.
- Composite team-logo image cached server-side.

### 7.5 Output mechanics

- Serve at `/xmltv.xml` and `/epg.xml`.
- Set an `ETag` from the complete emitted guide model so artwork, channel edits,
  deletions, horizon changes, and source live/repeat corrections all invalidate
  Plex's cache. Keep `Last-Modified` as database-revision telemetry for legacy
  clients, but do not use it as a conditional validator for this time-relative
  representation.
- Support `If-None-Match` as the sole authority for `304 Not Modified`.
  Deliberately ignore `If-Modified-Since`: the guide can change as rows cross
  the moving horizon without a database revision.
- Gzip the response.

---

## 8. Frontend

Pages, in priority order:

1. **Active Streams dashboard.** Real-time view of every active_stream and its clients. Provider slot usage bars per credential. Kill button per stream. Filterable by channel, provider, client. *This is the page you'll live on when something goes wrong.*
2. **Channel manager.** List, drag-to-reorder, edit, bulk import from M3U.
3. **EPG matcher.** Programs flagged `enrichment_status = failed` shown with TMDb search to manually resolve.
4. **Provider / credential manager.** Add provider, attach credentials with max_streams, test connection.
5. **Lineup builder.** Compose lineups from channels for different consumers.
6. **Logs viewer.** Tail of structured logs, filterable.
7. **Diagnostics.** "Test this stream" button that does a 10s pull and reports bitrate, codec, errors. "Validate XMLTV" that checks a programme against Plex parsing rules.

Use Server-Sent Events for the dashboard live updates — WebSocket overkill for unidirectional data.

---

## 9. Migration from Dispatcharr

A one-shot importer that reads Dispatcharr's database (it's Postgres-based) and produces:
- providers + credentials (split if Dispatcharr had them merged)
- channels with sources
- channel groups → group_tag
- EPG mappings → channel.epg_channel_id
- M3U URLs → handled as fresh imports

Provide a dry-run mode that prints what would be created. Provide a rollback flag that can undo an import in case Plex re-indexes mid-migration.

Document an "either-or" deploy: run Conductor on a different port, point one Plex tuner at it for testing, swap when stable.

---

## 10. Phased Roadmap

**Phase 0 — Skeleton (1–2 weeks of focused work)**
- Repo scaffold, CI, Docker build.
- Postgres schema + migrations.
- HTTP server with stub HDHomeRun endpoints serving a hardcoded test channel.
- Verify Plex sees the device and can play the test channel.

**Phase 1 — Stream pool (2–3 weeks)**
- Provider/credential model + slot leasing logic.
- active_stream / stream_client tables.
- Direct passthrough proxy with refcount and watchdogs.
- Multi-credential allocation. *This is the pain point fix; get this right.*

**Phase 2 — Basic EPG (1–2 weeks)**
- XMLTV ingestion + normalization.
- Output XMLTV with the Plex-correct tags.
- Verify Plex shows the guide, classifies movies vs episodes correctly.

**Phase 3 — Enrichment (2 weeks)**
- TMDb integration (movies + series).
- Poster URLs in XMLTV output.
- Manual override UI.

**Phase 4 — Live/New/Sports (1–2 weeks)**
- Live detection logic + `<live/>` output.
- New detection with episode history.
- TheSportsDB integration for sports.
- Composite team logos.

**Phase 5 — Stability (1–2 weeks)**
- Ring buffer for jitter smoothing.
- Failover between sources on health drop.
- Reconnect with buffer hold.
- Source Unavailable placeholder.

**Phase 6 — Frontend polish + monitoring (2 weeks)**
- Active streams dashboard with SSE.
- Diagnostics tools.
- Prometheus `/metrics` endpoint.
- Migration importer.

Total: 11–15 weeks of focused part-time work. Phases 1, 4, and 5 are the value drivers — phases 0 and 2 are setup that you'll resent if rushed but won't see direct payoff from.

---

## 11. Operational concerns

- **Single-binary deploy** with `docker compose` shipping the binary + Postgres.
- **Healthchecks**: `/healthz` (process up) and `/readyz` (DB reachable, EPG fresh, at least one provider healthy).
- **Prometheus metrics**:
  - `conductor_active_streams{channel,provider,credential}`
  - `conductor_credential_slots_used{credential}` / `_total{credential}`
  - `conductor_stream_errors_total{kind}`
  - `conductor_epg_programs_total{enrichment_status}`
  - `conductor_upstream_bytes_total{provider}`
- **Structured logs** (JSON) with stream_id correlation across upstream/client/error events.
- **Backup**: pg_dump on schedule; export channels + lineups as YAML so config is reproducible from version control.
- **Resource bounds**: cap total memory by tuning ring buffer sizes × max active streams. Document the math.

---

## 12. Open questions worth resolving early

1. **Go vs Python.** Pick before Phase 0. My bias is Go for the streaming hot path.
2. **External Postgres vs embedded.** I'd default to external Postgres in compose, since you're already running infra-savvy.
3. **Schedules Direct integration in v1?** Their API is solid and the EPG quality leap is significant. I'd say yes, behind a feature flag.
4. **Tunarr interop.** Worth a stretch goal: expose a "virtual provider" endpoint Tunarr can pull from, so Tunarr can use Conductor's slot management instead of going direct.
5. **Authentication.** Single-tenant home-lab, but consider a simple bearer token on the API for when this is exposed over Tailscale to other devices.

---

## 13. Notes on what NOT to copy from Dispatcharr

- **Don't model credentials as a property of the provider connection at runtime.** Make credentials a first-class entity with their own slot budgets, and resolve them at request time.
- **Don't trust client TCP-close as the only signal for slot release.** Belt-and-suspenders: refcount, heartbeat, sweeper.
- **Don't emit XMLTV with vendor-specific extensions.** Stick to the Plex-respected tag set; document any extension behind a flag.
- **Don't bundle stream proxy state in process memory only.** Persist active_stream in Postgres so a process restart is recoverable.
- **Don't rely on naming conventions for matching.** Use stable IDs (channel.id, epg_channel_id) and let users edit display names freely.

---

## 14. Reference: Plex Live TV behavior worth knowing

- Plex hits `lineup.json` once at setup, then on a manual rescan. Channel changes mid-stream do *not* re-hit lineup.
- Plex hits the XMLTV URL every ~12 hours by default. The complete projected
  content ETag is the sole authority for `304 Not Modified`. Last-Modified is
  retained as database-revision telemetry for legacy consumers, but
  If-Modified-Since alone always receives the current representation: the
  moving guide horizon can change without a
  database revision.
- Plex builds a local cache of channel logos. To force-refresh, change the icon URL (cache-bust query param).
- Plex's EPG match priority: IMDb id > TMDb id > exact title + year > fuzzy title. Provide IDs in `<icon>` — wait, no — Plex doesn't read IDs from XMLTV directly. Match accuracy comes from clean titles + year/episode-num.
- Plex's custom XMLTV provider derives New for recognized episodes primarily
  from the absence of `<previously-shown>`; it does not expose `<live/>` as a
  Live badge.
- Plex does *not* respect non-standard XMLTV extensions. If it's not in the XMLTV DTD, assume Plex will drop it.

---

## 15. Suggested module/file layout (Go)

```
cmd/
  conductor/                main.go
internal/
  api/                      HTTP handlers, OpenAPI spec
  hdhr/                     HDHomeRun emulation endpoints
  stream/
    pool.go                 slot allocator
    proxy.go                upstream→client copy
    buffer.go               ring buffer
    watchdog.go             heartbeat + sweeper
  epg/
    ingest.go               XMLTV fetch + parse
    normalize.go            cleaning
    output.go               XMLTV emit for Plex
  enrich/
    tmdb.go
    tvdb.go
    sportsdb.go
    matcher.go              fuzzy matching, confidence scoring
  store/                    Postgres queries (sqlc-generated)
  ssdp/                     UPnP discovery (optional)
web/                        React app
deploy/
  docker-compose.yml
  Dockerfile
migrations/                 SQL migration files
```

For Python, swap to `app/`, `app/api/`, etc., but the boundaries are the same.

---

## 16. Acceptance criteria (how you know it's done)

For each phase, write the acceptance test up front. Examples:

- **Phase 1:** Run 12 simultaneous channel changes across 3 credentials with max_streams=4 each. After 60s of churn, slot count in DB equals slot count Plex sees, and no upstream connection is orphaned. (Verify with provider's web portal showing actual session count.)
- **Phase 2:** Plex shows the correct program at the correct time for 24h continuous, on a movie channel and an episodic channel.
- **Phase 4:** During an NFL Sunday, every game window shows LIVE badge, team posters, and correct teams. NEW badge present, no false NEW on reruns.
- **Phase 5:** Pull the network cable on a provider for 5s. Stream resumes without Plex showing an error. Pull for 30s. Failover source kicks in within 5s of the third failure.

---

*End of spec.*
