# EPG episode numbers — making the guide a reliable indexer source

**Status:** Phase 1 shipped (SD episode extraction). Phases 2–3 open.
**Last updated:** 2026-06-30.

## The problem

Three Conductor consumers all depend on one field — `epg_program.episode_num_onscreen`
(canonical `SxxExx`) — and all three are starved because the active EPG sources
don't carry it:

| Consumer | Where | What breaks without S/E |
|---|---|---|
| Torznab indexer | `internal/dvr/torznab.go` | Can't emit `season`/`episode` attrs → Sonarr/Radarr won't import grabs. The original reason the indexer "records nothing" (`internal/reconcile/types.go` package doc). |
| Reconciler recordings | `internal/reconcile/worker.go` | Output path falls back to a timestamp filename (`The_Bear.20260630T101234.ts`) that the *arr won't match to a library episode. |
| Plex gap recorder | `internal/reconcile/match.go` `MatchPlexGaps` | Correctly emits nothing — a gap candidate now *requires* a real `SxxExx` (v0.29.5), so on the current EPG it is honestly near-empty. |

### Why the data is missing

The active EPG sources are six **community XMLTV guides** (`iBoost provider`,
`iptv-epg US/GB`, `EPGTalk-US`, `epg.pw US`; Dispatcharr disabled). Measured on
the live DB (next 48h, ~14.3K programs): **30 carry an onscreen/xmltv episode
number (0.2%)**. `sub_title` is present on ~8% but is frequently a *franchise
qualifier* ("Law & Order" → "Special Victims Unit", "Impractical Jokers" →
"Inside Jokes"), not an episode title — so it cannot stand in for episode
identity.

The enrichment pipeline (`internal/enrich`) resolves series/movie identity
(`tmdb_id`/`tvdb_id`) + artwork, but `epg_program_enrichment` has **no
season/episode columns** — enrichment never produced episode numbers.

## Two sources of truth (both partially built)

### Path A — Schedules Direct (recommended primary)

Schedules Direct ($25/yr; `internal/sd` package doc) is the gold-standard US/CA
broadcast guide and carries season/episode **intrinsically per program**, on
every airing including reruns. Conductor already has: the full SD client
(token/lineups/schedules/programs), the `sd_credentials`/`sd_lineup`/
`sd_station_state` tables, and an ingest worker that upserts to `epg_program`
(`internal/sd/worker.go`).

**Decisive property:** SD fixes the *rerun* case (a syndicated "South Park"
airing gets its real `SxxExx`), which dominates this library and which nothing
air-date-based can fix.

### Path B — TVDB episode enrichment (supplement)

Resolve S/E inside `internal/enrich` from the already-resolved `tvdb_id` +
`original_air_date` (or episode title). ~50% built: enrichment worker, a TVDB
v4 client with `SearchSeries`/`SearchMovie`, and the `tvdb_id` crosswalk exist;
missing are episode-listing lookups, episode columns on
`epg_program_enrichment`, and a filled `CONDUCTOR_TVDB_API_KEY`.

**Limitation:** air-date resolution needs the EPG row to *have* an original air
date — which these feeds lack on exactly the reruns we care about. So TVDB only
helps genuinely-new episodes (which Sonarr's wanted path already covers). Good
as a **fallback for channels SD can't map** (e.g. international/IPTV-only), not
as the primary.

## Plan

### Phase 1 — SD episode extraction ✅ (v0.30.0)

`ProgramDetails.EpisodeSeasonNum()` reads season/episode from SD's `metadata`
array (Gracenote preferred). The worker converts it through the existing XMLTV
normaliser (`epg.ParseEpisodeNum`) so both `episode_num_onscreen` and
`episode_num_xmltv` are populated identically to the XMLTV path. Dormant until
SD is enabled — safe to ship.

### Phase 2 — Enable SD (operational + one design decision)

1. **Subscribe** to Schedules Direct; set `CONDUCTOR_SD_USERNAME` /
   `CONDUCTOR_SD_PASSWORD` (rows land in `sd_credentials`).
2. **Add a lineup** matching the owned-show channels (US cable/OTA).
3. **Map channels:** set each Conductor `channel.epg_channel_id` to the SD
   `stationID`. Unmapped stations are skipped and counted
   (`IngestStats.UnmappedStations`) — use that to find the gaps. This is the
   bulk of the operational effort.
4. **⚠️ Source-priority decision (blocker):** the upsert keeps a row only when
   `EXCLUDED.source_priority <= epg_program.source_priority` (lower number =
   higher priority). Today `PrioritySD = 10` while the XMLTV feeds are priority
   **1–9**, so **XMLTV outranks SD** — on any channel covered by both, XMLTV's
   empty episode fields would overwrite SD's good ones. Options:
   - **(a)** Give SD the highest priority (lowest number) for mapped channels —
     simplest, but SD then wins on *all* fields (title/desc), which is usually
     fine since SD data is cleaner.
   - **(b)** Field-level merge: never overwrite a non-empty
     `episode_num_onscreen` with an empty one, regardless of source priority.
     More surgical; preserves whichever source has the best of each field.

   Recommend **(b)** if we want SD purely as an episode-number backfill over
   the existing XMLTV titles; **(a)** if SD becomes the authoritative guide for
   its mapped channels.

### Phase 3 — TVDB fallback enrichment (optional)

For channels SD can't map: add `season`/`episode` columns to
`epg_program_enrichment`, add a TVDB episode-by-air-date/title lookup to
`internal/enrich/tvdb.go`, populate during enrichment, and have consumers read
the enrichment S/E when the `epg_program` row lacks it. Gated on
`CONDUCTOR_TVDB_API_KEY`.

## Verification

After Phase 2, watch episode-number coverage climb:

```sql
SELECT count(*) total,
       count(*) FILTER (WHERE episode_num_onscreen <> '') AS has_se
FROM epg_program WHERE start_at > now() AND start_at < now() + interval '48 hours';
```

Then the reconciler gap preview and the Torznab indexer should both begin
emitting real `SxxExx`, and recordings should land in `Season NN/…SxxExx.ts`
paths the *arr import cleanly.
