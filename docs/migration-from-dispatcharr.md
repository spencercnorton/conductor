# Migrating from Dispatcharr to Conductor

This is the playbook for swapping Dispatcharr out from under Plex Live TV without losing channels, scheduled recordings, or guide data. Tested 2026-05-07 against a running Dispatcharr (port 9195) and a containerised Conductor deployment.

The migration runs in **coexistence mode** by default — Conductor sits alongside Dispatcharr as a second tuner under the same Plex DVR, sharing Dispatcharr's XMLTV guide. Once Conductor proves stable for whatever window the operator wants, Dispatcharr's tuner devices are disabled in Plex and the Dispatcharr stack is decommissioned.

## What gets migrated

| From Dispatcharr | To Conductor | Notes |
|---|---|---|
| `m3u_account` (provider) | `provider` + `provider_credential` | One credential per Dispatcharr account; `max_streams` + priority preserved |
| `channel` | `channel` | Idempotent on `channel_number` |
| `streams[]` per channel | `channel_source` | One source per upstream URL, priority = stream order (failover) |
| `channel_group` (1031 of them in the source DB) | `channel.group_tag` | Just a string label |
| `logo_url` | `channel.logo_url` | Resolved through Dispatcharr's `/api/channels/logos/{id}` cache |
| `tvg_id` | `channel.epg_channel_id` | Used for XMLTV ingest matching |

**Not migrated** (these stay where they are):

- **EPG/XMLTV programmes**: Plex DVR already tracks Dispatcharr's XMLTV at `http://localhost:9195/output/epg/D1`. When Conductor joins the same DVR, both tuners share that guide. No re-ingest needed.
- **Recording schedules**: Plex DVR owns these, not Dispatcharr. They keep working.
- **Channel profiles** (Dispatcharr's lineup variants): Conductor uses lineups instead; create them via `POST /admin/lineups` if you want per-Plex-server lineups.

## Prereqs

- Conductor running and reachable (see the README install section)
- Conductor admin API key (`CONDUCTOR_ADMIN_API_KEY` from your secret store)
- Dispatcharr API reachable at `http://dispatcharr.local:9195` with operator credentials
- Conductor's CONDUCTOR_TUNER_COUNT (or the live-derived value from sum of credential `max_streams`) ≥ what your Plex clients concurrently need

## 1. Run the importer

```bash
cd /path/to/conductor
DISPATCH_USER='<dispatcharr operator>' \
DISPATCH_PASS='<from credentials index>' \
CONDUCTOR_ADMIN_KEY='<conductor admin api key>' \
python3 scripts/dispatcharr_to_conductor.py
```

Idempotent — re-running matches existing providers by name and channels by number. Add new Dispatcharr channels and re-run; nothing breaks.

Expected output (2026-05-07 run):

```
━━━ providers ━━━
  [+] created provider 'iBoostTV' kind=m3u_xtream
    [+] created credential 'acc01' max_streams=1
  [+] created provider 'iBoost DUO' kind=m3u_xtream
    [+] created credential 'acc02' max_streams=1
  → 2 providers + 2 credentials mapped

━━━ channels ━━━
  ... 142 created, 285 sources
Result: created=142 skipped=0 failed=16 total_sources=285
```

The "failed=16" rows are placeholder Dispatcharr channels with no streams attached (e.g. `B1G PPV 1-16`). They show up in Dispatcharr's admin UI as empty slots and get skipped here.

## 2. Verify Conductor's lineup matches

```bash
curl -sS http://conductor.local:8409/lineup.json | jq 'length'
# 142

curl -sS http://conductor.local:8409/readyz | jq '.tuner_count'
# 2  (sum of credential max_streams)

# Tune a real channel — should return MPEG-TS bytes
curl -sS --max-time 4 -o /tmp/test.bin -w "%{http_code} %{size_download}b\n" \
  http://conductor.local:8409/auto/v2
# 200 ~5500000b   (ABC Denver, ~1.4MB/s = 11Mbps full HD)
```

If the byte count is non-trivial and HTTP 200, the slot leasing path is healthy.

## 3. Add Conductor to Plex (coexistence mode)

Plex's DVR-device-add API is undocumented; this is a UI step.

1. Open Plex Web at `https://app.plex.tv` (or `https://plex.local:32400/web` for direct).
2. **Settings → Live TV & DVR → DVR (Dispatcharr Guide) → Devices → Add Device**.
3. Choose **HDHomeRun** → **Don't see your tuner? Connect manually** → enter `conductor.local:8409`.
4. Plex hits Conductor's `/discover.json` and `/lineup.json`. You should see Conductor's 142 channels.
5. Channel numbers match Dispatcharr exactly — Plex dedupes by (lineup, channel number) so the existing program guide will populate Conductor's channels automatically.
6. Save. Conductor is now a second tuner under the same DVR.

Plex's tuner-allocator will pick whichever tuner has free slots when a viewer tunes a channel, with no operator intervention. Behind the scenes:

- Plex tunes `/auto/v<ch>` on Conductor (or Dispatcharr's port 9195).
- Conductor's slot allocator (Phase 1 spec §4.1) leases a credential atomically and serves the upstream Xtream/M3U URL.
- When the viewer disconnects, Conductor's orphan sweeper reclaims the slot within 30s.

## 4. Burn-in window

Watch a few channels through Conductor for a day. Plex's tuner-allocation is round-robin so you'll naturally hit both Dispatcharr and Conductor backends.

Useful commands during the burn-in:

```bash
# What's actively tuning right now?
curl -sS -H "Authorization: Bearer $CONDUCTOR_ADMIN_KEY" \
  http://conductor.local:8409/admin/streams | jq

# Recent Conductor logs
# docker logs conductor

# Concurrent-tune correctness check:
# Open the same channel from N Plex clients; only sum(max_streams) tuners
# should connect, the rest should get HTTP 503 (slot exhausted).
# Dispatcharr's known-broken behavior is to silently double-book.
```

## 5. Switch over (Dispatcharr off, Conductor only)

When ready:

1. **In Plex**: Settings → Live TV & DVR → DVR → Devices → click each Dispatcharr device → **Disable** (don't delete; Plex preserves recording history that way).
2. **Update DVR lineup** to point at Conductor's XMLTV (when Conductor has its own EPG ingested — see `docs/enhancements.md` §12 for Schedules Direct setup, or just let Conductor pull Dispatcharr's XMLTV via `POST /admin/epg/sources` while Dispatcharr's still up).
3. Wait 24h. Verify no scheduled recordings broke.
4. **Stop the Dispatcharr container or stack** (don't delete yet — keep its `/data` volume around for a week as a rollback option).
5. After a clean week, archive the volume + delete the stack.

## Rollback

If anything goes wrong:

1. Re-enable the Dispatcharr devices in Plex DVR.
2. Restart Dispatcharr (it was never deleted).
3. Disable the Conductor device in Plex.

The Conductor channel and credential data stays in Postgres — re-enabling it later is a one-click operation (re-enable the device in Plex DVR).

## Re-running the importer

The importer is idempotent — safe to re-run after Dispatcharr changes:

```bash
python3 scripts/dispatcharr_to_conductor.py
```

What re-running does:
- Existing providers (matched by name): **skipped** (no update; rare to need)
- Existing credentials (matched by username): skipped
- Existing channels (matched by `channel_number`): **skipped** (manual edits to Conductor channels are preserved)
- New Dispatcharr channels: created with their sources

To force a re-import of a single channel: delete it from Conductor first
(`DELETE /admin/channels/{id}` — *not yet implemented in Phase 4; for now,
delete from Postgres directly via psql in `conductor-pg`*).

## Known gaps / Phase 4c follow-ups

- **Channel groups not deeply migrated**: Dispatcharr has 1031 groups; we map each channel's primary group as a string in `channel.group_tag`. No group hierarchy or ordering preserved. Phase 4c could add a proper `channel_group` table + group-aware lineups.
- **Channel profiles not migrated**: Dispatcharr's "channel profiles" (lineup variants per Plex server) don't map yet — Conductor has its own `lineup` table that does the same job, but the importer doesn't auto-create lineup rows.
- **Logos**: Dispatcharr serves logos through its own cache (`/api/channels/logos/<id>/cache/`). The importer copies these URLs verbatim — meaning Conductor's `<icon>` URLs in XMLTV point back at Dispatcharr's cache. After Dispatcharr is decommissioned, Conductor would need to host these logos itself. Phase 4c: add a logo migration step that downloads + re-hosts under Conductor.
- **Recording schedules**: Plex DVR owns these, not Dispatcharr or Conductor — no migration needed, but worth noting that scheduled recordings will auto-pick whichever tuner is available.
