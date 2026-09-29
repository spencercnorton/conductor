# Changelog

All notable changes to Conductor are recorded here. Versions follow
[semantic versioning](https://semver.org/); each release is a tag on `main`.

## Unreleased

- A restarted transcode, a backup source or the recovery slate can rejoin a
  stream whose delivered audio ended behind its video. An attempt cut off
  mid-interleave, or stopped by the output A/V drift guard, used to leave the
  output clock refusing every aligned epoch after it until the 90 s reconnect
  budget ran out, so the viewer or recording failed. The join now leaves that
  delivered deficit as an audio hole, with video continuing on its grid. Only
  an epoch whose own audio starts with its video gets this credit.

## 0.60.1 — 2026-09-27

- Establish GitHub pull requests as the development workflow, with privacy checks.
- Add deployment, configuration, security, upgrade and recovery documentation.
- Require quickstart database, encryption and administration secrets and bind
  to loopback initially; provider traffic may use an explicitly configured proxy.

## v0.60.0

- `CONDUCTOR_UPSTREAM_PROXY` sends provider traffic — the panel request, the
  origin it redirects to, and the ppvsync catalogue read — through an HTTP
  proxy, so all of it leaves from one IP. Everything else (Plex, the arrs,
  the ops bot, EPG sources) stays direct. Empty keeps the direct path.

## v0.59.0

- `CONDUCTOR_LOGO_BASE_URL` moves channel logos in the XMLTV guide onto a
  separate origin, so they can be served over HTTPS. The redesigned Plex apps
  load guide images themselves and will not load plain-HTTP LAN URLs, which
  left every self-hosted logo blank. Stream and lineup URLs stay on
  `CONDUCTOR_BASE_URL`. Logos stored as absolute URLs on the base URL are
  rebased too. Changing the setting changes the guide's ETag, so Plex picks
  it up on its next fetch.

## v0.58.3 — first public release

Conductor's first published release. The project has been running in
production since early 2026; this is the point at which the tree became
public, so everything below is what the first public tag contains rather than
a list of changes against an earlier public version.

- HDHomeRun emulator: discovery, lineup and tuner endpoints that Plex pairs
  with as a network tuner.
- Credential-aware stream pool: per-credential slot budgets, transactional
  slot leasing, refcounted upstream sharing, and TCP-close, heartbeat and
  idle-sweep watchdogs for orphaned streams.
- XMLTV guide output with `xmltv_ns` and `onscreen` episode numbers,
  IMDb/TMDb/TVDB ids, repeat and premiere state, self-hosted logos and
  artwork.
- Optional enrichment from TMDb, TVDB, TheSportsDB and Schedules Direct.
- Pay-per-view slot handling: the current tile advertises the next event and
  its local kickoff, and a catalogue gap retains guide presence.
- DVR recording with passthrough (no transcode) and an optional hand-off to
  an external commercial-detection service before import.
- Admin API with Bearer-key and SSO-principal authentication, and AES-GCM
  encryption of provider credentials at rest.
