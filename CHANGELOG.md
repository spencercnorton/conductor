# Changelog

All notable changes to Conductor are recorded here. Versions follow
[semantic versioning](https://semver.org/); each release is a tag on `main`.

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
