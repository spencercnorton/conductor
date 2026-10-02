<h1 align="center">Conductor</h1>

<p align="center">
  <strong>A drop-in HDHomeRun tuner for Plex Live TV, backed by your own IPTV credentials.</strong><br>
  One Go service that pools provider slots, enriches the guide, and speaks the protocols Plex already trusts.
</p>

<p align="center">
  <a href="https://norvitech.com"><img alt="NorviTech Suite" src="https://img.shields.io/badge/NorviTech-Suite-FD8024.svg"></a>
  <a href="https://github.com/spencercnorton/conductor/tags"><img alt="Latest release" src="https://img.shields.io/github/v/tag/spencercnorton/conductor?label=release&sort=semver"></a>
  <a href="#install"><img alt="Docker Compose" src="https://img.shields.io/badge/install-docker%20compose-2496ed.svg?logo=docker&logoColor=white"></a>
  <a href="LICENSE"><img alt="Licence" src="https://img.shields.io/badge/licence-MIT-blue.svg"></a>
  <a href="https://buy.stripe.com/8x26oH2U44f65TRe574wM04"><img alt="Donate" src="https://img.shields.io/badge/donate-Stripe-635bff.svg?logo=stripe&logoColor=white"></a>
</p>

Conductor presents itself to Plex as a SiliconDust HDHomeRun tuner and serves
live channels from the IPTV subscriptions you already pay for. It is for
self-hosters who want Plex Live TV and DVR to behave like real hardware:
correct tuner counts, a guide with artwork, and recordings that land where
Plex expects them. It runs as a single Go binary next to Plex, with Postgres
for state.

## What it does

**Stream slots are accounted for, not guessed.** Credentials are first-class
entities with their own slot budgets, so one process replaces "run one
instance per credential set". The slot lease and the active-stream row are
written in the same transaction, and three independent watchdogs — TCP close,
heartbeat and idle sweep — reclaim orphans.

**The guide is rich enough for Plex to look good.** XMLTV output carries
episode numbers in both `xmltv_ns` and `onscreen` form, IMDb/TMDb/TVDB ids,
repeat and premiere state, self-hosted channel logos and artwork. Optional
TMDb, TVDB and TheSportsDB enrichment fills in posters, backdrops and episode
stills; Schedules Direct is supported and optional.

**Pay-per-view slots advertise the event, not the slot.** A dynamically
assigned PPV channel shows the exact next event and local kickoff in the
current Plex tile, and a temporary catalogue gap keeps guide presence instead
of vanishing from the lineup.

**The HDHomeRun surface is faithful.** Discovery, lineup and tuner endpoints
return what real SiliconDust firmware returns, so Plex pairs with it as a
network tuner with no special configuration.

**Recording is Plex's job, post-processing can be yours.** Conductor passes
streams through without transcoding and can hand a finished recording to an
external commercial-detection service before Plex imports it.

## Install

### Docker — Docker Compose

```bash
git clone https://github.com/spencercnorton/conductor.git
cd conductor
cp .env.example .env
openssl rand -hex 32          # generate each of POSTGRES_PASSWORD, CONDUCTOR_CRED_KEY and CONDUCTOR_ADMIN_API_KEY separately
docker compose -f docker-compose.example.yml up --build -d
```

Then in Plex: **Live TV & DVR → Set up Plex DVR → enter its address manually**
and point it at `http://<host>:8409`. For Plex on another host, configure CONDUCTOR_BIND_ADDRESS and CONDUCTOR_BASE_URL as described in the operations guide. There is no published image yet; the
compose file builds from this tree.

### Other platforms — from source

Go 1.25 or newer and a Postgres 16+ database:

```bash
go build ./cmd/conductor
CONDUCTOR_POSTGRES_DSN=postgres://... ./conductor
```

Running with no database serves the HDHomeRun emulator with one stub channel,
which is enough to prove Plex pairing.

## Documentation

- [Deployment and operations guide](docs/OPERATIONS.md) — setup, configuration, verification, upgrades, recovery and troubleshooting.
- [Releasing](docs/RELEASING.md) — public builds, release checks and private deployment boundaries.

- [`docs/migration-from-dispatcharr.md`](docs/migration-from-dispatcharr.md) — moving an existing Dispatcharr setup across.
- [`docs/epg-episode-numbers.md`](docs/epg-episode-numbers.md) — how episode numbers are recovered and emitted.
- [`docs/epg-live-new.md`](docs/epg-live-new.md) — live, new and repeat state as Plex reads it.
- [`docs/ppv-idle-guide.md`](docs/ppv-idle-guide.md) — what a pay-per-view slot shows between events.
- [`docs/source-diversity.md`](docs/source-diversity.md) — spreading a channel across providers.
- [`docs/dead-source-block.md`](docs/dead-source-block.md) — detecting a provider block that has gone dead.
- [`docs/spec/dispatcharr-replacement-spec.md`](docs/spec/dispatcharr-replacement-spec.md) — the full technical spec.
- [`CHANGELOG.md`](CHANGELOG.md) — what changed in each release.

## Where your data lives

| Path | Purpose |
|---|---|
| Postgres database | channels, providers, credentials (encrypted), guide data, recordings metadata |
| `/data/logos` | channel logos served to Plex |
| `/data/recordings` | DVR output, if you use Conductor's recorder |
| `/data/diag` | stream diagnostics, when explicitly enabled |

Conductor talks to your IPTV provider, to Plex, and — only when you configure
them — to the metadata services you name. Nothing else leaves the machine and
there is no telemetry.

## Contributing and support

- Bugs and feature requests: [open an issue](https://github.com/spencercnorton/conductor/issues/new/choose). Questions: [Discussions](https://github.com/spencercnorton/conductor/discussions).
- Security reports: [private vulnerability reporting](https://github.com/spencercnorton/conductor/security/advisories/new) — see [SECURITY.md](SECURITY.md). There is no e-mail address; that is deliberate.
- Pull requests are welcome; read [CONTRIBUTING.md](CONTRIBUTING.md) first — changes are reviewed and merged on GitHub, then shipped in tagged releases.
- If Conductor saves you time, you can [support its development](https://buy.stripe.com/8x26oH2U44f65TRe574wM04).

## Development

```bash
go test -race -count=1 ./...      # what CI runs
go vet ./...
```

## Licence

[MIT](LICENSE) © Spencer Norton

---

<p align="center">
  <a href="https://norvitech.com"><img alt="Part of the NorviTech Suite — open-source apps for the Linux desktop and the self-hosted stack" src="https://norvitech.com/assets/banner.svg" width="640"></a>
</p>

<p align="center">
  <a href="https://github.com/spencercnorton/helios">Helios</a> ·
  <a href="https://github.com/spencercnorton/bitagent">BitAgent</a> ·
  <a href="https://github.com/spencercnorton/xnote">XNote</a> ·
  <a href="https://github.com/spencercnorton/xnote-placement">XNote Placement</a> ·
  <a href="https://github.com/spencercnorton/snipsnap">SnipSnap</a> ·
  <a href="https://github.com/spencercnorton/conductor">Conductor</a> ·
  <a href="https://github.com/spencercnorton/norvi-os">NorviOS</a> ·
  <a href="https://github.com/spencercnorton/indigo">Indigo</a> ·
  <a href="https://github.com/spencercnorton/roadtrack">Road Track</a> ·
  <a href="https://norvitech.com">norvitech.com</a>
</p>
