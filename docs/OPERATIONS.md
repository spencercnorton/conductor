# Conductor deployment and operations

Conductor presents an HDHomeRun-compatible tuner to Plex and pools your IPTV
provider connections. PostgreSQL stores guide, provider and recording state.
Provider passwords are encrypted with an operator-owned credential key.

## Requirements and first deployment

Use Docker with Compose, persistent storage and PostgreSQL 16 or newer. Your
provider must authorize the channels and concurrent streams you configure.
Plex must be able to reach the advertised Conductor URL.

```bash
git clone https://github.com/spencercnorton/conductor.git
cd conductor
cp .env.example .env
openssl rand -hex 32   # POSTGRES_PASSWORD
openssl rand -hex 32   # CONDUCTOR_CRED_KEY
openssl rand -hex 32   # CONDUCTOR_ADMIN_API_KEY
```

Put these separately generated values in the local `.env`. Set
`CONDUCTOR_BASE_URL` to the URL reachable from Plex. The example binds to
loopback initially; set `CONDUCTOR_BIND_ADDRESS=0.0.0.0` only on a trusted
network with firewall restrictions when Plex runs on another host; `localhost` inside a Plex
container means that container, not Conductor. Check out a reviewed release
tag for a reproducible deployment.

```bash
docker compose -f docker-compose.example.yml config --quiet
docker compose -f docker-compose.example.yml up --build -d
docker compose -f docker-compose.example.yml ps
curl --fail http://localhost:8409/healthz
curl --fail http://localhost:8409/discover.json
curl --fail http://localhost:8409/lineup.json
```

An empty lineup is expected until providers, credentials and channels exist.
Pair Plex through Live TV & DVR → Set up Plex DVR → manual address, using
`http://your-host:8409`. Add configuration through the authenticated admin API;
the API contract is in [the technical specification](spec/dispatcharr-replacement-spec.md).

## Configuration map

| Variable | Purpose |
|---|---|
| `CONDUCTOR_BASE_URL` | Reachable public device, lineup and artwork URLs |
| `CONDUCTOR_POSTGRES_DSN` | Persistent state connection; use the Compose-provided value in the example stack |
| `CONDUCTOR_CRED_KEY` | Stable 32-byte hexadecimal credential-encryption key |
| `CONDUCTOR_ADMIN_API_KEY` | Bearer key for administration |
| `CONDUCTOR_AUTH_ENABLED` | Explicitly require admin authentication |
| `CONDUCTOR_DVR_INDEXER_KEY` | Optional Torznab/DVR integration key |
| `CONDUCTOR_UPSTREAM_PROXY` | Optional provider-traffic proxy URL; may contain credentials |
| `CONDUCTOR_TMDB_API_KEY` | Optional metadata enrichment key |

Keep the credential key stable. Losing or replacing it can make stored
provider passwords unreadable. Keep database and proxy credentials out of
shell history, copied Compose output and public reports.

## Network and access controls

The HDHomeRun and XMLTV surfaces are intended for Plex and are not admin
interfaces. Keep the service on a trusted network. Require authentication for
administration and restrict its routes at your reverse proxy. Do not expose
PostgreSQL. The example stack is a starting point; configure TLS and network
access controls before exposing a service beyond your trusted network.

## Verify a working system

1. Add one provider credential with its actual connection limit.
2. Add a small test lineup and guide feed before importing the full catalog.
3. Tune one channel in Plex, then end playback and verify its lease releases.
4. Verify guide times, timezone, artwork and episode numbering.
5. Test one short recording before enabling automated scheduling.

Avoid bulk provider probes while diagnosing a failure. Providers may throttle
or block the source IP; increasing retries can make the incident worse.

## Backup, upgrades and recovery

Back up PostgreSQL, the credential key and the persistent Conductor data
volume as one recovery set. Keep the key in a separate encrypted secret
backup. Back up recording outputs according to your retention requirements.
Test a restore with a disposable database and storage, with provider access
blocked until the recovered configuration has been checked.

Record the current version and image digest before an upgrade. Rebuild from
the new public tag, then check health, device identity, lineup, tune and lease
release. For an incompatible migration, restore the matching database backup
rather than only downgrading the binary.

## Troubleshooting

| Symptom | Check and next action |
|---|---|
| Plex cannot discover the tuner | Add it manually; verify its advertised base URL from Plex's network. |
| Healthy service, no channels | Check provider credentials, channel mapping and lineup import. |
| Tune fails after a burst of requests | Check provider throttling or source-IP blocks; stop aggressive probes. |
| Guide looks empty or stale | Check ingest errors, provider horizon and timezone configuration. |
| Stored credentials cannot decrypt | Recover the original credential key; do not overwrite it. |
| DVR conflicts | Check provider slots, live reserve, padding and concurrent recordings. |

Share scrubbed error categories and version information. Never share a
provider URL with embedded credentials, your key, a raw database dump or a
full guide capture containing account details.
