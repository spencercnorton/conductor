# Security policy

## Reporting a vulnerability

Please report vulnerabilities privately through GitHub:
**[Report a vulnerability](https://github.com/spencercnorton/conductor/security/advisories/new)**.
Do not open a public issue, and do not include real credentials, provider URLs, stream tokens
or personal paths in the report — a description and a minimal reproduction
are enough.

There is no e-mail address for security reports; the advisory form is the
only channel, and it is the one that is monitored. You will get an
acknowledgement within a week. Fixes ship as a tagged release; the advisory
is published once the release is out, and credits you unless you ask
otherwise.

## Supported versions

Only the latest tagged release is supported. Conductor has no LTS line.

## Scope

In scope: this repository's code and the artefacts it ships.
Out of scope: third-party services Conductor connects to — your IPTV
provider, Plex, TMDb, TVDB, TheSportsDB and Schedules Direct — and
deployments the maintainer does not operate.

## What Conductor does with credentials and data

Understanding the trust model helps you judge what is and is not a finding:

- **Provider credentials** are encrypted at rest with AES-GCM under the key in `CONDUCTOR_CRED_KEY`, which is read from the environment and never written to disk.
- **What leaves the machine:** upstream provider requests, and — only for the metadata services you configure — TMDb, TVDB, TheSportsDB and Schedules Direct lookups.
- **The admin API is not a sandbox:** a Bearer key, or an SSO principal with admin rights, can reconfigure every provider and credential. Network placement is still your boundary.
- **Local state** lives under the state directory you mount (logos, recordings and diagnostics; channels, credentials and guide data live in Postgres). No telemetry is
  sent anywhere.
