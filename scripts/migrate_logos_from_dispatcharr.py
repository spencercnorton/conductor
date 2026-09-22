#!/usr/bin/env python3
"""migrate_logos_from_dispatcharr.py — one-shot logo migration

Downloads channel logos from Dispatcharr's working API endpoint into
Conductor's local logo directory + rewrites Conductor channel.logo_url
to Conductor's own /logos/{filename} path.

Why this exists: the dispatcharr_to_conductor.py importer copied
Dispatcharr's logo URLs verbatim, but those URLs use Dispatcharr's
internal /data/logos/<filename> path which is not actually served
over HTTP (only /api/channels/logos/<id>/cache/ works). Result: every
channel showed as a blank cell in Plex's guide.

This script reads Dispatcharr's logo table to map filename → id,
fetches each logo via the working API endpoint, saves it to
Conductor's volume directory (/var/lib/conductor/logos inside the
container = /opt/conductor/data/logos on host), and updates
Conductor's channel.logo_url to point at Conductor's own
/logos/{filename} endpoint.

Idempotent: re-running re-downloads only files missing locally and
re-updates URLs to the Conductor pattern. URLs that don't match the
broken pattern are left alone.

Usage (run on a host that can reach both DBs):

    python3 migrate_logos_from_dispatcharr.py \\
        --dispatcharr-container=dispatcharr \\
        --conductor-pg-container=conductor-pg \\
        --logos-dir=/opt/conductor/data/logos \\
        --conductor-base-url=http://conductor.example:8409 \\
        --dispatcharr-base-url=http://dispatcharr.example:9195

Or with explicit DSNs:

    python3 migrate_logos_from_dispatcharr.py \\
        --dispatcharr-dsn='postgres://dispatch:...@host/dispatcharr' \\
        --conductor-dsn='postgres://conductor:...@host/conductor' \\
        ...

Both DBs typically run in containers; the simplest path is to
exec psql via docker. That avoids needing direct network access to the
Dispatcharr DB which only listens on its container's loopback.
"""
from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
import urllib.parse
import urllib.request
from pathlib import Path


def run_psql_json(container: str, db: str, user: str, sql: str) -> list[dict]:
    """Run a SQL query inside a postgres container and return rows as dicts.

    Uses psql's --csv mode would also work but JSON keeps types sane.
    """
    wrapped = f"COPY (SELECT row_to_json(t) FROM ({sql}) t) TO STDOUT"
    cmd = ["docker", "exec", container, "psql", "-U", user, "-d", db, "-tAc", wrapped]
    out = subprocess.check_output(cmd, text=True)
    return [json.loads(line) for line in out.strip().split("\n") if line]


def run_psql_exec(container: str, db: str, user: str, sql: str) -> str:
    cmd = ["docker", "exec", container, "psql", "-U", user, "-d", db, "-c", sql]
    return subprocess.check_output(cmd, text=True)


def fetch_logo(dispatcharr_base_url: str, logo_id: int, dest: Path) -> bool:
    """Download a logo by Dispatcharr id into dest. Returns True on success."""
    url = f"{dispatcharr_base_url.rstrip('/')}/api/channels/logos/{logo_id}/cache/"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Conductor/migrate-logos"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = resp.read()
        if len(data) < 200:
            print(f"  WARN logo id={logo_id} returned only {len(data)} bytes — skipping", file=sys.stderr)
            return False
        dest.write_bytes(data)
        return True
    except Exception as e:
        print(f"  ERROR fetch id={logo_id}: {e}", file=sys.stderr)
        return False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dispatcharr-container", default="dispatcharr",
                    help="docker container name running Dispatcharr's Postgres")
    ap.add_argument("--dispatcharr-db-user", default="dispatch")
    ap.add_argument("--dispatcharr-db-name", default="dispatcharr")
    ap.add_argument("--dispatcharr-base-url", default="http://127.0.0.1:9195",
                    help="HTTP base URL for Dispatcharr (used for /api/channels/logos)")
    ap.add_argument("--conductor-pg-container", default="conductor-pg")
    ap.add_argument("--conductor-db-user", default="conductor")
    ap.add_argument("--conductor-db-name", default="conductor")
    ap.add_argument("--conductor-base-url", required=True,
                    help="HTTP base URL Plex and other clients use to reach Conductor. "
                         "It is written into every logo URL, so it must be reachable "
                         "from Plex — not a loopback address unless Plex runs in the "
                         "same network namespace")
    ap.add_argument("--logos-dir", default="/opt/conductor/data/logos",
                    help="Host path where logos are saved (must be writable by current user)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    logos_dir = Path(args.logos_dir)
    logos_dir.mkdir(parents=True, exist_ok=True)

    print(f"=== Conductor logo migration ===")
    print(f"  dispatcharr container: {args.dispatcharr_container}")
    print(f"  conductor-pg container: {args.conductor_pg_container}")
    print(f"  logos dir: {logos_dir}")
    print(f"  conductor base: {args.conductor_base_url}")
    print(f"  dry-run: {args.dry_run}")
    print()

    # Step 1: get Conductor's channels with broken /data/logos/ URLs.
    print("Step 1: Conductor channels with broken Dispatcharr-data-path logo_url")
    conductor_channels = run_psql_json(
        args.conductor_pg_container, args.conductor_db_name, args.conductor_db_user,
        "SELECT id::text, name, logo_url FROM channel WHERE enabled "
        "AND logo_url LIKE '%9195/data/logos/%'"
    )
    print(f"  found {len(conductor_channels)} channels with broken URLs")

    # Step 2: build filename → Dispatcharr id map.
    print("Step 2: filename -> id map from Dispatcharr's logo table")
    dispatch_logos = run_psql_json(
        args.dispatcharr_container, args.dispatcharr_db_name, args.dispatcharr_db_user,
        "SELECT id, url FROM dispatcharr_channels_logo WHERE url LIKE '/data/logos/%'"
    )
    file2id: dict[str, int] = {}
    for row in dispatch_logos:
        # url like "/data/logos/<file>"
        fn = row["url"].rsplit("/", 1)[-1]
        # Prefer the lowest ID for the same filename (rare collisions, but stable).
        if fn not in file2id or row["id"] < file2id[fn]:
            file2id[fn] = row["id"]
    print(f"  {len(file2id)} unique filenames in Dispatcharr's logo table")

    # Step 3: per Conductor channel, download (if missing) + record new URL.
    print("Step 3: downloading + updating channel rows")
    n_downloaded = 0
    n_skipped_existing = 0
    n_no_match = 0
    n_updates: list[tuple[str, str]] = []  # (channel_id, new_url)
    for ch in conductor_channels:
        url = ch["logo_url"]
        # Filename is the part after the last slash.
        fn = url.rsplit("/", 1)[-1]
        if not fn:
            continue
        logo_id = file2id.get(fn)
        if logo_id is None:
            print(f"  no Dispatcharr match for filename: {fn} (channel {ch['name']!r})")
            n_no_match += 1
            continue
        dest = logos_dir / fn
        if not dest.exists():
            if args.dry_run:
                print(f"  DRY would download id={logo_id} -> {dest}")
            else:
                if fetch_logo(args.dispatcharr_base_url, logo_id, dest):
                    n_downloaded += 1
        else:
            n_skipped_existing += 1
        new_url = f"{args.conductor_base_url.rstrip('/')}/logos/{urllib.parse.quote(fn)}"
        n_updates.append((ch["id"], new_url))

    print(f"  downloaded: {n_downloaded}")
    print(f"  skipped (already on disk): {n_skipped_existing}")
    print(f"  no Dispatcharr match: {n_no_match}")

    # Step 4: bulk-update Conductor's channel.logo_url.
    print(f"Step 4: updating {len(n_updates)} Conductor channel.logo_url rows")
    if args.dry_run:
        for chid, u in n_updates[:5]:
            print(f"  DRY UPDATE channel {chid}: logo_url -> {u}")
        if len(n_updates) > 5:
            print(f"  ... and {len(n_updates) - 5} more")
    else:
        for chid, u in n_updates:
            sql = f"UPDATE channel SET logo_url = '{u}' WHERE id = '{chid}'"
            run_psql_exec(args.conductor_pg_container, args.conductor_db_name,
                          args.conductor_db_user, sql)
        print(f"  done.")

    print()
    print("=== Done. Trigger a Plex EPG reload to pick up the new logos. ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
