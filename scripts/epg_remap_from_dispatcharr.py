#!/usr/bin/env python3
"""Port dispatcharr's per-channel EPG mapping into Conductor (audit E1).

Dispatcharr aggregated four external XMLTV guides and carried a hand-vetted
channel→tvg_id mapping (epg_epgdata). When Conductor ingests those guides
directly, it needs the same tvg_ids on `channel.epg_channel_id` — this script
ports them by channel number (numbers were preserved by the original
dispatcharr→conductor migration).

Input map file: pipe-separated rows dumped from dispatcharr's Postgres:

    docker exec dispatcharr psql -U dispatch -d dispatcharr -t -A -F'|' -c \
      "SELECT c.channel_number, c.name, d.tvg_id, d.epg_source_id, s.name
         FROM dispatcharr_channels_channel c
         LEFT JOIN epg_epgdata d ON d.id = c.epg_data_id
         LEFT JOIN epg_epgsource s ON s.id = d.epg_source_id
        ORDER BY c.channel_number;" > dispatcharr_epgmap.psv

Mappings whose dispatcharr source is a placeholder generator (ppv-epg.xml,
dummy sources) are skipped — those tvg_ids exist in no real guide. Channels
that already carry the target id are left untouched, so re-runs are no-ops.

Usage:
    python3 scripts/epg_remap_from_dispatcharr.py \
        --map-file dispatcharr_epgmap.psv \
        --conductor http://127.0.0.1:8409 \
        --api-key "$CONDUCTOR_ADMIN_API_KEY" \
        [--apply]

Without --apply it prints the plan and exits.

Set EPG_SKIP_SOURCES to a comma-separated list of source names to skip in
addition to the built-in placeholder sources.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.request

# Dispatcharr source names whose tvg_ids are placeholders, not real guide ids.
# Providers differ, so the list is extensible at runtime: set
# EPG_SKIP_SOURCES to a comma-separated list of source names (case-insensitive)
# and they are skipped in addition to these.
SKIP_SOURCES = {"ppv-epg.xml", "nhl", "btn"} | {
    name.strip().lower()
    for name in os.environ.get("EPG_SKIP_SOURCES", "").split(",")
    if name.strip()
}


def api(base: str, key: str, path: str, method: str = "GET", body: dict | None = None):
    req = urllib.request.Request(
        base.rstrip("/") + path,
        method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={
            "Authorization": "Bearer " + key,
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


def load_map(path: str) -> dict[float, tuple[str, str]]:
    """channel_number -> (tvg_id, dispatcharr_source_name)"""
    out: dict[float, tuple[str, str]] = {}
    for line in open(path):
        parts = line.rstrip("\n").split("|")
        if len(parts) < 5:
            continue
        try:
            number = float(parts[0])
        except ValueError:
            continue
        tvg_id = parts[2].strip()
        src_name = (parts[4] or "").strip()
        if not tvg_id:
            continue
        if src_name.lower() in SKIP_SOURCES:
            continue
        out[number] = (tvg_id, src_name)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--map-file", required=True)
    ap.add_argument("--conductor", default="http://127.0.0.1:8409")
    ap.add_argument("--api-key", default=os.environ.get("CONDUCTOR_ADMIN_API_KEY", ""))
    ap.add_argument("--apply", action="store_true", help="write changes (default: dry-run)")
    args = ap.parse_args()
    if not args.api_key:
        print("missing --api-key / CONDUCTOR_ADMIN_API_KEY", file=sys.stderr)
        return 2

    mapping = load_map(args.map_file)
    channels = api(args.conductor, args.api_key, "/admin/channels")

    plan, skipped, unmatched = [], 0, 0
    for c in channels:
        number = float(c["Number"])
        cur = (c.get("EpgChannelID") or "").strip()
        m = mapping.get(number)
        if m is None:
            unmatched += 1
            continue
        tvg_id, src_name = m
        if cur == tvg_id:
            skipped += 1
            continue
        plan.append((c, tvg_id, src_name, cur))

    print(f"channels: {len(channels)} | to update: {len(plan)} | already correct: {skipped} | no mapping: {unmatched}")
    for c, tvg_id, src_name, cur in sorted(plan, key=lambda x: float(x[0]["Number"])):
        print(f"  ch{c['Number']:>6} {c['Name'][:32]:34} {cur or '(empty)':24} -> {tvg_id:28} [{src_name}]")

    if not args.apply:
        print("\ndry-run only — re-run with --apply to write.")
        return 0

    ok = fail = 0
    for c, tvg_id, _, _ in plan:
        try:
            api(args.conductor, args.api_key, f"/admin/channels/{c['ID']}",
                method="PATCH", body={"epg_channel_id": tvg_id})
            ok += 1
        except Exception as e:  # keep going; report at the end
            print(f"  FAILED ch{c['Number']} {c['Name']}: {e}", file=sys.stderr)
            fail += 1
    print(f"applied: {ok} | failed: {fail}")
    return 1 if fail else 0


if __name__ == "__main__":
    sys.exit(main())
