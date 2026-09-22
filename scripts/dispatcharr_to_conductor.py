#!/usr/bin/env python3
"""
Dispatcharr → Conductor migration importer.

Pulls providers + channels + per-channel streams from Dispatcharr's API
and posts them to Conductor's admin API. Idempotent: skips items that
already exist (matched by name for providers, by channel number for channels).

Run once after Conductor is stood up. Logs everything; stops on the first
failure rather than silently skipping.
"""

import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

DISPATCH_URL = os.environ.get("DISPATCH_URL", "")
DISPATCH_USER = os.environ.get("DISPATCH_USER", "")
DISPATCH_PASS = os.environ.get("DISPATCH_PASS", "")
if not (DISPATCH_URL and DISPATCH_USER and DISPATCH_PASS):
    print("ERROR: DISPATCH_URL, DISPATCH_USER and DISPATCH_PASS are required")
    sys.exit(1)

CONDUCTOR_URL = os.environ.get("CONDUCTOR_URL", "http://127.0.0.1:8409")
CONDUCTOR_ADMIN_KEY = os.environ.get("CONDUCTOR_ADMIN_KEY", "")
if not CONDUCTOR_ADMIN_KEY:
    print("ERROR: CONDUCTOR_ADMIN_KEY required (Bearer for Conductor admin API)")
    sys.exit(1)


def http(method, url, headers=None, body=None, timeout=30):
    req = urllib.request.Request(url, method=method)
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, data=data, timeout=timeout) as resp:
            text = resp.read().decode()
            return resp.status, text
    except urllib.error.HTTPError as e:
        text = e.read().decode() if e.fp else ""
        return e.code, text


# ─────────────────────── Dispatcharr session ───────────────────────

def dispatch_login():
    code, body = http("POST", f"{DISPATCH_URL}/api/accounts/token/",
                      body={"username": DISPATCH_USER, "password": DISPATCH_PASS})
    if code != 200:
        raise RuntimeError(f"Dispatcharr login failed: {code} {body[:200]}")
    return json.loads(body)["access"]


def dispatch_get(token, path):
    code, body = http("GET", f"{DISPATCH_URL}{path}",
                      headers={"Authorization": f"Bearer {token}"})
    if code != 200:
        raise RuntimeError(f"Dispatch GET {path} → {code}: {body[:200]}")
    return json.loads(body)


# ─────────────────────── Conductor session ───────────────────────

def conductor(method, path, body=None):
    code, text = http(method, f"{CONDUCTOR_URL}{path}",
                      headers={"Authorization": f"Bearer {CONDUCTOR_ADMIN_KEY}"},
                      body=body)
    if code >= 400:
        raise RuntimeError(f"Conductor {method} {path} → {code}: {text[:300]}")
    return json.loads(text) if text else None


# ─────────────────────── Importer ───────────────────────

def import_providers(token):
    """Returns dict: dispatcharr_m3u_id → conductor_provider_id"""
    print("\n━━━ providers ━━━")
    accts = dispatch_get(token, "/api/m3u/accounts/")

    # Skip the empty "custom" account that has no real source
    accts = [a for a in accts if a.get("server_url") and a.get("is_active")]

    # Get current Conductor providers (idempotency)
    existing = conductor("GET", "/admin/providers") or []
    existing_by_name = {p["Name"]: p["ID"] for p in existing}

    mapping = {}
    cred_mapping = {}  # disp_m3u_id → conductor_cred_id
    for a in accts:
        name = a["name"]
        # Try to find creds in profile.custom_properties.user_info or top-level
        profile = a.get("profiles", [{}])[0]
        cp = profile.get("custom_properties") or {}
        ui = cp.get("user_info") or {}
        username = ui.get("username") or a.get("username") or ""
        password = ui.get("password") or a.get("password") or ""

        max_streams = a.get("max_streams") or 1
        # max_streams=0 in Dispatcharr means "unlimited"; we cap at 4 for sanity
        if max_streams == 0:
            max_streams = 4

        # Provider
        if name in existing_by_name:
            pid = existing_by_name[name]
            print(f"  [skip] provider {name!r} already exists ({pid})")
        else:
            kind = "m3u_xtream" if a.get("account_type") == "XC" else "m3u_plain"
            # iBoost is Xtream Codes-style even when account_type=STD
            if "iboost" in name.lower():
                kind = "m3u_xtream"
            p = conductor("POST", "/admin/providers", body={
                "name": name,
                "kind": kind,
                "base_url": a["server_url"],
                "notes": f"Imported from Dispatcharr m3u_account {a['id']} on 2026-05-07",
                "enabled": True,
            })
            pid = p["ID"]
            print(f"  [+] created provider {name!r} ({pid}) kind={kind}")

        mapping[a["id"]] = pid

        # Credential — Conductor stores password encrypted via the cred key
        if username and password:
            existing_creds = conductor("GET", f"/admin/providers/{pid}/credentials") or []
            cred_match = next((c for c in existing_creds if c["username"] == username), None)
            if cred_match:
                cid = cred_match["id"]
                print(f"    [skip] credential {username!r} already exists ({cid})")
            else:
                cred = conductor("POST", f"/admin/providers/{pid}/credentials", body={
                    "username": username,
                    "password": password,
                    "max_streams": max_streams,
                    "priority": a.get("priority", 100),
                    "notes": f"Imported from Dispatcharr profile {profile.get('id')}",
                    "enabled": True,
                })
                cid = cred["id"]
                print(f"    [+] created credential {username!r} ({cid}) max_streams={max_streams}")
            cred_mapping[a["id"]] = cid
        else:
            print(f"    [warn] provider {name!r} has no username/password — skipping credential")

    return mapping, cred_mapping


def import_channels(token, provider_map):
    """Pull channels + their streams; create Conductor channel + sources."""
    print("\n━━━ channels ━━━")
    channels = dispatch_get(token, "/api/channels/channels/?page_size=500")
    if isinstance(channels, dict):
        channels = channels.get("results", [])

    # Pull groups for name lookup
    groups_resp = dispatch_get(token, "/api/channels/groups/?page_size=2000")
    if isinstance(groups_resp, dict):
        groups = groups_resp.get("results", [])
    else:
        groups = groups_resp
    group_by_id = {g["id"]: g["name"] for g in groups}

    # Pull all logos for URL lookup (logo_id → URL)
    logos_resp = dispatch_get(token, "/api/channels/logos/?page_size=2000")
    if isinstance(logos_resp, dict):
        logos = logos_resp.get("results", [])
    else:
        logos = logos_resp
    logo_by_id = {}
    for lg in logos:
        lid = lg.get("id")
        url = lg.get("url") or lg.get("cache_url") or ""
        # Convert relative paths to absolute Dispatcharr URLs
        if url and url.startswith("/"):
            url = DISPATCH_URL + url
        logo_by_id[lid] = url

    # Existing Conductor channels (idempotency by number)
    existing_channels = conductor("GET", "/admin/channels") or []
    existing_by_number = {float(c["Number"]): c["ID"] for c in existing_channels}

    n_created = 0
    n_skipped = 0
    n_sources_total = 0
    n_failed = 0

    for ch in channels:
        number = float(ch["channel_number"])
        name = ch["name"]
        tvg = ch.get("tvg_id") or ""
        gid = ch.get("channel_group_id")
        group = group_by_id.get(gid, "")
        logo = logo_by_id.get(ch.get("logo_id"), "") if ch.get("logo_id") else ""

        # Get streams for this channel
        try:
            stream_resp = dispatch_get(token, f"/api/channels/channels/{ch['id']}/streams/")
            streams = stream_resp if isinstance(stream_resp, list) else stream_resp.get("results", [])
        except Exception as e:
            print(f"  [skip] {name!r} #{number}: failed to fetch streams: {e}")
            n_failed += 1
            continue

        # Filter to streams whose m3u_account we successfully imported
        streams = [s for s in streams if s.get("m3u_account") in provider_map and s.get("url")]
        if not streams:
            print(f"  [skip] {name!r} #{number}: no streams after provider filter")
            n_failed += 1
            continue

        # Idempotent channel
        if number in existing_by_number:
            cid = existing_by_number[number]
            print(f"  [skip] channel #{number:g} {name!r} already exists ({cid})")
            n_skipped += 1
            continue

        try:
            ch_obj = conductor("POST", "/admin/channels", body={
                "number": number,
                "name": name,
                "call_sign": tvg.split(".")[0] if "." in tvg else "",
                "logo_url": logo,
                "group_tag": group,
                "epg_channel_id": tvg,
                "enabled": True,
            })
        except Exception as e:
            print(f"  [fail] channel #{number:g} {name!r}: {e}")
            n_failed += 1
            continue

        ch_id = ch_obj["ID"]
        n_created += 1

        # Add sources (one per stream, priority = index for failover order)
        for i, s in enumerate(streams):
            try:
                conductor("POST", f"/admin/channels/{ch_id}/sources", body={
                    "provider_id": provider_map[s["m3u_account"]],
                    "upstream_url": s["url"],
                    "priority": i,
                    "enabled": True,
                })
                n_sources_total += 1
            except Exception as e:
                print(f"    [fail] source {i} for {name!r}: {e}")

        if n_created % 25 == 0:
            print(f"  ... {n_created} created, {n_sources_total} sources")

    print(f"\nResult: created={n_created} skipped={n_skipped} failed={n_failed} total_sources={n_sources_total}")
    return n_created, n_failed


def main():
    print(f"Dispatcharr: {DISPATCH_URL}")
    print(f"Conductor:   {CONDUCTOR_URL}")
    token = dispatch_login()
    print(f"  Dispatcharr auth OK")
    provider_map, cred_map = import_providers(token)
    print(f"  → {len(provider_map)} providers + {len(cred_map)} credentials mapped")
    n_created, n_failed = import_channels(token, provider_map)
    print(f"\n━━━ done ━━━")
    if n_failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
