# Runbook — a provider stream block goes dead (2026-09-16)

## Symptom

`all sources exhausted this gap; holding on slate` with cause
`stage=headers host=iboostv.us reason=http_407` on channel after channel, DVR
rows failing with `upstream did not start in time`, the `slate_hold_burst`
sentinel tripping daily — while the provider accounts all report
`auth=1 / Active / active_cons=0`. A **407 at the headers stage on a stored
stream id means the id no longer exists**, not that a slot is busy.

## What happened

Every enabled `channel_source` in the provider's `GO:` block (stream ids
1568582–1568874, 116 rows across 59 channels) stopped delivering on 2026-09-12:

```
stream_session, dead block   09-10 17/21 ok · 09-11 2/7 · 09-12→09-16 0/25
stream_session, every other  100% ok the whole time
```

`get_live_streams` no longer lists those ids (the 101 that remain in the range
are `INNEBANDY PLAY PPV`). 56 channels had only dead rows; 5 of 6 recordings
failed for five days. The 2026-07-22 audit had declined a bulk remap while the
block still worked ("schedule and regional equivalence needs validation") — once
the block is gone there is nothing left to protect.

## How to see it

```sql
-- a whole block at health 0 is a renumbering, not a flap
select c.name, count(*) filter (where cs.health_score = 0) dead, count(*) total
from channel_source cs join channel c on c.id = cs.channel_id
where cs.enabled group by c.name having count(*) filter (where cs.health_score = 0) = count(*);
```

The stored `upstream_url` is a `${USER}/${PASS}` template: curl it raw and the
panel answers a meaningless 513. Substitute a real credential before probing.

## The repair (live data, no restart)

`PATCH /admin/channels/{id}/sources/{sid}` with the new URL and
`reset_failure`, exactly like `scripts/repair_hallmark_source_pair.py`. Targets
are the catalogue's plain `US: <NAME> HD` entries (over EAST/WEST — the
convention already used by TLC 325801 and HALLMARK 325790); `RK:`/`PRIME:` raw
feeds only where no `US:` entry exists (DraftKings, ESPN8, NBA TV). Every target
is fetched for a real MPEG-TS sync byte before any write. 116/116 applied on
2026-09-16; Cartoon Network, Travel Channel and HBO then tuned through
`/auto/v<n>` at 5.7–9.4 MB per 20 s with 199/200 packets synced.

The script (kept here rather than in `scripts/` because a `scripts/` change is
a release under `release_version_guard`, and this is a data repair):

```python
#!/usr/bin/env python3
"""Repoint every enabled conductor source that still targets the provider's dead
1568xxx ("GO:") stream block at the catalogue's current US feed for that channel.

The block stopped delivering bytes on 2026-09-12 (stream_session: 0/25 ok since,
vs 100% for every other block) and its ids are no longer in get_live_streams, so
there is no schedule equivalence left to protect — the alternative is 56 dead
channels. Mapping is explicit below (exact catalogue names), every target is
fetched for a real MPEG-TS sync byte before any PATCH, and the PATCH goes through
the admin API like scripts/repair_hallmark_source_pair.py. Dry run by default.

Usage:
    CONDUCTOR_ADMIN_API_KEY=... XTREAM_USER=... XTREAM_PASS=... python3 remap_dead_source_block.py [--apply]
"""
import argparse, json, os, re, sys, time, urllib.request, urllib.parse

BASE = "http://127.0.0.1:8409"
DEAD = range(1568000, 1569001)
SID_RE = re.compile(r"/(\d+)\.ts$")
# channel name (conductor) -> exact catalogue name. Plain "HD" over EAST/WEST where
# both exist (matches the existing TLC 325801 / HALLMARK 325790 convention).
TARGETS = {
    "AMC": "US: AMC HD", "Animal Planet": "US: ANIMAL PLANET HD", "BBC America": "US: BBC AMERICA HD",
    "BET": "US: BET HD", "BRAVO": "US: BRAVO HD", "Boomerang": "US: BOOMERANG HD",
    "CBS Sports Network": "US: CBS SPORTS NETWORK HD", "CMT": "US: CMT HD",
    "Cartoon Network": "US: CARTOON NETWORK HD", "Comedy Central": "US: COMEDY CENTRAL HD",
    "Destination America": "US: DESTINATION AMERICA HD", "Discovery": "US: DISCOVERY HD",
    "Disney Channel": "US: DISNEY CHANNEL HD", "Draft Kings": "RK: DRAFTKINGS ᴿᴬᵂ",
    "E!": "US: E! ENTERTAINMENT HD", "ESPN2": "US: ESPN 2 HD", "ESPN8": "RK: ESPN8 THE OCHO ᴿᴬᵂ",
    "ESPNU": "US: ESPN U HD", "FX": "US: FX HD", "FXX": "US: FXX HD", "Food Network": "US: FOOD NETWORK HD",
    "Fox Business Network": "US: FOX BUSINESS NETWORK HD", "HBO": "US: HBO HD", "HBO2": "US: HBO 2 HD",
    "HGTV": "US: HGTV HD", "HLN": "US: HLN HD", "Hallmark Channel": "US: HALLMARK HD",
    "History Channel": "US: HISTORY HD", "IFC": "US: IFC HD",
    "Investigation Discovery": "US: ID INVESTIGATION DISCOVERY HD", "Lifetime": "US: LIFETIME HD",
    "MGM+": "US: MGM HD", "MLB Network": "US: MLB NETWORK", "MSNBC": "US: MSNBC HD", "MTV": "US: MTV HD",
    "MTV2": "US: MTV 2 HD", "Magnolia Network": "US: Magnolia Network West (Y) HD",
    "MotorTrend": "US: MOTORTREND HD", "NBA TV": "PRIME: NBA TV HDTV ᴿᴬᵂ", "NFL Network": "US: NFL NETWORK HD",
    "NHL Network": "US: NHL NETWORK", "Nickelodeon": "US: NICKELODEON HD", "Oxygen True Crime": "US: OXYGEN HD",
    "SEC Network": "US: SEC NETWORK HD", "SYFY": "US: SYFY HD", "Smithsonian Channel": "US: SMITHSONIAN CHANNEL HD",
    "SunDance TV": "US: SUNDANCE HD", "TBS": "US: TBS HD", "TCM": "US: TCM HD", "TLC": "US: TLC HD",
    "TNT": "US: TNT HD", "TRUTV": "US: TRUTV 4K", "TV Land": "US: TV LAND HD",
    "The Weather Channel": "US: THE WEATHER CHANNEL HD", "Travel Channel": "US: TRAVEL CHANNEL HD",
    "USA": "US: USA NETWORK HD", "VH1": "US: VH 1 HD", "Vice": "US: VICELAND HD", "WE TV": "US: WE TV HD",
}
# Same exact name appears several times for a few channels; the first id that streams wins.
PREFER = {"US: FOX BUSINESS NETWORK HD": 324925, "US: SEC NETWORK HD": 325807}

def api(path, body=None, method=None):
    req = urllib.request.Request(BASE + path, data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Authorization": "Bearer " + os.environ["AK"], "Content-Type": "application/json"},
                                 method=method or ("POST" if body is not None else "GET"))
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r) if r.length != 0 else {}

def streams(url):
    """True when the feed answers 200 with an MPEG-TS sync byte; never prints the URL."""
    try:
        r = urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "VLC/3.0.20 LibVLC/3.0.20"}), timeout=15)
        head = r.read(188); r.close()
        return r.status == 200 and head[:1] == b"G"
    except Exception:
        return False

def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--apply", action="store_true"); args = ap.parse_args()
    user, pw = creds["env"]
    cat = json.load(urllib.request.urlopen(f"http://iboostv.us/player_api.php?username={urllib.parse.quote(user)}&password={urllib.parse.quote(pw)}&action=get_live_streams", timeout=120))
    byname = {}
    for s in cat: byname.setdefault((s.get("name") or "").strip(), []).append(int(s["stream_id"]))
    # one live provider credential for the catalogue and the stream probe (never printed)
    creds = {"env": (os.environ["XTREAM_USER"], os.environ["XTREAM_PASS"])}
    channels = api("/admin/channels")
    todo, skipped, verified = [], [], {}
    for ch in channels:
        for src in api(f"/admin/channels/{ch['ID']}/sources"):
            m = SID_RE.search(src["UpstreamURL"])
            if not src["Enabled"] or not m or int(m.group(1)) not in DEAD: continue
            target = TARGETS.get(ch["Name"])
            if not target: skipped.append((ch["Name"], "no mapping")); continue
            ids = byname.get(target, [])
            if PREFER.get(target) in ids: ids = [PREFER[target]] + [i for i in ids if i != PREFER[target]]
            if not ids: skipped.append((ch["Name"], f"{target!r} not in catalogue")); continue
            prefix = src["UpstreamURL"][: m.start(1)]
            # verify once per target name with a real credential (the stored URL is a ${USER}/${PASS} template)
            if target not in verified:
                user, pw = next(iter(creds.values()))
                verified[target] = next((i for i in ids if streams(prefix.replace("${USER}", user).replace("${PASS}", pw) + f"{i}.ts")), None)
                time.sleep(1)
            sid = verified[target]
            if sid is None: skipped.append((ch["Name"], f"{target!r} does not stream")); continue
            todo.append((ch["ID"], ch["Name"], src["ID"], src["Priority"], int(m.group(1)), sid, target, prefix + f"{sid}.ts"))
    for cid, name, sid_row, prio, old, new, target, _ in todo:
        print(f"PATCH {name:26s} p{prio} sid {old} -> {new:>8d}  ({target})")
    for name, why in skipped: print(f"SKIP  {name:26s} {why}")
    print(f"\n{len(todo)} source rows to repoint, {len(skipped)} skipped, {sum(1 for v in verified.values() if v)}/{len(verified)} targets stream")
    if not args.apply: print("dry run — pass --apply"); return
    for cid, name, sid_row, prio, old, new, target, url in todo:
        api(f"/admin/channels/{cid}/sources/{sid_row}", {"upstream_url": url, "priority": prio, "health_score": 1.0, "reset_failure": True}, method="PATCH")
    print(f"applied {len(todo)} PATCHes")

if __name__ == "__main__":
    main()
```
