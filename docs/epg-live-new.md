# Live broadcast and New guide metadata

`LIVE broadcast —` describes an airing that source metadata identifies as live.
It is available when Plex imports the schedule before the event begins. It
means **scheduled to air live**, not that a stream is currently playing or that
Plex's native Live badge is enabled. Programme start/stop times still define the
guide slot. The descriptor does not extend a PPV event or confirm provider video.

Conductor keeps recognized series/episode names and season/episode identifiers.
It removes explicit provider-added `LIVE:`/`(LIVE)` decorations from titles and
subtitles consistently on every airing, including ones already marked non-live.
Plex shares episode metadata across airings, so stripping only reruns or adding
a live subtitle would create competing metadata. Ordinary names such as
`Saturday Night Live`, `Live-Action Adventures` and bare `Live` are preserved.
No per-airing visible episode field is proven: eligible recognized episodes keep
only the compatibility `<live/>` marker, which Plex may not display. Events
without episode identity carry the descriptor in their title. An explicit repeat
or an exact episode's proven earlier actual airing overrides a conflicting live
flag. A future repeat marker or missing New evidence alone is insufficient.
Idle and upcoming-event **placeholder slots** stay unlabelled; the future actual
live-event airing can carry the descriptor. Sports/News categories alone do not
qualify. `Live with Kelly and Mark` is not treated as a live marker. Explicit
`Replay:`/`Encore:` annotations and idle markers override contradictory live
flags; ordinary words inside titles do not. The inferred movie flag alone does
not override explicit live evidence: long sports broadcasts can trigger it.

New remains independent. Canonical episode history suppresses later known
airings across channels; explicit repeats override source New. Recognized
episodes with credible first-run evidence can be New. Missing episode identity
or evidence fails closed, so some unnumbered news/events cannot receive a
trustworthy New classification. Conductor does not invent episode IDs or use
New to represent Live. Premiere is a separate source property.

## Measured baseline, 2026-09-06

The baseline XMLTV read began at 14:06 UTC on v0.56.8 / `d2d76c5`; later
readback ran across the v0.56.9 deployment at 14:13:24 UTC. That release did
not change guide projection. The XMLTV snapshot and a read-only Plex
guide-database snapshot matched 2,000 airings by exact
channel number and start time. All matched New states agreed: eight New and
1,992 non-new airings. Seven currently live airings had Conductor's former
`LIVE —` title but an undecorated Plex title: Fox Sports 2, beIN Sports, USA,
CBS Sports Network, LIVE EVENT 03, Fox News Channel and ESPN. This demonstrates
the fetch-time cue's cache defect; it does not establish how often every client
refreshes its guide.

Additional exact readbacks confirm CBS `60 Minutes` S58E49 and `Big Brother`
S28E30 are New. `Inside Edition` S38E260 on Fox/CW is not New despite each source
row asserting New; the effective history policy suppresses those reruns. The
current Plex provider stores these first-run flags as `at:premiere=1` and has no
equivalent live property in the inspected airing metadata. These are observed
importer behaviors, not a promised Plex API contract or every-client UI test.

The source snapshot has 15,658 canonical rows in its next seven days, with
1,149 source New candidates and 186 effective New rows. No live/explicit-repeat
conflict or explicit-New/future-airdate conflict appeared in this snapshot.
The correction changes the exported live description and adds positive prior
airing/repeat evidence to that projection; it does not change New history policy.
At 14:26 UTC, all 14 existing decorated Plex titles were movie-shaped metadata;
none prove safe per-airing episode decoration. One Inside Edition episode was
shared by three airings, and one Naked and Afraid episode by two. At 14:28 UTC,
five eligible live rows had formal episode identity and none had known earlier
actual history. Repeat suppression is therefore preventive coverage, not a claim
that those five were incorrectly live. They retain their source-backed live
extension without a new visible title or subtitle.

## Standards and limits

[Plex's XMLTV documentation](https://support.plex.tv/articles/using-an-xmltv-guide/)
supports supplying a custom guide, but does not promise a native Live badge
from `<live/>`. The [XMLTV DTD](https://github.com/XMLTV/xmltv/blob/master/xmltv.dtd)
defines `previously-shown`, `premiere` and `new`; it does not define `live` or
`finale`. Conductor retains the latter two as compatibility extensions and does
not infer support from the XML parsing successfully. XMLTV's textual definition
of `new` also differs from the episode-first-run convention observed in Plex.

## Validation and post-deploy readback

The pre-event cached-guide regression fails on the old projector and passes
with the stable descriptor. Tests cover unchanged bytes/ETags across start and
end, source correction invalidation, explicit repeats, placeholders, categories
without live evidence, stable series/episode identity, idempotent projection,
and independent New flags. A disposable PostgreSQL ingest-to-XMLTV regression
preserves repeat provenance and checks recognized first-run and repeat controls.
The earlier-airing lookup is independently RED when disabled and GREEN across
three race-enabled runs, including a started canonical row before archival,
archived history after deletion, and future-repeat evidence that must not
suppress an earlier live airing.

After the combined release is verified, use the existing DVR's normal **Refresh
Guide** action once. Re-read `/livetv/dvrs` first and bind the Conductor lineup
and UUID: at 14:16 UTC it was DVR **55**, UUID
`3fd13640-5a32-4c52-b413-30a180febafa`, pointing to Conductor `/epg.xml`.
The installed Plex Web 4.160.0 JavaScript explicitly uses
`POST /livetv/dvrs/{dvrID}/reloadGuide` for that action. Do not reuse old DVR 51
from historical notes, recreate the DVR, or change channel mappings.

Wait for the normal import to finish, then compare exact channel/start rows in
Conductor XMLTV and the provider grid/metadata: future and current true-live
event airings must retain the same descriptor; recognized series titles,
subtitles must share the same underlying names after provider decorations are
removed, and episode numbers must remain unchanged, with no invented visible
episode live badge; placeholder slots and proven repeats must lack a live claim;
recognized New and known rerun examples must retain their first-run state.
The advertised current provider routes are
`/tv.plex.providers.epg.xmltv:55/grid`, `/metadata`, and `/hubs/discover` under the
same provider prefix. Re-discover identities if Plex changes them. A successful
refresh request alone is not proof that the new guide has been imported.

No production refresh or candidate Plex import was performed while preparing
this change. Candidate title/grouping readback remains a post-deploy acceptance
check. The audit scripts and unmodified snapshots are under the task's
`_tmp_conductor_live_new_20260906/audit/` directory, outside version control.
