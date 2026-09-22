# PPV idle guide notices

Idle PPV slots show a guide notice instead of an empty current programme. If
Conductor has an announced event for that exact channel, the visible title is
`Next Sep 6 11:10 AM MDT — Brewers x Reds`. Otherwise it says `PPV idle —`
followed by the known idle status or untimed provider label. A label such as
ESPN2 identifies the announced relay; it does not identify the current game.
Unknown dates are never guessed from a network name or another PPV channel.

The title always includes the date, time and named zone selected by
`CONDUCTOR_PPV_DISPLAY_TIMEZONE` (default `America/Denver`). This remains clear
when Plex caches an entry across local midnight; MST and MDT distinguish the
repeated hour when daylight saving time ends. The description also contains
the full year. The existing real event keeps its exact scheduled start/stop;
its guide notice ends at kickoff. The existing PPV worker reconciles renamed,
rescheduled and withdrawn event notices using the same source identities.

Only rows generated with exact `ppv-off:` provenance are restored. Generic
provider filler such as `No Game Today` remains hidden. PPV notices use a
separate XMLTV emitter with their title, explanation, interval and an explicit
non-first-run marker (`previously-shown`). They
cannot inherit a movie/episode identity, enrichment artwork or synopsis,
original-air date, or LIVE/NEW/premiere flags. Ordinary movies, episodes and
actual events retain their normal serialization. This change does not alter
canonical scheduling, source priorities, recording rules or stored events.
It cannot prevent someone from manually choosing to record a notice in Plex.

## Plex limitation and deliberate tradeoff

The [XMLTV channel schema](https://github.com/XMLTV/xmltv/blob/master/xmltv.dtd)
provides display names, icons and URLs; it has no channel-level next-event
field. [Plex's XMLTV setup](https://support.plex.tv/articles/using-an-xmltv-guide/)
configures guide input and channel mapping, while its
[Channels guide](https://support.plex.tv/articles/225877387-program-guide/)
shows programme intervals in a timeline. No supported next-event overlay for
an otherwise empty programme cell was identified. Renaming a channel would
change the channel label rather than populate that cell.

The earlier removal was intentional: retained live Plex evidence
showed its provider-wide `today.onrightnow` / “Shows On Now” shelf included
130 idle notices among 162 items. The shelf did not filter by programme
section, so changing category could not hide notices there. Restoring PPV
notices can bring them back to that shelf. The user explicitly chose useful
idle grid information despite this limitation; do not claim that these
notices are excluded from Plex recommendations or that this repairs the
shelf's sorting. The no-artwork sports fallback policy remains unchanged.

PPV channel-bank coverage is tracked separately from this revised
idle-guide presentation choice. Banks without a parsed exact event cannot promise a
next date. The generated idle grid remains the existing rolling 24-hour
window, and guide refresh delays still apply.

## Validation and deployment acceptance

Tests cover explicit source provenance versus identical titles/priorities,
no programme identity/flags even with poisoned enrichment, exact kickoff
boundaries, ordinary movie/show bytes, timezone conversion and cache-stable
local-midnight labels. The existing PPV parse/sync controls cover later
blocks, dates beyond the placeholder window, title/time changes, definitive
idle transitions, relay labels and real-event replacement.

After the reviewed release is deployed, wait for its normal PPV worker pass,
verify `/xmltv.xml` includes the selected dated notices, then perform one
normal refresh on the existing Plex DVR. Read back its existing XMLTV provider
DB/API to confirm the next title/date and absence of episode/LIVE/NEW fields.
Do not recreate the DVR or re-pair channels. Record the selected actual event
and its original interval, visible notice title, provider refresh time and
runtime revision. A unit/XML parse pass is not proof that the physical Plex
guide has refreshed; that final readback remains a deployment check.
