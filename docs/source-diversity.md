# Live-TV source diversity

Conductor source rows serve two different purposes that must not be confused:

- Different Xtream accounts pointed at the same stream ID add concurrent-slot
  capacity. They normally redirect to the same media origin and do not provide
  failure independence.
- Different catalogue stream IDs that show the same channel/event and redirect
  to different origins provide actual media-path failover.

The July 22, 2026 Hallmark incident exposed this distinction. Before repair,
175 of 176 enabled channels had multiple enabled rows but only one underlying
stream ID. Hallmark now uses frame-verified US and GO catalogue twins on
different origins. Ten independently hosted PPV pairs were repaired in the
same incident response, leaving 164 enabled channels with duplicate SIDs.

## Runtime behavior

Since v0.36.3, Conductor keeps the downstream Plex/DVR connection open while it
retries or relocates upstream. Each upstream retry is necessarily a new HTTP
request; it cannot preserve an HTTP session after the provider closes that
session. A duplicate-SID pair can still fail together. In that case Conductor
feeds the unavailable slate during bounded backoff and ends the stream after
the continuous 90-second outage cap instead of oscillating forever.

Since v0.36.4, reaching that cap also carries the terminal pump error through
`ServeWriter`. The DVR marks the recording failed and keeps the partial file
instead of renaming a truncated file and reporting it completed.

## Safe PPV repair

`scripts/repair_ppv_source_diversity.py` repairs only PPV channels for which
all of these facts can be proved automatically:

1. The channel is in the 300-499 PPV range and has exactly two enabled Xtream
   source rows with one duplicated stream ID.
2. The current entry is in an allow-listed US/UK category pair.
3. The paired category contains exactly one byte-for-byte name match.
4. Probing the existing primary and proposed secondary redirects yields two
   different origin hosts.

The command is dry-run by default, never prints credential-bearing URLs, and
supports `--expect N` as an apply-time count guard. The July audit found 24
exact-name candidates, but origin probes proved only 10 were independent:
PPV EVENT 01-05 and LIVE EVENT 01-05. UFC 1-9 and PPV EVENT 06-10 still
redirected both twins to the same host, so the repair refused them. The ten
safe pairs were applied in production with an exact-count guard; both sampled
secondary feeds returned aligned MPEG-TS packets and all affected rows passed
post-change priority, health, and failure-state checks.

## Regular-channel follow-up

Do not automatically choose regular-channel alternates by fuzzy name. The
catalogue had plausible normalized-name matches for 74 regular channels, but
names alone do not establish schedule, region, resolution, or frame identity.
Build an explicit mapping only after simultaneous frame/schedule validation,
then require distinct redirect origins using the same safeguards as the
Hallmark repair. Until that mapping is audited, those rows remain capacity
redundancy rather than media-path redundancy.
