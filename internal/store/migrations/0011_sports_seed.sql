-- 0011_sports_seed.sql
-- Phase 5b: seed channel_sport_mapping with the well-known named
-- sports channels. PPV channels (NFL PPV 1-16, NHL PPV 1-18) are
-- intentionally not seeded — TheSportsDB has no per-PPV-slot
-- broadcast data, so blanket-mapping them would inject every NFL
-- game onto every PPV channel which is wrong.
--
-- Each INSERT...SELECT joins on channel.name with a regex (case-
-- insensitive) so the seed lands no rows when the channel doesn't
-- exist (e.g. the user disabled or removed it). Idempotent against
-- the (channel_id, league, team_name, broadcaster_filter) unique key.

-- ESPN main feed: covers NFL, NBA, MLB, NCAA football & basketball.
-- The broadcaster_filter narrows ingested events to ones where the
-- TV-station string contains "ESPN" so Sunday Night Football lands on
-- this channel and not, say, NFL Network.
INSERT INTO channel_sport_mapping (channel_id, league, broadcaster_filter, note)
SELECT id, 'nfl'::sport_league, 'ESPN', 'auto-seed: ESPN NFL'
  FROM channel WHERE lower(name) = 'espn'
ON CONFLICT DO NOTHING;
INSERT INTO channel_sport_mapping (channel_id, league, broadcaster_filter, note)
SELECT id, 'nba'::sport_league, 'ESPN', 'auto-seed: ESPN NBA'
  FROM channel WHERE lower(name) = 'espn'
ON CONFLICT DO NOTHING;
INSERT INTO channel_sport_mapping (channel_id, league, broadcaster_filter, note)
SELECT id, 'mlb'::sport_league, 'ESPN', 'auto-seed: ESPN MLB'
  FROM channel WHERE lower(name) = 'espn'
ON CONFLICT DO NOTHING;
INSERT INTO channel_sport_mapping (channel_id, league, broadcaster_filter, note)
SELECT id, 'ncaaf'::sport_league, 'ESPN', 'auto-seed: ESPN NCAAF'
  FROM channel WHERE lower(name) = 'espn'
ON CONFLICT DO NOTHING;
INSERT INTO channel_sport_mapping (channel_id, league, broadcaster_filter, note)
SELECT id, 'ncaab'::sport_league, 'ESPN', 'auto-seed: ESPN NCAAB'
  FROM channel WHERE lower(name) = 'espn'
ON CONFLICT DO NOTHING;

-- ESPN2: same set but filtered on "ESPN2" so we don't double-inject
-- ESPN's events here.
INSERT INTO channel_sport_mapping (channel_id, league, broadcaster_filter, note)
SELECT id, 'nfl'::sport_league, 'ESPN2', 'auto-seed: ESPN2 NFL'
  FROM channel WHERE lower(name) = 'espn2'
ON CONFLICT DO NOTHING;
INSERT INTO channel_sport_mapping (channel_id, league, broadcaster_filter, note)
SELECT id, 'nba'::sport_league, 'ESPN2', 'auto-seed: ESPN2 NBA'
  FROM channel WHERE lower(name) = 'espn2'
ON CONFLICT DO NOTHING;
INSERT INTO channel_sport_mapping (channel_id, league, broadcaster_filter, note)
SELECT id, 'ncaaf'::sport_league, 'ESPN2', 'auto-seed: ESPN2 NCAAF'
  FROM channel WHERE lower(name) = 'espn2'
ON CONFLICT DO NOTHING;
INSERT INTO channel_sport_mapping (channel_id, league, broadcaster_filter, note)
SELECT id, 'ncaab'::sport_league, 'ESPN2', 'auto-seed: ESPN2 NCAAB'
  FROM channel WHERE lower(name) = 'espn2'
ON CONFLICT DO NOTHING;

-- NFL Network: NFL only, all games their broadcaster carries.
INSERT INTO channel_sport_mapping (channel_id, league, broadcaster_filter, note)
SELECT id, 'nfl'::sport_league, 'NFL Network', 'auto-seed: NFL Network'
  FROM channel WHERE lower(name) = 'nfl network'
ON CONFLICT DO NOTHING;

-- NBA TV: NBA only.
INSERT INTO channel_sport_mapping (channel_id, league, broadcaster_filter, note)
SELECT id, 'nba'::sport_league, 'NBA TV', 'auto-seed: NBA TV'
  FROM channel WHERE lower(name) = 'nba tv'
ON CONFLICT DO NOTHING;

-- MLB Network: MLB only.
INSERT INTO channel_sport_mapping (channel_id, league, broadcaster_filter, note)
SELECT id, 'mlb'::sport_league, 'MLB Network', 'auto-seed: MLB Network'
  FROM channel WHERE lower(name) = 'mlb network'
ON CONFLICT DO NOTHING;

-- NHL Network: NHL only.
INSERT INTO channel_sport_mapping (channel_id, league, broadcaster_filter, note)
SELECT id, 'nhl'::sport_league, 'NHL Network', 'auto-seed: NHL Network'
  FROM channel WHERE lower(name) = 'nhl network'
ON CONFLICT DO NOTHING;

-- Fox Sports 1: NFL, NBA, MLB, NHL, NASCAR — major US slate. We
-- deliberately filter by the FS1 broadcaster string so NFL games
-- land on the right channel.
INSERT INTO channel_sport_mapping (channel_id, league, broadcaster_filter, note)
SELECT id, 'nfl'::sport_league, 'FS1', 'auto-seed: FS1 NFL'
  FROM channel WHERE lower(name) = 'fox sports 1'
ON CONFLICT DO NOTHING;
INSERT INTO channel_sport_mapping (channel_id, league, broadcaster_filter, note)
SELECT id, 'mlb'::sport_league, 'FS1', 'auto-seed: FS1 MLB'
  FROM channel WHERE lower(name) = 'fox sports 1'
ON CONFLICT DO NOTHING;

-- Fox Sports 2: similar but smaller slate.
INSERT INTO channel_sport_mapping (channel_id, league, broadcaster_filter, note)
SELECT id, 'nhl'::sport_league, 'FS2', 'auto-seed: FS2 NHL'
  FROM channel WHERE lower(name) = 'fox sports 2'
ON CONFLICT DO NOTHING;

-- CBS Sports Network: NCAAF, NCAAB.
INSERT INTO channel_sport_mapping (channel_id, league, broadcaster_filter, note)
SELECT id, 'ncaaf'::sport_league, 'CBS Sports Network', 'auto-seed: CBSSN NCAAF'
  FROM channel WHERE lower(name) = 'cbs sports network'
ON CONFLICT DO NOTHING;
INSERT INTO channel_sport_mapping (channel_id, league, broadcaster_filter, note)
SELECT id, 'ncaab'::sport_league, 'CBS Sports Network', 'auto-seed: CBSSN NCAAB'
  FROM channel WHERE lower(name) = 'cbs sports network'
ON CONFLICT DO NOTHING;

-- Bein Sports: international football — La Liga, Ligue 1, etc.
INSERT INTO channel_sport_mapping (channel_id, league, broadcaster_filter, note)
SELECT id, 'laliga'::sport_league, 'beIN', 'auto-seed: beIN La Liga'
  FROM channel WHERE lower(name) = 'bein sports'
ON CONFLICT DO NOTHING;
