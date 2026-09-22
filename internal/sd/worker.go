package sd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/epg"
	"github.com/spencercnorton/conductor/internal/store"
)

// Ingester pulls schedules + program details from Schedules Direct and
// upserts them as epg_program rows. Runs on the same cadence as the
// XMLTV worker (CONDUCTOR_EPG_INTERVAL, default 6h).
//
// Per-station incremental: SD returns an MD5 hash per station-day. If
// the MD5 hasn't changed since our last pull, we skip the program
// detail fetch entirely. Saves a LOT of bandwidth for stations whose
// schedule rarely changes.
//
// Channel mapping: SD station IDs are the source of truth — operators
// configure each Conductor channel.epg_channel_id = "<sd_station_id>"
// (via the admin endpoint or by importing from SD's lineup endpoint).
type Ingester struct {
	Logger   *slog.Logger
	Client   *Client
	DB       SDStore
	Interval time.Duration // for the long-running goroutine
}

// SDStore is the slice of *store.DB the ingester needs. Defined here
// (instead of importing the concrete type) so tests can substitute a fake.
type SDStore interface {
	AcquireSDIngestLease(ctx context.Context) (release func() error, err error)
	ListEnabledSDLineups(ctx context.Context) ([]store.SDLineup, error)
	ReconcileSDLineupStations(ctx context.Context, lineupID string, currentStationIDs []string) (store.SDLineupReconcileResult, error)
	GetSDStationMD5(ctx context.Context, lineupID, stationID string) (string, error)
	ReplaceSDStationSnapshot(ctx context.Context, lineupID, stationID, md5 string, channelID uuid.UUID, programs []store.EPGProgram) (store.SDStationSnapshotResult, error)
	MarkSDLineupResult(ctx context.Context, sdLineupID, status string) error
	LookupChannelByEPGID(ctx context.Context, epgChannelID string) (store.Channel, error)
}

func NewIngester(logger *slog.Logger, client *Client, db SDStore, interval time.Duration) *Ingester {
	if interval <= 0 {
		interval = 6 * time.Hour
	}
	return &Ingester{Logger: logger, Client: client, DB: db, Interval: interval}
}

// Run blocks until ctx cancels.
func (i *Ingester) Run(ctx context.Context) {
	i.Logger.Info("sd ingester starting", "interval", i.Interval)
	i.runOnce(ctx)
	t := time.NewTicker(i.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			i.Logger.Info("sd ingester stopped")
			return
		case <-t.C:
			i.runOnce(ctx)
		}
	}
}

// RunOnce triggers an immediate pass; returns aggregate stats for the
// admin endpoint to surface.
type IngestStats struct {
	LineupsAttempted  int
	LineupsOK         int
	LineupsFailed     int
	StationsTotal     int
	StationsSkipped   int // MD5 unchanged
	StationsBlocked   int // stronger direct candidate; MD5 retained for retry
	StationsWithdrawn int // removed from the provider's current lineup snapshot
	StationsFetched   int
	ProgramsAdded     int
	ProgramsUpdated   int
	ProgramsDeleted   int
	ProgramsUnchanged int
	UnmappedStations  int // SD station_id without a Conductor channel mapping
}

func (i *Ingester) RunOnce(ctx context.Context) IngestStats { return i.runOnce(ctx) }

func (i *Ingester) runOnce(ctx context.Context) IngestStats {
	var stats IngestStats
	release, err := i.DB.AcquireSDIngestLease(ctx)
	if err != nil {
		i.Logger.Warn("sd: acquire ingest lease failed", "err", err)
		return stats
	}
	defer func() {
		if err := release(); err != nil {
			i.Logger.Error("sd: release ingest lease failed", "err", err)
		}
	}()

	lineups, err := i.DB.ListEnabledSDLineups(ctx)
	if err != nil {
		i.Logger.Warn("sd: list lineups failed", "err", err)
		return stats
	}
	stats.LineupsAttempted = len(lineups)
	channelClaims := make(map[uuid.UUID]string)

	for _, l := range lineups {
		if err := ctx.Err(); err != nil {
			return stats
		}
		s, err := i.runLineup(ctx, l, channelClaims)
		stats.StationsTotal += s.StationsTotal
		stats.StationsSkipped += s.StationsSkipped
		stats.StationsBlocked += s.StationsBlocked
		stats.StationsWithdrawn += s.StationsWithdrawn
		stats.StationsFetched += s.StationsFetched
		stats.ProgramsAdded += s.ProgramsAdded
		stats.ProgramsUpdated += s.ProgramsUpdated
		stats.ProgramsDeleted += s.ProgramsDeleted
		stats.ProgramsUnchanged += s.ProgramsUnchanged
		stats.UnmappedStations += s.UnmappedStations
		if errors.Is(err, store.ErrSDCandidateBlocked) {
			// The provider pass and all non-conflicting stations completed. Keep
			// the lineup operational while making the incomplete publication
			// explicit in durable status and aggregate stats.
			stats.LineupsOK++
			_ = i.DB.MarkSDLineupResult(ctx, l.SDLineupID, "partial: "+err.Error())
			i.Logger.Warn("sd lineup ingest completed partially",
				"lineup", l.SDLineupID, "stations_blocked", s.StationsBlocked, "err", err)
		} else if err != nil {
			stats.LineupsFailed++
			_ = i.DB.MarkSDLineupResult(ctx, l.SDLineupID, "error: "+err.Error())
			i.Logger.Warn("sd lineup ingest failed",
				"lineup", l.SDLineupID, "err", err)
		} else {
			stats.LineupsOK++
			_ = i.DB.MarkSDLineupResult(ctx, l.SDLineupID, "ok")
		}
	}

	i.Logger.Info("sd ingest pass complete",
		"lineups_ok", stats.LineupsOK, "lineups_failed", stats.LineupsFailed,
		"stations_fetched", stats.StationsFetched, "stations_skipped", stats.StationsSkipped,
		"stations_blocked", stats.StationsBlocked,
		"stations_withdrawn", stats.StationsWithdrawn,
		"unmapped_stations", stats.UnmappedStations,
		"programs_added", stats.ProgramsAdded,
		"programs_updated", stats.ProgramsUpdated,
		"programs_deleted", stats.ProgramsDeleted)
	return stats
}

func (i *Ingester) runLineup(ctx context.Context, l store.SDLineup, channelClaims map[uuid.UUID]string) (IngestStats, error) {
	var s IngestStats
	var blockedStations []string

	// 1. Get the lineup's stations.
	lineup, err := i.Client.GetLineup(ctx, l.SDLineupID)
	if err != nil {
		return s, err
	}

	// 2. Validate the complete provider authority before withdrawing anything.
	//    A syntactically valid error object or partial map must retain the last-good
	//    guide; an explicitly empty map+station set with the exact requested lineup
	//    identity is the only empty response that is authoritative.
	stationIDs, err := validateLineupAuthority(l.SDLineupID, lineup)
	if err != nil {
		return s, err
	}
	s.StationsTotal = len(stationIDs)

	// 3. Resolve only stations Conductor actually maps, but do not mutate
	//    durable lineup state yet. The entire mapped provider response must be
	//    validated before a removed station can lose its last-good guide.
	//    The ingest lease fences concurrent remaps, so these channel IDs remain
	//    authoritative for the whole pass. Provider errors on an intentionally
	//    unmapped lineup station must not starve mapped channels.
	channelByStation := make(map[string]store.Channel, len(stationIDs))
	pendingClaims := make(map[uuid.UUID]string, len(stationIDs))
	md5Reqs := make([]ScheduleRequest, 0, len(stationIDs))
	for _, stationID := range stationIDs {
		ch, err := i.DB.LookupChannelByEPGID(ctx, stationID)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				s.UnmappedStations++
				continue
			}
			return s, fmt.Errorf("map SD station %s: %w", stationID, err)
		}
		claimedStation, claimed := pendingClaims[ch.ID]
		if !claimed {
			claimedStation, claimed = channelClaims[ch.ID]
		}
		if claimed && claimedStation != stationID {
			return s, fmt.Errorf("SD stations %s and %s map to the same Conductor channel %s",
				claimedStation, stationID, ch.ID)
		}
		pendingClaims[ch.ID] = stationID
		channelByStation[stationID] = ch
		md5Reqs = append(md5Reqs, ScheduleRequest{StationID: stationID})
	}
	// Claims fence the complete ingest pass, not only successful provider
	// publications. Once every mapped station in this lineup has resolved and
	// conflict-checked, retain those claims before any provider fetch can fail;
	// otherwise a later lineup could publish a distinct station onto the same
	// Conductor channel. Keeping the copy after the whole mapping loop preserves
	// all-or-nothing behavior when mapping itself fails partway through.
	for channelID, stationID := range pendingClaims {
		channelClaims[channelID] = stationID
	}
	reconcileLineup := func() error {
		lineupResult, err := i.DB.ReconcileSDLineupStations(ctx, l.SDLineupID, stationIDs)
		if err != nil {
			return fmt.Errorf("reconcile SD lineup %s stations: %w", l.SDLineupID, err)
		}
		s.StationsWithdrawn += lineupResult.StationsWithdrawn
		s.ProgramsDeleted += lineupResult.ProgramsDeleted
		return nil
	}
	if len(md5Reqs) == 0 {
		if err := reconcileLineup(); err != nil {
			return s, err
		}
		return s, nil
	}

	// 4. Ask the provider for its complete station/day manifest first. The
	//    composite watermark covers both the full date set and every daily MD5.
	//    A change to any day therefore fetches and publishes the whole horizon.
	manifestStartedUTC := time.Now().UTC()
	md5Manifest, err := i.Client.ScheduleMD5(ctx, md5Reqs)
	manifestFinishedUTC := time.Now().UTC()
	if err != nil {
		return s, err
	}
	authorities, err := buildScheduleAuthorities(md5Reqs, md5Manifest)
	if err != nil {
		return s, err
	}
	if err := validateDefaultScheduleAuthorities(authorities, manifestStartedUTC, manifestFinishedUTC); err != nil {
		return s, err
	}

	// The default manifest begins at UTC today, but an airing that started on
	// the prior UTC date can still be live. Fetch that permitted day explicitly
	// and merge it into the full-horizon authority. Expired prior-day airings are
	// validated but filtered before detail fetch/publication below.
	priorReqs := priorScheduleRequests(md5Reqs, authorities, manifestFinishedUTC)
	if len(priorReqs) > 0 {
		priorManifest, err := i.Client.ScheduleMD5(ctx, priorReqs)
		if err != nil {
			return s, err
		}
		if _, err := buildScheduleAuthorities(priorReqs, priorManifest); err != nil {
			return s, err
		}
		for stationID, entries := range priorManifest {
			for date, entry := range entries {
				if _, duplicate := md5Manifest[stationID][date]; duplicate {
					return s, fmt.Errorf("schedule MD5 response duplicated station/date %s/%s across horizon requests", stationID, date)
				}
				md5Manifest[stationID][date] = entry
			}
		}
	}
	authorities, err = buildScheduleAuthorities(md5Reqs, md5Manifest)
	if err != nil {
		return s, err
	}

	changedReqs := make([]ScheduleRequest, 0, len(md5Reqs))
	changedAuthorities := make(map[string]stationScheduleAuthority, len(md5Reqs))
	for _, req := range md5Reqs {
		authority := authorities[req.StationID]
		oldMD5, err := i.DB.GetSDStationMD5(ctx, l.SDLineupID, req.StationID)
		if err != nil {
			return s, fmt.Errorf("read SD station %s watermark: %w", req.StationID, err)
		}
		if oldMD5 != "" && oldMD5 == authority.Watermark {
			s.StationsSkipped++
			continue
		}
		changedReqs = append(changedReqs, ScheduleRequest{
			StationID: req.StationID,
			Dates:     append([]string(nil), authority.Dates...),
		})
		changedAuthorities[req.StationID] = authority
	}
	if len(changedReqs) == 0 {
		if err := reconcileLineup(); err != nil {
			return s, err
		}
		return s, nil
	}

	// 5. Request every explicit date for changed stations and require an exact
	//    match to the MD5 manifest before any details are fetched or state is
	//    published. This rejects silent station/day truncation.
	dailySchedules, err := i.Client.Schedules(ctx, changedReqs)
	if err != nil {
		return s, err
	}
	stationsWithChanges, err := aggregateDailySchedules(changedReqs, changedAuthorities, dailySchedules)
	if err != nil {
		return s, err
	}
	s.StationsFetched += len(stationsWithChanges)

	// 6. Collect the exact program-detail response set needed by mapped,
	//    changed stations.
	snapshotCutoff := time.Now().UTC()
	needPrograms := map[string]string{}
	for _, sr := range stationsWithChanges {
		for _, p := range sr.Programs {
			if strings.TrimSpace(p.ProgramID) == "" {
				return s, fmt.Errorf("station %s schedule contains an empty program id", sr.StationID)
			}
			if strings.TrimSpace(p.MD5) == "" {
				return s, fmt.Errorf("station %s schedule program %s has an empty MD5", sr.StationID, p.ProgramID)
			}
			_, end, err := scheduledProgramInterval(p)
			if err != nil {
				return s, fmt.Errorf("station %s program %s: %w", sr.StationID, p.ProgramID, err)
			}
			if !end.After(snapshotCutoff) {
				continue
			}
			programMD5 := strings.TrimSpace(p.MD5)
			if expectedMD5, exists := needPrograms[p.ProgramID]; exists && expectedMD5 != programMD5 {
				return s, fmt.Errorf("schedule contains conflicting MD5 values for program %s", p.ProgramID)
			}
			needPrograms[p.ProgramID] = programMD5
		}
	}

	// 7. Fetch and validate program details as an exact response set.
	progByID := map[string]ProgramDetails{}
	if len(needPrograms) > 0 {
		progIDs := make([]string, 0, len(needPrograms))
		for id := range needPrograms {
			progIDs = append(progIDs, id)
		}
		sort.Strings(progIDs)
		programs, err := i.Client.Programs(ctx, progIDs)
		if err != nil {
			return s, err
		}
		seenPrograms := make(map[string]struct{}, len(programs))
		for _, p := range programs {
			if strings.TrimSpace(p.ProgramID) == "" {
				return s, fmt.Errorf("program details response contains an empty program id")
			}
			if _, requested := needPrograms[p.ProgramID]; !requested {
				return s, fmt.Errorf("program details response contains unrequested program %s", p.ProgramID)
			}
			if _, duplicate := seenPrograms[p.ProgramID]; duplicate {
				return s, fmt.Errorf("program details response contains duplicate program %s", p.ProgramID)
			}
			if p.Code != 0 {
				return s, fmt.Errorf("program details response for %s has code %d", p.ProgramID, p.Code)
			}
			detailMD5 := strings.TrimSpace(p.MD5)
			if detailMD5 == "" {
				return s, fmt.Errorf("program details response for %s has an empty MD5", p.ProgramID)
			}
			if detailMD5 != needPrograms[p.ProgramID] {
				return s, fmt.Errorf("program details response for %s MD5 does not match schedule", p.ProgramID)
			}
			if pickTitle(p) == "" {
				return s, fmt.Errorf("program details response for %s has no non-empty title120", p.ProgramID)
			}
			seenPrograms[p.ProgramID] = struct{}{}
			progByID[p.ProgramID] = p
		}
		var missingPrograms []string
		for _, programID := range progIDs {
			if _, seen := seenPrograms[programID]; !seen {
				missingPrograms = append(missingPrograms, programID)
			}
		}
		if len(missingPrograms) > 0 {
			return s, fmt.Errorf("program details response omitted requested programs: %s", strings.Join(missingPrograms, ", "))
		}
	}

	// 8. Materialize every mapped station snapshot before any durable mutation.
	//    Provider data that is incomplete or semantically invalid must retain
	//    both omitted stations and retained stations' last-good guide state.
	type stagedStationSnapshot struct {
		stationID string
		md5       string
		channelID uuid.UUID
		programs  []store.EPGProgram
	}
	stagedSnapshots := make([]stagedStationSnapshot, 0, len(stationsWithChanges))
	for _, sr := range stationsWithChanges {
		ch, mapped := channelByStation[sr.StationID]
		if !mapped {
			return s, fmt.Errorf("station %s lost its fenced channel mapping", sr.StationID)
		}

		stationPrograms := make([]store.EPGProgram, 0, len(sr.Programs))
		for _, sp := range sr.Programs {
			start, end, err := scheduledProgramInterval(sp)
			if err != nil {
				return s, fmt.Errorf("station %s program %s: %w", sr.StationID, sp.ProgramID, err)
			}
			if !end.After(snapshotCutoff) {
				continue
			}
			pd, ok := progByID[sp.ProgramID]
			if !ok {
				return s, fmt.Errorf("station %s response omitted program details for %s", sr.StationID, sp.ProgramID)
			}
			title := pickTitle(pd)
			if title == "" {
				return s, fmt.Errorf("station %s response has no title for %s", sr.StationID, sp.ProgramID)
			}
			subTitle := pd.EpisodeTitle
			desc := pickDesc(pd)
			rating := pickRating(sp.Ratings)

			var oad *time.Time
			if originalAirDate := strings.TrimSpace(pd.OriginalAirDate); originalAirDate != "" {
				t, err := time.Parse("2006-01-02", originalAirDate)
				if err != nil {
					return s, fmt.Errorf("station %s program %s has invalid original air date %q",
						sr.StationID, sp.ProgramID, pd.OriginalAirDate)
				}
				oad = &t
			}

			isMovie := pd.ProgramID != "" && len(pd.ProgramID) >= 2 && pd.ProgramID[:2] == "MV"
			// Episode numbering is the whole point of an SD source: unlike the
			// community XMLTV feeds, SD carries real season/episode on every
			// airing (reruns included). Reuse the XMLTV normaliser so both the
			// onscreen ("S03E07") and xmltv_ns ("2.6.") forms are populated
			// identically to the XMLTV path.
			var epOnscreen, epXMLTV string
			if !isMovie {
				if season, episode := pd.EpisodeSeasonNum(); season > 0 && episode > 0 {
					epXMLTV, epOnscreen = epg.ParseEpisodeNum([]epg.EpisodeNumInput{
						{System: "onscreen", Value: fmt.Sprintf("S%02dE%02d", season, episode)},
					})
				}
			}

			newSignal := epg.IsNewBySignal(sp.New, derefTime(oad), start, false)
			airingCivilDate := epg.SourceCivilDate(start)
			program := store.EPGProgram{
				ChannelID:          ch.ID,
				StartAt:            start,
				EndAt:              end,
				Title:              title,
				SubTitle:           subTitle,
				Description:        desc,
				Category:           pd.Genres,
				IsMovie:            isMovie,
				IsLive:             strings.EqualFold(strings.TrimSpace(sp.LiveTapeDelay), "Live"),
				IsNew:              newSignal,
				NewExplicit:        sp.New,
				IsPremiere:         sp.Premiere || schedulePremiere(sp.PremiereOrFinale),
				IsFinale:           scheduleFinale(sp.PremiereOrFinale),
				OriginalAirDate:    oad,
				AiringCivilDate:    &airingCivilDate,
				EpisodeNumXMLTV:    epXMLTV,
				EpisodeNumOnscreen: epOnscreen,
				ProviderEpisodeID:  sdEpisodeID(pd.ProgramID, isMovie),
				Rating:             rating,
				SourcePriority:     store.PrioritySD,
			}
			program.SourceHash = sourceHashSD(sp.ProgramID, program)
			stationPrograms = append(stationPrograms, program)
		}
		stagedSnapshots = append(stagedSnapshots, stagedStationSnapshot{
			stationID: sr.StationID,
			md5:       sr.Metadata.MD5,
			channelID: ch.ID,
			programs:  stationPrograms,
		})
	}

	// 9. The provider authority is now complete. Publish retained station
	//    snapshots first; only after every non-conflicting publication succeeds
	//    may reconciliation withdraw stations omitted from this lineup. An
	//    operational publication failure can therefore leave newer retained
	//    data, but never deletes the omitted station's last-good guide.
	for _, snapshot := range stagedSnapshots {
		result, err := i.DB.ReplaceSDStationSnapshot(
			ctx, l.SDLineupID, snapshot.stationID, snapshot.md5, snapshot.channelID, snapshot.programs,
		)
		if err != nil {
			if errors.Is(err, store.ErrSDCandidateBlocked) {
				// A stronger direct candidate can temporarily own this station's
				// slot. ReplaceSDStationSnapshot rolls the attempted publication
				// back, including its MD5 watermark, so retry this station on the
				// next pass without starving later stations in the lineup.
				i.Logger.Warn("sd station snapshot blocked by stronger direct candidate; will retry",
					"lineup", l.SDLineupID, "station", snapshot.stationID, "err", err)
				s.StationsBlocked++
				blockedStations = append(blockedStations, snapshot.stationID)
				continue
			}
			return s, fmt.Errorf("publish station %s snapshot: %w", snapshot.stationID, err)
		}
		s.ProgramsAdded += result.Added
		s.ProgramsUpdated += result.Updated
		s.ProgramsDeleted += result.Deleted
		s.ProgramsUnchanged += result.Unchanged
	}
	if err := reconcileLineup(); err != nil {
		return s, err
	}
	if len(blockedStations) > 0 {
		snapshotLabel := "station snapshot"
		if len(blockedStations) != 1 {
			snapshotLabel += "s"
		}
		return s, fmt.Errorf("%w: %d %s blocked (%s)", store.ErrSDCandidateBlocked,
			len(blockedStations), snapshotLabel, strings.Join(blockedStations, ", "))
	}
	return s, nil
}

func validateLineupAuthority(expectedLineupID string, lineup *LineupResp) ([]string, error) {
	if lineup == nil {
		return nil, errors.New("lineup response is null")
	}
	if lineup.Metadata.Lineup != expectedLineupID {
		return nil, fmt.Errorf("lineup response identity %q does not match requested lineup %q",
			lineup.Metadata.Lineup, expectedLineupID)
	}
	// Nil distinguishes an omitted/null field from an explicitly empty JSON
	// array. Only the latter can authoritatively withdraw every station.
	if lineup.Stations == nil {
		return nil, errors.New("lineup response omitted the stations array")
	}
	if lineup.Map == nil {
		return nil, errors.New("lineup response omitted the map array")
	}

	stationIDs := make([]string, 0, len(lineup.Stations))
	stationSet := make(map[string]struct{}, len(lineup.Stations))
	for _, station := range lineup.Stations {
		stationID := station.StationID
		if strings.TrimSpace(stationID) != stationID || stationID == "" {
			return nil, errors.New("lineup contains an empty or whitespace-padded station id")
		}
		if _, duplicate := stationSet[stationID]; duplicate {
			return nil, fmt.Errorf("lineup contains duplicate station %s", stationID)
		}
		stationSet[stationID] = struct{}{}
		stationIDs = append(stationIDs, stationID)
	}

	mappedSet := make(map[string]struct{}, len(lineup.Map))
	for _, mapping := range lineup.Map {
		stationID := mapping.StationID
		if strings.TrimSpace(stationID) != stationID || stationID == "" {
			return nil, errors.New("lineup map contains an empty or whitespace-padded station id")
		}
		// A station may legitimately appear on more than one provider channel.
		mappedSet[stationID] = struct{}{}
	}
	for stationID := range stationSet {
		if _, mapped := mappedSet[stationID]; !mapped {
			return nil, fmt.Errorf("lineup station %s is missing from the channel map", stationID)
		}
	}
	for stationID := range mappedSet {
		if _, described := stationSet[stationID]; !described {
			return nil, fmt.Errorf("lineup channel map references undescribed station %s", stationID)
		}
	}
	return stationIDs, nil
}

func derefTime(value *time.Time) time.Time {
	if value == nil {
		return time.Time{}
	}
	return *value
}

func sdEpisodeID(programID string, isMovie bool) string {
	programID = strings.ToUpper(strings.TrimSpace(programID))
	if isMovie || !strings.HasPrefix(programID, "EP") || len(programID) <= 2 {
		return ""
	}
	for _, r := range programID[2:] {
		if (r < 'A' || r > 'Z') && (r < '0' || r > '9') {
			return ""
		}
	}
	return programID
}

type stationScheduleAuthority struct {
	Dates     []string
	DailyMD5  map[string]string
	Watermark string
}

func buildScheduleAuthorities(reqs []ScheduleRequest, manifest ScheduleMD5Response) (map[string]stationScheduleAuthority, error) {
	requested := make(map[string]struct{}, len(reqs))
	for _, req := range reqs {
		requested[req.StationID] = struct{}{}
	}

	responseStations := make([]string, 0, len(manifest))
	for stationID := range manifest {
		responseStations = append(responseStations, stationID)
	}
	sort.Strings(responseStations)
	for _, stationID := range responseStations {
		if strings.TrimSpace(stationID) == "" {
			return nil, fmt.Errorf("schedule MD5 response contains an empty station id")
		}
		if _, ok := requested[stationID]; !ok {
			return nil, fmt.Errorf("schedule MD5 response contains unrequested station %s", stationID)
		}
	}

	authorities := make(map[string]stationScheduleAuthority, len(reqs))
	var missingStations []string
	for _, req := range reqs {
		entries, ok := manifest[req.StationID]
		if !ok {
			missingStations = append(missingStations, req.StationID)
			continue
		}
		if len(entries) == 0 {
			return nil, fmt.Errorf("schedule MD5 response for station %s has no dates", req.StationID)
		}

		dates := make([]string, 0, len(entries))
		dailyMD5 := make(map[string]string, len(entries))
		for date, entry := range entries {
			if _, err := time.Parse("2006-01-02", date); err != nil {
				return nil, fmt.Errorf("schedule MD5 response for station %s has invalid date %q", req.StationID, date)
			}
			if entry.Code != 0 {
				return nil, fmt.Errorf("schedule MD5 response for station %s date %s has code %d", req.StationID, date, entry.Code)
			}
			md5 := strings.TrimSpace(entry.MD5)
			if md5 == "" {
				return nil, fmt.Errorf("schedule MD5 response for station %s date %s has an empty MD5", req.StationID, date)
			}
			dates = append(dates, date)
			dailyMD5[date] = md5
		}
		sort.Strings(dates)
		if len(req.Dates) > 0 {
			expectedDates := append([]string(nil), req.Dates...)
			sort.Strings(expectedDates)
			if len(expectedDates) != len(dates) {
				return nil, fmt.Errorf("schedule MD5 response for station %s did not match requested dates", req.StationID)
			}
			for index := range expectedDates {
				if expectedDates[index] != dates[index] {
					return nil, fmt.Errorf("schedule MD5 response for station %s did not match requested dates", req.StationID)
				}
			}
		}
		authorities[req.StationID] = stationScheduleAuthority{
			Dates:     dates,
			DailyMD5:  dailyMD5,
			Watermark: compositeScheduleWatermark(dates, dailyMD5),
		}
	}
	if len(missingStations) > 0 {
		return nil, fmt.Errorf("schedule MD5 response omitted requested stations: %s", strings.Join(missingStations, ", "))
	}
	return authorities, nil
}

func compositeScheduleWatermark(dates []string, dailyMD5 map[string]string) string {
	hash := sha256.New()
	for _, date := range dates {
		_, _ = hash.Write([]byte(date))
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(dailyMD5[date]))
		_, _ = hash.Write([]byte{0})
	}
	return "sha256:" + hex.EncodeToString(hash.Sum(nil))
}

// A station-only /schedules/md5 request is documented to return every UTC day
// from today through the provider's furthest available date. Enforce that
// range before treating the response as snapshot authority. Accept either UTC
// date around the request only when the call itself crossed midnight.
func validateDefaultScheduleAuthorities(authorities map[string]stationScheduleAuthority, startedUTC, finishedUTC time.Time) error {
	startedDate := startedUTC.UTC().Format("2006-01-02")
	finishedDate := finishedUTC.UTC().Format("2006-01-02")
	for stationID, authority := range authorities {
		if len(authority.Dates) == 0 {
			return fmt.Errorf("schedule MD5 response for station %s has no dates", stationID)
		}
		if authority.Dates[0] != startedDate && authority.Dates[0] != finishedDate {
			return fmt.Errorf("schedule MD5 response for station %s starts at %s instead of UTC request date",
				stationID, authority.Dates[0])
		}
		previous, err := time.Parse("2006-01-02", authority.Dates[0])
		if err != nil {
			return fmt.Errorf("schedule MD5 response for station %s has invalid date %q", stationID, authority.Dates[0])
		}
		for _, date := range authority.Dates[1:] {
			expected := previous.AddDate(0, 0, 1)
			if date != expected.Format("2006-01-02") {
				return fmt.Errorf("schedule MD5 response for station %s omits UTC date %s",
					stationID, expected.Format("2006-01-02"))
			}
			previous = expected
		}
	}
	return nil
}

// priorScheduleRequests asks only for the UTC day immediately preceding the
// completed manifest request. If a call crossed midnight and the default
// response already began on that day, requesting another day back would be
// outside Schedules Direct's permitted prior-day window.
func priorScheduleRequests(reqs []ScheduleRequest, authorities map[string]stationScheduleAuthority, finishedUTC time.Time) []ScheduleRequest {
	priorDate := finishedUTC.UTC().AddDate(0, 0, -1).Format("2006-01-02")
	priorReqs := make([]ScheduleRequest, 0, len(reqs))
	for _, req := range reqs {
		alreadyPresent := false
		for _, date := range authorities[req.StationID].Dates {
			if date == priorDate {
				alreadyPresent = true
				break
			}
		}
		if !alreadyPresent {
			priorReqs = append(priorReqs, ScheduleRequest{StationID: req.StationID, Dates: []string{priorDate}})
		}
	}
	return priorReqs
}

// Schedules Direct returns one response element per station/date. Require an
// exact match to the independently fetched MD5 manifest, then collapse the
// validated days into one deterministic full-horizon station snapshot.
func aggregateDailySchedules(reqs []ScheduleRequest, authorities map[string]stationScheduleAuthority, responses []ScheduleResp) ([]ScheduleResp, error) {
	daysByStation := make(map[string][]ScheduleResp, len(authorities))
	seenStationDates := make(map[string]struct{}, len(responses))
	for _, response := range responses {
		if strings.TrimSpace(response.StationID) == "" {
			return nil, fmt.Errorf("schedules response contains an empty station id")
		}
		authority, requested := authorities[response.StationID]
		if !requested {
			return nil, fmt.Errorf("schedules response contains unrequested station %s", response.StationID)
		}
		if response.Code != 0 {
			return nil, fmt.Errorf("schedules response for station %s has code %d", response.StationID, response.Code)
		}
		if response.Metadata.Code != 0 {
			return nil, fmt.Errorf("schedules response for station %s has metadata code %d", response.StationID, response.Metadata.Code)
		}
		startDate := strings.TrimSpace(response.Metadata.StartDate)
		if _, err := time.Parse("2006-01-02", startDate); err != nil {
			return nil, fmt.Errorf("schedules response for station %s has invalid start date %q", response.StationID, response.Metadata.StartDate)
		}
		expectedMD5, expectedDate := authority.DailyMD5[startDate]
		if !expectedDate {
			return nil, fmt.Errorf("schedules response contains unrequested station/date %s/%s", response.StationID, startDate)
		}
		actualMD5 := strings.TrimSpace(response.Metadata.MD5)
		if actualMD5 == "" {
			return nil, fmt.Errorf("schedules response for station %s date %s has an empty MD5", response.StationID, startDate)
		}
		if actualMD5 != expectedMD5 {
			return nil, fmt.Errorf("schedules response for station %s date %s MD5 does not match manifest", response.StationID, startDate)
		}
		if len(response.Programs) == 0 {
			return nil, fmt.Errorf("schedules response for station %s date %s has no programs", response.StationID, startDate)
		}
		stationDate := response.StationID + "\x00" + startDate
		if _, duplicate := seenStationDates[stationDate]; duplicate {
			return nil, fmt.Errorf("schedules response contains duplicate station/date %s/%s", response.StationID, startDate)
		}
		seenStationDates[stationDate] = struct{}{}
		response.Metadata.StartDate = startDate
		daysByStation[response.StationID] = append(daysByStation[response.StationID], response)
	}

	aggregated := make([]ScheduleResp, 0, len(reqs))
	var missingStations []string
	for _, req := range reqs {
		days := daysByStation[req.StationID]
		sort.Slice(days, func(a, b int) bool {
			return days[a].Metadata.StartDate < days[b].Metadata.StartDate
		})
		authority := authorities[req.StationID]
		var missingDates []string
		for _, date := range authority.Dates {
			if _, seen := seenStationDates[req.StationID+"\x00"+date]; !seen {
				missingDates = append(missingDates, req.StationID+"/"+date)
			}
		}
		if len(missingDates) > 0 {
			missingStations = append(missingStations, missingDates...)
			continue
		}
		combined := ScheduleResp{StationID: req.StationID}
		for _, day := range days {
			combined.Programs = append(combined.Programs, day.Programs...)
		}
		combined.Metadata.StartDate = days[0].Metadata.StartDate
		combined.Metadata.MD5 = authority.Watermark
		aggregated = append(aggregated, combined)
	}
	if len(missingStations) > 0 {
		return nil, fmt.Errorf("schedules response omitted requested station/dates: %s", strings.Join(missingStations, ", "))
	}
	return aggregated, nil
}

func schedulePremiere(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "season premiere", "series premiere", "premiere":
		return true
	default:
		return false
	}
}

func scheduleFinale(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "season finale", "series finale", "finale":
		return true
	default:
		return false
	}
}

// pickTitle returns the first non-empty title120 (SD sometimes has multiple).
func pickTitle(p ProgramDetails) string {
	for _, t := range p.Titles {
		if title := strings.TrimSpace(t.Title120); title != "" {
			return title
		}
	}
	return ""
}

// pickDesc returns description1000 if present, else description100.
func pickDesc(p ProgramDetails) string {
	for _, d := range p.Descriptions.Description1000 {
		if d.Description != "" {
			return d.Description
		}
	}
	for _, d := range p.Descriptions.Description100 {
		if d.Description != "" {
			return d.Description
		}
	}
	return ""
}

func pickRating(rs []ScheduleRating) string {
	for _, r := range rs {
		if r.Body == "USA Parental Rating" || strings.HasPrefix(r.Body, "MPAA") {
			return r.Code
		}
	}
	if len(rs) > 0 {
		return rs[0].Code
	}
	return ""
}

func parseSDTime(s string) (time.Time, error) {
	// SD timestamps are ISO 8601 with Z suffix, e.g. "2026-05-09T01:00:00Z".
	return time.Parse(time.RFC3339, s)
}

func scheduledProgramInterval(program ScheduledProgram) (time.Time, time.Time, error) {
	start, err := parseSDTime(program.AirDateTime)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("has invalid airtime: %w", err)
	}
	end := start.Add(time.Duration(program.Duration) * time.Second)
	if !end.After(start) {
		return time.Time{}, time.Time{}, fmt.Errorf("has non-positive duration")
	}
	return start, end, nil
}

func sourceHashSD(programID string, program store.EPGProgram) string {
	h := sha256.New()
	h.Write([]byte("sd|" + programID + "|" + epg.ProgramContentHash(program)))
	return "sd:" + hex.EncodeToString(h.Sum(nil))[:32]
}

// _ uuid import keeps test+admin path consistent with other store helpers.
var _ = uuid.New
