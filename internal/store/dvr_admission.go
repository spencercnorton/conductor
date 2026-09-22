package store

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// DVRAdmissionPolicy is shared by the schedule-time forecast and runtime
// admission. The forecast never reserves a future slot; it only makes a
// deterministic conflict visible until AcquireDVRLease revalidates reality at
// padded start. Earlier padded starts are non-preemptible; priority resolves
// only recordings that become eligible at the same start boundary.
type DVRAdmissionPolicy struct {
	LiveReserve int
	StartSlack  time.Duration
	MaxOverrun  time.Duration
}

const (
	defaultDVRForecastLiveReserve = 1
	defaultDVRForecastStartSlack  = 30 * time.Second
	defaultDVRForecastMaxOverrun  = 90 * time.Second
	dvrForecastReasonPrefix       = "capacity forecast:"
)

func defaultDVRAdmissionPolicy() DVRAdmissionPolicy {
	return DVRAdmissionPolicy{
		LiveReserve: defaultDVRForecastLiveReserve,
		StartSlack:  defaultDVRForecastStartSlack,
		MaxOverrun:  defaultDVRForecastMaxOverrun,
	}
}

func normalizeDVRAdmissionPolicy(p DVRAdmissionPolicy) DVRAdmissionPolicy {
	if p.LiveReserve < 0 {
		p.LiveReserve = 0
	}
	// Runtime treats zero duration as "use Recorder defaults" too.
	if p.StartSlack <= 0 {
		p.StartSlack = defaultDVRForecastStartSlack
	}
	if p.MaxOverrun <= 0 {
		p.MaxOverrun = defaultDVRForecastMaxOverrun
	}
	return p
}

type dvrForecastRecording struct {
	id        uuid.UUID
	channelID uuid.UUID
	start     time.Time
	end       time.Time
	priority  int
	state     string
}

type dvrForecastProvider struct {
	id               uuid.UUID
	capacityDomain   string
	priority         int
	health           float64
	providerCapacity int
	domainCapacity   int
	attachOnly       bool
}

type dvrForecastAllocation struct {
	providerID     uuid.UUID
	capacityDomain string
	channelID      uuid.UUID
	start          time.Time
	end            time.Time
	countsCapacity bool
}

type dvrForecastActiveProvider struct {
	id             uuid.UUID
	capacityDomain string
	countsCapacity bool
}

// RefreshDVRAdmissionForecast recomputes the advisory conflict surface for all
// still-scheduled recordings while treating recording rows as fixed existing
// commitments. A global transaction-scoped advisory lock makes concurrent
// schedule requests converge on one complete deterministic order. Runtime
// queue state (a reason not beginning with dvrForecastReasonPrefix) is never
// overwritten.
func (db *DB) RefreshDVRAdmissionForecast(ctx context.Context, policy DVRAdmissionPolicy) error {
	policy = normalizeDVRAdmissionPolicy(policy)
	return db.InTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			SELECT pg_advisory_xact_lock(
				hashtextextended('conductor:dvr-admission-forecast', 0)
			)`); err != nil {
			return fmt.Errorf("lock dvr admission forecast: %w", err)
		}

		recordings, err := loadDVRForecastRecordings(ctx, tx, policy)
		if err != nil || len(recordings) == 0 {
			return err
		}
		activeProviders, err := loadDVRForecastActiveProviders(ctx, tx, recordings)
		if err != nil {
			return err
		}
		providers, err := loadDVRForecastProviders(ctx, tx, recordings, activeProviders, policy.LiveReserve)
		if err != nil {
			return err
		}

		var allocations []dvrForecastAllocation
		conflicts := make(map[uuid.UUID]string)
		// A recording already in progress cannot be displaced by a newly
		// scheduled higher-priority row. Account for it first, on the provider
		// its real active_stream currently uses whenever that evidence exists.
		for _, rec := range recordings {
			if rec.state != "recording" {
				continue
			}
			activeProvider, ok := activeProviders[rec.channelID]
			providerID := activeProvider.id
			capacityDomain := activeProvider.capacityDomain
			countsCapacity := activeProvider.countsCapacity
			if !ok {
				options := providers[rec.channelID]
				if len(options) == 0 {
					continue
				}
				providerID = options[0].id
				capacityDomain = options[0].capacityDomain
				countsCapacity = true
			}
			allocations = append(allocations, dvrForecastAllocation{
				providerID: providerID, capacityDomain: capacityDomain,
				channelID: rec.channelID, start: rec.start, end: rec.end,
				countsCapacity: countsCapacity,
			})
		}
		for _, rec := range recordings {
			if rec.state != "scheduled" {
				continue
			}
			options := providers[rec.channelID]
			chosen, countsCapacity, ok := chooseDVRForecastProvider(rec, options, allocations)
			if !ok {
				conflicts[rec.id] = fmt.Sprintf(
					"%s priority=%d cannot fit the provider/domain DVR budget during padded window %s to %s; runtime admission remains authoritative",
					dvrForecastReasonPrefix, rec.priority,
					rec.start.UTC().Format(time.RFC3339), rec.end.UTC().Format(time.RFC3339))
				continue
			}
			allocations = append(allocations, dvrForecastAllocation{
				providerID: chosen.id, capacityDomain: chosen.capacityDomain,
				channelID: rec.channelID, start: rec.start, end: rec.end,
				countsCapacity: countsCapacity,
			})
		}

		for _, rec := range recordings {
			if rec.state != "scheduled" {
				continue
			}
			if reason, conflict := conflicts[rec.id]; conflict {
				if _, err := tx.Exec(ctx, `
					UPDATE dvr_recording
					   SET admission_state = 'queued', admission_retry_at = $2,
					       admission_reason = $3, error = $3
					 WHERE id = $1 AND state = 'scheduled'
					   AND (admission_state = 'pending'
					        OR admission_reason LIKE $4)`,
					rec.id, rec.start, reason, dvrForecastReasonPrefix+"%"); err != nil {
					return fmt.Errorf("surface dvr admission forecast: %w", err)
				}
				continue
			}
			if _, err := tx.Exec(ctx, `
				UPDATE dvr_recording
				   SET admission_state = 'pending', admission_retry_at = NULL,
				       admission_reason = '', error = ''
				 WHERE id = $1 AND state = 'scheduled'
				   AND admission_reason LIKE $2`,
				rec.id, dvrForecastReasonPrefix+"%"); err != nil {
				return fmt.Errorf("clear dvr admission forecast: %w", err)
			}
		}
		return nil
	})
}

func loadDVRForecastRecordings(ctx context.Context, tx pgx.Tx, policy DVRAdmissionPolicy) ([]dvrForecastRecording, error) {
	rows, err := tx.Query(ctx, `
		SELECT id, channel_id, scheduled_start, scheduled_end, priority, state::text
		  FROM dvr_recording
		 WHERE state IN ('scheduled', 'recording')
		 ORDER BY (state = 'recording') DESC,
		          scheduled_start ASC, priority ASC, created_at ASC, id ASC`)
	if err != nil {
		return nil, fmt.Errorf("list dvr forecast recordings: %w", err)
	}
	defer rows.Close()

	var out []dvrForecastRecording
	for rows.Next() {
		var r dvrForecastRecording
		if err := rows.Scan(&r.id, &r.channelID, &r.start, &r.end, &r.priority, &r.state); err != nil {
			return nil, err
		}
		r.start = r.start.Add(-policy.StartSlack)
		r.end = r.end.Add(policy.MaxOverrun)
		out = append(out, r)
	}
	return out, rows.Err()
}

func loadDVRForecastProviders(
	ctx context.Context,
	tx pgx.Tx,
	recordings []dvrForecastRecording,
	activeProviders map[uuid.UUID]dvrForecastActiveProvider,
	liveReserve int,
) (map[uuid.UUID][]dvrForecastProvider, error) {
	channelSet := make(map[uuid.UUID]struct{})
	for _, rec := range recordings {
		channelSet[rec.channelID] = struct{}{}
	}
	channelIDs := make([]uuid.UUID, 0, len(channelSet))
	for id := range channelSet {
		channelIDs = append(channelIDs, id)
	}

	// Pick one real source tuple per provider. Independent MIN(priority) and
	// MAX(health_score) aggregates can synthesize a provider rank no source
	// actually has (for example 0/.1 plus 100/1.0 appearing as 0/1.0).
	// This ordering is identical to ListSourcesForChannel's runtime order.
	rows, err := tx.Query(ctx, `
		SELECT ranked.channel_id, ranked.provider_id, p.capacity_domain,
		       ranked.priority, ranked.health_score
		  FROM (
		        SELECT cs.channel_id, cs.provider_id, cs.priority, cs.health_score,
		               row_number() OVER (
		                   PARTITION BY cs.channel_id, cs.provider_id
		                   ORDER BY cs.priority ASC, cs.health_score DESC,
		                            cs.provider_id ASC, cs.id ASC
		               ) AS provider_rank
		          FROM channel_source cs
		         WHERE cs.enabled AND cs.channel_id = ANY($1)
		       ) ranked
		  JOIN provider p ON p.id = ranked.provider_id
		 WHERE provider_rank = 1
		 ORDER BY ranked.channel_id, ranked.priority ASC,
		          ranked.health_score DESC, ranked.provider_id ASC`, channelIDs)
	if err != nil {
		return nil, fmt.Errorf("list dvr forecast providers: %w", err)
	}
	providers := make(map[uuid.UUID][]dvrForecastProvider)
	providerSet := make(map[uuid.UUID]struct{})
	for rows.Next() {
		var channelID uuid.UUID
		var p dvrForecastProvider
		var configuredDomain string
		if err := rows.Scan(
			&channelID, &p.id, &configuredDomain, &p.priority, &p.health,
		); err != nil {
			rows.Close()
			return nil, err
		}
		p.capacityDomain = effectiveCapacityDomain(p.id, configuredDomain)
		providers[channelID] = append(providers[channelID], p)
		providerSet[p.id] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	providerIDs := make([]uuid.UUID, 0, len(providerSet))
	for _, activeProvider := range activeProviders {
		providerSet[activeProvider.id] = struct{}{}
	}
	for id := range providerSet {
		providerIDs = append(providerIDs, id)
	}
	providerCapacity := make(map[uuid.UUID]int)
	if len(providerIDs) > 0 {
		capRows, err := tx.Query(ctx, `
			SELECT provider_id, COALESCE(SUM(max_streams), 0)::int
			  FROM provider_credential
			 WHERE enabled AND provider_id = ANY($1)
			 GROUP BY provider_id`, providerIDs)
		if err != nil {
			return nil, fmt.Errorf("load dvr forecast capacity: %w", err)
		}
		for capRows.Next() {
			var providerID uuid.UUID
			var total int
			if err := capRows.Scan(&providerID, &total); err != nil {
				return nil, err
			}
			providerCapacity[providerID] = total
		}
		if err := capRows.Err(); err != nil {
			capRows.Close()
			return nil, err
		}
		capRows.Close()
	}

	// Aggregate quota budgets include every enabled credential in a named
	// domain, even when a particular recording cannot source from every member.
	// The provider-local fit below prevents that aggregate from inventing access
	// to another provider's credential.
	domainCapacity := make(map[string]int)
	domainRows, err := tx.Query(ctx, `
		SELECT CASE WHEN p.capacity_domain = ''
		            THEN 'provider:' || p.id::text
		            ELSE 'domain:' || p.capacity_domain
		       END AS capacity_domain,
		       COALESCE(SUM(pc.max_streams), 0)::int
		  FROM provider p
		  JOIN provider_credential pc ON pc.provider_id = p.id
		 WHERE pc.enabled
		 GROUP BY 1`)
	if err != nil {
		return nil, fmt.Errorf("load dvr forecast domain capacity: %w", err)
	}
	for domainRows.Next() {
		var domain string
		var total int
		if err := domainRows.Scan(&domain, &total); err != nil {
			domainRows.Close()
			return nil, err
		}
		reserve := liveReserve
		if reserve < 0 {
			reserve = 0
		}
		if total > 0 && reserve >= total {
			reserve = total - 1
		}
		domainCapacity[domain] = total - reserve
	}
	if err := domainRows.Err(); err != nil {
		domainRows.Close()
		return nil, err
	}
	domainRows.Close()

	for channelID, options := range providers {
		for i := range options {
			options[i].providerCapacity = providerCapacity[options[i].id]
			options[i].domainCapacity = domainCapacity[options[i].capacityDomain]
		}
		// The SQL order is deterministic, but keep the rule local as well so a
		// future query refactor cannot silently change forecast allocation.
		sort.SliceStable(options, func(i, j int) bool {
			if options[i].priority != options[j].priority {
				return options[i].priority < options[j].priority
			}
			if options[i].health != options[j].health {
				return options[i].health > options[j].health
			}
			return options[i].id.String() < options[j].id.String()
		})
		providers[channelID] = options
	}
	// AcquireDVRLease searches the whole logical channel for an existing
	// stream before considering the current candidate source. Put the actual
	// provider first so an overlapping same-channel schedule mirrors that
	// attachment even when the pump has relocated to a fallback provider.
	for channelID, activeProvider := range activeProviders {
		options := providers[channelID]
		if len(options) == 0 {
			continue
		}
		actual := dvrForecastProvider{
			id: activeProvider.id, capacityDomain: activeProvider.capacityDomain,
			priority: -1, health: 2,
			providerCapacity: providerCapacity[activeProvider.id],
			domainCapacity:   domainCapacity[activeProvider.capacityDomain],
			attachOnly:       true,
		}
		found := -1
		for i := range options {
			if options[i].id == activeProvider.id {
				actual = options[i]
				found = i
				break
			}
		}
		if found >= 0 {
			options = append(options[:found], options[found+1:]...)
		}
		providers[channelID] = append([]dvrForecastProvider{actual}, options...)
	}
	return providers, nil
}

func loadDVRForecastActiveProviders(
	ctx context.Context,
	tx pgx.Tx,
	recordings []dvrForecastRecording,
) (map[uuid.UUID]dvrForecastActiveProvider, error) {
	recordingChannels := make(map[uuid.UUID]struct{})
	for _, rec := range recordings {
		if rec.state == "recording" {
			recordingChannels[rec.channelID] = struct{}{}
		}
	}
	if len(recordingChannels) == 0 {
		return map[uuid.UUID]dvrForecastActiveProvider{}, nil
	}
	channelIDs := make([]uuid.UUID, 0, len(recordingChannels))
	for channelID := range recordingChannels {
		channelIDs = append(channelIDs, channelID)
	}
	rows, err := tx.Query(ctx, `
		SELECT DISTINCT ON (a.channel_id)
		       a.channel_id, pc.provider_id, p.capacity_domain, pc.enabled
		  FROM active_stream a
		  JOIN provider_credential pc ON pc.id = a.credential_id
		  JOIN provider p ON p.id = pc.provider_id
		 WHERE a.channel_id = ANY($1)
		   AND a.state IN ('starting', 'running')
		 ORDER BY a.channel_id, a.started_at, a.id`, channelIDs)
	if err != nil {
		return nil, fmt.Errorf("load recording active providers: %w", err)
	}
	defer rows.Close()
	out := make(map[uuid.UUID]dvrForecastActiveProvider)
	for rows.Next() {
		var channelID, providerID uuid.UUID
		var configuredDomain string
		var enabled bool
		if err := rows.Scan(&channelID, &providerID, &configuredDomain, &enabled); err != nil {
			return nil, err
		}
		out[channelID] = dvrForecastActiveProvider{
			id:             providerID,
			capacityDomain: effectiveCapacityDomain(providerID, configuredDomain),
			countsCapacity: enabled,
		}
	}
	return out, rows.Err()
}

func chooseDVRForecastProvider(
	rec dvrForecastRecording,
	options []dvrForecastProvider,
	allocations []dvrForecastAllocation,
) (dvrForecastProvider, bool, bool) {
	// Runtime attaches to an existing logical-channel stream before examining
	// the candidate source's provider. Mirror that property across overlapping
	// scheduled windows so same-channel DVRs never forecast an extra slot.
	for _, option := range options {
		providerAllocations := dvrForecastAllocationsForProvider(allocations, option.id)
		if dvrForecastSameChannelCovers(rec, providerAllocations) {
			return option, false, true
		}
		partialOverlap := false
		partialCountsCapacity := false
		for _, a := range providerAllocations {
			if a.channelID == rec.channelID &&
				intervalsOverlap(rec.start, rec.end, a.start, a.end) {
				partialOverlap = true
				partialCountsCapacity = partialCountsCapacity || a.countsCapacity
			}
		}
		if partialOverlap && (!partialCountsCapacity ||
			dvrForecastOptionFits(rec, option, allocations)) {
			// A partial overlap attaches at runtime, but the later recording can
			// outlive the allocation it attached to. Carry the underlying pump's
			// capacity role forward so a same-channel chain cannot make its slot
			// disappear when the first recording ends. A disabled-credential pump
			// remains non-counting because it is outside the enabled-account budget.
			return option, partialCountsCapacity, true
		}
	}
	for _, option := range options {
		if option.attachOnly {
			continue
		}
		if dvrForecastOptionFits(rec, option, allocations) {
			return option, true, true
		}
	}
	return dvrForecastProvider{}, false, false
}

func dvrForecastAllocationsForProvider(
	allocations []dvrForecastAllocation,
	providerID uuid.UUID,
) []dvrForecastAllocation {
	out := make([]dvrForecastAllocation, 0, len(allocations))
	for _, allocation := range allocations {
		if allocation.providerID == providerID {
			out = append(out, allocation)
		}
	}
	return out
}

func dvrForecastAllocationsForDomain(
	allocations []dvrForecastAllocation,
	capacityDomain string,
) []dvrForecastAllocation {
	out := make([]dvrForecastAllocation, 0, len(allocations))
	for _, allocation := range allocations {
		if allocation.capacityDomain == capacityDomain {
			out = append(out, allocation)
		}
	}
	return out
}

func dvrForecastOptionFits(
	rec dvrForecastRecording,
	option dvrForecastProvider,
	allocations []dvrForecastAllocation,
) bool {
	return dvrForecastCapacityFits(
		rec, option.providerCapacity,
		dvrForecastAllocationsForProvider(allocations, option.id),
	) && dvrForecastCapacityFits(
		rec, option.domainCapacity,
		dvrForecastAllocationsForDomain(allocations, option.capacityDomain),
	)
}

func dvrForecastSameChannelCovers(rec dvrForecastRecording, allocations []dvrForecastAllocation) bool {
	var intervals []dvrForecastAllocation
	for _, a := range allocations {
		if a.channelID == rec.channelID && intervalsOverlap(rec.start, rec.end, a.start, a.end) {
			intervals = append(intervals, a)
		}
	}
	if len(intervals) == 0 {
		return false
	}
	sort.Slice(intervals, func(i, j int) bool { return intervals[i].start.Before(intervals[j].start) })
	coveredUntil := rec.start
	for _, interval := range intervals {
		if interval.start.After(coveredUntil) {
			return false
		}
		if interval.end.After(coveredUntil) {
			coveredUntil = interval.end
		}
		if !coveredUntil.Before(rec.end) {
			return true
		}
	}
	return false
}

func dvrForecastCapacityFits(rec dvrForecastRecording, capacity int, allocations []dvrForecastAllocation) bool {
	if capacity <= 0 || !rec.end.After(rec.start) {
		return false
	}
	// With half-open intervals, concurrency can increase only at the
	// candidate's start or another allocation's start. Check those points and
	// count distinct channels, not recordings, to preserve fan-out sharing.
	points := []time.Time{rec.start}
	for _, a := range allocations {
		if a.countsCapacity && intervalsOverlap(rec.start, rec.end, a.start, a.end) &&
			a.start.After(rec.start) && a.start.Before(rec.end) {
			points = append(points, a.start)
		}
	}
	for _, point := range points {
		channels := make(map[uuid.UUID]struct{})
		for _, a := range allocations {
			if !a.countsCapacity || a.channelID == rec.channelID {
				continue
			}
			if !point.Before(a.start) && point.Before(a.end) {
				channels[a.channelID] = struct{}{}
			}
		}
		if len(channels) >= capacity {
			return false
		}
	}
	return true
}

func intervalsOverlap(aStart, aEnd, bStart, bEnd time.Time) bool {
	return aStart.Before(bEnd) && aEnd.After(bStart)
}
