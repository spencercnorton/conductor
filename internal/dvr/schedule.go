package dvr

import (
	"context"
	"encoding/binary"
	"errors"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/spencercnorton/conductor/internal/store"
)

// ScheduleHandler is mounted at GET /dvr/schedule.torrent.
//
// Sonarr/Radarr "grab" a Torznab result by GET-ing the enclosure URL.
// We respond with:
//   - 200 + a tiny placeholder .torrent file (so download clients are happy)
//   - and have ALREADY persisted the dvr_recording row before returning
//
// The placeholder torrent is intentionally never going to download anything
// real — *arr's torrent download client will sit idle waiting for it.
// The actual delivery path is: Conductor records the program, writes the
// finished file into CONDUCTOR_DVR_OUTPUT_DIR, and *arr's
// "completed download handling" picks the file up via library scan or via
// a configured remote-download-path mapping.
//
// Operator setup is documented in docs/enhancements.md §10.
type ScheduleHandler struct {
	DB         *store.DB
	Torznab    *Torznab // for VerifyScheduleURL
	OutputDir  string   // CONDUCTOR_DVR_OUTPUT_DIR — root for recorded files
	IndexerKey string   // same APIKey, also accepted as `apikey=`
	Priority   int      // lower wins equal-start conflicts; config default 100
	// AdmissionPolicy is set by main so the advisory schedule-time forecast
	// uses the same live reserve and padded window as runtime admission. nil
	// uses store defaults for tests and in-process callers.
	AdmissionPolicy *store.DVRAdmissionPolicy
}

func (h *ScheduleHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	if h.IndexerKey == "" || q.Get("apikey") != h.IndexerKey {
		http.Error(w, "invalid api key", http.StatusUnauthorized)
		return
	}

	progID, chID, start, end, err := h.Torznab.VerifyScheduleURL(q.Get("ec"), q.Get("sig"))
	if err != nil {
		http.Error(w, "invalid schedule link: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Pull program for metadata (title, sub-title, episode num).
	prog, err := h.lookupProgram(r.Context(), progID)
	if err != nil {
		http.Error(w, "program not found", http.StatusNotFound)
		return
	}

	output := buildOutputPath(h.OutputDir, prog, start)

	intent := store.DVRRecording{
		ChannelID:          chID,
		ProgramID:          &progID,
		Title:              prog.Title,
		SubTitle:           prog.SubTitle,
		EpisodeNumXMLTV:    prog.EpisodeNumXMLTV,
		EpisodeNumOnscreen: prog.EpisodeNumOnscreen,
		IsMovie:            prog.IsMovie,
		ScheduledStart:     start,
		ScheduledEnd:       end,
		Priority:           h.Priority,
		RequestedBy:        sourceTag(r),
		OutputPath:         output,
	}
	var policies []store.DVRAdmissionPolicy
	if h.AdmissionPolicy != nil {
		policies = append(policies, *h.AdmissionPolicy)
	}
	rec, err := CreateDVRRecordingChecked(r.Context(), h.DB, intent, policies...)
	// ErrOutputPathBusy means another airing of this episode already owns the
	// file. Sonarr expects a grab to be idempotent, so hand back the existing
	// recording's torrent rather than starting a second writer on it.
	if err != nil && !errors.Is(err, store.ErrOutputPathBusy) &&
		!errors.Is(err, store.ErrValidatedArtifactExists) {
		http.Error(w, "schedule failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/x-bittorrent")
	w.Header().Set("Content-Disposition",
		`attachment; filename="conductor-`+rec.ID.String()+`.torrent"`)
	_, _ = w.Write(placeholderTorrent(rec.ID, prog))
}

// sourceTag identifies who's grabbing — we record it so the operator
// dashboard can see "this recording was requested by Sonarr at <ip>".
func sourceTag(r *http.Request) string {
	ua := r.Header.Get("User-Agent")
	switch {
	case strings.Contains(strings.ToLower(ua), "sonarr"):
		return "torznab:sonarr"
	case strings.Contains(strings.ToLower(ua), "radarr"):
		return "torznab:radarr"
	case strings.Contains(strings.ToLower(ua), "prowlarr"):
		return "torznab:prowlarr"
	default:
		return "torznab:unknown"
	}
}

// lookupProgram is a thin store helper. Defined here so the lookup query
// stays close to its only consumer.
type programDetail struct {
	ChannelID          uuid.UUID
	StartAt            time.Time
	EndAt              time.Time
	Title              string
	SubTitle           string
	EpisodeNumXMLTV    string
	EpisodeNumOnscreen string
	IsMovie            bool
	ChannelName        string
}

func (h *ScheduleHandler) lookupProgram(ctx context.Context, id uuid.UUID) (programDetail, error) {
	return h.lookupProgramRow(ctx, id, true)
}

// lookupReplacementProgram permits a previously persisted exact replacement
// to replay after its guide row stopped being canonical. CreateDVRReplacement
// independently requires canonical metadata before authorizing any new row.
func (h *ScheduleHandler) lookupReplacementProgram(
	ctx context.Context,
	id uuid.UUID,
) (programDetail, error) {
	return h.lookupProgramRow(ctx, id, false)
}

func (h *ScheduleHandler) lookupProgramRow(
	ctx context.Context,
	id uuid.UUID,
	canonicalOnly bool,
) (programDetail, error) {
	var p programDetail
	canonicalClause := ""
	if canonicalOnly {
		canonicalClause = " AND p.is_canonical"
	}
	row := h.DB.Pool.QueryRow(ctx, `
		SELECT p.channel_id, p.start_at, p.end_at,
		       p.title, p.sub_title, p.episode_num_xmltv, p.episode_num_onscreen,
		       p.is_movie, c.name
		  FROM epg_program p
		  JOIN channel c ON c.id = p.channel_id
		 WHERE p.id = $1`+canonicalClause, id)
	err := row.Scan(&p.ChannelID, &p.StartAt, &p.EndAt,
		&p.Title, &p.SubTitle, &p.EpisodeNumXMLTV, &p.EpisodeNumOnscreen,
		&p.IsMovie, &p.ChannelName)
	if errors.Is(err, pgx.ErrNoRows) {
		return programDetail{}, store.ErrNotFound
	}
	return p, err
}

// ErrReplacementTargetNotFuture aliases the store's lock-bound database-clock
// decision for API compatibility.
var ErrReplacementTargetNotFuture = store.ErrArtifactReplacementTargetNotFuture

// ScheduleReplacement binds authenticated operator intent to one exact future
// EPG programme. The store verifies that the selected owner is still the
// current validated artifact for this canonical output path in the same
// transaction that inserts the successor.
func (h *ScheduleHandler) ScheduleReplacement(
	ctx context.Context,
	ownerID, programID uuid.UUID,
	requestedBy string,
) (store.DVRRecording, error) {
	// Idempotent API retries must not re-authorize mutable state. Return only a
	// successor already bound to this exact owner/program pair before checking
	// the old owner's filesystem, guide canonicality, or padded-start deadline.
	if existing, err := h.DB.GetDVRReplacementByTarget(
		ctx, ownerID, programID,
	); err == nil {
		return existing, nil
	} else if !errors.Is(err, store.ErrNotFound) {
		return store.DVRRecording{}, err
	}
	owner, err := h.DB.GetDVRRecording(ctx, ownerID)
	if err != nil {
		return store.DVRRecording{}, err
	}
	if owner.ArtifactCurrent &&
		(owner.ArtifactState == "legacy" || owner.ArtifactState == "validated") {
		if _, err := ReconcileCurrentDVRArtifact(ctx, h.DB, owner); err != nil {
			return store.DVRRecording{}, err
		}
	}
	prog, err := h.lookupReplacementProgram(ctx, programID)
	if err != nil {
		return store.DVRRecording{}, err
	}
	intent := store.DVRRecording{
		ChannelID:          prog.ChannelID,
		ProgramID:          &programID,
		Title:              prog.Title,
		SubTitle:           prog.SubTitle,
		EpisodeNumXMLTV:    prog.EpisodeNumXMLTV,
		EpisodeNumOnscreen: prog.EpisodeNumOnscreen,
		IsMovie:            prog.IsMovie,
		ScheduledStart:     prog.StartAt,
		ScheduledEnd:       prog.EndAt,
		Priority:           h.Priority,
		RequestedBy:        requestedBy,
		// Re-recording replaces one exact artifact. Deriving this from the new
		// airing would make unnumbered repeats (timestamp paths) and corrected
		// subtitles impossible to replace safely. The store re-locks and
		// revalidates this owner/path with the target EPG identity atomically.
		OutputPath: owner.OutputPath,
	}
	if h.AdmissionPolicy == nil {
		return h.DB.CreateDVRReplacement(ctx, ownerID, intent)
	}
	return h.DB.CreateDVRReplacement(ctx, ownerID, intent, *h.AdmissionPolicy)
}

// buildOutputPath computes:
//   - <root>/Movies/Heat (1995)/Heat (1995).ts                  for movies
//   - <root>/TV/<Show>/Season 03/<Show>.S03E07.<sub>.ts          for episodes
//   - <root>/TV/<Show>/<Show>.<starttime>.ts                     for unmatched
//
// These shapes match Sonarr's default folder format (no remap needed).
func buildOutputPath(root string, p programDetail, scheduledStart time.Time) string {
	if p.IsMovie {
		base := safeFilename(p.Title)
		return filepath.Join(root, "Movies", base, base+".ts")
	}
	show := safeFilename(p.Title)
	if p.EpisodeNumOnscreen != "" {
		s, _ := splitOnscreen(p.EpisodeNumOnscreen)
		seasonDir := "Season " + zpad2(s)
		ep := show + "." + p.EpisodeNumOnscreen
		if p.SubTitle != "" {
			ep += "." + safeFilename(p.SubTitle)
		}
		return filepath.Join(root, "TV", show, seasonDir, ep+".ts")
	}
	// Fallback when we have no episode number — timestamp-suffix.
	return filepath.Join(root, "TV", show,
		show+"."+scheduledStart.UTC().Format("20060102T150405")+".ts")
}

func safeFilename(s string) string {
	r := strings.NewReplacer("/", "-", "\\", "-", ":", "-", "*", "", "?", "",
		`"`, "", "<", "", ">", "", "|", "", "\x00", "")
	return strings.TrimSpace(r.Replace(s))
}

func zpad2(n int) string {
	if n < 10 {
		return "0" + itoa2(n)
	}
	return itoa2(n)
}
func itoa2(n int) string {
	if n == 0 {
		return "0"
	}
	out := ""
	for n > 0 {
		out = string(rune('0'+(n%10))) + out
		n /= 10
	}
	return out
}

// placeholderTorrent emits a minimal, valid bencoded torrent file.
// info_hash is unique per recording so download clients deduplicate cleanly,
// but the torrent has no real peers — it'll never download anything. The
// name matches the recording for human debugging.
//
// Blackhole-style download clients will accept this and the actual file
// arrives via Conductor's DVR output → Sonarr's library scan.
func placeholderTorrent(recID uuid.UUID, p programDetail) []byte {
	name := "Conductor.DVR." + safeFilename(p.Title)
	if p.EpisodeNumOnscreen != "" {
		name += "." + p.EpisodeNumOnscreen
	}
	infoHash := make([]byte, 20)
	idBytes, _ := recID.MarshalBinary()
	copy(infoHash, idBytes)
	binary.BigEndian.PutUint32(infoHash[16:], uint32(len(name)))

	// Bencoded:
	//   d8:announce0:4:infod6:lengthi1e4:name<len>:<name>12:piece lengthi16384e6:pieces20:<sha-stub>ee
	body := "d8:announce0:4:infod6:lengthi1e4:name" +
		intDec(len(name)) + ":" + name +
		"12:piece lengthi16384e6:pieces20:" + string(infoHash) + "ee"
	return []byte(body)
}

func intDec(n int) string {
	if n == 0 {
		return "0"
	}
	out := ""
	for n > 0 {
		out = string(rune('0'+(n%10))) + out
		n /= 10
	}
	return out
}

// suppress unused-error lint when handler.go evolves.
var _ = errors.New
