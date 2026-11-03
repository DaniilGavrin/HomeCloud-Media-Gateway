package control

import (
	"strconv"
	"time"

	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/directws"
)

// Direct-TV attachment reports and actual media execution are different
// control commands. Only the latter is allowed to influence PLAYING.
func directUpdateCommand(u directws.Update) (action, requestID string, data any) {
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	if u.Phase == "STREAMING" || u.Phase == "ENDED" || u.Phase == "ERROR" {
		payload := map[string]any{
			"room_id":          u.RoomID,
			"playback_id":      u.PlaybackID,
			"generation":       u.Generation,
			"connection_epoch": u.Epoch,
			"participant":      "gateway",
			"report_sequence":  u.Sequence,
			"state":            u.Phase,
			"position_ms":      0, // only TV observes actual decoder progress
		}
		if u.ErrorCode != "" {
			payload["error_code"] = u.ErrorCode
		}
		return "media.room.report", "gw-report-" + suffix, payload
	}
	return "media.room.attach", "gw-attach-" + suffix, u
}
