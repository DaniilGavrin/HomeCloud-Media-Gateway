package control

import (
	"errors"
	"strconv"
	"time"

	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/config"
	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/rooms"
)

// heartbeatIntervalSeconds uses the one validated Gateway startup setting.
// The fallback is only for tests that construct a zero-value config.
func heartbeatIntervalSeconds(cfg config.Config) int {
	n := int(cfg.Heartbeat.Interval.Duration / time.Second)
	if n < 20 || n > 30 {
		return 20
	}
	return n
}

// roomHeartbeatMessage has its own control-plane request ID, independent of
// HCM1 media acknowledgements. Caller serializes writes via the existing mutex.
func roomHeartbeatMessage(snap rooms.Snapshot) (message, error) {
	if snap.RoomID == "" || snap.PlaybackID == "" || snap.Generation == 0 || snap.State != rooms.Active {
		return message{}, errors.New("room not active")
	}
	return message{V: 1, ID: "gw-heartbeat-" + strconv.FormatInt(time.Now().UnixNano(), 10), Type: "command", Action: "media.room.heartbeat", Data: map[string]any{
		"room_id": snap.RoomID, "playback_id": snap.PlaybackID, "generation": snap.Generation, "participant": "gateway",
	}}, nil
}
