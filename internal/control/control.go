package control

import (
	"encoding/json"
	"errors"
	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/directws"
)

// MediaControlEvent is accepted only over the already trusted loopback Backend
// control WS. The direct TV WS has no authority to send these messages.
type MediaControlEvent struct {
	RoomID          string `json:"room_id"`
	PlaybackID      string `json:"playback_id"`
	Generation      uint32 `json:"generation"`
	ConnectionEpoch uint64 `json:"connection_epoch"`
	CommandID       string `json:"command_id"`
	CommandSequence uint64 `json:"command_sequence"`
	Action          string `json:"action"`
}

func decodeMediaControl(raw json.RawMessage) (directws.PlaybackControl, error) {
	var r MediaControlEvent
	if len(raw) == 0 || json.Unmarshal(raw, &r) != nil || r.RoomID == "" || r.PlaybackID == "" || r.CommandID == "" || r.CommandSequence == 0 || r.ConnectionEpoch == 0 {
		return directws.PlaybackControl{}, errors.New("INVALID_CONTROL")
	}
	if r.Action != "pause" && r.Action != "resume" && r.Action != "play" {
		return directws.PlaybackControl{}, errors.New("INVALID_CONTROL")
	}
	return directws.PlaybackControl{RoomID: r.RoomID, PlaybackID: r.PlaybackID, Generation: r.Generation, Epoch: r.ConnectionEpoch, CommandID: r.CommandID, CommandSequence: r.CommandSequence, Action: r.Action}, nil
}
func controlErrorCode(err error) string {
	switch {
	case errors.Is(err, directws.ErrNoStream):
		return "NO_ACTIVE_STREAM"
	case errors.Is(err, directws.ErrEndedStream):
		return "STREAM_ENDED"
	case errors.Is(err, directws.ErrStaleControl):
		return "STALE_CONTROL"
	default:
		return "INVALID_CONTROL"
	}
}
