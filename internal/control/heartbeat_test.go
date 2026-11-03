package control

import (
	"encoding/json"
	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/rooms"
	"testing"
)

func TestGatewayHeartbeatEnvelope(t *testing.T) {
	snap := rooms.Snapshot{RoomID: "r", PlaybackID: "p", Generation: 2, State: rooms.Active, PlaybackState: rooms.Ended}
	msg, err := roomHeartbeatMessage(snap)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	var v struct {
		V      int    `json:"v"`
		ID     string `json:"id"`
		Type   string `json:"type"`
		Action string `json:"action"`
		Data   struct {
			RoomID      string `json:"room_id"`
			PlaybackID  string `json:"playback_id"`
			Generation  uint32 `json:"generation"`
			Participant string `json:"participant"`
		} `json:"data"`
	}
	if err = json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	if v.Type != "command" || v.Action != "media.room.heartbeat" || v.Data.RoomID != "r" || v.Data.Generation != 2 || v.Data.Participant != "gateway" {
		t.Fatalf("wrong heartbeat: %s", raw)
	}
	snap.State = rooms.Closed
	if _, err := roomHeartbeatMessage(snap); err == nil {
		t.Fatal("closed room still heartbeat")
	}
}
