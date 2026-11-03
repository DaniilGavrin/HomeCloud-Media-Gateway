package control

import (
	"encoding/json"
	"testing"
)

func TestControlDecode(t *testing.T) {
	raw := json.RawMessage(`{"room_id":"r","playback_id":"p","generation":3,"connection_epoch":7,"command_id":"c","command_sequence":9,"action":"pause"}`)
	q, e := decodeMediaControl(raw)
	if e != nil || q.RoomID != "r" || q.Epoch != 7 {
		t.Fatal(q, e)
	}
	raw = json.RawMessage(`{"room_id":"r","playback_id":"p","generation":3,"connection_epoch":7,"command_id":"c","command_sequence":9,"action":"volume"}`)
	if _, e = decodeMediaControl(raw); e == nil {
		t.Fatal("gateway accepted volume")
	}
}
