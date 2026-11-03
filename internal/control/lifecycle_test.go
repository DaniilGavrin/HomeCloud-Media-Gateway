package control

import (
	"errors"
	"testing"

	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/directws"
	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/rooms"
)

func TestKAN54GatewayEOFOnlyClosesPlayback(t *testing.T) {
	m, err := rooms.New(1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	s, roomCtx, err := m.CreateAssigned("room-eof", rooms.Target{NodeID: "node-1", DeviceID: "tv-1"})
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range []rooms.RoomState{rooms.Connecting, rooms.Active} {
		if _, err = m.AdvanceRoom(s.RoomID, state); err != nil {
			t.Fatal(err)
		}
	}
	s, _, worker, err := m.StartAssignedPlayback(s.RoomID, "play-1", rooms.Source{StorageNodeID: "ss", FileID: "f"})
	if err != nil {
		t.Fatal(err)
	}
	base := directws.Update{RoomID: s.RoomID, PlaybackID: s.PlaybackID, Generation: s.Generation, Phase: "STREAMING"}
	if err = observeStreamLifecycle(m, base); err != nil {
		t.Fatal(err)
	}
	base.Phase = "ENDED"
	if err = observeStreamLifecycle(m, base); err != nil {
		t.Fatal(err)
	}
	result, err := m.Get(s.RoomID)
	if err != nil || result.State != rooms.Active || result.PlaybackState != rooms.Ended {
		t.Fatalf("EOF removed room: %+v %v", result, err)
	}
	select {
	case <-worker.Done():
	default:
		t.Fatal("EOF left source worker alive")
	}
	select {
	case <-roomCtx.Done():
		t.Fatal("EOF closed room context")
	default:
	}
	base.Phase = "STREAMING"
	if err = observeStreamLifecycle(m, base); !errors.Is(err, rooms.ErrStalePlayback) {
		t.Fatalf("old sender resurrected EOF: %v", err)
	}
	if _, err = m.Stop(s.RoomID); err != nil {
		t.Fatal(err)
	}
	select {
	case <-roomCtx.Done():
	default:
		t.Fatal("STOP did not cancel room context")
	}
	next, _, err := m.CreateAssigned("room-next", rooms.Target{NodeID: "node-1", DeviceID: "tv-1"})
	if err != nil || next.RoomID == s.RoomID {
		t.Fatalf("new viewing should use new room: %+v %v", next, err)
	}
}
