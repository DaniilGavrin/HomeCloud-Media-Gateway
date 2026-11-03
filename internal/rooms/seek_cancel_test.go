package rooms

import (
	"testing"
)

func TestSeekCancelsOldPlaybackAndKeepsRoom(t *testing.T) {
	m, err := New(2)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	s, _, err := m.CreateAssigned("r", Target{NodeID: "n", DeviceID: "tv"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = m.AdvanceRoom(s.RoomID, Connecting)
	if err != nil {
		t.Fatal(err)
	}
	_, err = m.AdvanceRoom(s.RoomID, Active)
	if err != nil {
		t.Fatal(err)
	}
	s, tok, old, err := m.StartAssignedPlayback("r", "p", Source{StorageNodeID: "n", FileID: "f"})
	if err != nil {
		t.Fatal(err)
	}
	next, newToken, newCtx, err := m.Seek(tok, 42000)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-old.Done():
	default:
		t.Fatal("old read context not canceled")
	}
	select {
	case <-newCtx.Done():
		t.Fatal("new seek context already canceled")
	default:
	}
	if next.RoomID != s.RoomID || next.PlaybackID != s.PlaybackID || next.Generation != s.Generation+1 || next.PositionMS != 42000 || next.PlaybackState != Preparing {
		t.Fatalf("invalid new generation %+v", next)
	}
	if m.IsCurrent(tok) {
		t.Fatal("stale generation still current")
	}
	if !m.IsCurrent(newToken) {
		t.Fatal("new generation rejected")
	}
	if _, err = m.Stop("r"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-newCtx.Done():
	default:
		t.Fatal("stop did not cancel active seek")
	}
}
