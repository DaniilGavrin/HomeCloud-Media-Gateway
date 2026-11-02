package control

import (
	"errors"
	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/rooms"
	"testing"
)

func TestOfferLifecycleAndDedup(t *testing.T) {
	m, err := rooms.New(2)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	o := roomOffer{RoomID: "room-12345", PlaybackID: "play-1", Generation: 1, Target: rooms.Target{NodeID: "node1", DeviceID: "tv1"}, Source: rooms.Source{StorageNodeID: "nas", FileID: "movie1"}}
	if err := applyRoomOffer(m, o); err != nil {
		t.Fatal(err)
	}
	if err := applyRoomOffer(m, o); err != nil {
		t.Fatalf("idempotent offer: %v", err)
	}
	r, err := m.Get(o.RoomID)
	if err != nil || r.Generation != 1 || r.PlaybackID != "play-1" {
		t.Fatal(r, err)
	}
	old := o
	o.PlaybackID = "play-2"
	o.Generation = 2
	o.Source.FileID = "movie2"
	if err := applyRoomOffer(m, o); err != nil {
		t.Fatal(err)
	}
	r, err = m.Get(o.RoomID)
	if err != nil || r.RoomID != old.RoomID || r.Generation != 2 || r.Source.FileID != "movie2" {
		t.Fatal(r, err)
	}
	if err := applyRoomOffer(m, old); !errors.Is(err, rooms.ErrStalePlayback) {
		t.Fatalf("stale replay should not change room: %v", err)
	}
	if _, err := m.Stop(o.RoomID); err != nil {
		t.Fatal(err)
	}
}
func TestOfferRejectsDifferentTVAndBadGeneration(t *testing.T) {
	m, _ := rooms.New(1)
	defer m.Close()
	o := roomOffer{RoomID: "room-abc", PlaybackID: "play1", Generation: 1, Target: rooms.Target{NodeID: "node", DeviceID: "tv"}, Source: rooms.Source{StorageNodeID: "nas", FileID: "file"}}
	if err := applyRoomOffer(m, o); err != nil {
		t.Fatal(err)
	}
	different := o
	different.Target.DeviceID = "rogue"
	if err := applyRoomOffer(m, different); !errors.Is(err, rooms.ErrRoomConflict) {
		t.Fatal(err)
	}
	invalid := o
	invalid.PlaybackID = "next"
	invalid.Generation = 10
	if err := applyRoomOffer(m, invalid); !errors.Is(err, rooms.ErrStalePlayback) {
		t.Fatal(err)
	}
}
