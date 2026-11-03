package control

import (
	"errors"
	"testing"

	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/directws"
	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/rooms"
)

func TestSeekOfferPreservesPlaybackAndCancelsOldGeneration(t *testing.T) {
	manager, err := rooms.New(2)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	offer := roomOffer{RoomID: "r", PlaybackID: "p", Generation: 1,
		Target: rooms.Target{NodeID: "node", DeviceID: "tv"},
		Source: rooms.Source{StorageNodeID: "nas", FileID: "media"}}
	if err := applyRoomOffer(manager, offer); err != nil {
		t.Fatal(err)
	}
	original, err := manager.Get("r")
	if err != nil {
		t.Fatal(err)
	}
	target := int64(26000)
	offer.Generation = 2
	offer.SeekPositionMS = &target
	if err := applyRoomOffer(manager, offer); err != nil {
		t.Fatal(err)
	}
	updated, err := manager.Get("r")
	if err != nil || updated.RoomID != original.RoomID || updated.PlaybackID != original.PlaybackID || updated.Generation != 2 || updated.PositionMS != 26000 || updated.PlaybackState != rooms.Preparing {
		t.Fatalf("seek offer: %+v %v", updated, err)
	}
	if err := applyRoomOffer(manager, offer); err != nil {
		t.Fatalf("duplicate seek: %v", err)
	}
	offer.Generation = 1
	if err := applyRoomOffer(manager, offer); !errors.Is(err, rooms.ErrStalePlayback) {
		t.Fatalf("old generation accepted %v", err)
	}
}
func TestGatewaySeekSourceControlPayload(t *testing.T) {
	action, _, data := directUpdateCommand(directws.Update{RoomID: "r", PlaybackID: "p", Generation: 2, Epoch: 3, Phase: "SEEK_RESOLVED", ActualPositionMS: 24500})
	if action != "media.room.seek.source" || data == nil {
		t.Fatalf("source seek not routed: %q", action)
	}
}
