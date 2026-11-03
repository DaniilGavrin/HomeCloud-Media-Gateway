package control

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/rooms"
	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/source"
)

type storageRegistry struct{ client source.Client }

func (s storageRegistry) ForNode(_ context.Context, node string) (source.Client, error) {
	if node != "nas" {
		return nil, source.ErrSourceOffline
	}
	return s.client, nil
}

type storageClient struct{ metadata source.Metadata }

func (s storageClient) Stat(_ context.Context, fileID string) (source.Metadata, error) {
	return s.metadata, nil
}
func (s storageClient) OpenRange(context.Context, source.RangeRequest) (io.ReadCloser, source.RangeReply, error) {
	return nil, source.RangeReply{}, source.ErrNotConfigured
}
func TestGatewayOfferCanPrepareSourcePlan(t *testing.T) {
	size := source.PrefetchLimitBytes + 1
	client := storageClient{metadata: source.Metadata{OwnerNodeID: "nas", FileID: "movie", Version: "v1", SizeBytes: &size, RangeReadable: true}}
	co := &source.Coordinator{Resolver: &source.Resolver{Registry: storageRegistry{client: client}}, Plans: source.NewPlans()}
	manager, err := rooms.New(1)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	o := roomOffer{RoomID: "room1", PlaybackID: "film1", Generation: 1, Target: rooms.Target{NodeID: "node", DeviceID: "tv"}, Source: rooms.Source{StorageNodeID: "nas", FileID: "movie"}}
	if err := applyRoomOffer(manager, o); err != nil {
		t.Fatal(err)
	}
	if err := prepareRoomOfferSource(context.Background(), co, o); err != nil {
		t.Fatal(err)
	}
	plan, err := co.Plans.Get(source.Token{RoomID: o.RoomID, PlaybackID: o.PlaybackID, Generation: o.Generation})
	if err != nil || plan.Strategy != source.RemoteBlockStream {
		t.Fatal(plan, err)
	}
	// The already-selected old source must not leak into the next playback.
	next := o
	next.Generation = 2
	next.PlaybackID = "film2"
	if err := applyRoomOffer(manager, next); err != nil {
		t.Fatal(err)
	}
	if err := prepareRoomOfferSource(context.Background(), co, next); err != nil {
		t.Fatal(err)
	}
	if _, err := co.Plans.Get(source.Token{RoomID: o.RoomID, PlaybackID: o.PlaybackID, Generation: o.Generation}); !errors.Is(err, source.ErrStalePlan) {
		t.Fatal(err)
	}
	co.Remove(o.RoomID)
	if _, err := co.Plans.Get(source.Token{RoomID: next.RoomID, PlaybackID: next.PlaybackID, Generation: next.Generation}); !errors.Is(err, source.ErrStalePlan) {
		t.Fatal(err)
	}
}
func TestGatewayOfferRejectsMissingOwnerMetadata(t *testing.T) {
	size := int64(5)
	bad := storageClient{metadata: source.Metadata{OwnerNodeID: "other", FileID: "movie", Version: "v1", SizeBytes: &size, RangeReadable: true}}
	co := &source.Coordinator{Resolver: &source.Resolver{Registry: storageRegistry{client: bad}}, Plans: source.NewPlans()}
	o := roomOffer{RoomID: "r", PlaybackID: "p", Generation: 1, Source: rooms.Source{StorageNodeID: "nas", FileID: "movie"}}
	if err := prepareRoomOfferSource(context.Background(), co, o); !errors.Is(err, source.ErrInvalidMetadata) {
		t.Fatal(err)
	}
	if err := prepareRoomOfferSource(context.Background(), nil, o); err != nil {
		t.Fatal(err)
	}
}
