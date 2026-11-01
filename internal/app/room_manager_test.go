package app

import (
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/config"
	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/rooms"
)

func TestRoomManagerIsWiredIntoGateway(t *testing.T) {
	cfg := config.Config{
		Server: config.ServerConfig{WebSocketPath: "/ws/stream"},
		Limits: config.LimitsConfig{MaxRooms: 2},
	}
	gateway := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer gateway.Rooms().Close()
	if gateway.Rooms() == nil {
		t.Fatal("Room Manager must be initialized by Gateway")
	}
	_, _, err := gateway.Rooms().CreateAssigned("room-backend-1", rooms.Target{NodeID: "host-1", DeviceID: "tv-1"})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = gateway.Rooms().CreateAssigned("room-backend-2", rooms.Target{NodeID: "host-1", DeviceID: "tv-2"})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = gateway.Rooms().CreateAssigned("room-backend-3", rooms.Target{NodeID: "host-1", DeviceID: "tv-3"})
	if !errors.Is(err, rooms.ErrRoomLimit) {
		t.Fatalf("gateway config max_rooms was ignored: %v", err)
	}
}

func TestRoomManagerWorksWithMinimalTestConfig(t *testing.T) {
	gateway := New(config.Config{Server: config.ServerConfig{WebSocketPath: "/ws/stream"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer gateway.Rooms().Close()
	if gateway.Rooms() == nil {
		t.Fatal("nil room manager in minimal test config")
	}
}
