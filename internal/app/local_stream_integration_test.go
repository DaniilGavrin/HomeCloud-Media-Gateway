package app

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/config"
	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/directws"
	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/mediaframe"
	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/source"
	"github.com/coder/websocket"
)

type localTestRegistry struct {
	node string
	meta source.Metadata
}
type localTestClient struct{ meta source.Metadata }

func (r localTestRegistry) ForNode(_ context.Context, node string) (source.Client, error) {
	if node != r.node { return nil, source.ErrSourceOffline }
	return localTestClient{meta: r.meta}, nil
}
func (c localTestClient) Stat(_ context.Context, fileID string) (source.Metadata, error) {
	if fileID != c.meta.FileID { return source.Metadata{}, source.ErrInvalidReference }
	return c.meta, nil
}
func (localTestClient) OpenRange(context.Context, source.RangeRequest) (io.ReadCloser, source.RangeReply, error) {
	return nil, source.RangeReply{}, source.ErrRangeUnsupported
}

func TestAuthorizedRealDiskBytesReachHCM1ForAudioAndVideo(t *testing.T) {
	for _, tc := range []struct {
		name, mime, filename string
	}{
		{"video", "video/mp4", "movie.mp4"},
		{"audio", "audio/mp4", "track.m4a"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			// Binary delivery test, not a codec-decoding test.
			raw := bytes.Repeat([]byte("HomeCloud raw media bytes."), 4000)
			file := filepath.Join(root, tc.filename)
			if err := os.WriteFile(file, raw, 0600); err != nil { t.Fatal(err) }
			stat, err := os.Stat(file)
			if err != nil { t.Fatal(err) }
			size, mtime := stat.Size(), stat.ModTime().UnixNano()
			ref := source.Reference{StorageNodeID: "storage-unit", FileID: "opaque-media-file"}
			meta := source.Metadata{
				OwnerNodeID: ref.StorageNodeID, FileID: ref.FileID, Version: "rev1",
				ContentType: tc.mime, SizeBytes: &size, RangeReadable: true,
				Local: &source.LocalHint{SharedRootID: "media-root", RelativePath: tc.filename, ModifiedUnixNano: &mtime},
			}
			mapping := map[string]string{"media-root": root}
			co := &source.Coordinator{
				Resolver: &source.Resolver{
					Registry: localTestRegistry{node: ref.StorageNodeID, meta: meta},
					Local: source.RootProbe{Roots: mapping},
				},
				Plans: source.NewPlans(),
			}
			token := source.Token{RoomID: "room-1", PlaybackID: "play-1", Generation: 1}
			decision, err := co.Prepare(context.Background(), token, ref)
			if err != nil || decision.Strategy != source.LocalDirect { t.Fatalf("plan: %v %v", decision.Strategy, err) }

			a := New(config.Config{Server: config.ServerConfig{WebSocketPath: "/ws/stream"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
			a.SetSourceCoordinator(co)
			a.SetSourceReaders(map[source.Strategy]source.ReaderFactory{
				source.LocalDirect: source.LocalReaderFactory{Roots: mapping},
			})
			grant := directws.Grant{
				RoomID: token.RoomID, PlaybackID: token.PlaybackID, DeviceID: "tv-1",
				Generation: token.Generation,
				Ticket: "00112233445566778899aabbccddeeff",
				ExpiresAt: time.Now().Add(30 * time.Second),
			}
			if err := a.Tickets().Prepare(grant); err != nil { t.Fatal(err) }
			srv := httptest.NewServer(a.Handler())
			defer srv.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/ws/stream", nil)
			if err != nil { t.Fatal(err) }
			defer conn.CloseNow()
			conn.SetReadLimit(int64(mediaframe.HeaderSize + mediaframe.MaxPayloadBytes))
			hello, err := json.Marshal(directws.Hello{
				V: 1, Type: "media.hello", RoomID: token.RoomID, PlaybackID: token.PlaybackID,
				Generation: token.Generation, DeviceID: grant.DeviceID, AttachTicket: grant.Ticket,
				Capabilities: map[string]any{"hcm1": true},
			})
			if err != nil { t.Fatal(err) }
			if err := conn.Write(ctx, websocket.MessageText, hello); err != nil { t.Fatal(err) }
			kind, readyRaw, err := conn.Read(ctx)
			if err != nil || kind != websocket.MessageText { t.Fatalf("ready read: %v", err) }
			var ready struct { Type string `json:"type"`; Epoch uint64 `json:"connection_epoch"` }
			if err := json.Unmarshal(readyRaw, &ready); err != nil || ready.Type != "media.ready" { t.Fatalf("ready: %+v %v", ready, err) }
			id := mediaframe.Identity{RoomID: token.RoomID, PlaybackID: token.PlaybackID, Generation: token.Generation, Epoch: ready.Epoch}
			var received bytes.Buffer
			for {
				kind, packet, err := conn.Read(ctx)
				if err != nil || kind != websocket.MessageBinary { t.Fatalf("media frame: %v", err) }
				frame, err := mediaframe.Parse(packet, id)
				if err != nil { t.Fatal(err) }
				if frame.Offset != uint64(received.Len()) { t.Fatalf("unexpected offset: %d", frame.Offset) }
				if frame.EOF { break }
				if _, err := received.Write(frame.Payload); err != nil { t.Fatal(err) }
				ack, err := json.Marshal(mediaframe.Ack{
					V: 1, Type: "media.ack", RoomID: token.RoomID, PlaybackID: token.PlaybackID,
					Generation: token.Generation, Epoch: ready.Epoch, Sequence: frame.Sequence,
					ReceivedBytes: uint64(received.Len()),
				})
				if err != nil { t.Fatal(err) }
				if err := conn.Write(ctx, websocket.MessageText, ack); err != nil { t.Fatal(err) }
			}
			if !bytes.Equal(received.Bytes(), raw) { t.Fatalf("%s bytes corrupted: received %d expected %d", tc.mime, received.Len(), len(raw)) }
		})
	}
}
