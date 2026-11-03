package directws

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/mediaframe"
	"github.com/coder/websocket"
)

type closeTrackedSource struct {
	io.Reader
	closed *atomic.Int32
}

func (s *closeTrackedSource) Close() error { s.closed.Add(1); return nil }

func TestKAN54EOFKeepsDirectWSAndSTOPClosesIt(t *testing.T) {
	book := New(1)
	grant := grant()
	if err := book.Prepare(grant); err != nil {
		t.Fatal(err)
	}
	control := NewStreamControl()
	var sourcesClosed atomic.Int32
	updates := make(chan Update, 16)
	h := &Handler{Book: book, Streams: control, Report: func(u Update) { updates <- u }, OpenStream: func(_ context.Context, _ Lease) (io.ReadCloser, error) {
		return &closeTrackedSource{Reader: bytes.NewReader([]byte("media-fixture")), closed: &sourcesClosed}, nil
	}}
	srv := httptest.NewServer(h)
	defer srv.Close()
	conn := streamTestDial(t, "ws"+strings.TrimPrefix(srv.URL, "http"), grant, true)
	defer conn.CloseNow()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	kind, data, err := conn.Read(ctx)
	if err != nil || kind != websocket.MessageText {
		t.Fatal("missing media.ready", err)
	}
	var ready struct {
		Epoch uint64 `json:"connection_epoch"`
	}
	if err = json.Unmarshal(data, &ready); err != nil || ready.Epoch == 0 {
		t.Fatal("ready epoch", err)
	}
	identity := mediaframe.Identity{RoomID: grant.RoomID, PlaybackID: grant.PlaybackID, Generation: grant.Generation, Epoch: ready.Epoch}
	gotEOF := false
	var lastAck []byte
	for !gotEOF {
		kind, data, err = conn.Read(ctx)
		if err != nil || kind != websocket.MessageBinary {
			t.Fatal("media closed before EOF", err)
		}
		f, e := mediaframe.Parse(data, identity)
		if e != nil {
			t.Fatal(e)
		}
		if f.EOF {
			gotEOF = true
			break
		}
		ack, _ := json.Marshal(mediaframe.Ack{V: 1, Type: "media.ack", RoomID: grant.RoomID, PlaybackID: grant.PlaybackID, Generation: grant.Generation, Epoch: ready.Epoch, Sequence: f.Sequence, ReceivedBytes: f.Offset + uint64(len(f.Payload))})
		if err = conn.Write(ctx, websocket.MessageText, ack); err != nil {
			t.Fatal(err)
		}
		lastAck = ack
	}
	// A duplicated valid media ACK after EOF must be processed without closing
	// the connection. Client-side Ping needs a concurrent reader in some WS
	// implementations, so use a real application frame for this check.
	if len(lastAck) == 0 {
		t.Fatal("no media ACK was sent before EOF")
	}
	if err = conn.Write(ctx, websocket.MessageText, lastAck); err != nil {
		t.Fatalf("EOF closed direct WS: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	if err = conn.Write(ctx, websocket.MessageText, lastAck); err != nil {
		t.Fatalf("EOF closed direct WS after idle: %v", err)
	}
	deadline := time.After(time.Second)
	seenEnd := false
	for !seenEnd {
		select {
		case u := <-updates:
			if u.Phase == "ENDED" {
				seenEnd = true
			}
		case <-deadline:
			t.Fatal("Gateway did not report source EOF")
		}
	}
	for sourcesClosed.Load() == 0 {
		select {
		case <-time.After(5 * time.Millisecond):
		case <-ctx.Done():
			t.Fatal("source not closed at EOF")
		}
	}
	if sourcesClosed.Load() != 1 {
		t.Fatalf("source closed %d times", sourcesClosed.Load())
	}
	if !book.Valid(grant.RoomID, Lease{RoomID: grant.RoomID, PlaybackID: grant.PlaybackID, Generation: grant.Generation, Epoch: ready.Epoch}) {
		t.Fatal("EOF removed active lease")
	}
	book.Remove(grant.RoomID)
	book.Remove(grant.RoomID) // stop/retry idempotent
	control.RemoveRoom(grant.RoomID)
	if _, _, err = conn.Read(ctx); err == nil {
		t.Fatal("STOP did not close WebSocket")
	}
	if len(control.active) != 0 {
		t.Fatal("STOP leaked control state")
	}
}
