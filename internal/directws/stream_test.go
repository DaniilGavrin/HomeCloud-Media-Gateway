package directws

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/mediaframe"
	"github.com/coder/websocket"
)

func streamTestDial(t *testing.T, endpoint string, g Grant, cap bool) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, endpoint, nil)
	if err != nil {
		t.Fatal(err)
	}
	c.SetReadLimit(int64(mediaframe.HeaderSize + mediaframe.MaxPayloadBytes))
	h := hello(g)
	if cap {
		h.Capabilities = map[string]any{"hcm1": true}
	}
	raw, err := json.Marshal(h)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Write(ctx, websocket.MessageText, raw); err != nil {
		t.Fatal(err)
	}
	return c
}

func TestHCM1DirectWebSocketBinaryAndAck(t *testing.T) {
	book := New(2)
	g := grant()
	if err := book.Prepare(g); err != nil {
		t.Fatal(err)
	}
	original := bytes.Repeat([]byte("abcde"), 1<<16) // ~320 KiB, multiple frames
	var opened atomic.Int32
	h := &Handler{Book: book, OpenStream: func(_ context.Context, lease Lease) (io.ReadCloser, error) {
		if lease.RoomID != g.RoomID || lease.DeviceID != g.DeviceID || lease.Generation != g.Generation {
			return nil, errors.New("bad lease delivered to source")
		}
		opened.Add(1)
		return io.NopCloser(bytes.NewReader(original)), nil
	}}
	srv := httptest.NewServer(h)
	defer srv.Close()
	c := streamTestDial(t, "ws"+strings.TrimPrefix(srv.URL, "http"), g, true)
	defer c.CloseNow()
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	typ, raw, err := c.Read(ctx)
	if err != nil || typ != websocket.MessageText {
		t.Fatal("missing ready", err)
	}
	var ready struct {
		Type   string `json:"type"`
		Epoch  uint64 `json:"connection_epoch"`
		Window int    `json:"window_bytes"`
	}
	if err = json.Unmarshal(raw, &ready); err != nil || ready.Type != "media.ready" || ready.Epoch != 1 || ready.Window != mediaframe.WindowBytes {
		t.Fatalf("wrong ready %+v %v", ready, err)
	}
	id := mediaframe.Identity{RoomID: g.RoomID, PlaybackID: g.PlaybackID, Generation: g.Generation, Epoch: ready.Epoch}
	var received bytes.Buffer
	var seq uint64
	for {
		typ, raw, err = c.Read(ctx)
		if err != nil || typ != websocket.MessageBinary {
			t.Fatalf("no binary frame: %v", err)
		}
		f, err := mediaframe.Parse(raw, id)
		if err != nil {
			t.Fatal(err)
		}
		if f.Sequence != seq || f.Offset != uint64(received.Len()) {
			t.Fatalf("out of order %+v", f)
		}
		seq++
		if f.EOF {
			break
		}
		if _, err = received.Write(f.Payload); err != nil {
			t.Fatal(err)
		}
		ack, err := json.Marshal(mediaframe.Ack{V: 1, Type: "media.ack", RoomID: g.RoomID, PlaybackID: g.PlaybackID, Generation: g.Generation, Epoch: ready.Epoch, Sequence: f.Sequence, ReceivedBytes: uint64(received.Len())})
		if err != nil {
			t.Fatal(err)
		}
		if err = c.Write(ctx, websocket.MessageText, ack); err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(received.Bytes(), original) || opened.Load() != 1 {
		t.Fatal("corrupt direct WS transfer", opened.Load(), received.Len())
	}
	// EOF is not a room STOP; the connection stays alive awaiting another offer.
	book.Remove(g.RoomID)
	if _, _, err = c.Read(ctx); err == nil {
		t.Fatal("room STOP left media socket connected")
	}
}

func TestNoOptInDoesNotExposeMedia(t *testing.T) {
	book := New(1)
	g := grant()
	if err := book.Prepare(g); err != nil {
		t.Fatal(err)
	}
	var opened atomic.Bool
	srv := httptest.NewServer(&Handler{Book: book, OpenStream: func(context.Context, Lease) (io.ReadCloser, error) {
		opened.Store(true)
		return io.NopCloser(bytes.NewReader([]byte("secret"))), nil
	}})
	defer srv.Close()
	c := streamTestDial(t, "ws"+strings.TrimPrefix(srv.URL, "http"), g, false)
	defer c.CloseNow()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	kind, raw, err := c.Read(ctx)
	if err != nil || kind != websocket.MessageText || !bytes.Contains(raw, []byte(`"media.ready"`)) {
		t.Fatalf("attach: %v %s", err, raw)
	}
	if opened.Load() {
		t.Fatal("unnegotiated HCM1 opened protected source")
	}
	book.Remove(g.RoomID)
}
