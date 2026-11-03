package directws

import (
	"bytes"
	"context"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/mediaframe"
	"github.com/coder/websocket"
)

func TestSeekStreamOpenerSuppliesNewGenerationOnly(t *testing.T) {
	book := New(1)
	g := grant()
	target := int64(120000)
	g.SeekPositionMS = &target
	if err := book.Prepare(g); err != nil {
		t.Fatal(err)
	}
	events := make(chan Update, 12)
	handler := &Handler{Book: book, Report: func(v Update) { events <- v }, OpenStreamAt: func(_ context.Context, l Lease) (io.ReadCloser, int64, error) {
		if l.SeekPositionMS == nil || *l.SeekPositionMS != 120000 {
			t.Errorf("seek target missing")
		}
		return io.NopCloser(bytes.NewReader([]byte("AFTER_SEEK"))), 119000, nil
	}}
	srv := httptest.NewServer(handler)
	defer srv.Close()
	conn := streamTestDial(t, "ws"+strings.TrimPrefix(srv.URL, "http"), g, true)
	defer conn.CloseNow()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	kind, _, err := conn.Read(ctx)
	if err != nil || kind != websocket.MessageText {
		t.Fatalf("ready: %v", err)
	}
	kind, raw, err := conn.Read(ctx)
	if err != nil || kind != websocket.MessageBinary {
		t.Fatalf("binary: %v", err)
	}
	// First binary frame is bound to the new generation and epoch, not any old transfer.
	frame, err := mediaframe.Parse(raw, mediaframe.Identity{RoomID: g.RoomID, PlaybackID: g.PlaybackID, Generation: g.Generation, Epoch: 1})
	if err != nil || string(frame.Payload) != "AFTER_SEEK" {
		t.Fatalf("wrong data at seek position: %v %+v", err, frame)
	}
	sawSource := false
	deadline := time.After(3 * time.Second)
	for !sawSource {
		select {
		case update := <-events:
			if update.Phase == "SEEK_RESOLVED" {
				sawSource = true
				if update.ActualPositionMS != 119000 {
					t.Fatalf("bad keyframe %+v", update)
				}
			}
		case <-deadline:
			t.Fatal("no source accuracy report")
		}
	}
}
func TestSeekDoesNotFallBackToStreamStart(t *testing.T) {
	book := New(1)
	g := grant()
	p := int64(5000)
	g.SeekPositionMS = &p
	if err := book.Prepare(g); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(&Handler{Book: book, OpenStream: func(context.Context, Lease) (io.ReadCloser, error) {
		t.Error("unsafe fallback to stream start")
		return nil, nil
	}})
	defer srv.Close()
	c := streamTestDial(t, "ws"+strings.TrimPrefix(srv.URL, "http"), g, true)
	defer c.CloseNow()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	kind, _, err := c.Read(ctx)
	if err != nil || kind != websocket.MessageText {
		t.Fatal("no ready", err)
	}
	kind, raw, err := c.Read(ctx)
	if err != nil || kind != websocket.MessageText || !bytes.Contains(raw, []byte("SEEK_UNSUPPORTED")) {
		t.Fatalf("seek silently fell back: %s %v", raw, err)
	}
}
