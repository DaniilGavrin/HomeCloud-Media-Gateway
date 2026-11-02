package directws

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestDirectWSAttachOneUseAndInvalidTV(t *testing.T) {
	book := New(2)
	g := grant()
	if e := book.Prepare(g); e != nil {
		t.Fatal(e)
	}
	h := &Handler{Book: book}
	srv := httptest.NewServer(h)
	defer srv.Close()
	endpoint := "ws" + strings.TrimPrefix(srv.URL, "http")
	dial := func(msg Hello) (*websocket.Conn, struct {
		Type  string `json:"type"`
		Code  string `json:"code"`
		Epoch uint64 `json:"connection_epoch"`
	}) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		c, _, e := websocket.Dial(ctx, endpoint, nil)
		if e != nil {
			t.Fatal(e)
		}
		b, _ := json.Marshal(msg)
		if e = c.Write(ctx, websocket.MessageText, b); e != nil {
			t.Fatal(e)
		}
		kind, p, e := c.Read(ctx)
		if e != nil || kind != websocket.MessageText {
			t.Fatalf("read %v %v", kind, e)
		}
		var out struct {
			Type  string `json:"type"`
			Code  string `json:"code"`
			Epoch uint64 `json:"connection_epoch"`
		}
		if e = json.Unmarshal(p, &out); e != nil {
			t.Fatal(e)
		}
		return c, out
	}
	wrong := hello(g)
	wrong.DeviceID = "evil-tv"
	c, bad := dial(wrong)
	c.CloseNow()
	if bad.Type != "media.error" || bad.Code != "ATTACH_REJECTED" {
		t.Fatalf("invalid TV allowed %+v", bad)
	}
	c, ready := dial(hello(g))
	defer c.CloseNow()
	if ready.Type != "media.ready" || ready.Epoch != 1 {
		t.Fatalf("unexpected ready %+v", ready)
	}
	replay, replayed := dial(hello(g))
	replay.CloseNow()
	if replayed.Type != "media.error" || replayed.Code != "ATTACH_REJECTED" {
		t.Fatalf("replay allowed %+v", replayed)
	}
	book.Remove(g.RoomID)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, _, e := c.Read(ctx); e == nil {
		t.Fatal("stopped room did not close direct socket")
	}
}
func TestConcurrentClaimSingleWinner(t *testing.T) {
	b := New(1)
	g := grant()
	if e := b.Prepare(g); e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	results := make(chan bool, 50)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, e := b.Claim(hello(g)); results <- e == nil }()
	}
	wg.Wait()
	close(results)
	successes := 0
	for r := range results {
		if r {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("expected one ticket winner, got %d", successes)
	}
}
