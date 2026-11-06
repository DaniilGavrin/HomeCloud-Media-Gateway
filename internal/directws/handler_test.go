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

func TestKAN55HTTPInPlaceRebindAndStop(t *testing.T) {
	book := New(1)
	defer book.Close()
	first := grant()
	if err := book.Prepare(first); err != nil { t.Fatal(err) }
	handler := &Handler{Book:book, OfferHTTP:func(l Lease,_ string)(HTTPMediaOffer,error){
		return HTTPMediaOffer{
			Path:"/media/session/"+strings.Repeat("f",64),
			ContentType:"video/mp4",MediaKind:"video",
		},nil
	}}
	server := httptest.NewServer(handler)
	defer server.Close()
	ctx,cancel := context.WithTimeout(context.Background(),5*time.Second)
	defer cancel()
	ws,_,err := websocket.Dial(ctx,"ws"+strings.TrimPrefix(server.URL,"http"),nil)
	if err != nil {t.Fatal(err)}
	defer ws.CloseNow()
	hello1 := hello(first)
	hello1.Capabilities=map[string]any{"http_range":true,"http_switch":true}
	send := func(msg Hello){
		t.Helper()
		raw,e:=json.Marshal(msg)
		if e!=nil{t.Fatal(e)}
		if e=ws.Write(ctx,websocket.MessageText,raw);e!=nil{t.Fatal(e)}
	}
	recv := func(epoch uint64,playback string){
		t.Helper()
		kind,raw,e:=ws.Read(ctx)
		if e!=nil||kind!=websocket.MessageText{t.Fatalf("read: %v %v",kind,e)}
		var data struct {
			Type string `json:"type"`
			PlaybackID string `json:"playback_id"`
			ConnectionEpoch uint64 `json:"connection_epoch"`
			HTTPSwitch bool `json:"http_switch"`
		}
		if e=json.Unmarshal(raw,&data);e!=nil{t.Fatal(e)}
		if data.Type!="media.ready"||data.ConnectionEpoch!=epoch||data.PlaybackID!=playback||!data.HTTPSwitch {
			t.Fatalf("unexpected in-place ready: %+v",data)
		}
	}
	send(hello1)
	recv(1,first.PlaybackID)
	next:=first
	next.PlaybackID="play-next"
	next.Generation=2
	next.Ticket=strings.Repeat("cd",32)
	next.ExpiresAt=time.Now().Add(30*time.Second)
	if err=book.Prepare(next);err!=nil{t.Fatal(err)}
	// Rebind in-place: no second websocket.Dial, same connection and room ID.
	hello2:=hello(next)
	hello2.Capabilities=map[string]any{"http_range":true,"http_switch":true}
	send(hello2)
	recv(2,next.PlaybackID)
	book.Remove(first.RoomID)
	if _,_,err=ws.Read(ctx);err==nil{t.Fatal("STOP kept the room control socket alive")}
}
