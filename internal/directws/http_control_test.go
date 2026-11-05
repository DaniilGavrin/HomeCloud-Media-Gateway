package directws

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestHTTPRangeLeaseAcceptsControlWithoutHCM1Sender(t *testing.T) {
	book := New(1)
	grant := grant()
	if err := book.Prepare(grant); err != nil { t.Fatal(err) }
	streams := NewStreamControl()
	handler := &Handler{
		Book: book,
		Streams: streams,
		OfferHTTP: func(l Lease, peer string) (HTTPMediaOffer, error) {
			if !book.Valid(l.RoomID, l) { t.Fatal("unvalidated lease") }
			return HTTPMediaOffer{
				Path: "/media/session/" + strings.Repeat("f",64),
				ContentType:"audio/mpeg", MediaKind:"audio",
			}, nil
		},
	}
	srv := httptest.NewServer(handler)
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(),5*time.Second)
	defer cancel()
	ws, _, err := websocket.Dial(ctx,"ws"+strings.TrimPrefix(srv.URL,"http"),nil)
	if err != nil { t.Fatal(err) }
	defer ws.CloseNow()
	h := hello(grant)
	h.Capabilities = map[string]any{"http_range":true}
	raw,err:=json.Marshal(h)
	if err != nil {t.Fatal(err)}
	if err=ws.Write(ctx,websocket.MessageText,raw);err!=nil{t.Fatal(err)}
	kind,raw,err:=ws.Read(ctx)
	if err!=nil||kind!=websocket.MessageText{t.Fatal(err)}
	var ready struct {
		Type string `json:"type"`
		Epoch uint64 `json:"connection_epoch"`
		MediaKind string `json:"media_kind"`
		HTTPPath string `json:"http_media_path"`
	}
	if err=json.Unmarshal(raw,&ready);err!=nil||ready.Type!="media.ready"||
		ready.MediaKind!="audio"||ready.HTTPPath==""||ready.Epoch==0{
		t.Fatalf("HTTP offer missing: %+v %v",ready,err)
	}
	control:=PlaybackControl{
		RoomID:grant.RoomID,PlaybackID:grant.PlaybackID,Generation:grant.Generation,
		Epoch:ready.Epoch,CommandID:"c1",CommandSequence:1,Action:"pause",
	}
	if err=streams.Apply(control);err!=nil{t.Fatalf("HTTP pause incorrectly required HCM1: %v",err)}
	if err=streams.Apply(control);err!=nil{t.Fatalf("idempotent HTTP ACK: %v",err)}
	next:=control;next.CommandID="c2";next.CommandSequence=2;next.Action="resume"
	if err=streams.Apply(next);err!=nil{t.Fatalf("HTTP resume: %v",err)}
	old:=control
	if err=streams.Apply(old);!errors.Is(err,ErrStaleControl){t.Fatalf("stale control not rejected: %v",err)}
	book.Remove(grant.RoomID)
	// After a room STOP even an otherwise valid HTTP command is rejected.
	next.CommandID="c3";next.CommandSequence=3;next.Action="pause"
	if err=streams.Apply(next);!errors.Is(err,ErrNoStream){
		t.Fatalf("closed HTTP lease accepted control: %v",err)
	}
}
