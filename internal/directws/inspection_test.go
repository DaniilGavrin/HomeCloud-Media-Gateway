package directws

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
	"net/http/httptest"

	"github.com/coder/websocket"
	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/mediainspect"
)

func TestKAN61InspectionIsAsyncAndGenerationFenced(t *testing.T) {
	book:=New(1)
	defer book.Close()
	first:=grant()
	if err:=book.Prepare(first);err!=nil{t.Fatal(err)}
	started:=make(chan struct{})
	release:=make(chan struct{})
	defer func(){select{case <-release:default:close(release)}}()
	h:=&Handler{Book:book,OfferHTTP:func(_ Lease,_ string)(HTTPMediaOffer,error){
		return HTTPMediaOffer{Path:"/media/session/"+strings.Repeat("a",64),
			ContentType:"video/mp4",MediaKind:"video"},nil
	},Inspect:func(ctx context.Context,l Lease,caps map[string]any)mediainspect.Report{
		if l.Generation==1 {
			close(started)
			<-release
		}
		return mediainspect.Report{Status:"READY",
			Media:&mediainspect.Media{Container:"mp4",Tracks:[]mediainspect.Track{
				{Index:0,Kind:"video",Codec:"h264"}}},
			Decision:mediainspect.Decision{Mode:"UNKNOWN",Reason:"TV_CAPABILITIES_NOT_CONFIRMED"}}
	}}
	srv:=httptest.NewServer(h)
	defer srv.Close()
	ctx,cancel:=context.WithTimeout(context.Background(),5*time.Second)
	defer cancel()
	ws,_,err:=websocket.Dial(ctx,"ws"+strings.TrimPrefix(srv.URL,"http"),nil)
	if err!=nil{t.Fatal(err)}
	defer ws.CloseNow()
	send:=func(g Grant){
		t.Helper()
		hello:=hello(g)
		hello.Capabilities=map[string]any{"http_range":true,"http_switch":true}
		raw,_:=json.Marshal(hello)
		if err:=ws.Write(ctx,websocket.MessageText,raw);err!=nil{t.Fatal(err)}
	}
	read:=func(expectedType string)(map[string]any){
		t.Helper()
		kind,raw,err:=ws.Read(ctx)
		if err!=nil||kind!=websocket.MessageText{t.Fatalf("read %s: %v",expectedType,err)}
		var m map[string]any
		if err:=json.Unmarshal(raw,&m);err!=nil{t.Fatal(err)}
		if m["type"]!=expectedType{t.Fatalf("expected %s, got %+v",expectedType,m)}
		return m
	}
	send(first)
	read("media.ready") // proves ffprobe cannot delay media readiness
	select{
	case <-started:
	case <-time.After(time.Second):t.Fatal("inspection did not start")
	}
	next:=first
	next.Generation=2
	next.PlaybackID="play-next"
	next.Ticket=strings.Repeat("cd",32)
	next.ExpiresAt=time.Now().Add(30*time.Second)
	if err=book.Prepare(next);err!=nil{t.Fatal(err)}
	send(next)
	ready:=read("media.ready")
	if ready["playback_id"]!=next.PlaybackID{t.Fatal("old ready")}
	report:=read("media.inspection")
	if report["playback_id"]!=next.PlaybackID||report["generation"]!=float64(2){
		t.Fatalf("stale report sent after switch: %+v",report)
	}
	close(release) // delayed generation one must never appear after generation two
	book.Remove(first.RoomID)
	if _,_,err=ws.Read(ctx);err==nil{t.Fatal("STOP kept socket alive")}
}
