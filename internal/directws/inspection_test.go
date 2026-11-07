package directws

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
	"net/http/httptest"
	"sync/atomic"

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
		hello.Capabilities=map[string]any{"http_range":true,"http_switch":true,"media_inspection":true}
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

func TestKAN61ExactTVReportUpdatesDecisionWithoutReprobe(t *testing.T){
	book:=New(1)
	defer book.Close()
	g:=grant()
	if err:=book.Prepare(g);err!=nil{t.Fatal(err)}
	var calls atomic.Int32
	h:=&Handler{Book:book,OfferHTTP:func(_ Lease,_ string)(HTTPMediaOffer,error){
		return HTTPMediaOffer{Path:"/media/session/"+strings.Repeat("b",64),
			ContentType:"video/mp4",MediaKind:"video"},nil
	},Inspect:func(_ context.Context,_ Lease,_ map[string]any) mediainspect.Report{
		calls.Add(1)
		return mediainspect.Report{Status:"READY",
			Media:&mediainspect.Media{Container:"mp4",Tracks:[]mediainspect.Track{
				{Index:0,Kind:"video",Codec:"h264",Profile:"High",Level:40,
					Width:1920,Height:1080},
				{Index:1,Kind:"audio",Codec:"aac",Default:true}}},
			Decision:mediainspect.Decision{Mode:"UNKNOWN",Reason:"TV_CAPABILITIES_NOT_CONFIRMED"}}
	}}
	srv:=httptest.NewServer(h)
	defer srv.Close()
	ctx,cancel:=context.WithTimeout(context.Background(),5*time.Second)
	defer cancel()
	ws,_,err:=websocket.Dial(ctx,"ws"+strings.TrimPrefix(srv.URL,"http"),nil)
	if err!=nil{t.Fatal(err)}
	defer ws.CloseNow()
	send:=func(m any){
		raw,e:=json.Marshal(m)
		if e!=nil{t.Fatal(e)}
		if e=ws.Write(ctx,websocket.MessageText,raw);e!=nil{t.Fatal(e)}
	}
	read:=func()map[string]any{
		_,raw,e:=ws.Read(ctx)
		if e!=nil{t.Fatal(e)}
		var m map[string]any
		if e=json.Unmarshal(raw,&m);e!=nil{t.Fatal(e)}
		return m
	}
	helloMsg:=hello(g)
	helloMsg.Capabilities=map[string]any{"http_range":true,"http_switch":true,"media_inspection":true}
	send(helloMsg)
	if x:=read();x["type"]!="media.ready"{t.Fatal(x)}
	if x:=read();x["type"]!="media.inspection"{t.Fatal(x)}
	caps:=mediainspect.TVCapabilities{Version:1,Source:"media_capabilities_api",
		Formats:[]mediainspect.FormatSupport{{Container:"mp4",VideoCodec:"h264",
			AudioCodec:"aac",VideoProfiles:[]string{"High"},MaxLevel:40,
			MaxWidth:1920,MaxHeight:1080,Certainty:"probably"}}}
	send(Hello{V:1,Type:"media.decoder.report",RoomID:g.RoomID,
		PlaybackID:g.PlaybackID,Generation:g.Generation,DeviceID:g.DeviceID,
		Capabilities:map[string]any{"decoder":caps}})
	update:=read()
	if update["type"]!="media.inspection"{t.Fatalf("no decision update: %+v",update)}
	report,ok:=update["report"].(map[string]any)
	if !ok {t.Fatalf("invalid report: %+v",update)}
	decision,ok:=report["decision"].(map[string]any)
	if !ok||decision["mode"]!="DIRECT" {t.Fatalf("expected exact DIRECT: %+v",report)}
	if calls.Load()!=1{t.Fatal("decoding feedback reran ffprobe")}
	book.Remove(g.RoomID)
}


func TestKAN63SelectionIsTrackScopedAndGenerationFenced(t *testing.T){
 book:=New(1);defer book.Close()
 g:=grant()
 if err:=book.Prepare(g);err!=nil{t.Fatal(err)}
 var calls atomic.Int32
 h:=&Handler{Book:book,
  OfferHTTP:func(_ Lease,_ string)(HTTPMediaOffer,error){
   return HTTPMediaOffer{Path:"/media/session/"+strings.Repeat("a",64),ContentType:"video/mp4",MediaKind:"video"},nil
  },
  Inspect:func(_ context.Context,_ Lease,_ map[string]any)mediainspect.Report{
   return mediainspect.Report{Status:"READY",Media:&mediainspect.Media{
    Container:"matroska",Tracks:[]mediainspect.Track{
     {Index:0,Kind:"video",Codec:"h264",Profile:"High",Level:40,Width:1920,Height:1080},
     {Index:1,Kind:"audio",Codec:"dts"},
     {Index:2,Kind:"audio",Codec:"aac"},
    },
   },Decision:mediainspect.Decision{Mode:"ADAPTIVE",Reason:"DECLARED_FORMAT_UNSUPPORTED"}}
  },
  OfferConversion:func(l Lease,_ string,m mediainspect.Media,caps mediainspect.TVCapabilities,index int)(HTTPMediaOffer,error){
   calls.Add(1)
   if l.PlaybackID!=g.PlaybackID||index!=1||len(m.Tracks)!=3||caps.Version!=1{
    t.Error("bad inspected selection or untrusted output caps",index,caps)
   }
   return HTTPMediaOffer{Path:"/media/convert/"+strings.Repeat("b",64),ContentType:"video/mp4",MediaKind:"video"},nil
  },
 }
 server:=httptest.NewServer(h);defer server.Close()
 ctx,cancel:=context.WithTimeout(context.Background(),5*time.Second);defer cancel()
 ws,_,err:=websocket.Dial(ctx,"ws"+strings.TrimPrefix(server.URL,"http"),nil)
 if err!=nil{t.Fatal(err)}
 defer ws.CloseNow()
 send:=func(m any){raw,e:=json.Marshal(m);if e!=nil{t.Fatal(e)};if e=ws.Write(ctx,websocket.MessageText,raw);e!=nil{t.Fatal(e)}}
 recv:=func()map[string]any{
  _,raw,e:=ws.Read(ctx);if e!=nil{t.Fatal(e)}
  var m map[string]any;if e=json.Unmarshal(raw,&m);e!=nil{t.Fatal(e)}
  return m
 }
 caps:=mediainspect.TVCapabilities{Version:1,Source:"media_capabilities_api",Formats:[]mediainspect.FormatSupport{{
  Container:"mp4",VideoCodec:"h264",AudioCodec:"aac",Certainty:"probably",
  VideoProfiles:[]string{"High"},MaxLevel:40,MaxWidth:1920,MaxHeight:1080,
 }}}
 helloMsg:=hello(g);helloMsg.Capabilities=map[string]any{
  "http_range":true,"http_switch":true,"media_inspection":true,"decoder":caps,
 }
 send(helloMsg)
 if m:=recv();m["type"]!="media.ready"{t.Fatal(m)}
 if m:=recv();m["type"]!="media.inspection"{t.Fatal(m)}
 invalid:=1_000
 send(Hello{V:1,Type:"media.audio.select",RoomID:g.RoomID,PlaybackID:g.PlaybackID,
  Generation:g.Generation,DeviceID:g.DeviceID,AudioIndex:&invalid})
 if m:=recv();m["type"]!="media.convert.error"{t.Fatal(m)}
 if calls.Load()!=0{t.Fatal("unverified index invoked converter")}
 selected:=1
 send(Hello{V:1,Type:"media.audio.select",RoomID:g.RoomID,PlaybackID:g.PlaybackID,
  Generation:g.Generation,DeviceID:g.DeviceID,AudioIndex:&selected,
  Capabilities:map[string]any{"decoder":caps}})
 m:=recv()
 if m["type"]!="media.convert.ready"||m["audio_index"]!=float64(1)||m["seek_supported"]!=false{
  t.Fatalf("bad conversion offer: %v",m)
 }
 if calls.Load()!=1{t.Fatalf("wanted one conversion offer, got %d",calls.Load())}
 book.Remove(g.RoomID)
}
