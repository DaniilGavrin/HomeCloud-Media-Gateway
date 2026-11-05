package source

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// Implements a test HCS1 server with on-wire text header + binary range,
// without coupling the gateway module to HC SS's private packages.
func mockHCS1(t *testing.T, data []byte, corruptHash bool) (*httptest.Server, string, Metadata) {
	t.Helper()
	id, version := "hc1.mock-fixture", "rev1"
	n := int64(len(data))
	meta := Metadata{
		OwnerNodeID:"storage-a",FileID:id,Version:version,SizeBytes:&n,
		ContentType:"audio/mpeg",RangeReadable:true,
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){
		if r.URL.Path != "/ws/source" { http.NotFound(w,r);return }
		conn,err:=websocket.Accept(w,r,&websocket.AcceptOptions{InsecureSkipVerify:true})
		if err!=nil{return}
		defer conn.CloseNow()
		ctx,cancel:=context.WithTimeout(r.Context(),10*time.Second)
		defer cancel()
		kind,raw,err:=conn.Read(ctx)
		if err!=nil||kind!=websocket.MessageText{return}
		var q struct{
			V int `json:"v"`
			Type string `json:"type"`
			ID string `json:"id"`
			FileID string `json:"file_id"`
			Version string `json:"version"`
			Offset int64 `json:"offset"`
			Length int64 `json:"length"`
		}
		if json.Unmarshal(raw,&q)!=nil || q.FileID!=id{return}
		var reply any
		var payload []byte
		switch q.Type{
		case "source.stat":
			reply=map[string]any{"v":1,"type":"source.metadata","id":q.ID,"metadata":meta}
		case "source.read":
			if q.Version!=version{
				reply=map[string]any{"v":1,"type":"source.error","id":q.ID,"code":"SOURCE_VERSION_CHANGED"}
			}else{
				end:=q.Offset+q.Length
				if end>int64(len(data)){end=int64(len(data))}
				payload=append([]byte(nil),data[q.Offset:end]...)
				hash:=sha256.Sum256(payload)
				digest:=hex.EncodeToString(hash[:])
				if corruptHash{digest=strings.Repeat("0",64)}
				reply=map[string]any{"v":1,"type":"source.range","id":q.ID,
					"offset":q.Offset,"length":len(payload),"version":version,
					"eof":end>=int64(len(data)),"checksum_sha256":digest}
			}
		default:return
		}
		b,_:=json.Marshal(reply)
		if conn.Write(ctx,websocket.MessageText,b)!=nil{return}
		if q.Type=="source.read" && q.Version==version{
			_ = conn.Write(ctx,websocket.MessageBinary,payload)
		}
	}))
	return server,"ws"+strings.TrimPrefix(server.URL,"http")+"/ws/source",meta
}

func TestHCS1PrivateEndpointAndNodeRegistry(t *testing.T) {
	for _, bad:=range []string{"", "ws://8.8.8.8:8092/ws/source",
		"ws://0.0.0.0:8092/ws/source", "ws://example.com:8092/ws/source",
		"ws://192.168.0.10:8092/ws/other", "ws://192.168.0.10/ws/source",
		"ws://192.168.0.10:8092/ws/source?file=secret", "http://127.0.0.1:8092/ws/source"}{
		if err:=ValidateHCS1URL(bad);err==nil{t.Fatalf("accepted %q",bad)}
	}
	for _, good:=range []string{"ws://127.0.0.1:8092/ws/source","ws://192.168.0.10:8092/ws/source","wss://100.83.9.1:8092/ws/source"}{
		if err:=ValidateHCS1URL(good);err!=nil{t.Fatalf("rejected %s: %v",good,err)}
	}
	r:=StaticRegistry{Nodes:map[string]string{"a":"ws://127.0.0.1:8092/ws/source"}}
	if _,err:=r.ForNode(context.Background(),"wrong");!errors.Is(err,ErrSourceOffline){t.Fatal(err)}
	if _,err:=r.ForNode(context.Background(),"a");err!=nil{t.Fatal(err)}
}

func TestHCS1StatReadAndChecksum(t *testing.T){
	payload:=bytes.Repeat([]byte("audio-"),40000)
	srv,endpoint,meta:=mockHCS1(t,payload,false)
	defer srv.Close()
	c:=HCS1Client{URL:endpoint}
	got,err:=c.Stat(context.Background(),meta.FileID)
	if err!=nil||got.FileID!=meta.FileID||got.ContentType!="audio/mpeg"{t.Fatal(got,err)}
	f,head,err:=c.OpenRange(context.Background(),RangeRequest{FileID:meta.FileID,Version:meta.Version,Offset:10,Length:12345})
	if err!=nil{t.Fatal(err)}
	raw,err:=io.ReadAll(f)
	_ = f.Close()
	if err!=nil||!bytes.Equal(raw,payload[10:10+12345])||head.Length!=12345||head.EOF{
		t.Fatalf("range %+v length %d: %v",head,len(raw),err)
	}
	if _,_,err:=c.OpenRange(context.Background(),RangeRequest{FileID:meta.FileID,Version:"stale",Offset:0,Length:4});!errors.Is(err,ErrSourceChanged){t.Fatal(err)}
	srv2,endpoint2,_:=mockHCS1(t,payload,true)
	defer srv2.Close()
	if _,_,err:= (HCS1Client{URL:endpoint2}).OpenRange(context.Background(),RangeRequest{FileID:meta.FileID,Version:meta.Version,Offset:0,Length:9});!errors.Is(err,ErrInvalidMetadata){t.Fatal("bad checksum not rejected",err)}
}

func TestRemoteReaderChunkingAudioAndVideo(t *testing.T){
	for _,kind:=range []string{"audio/mpeg","video/mp4"}{
		t.Run(kind,func(t *testing.T){
			payload:=bytes.Repeat([]byte("HCS1 data byte."),30000)
			srv,endpoint,meta:=mockHCS1(t,payload,false)
			defer srv.Close()
			meta.ContentType=kind
			reg:=StaticRegistry{Nodes:map[string]string{"storage-a":endpoint}}
			for _,strategy:=range []Strategy{RemotePrefetch,RemoteBlockStream}{
				plan:=Decision{
					Strategy:strategy,
					ref:Reference{StorageNodeID:meta.OwnerNodeID,FileID:meta.FileID},
					meta:meta,
				}
				reader,err:=(RemoteReaderFactory{Registry:reg}).Open(context.Background(),plan,7)
				if err!=nil{t.Fatal(err)}
				got,err:=io.ReadAll(reader)
				if err!=nil{t.Fatalf("%s: %v",kind,err)}
				if !bytes.Equal(got,payload[7:]){t.Fatalf("%s corrupted: got %d want %d",kind,len(got),len(payload)-7)}
				if _,err:=reader.Read(make([]byte,1));!errors.Is(err,io.EOF){t.Fatal("missing EOF",err)}
				_ = reader.Close()
				if _,err:=reader.Read(make([]byte,1));!errors.Is(err,io.ErrClosedPipe){t.Fatal("read after close",err)}
			}
		})
	}
}

func TestRemoteReaderRejectsUnknownNodeAndBadOffset(t *testing.T) {
	n:=int64(12)
	m:=Metadata{OwnerNodeID:"storage-a",FileID:"hc1.abc",Version:"v1",SizeBytes:&n,RangeReadable:true}
	p:=Decision{Strategy:RemoteBlockStream,ref:Reference{StorageNodeID:m.OwnerNodeID,FileID:m.FileID},meta:m}
	f:=RemoteReaderFactory{Registry:StaticRegistry{Nodes:map[string]string{}}}
	if _,err:=f.Open(context.Background(),p,0);!errors.Is(err,ErrSourceOffline){t.Fatal(err)}
	if _,err:=f.Open(context.Background(),p,13);!errors.Is(err,ErrInvalidReference){t.Fatal(err)}
	ctx,cancel:=context.WithCancel(context.Background())
	cancel()
	if _,err:=f.Open(ctx,p,0);!errors.Is(err,context.Canceled){t.Fatal(err)}
}
