package mediahttp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/directws"
	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/source"
 "github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/mediainspect"
 "github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/remux"
)

type testClient struct { meta source.Metadata }
func (c testClient) Stat(_ context.Context, _ string)(source.Metadata,error){return c.meta,nil}
func (c testClient) OpenRange(context.Context,source.RangeRequest)(io.ReadCloser,source.RangeReply,error){return nil,source.RangeReply{},source.ErrNotConfigured}
type testRegistry struct { meta source.Metadata }
func (r testRegistry) ForNode(_ context.Context,node string)(source.Client,error){
	if node != r.meta.OwnerNodeID {return nil,source.ErrSourceOffline}
	return testClient{meta:r.meta},nil
}
type readerFactory struct { raw []byte }
func (r readerFactory) Open(_ context.Context,plan source.Decision,offset int64)(source.Reader,error){
	if offset>int64(len(r.raw))||offset<0{return nil,source.ErrInvalidReference}
	return io.NopCloser(bytes.NewReader(r.raw[offset:])),nil
}

func createGrant(t *testing.T,kind string)(*Store,*directws.Book,directws.Lease){
	t.Helper()
	data:=[]byte("0123456789")
	size:=int64(len(data))
	ref:=source.Reference{StorageNodeID:"node-1",FileID:"file-1"}
	meta:=source.Metadata{OwnerNodeID:ref.StorageNodeID,FileID:ref.FileID,Version:"v1",
		SizeBytes:&size,ContentType:kind,RangeReadable:true}
	plans:=source.NewPlans()
	token:=source.Token{RoomID:"room-1",PlaybackID:"play-1",Generation:1}
	co:=source.Coordinator{Resolver:&source.Resolver{Registry:testRegistry{meta:meta}},Plans:plans}
	if _,err:=co.Prepare(context.Background(),token,ref);err!=nil{t.Fatal(err)}
	book:=directws.New(2)
	g:=directws.Grant{
		RoomID:token.RoomID,PlaybackID:token.PlaybackID,Generation:token.Generation,
		DeviceID:"tv-1",Ticket:strings.Repeat("a",64),
		ExpiresAt:time.Now().Add(30*time.Second),
	}
	if err:=book.Prepare(g);err!=nil{t.Fatal(err)}
	lease,err:=book.Claim(directws.Hello{V:1,Type:"media.hello",RoomID:g.RoomID,
		PlaybackID:g.PlaybackID,DeviceID:g.DeviceID,Generation:g.Generation,
		AttachTicket:g.Ticket})
	if err!=nil{t.Fatal(err)}
	return New(book,plans,map[source.Strategy]source.ReaderFactory{
		source.RemotePrefetch:readerFactory{raw:data},
	},nil),book,lease
}
func TestTicketScopedHTTPRangesAudioVideo(t *testing.T){
	for _,kind:=range []string{"video/mp4","audio/mpeg"}{
		t.Run(kind,func(t *testing.T){
			s,book,lease:=createGrant(t,kind)
			defer book.Close()
			var phases []string
			s.report = func(u directws.Update) { phases=append(phases,u.Phase) }
			offer,err:=s.Grant(lease,"127.0.0.1:40405")
			if err!=nil||offer.ContentType!=kind||len(offer.Path)!=len("/media/session/")+64{
				t.Fatal(offer,err)
			}
			request:=func(method,rangeValue,peer,path string)*httptest.ResponseRecorder{
				t.Helper()
				req:=httptest.NewRequest(method,path,nil)
				req.RemoteAddr=peer
				if rangeValue!=""{req.Header.Set("Range",rangeValue)}
				resp:=httptest.NewRecorder()
				s.ServeHTTP(resp,req)
				return resp
			}
			ok:=request("GET","bytes=2-5","127.0.0.1:9999",offer.Path)
			if ok.Code!=206||ok.Body.String()!="2345"||ok.Header().Get("Content-Range")!="bytes 2-5/10"{
				t.Fatalf("partial: status=%d body=%q headers=%v",ok.Code,ok.Body.String(),ok.Header())
			}
			suffix:=request("GET","bytes=-3","127.0.0.1:9999",offer.Path)
			if suffix.Code!=206||suffix.Body.String()!="789"{t.Fatalf("suffix %d %s",suffix.Code,suffix.Body.String())}
			for _,phase := range phases {
				if phase == "ENDED" { t.Fatalf("partial/suffix bytes cannot report decoder EOF: %v", phases) }
			}
			all:=request("GET","","127.0.0.1:9999",offer.Path)
			if all.Code!=200||all.Body.String()!="0123456789"{t.Fatal(all.Code,all.Body.String())}
			// Even a complete HTTP 200 body may be buffered before playback
			// ends. Decoder EOF belongs to the TV, not the file delivery server.
			for _,phase := range phases {
				if phase == "ENDED" { t.Fatalf("HTTP download reported decoder EOF: %v", phases) }
			}
			head:=request("HEAD","","127.0.0.1:9999",offer.Path)
			if head.Code!=200||head.Body.Len()!=0||head.Header().Get("Content-Length")!="10"{t.Fatal(head.Code,head.Body.String())}
			invalid:=request("GET","bytes=0-10,99-","127.0.0.1:9999",offer.Path)
			if invalid.Code!=416{t.Fatal("multi-range allowed",invalid.Code)}
			forged:=request("GET","","127.0.0.1:9999","/media/session/"+strings.Repeat("0",64))
			if forged.Code!=404{t.Fatal("forged token allowed",forged.Code)}
			otherIP:=request("GET","","192.168.2.50:4010",offer.Path)
			if otherIP.Code!=404{t.Fatal("other TV IP allowed",otherIP.Code)}
			book.Remove(lease.RoomID)
			revoked:=request("GET","","127.0.0.1:9999",offer.Path)
			if revoked.Code!=404{t.Fatal("revoked ticket still usable",revoked.Code)}
		})
	}
}
func TestHTTPMediaNotGrantedBeforeValidTicket(t *testing.T){
	s,book,lease:=createGrant(t,"audio/mpeg")
	book.Remove(lease.RoomID)
	if _,err:=s.Grant(lease,"127.0.0.1:1234");err==nil{t.Fatal("grant after STOP")}
}
func TestByteRangeSyntax(t *testing.T){
	for _,tc:=range []struct{input string;size int64;start,end int64;ok bool}{
		{"",10,0,9,true},{"bytes=0-",10,0,9,true},
		{"bytes=-3",10,7,9,true},{"bytes=9-99",10,9,9,true},
		{"bytes=10-",10,0,0,false},{"bytes=3-2",10,0,0,false},
		{"bytes=0-1,2-3",10,0,0,false},{"bytes=-0",10,0,0,false},
	}{
		start,end,_,ok:=parseByteRange(tc.input,tc.size)
		if start!=tc.start||end!=tc.end||ok!=tc.ok{t.Fatalf("%q => %d,%d,%v",tc.input,start,end,ok)}
	}
}

type blockedReadFactory struct{ entered chan struct{} }
func (f blockedReadFactory) Open(ctx context.Context,_ source.Decision,_ int64)(source.Reader,error){
	return &blockedRead{ctx:ctx,entered:f.entered},nil
}
type blockedRead struct {
	ctx context.Context
	entered chan struct{}
	once sync.Once
}
func (r *blockedRead) Read(_ []byte)(int,error){
	r.once.Do(func(){close(r.entered)})
	<-r.ctx.Done()
	return 0,r.ctx.Err()
}
func (*blockedRead) Close()error{return nil}

func TestKAN55SwitchCancelsInFlightHTTPRange(t *testing.T){
	s,book,lease:=createGrant(t,"video/mp4")
	defer book.Close()
	entered:=make(chan struct{})
	s.factories[source.RemotePrefetch]=blockedReadFactory{entered:entered}
	offer,err:=s.Grant(lease,"127.0.0.1:40405")
	if err!=nil{t.Fatal(err)}
	req:=httptest.NewRequest("GET",offer.Path,nil)
	req.RemoteAddr="127.0.0.1:40405"
	resp:=httptest.NewRecorder()
	finished:=make(chan struct{})
	go func(){
		defer close(finished)
		s.ServeHTTP(resp,req)
	}()
	select {
	case <-entered:
	case <-time.After(3*time.Second):t.Fatal("range never started")
	}
	// New playback revokes the old lease, while preserving room_id.
	g:=directws.Grant{RoomID:lease.RoomID,PlaybackID:"play-2",Generation:2,
		DeviceID:lease.DeviceID,Ticket:strings.Repeat("e",64),
		ExpiresAt:time.Now().Add(30*time.Second)}
	if err=book.Prepare(g);err!=nil{t.Fatal(err)}
	select {
	case <-finished:
	case <-time.After(3*time.Second):t.Fatal("old HTTP Range ignored playback switch")
	}
	if resp.Body.Len()!=0{t.Fatalf("old playback sent %d bytes after cancellation",resp.Body.Len())}
	if !errors.Is((&firstByteReporter{ResponseWriter:httptest.NewRecorder(),revoked:lease.Done}).writeProbe(),context.Canceled){
		t.Fatal("revoked stream was still writable")
	}
	newReq:=httptest.NewRequest(http.MethodGet,offer.Path,nil)
	newReq.RemoteAddr="127.0.0.1:40405"
	old:=httptest.NewRecorder()
	s.ServeHTTP(old,newReq)
	if old.Code!=http.StatusNotFound{t.Fatalf("old token accepted after switch: %d",old.Code)}
}

func (w *firstByteReporter) writeProbe() error {
	_,err:=w.Write([]byte("stale-media"))
	return err
}


func TestKAN63ConversionGrantNeedsLiveLeaseAndOutputCapability(t *testing.T){
 s,book,lease:=createGrant(t,"video/x-matroska")
 defer book.Close()
 s.converter=&remux.Runner{Binary:"ffmpeg"} // only grant is exercised
 media:=mediainspect.Media{Container:"matroska",Tracks:[]mediainspect.Track{
  {Index:0,Kind:"video",Codec:"h264",Profile:"High",Level:40,Width:1920,Height:1080},
  {Index:1,Kind:"audio",Codec:"dts"},
 }}
 cap:=mediainspect.TVCapabilities{Version:1,Formats:[]mediainspect.FormatSupport{{
  Container:"mp4",VideoCodec:"h264",AudioCodec:"aac",Certainty:"probably",
  VideoProfiles:[]string{"High"},MaxLevel:40,MaxWidth:1920,MaxHeight:1080,
 }}}
 first,err:=s.GrantConversion(lease,"127.0.0.1:2000",media,cap,1)
 if err!=nil||!strings.HasPrefix(first.Path,"/media/convert/"){
  t.Fatalf("grant conversion failed: %+v %v",first,err)
 }
 if first.ContentType!="video/mp4"||first.MediaKind!="video"{t.Fatal(first)}
 other:=httptest.NewRequest("GET",first.Path,nil)
 other.RemoteAddr="127.0.0.2:3123"
 resp:=httptest.NewRecorder();s.ServeHTTP(resp,other)
 if resp.Code!=404{t.Fatalf("other IP used conversion token: %d",resp.Code)}
 seek:=httptest.NewRequest("GET",first.Path,nil)
 seek.RemoteAddr="127.0.0.1:3123"
 seek.Header.Set("Range","bytes=100-")
 resp=httptest.NewRecorder();s.ServeHTTP(resp,seek)
 if resp.Code!=416{t.Fatalf("pretended output byte seek: %d",resp.Code)}
 cap.Formats[0].Certainty="maybe"
 if _,err:=s.GrantConversion(lease,"127.0.0.1:2000",media,cap,1);!errors.Is(err,remux.ErrUnsupported){
  t.Fatalf("unknown TV output incorrectly authorized: %v",err)
 }
 cap.Formats[0].Certainty="probably"
 second,err:=s.GrantConversion(lease,"127.0.0.1:2000",media,cap,1)
 if err!=nil{t.Fatal(err)}
 if second.Path==first.Path{t.Fatal("reused a conversion URL")}
 old:=httptest.NewRequest("HEAD",first.Path,nil)
 old.RemoteAddr="127.0.0.1:5432"
 oldResponse:=httptest.NewRecorder()
 s.ServeHTTP(oldResponse,old)
 if oldResponse.Code!=404{t.Fatalf("replaced audio URL survived: %d",oldResponse.Code)}
 book.Remove(lease.RoomID)
 if _,err:=s.GrantConversion(lease,"127.0.0.1:2000",media,cap,1);err==nil{
  t.Fatal("conversion issued after STOP")
 }
}
