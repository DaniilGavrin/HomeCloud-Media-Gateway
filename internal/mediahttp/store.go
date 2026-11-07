// Package mediahttp provides an opt-in, room-ticket-bound HTTP byte-range
// delivery path for Android's native WebView media decoder. No arbitrary file
// URL, file path or storage endpoint is accepted from TV clients.
package mediahttp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/directws"
	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/source"
	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/mediainspect"
	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/remux"
)

type Offer struct {
	Path        string
	ContentType string
	MediaKind   string
}

type Report func(directws.Update)

type session struct {
	lease directws.Lease
	peer netip.Addr
	token source.Token
	meta source.Metadata
	expires time.Time
	streamed bool
	ended bool
	reportSeq uint64
 conversion *remux.Profile
 convertRevoked chan struct{}
 convertActive bool
}

type Store struct {
	mu sync.Mutex
	byToken map[string]*session
	book *directws.Book
	plans *source.Plans
	factories map[source.Strategy]source.ReaderFactory
	report Report
 converter *remux.Runner
}

func New(book *directws.Book, plans *source.Plans, factories map[source.Strategy]source.ReaderFactory, report Report) *Store {
	return &Store{book:book,plans:plans,factories:factories,byToken:make(map[string]*session),report:report}
}

func (s *Store) SetConverter(r *remux.Runner){s.converter=r}

// GrantConversion can be called only on a valid TV direct-WS lease. The
// selected track is checked against ffprobe and declared OUTPUT capabilities.
func (s *Store) GrantConversion(l directws.Lease,peer string,media mediainspect.Media,
 caps mediainspect.TVCapabilities,selectedAudio int)(Offer,error) {
 if s==nil||s.converter==nil||s.plans==nil||s.book==nil||!s.book.Valid(l.RoomID,l){return Offer{},remux.ErrUnsupported}
 ip,err:=peerIP(peer);if err!=nil{return Offer{},err}
 profile,err:=remux.Select(media,caps,selectedAudio);if err!=nil{return Offer{},err}
 token:=source.Token{RoomID:l.RoomID,PlaybackID:l.PlaybackID,Generation:l.Generation}
 plan,err:=s.plans.Get(token);if err!=nil{return Offer{},err}
 if s.factories[plan.Strategy]==nil{return Offer{},source.ErrNotConfigured}
 meta:=plan.Metadata();if meta.SizeBytes==nil||*meta.SizeBytes<=0{return Offer{},source.ErrInvalidMetadata}
 var entropy [32]byte
 if _,err=rand.Read(entropy[:]);err!=nil{return Offer{},err}
 secret:=hex.EncodeToString(entropy[:])
 entry:=&session{lease:l,peer:ip,token:token,meta:meta,expires:time.Now().Add(24*time.Hour),
  conversion:&profile,convertRevoked:make(chan struct{})}
 s.mu.Lock()
 if !s.book.Valid(l.RoomID,l){s.mu.Unlock();return Offer{},remux.ErrUnsupported}
 for existing,old:=range s.byToken {
  if old.conversion!=nil&&old.lease.RoomID==l.RoomID&&old.lease.PlaybackID==l.PlaybackID&&
   old.lease.Generation==l.Generation {
   delete(s.byToken,existing)
   close(old.convertRevoked)
  }
 }
 s.byToken[secret]=entry
 s.mu.Unlock()
 go func(){
  <-l.Done
  s.mu.Lock()
  if s.byToken[secret]==entry{delete(s.byToken,secret)}
  s.mu.Unlock()
 }()
 mime:="video/mp4";if profile.MediaKind=="audio"{mime="audio/mp4"}
 return Offer{Path:"/media/convert/"+secret,ContentType:mime,MediaKind:profile.MediaKind},nil
}

func peerIP(address string) (netip.Addr,error) {
	host, _, err:=net.SplitHostPort(address)
	if err!=nil{return netip.Addr{},err}
	ip,err:=netip.ParseAddr(host)
	if err!=nil{return netip.Addr{},err}
	return ip.Unmap(),nil
}

// Grant can be called ONLY after directws.Book.Claim has successfully consumed
// a Backend-issued one-use attachment ticket. Tokens are random and bound to
// the same private IP as that TV's WebSocket. The lease revokes on room STOP.
func (s *Store) Grant(lease directws.Lease, remoteAddr string) (Offer,error) {
	if s==nil||s.plans==nil||s.book==nil||!s.book.Valid(lease.RoomID,lease) {
		return Offer{},source.ErrNotConfigured
	}
	ip,err:=peerIP(remoteAddr)
	if err!=nil{return Offer{},err}
	token:=source.Token{RoomID:lease.RoomID,PlaybackID:lease.PlaybackID,Generation:lease.Generation}
	plan,err:=s.plans.Get(token)
	if err!=nil{return Offer{},err}
	if s.factories[plan.Strategy]==nil{return Offer{},source.ErrNotConfigured}
	meta:=plan.Metadata()
	if meta.SizeBytes==nil||*meta.SizeBytes<0{
		return Offer{},source.ErrInvalidMetadata
	}
	kind:=""
	if strings.HasPrefix(meta.ContentType,"audio/"){kind="audio"}
	if strings.HasPrefix(meta.ContentType,"video/"){kind="video"}
	if kind==""{return Offer{},source.ErrInvalidMetadata}
	var entropy [32]byte
	if _,err:=rand.Read(entropy[:]);err!=nil{return Offer{},err}
	secret:=hex.EncodeToString(entropy[:])
	entry:=&session{lease:lease,peer:ip,token:token,meta:meta,expires:time.Now().Add(24*time.Hour)}
	s.mu.Lock()
	s.byToken[secret]=entry
	s.mu.Unlock()
	go func(){
		<-lease.Done
		s.mu.Lock()
		delete(s.byToken,secret)
		s.mu.Unlock()
	}()
	return Offer{Path:"/media/session/"+secret,ContentType:meta.ContentType,MediaKind:kind},nil
}

func (s *Store) ServeHTTP(w http.ResponseWriter,r *http.Request){
	if s==nil||s.book==nil {http.NotFound(w,r);return}
	if r.Method!="GET" && r.Method!="HEAD" {http.Error(w,"method not allowed",405);return}
	converted:=strings.HasPrefix(r.URL.Path,"/media/convert/")
	secret:=strings.TrimPrefix(r.URL.Path,"/media/session/")
	if converted {secret=strings.TrimPrefix(r.URL.Path,"/media/convert/")}
	if len(secret)!=64 || strings.Contains(secret,"/") {
		http.NotFound(w,r);return
	}
	ip,err:=peerIP(r.RemoteAddr)
	if err!=nil{http.NotFound(w,r);return}
	s.mu.Lock()
	entry:=s.byToken[secret]
	s.mu.Unlock()
	if entry==nil||entry.peer!=ip||time.Now().After(entry.expires)||
		!s.book.Valid(entry.lease.RoomID,entry.lease)||
  (entry.conversion!=nil)!=converted {
		http.NotFound(w,r);return
	}
 if entry.conversion!=nil {
  s.serveConversion(w,r,entry)
  return
 }
	size:=*entry.meta.SizeBytes
	start,end,status,ok:=parseByteRange(r.Header.Get("Range"),size)
	if !ok{
		w.Header().Set("Content-Range",fmt.Sprintf("bytes */%d",size))
		http.Error(w,"invalid range",http.StatusRequestedRangeNotSatisfiable)
		return
	}
	w.Header().Set("Accept-Ranges","bytes")
	w.Header().Set("Content-Type",entry.meta.ContentType)
	w.Header().Set("Cache-Control","private, no-store")
	w.Header().Set("X-Content-Type-Options","nosniff")
	w.Header().Set("Content-Length",strconv.FormatInt(end-start+1,10))
	if status==http.StatusPartialContent{
		w.Header().Set("Content-Range",fmt.Sprintf("bytes %d-%d/%d",start,end,size))
	}
	if r.Method=="HEAD"{w.WriteHeader(status);return}
	if end<start{
		w.WriteHeader(status);return
	}
	// A TV can continue an already-started HTTP Range request even after its
	// ticket is revoked. Tie that request to the lease, not only the initial
	// authorization check. The canceled context interrupts remote HCS1 reads,
	// and closing the reader releases the local file descriptor.
	streamCtx,cancel:=context.WithCancel(r.Context())
	defer cancel()
	reader,err:=source.OpenFromPlan(streamCtx,entry.token,start,s.plans,s.factories)
	if err!=nil{
		http.Error(w,"source unavailable",http.StatusServiceUnavailable)
		return
	}
	defer reader.Close()
	closed:=make(chan struct{})
	go func(){
		select {
		case <-entry.lease.Done:
			cancel()
			_ = reader.Close()
		case <-closed:
		}
	}()
	defer close(closed)
	// Revocation may have happened while opening a slow remote source.
	select {
	case <-entry.lease.Done:
		return
	default:
	}
	// The reader's Read is bounded (HCS1 blocks <=256KiB); io.CopyN uses a
	// small heap buffer, so gigabyte files do not load into Gateway RAM.
	w.WriteHeader(status)
	// A completed HTTP response is only a completed read, not playback EOF.
	// WebView may download the entire file into its buffer long before it
	// finishes decoding and playing it. Only the TV decoder reports ENDED.
	// Check revocation before every write, not just before starting the GET.
	_,_ = io.CopyN(&firstByteReporter{ResponseWriter:w, revoked:entry.lease.Done,
		onWrite:func(){s.emit(entry,"STREAMING","")}},reader,end-start+1)
}

// serveConversion never pretends an encoded stream has byte-offset parity
// with its original source. Seek/range must be negotiated in a later protocol.
func (s *Store) serveConversion(w http.ResponseWriter,r *http.Request,entry *session){
 if s.converter==nil{http.Error(w,"converter unavailable",503);return}
 if x:=r.Header.Get("Range");x!=""&&x!="bytes=0-" {
  http.Error(w,"converted stream is not seekable",http.StatusRequestedRangeNotSatisfiable);return
 }
 mime:="video/mp4";if entry.conversion.MediaKind=="audio"{mime="audio/mp4"}
 w.Header().Set("Content-Type",mime)
 w.Header().Set("Cache-Control","private, no-store")
 w.Header().Set("X-Content-Type-Options","nosniff")
 w.Header().Set("Accept-Ranges","none")
 if r.Method=="HEAD"{w.WriteHeader(http.StatusOK);return}
 s.mu.Lock()
 if entry.convertActive{s.mu.Unlock();http.Error(w,"conversion already active",http.StatusConflict);return}
 entry.convertActive=true
 s.mu.Unlock()
 defer func(){s.mu.Lock();entry.convertActive=false;s.mu.Unlock()}()
 ctx,cancel:=context.WithCancel(r.Context())
 defer cancel()
 finished:=make(chan struct{})
 go func(){
  select {
  case <-entry.lease.Done:cancel()
  case <-entry.convertRevoked:cancel()
  case <-finished:
  }
 }()
 defer close(finished)
 src:=remux.Input{Size:*entry.meta.SizeBytes,Open:func(ctx context.Context,offset int64)(io.ReadCloser,error){
  return source.OpenFromPlan(ctx,entry.token,offset,s.plans,s.factories)
 }}
 w.WriteHeader(http.StatusOK)
 err:=s.converter.Run(ctx,src,*entry.conversion,&firstByteReporter{
  ResponseWriter:w,revoked:entry.lease.Done,
  onWrite:func(){s.emit(entry,"STREAMING","")},
 })
 if err!=nil&&ctx.Err()==nil{s.emit(entry,"ERROR","REMUX_STREAM_ERROR")}
}

type firstByteReporter struct {
	http.ResponseWriter
	revoked <-chan struct{}
	onWrite func()
	emitted bool
}
func (w *firstByteReporter) Write(p []byte)(int,error){
	select {
	case <-w.revoked:
		return 0,context.Canceled
	default:
	}
	n,err:=w.ResponseWriter.Write(p)
	if n>0&&!w.emitted {
		w.emitted=true
		w.onWrite()
	}
	return n,err
}

func (s *Store) emit(entry *session,phase,code string){
	if s.report==nil{return}
	s.mu.Lock()
	if (phase=="STREAMING"&&entry.streamed)||(phase=="ENDED"&&entry.ended){
		s.mu.Unlock();return
	}
	if phase=="STREAMING"{entry.streamed=true}
	if phase=="ENDED"{entry.ended=true}
	entry.reportSeq++
	seq:=entry.reportSeq
	s.mu.Unlock()
	if !s.book.Valid(entry.lease.RoomID,entry.lease){return}
	s.report(directws.Update{RoomID:entry.lease.RoomID,PlaybackID:entry.lease.PlaybackID,
		Generation:entry.lease.Generation,Epoch:entry.lease.Epoch,
		Phase:phase,Sequence:seq,ErrorCode:code})
}

func parseByteRange(value string,size int64)(start,end int64,status int,ok bool){
	if size<0{return 0,0,0,false}
	if value==""{return 0,size-1,http.StatusOK,true}
	if size==0||!strings.HasPrefix(value,"bytes="){return 0,0,0,false}
	value=strings.TrimPrefix(value,"bytes=")
	if strings.Contains(value,","){return 0,0,0,false}
	pieces:=strings.Split(value,"-")
	if len(pieces)!=2{return 0,0,0,false}
	if pieces[0]=="" {
		// Suffix request, e.g. bytes=-500.
		n,err:=strconv.ParseInt(pieces[1],10,64)
		if err!=nil||n<=0{return 0,0,0,false}
		if n>size{n=size}
		return size-n,size-1,http.StatusPartialContent,true
	}
	start,err:=strconv.ParseInt(pieces[0],10,64)
	if err!=nil||start<0||start>=size{return 0,0,0,false}
	end=size-1
	if pieces[1]!=""{
		finish,e:=strconv.ParseInt(pieces[1],10,64)
		if e!=nil||finish<start{return 0,0,0,false}
		if finish<end{end=finish}
	}
	return start,end,http.StatusPartialContent,true
}

var _ http.Handler = (*Store)(nil)
