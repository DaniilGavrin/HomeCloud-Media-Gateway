package mediainspect

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/source"
)

var (
	ErrUnavailable = errors.New("INSPECTOR_UNAVAILABLE")
	ErrBudget = errors.New("INSPECTION_BYTE_BUDGET_EXCEEDED")
	ErrBusy = errors.New("INSPECTOR_BUSY")
)

const (
	maxProbeOutput = 256 << 10
	maxProbeBytes int64 = 32 << 20
	defaultProbeTimeout = 8 * time.Second
)

// Inspector is an optional, bounded ffprobe runner. It does not resolve paths:
// the HC SS owner must authorize and version-fence the source beforehand.
type Inspector struct {
	Binary string // trusted administrator setting, never TV/user input
	Plans *source.Plans
	Readers map[source.Strategy]source.ReaderFactory
	Timeout time.Duration
	LimitBytes int64
	slots chan struct{}
	mu sync.Mutex
	cache map[source.Token]probeCache
}
type probeCache struct {
	version string
	media Media
	expires time.Time
}

func New(binary string,plans *source.Plans,readers map[source.Strategy]source.ReaderFactory) *Inspector {
	return &Inspector{Binary:binary,Plans:plans,Readers:readers,
		Timeout:defaultProbeTimeout,LimitBytes:maxProbeBytes,slots:make(chan struct{},2),
		cache:make(map[source.Token]probeCache)}
}

// Probe uses a short-lived loopback-only seekable HTTP facade. ffprobe may
// request tail ranges of remote MKV/MP4 without downloading the whole file,
// and HC SS still enforces roots and source version on every ranged read.
func (p *Inspector) Probe(parent context.Context,tok source.Token)(Media,error) {
	if p==nil||p.Plans==nil||p.Binary==""||p.slots==nil {
		return Media{},ErrUnavailable
	}
	if err:=parent.Err();err!=nil{return Media{},err}
	plan,err:=p.Plans.Get(tok)
	if err!=nil{return Media{},err}
	meta:=plan.Metadata()
	if meta.SizeBytes==nil||*meta.SizeBytes<=0 {
		return Media{},ErrUnavailable
	}
	p.mu.Lock()
	if entry,ok:=p.cache[tok];ok&&entry.version==meta.Version&&time.Now().Before(entry.expires) {
		p.mu.Unlock()
		return entry.media,nil
	}
	p.mu.Unlock()
	select {
	case p.slots<-struct{}{}:defer func(){<-p.slots}()
	case <-parent.Done():return Media{},parent.Err()
	default:return Media{},ErrBusy
	}
	timeout:=p.Timeout
	if timeout<=0||timeout>15*time.Second{timeout=defaultProbeTimeout}
	ctx,cancel:=context.WithTimeout(parent,timeout)
	defer cancel()
	if p.Readers[plan.Strategy]==nil {return Media{},source.ErrNotConfigured}
	binary,err:=exec.LookPath(p.Binary)
	if err!=nil {return Media{},ErrUnavailable}
	entropy:=make([]byte,16)
	if _,err=rand.Read(entropy);err!=nil{return Media{},err}
	secret:=hex.EncodeToString(entropy)
	budget:=&atomic.Int64{}
	limit:=p.LimitBytes
	if limit<=0||limit>maxProbeBytes{limit=maxProbeBytes}
	budget.Store(limit)
	handler:=http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){
		w.Header().Set("Cache-Control","no-store")
		w.Header().Set("X-Content-Type-Options","nosniff")
		if r.URL.Path!="/"+secret||(r.Method!="GET"&&r.Method!="HEAD") {
			http.NotFound(w,r);return
		}
		if r.Method=="GET"&&budget.Load()<=0 {
			http.Error(w,"probe budget exceeded",http.StatusServiceUnavailable);return
		}
		if ctx.Err()!=nil {http.NotFound(w,r);return}
		reader:=&seekable{ctx:r.Context(),lifetime:ctx,token:tok,
			size:*meta.SizeBytes,plans:p.Plans,readers:p.Readers,budget:budget}
		defer reader.Close()
		w.Header().Set("Content-Type","application/octet-stream")
		http.ServeContent(w,r,"source",time.Time{},reader)
	})
	listener,err:=net.Listen("tcp","127.0.0.1:0")
	if err!=nil{return Media{},fmt.Errorf("%w: loopback listen: %v",ErrUnavailable,err)}
	server:=&http.Server{Handler:handler,ReadHeaderTimeout:2*time.Second}
	go func(){_ = server.Serve(listener)}()
	defer server.Close()
	url:="http://"+listener.Addr().String()+"/"+secret
	command:=exec.CommandContext(ctx,binary,
		"-v","error","-rw_timeout","5000000","-probesize","4194304",
		"-analyzeduration","5000000","-show_format","-show_streams",
		"-of","json","-i",url)
	// Bound waits for inherited child pipe handles after forced cancellation.
	command.WaitDelay=250*time.Millisecond
	var output cappedOutput
	command.Stdout=&output
	command.Stderr=io.Discard // never log probe process output/paths/tokens
	if err=command.Run();err!=nil{
		if ctx.Err()!=nil{return Media{},ctx.Err()}
		return Media{},fmt.Errorf("%w: ffprobe exit: %v",ErrUnavailable,err)
	}
	if output.overflow {return Media{},ErrUnavailable}
	media,err:=Parse(output.Bytes(),meta.ContentType)
	if err!=nil{return Media{},err}
	// Avoid repeated ffprobe/HC SS reads for TV reconnect/ticket refresh.
	// Cache is bounded and keyed by room/playback/generation and file version.
	if ctx.Err()==nil {
		p.mu.Lock()
		if p.cache==nil{p.cache=make(map[source.Token]probeCache)}
		if len(p.cache)>=64{
			for k:=range p.cache {delete(p.cache,k)}
		}
		p.cache[tok]=probeCache{version:meta.Version,media:media,
			expires:time.Now().Add(5*time.Minute)}
		p.mu.Unlock()
	}
	return media,nil
}

type cappedOutput struct {
	bytes.Buffer
	overflow bool
}
func (w *cappedOutput) Write(data []byte)(int,error){
	if w.Len()+len(data)>maxProbeOutput {
		w.overflow=true
		return 0,ErrBudget
	}
	return w.Buffer.Write(data)
}

type seekable struct {
	ctx context.Context
	lifetime context.Context
	token source.Token
	plans *source.Plans
	readers map[source.Strategy]source.ReaderFactory
	budget *atomic.Int64
	size int64
	offset int64
	active io.ReadCloser
	mu sync.Mutex
}
func (r *seekable) Close()error{
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.active!=nil{err:=r.active.Close();r.active=nil;return err}
	return nil
}
func (r *seekable) Seek(n int64,whence int)(int64,error){
	r.mu.Lock()
	defer r.mu.Unlock()
	var base int64
	switch whence{
	case io.SeekStart:base=0
	case io.SeekCurrent:base=r.offset
	case io.SeekEnd:base=r.size
	default:return r.offset,errors.New("INVALID_SEEK_WHENCE")
	}
	if n>0&&base>int64(^uint64(0)>>1)-n||
		n<0&&base< -int64(^uint64(0)>>1)-1-n {
		return r.offset,source.ErrInvalidReference
	}
	next:=base+n
	if next<0||next>r.size{return r.offset,source.ErrInvalidReference}
	if r.active!=nil{_ =r.active.Close();r.active=nil}
	r.offset=next
	return next,nil
}
func (r *seekable) Read(data []byte)(int,error){
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(data)==0{return 0,nil}
	if err:=r.ctx.Err();err!=nil{return 0,err}
	if err:=r.lifetime.Err();err!=nil{return 0,err}
	if r.offset>=r.size{return 0,io.EOF}
	var reserve int64
	for {
		remaining:=r.budget.Load()
		if remaining<=0{return 0,ErrBudget}
		reserve=int64(len(data))
		if reserve>remaining{reserve=remaining}
		if reserve>r.size-r.offset{reserve=r.size-r.offset}
		if r.budget.CompareAndSwap(remaining,remaining-reserve){break}
	}
	if r.active==nil {
		reader,err:=source.OpenFromPlan(r.lifetime,r.token,r.offset,r.plans,r.readers)
		if err!=nil{return 0,err}
		r.active=reader
	}
	n,err:=r.active.Read(data[:int(reserve)])
	r.offset+=int64(n)
	if err!=nil||n==0 {
		_ = r.active.Close()
		r.active=nil
	}
	return n,err
}

var _ io.ReadSeeker = (*seekable)(nil)

// Parse requested numeric FFmpeg fields as strings; ffprobe emits N/A for
// some containers, and integer overflow must never reach TV JSON.
func parseFrameRate(s string) string {
	if len(s)>48||strings.ContainsAny(s,"\r\n\x00"){return ""}
	if strings.Contains(s,"/"){
		p:=strings.Split(s,"/")
		if len(p)!=2{return ""}
		if _,err:=strconv.ParseUint(p[0],10,64);err!=nil{return ""}
		if _,err:=strconv.ParseUint(p[1],10,64);err!=nil{return ""}
	}
	return s
}
