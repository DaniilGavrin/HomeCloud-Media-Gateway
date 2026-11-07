package remux

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
 "time"
)

type Input struct {
 Size int64
 // Open returns a reader starting exactly at byte offset. Callers must
 // authorize the underlying Source Plan before constructing this callback.
 Open func(context.Context,int64)(io.ReadCloser,error)
}

var ErrSource = errors.New("REMUX_SOURCE_UNAVAILABLE")

// Runner limits simultaneous FFmpeg conversions, to protect low-power homes.
type Runner struct {
 Binary string
 slots chan struct{}
}

func New(binary string, concurrent int) (*Runner,error) {
 if strings.TrimSpace(binary)=="" {return nil,errors.New("FFMPEG_BINARY_REQUIRED")}
 path,err:=exec.LookPath(binary)
 if err!=nil {return nil,fmt.Errorf("FFMPEG_UNAVAILABLE: %w",err)}
 if concurrent<1||concurrent>4 {return nil,errors.New("INVALID_REMUX_CONCURRENCY")}
 return &Runner{Binary:path,slots:make(chan struct{},concurrent)},nil
}

// Run streams fragmented MP4 directly to output. It does not allocate a
// complete media file and does not encode video. FFmpeg input is a private,
// short-lived, seekable loopback Range endpoint; it is NOT a client file URL.
// Reader, local HTTP listener and child process all die with ctx/STOP.
func (r *Runner) Run(ctx context.Context,src Input,p Profile,out io.Writer) error {
 if r==nil||out==nil||src.Open==nil||src.Size<=0 {return ErrSource}
 if err:=p.Validate();err!=nil{return err}
 select {
 case r.slots<-struct{}{}:defer func(){<-r.slots}()
 case <-ctx.Done():return ctx.Err()
 }
 ln,err:=net.Listen("tcp","127.0.0.1:0")
 if err!=nil{return err}
 var nonce [16]byte
 if _,err=rand.Read(nonce[:]);err!=nil{_ = ln.Close();return err}
 route:="/input/"+hex.EncodeToString(nonce[:])
 server:=&http.Server{ReadHeaderTimeout:5*time.Second,Handler:http.HandlerFunc(func(w http.ResponseWriter,q *http.Request){
  if q.URL.Path!=route || q.URL.RawQuery!="" {http.NotFound(w,q);return}
  if q.Method!="GET" && q.Method!="HEAD" {http.Error(w,"method not allowed",http.StatusMethodNotAllowed);return}
  from,to,status,ok:=inputRange(q.Header.Get("Range"),src.Size)
  if !ok {
   w.Header().Set("Content-Range",fmt.Sprintf("bytes */%d",src.Size))
   http.Error(w,"invalid byte range",http.StatusRequestedRangeNotSatisfiable);return
  }
  reader,err:=src.Open(q.Context(),from)
  if err!=nil{http.Error(w,"source unavailable",http.StatusServiceUnavailable);return}
  defer reader.Close()
  w.Header().Set("Content-Type","application/octet-stream")
  w.Header().Set("Accept-Ranges","bytes")
  w.Header().Set("Content-Length",strconv.FormatInt(to-from+1,10))
  if status==http.StatusPartialContent{
   w.Header().Set("Content-Range",fmt.Sprintf("bytes %d-%d/%d",from,to,src.Size))
  }
  w.WriteHeader(status)
  if q.Method=="HEAD" {return}
  _,_ = io.CopyN(w,reader,to-from+1)
 })}
 serveDone:=make(chan struct{})
 go func(){_ = server.Serve(ln);close(serveDone)}()
 defer func(){_ = server.Close();<-serveDone}()
 // Explicit demuxer prevents playlist/script inputs from making FFmpeg open
 // untrusted URLs. FFmpeg may read only the internal HTTP source, not TV paths.
 args:=[]string{"-hide_banner","-nostdin","-loglevel","error",
  "-probesize","4194304","-analyzeduration","5000000",
  "-protocol_whitelist","http,tcp","-f",p.InputDemuxer(),
  "-i","http://"+ln.Addr().String()+route,
 }
 if p.VideoIndex>=0 {args=append(args,"-map",fmt.Sprintf("0:%d",p.VideoIndex),"-c:v","copy")}
 args=append(args,"-map",fmt.Sprintf("0:%d",p.AudioIndex))
 if p.CopyAudio {args=append(args,"-c:a","copy")} else {
  args=append(args,"-c:a","aac","-b:a","160k","-ac","2","-threads","1")
 }
 args=append(args,"-sn","-dn","-map_metadata","-1",
  "-movflags","+empty_moov+frag_keyframe+default_base_moof",
  "-frag_duration","1000000","-f","mp4","pipe:1")
 cmd:=exec.CommandContext(ctx,r.Binary,args...)
 cmd.Stdout=out
 var stderr cappedLog
 cmd.Stderr=&stderr
 if err=cmd.Run();err!=nil{
  if ctx.Err()!=nil {return ctx.Err()}
  return fmt.Errorf("FFMPEG_REMUX_FAILED: %w: %s",err,stderr.String())
 }
 return nil
}

type cappedLog struct{ bytes.Buffer }
func (b *cappedLog) Write(data []byte)(int,error) {
 if b.Len()<4096 {_,_ = b.Buffer.Write(data[:min(len(data),4096-b.Len())])}
 return len(data),nil
}

func inputRange(value string,size int64)(start,end int64,status int,ok bool) {
 if size<=0{return}
 if value==""{return 0,size-1,http.StatusOK,true}
 if !strings.HasPrefix(value,"bytes=")||strings.Contains(value,","){return}
 parts:=strings.Split(strings.TrimPrefix(value,"bytes="),"-")
 if len(parts)!=2{return}
 if parts[0]=="" {
  n,err:=strconv.ParseInt(parts[1],10,64)
  if err!=nil||n<=0{return}
  if n>size {n=size}
  return size-n,size-1,http.StatusPartialContent,true
 }
 start,err:=strconv.ParseInt(parts[0],10,64)
 if err!=nil||start<0||start>=size{return}
 end=size-1
 if parts[1]!=""{
  n,e:=strconv.ParseInt(parts[1],10,64)
  if e!=nil||n<start{return 0,0,0,false}
  if n<end {end=n}
 }
 return start,end,http.StatusPartialContent,true
}
