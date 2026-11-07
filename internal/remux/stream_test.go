package remux

import (
 "bytes"
 "context"
 "errors"
 "io"
 "os"
 "os/exec"
 "path/filepath"
 "testing"
 "time"

 "github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/mediainspect"
)

func TestSelectKeepsVideoAndSelectsAudio(t *testing.T) {
 media:=mediainspect.Media{Container:"matroska",Tracks:[]mediainspect.Track{
  {Index:0,Kind:"video",Codec:"h264",Profile:"High",Level:40,Width:1920,Height:1080},
  {Index:1,Kind:"audio",Codec:"dts",Default:true},
  {Index:2,Kind:"audio",Codec:"aac",Language:"rus"},
 }}
 caps:=mediainspect.TVCapabilities{Version:1,Formats:[]mediainspect.FormatSupport{
  {Container:"mp4",VideoCodec:"h264",AudioCodec:"aac",VideoProfiles:[]string{"High"},
   MaxLevel:40,MaxWidth:1920,MaxHeight:1080,Certainty:"probably"},
 }}
 dts,err:=Select(media,caps,1)
 if err!=nil || dts.VideoIndex!=0 || dts.AudioIndex!=1 || dts.CopyAudio {
  t.Fatalf("unexpected selective audio transcode: %+v, %v",dts,err)
 }
 aac,err:=Select(media,caps,2)
 if err!=nil||!aac.CopyAudio||aac.AudioIndex!=2||aac.InputDemuxer()!="matroska"{
  t.Fatalf("unexpected copy remux: %+v, %v",aac,err)
 }
 for _,wrong:=range []int{-1,3,255} {
  if _,err:=Select(media,caps,wrong);!errors.Is(err,ErrUnsupported){t.Fatalf("accepted audio %d",wrong)}
 }
 caps.Formats[0].Certainty="maybe"
 if _,err:=Select(media,caps,1);!errors.Is(err,ErrUnsupported){t.Fatal("accepted uncertain output capability")}
 media.Tracks[0].Codec="av1"
 caps.Formats[0].Certainty="probably"
 if _,err:=Select(media,caps,1);!errors.Is(err,ErrUnsupported){t.Fatal("transcoded incompatible video silently")}
}

func TestRangeSourceValidation(t *testing.T){
 for _,c:=range []struct{rangeHeader string;size,start,end int64;ok bool}{
  {"",100,0,99,true},{"bytes=0-9",100,0,9,true},
  {"bytes=-10",100,90,99,true},{"bytes=50-",100,50,99,true},
  {"bytes=200-",100,0,0,false},{"bytes=2-1",100,0,0,false},
  {"bytes=0-2,9-12",100,0,0,false},
 } {
  start,end,_,ok:=inputRange(c.rangeHeader,c.size)
  if start!=c.start||end!=c.end||ok!=c.ok{t.Fatalf("range %q => %d..%d %t",c.rangeHeader,start,end,ok)}
 }
}

func TestRealFFmpegStreamingMP4(t *testing.T){
 binary,err:=exec.LookPath("ffmpeg")
 if err!=nil{t.Skip("ffmpeg unavailable")}
 dir:=t.TempDir()
 input:=filepath.Join(dir,"audio.wav")
 generate:=exec.Command(binary,"-hide_banner","-loglevel","error",
  "-f","lavfi","-i","sine=frequency=440:duration=0.5",
  "-c:a","pcm_s16le","-y",input)
 if out,err:=generate.CombinedOutput();err!=nil{t.Fatalf("generate WAV: %v %s",err,out)}
 bytesIn,err:=os.ReadFile(input)
 if err!=nil{t.Fatal(err)}
 runner,err:=New(binary,1)
 if err!=nil{t.Fatal(err)}
 cap:=mediainspect.TVCapabilities{Version:1,Formats:[]mediainspect.FormatSupport{
  {Container:"mp4",AudioCodec:"aac",Certainty:"probably"},
 }}
 profile,err:=Select(mediainspect.Media{Container:"wav",Tracks:[]mediainspect.Track{
  {Index:0,Kind:"audio",Codec:"pcm_s16le"},
 }},cap,0)
 if err!=nil{t.Fatal(err)}
 ctx,cancel:=context.WithTimeout(context.Background(),12*time.Second)
 defer cancel()
 var output bytes.Buffer
 open:=func(ctx context.Context,offset int64)(io.ReadCloser,error){
  if offset<0||offset>int64(len(bytesIn)){return nil,ErrSource}
  return io.NopCloser(bytes.NewReader(bytesIn[offset:])),nil
 }
 if err:=runner.Run(ctx,Input{Size:int64(len(bytesIn)),Open:open},profile,&output);err!=nil{
  t.Fatalf("stream FFmpeg: %v",err)
 }
 data:=output.Bytes()
 if len(data)<256||!bytes.Contains(data,[]byte("ftyp"))||!bytes.Contains(data,[]byte("moof")){
  t.Fatalf("not a fragmented MP4: %d bytes",len(data))
 }
}

func TestCancelBeforeStart(t *testing.T){
 runner:=&Runner{Binary:"ffmpeg",slots:make(chan struct{},1)}
 runner.slots<-struct{}{}
 ctx,cancel:=context.WithCancel(context.Background())
 cancel()
 err:=runner.Run(ctx,Input{Size:1,Open:func(context.Context,int64)(io.ReadCloser,error){
  return io.NopCloser(bytes.NewReader([]byte{1})),nil
 }},Profile{SourceFormat:"wav",VideoIndex:-1,AudioIndex:0,MediaKind:"audio"},io.Discard)
 if !errors.Is(err,context.Canceled){t.Fatalf("unexpected cancel: %v",err)}
}
