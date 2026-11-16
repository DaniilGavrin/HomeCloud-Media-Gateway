package remux

import (
 "bytes"
 "context"
 "errors"
 "encoding/json"
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
  {Index:2,Kind:"audio",Codec:"aac",Language:"rus",SampleRate:48000,Channels:2,BitRate:128000},
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


func TestRealFFmpegCopiesH264VideoAndSelectsSecondAudioTrack(t *testing.T){
 ffmpeg,err:=exec.LookPath("ffmpeg")
 if err!=nil{t.Skip("ffmpeg unavailable")}
 ffprobe,err:=exec.LookPath("ffprobe")
 if err!=nil{t.Skip("ffprobe unavailable")}
 // Some local CI mirrors use FFmpeg without libx264. The production code
 // still supports H264 copy without requiring an encoder at runtime.
 encoders,err:=exec.Command(ffmpeg,"-hide_banner","-encoders").CombinedOutput()
 if err!=nil||!bytes.Contains(encoders,[]byte("libx264")){t.Skip("libx264 fixture encoder unavailable")}
 dir:=t.TempDir()
 input:=filepath.Join(dir,"two-audio-tracks.mkv")
 create:=exec.Command(ffmpeg,"-hide_banner","-loglevel","error",
  "-f","lavfi","-i","testsrc=size=160x90:rate=15:duration=0.8",
  "-f","lavfi","-i","sine=frequency=440:duration=0.8",
  "-f","lavfi","-i","sine=frequency=880:duration=0.8",
  "-map","0:v","-map","1:a","-map","2:a",
  "-c:v","libx264","-pix_fmt","yuv420p","-preset","ultrafast",
  "-c:a:0","aac","-c:a:1","ac3","-shortest","-f","matroska",input)
 if output,err:=create.CombinedOutput();err!=nil{t.Fatalf("create fixture: %v: %s",err,output)}
 raw,err:=os.ReadFile(input);if err!=nil{t.Fatal(err)}
 // Audio index 2 is deliberately AC3: video stays H.264 (stream copy),
 // while the selected incompatible soundtrack must become AAC.
 profile:=Profile{SourceFormat:"matroska",VideoIndex:0,AudioIndex:2,CopyAudio:false,MediaKind:"video"}
 runner,err:=New(ffmpeg,1);if err!=nil{t.Fatal(err)}
 ctx,cancel:=context.WithTimeout(context.Background(),15*time.Second)
 defer cancel()
 var converted bytes.Buffer
 err=runner.Run(ctx,Input{Size:int64(len(raw)),Open:func(_ context.Context,offset int64)(io.ReadCloser,error){
  return io.NopCloser(bytes.NewReader(raw[offset:])),nil
 }},profile,&converted)
 if err!=nil{t.Fatalf("fragmented copy video: %v",err)}
 if !bytes.Contains(converted.Bytes(),[]byte("moof")){t.Fatal("no fragmented MP4 output")}
 result:=filepath.Join(dir,"converted.mp4")
 if err:=os.WriteFile(result,converted.Bytes(),0600);err!=nil{t.Fatal(err)}
 probe:=exec.Command(ffprobe,"-v","error","-show_entries","stream=codec_name,codec_type",
  "-of","json",result)
 output,err:=probe.Output();if err!=nil{t.Fatal(err)}
 var payload struct {Streams []struct {CodecName string `json:"codec_name"`; CodecType string `json:"codec_type"`} `json:"streams"`}
 if err:=json.Unmarshal(output,&payload);err!=nil{t.Fatal(err)}
 if len(payload.Streams)!=2 || payload.Streams[0].CodecName!="h264" ||
  payload.Streams[0].CodecType!="video" || payload.Streams[1].CodecName!="aac" ||
  payload.Streams[1].CodecType!="audio"{
  t.Fatalf("expected exactly copied h264 + selected AAC track, got %+v",payload.Streams)
 }
}


// The converted AAC format must exactly match the output codec declaration
// passed to WebView MediaCapabilities: 160k/48kHz/stereo when re-encoding.
func TestCopyAACOnlyWhenOutputParametersAreCompatible(t *testing.T){
 caps:=mediainspect.TVCapabilities{Version:1,Formats:[]mediainspect.FormatSupport{{
  Container:"mp4",AudioCodec:"aac",Certainty:"probably",
 }}}
 for _,tc:=range []struct{name string; rate,channels int;bitRate int64;copy bool}{
  {"matching",48000,2,128000,true},
  {"surround",48000,6,256000,false},
  {"other-sample-rate",44100,2,128000,false},
  {"unknown-bit-rate",48000,2,0,false},
  {"large-bitrate",48000,2,256000,false},
 } {
  t.Run(tc.name,func(t *testing.T){
   media:=mediainspect.Media{Container:"matroska",Tracks:[]mediainspect.Track{{
    Index:0,Kind:"audio",Codec:"aac",SampleRate:tc.rate,
    Channels:tc.channels,BitRate:tc.bitRate,
   }}}
   profile,err:=Select(media,caps,0)
   if err!=nil{t.Fatal(err)}
   if profile.CopyAudio!=tc.copy{t.Fatalf("unexpected copy for %s: %v",tc.name,profile.CopyAudio)}
  })
 }
}


func TestAdmissionRejectsOverCapacityImmediately(t *testing.T){
 runner:=&Runner{Binary:"unused",slots:make(chan struct{},1)}
 first,err:=runner.TryReserve()
 if err!=nil{t.Fatal(err)}
 start:=time.Now()
 if _,err=runner.TryReserve();!errors.Is(err,ErrBusy){t.Fatalf("expected REMUX_BUSY, got %v",err)}
 if time.Since(start)>time.Second{t.Fatal("second conversion blocked instead of failing")}
 first.Release()
 first.Release() // idempotent; must never deadlock
 second,err:=runner.TryReserve()
 if err!=nil{t.Fatalf("capacity not restored after release: %v",err)}
 second.Release()
}

func TestPermitRejectsReusedOrReleasedReservation(t *testing.T){
 runner:=&Runner{Binary:"unused",slots:make(chan struct{},1)}
 permit,err:=runner.TryReserve()
 if err!=nil{t.Fatal(err)}
 permit.Release()
 input:=Input{Size:1,Open:func(context.Context,int64)(io.ReadCloser,error){
  return io.NopCloser(bytes.NewReader([]byte{1})),nil
 }}
 profile:=Profile{SourceFormat:"wav",VideoIndex:-1,AudioIndex:0,MediaKind:"audio"}
 if err:=permit.Run(context.Background(),input,profile,io.Discard);!errors.Is(err,ErrPermit){
  t.Fatalf("released permit used again: %v",err)
 }
}
