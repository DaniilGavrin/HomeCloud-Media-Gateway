package mediainspect

import (
	"encoding/json"
	"strings"
	"testing"
)

const sample = `{
  "streams":[
    {"index":0,"codec_type":"video","codec_name":"h264","profile":"High",
     "level":40,"width":1920,"height":1080,"pix_fmt":"yuv420p",
     "avg_frame_rate":"24000/1001","time_base":"1/24000"},
    {"index":1,"codec_type":"audio","codec_name":"aac","channels":2,
     "sample_rate":"48000","tags":{"language":"eng"},"disposition":{"default":1}},
    {"index":2,"codec_type":"audio","codec_name":"dts","channels":6,
     "sample_rate":"48000","tags":{"language":"rus"}}
  ],
  "format":{"format_name":"mov,mp4,m4a,3gp,3g2,mj2","duration":"124.750",
  "bit_rate":"2000000"}}`
func example(t *testing.T) Media {
	t.Helper()
	m,err:=Parse([]byte(sample),"video/mp4")
	if err!=nil{t.Fatal(err)}
	return m
}
func TestKAN61ParseFFprobe(t *testing.T){
	m:=example(t)
	if m.Container!="mp4"||m.DurationMS!=124750||m.BitRate!=2000000||len(m.Tracks)!=3{
		t.Fatalf("unexpected media: %+v",m)
	}
	if m.Tracks[0].Profile!="High"||m.Tracks[0].Level!=40||
		m.Tracks[1].SampleRate!=48000||!m.Tracks[1].Default||
		m.Tracks[2].Language!="rus"{t.Fatalf("bad tracks %+v",m.Tracks)}
	for _,bad:=range []string{"{}",`{"streams":[]}`,`{"streams":[{"index":0,"codec_type":"video","width":999999}],"format":{"format_name":"mp4"}}`} {
		if _,err:=Parse([]byte(bad),"video/mp4");err==nil {t.Fatalf("accepted %q",bad)}
	}
	if _,err:=Parse([]byte(strings.Repeat("x",256<<10+1)),"video/mp4");err==nil{
		t.Fatal("unbounded ffprobe output accepted")
	}
	var decoded map[string]any
	if err:=json.Unmarshal([]byte(sample),&decoded);err!=nil{t.Fatal(err)}
}
func TestKAN61CapabilityDecisions(t *testing.T){
	m:=example(t)
	caps:=TVCapabilities{Version:1,Source:"declared-tv",
		Formats:[]FormatSupport{{Container:"mp4",VideoCodec:"h264",AudioCodec:"aac",
			VideoProfiles:[]string{"High"},MaxLevel:41,MaxWidth:1920,MaxHeight:1080,Certainty:"probably"}}}
	got:=Decide(m,caps)
	if got.Mode!="DIRECT"||got.AudioStreamIndex==nil||*got.AudioStreamIndex!=1{
		t.Fatalf("H264/AAC should be direct relative to declared decoder: %+v",got)
	}
	caps.Formats[0].MaxWidth=1280
	got=Decide(m,caps)
	if got.Mode!="ADAPTIVE"{t.Fatalf("resolution too large: %+v",got)}
	caps.Formats[0].MaxWidth=1920
	caps.Formats[0].MaxLevel=31
	if got=Decide(m,caps);got.Mode!="ADAPTIVE"{t.Fatalf("H264 level too high: %+v",got)}
	caps.Formats[0].MaxLevel=41
	caps.Formats[0].VideoProfiles=nil
	if got=Decide(m,caps);got.Mode!="UNKNOWN"{t.Fatalf("unknown profile support accepted: %+v",got)}
	caps.Formats[0].VideoProfiles=[]string{"High"}
	caps.RequestedAudioIndex=new(int);*caps.RequestedAudioIndex=2
	if got=Decide(m,caps);got.Mode!="ADAPTIVE"||got.Reason!="AUDIO_TRACK_SELECTION_REQUIRES_REMUX"{
		t.Fatalf("alternate DTS track must require adaptation: %+v",got)
	}
	caps.RequestedAudioIndex=nil
	caps.PreferredAudioLanguage="rus"
	if got=Decide(m,caps);got.Mode!="ADAPTIVE"{t.Fatalf("language selection not respected: %+v",got)}
	caps.PreferredAudioLanguage=""
	hevc:=m
	hevc.Tracks=append([]Track(nil),m.Tracks...)
	hevc.Tracks[0].Codec="hevc"
	hevc.Tracks[1].Codec="dts"
	caps.Formats=[]FormatSupport{{Container:"mp4",VideoCodec:"hevc",AudioCodec:"dts",Certainty:"unsupported"}}
	if got=Decide(hevc,caps);got.Mode!="ADAPTIVE"{t.Fatalf("HEVC/DTS explicit unsupported: %+v",got)}
	caps.Formats=nil
	if got=Decide(hevc,caps);got.Mode!="UNKNOWN"{t.Fatalf("missing TV report must not imply support: %+v",got)}
}

func TestKAN61AudioOnly(t *testing.T){
	m:=Media{Container:"mp3",Tracks:[]Track{{Index:0,Kind:"audio",Codec:"mp3",Default:true}}}
	c:=TVCapabilities{Version:1,Formats:[]FormatSupport{{Container:"mp3",AudioCodec:"mp3",Certainty:"probably"}}}
	if got:=Decide(m,c);got.Mode!="DIRECT"||got.AudioStreamIndex==nil||*got.AudioStreamIndex!=0 {t.Fatal(got)}
	c.Formats[0].Certainty="maybe"
	if got:=Decide(m,c);got.Mode!="UNKNOWN" {t.Fatalf("maybe is not direct: %+v",got)}
}
