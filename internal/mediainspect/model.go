// Package mediainspect implements bounded, advisory media inspection and TV
// capability matching. It never grants file access or changes playback state.
package mediainspect

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

type Track struct {
	Index int `json:"index"`
	Kind string `json:"kind"`
	Codec string `json:"codec"`
	Profile string `json:"profile,omitempty"`
	Level int `json:"level,omitempty"`
	Width int `json:"width,omitempty"`
	Height int `json:"height,omitempty"`
	PixelFormat string `json:"pixel_format,omitempty"`
	Channels int `json:"channels,omitempty"`
	SampleRate int `json:"sample_rate,omitempty"`
	BitRate int64 `json:"bit_rate,omitempty"`
	TimeBase string `json:"time_base,omitempty"`
	FrameRate string `json:"frame_rate,omitempty"`
	Language string `json:"language,omitempty"`
	Title string `json:"title,omitempty"`
	Default bool `json:"default,omitempty"`
}
type Media struct {
	Container string `json:"container"`
	DurationMS int64 `json:"duration_ms,omitempty"`
	BitRate int64 `json:"bit_rate,omitempty"`
	Tracks []Track `json:"tracks"`
}
type FormatSupport struct {
	Container string `json:"container"`
	VideoCodec string `json:"video_codec,omitempty"`
	AudioCodec string `json:"audio_codec,omitempty"`
	VideoProfiles []string `json:"video_profiles,omitempty"`
	MaxLevel int `json:"max_level,omitempty"`
	MaxWidth int `json:"max_width,omitempty"`
	MaxHeight int `json:"max_height,omitempty"`
	Certainty string `json:"certainty"` // "probably", "maybe", "unsupported"
}
type TVCapabilities struct {
	Version int `json:"version"`
	Source string `json:"source"`
	Formats []FormatSupport `json:"formats"`
	PreferredAudioLanguage string `json:"preferred_audio_language,omitempty"`
	RequestedAudioIndex *int `json:"requested_audio_index,omitempty"`
}
type Decision struct {
	Mode string `json:"mode"` // DIRECT, ADAPTIVE, UNKNOWN (not an FFmpeg execution promise)
	Reason string `json:"reason"`
	AudioStreamIndex *int `json:"audio_stream_index,omitempty"`
}
type Report struct {
	Status string `json:"status"` // READY, UNAVAILABLE
	Media *Media `json:"media,omitempty"`
	Decision Decision `json:"decision"`
	ErrorCode string `json:"error_code,omitempty"`
}

type rawProbe struct {
	Format struct {
		Name string `json:"format_name"`
		Duration string `json:"duration"`
		BitRate string `json:"bit_rate"`
	} `json:"format"`
	Streams []struct {
		Index int `json:"index"`
		Kind string `json:"codec_type"`
		Codec string `json:"codec_name"`
		Profile string `json:"profile"`
		Level int `json:"level"`
		Width int `json:"width"`
		Height int `json:"height"`
		PixelFormat string `json:"pix_fmt"`
		Channels int `json:"channels"`
		SampleRate string `json:"sample_rate"`
		BitRate string `json:"bit_rate"`
		TimeBase string `json:"time_base"`
		FrameRate string `json:"avg_frame_rate"`
		Tags struct {
			Language string `json:"language"`
			Title string `json:"title"`
		} `json:"tags"`
		Disposition struct {
			Default int `json:"default"`
		} `json:"disposition"`
	} `json:"streams"`
}
func positiveInt(raw string) int64 {
	if raw == "" { return 0 }
	n,err:=strconv.ParseInt(raw,10,64)
	if err!=nil||n<=0 {return 0}
	return n
}
func containerName(format,hint string) string {
	all:=strings.Split(strings.ToLower(format),",")
	has:=func(target string)bool {for _,x:=range all {if x==target{return true}};return false}
	switch {
	case has("webm") && strings.Contains(hint,"webm"):return "webm"
	case has("matroska"):return "matroska"
	case has("mp4"):return "mp4"
	case has("mp3"):return "mp3"
	case has("flac"):return "flac"
	case has("ogg"):return "ogg"
	case has("wav"):return "wav"
	case len(all)==1:return all[0]
	default:return ""
	}
}
func Parse(raw []byte,contentType string)(Media,error) {
	if len(raw)==0||len(raw)>256<<10{return Media{},errors.New("INVALID_PROBE_OUTPUT_SIZE")}
	var v rawProbe
	// Unmarshal ignores unrelated ffprobe fields: no full packets or private
	// tags are exported to TV, and the input JSON is size capped.
	if err:=json.Unmarshal(raw,&v);err!=nil{return Media{},fmt.Errorf("INVALID_PROBE_JSON: %w",err)}
	if len(v.Streams)==0||len(v.Streams)>32{return Media{},errors.New("INVALID_STREAM_COUNT")}
	out:=Media{Container:containerName(v.Format.Name,contentType),BitRate:positiveInt(v.Format.BitRate),
		Tracks:make([]Track,0,len(v.Streams))}
	if out.Container=="" {return Media{},errors.New("UNKNOWN_CONTAINER")}
	if seconds,err:=strconv.ParseFloat(v.Format.Duration,64);err==nil&&seconds>0&&
		seconds<float64(math.MaxInt64/1000)&&!math.IsNaN(seconds)&&!math.IsInf(seconds,0) {
		out.DurationMS=int64(seconds*1000)
	}
	indexes:=make(map[int]struct{},len(v.Streams))
	for _,p:=range v.Streams {
		if p.Index<0||p.Index>255 {return Media{},errors.New("INVALID_STREAM_INDEX")}
		if _,ok:=indexes[p.Index];ok{return Media{},errors.New("DUPLICATE_STREAM_INDEX")}
		indexes[p.Index]=struct{}{}
		if p.Kind!="video"&&p.Kind!="audio"&&p.Kind!="subtitle" {continue}
		if p.Width<0||p.Height<0||p.Width>32768||p.Height>32768||
			p.Channels<0||p.Channels>128||p.Level<0 {return Media{},errors.New("INVALID_STREAM_METADATA")}
		r:=Track{Index:p.Index,Kind:p.Kind,Codec:strings.ToLower(p.Codec),
			Profile:p.Profile,Level:p.Level,Width:p.Width,Height:p.Height,
			PixelFormat:p.PixelFormat,Channels:p.Channels,
			SampleRate:int(positiveInt(p.SampleRate)),BitRate:positiveInt(p.BitRate),
			TimeBase:p.TimeBase,FrameRate:p.FrameRate,
			Language:strings.ToLower(p.Tags.Language),Title:p.Tags.Title,
			Default:p.Disposition.Default==1}
		if len(r.Title)>256{r.Title=r.Title[:256]}
		out.Tracks=append(out.Tracks,r)
	}
	if len(out.Tracks)==0{return Media{},errors.New("NO_PLAYABLE_TRACKS")}
	return out,nil
}

// Decide is conservative: a missing/ambiguous WebView decoder report never
// becomes DIRECT. ADAPTIVE means conversion is indicated, not implemented.
func Decide(m Media,c TVCapabilities) Decision {
	result:=Decision{Mode:"UNKNOWN",Reason:"TV_CAPABILITIES_NOT_CONFIRMED"}
	if c.Version!=1||len(c.Formats)==0||len(c.Formats)>64{return result}
	var video *Track
	var audios []*Track
	for i:=range m.Tracks {
		t:=&m.Tracks[i]
		if t.Kind=="video"&&video==nil{video=t}
		if t.Kind=="audio"{audios=append(audios,t)}
	}
	if video==nil&&len(audios)==0 {result.Reason="NO_AV_TRACKS";return result}
	var audio *Track
	if c.RequestedAudioIndex!=nil {
		for _,a:=range audios {if a.Index==*c.RequestedAudioIndex{audio=a}}
		if audio==nil {result.Reason="AUDIO_TRACK_NOT_FOUND";return result}
	} else {
		lang:=strings.ToLower(c.PreferredAudioLanguage)
		if lang!="" {for _,a:=range audios {if a.Language==lang {audio=a;break}}}
		if audio==nil {for _,a:=range audios{if a.Default{audio=a;break}}}
		if audio==nil&&len(audios)>0{audio=audios[0]}
	}
	if audio!=nil {idx:=audio.Index;result.AudioStreamIndex=&idx}
	if video!=nil&&audio==nil&&len(audios)>0{result.Reason="AUDIO_TRACK_UNAVAILABLE";return result}
	if (c.RequestedAudioIndex!=nil||c.PreferredAudioLanguage!="")&&audio!=nil&&!audio.Default&&len(audios)>1 {
		result.Mode="ADAPTIVE";result.Reason="AUDIO_TRACK_SELECTION_REQUIRES_REMUX";return result
	}
	if video!=nil&&video.Codec==""||audio!=nil&&audio.Codec=="" {
		result.Reason="UNKNOWN_CODEC";return result
	}
	var uncertain bool
	var explicitNo bool
	for _,f:=range c.Formats {
		if f.Container!=m.Container {continue}
		if video!=nil&&f.VideoCodec!=video.Codec {continue}
		if video==nil&&f.VideoCodec!="" {continue}
		if audio!=nil&&f.AudioCodec!=audio.Codec {continue}
		if audio==nil&&f.AudioCodec!="" {continue}
		if f.Certainty=="unsupported" {explicitNo=true;continue}
		if f.Certainty!="probably" {uncertain=true;continue}
		if video!=nil {
			if f.MaxWidth<=0||f.MaxHeight<=0||f.MaxLevel<=0||len(f.VideoProfiles)==0||
				video.Level==0||video.Profile=="" {
				uncertain=true;continue
			}
			if video.Width>f.MaxWidth||video.Height>f.MaxHeight||video.Level>f.MaxLevel {
				explicitNo=true;continue
			}
			match:=false
			for _,p:=range f.VideoProfiles {if strings.EqualFold(p,video.Profile){match=true}}
			if !match {explicitNo=true;continue}
		}
		result.Mode="DIRECT";result.Reason="DECLARED_DECODER_COMPATIBLE";return result
	}
	if explicitNo&&!uncertain {
		result.Mode="ADAPTIVE";result.Reason="DECLARED_FORMAT_UNSUPPORTED"
	} else if uncertain {result.Reason="DECODER_SUPPORT_UNCERTAIN"} else {result.Reason="NO_MATCHING_FORMAT_CAPABILITY"}
	return result
}
