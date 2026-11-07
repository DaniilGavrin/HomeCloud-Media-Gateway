// Package remux provides bounded, lease-cancellable on-the-fly fragmented MP4.
// It never accepts a file path or URL from TV. Inputs come from the already
// authorized HC SS Source Plan and a random loopback-only Range facade.
package remux

import (
 "errors"
 "fmt"
 "strings"

 "github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/mediainspect"
)

var (
 ErrUnsupported = errors.New("REMUX_UNSUPPORTED")
 ErrInvalid = errors.New("REMUX_INVALID_PROFILE")
)

// Profile is derived from ffprobe and TV format capabilities, never client
// supplied FFmpeg flags. Video is always copied, never transcoded here.
type Profile struct {
 SourceFormat string
 VideoIndex int // -1 for audio-only
 AudioIndex int
 CopyAudio bool
 MediaKind string
}

func (p Profile) Validate() error {
 switch p.SourceFormat {
 case "matroska","webm","mp4","mov","wav","flac","mp3","ogg":
 default:return fmt.Errorf("%w: input container",ErrInvalid)
 }
 if p.VideoIndex < -1 || p.VideoIndex > 255 || p.AudioIndex < 0 || p.AudioIndex > 255 ||
    p.AudioIndex == p.VideoIndex {
  return fmt.Errorf("%w: stream indexes",ErrInvalid)
 }
 if p.MediaKind!="video"&&p.MediaKind!="audio" {return fmt.Errorf("%w: media kind",ErrInvalid)}
 if (p.VideoIndex<0)!=(p.MediaKind=="audio") {return fmt.Errorf("%w: inconsistent video track",ErrInvalid)}
 return nil
}

func (p Profile) InputDemuxer() string {
 switch p.SourceFormat {
 case "mp4","mov":return "mov"
 case "webm":return "matroska"
 default:return p.SourceFormat
 }
}

// Select returns a conservative conversion plan only when TV explicitly
// reported "probably" for the OUTPUT MP4 video+AAC combination. Unknown
// video codec support is never silently promoted to a conversion offer.
func Select(media mediainspect.Media, caps mediainspect.TVCapabilities, audioIndex int) (Profile,error) {
 if caps.Version!=1 || audioIndex<0 || audioIndex>255 || len(caps.Formats)==0 || len(caps.Formats)>64 {
  return Profile{},ErrUnsupported
 }
 p:=Profile{SourceFormat:media.Container,VideoIndex:-1,AudioIndex:audioIndex}
 var video, audio *mediainspect.Track
 for i:=range media.Tracks {
  t:=&media.Tracks[i]
  if t.Kind=="video"&&video==nil {video=t}
  if t.Kind=="audio"&&t.Index==audioIndex {audio=t}
 }
 if audio==nil || audio.Codec=="" {return Profile{},ErrUnsupported}
 if video!=nil {
  if video.Codec!="h264" && video.Codec!="hevc" {return Profile{},ErrUnsupported}
  if video.Profile=="" || video.Level==0 || video.Width==0 || video.Height==0 {return Profile{},ErrUnsupported}
  p.VideoIndex=video.Index
  p.MediaKind="video"
 } else {
  p.MediaKind="audio"
 }
 p.CopyAudio=audio.Codec=="aac"
 if err:=p.Validate();err!=nil{return Profile{},ErrUnsupported}
 for _,f:=range caps.Formats {
  if f.Container!="mp4" || f.AudioCodec!="aac" || f.Certainty!="probably" {continue}
  if video==nil {
   if f.VideoCodec=="" {return p,nil}
   continue
  }
  if f.VideoCodec!=video.Codec || f.MaxLevel<video.Level ||
     f.MaxWidth<video.Width || f.MaxHeight<video.Height {continue}
  for _,profile:=range f.VideoProfiles {
   if strings.EqualFold(profile,video.Profile) {return p,nil}
  }
 }
 return Profile{},ErrUnsupported
}
