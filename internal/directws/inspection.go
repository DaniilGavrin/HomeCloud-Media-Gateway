package directws

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/mediainspect"
	"github.com/coder/websocket"
)

// inspectionState is scoped to one trusted direct WS, not a global media cache.
// It prevents old async probes and TV capability messages from crossing epochs.
type inspectionState struct {
	mu sync.Mutex
	lease Lease
	report mediainspect.Report
	ready bool
	initialCaps mediainspect.TVCapabilities
}
func (s *inspectionState) Save(l Lease,r mediainspect.Report,caps map[string]any){
	s.mu.Lock()
	s.lease=l
	s.report=r
	s.ready=true
	// Output MP4 format support is independently reported by WebView at
	// handshake time; an exact SOURCE report may say "unsupported".
	raw,_:=json.Marshal(caps["decoder"])
	var initial mediainspect.TVCapabilities
	if len(raw)<=8192&&json.Unmarshal(raw,&initial)==nil{s.initialCaps=initial}
	s.mu.Unlock()
}
func (s *inspectionState) Decide(l Lease,c mediainspect.TVCapabilities)(mediainspect.Report,bool){
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.ready||s.lease.RoomID!=l.RoomID||
		s.lease.PlaybackID!=l.PlaybackID||s.lease.Generation!=l.Generation||
		s.lease.Epoch!=l.Epoch||s.report.Media==nil {
		return mediainspect.Report{},false
	}
	if c.Version!=1||len(c.Formats)==0||len(c.Formats)>64{
		return mediainspect.Report{},false
	}
	next:=s.report
	next.Decision=mediainspect.Decide(*next.Media,c)
	s.report=next
	return next,true
}

// Selected verifies the track belongs to the inspected active source and
// returns the TV's declared OUTPUT capability matrix without guessing support.
func (s *inspectionState) Selected(l Lease,index int)(mediainspect.Media,mediainspect.TVCapabilities,bool){
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.ready||s.report.Status!="READY"||s.report.Media==nil||
		s.lease.RoomID!=l.RoomID||s.lease.PlaybackID!=l.PlaybackID||
		s.lease.Generation!=l.Generation||s.lease.Epoch!=l.Epoch{
		return mediainspect.Media{},mediainspect.TVCapabilities{},false
	}
	for _,track:=range s.report.Media.Tracks{
		if track.Kind=="audio"&&track.Index==index{
			return *s.report.Media,s.initialCaps,true
		}
	}
	return mediainspect.Media{},mediainspect.TVCapabilities{},false
}

func (h *Handler) publishConversionError(ctx context.Context,conn *websocket.Conn,l Lease,code string)error{
	raw,_:=json.Marshal(map[string]any{
		"v":1,"type":"media.convert.error","room_id":l.RoomID,
		"playback_id":l.PlaybackID,"generation":l.Generation,
		"connection_epoch":l.Epoch,"code":code,
	})
	writeCtx,done:=context.WithTimeout(ctx,3*time.Second)
	defer done()
	return conn.Write(writeCtx,websocket.MessageText,raw)
}

func (h *Handler) handleAudioSelect(ctx context.Context,conn *websocket.Conn,l Lease,
	msg Hello,state *inspectionState,peer string)bool{
	if h.Book==nil||!h.Book.Valid(l.RoomID,l)||
		msg.V!=1||msg.Type!="media.audio.select"||
		msg.RoomID!=l.RoomID||msg.PlaybackID!=l.PlaybackID||
		msg.Generation!=l.Generation||msg.DeviceID!=l.DeviceID||
		msg.AudioIndex==nil||*msg.AudioIndex<0||*msg.AudioIndex>255||
		h.OfferConversion==nil{return false}
	media,_,ok:=state.Selected(l,*msg.AudioIndex)
	if !ok{return false}
	// Convert the OUTPUT codec combination, not the original source.
	// The output capability is independently measured by TV MediaCapabilities.
	rawCaps,marshalErr:=json.Marshal(msg.Capabilities["decoder"])
	if marshalErr!=nil||len(rawCaps)>8192{return false}
	var caps mediainspect.TVCapabilities
	if json.Unmarshal(rawCaps,&caps)!=nil||caps.Source!="media_capabilities_api" {return false}
	offer,err:=h.OfferConversion(l,peer,media,caps,*msg.AudioIndex)
	if err!=nil{return false}
	raw,_:=json.Marshal(map[string]any{
		"v":1,"type":"media.convert.ready","room_id":l.RoomID,
		"playback_id":l.PlaybackID,"generation":l.Generation,
		"connection_epoch":l.Epoch,"http_media_path":offer.Path,
		"content_type":offer.ContentType,"media_kind":offer.MediaKind,
		"audio_index":*msg.AudioIndex,"seek_supported":false,
	})
	writeCtx,done:=context.WithTimeout(ctx,3*time.Second)
	defer done()
	return conn.Write(writeCtx,websocket.MessageText,raw)==nil
}

func (h *Handler) publishInspection(ctx context.Context,conn *websocket.Conn,l Lease,r mediainspect.Report){
	raw,err:=json.Marshal(map[string]any{
		"v":1,"type":"media.inspection","room_id":l.RoomID,
		"playback_id":l.PlaybackID,"generation":l.Generation,
		"connection_epoch":l.Epoch,"report":r,
	})
	if err!=nil{return}
	sendCtx,cancel:=context.WithTimeout(ctx,3*time.Second)
	defer cancel()
	_ = conn.Write(sendCtx,websocket.MessageText,raw)
}

func (h *Handler) handleDecoderReport(ctx context.Context,conn *websocket.Conn,l Lease,
	msg Hello,state *inspectionState) bool {
	if h.Book==nil||!h.Book.Valid(l.RoomID,l)||
		msg.V!=1||msg.Type!="media.decoder.report"||
		msg.RoomID!=l.RoomID||msg.PlaybackID!=l.PlaybackID||
		msg.Generation!=l.Generation||msg.DeviceID!=l.DeviceID{
		return false
	}
	raw,err:=json.Marshal(msg.Capabilities["decoder"])
	if err!=nil||len(raw)>8192{return false}
	var caps mediainspect.TVCapabilities
	if err=json.Unmarshal(raw,&caps);err!=nil{return false}
	report,ok:=state.Decide(l,caps)
	if !ok{return false}
	h.publishInspection(ctx,conn,l,report)
	return true
}
