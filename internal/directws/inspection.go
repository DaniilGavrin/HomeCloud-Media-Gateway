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
}
func (s *inspectionState) Save(l Lease,r mediainspect.Report){
	s.mu.Lock()
	s.lease=l
	s.report=r
	s.ready=true
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
