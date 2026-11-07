package directws

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/mediaframe"
	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/mediainspect"
	"github.com/coder/websocket"
)

const HandshakeTimeout = 10 * time.Second

// StreamOpener is a trusted, in-process media source. It must authorize the
// given lease and return an independently closable reader.
type StreamOpener func(context.Context, Lease) (io.ReadCloser, error)

// HTTPMediaOffer is minted from an already-claimed room ticket. Its path
// identifies only a private, lease-bound Gateway HTTP Range session.
type HTTPMediaOffer struct {
	Path string
	ContentType string
	MediaKind string
}

type Handler struct {
	Book   *Book
	Log    *slog.Logger
	Report func(Update)
	// OpenStream is an INTERNAL source provider supplied by a future storage/codec
	// integration. Never accept arbitrary file paths or source URLs from TV.
	// A nil provider preserves the KAN-48 handshake-only behaviour.
	OpenStream   StreamOpener
	OpenStreamAt SeekStreamOpener
	Streams      *StreamControl
	// Optional direct-attach HTTP Range source used by WebView native codecs.
	// Called only after Book.Claim consumed a Backend-issued one-use ticket.
	OfferHTTP func(Lease,string) (HTTPMediaOffer,error)
	// Advisory-only: expensive ffprobe runs asynchronously after media.ready.
	// It never delays media delivery or authorizes an untrusted file reference.
	Inspect func(context.Context,Lease,map[string]any) mediainspect.Report
	// Converted media is offered only after room/track/output-capability
	// validation. Ordinary HTTP Range playback is never overridden.
	OfferConversion func(Lease,string,mediainspect.Media,mediainspect.TVCapabilities,int)(HTTPMediaOffer,error)
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// A Tauri Android WebView has a different Origin from this private Gateway.
	// The one-use cryptographic attach ticket, not the Origin header, proves
	// possession of an authorized room attachment grant.
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify:true})
	if err != nil {
		return
	}
	defer conn.CloseNow()
	conn.SetReadLimit(16 * 1024)
	readCtx, cancel := context.WithTimeout(r.Context(), HandshakeTimeout)
	typ, raw, err := conn.Read(readCtx)
	cancel()
	if err != nil {
		return
	}
	var hello Hello
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	err = dec.Decode(&hello)
	if err == nil {
		err = dec.Decode(new(any))
		if errors.Is(err, io.EOF) {
			err = nil
		}
	}
	if typ != websocket.MessageText || err != nil || hello.Type != "media.hello" || hello.V != 1 {
		code := "INVALID_REQUEST"
		if hello.V != 0 && hello.V != 1 {
			code = "UNSUPPORTED_VERSION"
		}
		reject(conn, code)
		return
	}
	if h.Book == nil {
		reject(conn, "ATTACH_REJECTED")
		return
	}
	lease, err := h.Book.Claim(hello)
	if err != nil {
		reject(conn, Code(err))
		return
	}
	notify := func(phase string) {
		if h.Report != nil {
			h.Report(Update{RoomID: lease.RoomID, PlaybackID: lease.PlaybackID, Generation: lease.Generation, Epoch: lease.Epoch, Phase: phase})
		}
	}
	// Forward attaching/ready to control independently of media availability.
	notify("ATTACHING")
	httpRangeLease := false
	ready := map[string]any{
		"v": 1, "type": "media.ready", "room_id": lease.RoomID, "playback_id": lease.PlaybackID,
		"generation": lease.Generation, "connection_epoch": lease.Epoch,
		"max_frame_payload_bytes": 65536, "window_bytes": 4194304,
	}
	if hello.Capabilities["http_range"] == true && h.OfferHTTP != nil {
		offer, err := h.OfferHTTP(lease, r.RemoteAddr)
		if err != nil {
			h.Book.Release(lease)
			reject(conn,"SOURCE_UNAVAILABLE")
			return
		}
		httpRangeLease = true
		ready["http_media_path"] = offer.Path
		ready["content_type"] = offer.ContentType
		ready["media_kind"] = offer.MediaKind
	}
	if h.OfferConversion!=nil&&hello.Capabilities["audio_selection"]==true {
		ready["audio_selection"]=true
	}
	reuseEnabled := httpRangeLease && hello.Capabilities["http_switch"] == true
	if reuseEnabled {
		ready["http_switch"] = true
	}
	// Register the HTTP control lease BEFORE advertising media.ready.
	// Otherwise the TV can issue pause/resume immediately after reading ready
	// and race this handler, getting a spurious NO_ACTIVE_STREAM.
	if httpRangeLease && h.Streams != nil {
		h.Streams.RegisterHTTP(lease)
		defer func() { h.Streams.Unregister(lease) }()
	}
	b, err := json.Marshal(ready)
	if err != nil {
		if h.Book.Release(lease) {
			notify("DISCONNECTED")
		}
		return
	}
	writeCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	err = conn.Write(writeCtx, websocket.MessageText, b)
	stop()
	if err != nil {
		if h.Book.Release(lease) {
			notify("DISCONNECTED")
		}
		return
	}
	notify("READY")
	inspection:=&inspectionState{}
	if httpRangeLease && hello.Capabilities["media_inspection"] == true {
		h.inspectAsync(r.Context(),conn,lease,hello.Capabilities,inspection)
	}
	// HTTP Range with http_switch keeps the control WebSocket across playback
	// generations. The old lease is revoked immediately (including HTTP GETs),
	// but the room is not stopped. Legacy clients still reconnect as before.
	watchStop := watchAttachment(conn, lease, reuseEnabled)
	defer func() { close(watchStop) }()
	defer func() {
		if h.Book.Release(lease) {
			notify("DISCONNECTED")
		}
	}()
	heartbeatCtx, stopHeartbeat := context.WithCancel(context.Background())
	defer stopHeartbeat()
	go func() {
		timer := time.NewTicker(25 * time.Second)
		defer timer.Stop()
		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-timer.C:
				probeCtx, cancel := context.WithTimeout(heartbeatCtx, 5*time.Second)
				err := conn.Ping(probeCtx)
				cancel()
				if err != nil {
					conn.CloseNow()
					return
				}
			}
		}
	}()
	conn.SetReadLimit(4096)
	if lease.SeekPositionMS != nil && hello.Capabilities["hcm1"] != true {
		reject(conn, "SEEK_UNSUPPORTED")
		return
	}
	if hello.Capabilities["hcm1"] == true && (h.OpenStream != nil || lease.SeekPositionMS != nil) {
		h.serveStream(r.Context(), conn, lease)
		return
	}
	if reuseEnabled {
		// Rebind only using a NEW Backend-issued one-use ticket on this same
		// socket. Never accept a file URL or arbitrary source from the TV.
		for {
			kind, raw, readErr := conn.Read(r.Context())
			if readErr != nil { return }
			var next Hello
			dec := json.NewDecoder(bytes.NewReader(raw))
			dec.DisallowUnknownFields()
			decodeErr := dec.Decode(&next)
			if decodeErr == nil {
				decodeErr = dec.Decode(new(any))
				if errors.Is(decodeErr, io.EOF) { decodeErr = nil }
			}
			if kind == websocket.MessageText && decodeErr == nil &&
				next.Type == "media.decoder.report" {
				if !h.handleDecoderReport(r.Context(),conn,lease,next,inspection) {
					reject(conn,"INVALID_DECODER_REPORT")
					return
				}
				continue
			}
			if kind==websocket.MessageText && decodeErr==nil && next.Type=="media.audio.select" {
				if !h.handleAudioSelect(r.Context(),conn,lease,next,inspection,r.RemoteAddr) {
					_ = h.publishConversionError(r.Context(),conn,lease,"UNSUPPORTED_AUDIO_SELECTION")
				}
				continue
			}
			if kind != websocket.MessageText || decodeErr != nil ||
				next.Type != "media.hello" || next.V != 1 ||
				next.RoomID != lease.RoomID || next.DeviceID != lease.DeviceID ||
				next.Generation <= lease.Generation ||
				next.Capabilities["http_range"] != true || next.Capabilities["http_switch"] != true {
				reject(conn,"INVALID_REQUEST")
				return
			}
			// Backend sends offers to Gateway and TV independently. The TV can
			// reach this socket before Gateway has finished HC SS source lookup
			// and installed the new ticket. Wait only for that known generation.
			renewed, claimErr := waitRebindClaim(r.Context(), h.Book, next)
			if claimErr != nil {
				reject(conn,Code(claimErr))
				return
			}
			offer, offerErr := h.OfferHTTP(renewed,r.RemoteAddr)
			if offerErr != nil {
				h.Book.Release(renewed)
				reject(conn,"SOURCE_UNAVAILABLE")
				return
			}
			if h.Streams != nil {
				h.Streams.Unregister(lease)
				h.Streams.RegisterHTTP(renewed)
			}
			// Release the old generation watcher before the grace timeout.
			close(watchStop)
			lease = renewed
			watchStop = watchAttachment(conn,lease,true)
			notify("ATTACHING")
			nextReady := map[string]any{
				"v":1,"type":"media.ready","room_id":lease.RoomID,
				"playback_id":lease.PlaybackID,"generation":lease.Generation,
				"connection_epoch":lease.Epoch,"http_switch":true,
				"http_media_path":offer.Path,"content_type":offer.ContentType,
				"media_kind":offer.MediaKind,"max_frame_payload_bytes":65536,
				"window_bytes":4194304,
			}
			if h.OfferConversion!=nil&&next.Capabilities["audio_selection"]==true {
				nextReady["audio_selection"]=true
			}
			data, encErr := json.Marshal(nextReady)
			if encErr != nil { return }
			writeCtx, finish := context.WithTimeout(r.Context(),5*time.Second)
			writeErr := conn.Write(writeCtx,websocket.MessageText,data)
			finish()
			if writeErr != nil { return }
			notify("READY")
			if next.Capabilities["media_inspection"] == true {
				h.inspectAsync(r.Context(),conn,lease,next.Capabilities,inspection)
			}
			if h.Log != nil {
				h.Log.Info("TV media WS rebind complete", "room_id",lease.RoomID,
					"playback_id",lease.PlaybackID,"generation",lease.Generation,
					"connection_epoch",lease.Epoch)
			}
		}
	}
	// An older TV or a Gateway without a source provider can still attach and
	// remain READY. Never start sending data without explicit HCM1 capability.
	for {
		kind, _, err := conn.Read(r.Context())
		if err != nil {
			return
		}
		// Handshake-only clients have no media protocol negotiated.
		if kind == websocket.MessageBinary {
			reject(conn, "UNSUPPORTED_VERSION")
			return
		}
		reject(conn, "INVALID_REQUEST")
		return
	}
}
// inspectAsync reports metadata separately from media.ready so a missing or
// slow ffprobe never delays the player. A superseded playback cannot publish
// a successful stale inspection to the TV.
func (h *Handler) inspectAsync(parent context.Context,conn *websocket.Conn,lease Lease,caps map[string]any,state *inspectionState) {
	if h.Inspect==nil||h.Book==nil {return}
	go func(){
		ctx,cancel:=context.WithTimeout(parent,9*time.Second)
		defer cancel()
		stopped:=make(chan struct{})
		go func(){
			select {
			case <-lease.Done:cancel()
			case <-stopped:
			}
		}()
		defer close(stopped)
		report:=h.Inspect(ctx,lease,caps)
		if ctx.Err()!=nil||!h.Book.Valid(lease.RoomID,lease){return}
		state.Save(lease,report,caps)
		h.publishInspection(ctx,conn,lease,report)
	}()
}

// watchAttachment closes a normal/HCM1 socket on lease revocation. Opt-in
// HTTP clients may rebind to the new generation within a bounded grace period;
// only room STOP allows no grace (RoomDone closes immediately).
// waitRebindClaim accommodates the offer-delivery race without accepting
// any unknown ticket or increasing the room limit. Each authorized WS has one
// bounded waiter; STOP or wrong credentials fail immediately.
func waitRebindClaim(ctx context.Context, book *Book, next Hello) (Lease,error) {
	waitCtx,cancel:=context.WithTimeout(ctx,7*time.Second)
	defer cancel()
	ticker:=time.NewTicker(50*time.Millisecond)
	defer ticker.Stop()
	for {
		lease,err:=book.Claim(next)
		if err==nil {return lease,nil}
		if !errors.Is(err,ErrStale) {return Lease{},err}
		select {
		case <-waitCtx.Done():
			return Lease{},ErrStale
		case <-ticker.C:
		}
	}
}

func watchAttachment(conn *websocket.Conn, lease Lease, allowSwitch bool) chan struct{} {
	stop := make(chan struct{})
	go func() {
		select {
		case <-lease.RoomDone:
			conn.CloseNow()
		case <-lease.Done:
			if !allowSwitch {
				conn.CloseNow()
				return
			}
			timer := time.NewTimer(10*time.Second)
			defer timer.Stop()
			select {
			case <-lease.RoomDone:
				conn.CloseNow()
			case <-timer.C:
				conn.CloseNow()
			case <-stop:
			}
		case <-stop:
		}
	}()
	return stop
}

func reject(conn *websocket.Conn, code string) {
	if conn == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	_ = conn.Write(ctx, websocket.MessageText, []byte(`{"v":1,"type":"media.error","code":"`+code+`","retryable":false}`))
	cancel()
	_ = conn.Close(websocket.StatusPolicyViolation, code)
}

// serveStream retains one outbound media writer. The receive loop only parses
// cumulative ACKs, so an untrusted TV cannot choose a file or control playback.
// STOP/switch revokes the lease via Book, which closes the socket and context.
func (h *Handler) serveStream(parent context.Context, conn *websocket.Conn, lease Lease) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	var source io.ReadCloser
	var actualPositionMS int64
	var err error
	if lease.SeekPositionMS != nil {
		if h.OpenStreamAt == nil {
			reject(conn, "SEEK_UNSUPPORTED")
			return
		}
		source, actualPositionMS, err = h.OpenStreamAt(ctx, lease)
		if err == nil && !validSeekResolution(lease, actualPositionMS) {
			err = ErrSeekResolution
		}
	} else if h.OpenStream != nil {
		source, err = h.OpenStream(ctx, lease)
	} else {
		reject(conn, "SOURCE_UNAVAILABLE")
		return
	}
	if err != nil || source == nil {
		if source != nil {
			_ = source.Close()
		}
		if errors.Is(err, ErrSeekUnsupported) {
			reject(conn, "SEEK_UNSUPPORTED")
		} else {
			reject(conn, "SOURCE_UNAVAILABLE")
		}
		return
	}
	if lease.SeekPositionMS != nil && h.Report != nil {
		h.Report(Update{RoomID: lease.RoomID, PlaybackID: lease.PlaybackID, Generation: lease.Generation,
			Epoch: lease.Epoch, Phase: "SEEK_RESOLVED", ActualPositionMS: actualPositionMS})
	}
	id := mediaframe.Identity{RoomID: lease.RoomID, PlaybackID: lease.PlaybackID, Generation: lease.Generation, Epoch: lease.Epoch}
	sender := mediaframe.NewSender(id)
	if h.Streams != nil {
		h.Streams.Register(lease, sender)
		defer h.Streams.Unregister(lease)
	}
	done := make(chan error, 1)
	go func() {
		var reportSequence uint64
		report := func(phase, code string) {
			if h.Report == nil || !h.Book.Valid(lease.RoomID, lease) || ctx.Err() != nil {
				return
			}
			reportSequence++
			h.Report(Update{RoomID: lease.RoomID, PlaybackID: lease.PlaybackID, Generation: lease.Generation, Epoch: lease.Epoch, Phase: phase, Sequence: reportSequence, ErrorCode: code})
		}
		started := false
		err := sender.Run(ctx, source, func(writeCtx context.Context, raw []byte) error {
			if err := conn.Write(writeCtx, websocket.MessageBinary, raw); err != nil {
				return err
			}
			if len(raw) > mediaframe.HeaderSize && !started {
				started = true
				report("STREAMING", "") // first binary media frame was actually written
			} else if len(raw) == mediaframe.HeaderSize {
				report("ENDED", "") // EOF sent, TV still independently decides when decoding ends
			}
			return nil
		})
		done <- err
		if err != nil {
			if ctx.Err() == nil {
				report("ERROR", "MEDIA_STREAM_ERROR")
			}
			conn.CloseNow()
		}
	}()
	for {
		kind, raw, readErr := conn.Read(ctx)
		if readErr != nil {
			break
		}
		if kind != websocket.MessageText || len(raw) > 4096 {
			break
		}
		var ack mediaframe.Ack
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.DisallowUnknownFields()
		if dec.Decode(&ack) != nil || !errors.Is(dec.Decode(new(any)), io.EOF) || sender.Acknowledge(ack) != nil {
			break
		}
	}
	cancel()
	// The only writer exits before the socket is closed. A non-cooperative
	// source cannot hold the HTTP handler forever.
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		conn.CloseNow()
	}
}
