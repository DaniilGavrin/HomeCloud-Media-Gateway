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
	"github.com/coder/websocket"
)

const HandshakeTimeout = 10 * time.Second

// StreamOpener is a trusted, in-process media source. It must authorize the
// given lease and return an independently closable reader.
type StreamOpener func(context.Context, Lease) (io.ReadCloser, error)

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
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{})
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
	b, err := json.Marshal(map[string]any{
		"v": 1, "type": "media.ready", "room_id": lease.RoomID, "playback_id": lease.PlaybackID,
		"generation": lease.Generation, "connection_epoch": lease.Epoch,
		"max_frame_payload_bytes": 65536, "window_bytes": 4194304,
	})
	if err != nil {
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
	// An old connection is forcibly closed when its lease is superseded or room stops.
	done := make(chan struct{})
	go func() {
		select {
		case <-lease.Done:
			conn.CloseNow()
		case <-done:
		}
	}()
	defer close(done)
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
