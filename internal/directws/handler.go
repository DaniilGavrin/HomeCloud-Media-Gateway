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

	"github.com/coder/websocket"
)

const HandshakeTimeout = 10 * time.Second

type Handler struct {
	Book   *Book
	Log    *slog.Logger
	Report func(Update)
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
	conn.SetReadLimit(4096) // KAN-49 will add bounded HCM1 frame handling.
	for {
		kind, _, err := conn.Read(r.Context())
		if err != nil {
			return
		}
		// KAN-48 deliberately accepts no media payload/control writes after hello.
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
