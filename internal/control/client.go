// Package control attaches the Media Gateway to its co-located HC WS Backend.
// It sends only JSON commands/health. Video bytes NEVER use this connection.
package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/directws"
	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/rooms"
	"log/slog"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/config"
	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/netbind"
	"github.com/coder/websocket"
)

const deviceID = "homecloud-media-gateway"

type Endpoint struct {
	Network string `json:"network"`
	URL     string `json:"url"`
}

func AdvertisedEndpoints(bound []netbind.Endpoint, path string) []Endpoint {
	result := make([]Endpoint, 0, len(bound))
	for _, ep := range bound {
		if ep.Network == "lan" || ep.Network == "tailscale" {
			result = append(result, Endpoint{Network: ep.Network, URL: ep.MediaURL(path)})
		}
	}
	return result
}
func Run(ctx context.Context, cfg config.Config, bound []netbind.Endpoint, log *slog.Logger, managers ...*rooms.Manager) {
	var manager *rooms.Manager
	if len(managers) > 0 {
		manager = managers[0]
	}
	run(ctx, cfg, bound, log, manager, nil, nil, nil)
}

// RunWithAttach connects direct-TV attachment state to the same control plane.
func RunWithAttach(ctx context.Context, cfg config.Config, bound []netbind.Endpoint, log *slog.Logger, manager *rooms.Manager, book *directws.Book, updates <-chan directws.Update) {
	run(ctx, cfg, bound, log, manager, book, updates, nil)
}

// RunWithControls binds trusted Backend events to current HCM1 sender controls.
func RunWithControls(ctx context.Context, cfg config.Config, bound []netbind.Endpoint, log *slog.Logger, manager *rooms.Manager, book *directws.Book, updates <-chan directws.Update, streams *directws.StreamControl) {
	run(ctx, cfg, bound, log, manager, book, updates, streams)
}
func run(ctx context.Context, cfg config.Config, bound []netbind.Endpoint, log *slog.Logger, roomManager *rooms.Manager, book *directws.Book, updates <-chan directws.Update, streams *directws.StreamControl) {
	endpoints := AdvertisedEndpoints(bound, cfg.Server.WebSocketPath)
	if len(endpoints) == 0 {
		log.Warn("No reachable private LAN/Tailscale Gateway endpoint; control registration disabled (loopback-only mode)")
		return
	}
	retry := time.Second
	for ctx.Err() == nil {
		err := connectWithAttach(ctx, cfg.Backend.WebSocketURL, endpoints, log, roomManager, book, updates, cfg, streams)
		if book != nil {
			book.Reset()
		}
		if streams != nil {
			streams.Reset()
		}
		if roomManager != nil {
			for _, snap := range roomManager.List() {
				_, _ = roomManager.Fail(snap.RoomID, rooms.ReasonLost)
			}
		}
		if ctx.Err() != nil {
			return
		}
		log.Warn("Gateway control channel disconnected; reconnect scheduled", "error", err, "retry_in", retry)
		timer := time.NewTimer(retry)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return
		}
		if retry < 15*time.Second {
			retry *= 2
			if retry > 15*time.Second {
				retry = 15 * time.Second
			}
		}
		// A newly registered session is a successful reconnect. We retain
		// bounded backoff for repeatedly unstable connections.
	}
}

type message struct {
	V          int    `json:"v"`
	ID         string `json:"id,omitempty"`
	Type       string `json:"type"`
	Action     string `json:"action,omitempty"`
	DeviceID   string `json:"device_id,omitempty"`
	ClientType string `json:"client_type,omitempty"`
	Data       any    `json:"data,omitempty"`
}
type response struct {
	Topic  string          `json:"topic"`
	V      int             `json:"v"`
	ID     string          `json:"id"`
	Type   string          `json:"type"`
	Action string          `json:"action"`
	Error  string          `json:"error"`
	Data   json.RawMessage `json:"data"`
}

func connect(ctx context.Context, wsURL string, endpoints []Endpoint, log *slog.Logger, roomManager *rooms.Manager) error {
	return connectWithAttach(ctx, wsURL, endpoints, log, roomManager, nil, nil, config.Config{})
}
func connectWithAttach(ctx context.Context, wsURL string, endpoints []Endpoint, log *slog.Logger, roomManager *rooms.Manager, book *directws.Book, updates <-chan directws.Update, cfg config.Config, streamControls ...*directws.StreamControl) error {
	var streams *directws.StreamControl
	if len(streamControls) > 0 {
		streams = streamControls[0]
	}
	// The companion always connects to the local Backend. Never use an
	// arbitrary LAN server for privileged media gateway registration.
	u, err := url.Parse(wsURL)
	if err != nil {
		return err
	}
	ip, err := netip.ParseAddr(u.Hostname())
	if err != nil || !ip.IsLoopback() {
		return errors.New("Gateway control WS must target local Backend loopback IP")
	}
	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	conn, _, err := websocket.Dial(dialCtx, wsURL, nil)
	cancel()
	if err != nil {
		return err
	}
	defer conn.CloseNow()
	conn.SetReadLimit(65536)

	exchange := func(m message, expected string) (response, error) {
		var out response
		reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		b, e := json.Marshal(m)
		if e != nil {
			return out, e
		}
		if e = conn.Write(reqCtx, websocket.MessageText, b); e != nil {
			return out, e
		}
		for {
			typ, raw, e := conn.Read(reqCtx)
			if e != nil {
				return out, e
			}
			if typ != websocket.MessageText {
				return out, errors.New("binary control frame forbidden")
			}
			if e = json.Unmarshal(raw, &out); e != nil {
				return out, e
			}
			if out.V != 1 {
				return out, errors.New("unexpected control protocol version")
			}
			if out.Type == "event" {
				continue
			}
			if out.ID != m.ID {
				return out, fmt.Errorf("control response ID mismatch: %q", out.ID)
			}
			if out.Type == "error" {
				return out, fmt.Errorf("Backend rejected %s: %s", m.Action, out.Error)
			}
			if out.Type != expected {
				return out, fmt.Errorf("expected %s; got %s", expected, out.Type)
			}
			return out, nil
		}
	}
	_, err = exchange(message{V: 1, ID: "gateway-hello", Type: "hello", DeviceID: deviceID, ClientType: "media_gateway"}, "identified")
	if err != nil {
		return fmt.Errorf("hello: %w", err)
	}
	out, err := exchange(message{V: 1, ID: "gateway-register", Type: "command", Action: "media.gateway.register", Data: map[string]any{
		"gateway_id":                 deviceID,
		"endpoints":                  endpoints,
		"heartbeat_interval_seconds": heartbeatIntervalSeconds(cfg),
		"heartbeat_grace_seconds":    10,
		"capabilities": map[string]any{
			"media_protocol": 1,
			"binary_frame":   "HCM1",
			"direct_ws":      true,       // KAN-48: attach handshake only; no HCM1 media yet
			"modes":          []string{}, // Direct/Adaptive not yet implemented
		},
	}}, "result")
	if err != nil {
		return fmt.Errorf("register: %w", err)
	}
	if out.Action != "media.gateway.register" {
		return errors.New("unexpected action in register result")
	}
	log.Info("Media Gateway registered with HomeCloud Backend", "gateway_id", deviceID, "endpoints", endpoints, "media_ready", true, "streaming_ready", false)
	// KAN-47: process unsolicited room offers as soon as they arrive.
	// A single reader owns all incoming frames; unsolicited command results
	// for participant acknowledgements are identified by their request ID.
	var writeMu sync.Mutex
	sendAck := func(roomID, playbackID, phase string) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		b, e := json.Marshal(message{V: 1, ID: "gw-room-" + strconv.FormatInt(time.Now().UnixNano(), 10), Type: "command", Action: "media.room.participant", Data: map[string]any{
			"room_id": roomID, "playback_id": playbackID, "participant": "gateway", "phase": phase,
		}})
		if e != nil {
			return e
		}
		timeout, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return conn.Write(timeout, websocket.MessageText, b)
	}
	sendRoomHeartbeat := func(snap rooms.Snapshot) error {
		msg, e := roomHeartbeatMessage(snap)
		if e != nil {
			return e
		}
		b, e := json.Marshal(msg)
		if e != nil {
			return e
		}
		writeMu.Lock()
		defer writeMu.Unlock()
		timeout, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return conn.Write(timeout, websocket.MessageText, b)
	}
	background, stop := context.WithCancel(ctx)
	defer stop()
	// Attachment updates are sent only on this trusted Gateway control socket.
	// The existing single reader consumes and checks command acknowledgements.
	if updates != nil {
		go func() {
			for {
				select {
				case <-background.Done():
					return
				case update, ok := <-updates:
					if !ok {
						return
					}
					if err := observeStreamLifecycle(roomManager, update); err != nil {
						log.Debug("Ignoring stale Gateway playback observation", "room_id", update.RoomID, "phase", update.Phase, "error", err)
					}
					action, requestID, data := directUpdateCommand(update)
					b, e := json.Marshal(message{V: 1, ID: requestID, Type: "command", Action: action, Data: data})
					if e != nil {
						continue
					}
					writeMu.Lock()
					timeout, cancel := context.WithTimeout(background, 5*time.Second)
					e = conn.Write(timeout, websocket.MessageText, b)
					cancel()
					writeMu.Unlock()
					if e != nil {
						conn.CloseNow()
						return
					}
				}
			}
		}()
	}
	if roomManager != nil {
		go func() {
			ticker := time.NewTicker(time.Duration(heartbeatIntervalSeconds(cfg)) * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-background.Done():
					return
				case <-ticker.C:
					for _, snap := range roomManager.List() {
						if snap.State != rooms.Active || snap.PlaybackID == "" {
							continue
						}
						if e := sendRoomHeartbeat(snap); e != nil {
							log.Warn("Gateway room heartbeat send failed", "room_id", snap.RoomID, "error", e)
							conn.CloseNow()
							return
						}
					}
				}
			}
		}()
	}
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-background.Done():
				return
			case <-ticker.C:
				pingCtx, cancel := context.WithTimeout(background, 5*time.Second)
				err := conn.Ping(pingCtx)
				cancel()
				if err != nil {
					conn.CloseNow()
					return
				}
			}
		}
	}()
	for {
		typ, raw, err := conn.Read(ctx)
		if err != nil {
			return fmt.Errorf("Gateway control read: %w", err)
		}
		if typ != websocket.MessageText {
			return errors.New("binary media bytes forbidden on control WS")
		}
		var event response
		if err := json.Unmarshal(raw, &event); err != nil {
			return fmt.Errorf("decode control event: %w", err)
		}
		if event.V != 1 {
			return errors.New("invalid control event version")
		}
		if event.Type == "result" || event.Type == "error" {
			if strings.HasPrefix(event.ID, "gw-room-") || strings.HasPrefix(event.ID, "gw-attach-") || strings.HasPrefix(event.ID, "gw-heartbeat-") || strings.HasPrefix(event.ID, "gw-report-") || strings.HasPrefix(event.ID, "gw-control-") || strings.HasPrefix(event.ID, "gw-seek-") {
				if event.Type == "error" {
					log.Warn("Backend rejected room acknowledgement", "error", event.Error)
				}
				continue
			}
			return fmt.Errorf("unsolicited control response %s", event.ID)
		}
		if event.Type != "event" {
			continue
		}
		switch event.Topic {
		case "media.room.offer":
			if roomManager == nil {
				continue
			}
			var offer roomOffer
			if e := json.Unmarshal(event.Data, &offer); e != nil {
				log.Warn("Invalid room offer", "error", e)
				continue
			}
			e := applyRoomOffer(roomManager, offer)
			if e == nil && book != nil {
				e = book.Prepare(directws.Grant{RoomID: offer.RoomID, PlaybackID: offer.PlaybackID, DeviceID: offer.Target.DeviceID, Generation: offer.Generation, Ticket: offer.AttachTicket, ExpiresAt: offer.AttachExpiresAt, SeekPositionMS: offer.SeekPositionMS})
			}
			phase := "APPLIED"
			if e != nil {
				phase = "FAILED"
				log.Warn("Gateway room offer rejected", "room_id", offer.RoomID, "error", e)
			}
			if offer.RoomID != "" && offer.PlaybackID != "" {
				if e := sendAck(offer.RoomID, offer.PlaybackID, phase); e != nil {
					return e
				}
				if e == nil {
					if snap, getErr := roomManager.Get(offer.RoomID); getErr == nil {
						if sendErr := sendRoomHeartbeat(snap); sendErr != nil {
							return sendErr
						}
					}
				}
			}
		case "media.room.ticket":
			if roomManager == nil || book == nil {
				continue
			}
			var renewal roomOffer
			if e := json.Unmarshal(event.Data, &renewal); e != nil {
				log.Warn("Invalid room ticket refresh")
				continue
			}
			snap, e := roomManager.Get(renewal.RoomID)
			if e != nil || snap.Target != renewal.Target || snap.PlaybackID != renewal.PlaybackID || snap.Generation != renewal.Generation {
				log.Warn("Ignoring stale room ticket", "room_id", renewal.RoomID)
				continue
			}
			if e = book.Prepare(directws.Grant{RoomID: renewal.RoomID, PlaybackID: renewal.PlaybackID, DeviceID: renewal.Target.DeviceID, Generation: renewal.Generation, Ticket: renewal.AttachTicket, ExpiresAt: renewal.AttachExpiresAt, SeekPositionMS: renewal.SeekPositionMS}); e != nil {
				log.Warn("Rejecting room ticket refresh", "room_id", renewal.RoomID, "error", e)
			}
		case "media.room.control":
			q, decodeErr := decodeMediaControl(event.Data)
			if decodeErr != nil {
				log.Warn("Invalid Gateway control command", "error", decodeErr)
				continue
			}
			var applyErr error
			if roomManager == nil || streams == nil {
				applyErr = directws.ErrNoStream
			} else {
				snap, e := roomManager.Get(q.RoomID)
				if e != nil || snap.PlaybackID != q.PlaybackID || snap.Generation != q.Generation {
					applyErr = directws.ErrStaleControl
				} else {
					applyErr = streams.Apply(q)
				}
			}
			phase := "APPLIED"
			ackData := map[string]any{"room_id": q.RoomID, "playback_id": q.PlaybackID, "generation": q.Generation, "connection_epoch": q.Epoch, "command_id": q.CommandID, "command_sequence": q.CommandSequence, "participant": "gateway"}
			if applyErr != nil {
				phase = "FAILED"
				ackData["error_code"] = controlErrorCode(applyErr)
			}
			ackData["phase"] = phase
			b, e := json.Marshal(message{V: 1, ID: "gw-control-" + strconv.FormatInt(time.Now().UnixNano(), 10), Type: "command", Action: "media.room.control.ack", Data: ackData})
			if e != nil {
				return e
			}
			writeMu.Lock()
			sendCtx, cancelSend := context.WithTimeout(background, 5*time.Second)
			e = conn.Write(sendCtx, websocket.MessageText, b)
			cancelSend()
			writeMu.Unlock()
			if e != nil {
				return e
			}
		case "media.room.closed":
			var closed struct {
				RoomID string `json:"room_id"`
			}
			if json.Unmarshal(event.Data, &closed) == nil && closed.RoomID != "" {
				// STOP always revokes the direct media lease and releases the active
				// sender, even if the room manager is already missing this room.
				if book != nil {
					book.Remove(closed.RoomID)
				}
				if streams != nil {
					streams.RemoveRoom(closed.RoomID)
				}
				if roomManager != nil {
					_, _ = roomManager.Stop(closed.RoomID)
				}
			}
		}
	}
}

type roomOffer struct {
	RoomID          string       `json:"room_id"`
	PlaybackID      string       `json:"playback_id"`
	Generation      uint32       `json:"generation"`
	Target          rooms.Target `json:"target"`
	Source          rooms.Source `json:"source"`
	GatewayID       string       `json:"gateway_id"`
	AttachTicket    string       `json:"attach_ticket"`
	AttachExpiresAt time.Time    `json:"attach_expires_at"`
	SeekPositionMS  *int64       `json:"seek_position_ms,omitempty"`
}

func applyRoomOffer(manager *rooms.Manager, offer roomOffer) error {
	if offer.RoomID == "" || offer.PlaybackID == "" || offer.Generation == 0 {
		return rooms.ErrInvalidInput
	}
	snap, err := manager.Get(offer.RoomID)
	created := false
	if errors.Is(err, rooms.ErrRoomNotFound) {
		snap, _, err = manager.CreateAssigned(offer.RoomID, offer.Target)
		if err != nil {
			return err
		}
		created = true
		if _, err = manager.AdvanceRoom(snap.RoomID, rooms.Connecting); err != nil {
			_, _ = manager.Stop(snap.RoomID)
			return err
		}
		if _, err = manager.AdvanceRoom(snap.RoomID, rooms.Active); err != nil {
			_, _ = manager.Stop(snap.RoomID)
			return err
		}
	} else if err != nil {
		return err
	}
	if snap.Target != offer.Target {
		return rooms.ErrRoomConflict
	}
	if snap.PlaybackID == offer.PlaybackID && snap.Generation == offer.Generation && snap.Source != nil && *snap.Source == offer.Source {
		return nil
	} // retransmission
	if snap.Generation+1 != offer.Generation || snap.Generation == ^uint32(0) {
		if created {
			_, _ = manager.Stop(offer.RoomID)
		}
		return rooms.ErrStalePlayback
	}
	var result rooms.Snapshot
	if offer.SeekPositionMS != nil {
		if snap.PlaybackID != offer.PlaybackID || snap.Source == nil || *snap.Source != offer.Source {
			return rooms.ErrStalePlayback
		}
		result, _, _, err = manager.Seek(rooms.Token{RoomID: snap.RoomID, PlaybackID: snap.PlaybackID, Generation: snap.Generation}, *offer.SeekPositionMS)
	} else {
		if snap.PlaybackID == offer.PlaybackID {
			return rooms.ErrStalePlayback
		}
		result, _, _, err = manager.StartAssignedPlayback(offer.RoomID, offer.PlaybackID, offer.Source)
	}
	if err != nil {
		if created {
			_, _ = manager.Stop(offer.RoomID)
		}
		return err
	}
	if result.Generation != offer.Generation {
		if created {
			_, _ = manager.Stop(offer.RoomID)
		}
		return rooms.ErrStalePlayback
	}
	return nil
}
