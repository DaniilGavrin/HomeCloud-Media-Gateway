// Package control attaches the Media Gateway to its co-located HC WS Backend.
// It sends only JSON commands/health. Video bytes NEVER use this connection.
package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/netip"
	"net/url"
	"strconv"
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
func Run(ctx context.Context, cfg config.Config, bound []netbind.Endpoint, log *slog.Logger) {
	endpoints := AdvertisedEndpoints(bound, cfg.Server.WebSocketPath)
	if len(endpoints) == 0 {
		log.Warn("No reachable private LAN/Tailscale Gateway endpoint; control registration disabled (loopback-only mode)")
		return
	}
	retry := time.Second
	for ctx.Err() == nil {
		err := connect(ctx, cfg.Backend.WebSocketURL, endpoints, log)
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
	V      int             `json:"v"`
	ID     string          `json:"id"`
	Type   string          `json:"type"`
	Action string          `json:"action"`
	Error  string          `json:"error"`
	Data   json.RawMessage `json:"data"`
}

func connect(ctx context.Context, wsURL string, endpoints []Endpoint, log *slog.Logger) error {
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
		"gateway_id": deviceID,
		"endpoints":  endpoints,
		"capabilities": map[string]any{
			"media_protocol": 1,
			"binary_frame":   "HCM1",
			"direct_ws":      false,      // reserved 501 route until KAN-48
			"modes":          []string{}, // Direct/Adaptive not yet implemented
		},
	}}, "result")
	if err != nil {
		return fmt.Errorf("register: %w", err)
	}
	if out.Action != "media.gateway.register" {
		return errors.New("unexpected action in register result")
	}
	log.Info("Media Gateway registered with HomeCloud Backend", "gateway_id", deviceID, "endpoints", endpoints, "media_ready", false)
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			_, err = exchange(message{V: 1, ID: "gateway-heartbeat-" + strconv.FormatInt(time.Now().UnixNano(), 10), Type: "ping"}, "pong")
			if err != nil {
				return fmt.Errorf("control heartbeat: %w", err)
			}
		}
	}
}
