// Package app owns Gateway HTTP listeners and the co-located Backend control connection.
package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/config"
	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/control"
	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/netbind"
)

type App struct {
	cfg    config.Config
	logger *slog.Logger
}

func New(cfg config.Config, logger *slog.Logger) *App { return &App{cfg: cfg, logger: logger} }

// Handler can be tested without real TCP sockets; KAN-48 implements /ws/stream.
func (a *App) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "alive", "service": "homecloud-media-gateway"})
	})
	mux.HandleFunc("GET "+a.cfg.Server.WebSocketPath, func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "media WebSocket is not implemented yet (MG-08)", http.StatusNotImplemented)
	})
	return mux
}

// Run binds only concrete allowed private interfaces, then registers addresses
// of the successfully bound LAN/Tailscale sockets with the local Backend.
func (a *App) Run(ctx context.Context) error {
	discovered, err := netbind.Discover(a.cfg.Server.Host, a.cfg.Server.Port)
	if err != nil {
		return fmt.Errorf("private interface discovery: %w", err)
	}
	type active struct {
		ep       netbind.Endpoint
		listener net.Listener
		server   *http.Server
	}
	var listeners []active
	baseHandler := a.Handler()
	for _, ep := range discovered {
		listener, e := (&net.ListenConfig{}).Listen(ctx, "tcp", ep.Address)
		if e != nil {
			a.logger.Warn("Media Gateway interface unavailable", "address", ep.Address, "error", e)
			continue
		}
		listeners = append(listeners, active{ep: ep, listener: listener, server: &http.Server{
			Handler:           netbind.RestrictPeer(ep, baseHandler),
			ReadHeaderTimeout: 5 * time.Second,
			IdleTimeout:       60 * time.Second,
		}})
	}
	if len(listeners) == 0 {
		return errors.New("could not bind any allowed private Gateway interface")
	}
	defer func() {
		for _, l := range listeners {
			_ = l.listener.Close()
		}
	}()

	result := make(chan error, len(listeners))
	bound := make([]netbind.Endpoint, 0, len(listeners))
	for _, l := range listeners {
		bound = append(bound, l.ep)
		a.logger.Info("HomeCloud Media Gateway listening", "network", l.ep.Network,
			"interface", l.ep.Interface, "address", l.ep.Address,
			"health_path", "/healthz", "reserved_media_ws_path", a.cfg.Server.WebSocketPath)
		l := l
		go func() { result <- l.server.Serve(l.listener) }()
	}
	controlCtx, stopControl := context.WithCancel(ctx)
	defer stopControl()
	go control.Run(controlCtx, a.cfg, bound, a.logger)

	var serveErr error
	select {
	case e := <-result:
		if e != nil && !errors.Is(e, http.ErrServerClosed) {
			serveErr = fmt.Errorf("serve: %w", e)
		}
	case <-ctx.Done():
		a.logger.Info("HomeCloud Media Gateway shutting down")
	}
	stopControl()
	stopCtx, cancel := context.WithTimeout(context.Background(), a.cfg.Server.ShutdownTimeout.Duration)
	defer cancel()
	for _, l := range listeners {
		if e := l.server.Shutdown(stopCtx); e != nil {
			_ = l.server.Close()
			if serveErr == nil {
				serveErr = fmt.Errorf("graceful shutdown %s: %w", l.ep.Address, e)
			}
		}
	}
	// Other Serve goroutines will terminate after Shutdown. Their result channels
	// are buffered to prevent goroutine leaks if one listener failed first.
	return serveErr
}
