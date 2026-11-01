// Package app owns the HTTP server lifecycle of HomeCloud Media Gateway.
package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/config"
)

// App is a minimal host for future WS and media components.
type App struct {
	cfg    config.Config
	logger *slog.Logger
	server *http.Server
}

func New(cfg config.Config, logger *slog.Logger) *App {
	a := &App{cfg: cfg, logger: logger}
	a.server = &http.Server{
		Handler:           a.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	return a
}

// Handler is exported so endpoints can be tested without opening a TCP port.
func (a *App) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status":  "alive",
			"service": "homecloud-media-gateway",
		})
	})

	// Reserved only. WS handshake and binary streaming belong to MG-08/MG-09.
	mux.HandleFunc("GET "+a.cfg.Server.WebSocketPath, func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "media WebSocket is not implemented yet (MG-08)", http.StatusNotImplemented)
	})
	return mux
}

// Run binds first, then reports startup. A signal triggers graceful shutdown.
func (a *App) Run(ctx context.Context) error {
	addr := net.JoinHostPort(a.cfg.Server.Host, strconv.Itoa(a.cfg.Server.Port))
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}

	result := make(chan error, 1)
	go func() { result <- a.server.Serve(listener) }()
	a.logger.Info("HomeCloud Media Gateway listening", "address", listener.Addr().String(),
		"health_path", "/healthz", "reserved_media_ws_path", a.cfg.Server.WebSocketPath)

	select {
	case err := <-result:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve: %w", err)
		}
		return nil

	case <-ctx.Done():
		a.logger.Info("HomeCloud Media Gateway shutting down")
		stopCtx, cancel := context.WithTimeout(context.Background(), a.cfg.Server.ShutdownTimeout.Duration)
		defer cancel()

		shutdownErr := a.server.Shutdown(stopCtx)
		if shutdownErr != nil {
			_ = a.server.Close()
		}
		serveErr := <-result
		if shutdownErr != nil {
			return fmt.Errorf("graceful shutdown failed: %w", shutdownErr)
		}
		if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			return fmt.Errorf("serve during shutdown: %w", serveErr)
		}
		return nil
	}
}
