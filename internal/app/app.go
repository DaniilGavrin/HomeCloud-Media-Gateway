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
	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/directws"
	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/netbind"
	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/rooms"
	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/source"
)

type App struct {
	cfg               config.Config
	logger            *slog.Logger
	roomManager       *rooms.Manager
	tickets           *directws.Book
	attachUpdates     chan directws.Update
	streamOpener      directws.StreamOpener
	seekOpener        directws.SeekStreamOpener
	streamControl     *directws.StreamControl
	sourceCoordinator *source.Coordinator // optional until HC Storage Service is deployed
	sourceReaders     map[source.Strategy]source.ReaderFactory // trusted factories only; configure before Run
}

func New(cfg config.Config, logger *slog.Logger) *App {
	limit := cfg.Limits.MaxRooms
	if limit == 0 {
		// Existing Handler-only unit tests create minimal, non-validated configs.
		limit = 1
	}
	manager, err := rooms.New(limit)
	if err != nil {
		panic("app.New requires validated limits.max_rooms: " + err.Error())
	}
	return &App{cfg: cfg, logger: logger, roomManager: manager, tickets: directws.New(limit), attachUpdates: make(chan directws.Update, 1024), streamControl: directws.NewStreamControl()}
}

// Rooms returns Gateway-local state only. HC WS Backend is authoritative.
// KAN-47 will route validated Backend commands through this manager.
func (a *App) Rooms() *rooms.Manager   { return a.roomManager }
func (a *App) Tickets() *directws.Book { return a.tickets }

// SetStreamOpener installs a trusted, in-process media source before Run.
// No source is installed by default; KAN-48 attaches stay handshake-only.
func (a *App) SetStreamOpener(opener directws.StreamOpener) { a.streamOpener = opener }

// SetSeekStreamOpener is for future authenticated storage readers / FFmpeg.
// It must return bytes starting at the actual achieved timeline/keyframe.
func (a *App) SetSeekStreamOpener(opener directws.SeekStreamOpener) { a.seekOpener = opener }

// SetSourceCoordinator enables KAN-56 metadata resolution for each trusted
// Backend media.room.offer. The default remains handshake-only until HC SS
// (KAN-39) is present; never synthesize a source or media availability.
// Configure this before calling Run; concurrent configuration is unsupported.
func (a *App) SetSourceCoordinator(coordinator *source.Coordinator) {
	a.sourceCoordinator = coordinator
}

// SetSourceReaders registers trusted local/remote readers BEFORE Run. Readers
// only receive previously authorized, generation-fenced HC SS source plans.
// No source connector/readers are registered in production by default.
func (a *App) SetSourceReaders(factories map[source.Strategy]source.ReaderFactory) {
	a.sourceReaders = make(map[source.Strategy]source.ReaderFactory, len(factories))
	for strategy, factory := range factories {
		if factory != nil {
			a.sourceReaders[strategy] = factory
		}
	}
}

// Handler can be tested without real TCP sockets; KAN-48 implements /ws/stream.
func (a *App) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "alive", "service": "homecloud-media-gateway"})
	})
	opener := a.streamOpener
	if opener == nil && a.sourceCoordinator != nil && a.sourceCoordinator.Plans != nil && len(a.sourceReaders) > 0 {
		// HCM1 sender is already bounded by credit/ACK. The only file identifier
		// available here is the trusted Backend-issued room/playback/generation.
		opener = func(ctx context.Context, lease directws.Lease) (source.Reader, error) {
			return source.OpenFromPlan(ctx, source.Token{
				RoomID: lease.RoomID, PlaybackID: lease.PlaybackID,
				Generation: lease.Generation,
			}, 0, a.sourceCoordinator.Plans, a.sourceReaders)
		}
	}
	handler := &directws.Handler{Book: a.tickets, Log: a.logger, OpenStream: opener, OpenStreamAt: a.seekOpener, Streams: a.streamControl, Report: func(u directws.Update) {
		select {
		case a.attachUpdates <- u:
		default:
			a.logger.Warn("Direct TV attach status queue full", "room_id", u.RoomID)
		}
	}}
	mux.Handle("GET "+a.cfg.Server.WebSocketPath, handler)
	return mux
}

// Run binds only concrete allowed private interfaces, then registers addresses
// of the successfully bound LAN/Tailscale sockets with the local Backend.
func (a *App) Run(ctx context.Context) error {
	defer a.roomManager.Close() // stop local room/playback workers on any exit
	defer a.tickets.Close()
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
	go control.RunWithSources(controlCtx, a.cfg, bound, a.logger, a.roomManager, a.tickets, a.attachUpdates, a.streamControl, a.sourceCoordinator)

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
