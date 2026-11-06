package main

import (
	"context"
	"flag"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/app"
	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/config"
	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/mediainspect"
	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/source"
)

func main() { os.Exit(run()) }

func run() int {
	configPath := flag.String("config", "config/config.yaml", "path to Media Gateway YAML settings")
	supervised := flag.Bool("supervised", false, "stop gracefully on inherited stdin EOF (Backend companion)")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Error("Failed to load startup configuration", "error", err)
		return 1
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if *supervised {
		// This watcher is intentionally only enabled in companion mode.
		// The Backend closes its private stdin pipe on Stop or process death.
		// Unlike Unix SIGTERM, this works on Windows and Linux.
		watchSupervisorPipe(os.Stdin, cancel)
	}
	logger.Info("HomeCloud Media Gateway starting", "config", *configPath, "supervised", *supervised)
	logger.Info("Backend control endpoint configured", "backend_ws_url", cfg.Backend.WebSocketURL)

	service := app.New(cfg, logger)
	if len(cfg.Storage.SourceNodes) > 0 {
		registry := source.StaticRegistry{Nodes: cfg.Storage.SourceNodes}
		coordinator := &source.Coordinator{
			Resolver: &source.Resolver{Registry: registry,
				Local: source.RootProbe{Roots: cfg.Storage.LocalRoots}},
			Plans: source.NewPlans(),
		}
		// The Gateway cannot read arbitrary paths. Each room offer must first
		// pass the storage owner's Stat and room-generation source plan fence.
		service.SetSourceCoordinator(coordinator)
		remote := source.RemoteReaderFactory{Registry: registry}
		readers := map[source.Strategy]source.ReaderFactory{
			// Temporary bounded-range streaming fallback for <=1GiB;
			// the on-disk prefetch implementation is not yet available.
			source.RemotePrefetch: remote,
			source.RemoteBlockStream: remote,
		}
		if len(cfg.Storage.LocalRoots) > 0 {
			readers[source.LocalDirect] = source.LocalReaderFactory{Roots: cfg.Storage.LocalRoots}
		}
		service.SetSourceReaders(readers)
		if cfg.Inspector.Enabled {
			service.SetMediaInspector(mediainspect.New(cfg.Inspector.Binary, coordinator.Plans, readers))
			logger.Info("Media Inspector enabled", "binary", cfg.Inspector.Binary)
		}
		logger.Info("HCS1 remote source readers configured",
			"nodes", len(cfg.Storage.SourceNodes), "shared_roots", len(cfg.Storage.LocalRoots))
	}
	if err := service.Run(ctx); err != nil {
		logger.Error("HomeCloud Media Gateway stopped with error", "error", err)
		return 1
	}
	logger.Info("HomeCloud Media Gateway stopped")
	return 0
}

func watchSupervisorPipe(in io.Reader, stop context.CancelFunc) {
	go func() {
		_, _ = io.Copy(io.Discard, in)
		stop()
	}()
}
