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
