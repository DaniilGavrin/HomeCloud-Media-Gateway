package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/app"
	"github.com/DaniilGavrin/HomeCloud-Media-Gateway/internal/config"
)

func main() {
	os.Exit(run())
}

func run() int {
	configPath := flag.String("config", "config/config.yaml", "path to Media Gateway YAML settings")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Error("Failed to load startup configuration", "error", err)
		return 1
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	logger.Info("HomeCloud Media Gateway starting", "config", *configPath)
	logger.Info("Backend endpoint configured (not yet connected)", "backend_ws_url", cfg.Backend.WebSocketURL)

	service := app.New(cfg, logger)
	if err := service.Run(ctx); err != nil {
		logger.Error("HomeCloud Media Gateway stopped with error", "error", err)
		return 1
	}
	logger.Info("HomeCloud Media Gateway stopped")
	return 0
}
