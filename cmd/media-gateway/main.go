package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
	)
	defer stop()

	logger.Info("HomeCloud Media Gateway starting")

	// TODO: Load YAML configuration.
	// TODO: Initialize application components.
	// TODO: Connect to HomeCloud WS Backend.
	// TODO: Start Media WebSocket listener.

	logger.Info("HomeCloud Media Gateway started")

	// Keep the process alive until interrupted.
	<-ctx.Done()

	logger.Info("HomeCloud Media Gateway shutting down")
	logger.Info("HomeCloud Media Gateway stopped")
}
