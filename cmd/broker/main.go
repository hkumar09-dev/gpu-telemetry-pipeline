package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gpu-telemetry-pipeline/internal/mq"
	"github.com/gpu-telemetry-pipeline/utils"
)

func main() {

	// Setup logging
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	addr := utils.Getenv("MQ_ADDR", ":9000")
	engine := mq.NewEngine(mq.Config{MaxRetries: 5, RetryBackoff: 5 * time.Millisecond})

	// Context is cancelled when SIGINT/SIGTERM is received.
	ctx, stop := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)

	defer stop()

	srv := mq.NewServer(ctx, engine, log)

	// Start broker.
	serverErr := make(chan error, 1)
	go func() {
		log.Info("starting broker", "addr", addr)
		if err := srv.ListenAndServe(addr); err != nil {
			log.Error("broker stopped", "err", err)
			os.Exit(1)
		}
	}()

	// Wait for either:
	// 1. shutdown signal
	// 2. server failure
	select {
	case <-ctx.Done():
		log.Info("shutdown signal received")

	case err := <-serverErr:
		log.Error("broker stopped unexpectedly", "err", err)
	}

	// Give in-flight operations time to finish.
	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	log.Info("shutdown signal received")
	if err := srv.Close(); err != nil {
		log.Error("broker shutdown failed", "err", err)
	}

	// Keep shutdown context available
	// graceful shutdown with context.
	_ = shutdownCtx
	log.Info("broker shutdown complete")
}
