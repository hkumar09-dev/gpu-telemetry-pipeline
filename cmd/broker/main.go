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

var osExit = os.Exit

func main() {
	if err := run(context.Background()); err != nil {
		osExit(1)
	}
}

func run(parent context.Context) error {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	addr := utils.Getenv("MQ_ADDR", ":9000")
	engine := mq.NewEngine(mq.Config{MaxRetries: 5, RetryBackoff: 5 * time.Millisecond})

	ctx, stop := signal.NotifyContext(parent, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	srv := mq.NewServer(ctx, engine, log)

	serverErr := make(chan error, 1)
	go func() {
		log.Info("starting broker", "addr", addr)
		if err := srv.ListenAndServe(addr); err != nil {
			log.Error("broker stopped", "err", err)
			serverErr <- err
			return
		}
		serverErr <- nil
	}()

	select {
	case <-ctx.Done():
		log.Info("shutdown signal received")
	case err := <-serverErr:
		if err != nil {
			log.Error("broker stopped unexpectedly", "err", err)
			_ = srv.Close()
			return err
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = shutdownCtx

	log.Info("shutdown signal received")
	_ = srv.Close()
	log.Info("broker shutdown complete")
	return nil
}
