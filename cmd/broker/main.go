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
	engine := mq.NewEngine(mq.Config{})
	ctx := context.Background()
	srv := mq.NewServer(ctx, engine, log)

	go func() {
		if err := srv.ListenAndServe(addr); err != nil {
			log.Error("broker stopped", "err", err)
			os.Exit(1)
		}
	}()

	// Wait for SIGTERM / SIGINT
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

	<-stop

	log.Info("shutdown signal received")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	wait()
	_ = srv.Close()
}

// Wait for SIGTERM / SIGINT
func wait() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
}
