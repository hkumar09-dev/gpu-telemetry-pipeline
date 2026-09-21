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
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	addr := utils.Getenv("MQ_ADDR", ":9000")
	engine := mq.NewEngine(mq.Config{})
	srv := mq.NewServer(engine, log)

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
	//_ = ctx
	//
	//if err := srv.Shutdown(ctx); err != nil {
	//	log.Printf("graceful shutdown failed: %v", err)
	//
	//	// Force close if graceful shutdown exceeds timeout
	//	_ = srv.Close()
	//}

	wait()
	_ = srv.Close(ctx)
}

func wait() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
}
