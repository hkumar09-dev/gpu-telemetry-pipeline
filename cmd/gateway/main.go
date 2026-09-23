package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gpu-telemetry-pipeline/internal/api"
	"github.com/gpu-telemetry-pipeline/internal/storage"
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
	db, err := storage.Open(utils.Getenv("DB_PATH", "/var/lib/gpu-telemetry/telemetry.json"))
	if err != nil {
		log.Error("open db", "err", err)
		return err
	}
	defer db.Close()

	svc := api.NewService(db, log)
	srv := &http.Server{
		Addr:              utils.Getenv("HTTP_ADDR", ":8080"),
		Handler:           api.NewHandler(svc),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, cancel := signal.NotifyContext(parent, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	go func() {
		<-ctx.Done()
		shctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = srv.Shutdown(shctx)
	}()

	log.Info("gateway listening", "addr", srv.Addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Error("gateway failed", "err", err)
		return err
	}
	return nil
}
