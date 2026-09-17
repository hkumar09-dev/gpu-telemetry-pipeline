package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/himanshubh/gpu-telemetry-pipeline/internal/gateway"
	"github.com/himanshubh/gpu-telemetry-pipeline/internal/storage"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	db, err := storage.Open(getenv("DB_PATH", "/var/lib/gpu-telemetry/telemetry.json"))
	if err != nil {
		log.Error("open db", "err", err)
		os.Exit(1)
	}
	defer db.Close()

	api := gateway.New(db, log)
	srv := &http.Server{
		Addr:              getenv("HTTP_ADDR", ":8080"),
		Handler:           api.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
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
		os.Exit(1)
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
