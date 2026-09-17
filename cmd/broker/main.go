package main

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/himanshubh/gpu-telemetry-pipeline/internal/mq"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	addr := getenv("MQ_ADDR", ":9000")
	engine := mq.NewEngine(mq.Config{})
	srv := mq.NewServer(engine, log)

	go func() {
		if err := srv.ListenAndServe(addr); err != nil {
			log.Error("broker stopped", "err", err)
			os.Exit(1)
		}
	}()

	wait()
	_ = srv.Close()
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func wait() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
}
