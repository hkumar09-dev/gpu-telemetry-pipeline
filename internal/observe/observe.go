package observe

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/gpu-telemetry-pipeline/internal/metrics"
)

// Status is a process-local readiness flag.
type Status struct {
	ready atomic.Bool
}

func (s *Status) SetReady(v bool) { s.ready.Store(v) }
func (s *Status) Ready() bool     { return s.ready.Load() }

// Handler serves /metrics, /health, /ready (and /healthz, /livez aliases).
func Handler(ready func() bool) http.Handler {
	if ready == nil {
		ready = func() bool { return true }
	}
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", metrics.Handler())
	mux.HandleFunc("GET /health", liveness)
	mux.HandleFunc("GET /health/", liveness)
	mux.HandleFunc("GET /healthz", liveness)
	mux.HandleFunc("GET /healthz/", liveness)
	mux.HandleFunc("GET /livez", liveness)
	mux.HandleFunc("GET /livez/", liveness)
	mux.HandleFunc("GET /ready", readiness(ready))
	mux.HandleFunc("GET /ready/", readiness(ready))
	return mux
}

func liveness(w http.ResponseWriter, _ *http.Request) {
	writeStatus(w, http.StatusOK, "ok")
}

func readiness(ready func() bool) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if !ready() {
			writeStatus(w, http.StatusServiceUnavailable, "not ready")
			return
		}
		writeStatus(w, http.StatusOK, "ok")
	}
}

func writeStatus(w http.ResponseWriter, code int, status string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": status})
}

// Listen starts a sidecar HTTP server. Empty or "-" addr is a no-op.
// Call the returned shutdown function with a timeout context from the process main.
func Listen(_ context.Context, addr string, ready func() bool, log *slog.Logger) (func(context.Context) error, error) {
	if addr == "" || addr == "-" {
		return func(context.Context) error { return nil }, nil
	}
	if log == nil {
		log = slog.Default()
	}
	srv := &http.Server{
		Addr:              addr,
		Handler:           Handler(ready),
		ReadHeaderTimeout: 5 * time.Second,
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	go func() {
		log.Info("metrics listening", "addr", ln.Addr().String())
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Error("metrics server", "err", err)
		}
	}()
	return func(shctx context.Context) error {
		return HTTPShutdown(shctx, srv)
	}, nil
}

// HTTPShutdown drains the HTTP server. Close is only used if Shutdown hits the deadline.
func HTTPShutdown(ctx context.Context, srv *http.Server) error {
	if srv == nil {
		return nil
	}
	err := srv.Shutdown(ctx)
	if err == nil || errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		_ = srv.Close()
		return err
	}
	_ = srv.Close()
	return err
}
