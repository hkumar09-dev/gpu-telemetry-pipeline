package api

import (
	"net/http"

	"github.com/gpu-telemetry-pipeline/internal/metrics"
)

type Handler struct {
	svc *Service
	mux http.Handler
}

func NewHandler(svc *Service) *Handler {
	h := &Handler{svc: svc}
	h.mux = h.routes()
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

func (h *Handler) routes() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", metrics.Handler())
	mux.HandleFunc("GET /health", h.svc.health)
	mux.HandleFunc("GET /health/", h.svc.health)
	mux.HandleFunc("GET /ready", h.svc.ready)
	mux.HandleFunc("GET /ready/", h.svc.ready)
	mux.HandleFunc("GET /healthz", h.svc.health)
	mux.HandleFunc("GET /healthz/{$}", h.svc.health)
	mux.HandleFunc("GET /livez", h.svc.health)
	mux.HandleFunc("GET /livez/{$}", h.svc.health)
	mux.HandleFunc("GET /openapi.yaml", h.svc.openapi)
	mux.HandleFunc("GET /api/openapi.yaml", h.svc.openapi)
	mux.HandleFunc("GET /api/v1/gpus", h.svc.listGPUs)
	mux.HandleFunc("GET /api/v1/gpus/{id}/telemetry", h.svc.queryTelemetry)
	mux.HandleFunc("POST /internal/v1/telemetry", h.svc.ingest)
	return instrument(h.svc.log, mux)
}
