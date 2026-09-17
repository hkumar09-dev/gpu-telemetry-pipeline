package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/himanshubh/gpu-telemetry-pipeline/internal/domain"
)

// HTTPWriter posts telemetry to the gateway ingest endpoint.
type HTTPWriter struct {
	BaseURL    string
	Client     *http.Client
	Path       string
}

func NewHTTPWriter(baseURL string) *HTTPWriter {
	return &HTTPWriter{
		BaseURL: baseURL,
		Path:    "/internal/v1/telemetry",
		Client:  &http.Client{Timeout: 10 * time.Second},
	}
}

func (w *HTTPWriter) Write(ctx context.Context, t domain.Telemetry) error {
	body, err := json.Marshal(t)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.BaseURL+w.Path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := w.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("ingest status %d", resp.StatusCode)
	}
	return nil
}
