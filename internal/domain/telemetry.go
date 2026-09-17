package domain

import (
	"fmt"
	"strings"
	"time"
)

// GPU uniquely identifies a device in the cluster. Local gpu_id values collide
// across hosts, so UUID is the canonical identifier exposed by the API.
type GPU struct {
	ID        string `json:"id"`
	GPUIndex  string `json:"gpu_index"`
	Device    string `json:"device"`
	ModelName string `json:"model_name"`
	Hostname  string `json:"hostname"`
}

// Telemetry is one independent datapoint from DCGM.
type Telemetry struct {
	ProcessedAt time.Time `json:"processed_at"`
	MetricName  string    `json:"metric_name"`
	GPUIndex    string    `json:"gpu_index"`
	Device      string    `json:"device"`
	UUID        string    `json:"uuid"`
	ModelName   string    `json:"model_name"`
	Hostname    string    `json:"hostname"`
	Container   string    `json:"container,omitempty"`
	Pod         string    `json:"pod,omitempty"`
	Namespace   string    `json:"namespace,omitempty"`
	Value       float64   `json:"value"`
	LabelsRaw   string    `json:"labels_raw,omitempty"`
}

func (t Telemetry) GPU() GPU {
	return GPU{
		ID:        t.UUID,
		GPUIndex:  t.GPUIndex,
		Device:    t.Device,
		ModelName: t.ModelName,
		Hostname:  t.Hostname,
	}
}

func (t Telemetry) Validate() error {
	if strings.TrimSpace(t.UUID) == "" {
		return fmt.Errorf("telemetry uuid is required")
	}
	if strings.TrimSpace(t.MetricName) == "" {
		return fmt.Errorf("telemetry metric_name is required")
	}
	if t.ProcessedAt.IsZero() {
		return fmt.Errorf("telemetry processed_at is required")
	}
	return nil
}

// TimeWindow is an inclusive filter used by the query API.
type TimeWindow struct {
	Start *time.Time
	End   *time.Time
}

func (w TimeWindow) Contains(ts time.Time) bool {
	if w.Start != nil && ts.Before(w.Start.UTC()) {
		return false
	}
	if w.End != nil && ts.After(w.End.UTC()) {
		return false
	}
	return true
}
