package telemetry

import (
	"testing"
	"time"
)

func TestTelemetryFields(t *testing.T) {
	ts := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	m := Telemetry{
		HostID:         "h1",
		GPUUUID:        "GPU-1",
		GPUTemperature: 70,
		GPUUtilization: 80,
		GPUMemoryUsed:  1,
		GPUMemoryTotal: 2,
		Timestamp:      ts,
	}
	if m.HostID != "h1" || m.GPUUUID != "GPU-1" || m.Timestamp.IsZero() {
		t.Fatalf("%+v", m)
	}
}
