package telemetry

import "time"

type Telemetry struct {
    HostID      string    `json:"host_id"`
    GPUUUID     string    `json:"gpu_uuid"`
    GPUTemperature float64 `json:"gpu_temperature"`
    GPUUtilization  float64 `json:"gpu_utilization"`
    GPUMemoryUsed   uint64  `json:"gpu_memory_used"`
    GPUMemoryTotal  uint64  `json:"gpu_memory_total"`

    Timestamp time.Time `json:"timestamp"`
}