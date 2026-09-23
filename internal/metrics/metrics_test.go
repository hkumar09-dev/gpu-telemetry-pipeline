package metrics

import (
	"testing"
)

func TestSetQueueDepthAndHandler(t *testing.T) {
	SetQueueDepth("", 3)
	SetQueueDepth("gpu-telemetry", 1)
	MQPublished.Inc()
	if Handler() == nil {
		t.Fatal("handler")
	}
}
