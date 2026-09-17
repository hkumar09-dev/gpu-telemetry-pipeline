package domain

import (
	"testing"
	"time"
)

func TestValidateAndWindow(t *testing.T) {
	ts := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tel := Telemetry{UUID: "id", MetricName: "m", ProcessedAt: ts}
	if err := tel.Validate(); err != nil {
		t.Fatal(err)
	}
	if tel.GPU().ID != "id" {
		t.Fatal(tel.GPU())
	}
	start := ts.Add(-time.Second)
	end := ts.Add(time.Second)
	window := TimeWindow{Start: &start, End: &end}
	if !window.Contains(ts) {
		t.Fatal("should contain")
	}
	late := ts.Add(2 * time.Second)
	closed := TimeWindow{End: &end}
	if closed.Contains(late) {
		t.Fatal("should exclude")
	}
}
