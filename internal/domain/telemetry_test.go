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
	if err := (Telemetry{}).Validate(); err == nil {
		t.Fatal("uuid required")
	}
	if err := (Telemetry{UUID: "id"}).Validate(); err == nil {
		t.Fatal("metric required")
	}
	if err := (Telemetry{UUID: "id", MetricName: "m"}).Validate(); err == nil {
		t.Fatal("time required")
	}
	early := ts.Add(-time.Second)
	startOnly := TimeWindow{Start: &ts}
	if startOnly.Contains(early) {
		t.Fatal("before start")
	}
	open := TimeWindow{}
	if !open.Contains(ts) {
		t.Fatal("empty window")
	}
	late := ts.Add(2 * time.Second)
	closed := TimeWindow{End: &end}
	if closed.Contains(late) {
		t.Fatal("should exclude")
	}
}
