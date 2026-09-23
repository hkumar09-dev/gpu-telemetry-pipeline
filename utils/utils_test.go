package utils

import "testing"

func TestGetenv(t *testing.T) {
	t.Setenv("GPU_TELEMETRY_TEST_KEY", "set")
	if Getenv("GPU_TELEMETRY_TEST_KEY", "def") != "set" {
		t.Fatal("env")
	}
	if Getenv("GPU_TELEMETRY_TEST_MISSING", "def") != "def" {
		t.Fatal("default")
	}
}
