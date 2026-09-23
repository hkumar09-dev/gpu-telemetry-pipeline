package utils

import (
	"log/slog"
	"testing"
	"time"
)

func TestFirstEnvAndParsers(t *testing.T) {
	t.Setenv("GPU_A", "one")
	if FirstEnv("def", "GPU_A", "GPU_B") != "one" {
		t.Fatal("first")
	}
	t.Setenv("GPU_A", "")
	t.Setenv("GPU_B", "two")
	if FirstEnv("def", "GPU_A", "GPU_B") != "two" {
		t.Fatal("second")
	}
	t.Setenv("GPU_B", "")
	if FirstEnv("def", "GPU_A", "GPU_B") != "def" {
		t.Fatal("default")
	}

	t.Setenv("GPU_INT", "7")
	if EnvInt("GPU_INT", 1) != 7 {
		t.Fatal("int")
	}
	t.Setenv("GPU_INT", "nope")
	if EnvInt("GPU_INT", 1) != 1 {
		t.Fatal("int default")
	}
	if EnvInt("GPU_INT_MISSING", 3) != 3 {
		t.Fatal("int missing")
	}

	t.Setenv("GPU_DUR", "15ms")
	if EnvDuration("GPU_DUR", time.Second) != 15*time.Millisecond {
		t.Fatal("dur")
	}
	t.Setenv("GPU_DUR", "nope")
	if EnvDuration("GPU_DUR", time.Second) != time.Second {
		t.Fatal("dur default")
	}

	t.Setenv("GPU_BOOL", "true")
	if !EnvBool("GPU_BOOL", false) {
		t.Fatal("bool true")
	}
	t.Setenv("GPU_BOOL", "0")
	if EnvBool("GPU_BOOL", true) {
		t.Fatal("bool false")
	}
	t.Setenv("GPU_BOOL", "maybe")
	if !EnvBool("GPU_BOOL", true) {
		t.Fatal("bool invalid")
	}
}

func TestStorePathAndCSVFile(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("DB_PATH", "")
	t.Setenv("CSV_FILE", "")
	t.Setenv("CSV_PATH", "")
	if StorePath() != DefaultDatabaseURL {
		t.Fatalf("store default %q", StorePath())
	}
	t.Setenv("DATABASE_URL", "file:///var/lib/gpu-telemetry/telemetry.json")
	if StorePath() != "/var/lib/gpu-telemetry/telemetry.json" {
		t.Fatal("file url")
	}
	t.Setenv("DATABASE_URL", "")
	t.Setenv("DB_PATH", "./tmp/alt.json")
	if StorePath() != "./tmp/alt.json" {
		t.Fatal("db path")
	}

	if CSVFile() != DefaultCSVFile {
		t.Fatal("csv default")
	}
	t.Setenv("CSV_FILE", "/data/dcgm_metrics.csv")
	if CSVFile() != "/data/dcgm_metrics.csv" {
		t.Fatal("csv file")
	}
}

func TestLogLevel(t *testing.T) {
	t.Setenv("LOG_LEVEL", "debug")
	if LogLevel() != slog.LevelDebug {
		t.Fatal("debug")
	}
	t.Setenv("LOG_LEVEL", "ERROR")
	if LogLevel() != slog.LevelError {
		t.Fatal("error")
	}
	t.Setenv("LOG_LEVEL", "warn")
	if LogLevel() != slog.LevelWarn {
		t.Fatal("warn")
	}
	t.Setenv("LOG_LEVEL", "")
	if LogLevel() != slog.LevelInfo {
		t.Fatal("info")
	}
	if NewJSONLogger() == nil {
		t.Fatal("logger")
	}
}

func TestShutdownTimeout(t *testing.T) {
	if ShutdownTimeout() != DefaultShutdownTimeout {
		t.Fatal("default")
	}
	t.Setenv("SHUTDOWN_TIMEOUT", "3s")
	if ShutdownTimeout() != 3*time.Second {
		t.Fatal("env")
	}
}
