package utils

import (
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultMQListenAddr     = ":9000"
	DefaultMQClientAddr     = "127.0.0.1:9000"
	DefaultMQTopic          = "gpu-telemetry"
	DefaultMQGroup          = "collectors"
	DefaultMQPartitions     = 8
	DefaultMQMaxQueueSize   = 10_000
	DefaultMQAckTimeout     = 30 * time.Second
	DefaultMQRetryLimit     = 5
	DefaultMQRetryBackoff   = 5 * time.Millisecond
	DefaultMQRetryQueueSize = 1024
	DefaultHTTPAddr         = ":8080"
	DefaultGatewayURL       = "http://127.0.0.1:8080"
	DefaultDatabaseURL      = "./tmp/telemetry.json"
	DefaultDBMaxOpenConns   = 4
	DefaultDBMaxIdleConns   = 2
	DefaultDBMaxRecords     = 20_000
	DefaultCSVFile          = "./data/dcgm_metrics.csv"
	DefaultStreamInterval   = 10 * time.Millisecond
	DefaultLogLevel         = "info"
	DefaultBrokerMetrics    = ":9091"
	DefaultStreamerMetrics  = ":9092"
	DefaultCollectorMetrics = ":9093"
	DefaultShutdownTimeout  = 10 * time.Second
)

func FirstEnv(def string, keys ...string) string {
	for _, k := range keys {
		if v := os.Getenv(k); v != "" {
			return v
		}
	}
	return def
}

func EnvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func EnvDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return def
	}
	return d
}

func EnvBool(key string, def bool) bool {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	switch strings.ToLower(v) {
	case "1", "true", "t", "yes", "y", "on":
		return true
	case "0", "false", "f", "no", "n", "off":
		return false
	default:
		return def
	}
}

func StorePath() string {
	raw := FirstEnv(DefaultDatabaseURL, "DATABASE_URL", "DB_PATH")
	return strings.TrimPrefix(raw, "file://")
}

func CSVFile() string {
	return FirstEnv(DefaultCSVFile, "CSV_FILE", "CSV_PATH")
}

func LogLevel() slog.Level {
	switch strings.ToLower(strings.TrimSpace(FirstEnv(DefaultLogLevel, "LOG_LEVEL"))) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func ShutdownTimeout() time.Duration {
	return FirstDuration(DefaultShutdownTimeout, "SHUTDOWN_TIMEOUT", "HTTP_SHUTDOWN_TIMEOUT", "MQ_SHUTDOWN_TIMEOUT")
}

func FirstDuration(def time.Duration, keys ...string) time.Duration {
	for _, k := range keys {
		if os.Getenv(k) != "" {
			return EnvDuration(k, def)
		}
	}
	return def
}

func NewJSONLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: LogLevel()}))
}
