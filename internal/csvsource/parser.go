package csvsource

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gpu-telemetry-pipeline/internal/domain"
)

const expectedColumns = 12

// FileSource loads DCGM CSV rows. Original CSV timestamps are ignored; callers
// stamp ProcessedAt when a row is actually streamed (per the project spec).
type FileSource struct {
	Path string
}

func (s FileSource) Load() ([]domain.Telemetry, error) {
	f, err := os.Open(s.Path)
	if err != nil {
		return nil, fmt.Errorf("open csv: %w", err)
	}
	defer f.Close()
	return Parse(f)
}

func Parse(r io.Reader) ([]domain.Telemetry, error) {
	cr := csv.NewReader(r)
	cr.FieldsPerRecord = -1
	cr.LazyQuotes = true

	header, err := cr.Read()
	if err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	if err := validateHeader(header); err != nil {
		return nil, err
	}

	var out []domain.Telemetry
	line := 1
	for {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("csv line %d: %w", line+1, err)
		}
		line++
		if len(rec) < expectedColumns {
			return nil, fmt.Errorf("csv line %d: expected %d columns, got %d", line, expectedColumns, len(rec))
		}
		t, err := rowToTelemetry(rec)
		if err != nil {
			return nil, fmt.Errorf("csv line %d: %w", line, err)
		}
		out = append(out, t)
	}
	return out, nil
}

func validateHeader(header []string) error {
	want := []string{
		"timestamp", "metric_name", "gpu_id", "device", "uuid", "modelName",
		"Hostname", "container", "pod", "namespace", "value", "labels_raw",
	}
	if len(header) < len(want) {
		return fmt.Errorf("unexpected csv header: %v", header)
	}
	for i, name := range want {
		if strings.TrimSpace(header[i]) != name {
			return fmt.Errorf("unexpected csv header column %d: got %q want %q", i, header[i], name)
		}
	}
	return nil
}

func rowToTelemetry(rec []string) (domain.Telemetry, error) {
	value, err := strconv.ParseFloat(strings.TrimSpace(rec[10]), 64)
	if err != nil {
		return domain.Telemetry{}, fmt.Errorf("parse value %q: %w", rec[10], err)
	}
	return domain.Telemetry{
		MetricName: strings.TrimSpace(rec[1]),
		GPUIndex:   strings.TrimSpace(rec[2]),
		Device:     strings.TrimSpace(rec[3]),
		UUID:       strings.TrimSpace(rec[4]),
		ModelName:  strings.TrimSpace(rec[5]),
		Hostname:   strings.TrimSpace(rec[6]),
		Container:  strings.TrimSpace(rec[7]),
		Pod:        strings.TrimSpace(rec[8]),
		Namespace:  strings.TrimSpace(rec[9]),
		Value:      value,
		LabelsRaw:  strings.TrimSpace(rec[11]),
		// ProcessedAt is set by the streamer at emit time.
		ProcessedAt: time.Time{},
	}, nil
}
