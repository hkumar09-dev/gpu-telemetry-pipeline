package csvsource

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gpu-telemetry-pipeline/internal/domain"
)

const expectedColumns = 12

// FileSource loads DCGM CSV rows. Original CSV timestamps are ignored; callers
// stamp ProcessedAt when a row is actually streamed (per the project spec).
type FileSource struct {
	Path string
}

type job struct {
	line int
	rec  []string
}

type result struct {
	line  int
	t     domain.Telemetry
	err   error
	fatal bool
}

func (s FileSource) Load(ctx context.Context) ([]domain.Telemetry, error) {
	f, err := os.Open(s.Path)
	if err != nil {
		return nil, fmt.Errorf("open csv: %w", err)
	}
	defer f.Close()
	return Parse(ctx, f)
}

func Parse(ctx context.Context, r io.Reader) ([]domain.Telemetry, error) {
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

	workers := runtime.NumCPU()

	jobs := make(chan job, workers*2)
	results := make(chan result, workers*2)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	// Start worker goroutines.
	for i := 0; i < workers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for {
				select {
				case <-ctx.Done():
					return

				case j, ok := <-jobs:
					if !ok {
						return
					}

					func() {
						defer func() {
							if rec := recover(); rec != nil {
								select {
								case results <- result{line: j.line, err: fmt.Errorf("csv line %d: %v", j.line, rec)}:
								case <-ctx.Done():
								}
							}
						}()

						t, err := rowToTelemetry(j.rec)
						if err != nil {
							select {
							case results <- result{line: j.line, err: fmt.Errorf("csv line %d: %w", j.line, err)}:
							case <-ctx.Done():
							}
							return
						}

						select {
						case results <- result{line: j.line, t: t}:
						case <-ctx.Done():
						}
					}()
				}
			}
		}()
	}

	// Read rows from the CSV file and send them to the worker goroutines.
	go func() {
		defer close(jobs)
		line := 1
		for {
			rec, err := cr.Read()

			if err == io.EOF {
				return
			}
			line++
			if err != nil {
				if _, ok := err.(*csv.ParseError); ok {
					continue
				}
				select {
				case results <- result{
					line:  line,
					err:   fmt.Errorf("csv line %d: %w", line, err),
					fatal: true,
				}:
				case <-ctx.Done():
				}
				return
			}
			if len(rec) < expectedColumns {
				continue
			}
			select {
			case jobs <- job{line: line, rec: rec}:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Wait for all worker goroutines to finish.
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results from the worker goroutines.
	out := make([]domain.Telemetry, 0)

	for result := range results {
		if result.fatal {
			cancel()
			return nil, result.err
		}
		if result.err != nil {
			continue
		}
		if err := ctx.Err(); err != nil {
			return out, fmt.Errorf("parse csv: %w", err)
		}
		out = append(out, result.t)
	}

	return out, nil
}

// validateHeader validates the CSV header.
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
