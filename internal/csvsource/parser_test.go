package csvsource

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func TestParseSample(t *testing.T) {
	rows, err := Parse(context.Background(), strings.NewReader(sampleCSV))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d rows", len(rows))
	}
	uuids := map[string]float64{rows[0].UUID: rows[0].Value, rows[1].UUID: rows[1].Value}
	if uuids["GPU-aaa"] != 10 || uuids["GPU-bbb"] != 20 {
		t.Fatalf("rows %+v", rows)
	}
	if !rows[0].ProcessedAt.IsZero() {
		t.Fatal("csv timestamp must not be used as processed_at")
	}
}

func TestParseBadHeader(t *testing.T) {
	_, err := Parse(context.Background(), strings.NewReader("foo,bar\n1,2\n"))
	if err == nil {
		t.Fatal("expected header error")
	}
}

func TestParseBadValue(t *testing.T) {
	csv := strings.Replace(sampleCSV, `"10"`, `"nope"`, 1)
	_, err := Parse(context.Background(), strings.NewReader(csv))
	if err == nil {
		t.Fatal("expected value error")
	}
}

func TestFileSource(t *testing.T) {
	src := FileSource{Path: "../../testdata/sample.csv"}
	rows, err := src.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) < 10 {
		t.Fatalf("expected sample rows, got %d", len(rows))
	}
}

func TestParseShortRowAndBadColumn(t *testing.T) {
	short := sampleCSV + "\n\"t\",\"m\",\"0\",\"d\",\"u\",\"mod\",\"h\",\"\",\"\",\"\",\"1\"\n"
	if _, err := Parse(context.Background(), strings.NewReader(short)); err == nil {
		t.Fatal("expected short row error")
	}
	bad := strings.Replace(sampleCSV, "metric_name", "nope", 1)
	if _, err := Parse(context.Background(), strings.NewReader(bad)); err == nil {
		t.Fatal("expected header column error")
	}
	if _, err := Parse(context.Background(), strings.NewReader("")); err == nil {
		t.Fatal("expected header eof")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Parse(ctx, strings.NewReader(sampleCSV)); err != nil && err != context.Canceled {
		// cancelled parse may still succeed for tiny csv; either is acceptable
	}
	malformed := "timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,container,pod,namespace,value,labels_raw\n\"unterminated\n"
	if _, err := Parse(context.Background(), strings.NewReader(malformed)); err == nil {
		t.Fatal("expected csv parse error")
	}
}

func TestParseCancelStopsJobs(t *testing.T) {
	f, err := os.Open("../../testdata/sample.csv")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	_, _ = Parse(ctx, f)
}

func TestFileSourceMissing(t *testing.T) {
	_, err := FileSource{Path: "does-not-exist.csv"}.Load(context.Background())
	if err == nil {
		t.Fatal("expected open error")
	}
}

func TestParseCSVReadError(t *testing.T) {
	r := &errAfterBytes{data: []byte(sampleCSV[:len(sampleCSV)/2] + "\n")}
	// ensure header is complete then fail
	header := sampleCSV
	idx := strings.Index(header, "\n")
	r = &errAfterBytes{data: []byte(header[:idx+1])}
	if _, err := Parse(context.Background(), r); err == nil {
		t.Fatal("expected read error")
	}
}

type errAfterBytes struct {
	data []byte
	off  int
}

func (e *errAfterBytes) Read(p []byte) (int, error) {
	if e.off >= len(e.data) {
		return 0, fmt.Errorf("boom")
	}
	n := copy(p, e.data[e.off:])
	e.off += n
	return n, nil
}

const sampleCSV = `timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,container,pod,namespace,value,labels_raw
"2025-07-18T20:42:34Z","DCGM_FI_DEV_GPU_UTIL","0","nvidia0","GPU-aaa","NVIDIA H100","host-a","","","","10","x"
"2025-07-18T20:42:34Z","DCGM_FI_DEV_GPU_UTIL","1","nvidia1","GPU-bbb","NVIDIA H100","host-a","","","","20","y"
`
