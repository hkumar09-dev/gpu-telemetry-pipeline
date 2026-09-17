package csvsource

import (
	"strings"
	"testing"
)

func TestParseSample(t *testing.T) {
	rows, err := Parse(strings.NewReader(sampleCSV))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d rows", len(rows))
	}
	if rows[0].UUID != "GPU-aaa" || rows[0].Value != 10 {
		t.Fatalf("row0 %+v", rows[0])
	}
	if !rows[0].ProcessedAt.IsZero() {
		t.Fatal("csv timestamp must not be used as processed_at")
	}
}

func TestParseBadHeader(t *testing.T) {
	_, err := Parse(strings.NewReader("foo,bar\n1,2\n"))
	if err == nil {
		t.Fatal("expected header error")
	}
}

func TestParseBadValue(t *testing.T) {
	csv := strings.Replace(sampleCSV, `"10"`, `"nope"`, 1)
	_, err := Parse(strings.NewReader(csv))
	if err == nil {
		t.Fatal("expected value error")
	}
}

func TestFileSource(t *testing.T) {
	src := FileSource{Path: "../../testdata/sample.csv"}
	rows, err := src.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) < 10 {
		t.Fatalf("expected sample rows, got %d", len(rows))
	}
}

const sampleCSV = `timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,container,pod,namespace,value,labels_raw
"2025-07-18T20:42:34Z","DCGM_FI_DEV_GPU_UTIL","0","nvidia0","GPU-aaa","NVIDIA H100","host-a","","","","10","x"
"2025-07-18T20:42:34Z","DCGM_FI_DEV_GPU_UTIL","1","nvidia1","GPU-bbb","NVIDIA H100","host-a","","","","20","y"
`
