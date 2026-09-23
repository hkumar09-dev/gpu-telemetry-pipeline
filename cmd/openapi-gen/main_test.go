package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/gpu-telemetry-pipeline/internal/api"
)

func TestGenerateDefaultAndCustomPath(t *testing.T) {
	dir := t.TempDir()
	custom := filepath.Join(dir, "openapi.yaml")
	if err := generate([]string{"openapi-gen", custom}); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(custom)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != api.OpenAPISpec {
		t.Fatal("spec mismatch")
	}
}

func TestGenerateDefaultPath(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	if err := os.MkdirAll("api", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := generate([]string{"openapi-gen"}); err != nil {
		t.Fatal(err)
	}
}

func TestGenerateWriteError(t *testing.T) {
	if err := generate([]string{"openapi-gen", filepath.Join(t.TempDir(), "no", "such", "openapi.yaml")}); err == nil {
		t.Fatal("expected error")
	}
}

func TestMainExitOnError(t *testing.T) {
	code := -1
	osExit = func(c int) { code = c }
	t.Cleanup(func() { osExit = os.Exit })
	old := os.Args
	os.Args = []string{"openapi-gen", filepath.Join(t.TempDir(), "missing", "x.yaml")}
	defer func() { os.Args = old }()
	main()
	if code != 1 {
		t.Fatalf("exit %d", code)
	}
}

func TestMainSuccess(t *testing.T) {
	osExit = func(int) { t.Error("os.Exit") }
	t.Cleanup(func() { osExit = os.Exit })
	old := os.Args
	path := filepath.Join(t.TempDir(), "openapi.yaml")
	os.Args = []string{"openapi-gen", path}
	defer func() { os.Args = old }()
	main()
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
}
