package main

import (
	"os"

	"github.com/gpu-telemetry-pipeline/internal/api"
)

var osExit = os.Exit

func main() {
	if err := generate(os.Args); err != nil {
		osExit(1)
	}
}

func generate(args []string) error {
	path := "api/openapi.yaml"
	if len(args) > 1 {
		path = args[1]
	}
	return os.WriteFile(path, []byte(api.OpenAPISpec), 0o644)
}
