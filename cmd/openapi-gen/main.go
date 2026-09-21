package main

import (
	"os"

	"github.com/gpu-telemetry-pipeline/internal/api"
)

func main() {
	path := "api/openapi.yaml"
	if len(os.Args) > 1 {
		path = os.Args[1]
	}
	if err := os.WriteFile(path, []byte(api.OpenAPISpec), 0o644); err != nil {
		panic(err)
	}
}
