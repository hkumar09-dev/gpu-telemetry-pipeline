package main

import (
	"os"

	"github.com/gpu-telemetry-pipeline/internal/gateway"
)

func main() {
	path := "api/openapi.yaml"
	if len(os.Args) > 1 {
		path = os.Args[1]
	}
	if err := os.WriteFile(path, []byte(gateway.OpenAPISpec), 0o644); err != nil {
		panic(err)
	}
}
