package api

import (
	"testing"
)

func TestOpenAPISpec(t *testing.T) {
	if OpenAPISpec == "" {
		t.Fatal("empty spec")
	}
	if want := "openapi: 3.0.3"; len(OpenAPISpec) < len(want) || OpenAPISpec[:len(want)] != want {
		t.Fatalf("unexpected spec prefix %q", OpenAPISpec[:20])
	}
}
