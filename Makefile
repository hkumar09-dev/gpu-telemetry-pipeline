.PHONY: test coverage coverage-html openapi build docker-build tidy

COVERPKG := ./internal/...

test:
	go test ./...

coverage:
	go test -coverprofile=coverage.out -covermode=atomic -coverpkg=$(COVERPKG) ./...
	go tool cover -func=coverage.out

coverage-html: coverage
	go tool cover -html=coverage.out -o coverage.html

openapi:
	mkdir -p api
	go run ./cmd/openapi-gen api/openapi.yaml

build:
	mkdir -p bin
	go build -o bin/broker ./cmd/broker
	go build -o bin/streamer ./cmd/streamer
	go build -o bin/collector ./cmd/collector
	go build -o bin/gateway ./cmd/gateway

tidy:
	go mod tidy

docker-build:
	docker build --build-arg SERVICE=broker -t gpu-telemetry/broker:local .
	docker build --build-arg SERVICE=streamer -t gpu-telemetry/streamer:local .
	docker build --build-arg SERVICE=collector -t gpu-telemetry/collector:local .
	docker build --build-arg SERVICE=gateway -t gpu-telemetry/gateway:local .
