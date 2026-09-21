.PHONY: test coverage coverage-html openapi build docker-build tidy k8s-deploy k8s-delete

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

k8s-deploy: docker-build
	minikube image load gpu-telemetry/broker:local
	minikube image load gpu-telemetry/streamer:local
	minikube image load gpu-telemetry/collector:local
	minikube image load gpu-telemetry/gateway:local
	kubectl apply -k deploy/k8s
	kubectl -n gpu-telemetry rollout status deploy/gpu-broker --timeout=180s
	kubectl -n gpu-telemetry rollout status deploy/gpu-gateway --timeout=180s
	kubectl -n gpu-telemetry rollout status deploy/gpu-collector --timeout=180s
	kubectl -n gpu-telemetry rollout status statefulset/gpu-streamer --timeout=180s

k8s-delete:
	kubectl delete -k deploy/k8s --ignore-not-found
