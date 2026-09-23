.PHONY: test test-race coverage coverage-html openapi build docker-build tidy helm-deploy helm-delete

COVERPKG := ./...

IMAGE_TAG ?= local
RELEASE_NAME := gpu
NAMESPACE := gpu-telemetry
CHART := deploy/helm/gpu-telemetry

test:
	go test ./...

test-race:
	go test -race ./...

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


IMAGES := \
	gpu-telemetry/broker:$(IMAGE_TAG) \
	gpu-telemetry/streamer:$(IMAGE_TAG) \
	gpu-telemetry/collector:$(IMAGE_TAG) \
	gpu-telemetry/gateway:$(IMAGE_TAG)

docker-build:
	@for img in $(IMAGES); do \
		svc=$${img##*/}; \
		svc=$${svc%%:*}; \
		docker build --build-arg SERVICE=$$svc -t $$img .; \
	done

helm-deploy: docker-build
	@set -e; \
	CLUSTER_NAME="gpu-telemetry"; \
	\
	if ! kind get clusters 2>/dev/null | grep -qx "$$CLUSTER_NAME"; then \
		echo "Creating Kind cluster: $$CLUSTER_NAME"; \
		kind create cluster --name "$$CLUSTER_NAME"; \
	else \
		echo "Kind cluster already exists: $$CLUSTER_NAME"; \
	fi; \
	\
	kubectl config use-context "kind-$$CLUSTER_NAME"; \
	echo "Loading images into Kind..."; \
	kind load docker-image --name "$$CLUSTER_NAME" $(IMAGES); \
	\
	echo "Deploying $(RELEASE_NAME)..."; \
	helm upgrade --install $(RELEASE_NAME) $(CHART) \
		--namespace $(NAMESPACE) \
		--create-namespace \
		--wait \
		--timeout 3m; \
	\
	kubectl -n $(NAMESPACE) get pods,svc

helm-delete:
	helm uninstall $(RELEASE_NAME) --namespace $(NAMESPACE) --wait || true

forward:
	kubectl -n $(NAMESPACE) port-forward service/gpu-gateway 8080:8080