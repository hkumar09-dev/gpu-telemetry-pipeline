.PHONY: test coverage coverage-html openapi build docker-build tidy helm-deploy helm-delete

COVERPKG := ./internal/...

IMAGE_TAG ?= local
RELEASE_NAME := gpu
NAMESPACE := gpu-telemetry
CHART := deploy/helm/gpu-telemetry

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
	ctx="$$(kubectl config current-context)"; \
	echo "Kubernetes context: $$ctx"; \
	echo "Loading images..."; \
	if [ "$$ctx" = "minikube" ]; then \
		for image in $(IMAGES); do \
			minikube image load "$$image"; \
		done; \
	elif echo "$$ctx" | grep -q '^kind-'; then \
		kind load docker-image --name "$${ctx#kind-}" $(IMAGES); \
	else \
		echo "Using current cluster $$ctx (not loading local images)"; \
	fi; \
	helm upgrade --install $(RELEASE_NAME) $(CHART) \
		--namespace $(NAMESPACE) \
		--create-namespace \
		--wait \
		--timeout 3m; \
	kubectl -n $(NAMESPACE) get pods,svc

helm-delete:
	helm uninstall $(RELEASE_NAME) --namespace $(NAMESPACE) --wait || true
