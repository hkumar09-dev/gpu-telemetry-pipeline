# Elastic GPU Telemetry Pipeline

Custom message-queue pipeline for DCGM GPU metrics: CSV streamers publish into a purpose-built broker, collectors persist datapoints, and an API gateway serves them.

GPU identity in the HTTP API is the device **UUID**. Local `gpu_id` values (`0`–`7`) collide across hosts.

## Architecture

```
                 ┌─────────────┐
   CSV shard ──► │  Streamer   │──publish(key=UUID)──┐
   (StatefulSet) └─────────────┘                     │
                 ┌─────────────┐                     ▼
   CSV shard ──► │  Streamer   │────────────► ┌────────────┐
                 └─────────────┘              │   Broker   │  custom TCP MQ
                                              │ partitions │  consumer groups
                                              └─────┬──────┘
                                                    │ consume / ack
                 ┌─────────────┐                    │
                 │  Collector  │◄───────────────────┤
                 └──────┬──────┘                    │
                        │ HTTP ingest               │
                 ┌──────▼──────┐              ┌─────┴──────┐
                 │   Gateway   │◄─────────────│  Collector │
                 │ SQLite + API│              └────────────┘
                 └─────────────┘
```

### Components

| Process | Role |
| --- | --- |
| **broker** | In-process partitioned topic engine exposed over a length-prefixed JSON TCP protocol. Same GPU UUID always hashes to the same partition (ordering). Consumer groups rebalance partitions across collectors. Bounded partitions apply backpressure. Unacked messages are redelivered after `AckTimeout`. |
| **streamer** | Loads `data/dcgm_metrics.csv`, takes every `Count`th row (`Index`), stamps `processed_at` with **emit time** (not the CSV timestamp), loops the file. Scale by changing StatefulSet replicas. |
| **collector** | Competing consumer in group `collectors`. Parses JSON, validates, writes via HTTP to the gateway. Invalid payloads are nacked. |
| **gateway** | JSON-file repository, public REST API, and internal ingest. Serves generated OpenAPI at `/openapi.yaml`. |

Dependencies point at **ports** (`Publisher`, `Consumer`, `Repository`, `Writer`, `TelemetrySource`). SQLite, in-memory, HTTP writer, TCP client, and in-process engine adapters are interchangeable — DIP / ISP for tests.

### Design limits (exercise)

- Streamer/collector replicas stay ≤ 10 as specified.
- Broker is a single process (HA would add a replicated WAL + leader election; the engine is isolated so that can be added behind `Engine`).
- Gateway is the system of record (one replica + PVC). Collectors are stateless.

## API

- `GET /api/v1/gpus` — inventory
- `GET /api/v1/gpus/{id}/telemetry` — time-ordered points
- `GET /api/v1/gpus/{id}/telemetry?start_time=RFC3339&end_time=RFC3339` — inclusive window
- `GET /healthz`
- `GET /openapi.yaml`
- `POST /internal/v1/telemetry` — collector ingest (not a public product API)

`{id}` is the GPU UUID from the CSV `uuid` column.

## Build and test

Requires Go 1.22+.

```bash
make tidy
make test
make coverage          # prints coverage.out + function coverage
make coverage-html     # coverage.html
make openapi           # writes api/openapi.yaml
make build             # binaries in bin/
```

## Local run (no Kubernetes)

Terminal 1–4 from the repo root after `make build`:

```bash
./bin/broker
DB_PATH=./tmp/telemetry.json HTTP_ADDR=:8080 ./bin/gateway
MQ_ADDR=127.0.0.1:9000 GATEWAY_URL=http://127.0.0.1:8080 ./bin/collector
CSV_PATH=./data/dcgm_metrics.csv MQ_ADDR=127.0.0.1:9000 STREAM_INTERVAL=5ms ./bin/streamer
```

Sample calls:

```bash
curl -s localhost:8080/api/v1/gpus | head
curl -s "localhost:8080/api/v1/gpus/GPU-5fd4f087-86f3-7a43-b711-4771313afc50/telemetry?start_time=2026-01-01T00:00:00Z"
```

Docker Compose:

```bash
docker compose up --build --scale collector=2
```

## Kubernetes (Helm)

Requires Helm 3 and a Kubernetes cluster in `kubectl`. Local example with Kind:

```bash
kind create cluster --name gpu-telemetry
make helm-deploy
kubectl -n gpu-telemetry port-forward svc/gpu-gateway 8080:8080
curl -s localhost:8080/healthz
curl -s localhost:8080/api/v1/gpus | head
```

Scale:

```bash
helm upgrade gpu deploy/helm/gpu-telemetry --namespace gpu-telemetry \
  --reuse-values \
  --set replicaCount.streamer=4 \
  --set replicaCount.collector=3
```

Uninstall:

```bash
make helm-delete
```

Streamers take the StatefulSet ordinal from `POD_NAME` so shards stay disjoint. Collectors join the same consumer group with `CONSUMER_ID=pod name`.

## Layout

```
cmd/                 process entrypoints
internal/domain      entities
internal/ports       interfaces (SOLID)
internal/mq          custom broker engine + TCP
internal/streamer    CSV shard publisher
internal/collector   consumer + persist
internal/api         HTTP API
internal/storage     JSON file / memory / HTTP writer
data/                full DCGM CSV
deploy/helm          Helm chart (primary Kubernetes install)
```

## forward to localhost:
kubectl -n gpu-telemetry port-forward service/gpu-gateway 8080:8080
## AI assistance

See [docs/AI_ASSISTANCE.md](docs/AI_ASSISTANCE.md) for prompts, what the model generated, and where manual fixes were required.
