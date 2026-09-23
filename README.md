# Elastic GPU Telemetry Pipeline

Custom message-queue pipeline for DCGM GPU metrics: CSV streamers publish into a purpose-built broker, collectors persist datapoints, and an API gateway serves them.

GPU identity in the HTTP API is the device **UUID**. Local `gpu_id` values (`0`–`7`) collide across hosts.

Requires Go 1.22+.

## 1. Project overview

This repository is a small, elastic telemetry pipeline:

1. **Streamers** read `data/dcgm_metrics.csv`, shard rows by replica index, stamp `processed_at` with emit time, and publish JSON to topic `gpu-telemetry`.
2. A **custom TCP broker** partitions by GPU UUID, assigns partitions to a consumer group, and redelivers unacked work.
3. **Collectors** consume from group `collectors`, validate payloads, and POST them to the gateway.
4. The **gateway** is the system of record (JSON file on disk) and exposes the public REST API on port **8080**.

Nothing in the hot path depends on Kafka, RabbitMQ, or another off-the-shelf queue. Dependencies in Go point at ports (`Publisher`, `Consumer`, `Repository`, `Writer`, `TelemetrySource`) so TCP, in-process engine, HTTP writer, JSON file, and in-memory stores can be swapped in tests.

## 2. Architecture diagram

```
                 ┌─────────────┐
   CSV shard ──► │  Streamer   │──publish(key=UUID)──┐
   (StatefulSet) └─────────────┘                     │
                 ┌─────────────┐                     ▼
   CSV shard ──► │  Streamer   │────────────► ┌────────────┐
                 └─────────────┘              │   Broker   │  custom TCP MQ
                                              │ partitions │  consumer groups
                                              └─────┬──────┘
                                                    │ consume / ack / nack
                 ┌─────────────┐                    │
                 │  Collector  │◄───────────────────┤
                 └──────┬──────┘                    │
                        │ HTTP ingest               │
                 ┌──────▼──────┐              ┌─────┴──────┐
                 │   Gateway   │◄─────────────│  Collector │
                 │ JSON + API  │              └────────────┘
                 │   :8080     │
                 └─────────────┘
```

```
cmd/                 process entrypoints (broker, streamer, collector, gateway, openapi-gen)
internal/domain      GPU and telemetry entities
internal/ports       interfaces (SOLID)
internal/mq          custom broker engine + length-prefixed JSON TCP
internal/streamer    CSV shard publisher
internal/collector   consumer + persist
internal/api         HTTP API
internal/storage     JSON file / memory / HTTP writer
data/                full DCGM CSV
api/openapi.yaml     generated OpenAPI document
deploy/helm          Helm chart (primary Kubernetes install)
deploy/k8s           static manifests
docs/                AI assistance log
```

## 3. Component responsibilities

| Process | Role |
| --- | --- |
| **broker** | In-process partitioned topic engine on TCP `:9000`. Same GPU UUID always hashes to the same partition (ordering). Consumer groups rebalance partitions across collectors. Bounded partitions apply backpressure. Unacked messages retry, then land on `{topic}.dlq`. |
| **streamer** | Loads `data/dcgm_metrics.csv`, takes every `Count`th row (`Index`), stamps `processed_at` with **emit time** (not the CSV timestamp), optionally loops the file. Scale by changing StatefulSet replicas. |
| **collector** | Competing consumer in group `collectors`. Parses JSON, validates, writes via HTTP to the gateway. Invalid payloads are nacked. |
| **gateway** | JSON-file repository, public REST API on `:8080`, and internal ingest. Serves generated OpenAPI at `/openapi.yaml`. One replica + PVC is the system of record. |

## 4. Custom MQ design

The broker is `internal/mq`: an `Engine` plus a TCP `Server`/`Client`.

- **Topics** are created on first publish. Default topic is `gpu-telemetry`.
- **Partitions** default to 8. A message key (GPU UUID) is hashed with SHA-1; `hash % partitionCount` picks the partition, so one GPU stays on one partition.
- Each partition is a **bounded channel** (`MaxPerPartition`, default 10_000).
- **Consumer groups** (default `collectors`) assign partitions round-robin across members. Subscribe/unsubscribe rebalances.
- Protocol: 4-byte big-endian length prefix, then a JSON frame (`publish`, `subscribe`, `unsubscribe`, `consume`, `ack`, `nack`, `response`). Max frame 16 MiB.
- The engine can also be used in-process (tests) through `EnginePublisher` / `EngineConsumer` without TCP.

## 5. Message delivery semantics

Delivery is **at-least-once** with explicit ack:

1. `Consume` dequeues a record from an assigned partition and holds it **in-flight** until `Ack` or `Nack`.
2. `Ack` drops the in-flight entry (success).
3. `Nack` increments `attempts` and enqueues a retry.
4. If `AckTimeout` (default 30s) expires, the message is retried the same way.
5. Unsubscribe of a consumer requeues that consumer’s in-flight messages.
6. After `MaxRetries` (broker default **5**), the payload is published to `{topic}.dlq` (for example `gpu-telemetry.dlq`).
7. Duplicate ingest (same UUID + `processed_at` + `metric_name`) is treated as idempotent at the store: the second save is a no-op and still returns HTTP **202**.

## 6. Retry strategy

| Layer | What retries | Policy |
| --- | --- | --- |
| Broker | Nack and ack-timeout | Linear backoff: `RetryBackoff * attempts` (default 5ms). After 5 attempts → DLQ. If the retry channel is full, message goes to DLQ immediately. |
| Streamer `retryPublisher` | `Publish` (including `ErrBackpressure`) | Up to 10 attempts, exponential backoff 200ms → 2s cap. |
| Collector subscribe | TCP subscribe before Run | Retry every 2s until context cancel. |
| Collector handle | Persist after nack | Extra 1s sleep when the write error looks like connection refused/reset/no such host. |
| HTTP writer | Gateway ingest | Up to 10 attempts, exponential backoff 200ms → 2s. Retries timeouts, connection errors, HTTP 502/503. Does not retry 4xx. |

Invalid JSON / validation failures are nacked (broker retry/DLQ) and are **not** treated as HTTP-transient.

## 7. Failure handling

Operational errors are wrapped (`fmt.Errorf("persist telemetry: %w", err)`) and never panic.

| Case | Behavior |
| --- | --- |
| Malformed CSV row | Parser skips the row; unrecoverable I/O still fails the load. |
| Invalid telemetry | Streamer skips; collector wraps `ErrInvalidPayload` and nacks (broker retry/DLQ). Ingest returns **400**. |
| Database unavailable | Persist wraps the error. Gateway **503**. Collector retries then nacks. |
| Database timeout | Persist wraps `context.DeadlineExceeded` / timeout. Gateway **504**. |
| Queue unavailable | Client wraps dial/closed as `queue unavailable`. Streamer retries then exits. |
| Queue full | `queue full: %w` (`ErrBackpressure`). Streamer retries with backoff. |
| Consumer / producer disconnect | Wrapped as `consumer disconnect` / `producer disconnect`. Broker recovers handler panics. |
| Duplicate message | Idempotent save; ingest **202**. |
| Invalid API timestamp | `ErrInvalidTime` → **400**. |
| Unknown GPU | `ErrUnknownGPU` → **404**. Known GPU with no points in the window → **200** `[]`. |
| Context cancellation | Propagated with `%w`. Processes shut down; in-flight HTTP **503**. |

Also: store file > 32 MiB is reset with a warning; corrupt JSON store fails gateway start. Consume idle timeout is not an error.

## 8. Backpressure strategy

Backpressure is **partition-bounded, fail-fast at publish**:

- `Publish` is non-blocking on the partition channel. If the partition is full, the engine returns `ErrBackpressure` instead of growing memory without bound.
- Streamers slow down via `STREAM_INTERVAL` and retry publish with backoff.
- Collectors pull with a consume timeout (default 2s) so they do not busy-spin.
- Gateway persist is batched: in-memory save plus flush at most every 500ms (`PersistInterval`), so ingest does not fsync every point.
- Memory/file stores cap retained telemetry records (in-memory max) so the JSON snapshot cannot grow forever.

## 9. Scaling strategy

Scale **streamers** and **collectors** independently. Keep **broker = 1** and **gateway = 1**.

| Component | How it scales | Constraint |
| --- | --- | --- |
| Streamer | StatefulSet replicas; each pod takes a disjoint CSV shard (`index % count`) | Replicas should stay ≤ 10 for this exercise. `STREAMER_COUNT` must match replica count. |
| Collector | Deployment replicas in one consumer group | Replicas should stay ≤ 10. More collectors than partitions (8) idle some pods. |
| Broker | Single process | All partitions live in one engine. HA would need a WAL + election behind `Engine`. |
| Gateway | Single replica + PVC | Shared JSON file is not multi-writer safe. |

Helm defaults: 2 streamers, 2 collectors, 8 partitions, gateway ClusterIP 8080.

## 10. Database schema

Persistence is a **JSON snapshot file**, not SQL. Default path: `DB_PATH` (`./tmp/telemetry.json` locally, `/var/lib/gpu-telemetry/telemetry.json` in containers).

```json
{
  "gpus": {
    "<uuid>": {
      "id": "GPU-…",
      "gpu_index": "0",
      "device": "nvidia0",
      "model_name": "NVIDIA H100 80GB HBM3",
      "hostname": "mtv5-dgx1-hgpu-031"
    }
  },
  "telemetry": [
    {
      "processed_at": "2026-09-17T04:00:00Z",
      "metric_name": "DCGM_FI_DEV_GPU_UTIL",
      "gpu_index": "0",
      "device": "nvidia0",
      "uuid": "GPU-…",
      "model_name": "NVIDIA H100 80GB HBM3",
      "hostname": "mtv5-dgx1-hgpu-031",
      "container": "",
      "pod": "",
      "namespace": "",
      "value": 0,
      "labels_raw": ""
    }
  ]
}
```

- `gpus` is keyed by UUID (upsert on ingest).
- `telemetry` is an append-only array of points; queries filter by UUID and inclusive `processed_at` window.
- Path comes from `DATABASE_URL` (or `DB_PATH`). Writes go to `path.tmp` then rename onto `path`.
- Tests use `internal/storage.Memory` with the same domain types.

## 11. API documentation

Base URL: `http://localhost:8080` (see `api/openapi.yaml`).

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/healthz`, `/livez` | Liveness (aliases of `/health`) |
| `GET` | `/health` | Liveness |
| `GET` | `/ready` | Readiness (store readable) |
| `GET` | `/metrics` | Prometheus metrics |
| `GET` | `/openapi.yaml`, `/api/openapi.yaml` | OpenAPI 3.0.3 spec |
| `GET` | `/api/v1/gpus` | GPU inventory |
| `GET` | `/api/v1/gpus/{id}/telemetry` | Time-ordered points for a UUID |
| `GET` | `/api/v1/gpus/{id}/telemetry?start_time=RFC3339&end_time=RFC3339` | Inclusive window |
| `POST` | `/internal/v1/telemetry` | Collector ingest (not a public product API) |

`{id}` is the GPU UUID from the CSV `uuid` column. Invalid RFC3339 filters return **400**. Unknown GPU UUID returns **404**. Store unavailable **503**; store timeout **504**.

## 12. Local development

Configuration is environment-only. Binaries use **development defaults** (localhost, `./tmp`, `./data`). Kubernetes/Compose set production paths explicitly.

```bash
make tidy
make test
make test-race
make coverage
make coverage-html
make build             # binaries in bin/
```

Four terminals from the repo root after `make build` (defaults are enough; env shown for clarity):

```bash
LOG_LEVEL=info MQ_ADDR=:9000 ./bin/broker
DATABASE_URL=./tmp/telemetry.json HTTP_ADDR=:8080 ./bin/gateway
MQ_ADDR=127.0.0.1:9000 GATEWAY_URL=http://127.0.0.1:8080 ./bin/collector
CSV_FILE=./data/dcgm_metrics.csv MQ_ADDR=127.0.0.1:9000 STREAM_INTERVAL=5ms ./bin/streamer
```

Sample calls:

```bash
curl -s localhost:8080/api/v1/gpus | head
curl -s "localhost:8080/api/v1/gpus/GPU-5fd4f087-86f3-7a43-b711-4771313afc50/telemetry?start_time=2026-01-01T00:00:00Z"
```

### Environment variables

Every important setting is an environment variable. Empty or invalid values fall back to the development default.

**Logging (all processes)**

| Variable | Default | Description |
| --- | --- | --- |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, or `error` |

**Broker / MQ**

| Variable | Default | Description |
| --- | --- | --- |
| `MQ_ADDR` | `:9000` (broker listen), `127.0.0.1:9000` (clients) | TCP bind or broker host:port |
| `MQ_TOPIC` | `gpu-telemetry` | Publish/consume topic |
| `MQ_GROUP` | `collectors` | Consumer group |
| `MQ_PARTITIONS` | `8` | Partition count |
| `MQ_MAX_QUEUE_SIZE` | `10000` | Max in-memory messages per partition (backpressure) |
| `MQ_ACK_TIMEOUT` | `30s` | In-flight ack deadline before retry |
| `MQ_RETRY_LIMIT` | `5` | Attempts before DLQ |
| `MQ_RETRY_BACKOFF` | `5ms` | Base retry backoff (linear with attempts) |
| `MQ_RETRY_QUEUE_SIZE` | `1024` | Internal retry channel size |
| `MQ_SHUTDOWN_TIMEOUT` | `10s` | Broker shutdown budget |
| `MQ_SUBSCRIBE_RETRY` | `2s` | Collector subscribe retry interval |

**Gateway / HTTP / store**

The store is a JSON file, not SQL. `DATABASE_URL` is the file path (`file://` prefix is stripped). `DB_MAX_OPEN_CONNS` limits concurrent writes; `DB_MAX_IDLE_CONNS` is accepted for ops compatibility (no SQL pool).

| Variable | Default | Description |
| --- | --- | --- |
| `HTTP_ADDR` | `:8080` | Gateway listen address |
| `DATABASE_URL` | `./tmp/telemetry.json` | Store path (preferred) |
| `DB_PATH` | same as `DATABASE_URL` | Alias if `DATABASE_URL` is unset |
| `DB_MAX_OPEN_CONNS` | `4` | Max concurrent store writes |
| `DB_MAX_IDLE_CONNS` | `2` | Reserved; JSON store has no idle pool |
| `DB_MAX_RECORDS` | `20000` | Max telemetry points retained |
| `HTTP_READ_HEADER_TIMEOUT` | `5s` | HTTP server header timeout |
| `HTTP_SHUTDOWN_TIMEOUT` | `5s` | Graceful HTTP shutdown |
| `HTTP_TIMEOUT` | `10s` | Collector → gateway ingest client timeout |
| `HTTP_MAX_RETRIES` | `10` | Ingest retries |
| `HTTP_INITIAL_BACKOFF` | `200ms` | Ingest retry backoff (caps at 2s) |
| `GATEWAY_URL` | `http://127.0.0.1:8080` | Collector ingest base URL |

**Streamer**

| Variable | Default | Description |
| --- | --- | --- |
| `CSV_FILE` | `./data/dcgm_metrics.csv` | CSV path (preferred) |
| `CSV_PATH` | same as `CSV_FILE` | Alias if `CSV_FILE` is unset |
| `STREAM_INTERVAL` | `10ms` | Delay between published rows |
| `STREAM_LOOP` | `true` | Replay CSV when the file ends |
| `STREAMER_INDEX` | `0` | Shard index |
| `STREAMER_COUNT` | `1` | Shard modulus (must match replica count) |
| `POD_NAME` | hostname | Used to derive StatefulSet ordinal if index unset |
| `PUBLISH_MAX_RETRIES` | `10` | Publish retries on backpressure/errors |
| `PUBLISH_INITIAL_BACKOFF` | `200ms` | Publish retry backoff (caps at 2s) |

**Collector**

| Variable | Default | Description |
| --- | --- | --- |
| `CONSUMER_ID` | hostname / pod name | Unique member in the consumer group |
| `CONSUME_TIMEOUT` | `2s` | Idle consume wait |

**Metrics sidecar (broker, streamer, collector)**

| Variable | Default | Description |
| --- | --- | --- |
| `METRICS_ADDR` | `:9091` / `:9092` / `:9093` | Bind address for `/metrics`, `/health`, `/ready`. `-` disables. Gateway serves these on `HTTP_ADDR`. |
| `SHUTDOWN_TIMEOUT` | `10s` | Graceful drain budget (`HTTP_SHUTDOWN_TIMEOUT` / `MQ_SHUTDOWN_TIMEOUT` aliases) |

## 13. Docker instructions

Build all images (`gpu-telemetry/broker:local`, `streamer`, `collector`, `gateway`):

```bash
make docker-build
```

Or Compose (gateway published on **8080**, broker on **9000**):

```bash
docker compose up --build --scale collector=2
```

`deploy.replicas` in Compose is Swarm-only; use `--scale` for collector count. Streamer shard env in Compose is a single replica (`STREAMER_INDEX=0`, `STREAMER_COUNT=1`) unless you override those variables per container.

## 14. Kubernetes instructions

Requires a cluster in `kubectl`. Static manifests live under `deploy/k8s/`; the supported path is Helm (next section).

Local Kind example:

```bash
kind create cluster --name gpu-telemetry
make helm-deploy
kubectl -n gpu-telemetry port-forward svc/gpu-gateway 8080:8080
curl -s localhost:8080/healthz
curl -s localhost:8080/api/v1/gpus | head
```

`make helm-deploy` builds images and loads them into Kind or Minikube when the current context matches. `make forward` is the same port-forward to 8080.

Uninstall:

```bash
make helm-delete
```

## 15. Helm installation

Chart: `deploy/helm/gpu-telemetry`. Release name `gpu`, namespace `gpu-telemetry`.

```bash
make helm-deploy
```

Equivalent:

```bash
helm upgrade --install gpu deploy/helm/gpu-telemetry \
  --namespace gpu-telemetry --create-namespace --wait --timeout 3m
```

Important values (`deploy/helm/gpu-telemetry/values.yaml`):

- `replicaCount.streamer` / `replicaCount.collector`
- `broker.port` (9000), `gateway.port` (8080)
- `broker.partitions` (8)
- `gateway.persistence` (PVC for the JSON store)
- `topic` / `group`

## 16. Scaling Streamers

Streamers are a **StatefulSet**. Shard formula: emit row `i` if `i % STREAMER_COUNT == STREAMER_INDEX`.

- Index comes from `STREAMER_INDEX`, else the ordinal in `POD_NAME` / hostname (`…-0`, `…-1`, …).
- Helm sets `STREAMER_COUNT` from `replicaCount.streamer` so shards stay disjoint.

```bash
helm upgrade gpu deploy/helm/gpu-telemetry --namespace gpu-telemetry \
  --reuse-values \
  --set replicaCount.streamer=4
```

Keep streamer replicas ≤ 10 for this exercise. After scale, each pod must see a `STREAMER_COUNT` that matches the new replica count (Helm does this).

## 17. Scaling Collectors

Collectors are a **Deployment**. They join group `collectors` with `CONSUMER_ID` = pod name so the broker can rebalance partitions.

```bash
helm upgrade gpu deploy/helm/gpu-telemetry --namespace gpu-telemetry \
  --reuse-values \
  --set replicaCount.collector=3
```

Compose:

```bash
docker compose up --scale collector=2
```

More collectors than partitions (default 8) do not increase throughput. Collectors are stateless; they do not own the JSON store.

## 18. Testing

Unit tests use ports and in-process fakes (MQ `Engine`, `storage.Memory`, `httptest`). They do not require PostgreSQL, Kafka, or a running cluster.

```bash
make test          # go test ./...
make test-race     # go test -race ./...
go test ./... -v
```

| Area | What is covered |
| --- | --- |
| Queue | Publish/consume, ack/nack, retry and ack-timeout redelivery, backpressure (`ErrBackpressure`), consumer failure, TCP client disconnect, `Server.Shutdown`, concurrent producers/consumers, DLQ |
| Streamer | CSV parse (including skipped malformed rows), shard + loop, emit `processed_at`, publish failures, context cancel |
| Collector | JSON parse, `Validate`, persist, nack on invalid payload, retryable persist, idempotent duplicate save |
| API | List GPUs, telemetry query, `start_time`/`end_time`, invalid timestamps (**400**), unknown GPU (**404**), repository errors (**500**/**503**/**504**), ingest validation |

`go test -race` is practical on the MQ engine/server (WaitGroup vs Shutdown) and HTTP tests. Process `run()` hooks cover SIGINT/SIGTERM without killing the test binary.

## 19. Code coverage

```bash
make test
make test-race
make coverage          # coverage.out + go tool cover -func
make coverage-html     # coverage.html
```

Coverage is measurable: `go test -coverprofile=coverage.out -covermode=atomic -coverpkg=./...` then `go tool cover -func=coverage.out`. Open `coverage.html` after `make coverage-html`.

## 20. OpenAPI generation

Source of truth is the `OpenAPISpec` constant in `internal/api/openapi.go` (served at `GET /openapi.yaml`). The file on disk is generated:

```bash
make openapi           # writes api/openapi.yaml via cmd/openapi-gen
```

Server URL in the spec is `http://localhost:8080`. Generation does not require `swag`.

## 21. Observability

- Structured **JSON logs** on stdout (`slog`) for broker, streamer, collector, and gateway.
- Gateway HTTP middleware logs requests.
- Probes: `GET /health` and `GET /ready` (aliases `/healthz`, `/livez` remain). Compose healthcheck uses `/ready`.
- Kubernetes: gateway liveness `/health` and readiness `/ready` on 8080; broker/streamer/collector expose the same paths on their metrics ports.
- Prometheus: `GET /metrics` on the gateway (`:8080`) and on `METRICS_ADDR` for other processes (defaults `:9091` broker, `:9092` streamer, `:9093` collector). Set `METRICS_ADDR=-` to disable the sidecar.
- Metric names are prefixed `gpu_telemetry_`:
  - MQ: `mq_messages_published_total`, `mq_messages_consumed_total`, `mq_messages_acknowledged_total`, `mq_messages_retried_total`, `mq_queue_depth`, `mq_publish_failures_total`, `mq_consumer_failures_total`
  - Streamer: `streamer_records_processed_total`, `streamer_publish_failures_total`
  - Collector: `collector_records_consumed_total`, `collector_records_persisted_total`, `collector_persistence_failures_total`
  - API: `http_requests_total`, `http_request_duration_seconds`, `http_errors_total`

## 22. Graceful shutdown

All four processes trap **SIGINT** and **SIGTERM** with `signal.NotifyContext`. Shutdown uses a timeout (`SHUTDOWN_TIMEOUT`, default 10s; `HTTP_SHUTDOWN_TIMEOUT` / `MQ_SHUTDOWN_TIMEOUT` are aliases). HTTP `Close()` is only a last resort after `Shutdown` hits that deadline.

Order:

1. Mark not ready (`/ready` → 503) so load balancers stop sending work.
2. Stop producing (streamer cancels the emit loop; broker rejects new `publish`).
3. Stop accepting (HTTP `Shutdown` / close the MQ listener).
4. Drain in-flight work (gateway requests, collector persist+ack, broker TCP handlers).
5. **Ack only after a successful persist.** Failed or interrupted work is nacked so it can be redelivered.
6. Close the JSON store (`db.Close()` flushes).
7. Close the MQ consumer (unsubscribe) and then the broker engine.
8. Exit 0.

| Process | Shutdown |
| --- | --- |
| Gateway | `Stop()` → `http.Server.Shutdown(ctx)` → `db.Close()` |
| Broker | metrics `Shutdown` → `Server.Shutdown(ctx)` (listener, drain handlers, `Engine.Close`) |
| Streamer | cancel emit loop (no new publishes) → metrics `Shutdown` |
| Collector | finish current persist → ack or nack → metrics `Shutdown` → `consumer.Close()` |

In-flight MQ deliveries that are still unacked when the **engine** closes are dropped (in-memory broker). Collectors nack or leave them for ack-timeout redelivery if persist did not finish.

## 23. Known limitations

- Streamer/collector replicas stay ≤ 10 as specified for the exercise.
- Broker is a single in-memory process: no WAL, no replication, restart drops the queue.
- Gateway is one replica; the JSON file is not safe for multiple writers.
- At-least-once ingest can still duplicate if UUID/`processed_at`/`metric_name` differ (clock skew). Same-key duplicates are idempotent.
- DLQ is another in-memory topic; nothing consumes it by default.
- Compose streamer does not auto-shard when you `--scale streamer`.
- No TLS or auth.
- JSON store is capped (~32 MiB file / in-memory record cap); large historical queries are not the design center.

## 24. Future improvements

- Replicated broker (WAL + leader election) behind the existing `Engine` interface.
- Deduplicated ingest already keys UUID + processed_at + metric; extend if more fields should participate.
- DLQ consumer and operator alerts.
- Alerting rules and a Grafana dashboard on the existing `/metrics` series.
- SQL or time-series backend if the JSON snapshot is too small.
- mTLS between streamer/collector/broker/gateway.
- Streamer shard assignment for Compose comparable to the StatefulSet ordinal.

## 25. AI assistance documentation

See [docs/AI_ASSISTANCE.md](docs/AI_ASSISTANCE.md) for prompts, what the model generated, and where manual fixes were required.
