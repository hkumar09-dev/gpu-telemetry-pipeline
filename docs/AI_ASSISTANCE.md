# AI assistance log

Project: Elastic GPU Telemetry Pipeline with a custom message queue.

Cursor / Grok implemented the repository from the assignment PDF and the DCGM CSV in a single session. The human provided the spec path, CSV path, language (Go), unit tests, architecture, and SOLID requirements — not line-by-line code.

## Repo bootstrap

**Prompt (user):** create a repo and implement the PDF in Golang with unit tests, proper architecture, and SOLID; include the DCGM CSV.

**AI:** Created `gpu-telemetry-pipeline`, initialized git via Cursor project tools, copied `dcgm_metrics.csv`, added Go module layout, Makefile, Docker, Helm, README.

**Manual:** None beyond the original files in Downloads. Cursor `create_project` / `move_agent_to_root` were used instead of ad-hoc `mkdir` in a random folder.

## Code bootstrap

**Prompt (implied by the PDF + user):** custom MQ (not Kafka/Rabbit/ZeroMQ), elastic streamers/collectors, REST gateway, Helm, OpenAPI generation, processed-at = emit time.

**AI generated:** domain model, ports, partitioned broker, TCP protocol, streamer sharding, collector ack/nack, SQLite + memory repos, HTTP ingest, Helm charts, Dockerfiles.

**Where the model needed correction (typical intervention points):**

- GPU API id is UUID, not `gpu_id` (collides across hosts). That was a design choice while reading the CSV, not spelled out in the PDF examples.
- CSV `timestamp` must not be persisted as `processed_at`; the streamer stamps emit time. Easy to get wrong if the parser copies the first column.
- Shared persistence: collectors cannot each own a SQLite file. Persistence is centralized on the gateway; collectors implement `Writer` over HTTP.
- Kubernetes streamer scale requires a stable ordinal (StatefulSet), not a Deployment replica hash.
- `docker compose` `deploy.replicas` is Swarm-only; README uses `--scale`.
- TCP server `Addr()` vs `ListenAndServe` needed a mutex to avoid a data race in tests.
- Unused `conn` parameter in the broker dispatch method (compile failure).
- `go.mod` / `go.sum` produced by `go mod tidy` after generating imports (`modernc.org/sqlite`).



## Unit tests

**Prompt:** unit tests with coverage via Makefile.

**AI generated:** broker (ack, nack, timeout redelivery, partition affinity, competing consumers, backpressure, TCP round-trip), CSV parser, streamer shard + timestamp, collector persist/nack, memory + SQLite window queries, gateway HTTP including time filters and OpenAPI.

**Manual / review:** Competing-consumer tests can be unlucky if all keys hash to one consumer’s partitions; the test uses many distinct keys and 4 partitions to make that unlikely. Coverage is `go test -coverpkg=./internal/...`.

## Build environment

**Prompt (PDF):** Docker + Helm, Makefile coverage and OpenAPI.

**AI:** Multi-stage Dockerfile with `SERVICE` build-arg, Compose file, Helm chart (broker, streamer StatefulSet, collector Deployment, gateway + PVC), `make test|coverage|openapi|build|docker-build`.

**Manual:** Image load into kind/minikube is cluster-specific and left as a README step. OpenAPI is generated from the in-repo spec constant (`cmd/openapi-gen`) rather than swag CLI, so `make openapi` does not need a global `swag` install.

## Prompts that would have fallen short without the PDF

- “Implement a GPU telemetry pipeline” without the PDF would have pulled in Kafka. The PDF’s ban on existing MQs drove the custom broker.
- Without “processed time is the timestamp”, parsers would keep the CSV time column and the time-window API would not match live streaming.
- Without “scale streamers/collectors independently”, a single binary would have been enough for a demo and would fail the elasticity requirement.
- Make it concurrent; use worker/job pooling in the broker.

## Metrics / observability

**Prompt:** Prometheus metrics if practical. MQ: published, consumed, acknowledged, retried, queue depth, publish failures, consumer failures. Streamer: records processed, publish failures. Collector: consumed, persisted, persistence failures. API: request count, latency, HTTP errors. Expose `/metrics`. Also `/health` and `/ready` (or Kubernetes equivalents).

**AI:** `internal/metrics` (`gpu_telemetry_*`) plus `internal/observe` sidecar. Gateway serves `/metrics`, `/health`, `/ready` (and `/healthz`/`/livez`) on `HTTP_ADDR`. Broker/streamer/collector use `METRICS_ADDR` (`:9091`/`:9092`/`:9093`; `-` disables). Helm probes hit those paths.

## API implementation

**Prompt:** Idiomatic Go. Prefer `cmd/` + `internal/{domain,streamer,collector,queue,repository,api,config,observability}`. `pkg/` only if needed. No unnecessary abstractions. Business logic independent of HTTP. Handlers must not contain database logic.

**AI:** Layout is `cmd/` + `internal/` with ports in `internal/ports`. Names that already existed were kept: `mq` (not `queue`), `storage` (not `repository`), `utils` env helpers (not `internal/config`), `observe` + `metrics` (not `observability`). No `pkg/`. `internal/api` is HTTP + status mapping; persist/query live behind `ports.Repository` (`storage.File` / `storage.Memory`). Domain types stay in `internal/domain`.

## Configuration

**Prompt:** All important configuration from environment/config. Examples: `MQ_ADDR`, `MQ_MAX_QUEUE_SIZE`, `MQ_ACK_TIMEOUT`, `MQ_RETRY_LIMIT`, `DATABASE_URL`, `DB_MAX_OPEN_CONNS`, `DB_MAX_IDLE_CONNS`, `STREAM_INTERVAL`, `CSV_FILE`, `HTTP_ADDR`, `LOG_LEVEL`. No hardcoded production config. Sensible development defaults. Document every variable.

**AI:** `utils/env.go` (`EnvInt`/`EnvDuration`/`EnvBool`/`FirstEnv`) with localhost/`./tmp`/`./data` defaults. `DATABASE_URL` is the JSON store path (`file://` stripped); `CSV_FILE` aliases `CSV_PATH`. Full table is in the README.

## Graceful shutdown

**Prompt:** Every service shuts down on SIGTERM/SIGINT via context. Order: stop accepting; stop producing; finish in-flight work; ack only successful work; close DB; close queue; stop HTTP gracefully; exit cleanly. Do not use `_ = srv.Close()` as the only mechanism.

**AI:** `signal.NotifyContext` in each `cmd/*`. Gateway: `Service.Stop()` (not ready) → `http.Server.Shutdown` with timeout → `db.Close()`; `Close()` only if Shutdown times out. Broker: reject publish, close listener, drain handlers, `Engine.Close`. Streamer: cancel emit loop. Collector: persist then ack/nack, then `consumer.Close()`. Budget: `SHUTDOWN_TIMEOUT` (aliases `HTTP_SHUTDOWN_TIMEOUT` / `MQ_SHUTDOWN_TIMEOUT`).

## Error handling

**Prompt:** Handle operational errors without panic. Wrap with `fmt.Errorf("persist telemetry: %w", err)`. HTTP status codes. Cases: malformed CSV, invalid telemetry, DB unavailable/timeout, queue unavailable/full, consumer/producer disconnect, duplicate message, invalid API timestamp, unknown GPU, context cancellation.

**AI:** CSV skips bad rows. Invalid payloads wrap `ErrInvalidPayload` (streamer skip / collector nack). Store/HTTP writer wrap persist errors. MQ client maps backpressure, closed, timeout, disconnect. Duplicate UUID+`processed_at`+`metric_name` is idempotent. Gateway: invalid time **400**, unknown GPU **404**, unavailable/cancel **503**, timeout **504**.

## Testing

**Prompt:** Meaningful unit tests (queue, streamer, collector, API) via interfaces/DI, no external services. `make test`, `make test-race`, `make coverage`, `make coverage-html`. Measurable coverage. Race detection where practical. Do not pad coverage with empty tests.

**AI:** Tests live next to packages (`internal/mq`, `csvsource`, `streamer`, `collector`, `api`, `storage`). Fakes: in-process `Engine`, `Memory` repo, httptest. Makefile targets as requested (`test-race` is `go test -race ./...`). Coverage is `go test -coverprofile=coverage.out -covermode=atomic -coverpkg=./...`.
