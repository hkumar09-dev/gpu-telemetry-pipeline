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
