# syntax=docker/dockerfile:1
FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG SERVICE=gateway
RUN CGO_ENABLED=0 go build -o /out/app ./cmd/${SERVICE}

FROM alpine:3.20
RUN apk add --no-cache ca-certificates \
	&& adduser -D -H -u 65532 appuser
WORKDIR /app
COPY --from=build /out/app /usr/local/bin/app
COPY data/dcgm_metrics.csv /data/dcgm_metrics.csv
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/app"]
