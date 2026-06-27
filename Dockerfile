# syntax=docker/dockerfile:1

# ---- build ----
FROM golang:1.25-alpine AS build
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /cronkit .

# ---- runtime ----
FROM alpine:3.20

# ca-certificates + tzdata: TLS and timezones.
# tini: PID 1 init that reaps zombies — important because a timed-out/canceled
#       job's process group is SIGKILLed and orphaned grandchildren reparent here.
# docker-cli: so jobs can `docker exec` into sibling containers (mount the docker
#             socket at runtime). Drop it if your jobs don't need docker.
RUN apk add --no-cache ca-certificates tzdata tini docker-cli

COPY --from=build /cronkit /usr/local/bin/cronkit

ENV CRONKIT_CONFIG=/config/jobs.yml \
    CRONKIT_DATA=/data \
    CRONKIT_ADDR=:8080 \
    TZ=UTC

# Run history + logs persist here.
VOLUME ["/data"]
EXPOSE 8080

# Liveness: hit the built-in /healthz endpoint (busybox wget).
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD wget -q -O- http://127.0.0.1:8080/healthz >/dev/null 2>&1 || exit 1

LABEL org.opencontainers.image.title="cronkit" \
      org.opencontainers.image.description="Tiny cron scheduler with a web UI, per-run logs, live tail, chaining and concurrency groups" \
      org.opencontainers.image.source="https://github.com/doino-gretchenliev/cronkit" \
      org.opencontainers.image.licenses="PolyForm-Noncommercial-1.0.0"

# tini as PID 1 → clean signal forwarding + zombie reaping.
ENTRYPOINT ["/sbin/tini", "--", "cronkit"]
