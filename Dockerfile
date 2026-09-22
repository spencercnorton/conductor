# syntax=docker/dockerfile:1.7

# Build stage --------------------------------------------------------------
FROM golang:1.25-alpine AS build

WORKDIR /src
RUN apk add --no-cache git ca-certificates

# Cache deps — go.sum may be empty in Phase 0; add when dependencies appear.
COPY go.mod go.sum* ./
RUN go mod download

COPY . .

ARG COMMIT=unknown
ARG BUILD_DATE=unknown

RUN set -eu; \
    VERSION_VALUE="$(tr -d '[:space:]' < VERSION)"; \
    COMMIT_VALUE="$COMMIT"; \
    BUILD_DATE_VALUE="$BUILD_DATE"; \
    if [ -s .ci-commit ]; then \
      COMMIT_VALUE="$(tr -d '\r\n' < .ci-commit)"; \
    fi; \
    if [ -s .ci-build-date ]; then \
      BUILD_DATE_VALUE="$(tr -d '\r\n' < .ci-build-date)"; \
    fi; \
    printf '%s\n' "$VERSION_VALUE" \
      | grep -Eq '^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$' \
      || { echo "invalid tracked VERSION" >&2; exit 1; }; \
    if [ "$COMMIT_VALUE" != unknown ]; then \
      printf '%s\n' "$COMMIT_VALUE" | grep -Eq '^[0-9a-f]{40}$' \
        || { echo "invalid build commit metadata" >&2; exit 1; }; \
    fi; \
    if [ "$BUILD_DATE_VALUE" != unknown ]; then \
      printf '%s\n' "$BUILD_DATE_VALUE" \
        | grep -Eq '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]+)?(Z|[+-][0-9]{2}:[0-9]{2})$' \
        || { echo "invalid build date metadata" >&2; exit 1; }; \
    fi; \
    CGO_ENABLED=0 go build \
      -trimpath \
      -ldflags="-s -w \
        -X github.com/spencercnorton/conductor/internal/version.Version=${VERSION_VALUE} \
        -X github.com/spencercnorton/conductor/internal/version.Commit=${COMMIT_VALUE} \
        -X github.com/spencercnorton/conductor/internal/version.Date=${BUILD_DATE_VALUE}" \
      -o /out/conductor \
      ./cmd/conductor

# Runtime stage ------------------------------------------------------------
FROM alpine:3.20

# ffmpeg is required for transcode profiles (Phase 4). The alpine package
# is CPU-only — sufficient for stabilize-cpu / audio-fix profiles. To use
# the nvenc-* profiles in production, swap to a CUDA-enabled base image
# (linuxserver/ffmpeg:latest) — see docs/enhancements.md §11.
# su-exec is used by entrypoint.sh to drop privileges after chown.
RUN apk add --no-cache ca-certificates tzdata wget ffmpeg su-exec \
    && addgroup -S conductor \
    && adduser -S -G conductor -u 10001 conductor \
    && mkdir -p /var/lib/conductor /etc/conductor \
    && chown -R conductor:conductor /var/lib/conductor /etc/conductor

COPY --from=build /out/conductor /usr/local/bin/conductor
COPY scripts/entrypoint.sh /usr/local/bin/entrypoint.sh
COPY assets/default-logos/ /usr/share/conductor/default-logos/
RUN chmod +x /usr/local/bin/entrypoint.sh

# Run entrypoint as root so it can chown bind-mounted state dirs;
# the entrypoint drops to uid 10001 (conductor) before exec'ing the binary.
WORKDIR /var/lib/conductor
ENV CONDUCTOR_DATA_DIR=/var/lib/conductor \
    CONDUCTOR_LISTEN=:8409 \
    CONDUCTOR_TUNER_COUNT=8

EXPOSE 8409

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD wget -qO- http://127.0.0.1:8409/healthz | grep -q '"ok"' || exit 1

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
