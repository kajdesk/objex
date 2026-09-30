# syntax=docker/dockerfile:1

FROM golang:1.24-bookworm AS build
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY cmd ./cmd
COPY internal ./internal
COPY pkg ./pkg
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    mkdir -p /out && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /out/objex ./cmd/objex
RUN mkdir -p /out/data /out/config && chown -R 65532:65532 /out/data /out/config

FROM gcr.io/distroless/static-debian12:nonroot
LABEL org.opencontainers.image.licenses="Elastic-2.0" \
      org.opencontainers.image.source="https://github.com/kajdesk/objex"
COPY --from=build /out/objex /usr/local/bin/objex
COPY LICENSE NOTICE /usr/share/doc/objex/
COPY --from=build --chown=65532:65532 /out/data /data
COPY --from=build --chown=65532:65532 /out/config /config

ENV OBJEX_LISTEN=0.0.0.0:9000 \
    OBJEX_DATA_DIR=/data \
    OBJEX_CONFIG=/config/objex.json

USER 65532:65532
VOLUME ["/data"]
EXPOSE 9000
HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --retries=3 CMD ["/usr/local/bin/objex", "health", "http://127.0.0.1:9000/_objex/health"]
ENTRYPOINT ["/usr/local/bin/objex"]
