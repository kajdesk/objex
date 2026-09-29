# syntax=docker/dockerfile:1

# ---- build ----
FROM rust:1-bookworm AS build
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked --bin objex \
 && cp target/release/objex /objex
# Directories the non-root runtime user must own (distroless has no shell).
RUN mkdir -p /out/data /out/config && chown -R 65532:65532 /out

# ---- runtime ----
FROM gcr.io/distroless/cc-debian12:nonroot
LABEL org.opencontainers.image.licenses="Elastic-2.0" \
      org.opencontainers.image.source="https://github.com/kajdesk/objex"
COPY --from=build /objex /usr/local/bin/objex
COPY LICENSE NOTICE /usr/share/doc/objex/
COPY --from=build --chown=65532:65532 /out/data /data
COPY --from=build --chown=65532:65532 /out/config /config

ENV OBJEX_LISTEN=0.0.0.0:9000 \
    OBJEX_DATA_DIR=/data \
    OBJEX_CONFIG=/config/objex.toml

USER 65532:65532
VOLUME ["/data"]
EXPOSE 9000
HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --retries=3 CMD ["/usr/local/bin/objex", "health"]
ENTRYPOINT ["/usr/local/bin/objex"]
CMD ["server"]
