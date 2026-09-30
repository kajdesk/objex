# objex

objex is a lightweight S3-compatible object storage server written in Go. It
runs as a single process and stores transactional metadata in bbolt alongside
immutable object blobs on the local filesystem.

> **Status:** early development. The core single-node API works, but the
> compatibility gaps listed below should be reviewed before production use.

## Features

- S3 Signature Version 4 header authentication and presigned URLs.
- Multiple access keys, optional bucket restrictions, and read-only keys.
- Path-style and virtual-host-style bucket addressing.
- Create, list, inspect, locate, and delete buckets.
- Put, get, inspect, and delete objects.
- Range reads, standard object metadata, user metadata, and conditional reads.
- ListObjects V1/V2 with prefix, delimiter, markers, and continuation tokens.
- Private buckets and buckets created with the `public-read` canned ACL.
- Streaming uploads with SHA-256 verification, MD5 ETags, and internal CRC32C.
- Durable local storage by default, graceful shutdown, connection limits, and a
  health endpoint.

Not implemented yet: multipart uploads, object copy, batch delete, streaming
`aws-chunked` decoding, CORS mutation, the full S3 checksum family, background
scrubbing/repair, and live key management. Unsupported operations return an S3
`NotImplemented` response.

## Install

### Docker

```sh
docker run -d --name objex -p 9000:9000 \
  -e OBJEX_ACCESS_KEY=OBXCHANGEME000000001 \
  -e OBJEX_SECRET_KEY=change-me-to-a-long-random-secret \
  -v objex-data:/data \
  ghcr.io/kajdesk/objex:latest
```

The image runs as a non-root user and includes a healthcheck. Mount persistent
storage at `/data`.

### From source

Go 1.24 or newer is required.

```sh
go build -trimpath -o ./bin/objex ./cmd/objex
go build -trimpath -o ./bin/objex-bench ./cmd/objex-bench
```

## Quick start

The simplest development setup uses environment credentials:

```sh
export OBJEX_ACCESS_KEY=OBXCHANGEME000000001
export OBJEX_SECRET_KEY=change-me-to-a-long-random-secret
./bin/objex server -listen 127.0.0.1:9000 -data ./data
```

Use the endpoint with the AWS CLI:

```sh
export AWS_ACCESS_KEY_ID="$OBJEX_ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$OBJEX_SECRET_KEY"
aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://photos
aws --endpoint-url http://127.0.0.1:9000 s3 cp ./cat.jpg s3://photos/
aws --endpoint-url http://127.0.0.1:9000 s3 ls s3://photos/
```

Use path-style addressing when the endpoint hostname cannot resolve bucket
subdomains. For example, set `forcePathStyle: true` in the JavaScript AWS SDK or
`addressing_style: "path"` in boto3.

## Configuration

objex reads `objex.json` by default. Select another file with `-config` or
`OBJEX_CONFIG`. A missing file is allowed, which makes an environment-only
configuration convenient for containers.

```json
{
  "listen": "0.0.0.0:9000",
  "data_dir": "./data",
  "region": "auto",
  "domain": "s3.example.com",
  "fsync": true,
  "max_connections": 4096,
  "keys": [
    {
      "name": "admin",
      "access_key": "OBX...",
      "secret_key": "replace-with-a-long-random-secret"
    }
  ]
}
```

Supported environment overrides:

- `OBJEX_CONFIG`
- `OBJEX_LISTEN`
- `OBJEX_DATA_DIR`
- `OBJEX_REGION`
- `OBJEX_DOMAIN`
- `OBJEX_FSYNC`
- `OBJEX_MAX_CONNECTIONS`
- `OBJEX_ACCESS_KEY` and `OBJEX_SECRET_KEY`

Command-line flags override the loaded configuration:

```sh
objex server -config ./objex.json -listen 0.0.0.0:9000 -data ./data
objex server -config ./objex.json -no-fsync
```

Disabling fsync improves write throughput but means acknowledged writes may be
lost after a crash or power failure.

## Health checks

The unauthenticated endpoint returns `200 OK`:

```text
GET /_objex/health
```

The binary can probe it directly:

```sh
objex health http://127.0.0.1:9000/_objex/health
```

## Architecture

```text
cmd/
  objex/          server and health-check executable
  objex-bench/    endpoint benchmark
internal/
  auth/           SigV4 verification
  config/         JSON and environment configuration
  s3/             S3 routing and XML responses
  server/         HTTP lifecycle and connection limits
  storage/        storage contracts
    local/        bbolt metadata and immutable local blobs
pkg/
  sigv4/          reusable request signer
```

Metadata is committed transactionally in `meta.db`. Object bodies are streamed
into temporary files while hashes are calculated, then renamed into sharded
paths under `blobs/`. Metadata is committed only after the blob is in place.
Overwrites publish new immutable blobs and reclaim the old blobs after commit.

The storage interface is kept behind `internal/storage`, allowing another
backend to be added without coupling it to HTTP routing or authentication.

## Docker Compose

The example in [`examples/docker-compose`](examples/docker-compose) starts
objex, creates example buckets, and provides a boto3 smoke test:

```sh
cd examples/docker-compose
cp .env.example .env
docker compose up -d --wait objex
docker compose --profile test run --rm smoke-test
```

Use one objex process per data directory. On Docker Desktop, a named volume is
recommended because durable writes to host bind mounts can be slow.

## Development

```sh
go test ./...
go test -race ./...
go vet ./...
```

Run the included workload against a development server:

```sh
go run ./cmd/objex-bench \
  -endpoint http://127.0.0.1:9000 \
  -access-key "$OBJEX_ACCESS_KEY" \
  -secret-key "$OBJEX_SECRET_KEY"
```

## License

objex is source-available under the [Elastic License 2.0](LICENSE). Copyright
2026 kajdesk.

- You may use, modify, and run it, including inside commercial products.
- You may not offer objex to third parties as a hosted or managed service.
- You may not remove the license notices, and modified copies must be marked.

This is not an OSI-approved open-source license. Contact kajdesk for other
terms.
