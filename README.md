# objex

objex is a lightweight S3-compatible object storage server written in Go. It
runs as a single process and stores transactional metadata in bbolt alongside
immutable object blobs on the local filesystem.

> **Status:** early development, single node. The full first-release S3 API
> is implemented and tested against the official AWS SDK for Go.

## Features

- **S3 / R2 API compatible:** works with the AWS SDKs and CLI, rclone, boto3,
  s3cmd and anything else that speaks S3.
- **SigV4 authentication:** header auth, presigned URLs, and streaming
  `aws-chunked` uploads (signed, unsigned, and with trailing checksums), which
  current AWS SDKs use over HTTPS. Any region is accepted, including `auto`.
- **Access keys:** multiple keys, optional bucket restrictions, read-only keys,
  managed with `objex key` and picked up by a running server within seconds.
- **Buckets:** create, delete, head, list, location, private or `public-read`,
  bucket ACLs, and bucket CORS with OPTIONS preflight.
- **Objects:** put, get, head, delete, batch delete, copy (hard links, so copies
  are instant), Range reads, `partNumber` reads, user metadata, `response-*`
  overrides, and conditional requests (`If-Match`, `If-None-Match`,
  `If-Modified-Since`, `If-Unmodified-Since`, `If-None-Match: *` on PUT).
- **Listing:** ListObjects V1 and V2 with prefix, delimiter, pagination and
  `encoding-type=url`; common prefixes are skipped in one seek, not scanned.
- **Multipart uploads:** create, upload part, upload part copy (with ranges),
  complete, abort, list parts, list uploads. Completing copies no data.
- **Checksums:** CRC32, CRC32C, CRC64NVME, SHA1 and SHA256 are verified on
  upload and returned with `x-amz-checksum-mode`, including composite and
  full-object multipart checksums.
- **Integrity:** every blob carries a CRC32C. Full reads are verified before
  their last bytes are sent, a background scrub re-checks everything weekly,
  and `objex scrub` does it offline.
- **Durable by default:** data and metadata are flushed before a write is
  acknowledged; concurrent writes share one metadata flush (group commit).
- **Addressing:** path-style (`host/bucket/key`) and virtual-host style
  (`bucket.domain/key`).

Not supported: versioning, object tagging, lifecycle rules, object lock,
server-side encryption, bucket policies, SigV2 and SigV4a. These return S3
`NotImplemented` (or report "not configured").

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

### Prebuilt binary

Each [release](https://github.com/kajdesk/objex/releases) has static binaries
for Linux and macOS (`linux-amd64`, `linux-arm64`, `darwin-arm64`,
`darwin-amd64`), each with a `.sha256` checksum:

```sh
VERSION=v0.2.0
TARGET=linux-amd64
curl -LO https://github.com/kajdesk/objex/releases/download/$VERSION/objex-$VERSION-$TARGET.tar.gz
tar xzf objex-$VERSION-$TARGET.tar.gz
sudo install objex-$VERSION-$TARGET/objex /usr/local/bin/
objex version
```

### From source

Go 1.24 or newer is required.

```sh
go build -trimpath -o ./bin/objex ./cmd/objex
go build -trimpath -o ./bin/objex-bench ./cmd/objex-bench
```

## Quick start

```sh
objex init                       # writes objex.json (owner-only; it holds secrets)
objex key add admin              # prints an access key and secret
objex server -data ./data
```

Or, without a config file:

```sh
OBJEX_ACCESS_KEY=OBXCHANGEME000000001 OBJEX_SECRET_KEY=change-me-to-a-long-random-secret \
  objex server -listen 127.0.0.1:9000 -data ./data
```

Use it with the AWS CLI:

```sh
export AWS_ACCESS_KEY_ID=<access key> AWS_SECRET_ACCESS_KEY=<secret>
aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://photos
aws --endpoint-url http://127.0.0.1:9000 s3 cp ./cat.jpg s3://photos/
aws --endpoint-url http://127.0.0.1:9000 s3 ls s3://photos/
```

Use path-style addressing when the endpoint hostname cannot resolve bucket
subdomains: `forcePathStyle: true` in the JavaScript SDK, `addressing_style:
"path"` in boto3, `UsePathStyle: true` in Go.

## Commands

```text
objex server [-config f] [-listen addr] [-data dir] [-no-fsync]
objex init                                   write a default config file
objex key add <name> [-bucket b]... [-read-only]
objex key list
objex key rm <access-key-or-name>
objex scrub [-data dir]                      verify every blob (server stopped)
objex health [URL]                           probe a server (healthchecks)
```

Key changes are picked up by a running server within two seconds.

## Configuration

objex reads `objex.json` by default (`-config` or `OBJEX_CONFIG` to change).
A missing file is allowed. Unknown fields are rejected, so typos fail loudly.

```json
{
  "listen": "0.0.0.0:9000",
  "data_dir": "./data",
  "region": "auto",
  "domain": "s3.example.com",
  "fsync": true,
  "verify_reads": true,
  "max_connections": 4096,
  "scrub_interval_hours": 168,
  "scrub_mb_per_sec": 64,
  "gc_interval_hours": 24,
  "keys": [
    { "name": "admin", "access_key": "OBX...", "secret_key": "..." },
    { "name": "viewer", "access_key": "OBX...", "secret_key": "...", "buckets": ["photos"], "read_only": true }
  ]
}
```

Environment overrides: `OBJEX_LISTEN`, `OBJEX_DATA_DIR`, `OBJEX_REGION`,
`OBJEX_DOMAIN`, `OBJEX_FSYNC`, `OBJEX_VERIFY_READS`, `OBJEX_MAX_CONNECTIONS`,
`OBJEX_SCRUB_INTERVAL_HOURS`, `OBJEX_SCRUB_MB_PER_SEC`,
`OBJEX_GC_INTERVAL_HOURS`, `OBJEX_LOG` (`debug`, `info`, `warn`), and
`OBJEX_ACCESS_KEY` + `OBJEX_SECRET_KEY` for a key.

Disabling fsync improves write throughput, but acknowledged writes may be lost
after a crash or power failure.

## Deployment

objex speaks plain HTTP. Put a TLS-terminating reverse proxy (Caddy, nginx, a
load balancer) in front for anything beyond a trusted network, with a body size
limit of at least 5 GiB. Request headers must arrive within 30 seconds, a
stalled request body is dropped after 60 seconds, and at most
`max_connections` connections are served at once. Only one objex process may
use a data directory; a second one refuses to start.

### Durability

With `fsync` on, a write is acknowledged only after the object data is flushed,
any new shard directories and the blob's directory entry are flushed, and the
metadata transaction commits with a full flush. On macOS the blob flushes use
`fsync` and the metadata commit uses `F_FULLFSYNC`, which forces the drive
cache, and so everything written before it, to stable storage. This ordering
has been reviewed but not power-cut tested; it relies on the filesystem
honoring `fsync` on files and directories (ext4, XFS and APFS do).

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
cmd/objex/          server, init, key, scrub and health commands
cmd/objex-bench/    throughput benchmark
internal/
  auth/             SigV4 verification and aws-chunked decoding
  checksum/         S3 checksum algorithms, composite and full-object combining
  config/           JSON configuration and key management
  s3/               S3 routing, handlers, XML and CORS
  s3err/            S3 error codes
  server/           HTTP lifecycle, limits, key reload, scheduled GC and scrub
  storage/          storage contract
    local/          bbolt metadata + immutable blob files
  e2e/              end-to-end tests with the AWS SDK for Go
pkg/sigv4/          reusable request signer
```

Metadata lives in `meta.db` (bbolt). Object data is written to `tmp/` while
MD5, CRC32C and any requested checksums are computed, flushed, and renamed
into `blobs/ab/cd/<id>`; only then is the metadata committed, together with
concurrent writes. Replaced and deleted blobs are removed in the background,
never while a download is still reading them. Multipart completion and copies
move no data. GC removes blob files that metadata does not reference, such as
those left by a crash between writing and committing.

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
