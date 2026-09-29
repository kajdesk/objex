# objex

A fast, lightweight, S3 / Cloudflare R2 compatible object storage server written in Rust.

objex ships as a single static binary. It runs as a single node today, and its storage layer is designed so it can grow into an erasure-coded distributed cluster without changing the S3 API layer.

> **Status:** early development. The single-node server implements the full first-release API; see [Roadmap](#roadmap).

## Features

- **S3 / R2 API compatible**: works with the AWS SDKs, AWS CLI, rclone, s3cmd, boto3, and anything else that speaks S3.
- **SigV4 authentication**: header auth, presigned URLs, and streaming `aws-chunked` uploads (signed, unsigned, and with trailers). Multiple access keys. Any region is accepted, including R2's `auto`.
- **Buckets**: create, delete, head, list, location, public-read or private access, and bucket CORS with OPTIONS preflight.
- **Objects**: put, get, head, delete, batch delete, and copy. Supports Range reads, user metadata, `response-*` overrides, and conditional requests (`If-Match`, `If-None-Match`, `If-Modified-Since`, `If-Unmodified-Since`, and `If-None-Match: *` on PUT).
- **Listing**: ListObjects V1 and V2 with prefix, delimiter, pagination, and `encoding-type=url`.
- **Multipart uploads**: create, upload part, upload part copy (with ranges), complete, abort, list parts, and list uploads. Completing an upload copies no data.
- **Checksums**: CRC32, CRC32C, CRC64NVME, SHA1, and SHA256 are verified on upload and returned when requested with `x-amz-checksum-mode`. These are the defaults in newer AWS SDKs.
- **Addressing**: both path-style (`host/bucket/key`) and virtual-host style (`bucket.domain/key`).
- **Durable by default**: data and metadata are flushed to disk before a write is acknowledged (see [Durability](#durability)). Pass `--no-fsync` to trade durability for speed.
- **Integrity checked**: every blob carries an internal CRC32C. Full reads are verified before the last bytes are sent, and a background scrub re-checks everything weekly (`objex scrub` does it offline).

## Install

objex is a single self-contained program. Run it as a Docker image, as a downloaded binary, or build it from source.

### Docker

```sh
docker run -d --name objex -p 9000:9000 \
  -e OBJEX_ACCESS_KEY=OBXCHANGEME000000001 \
  -e OBJEX_SECRET_KEY=change-me-to-a-long-random-secret \
  -v objex-data:/data \
  ghcr.io/kajdesk/objex:latest
```

The image is multi-arch (`linux/amd64`, `linux/arm64`), about 45 MB, runs as a non-root user, and has a built-in healthcheck. Images are published for each release tag (`:latest`, `:1.2.3`, `:1.2`).

| Path / port | Purpose |
|---|---|
| `9000` | S3 API |
| `/data` | Object data and metadata. Mount a volume here. |
| `/config/objex.toml` | Optional config file. Mount a volume on `/config` to keep keys added with `docker exec objex objex key add <name>`. |

### Prebuilt binary

Each [release](https://github.com/kajdesk/objex/releases) has archives for Linux (x86_64, arm64; statically linked, no dependencies) and macOS (Apple Silicon, Intel), each with a `.sha256` checksum.

```sh
VERSION=v0.1.0
TARGET=x86_64-unknown-linux-musl   # or aarch64-unknown-linux-musl, aarch64-apple-darwin, x86_64-apple-darwin
curl -LO https://github.com/kajdesk/objex/releases/download/$VERSION/objex-$VERSION-$TARGET.tar.gz
tar xzf objex-$VERSION-$TARGET.tar.gz
sudo install objex-$VERSION-$TARGET/objex /usr/local/bin/
```

### From source

Requires Rust 1.89 or newer.

```sh
cargo build --release   # produces target/release/objex
```

## Quick start

```sh
objex init                                 # writes objex.toml
objex key add admin                        # prints an access key and secret
objex server --data ./data --listen 0.0.0.0:9000
```

Use it with the AWS CLI:

```sh
export AWS_ACCESS_KEY_ID=<access key>
export AWS_SECRET_ACCESS_KEY=<secret>
aws --endpoint-url http://localhost:9000 s3 mb s3://photos
aws --endpoint-url http://localhost:9000 s3 cp ./cat.jpg s3://photos/
aws --endpoint-url http://localhost:9000 s3 ls s3://photos/
```

Or from code. Any S3 SDK works when you point it at the endpoint:

```js
// JavaScript (AWS SDK v3)
const s3 = new S3Client({ endpoint: "http://localhost:9000", region: "auto", forcePathStyle: true,
                          credentials: { accessKeyId: "...", secretAccessKey: "..." } });
```

```python
# Python (boto3)
s3 = boto3.client("s3", endpoint_url="http://localhost:9000", region_name="auto",
                  config=Config(s3={"addressing_style": "path"}))
```

## Docker Compose

[`examples/docker-compose`](examples/docker-compose) runs objex next to an application, creates buckets on startup, and includes an end-to-end check written with boto3:

```sh
cd examples/docker-compose
cp .env.example .env          # set real keys
docker compose up -d
docker compose --profile test run --rm smoke-test
```

The core of it:

```yaml
services:
  objex:
    image: ghcr.io/kajdesk/objex:latest
    environment:
      OBJEX_ACCESS_KEY: ${OBJEX_ACCESS_KEY}
      OBJEX_SECRET_KEY: ${OBJEX_SECRET_KEY}
    volumes: [objex-data:/data, objex-config:/config]
    stop_grace_period: 30s        # lets in-flight uploads finish
  app:
    environment:
      S3_ENDPOINT: http://objex:9000
    depends_on:
      objex: { condition: service_healthy }
volumes: { objex-data: {}, objex-config: {} }
```

Things to know:

- **Use path-style addressing.** Inside a compose network the host is `objex`, and `bucket.objex` does not resolve. Set `forcePathStyle: true` (JavaScript) or `addressing_style: "path"` (boto3).
- **Sign browser URLs for the public address.** A presigned URL made with `http://objex:9000` only works inside the network. Create a second S3 client with the address browsers use (for example `https://s3.example.com`) to generate URLs you hand out.
- **More than one key.** The `OBJEX_ACCESS_KEY`/`OBJEX_SECRET_KEY` variables define one key. For more, keep a volume on `/config` and run `docker compose exec objex objex key add <name> [--bucket b] [--read-only]`; the server picks it up within seconds.
- **One container per data volume.** A second objex on the same volume refuses to start. Scale by giving each instance its own volume.
- **Docker Desktop (macOS/Windows):** use a named volume, not a bind mount. Bind mounts make durable writes very slow.

## Configuration

Settings are read from `objex.toml` (or the file named by `--config` / `OBJEX_CONFIG`). Any setting can be overridden with an environment variable, which is convenient for Docker: `OBJEX_LISTEN`, `OBJEX_DATA_DIR`, `OBJEX_REGION`, `OBJEX_DOMAIN`, `OBJEX_FSYNC`, `OBJEX_SCRUB_INTERVAL_HOURS`, `OBJEX_MAX_CONNECTIONS`, and `OBJEX_ACCESS_KEY` + `OBJEX_SECRET_KEY` for a key. The config file is optional when these are set.

```toml
listen = "0.0.0.0:9000"
data_dir = "./data"
region = "auto"            # region reported to clients; any signed region is accepted
domain = "s3.example.com"  # optional: enables virtual-host style bucket.s3.example.com
fsync = true
scrub_interval_hours = 168 # background integrity scrub; 0 disables
max_connections = 4096

[[keys]]
name = "admin"
access_key = "OBX..."
secret_key = "..."
```

Key management:

```sh
objex key add <name>                          # generate a new key pair
objex key add <name> --bucket photos --read-only  # restrict to buckets, reads only
objex key list
objex key rm <access-key-or-name>
```

A running server picks up key changes within a couple of seconds; no restart is needed.

Buckets are private by default. A bucket created with the `public-read` canned ACL (or switched with `PutBucketAcl`) also serves anonymous `GET`, `HEAD`, and listing requests.

Logging is controlled with `OBJEX_LOG` (for example `OBJEX_LOG=debug`, or `OBJEX_LOG=info,objex::access=off` to silence the access log).

## Deployment

objex speaks plain HTTP. Put a TLS-terminating reverse proxy (Caddy, nginx, a load balancer) in front of it for anything beyond a trusted network, and set the proxy's body size limit high enough for your largest single PUT or part (5 GiB).

Built-in limits: request headers must arrive within 30 seconds (which also closes idle keep-alive connections), a request body that stalls for 60 seconds is abandoned, and at most `max_connections` connections are served at once. There are no per-key quotas or rate limits yet.

Only one objex process may use a data directory; a second one refuses to start.

`GET /_objex/health` returns `200 ok` without authentication, for load balancers and orchestrators. `objex health` probes it from inside a container.

## Durability

With `fsync = true` (the default), a write is acknowledged only after:

1. the object data is written to a temporary file and flushed,
2. any new shard directories and the renamed blob's directory entry are flushed, and
3. the metadata transaction is committed with a full flush.

On Linux each step uses `fsync`. On macOS, steps 1 and 2 use `fsync` and step 3 uses `F_FULLFSYNC`, which forces the drive's cache (and so everything written before it) to stable storage. Concurrent writes share metadata commits, so heavy parallel upload traffic costs far fewer flushes than one per object.

This ordering has been reviewed but not yet power-cut tested. It relies on the filesystem honoring `fsync` on files and directories (ext4, XFS, APFS do).

## Architecture

```
          S3 HTTP API (hyper)           SigV4 auth, XML, CORS, routing
                  │
            ObjectLayer trait           ← seam for a distributed implementation
                  │
       ┌──────────┴──────────┐
   MetaStore trait      BlobStore trait
   (redb, single node)  (local files; later erasure-coded shards across nodes)
```

- **Metadata** lives in [redb](https://github.com/cberner/redb), an embedded, pure-Rust, ACID B-tree. Keys are stored sorted, so prefix and delimiter listings are fast range scans.
- **Object data** is stored as immutable blob files under `data/blobs/ab/cd/<id>`. An upload streams to a temp file while the ETag and checksums are computed. The file is then fsynced and renamed into place, and only after that is the metadata committed. Overwrites are atomic, and readers that are already streaming the old version keep it until they finish.
- **Multipart completion** does not copy data. The object records its ordered list of part blobs, and GET streams across them.
- **CopyObject** uses hardlinks where possible, so copying a large object on one node is instant.
- **Garbage collection** runs in the background and removes blobs that no metadata references. These are left behind if the server crashes mid-write.

### Toward distributed mode

Blob references are shard-aware from the start: each one carries the erasure layout (data shards, parity shards, shard index, node). Single-node mode is simply 1 data shard and 0 parity. A distributed deployment will add:

1. an erasure-coded `BlobStore` that spreads each object across nodes as data + parity shards, and
2. a replicated `MetaStore`.

The S3 API layer does not change.

## Roadmap

- [x] Single-node engine (redb + blob files)
- [x] Full SigV4 (header, presigned, streaming chunked + trailers)
- [x] Core bucket and object API, ListObjects V1/V2
- [x] Multipart upload, copy, Range, conditional requests
- [x] Checksums (CRC32/CRC32C/CRC64NVME/SHA1/SHA256)
- [x] Bucket CORS
- [ ] Erasure-coded distributed mode
- [ ] Versioning, tagging, lifecycle

## Development

```sh
cargo test
```

The test suite includes end-to-end tests that start a real server and drive it with the official `aws-sdk-s3` crate, plus hand-built `aws-chunked` uploads for the streaming signature modes.

Not planned for the first release: versioning, tagging, lifecycle rules, object lock, SSE, bucket policies, and SigV2.

## License

objex is source-available under the [Elastic License 2.0](LICENSE). Copyright 2026 kajdesk.

- **You can** use, modify, and run it for free, for any purpose, including in commercial products and as the storage for your own applications and backend systems.
- **You cannot** offer objex to others as a hosted or managed service, remove the license or copyright notices, or distribute modified copies without marking them as modified.

This is not an OSI-approved open-source license. To offer objex as a service, or for other terms, contact kajdesk.
