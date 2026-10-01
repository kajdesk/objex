# Performance audit

Audit date: 2026-09-30  
Revision: `4b6de0a`  
Runtime: Go 1.24.4, darwin/arm64  
Storage: local APFS on the audit machine

## Executive summary

objex has a sound performance-oriented foundation: request bodies stream to
disk, uploads do not buffer whole objects, write buffers are pooled, metadata
writes can share one durable transaction, listings use ordered bbolt cursors,
copies use hard links when possible, and range reads can use the efficient file
copy path.

The system is not yet capacity-certified. The present benchmark is useful as a
directional throughput smoke test, but it does not report latency percentiles,
CPU, allocation rate, disk latency, queue depth, or cold-cache behavior. The
largest measured weakness is durable small-object ingestion. The largest
scalability risk is memory growth from the upload pipeline at high concurrent
upload counts.

## Validation performed

- `go test -race ./...`: passed.
- `go vet ./...`: passed.
- whole-module statement coverage: 69.2%.
- static Linux builds for amd64 and arm64: passed.
- current production Docker image: built and started successfully.
- stripped arm64 Linux image size: 8,999,845 bytes (about 8.6 MiB).
- local unstripped darwin server binary: about 9.9 MiB.

There are no Go `Benchmark*` functions in the repository. All throughput
results below come from `cmd/objex-bench` over HTTP on loopback.

## Measured baseline

Both modes used a fresh data directory and fresh keys. Reads immediately
followed writes and therefore used a warm operating-system page cache.
`verify_reads` remained enabled. The client and server shared one machine.

| Workload | Parallel | Durable | `fsync=false` |
|---|---:|---:|---:|
| 16 KiB PUT | 64 | 1,644 ops/s, 25.7 MiB/s | 8,092 ops/s, 126.4 MiB/s |
| 16 KiB GET | 64 | 43,600 ops/s, 681.2 MiB/s | 41,614 ops/s, 650.2 MiB/s |
| 1 MiB PUT | 32 | 394 ops/s, 394.5 MiB/s | 1,688 ops/s, 1,687.7 MiB/s |
| 1 MiB GET | 32 | 2,901 ops/s, 2,900.7 MiB/s | 2,520 ops/s, 2,520.0 MiB/s |
| 64 MiB PUT | 8 | 11 ops/s, 730.8 MiB/s | 14 ops/s, 885.4 MiB/s |
| 64 MiB GET | 8 | 40 ops/s, 2,567.9 MiB/s | 34 ops/s, 2,154.5 MiB/s |

These are first-run measurements, not SLA numbers. The multi-gigabyte GET
figures are page-cache and memory-copy throughput, not physical disk bandwidth.

Durability cost was most visible for small and medium PUTs:

- 16 KiB PUT was about 4.9 times faster without fsync.
- 1 MiB PUT was about 4.3 times faster without fsync.
- 64 MiB PUT was about 1.2 times faster without fsync.

Three sustained fresh-key runs showed substantial device pressure and run-order
variance. Durable PUT ranges were 1,550–1,644 ops/s for 16 KiB, 394–556 MiB/s
for 1 MiB, and 209–731 MiB/s for 64 MiB. Non-durable ranges were even wider.
This confirms that the current single-host harness is unsuitable for firm
regression thresholds without cooling, cache control, randomized test order,
and more samples.

During a repeated overwrite workload, maximum server RSS was approximately
118 MiB in durable mode and 134 MiB with fsync disabled. That workload used at
most 64 concurrent small uploads, 32 medium uploads, and 8 large uploads; it did
not exercise the configured 4,096-connection ceiling with large uploads.

## Findings

### P1 — Concurrent upload memory can greatly exceed a practical budget

`internal/storage/local/blobs.go` uses 1 MiB pooled buffers, a two-entry channel,
and one writer goroutine per upload. At peak, an upload may have one buffer in
the writer, two queued, and one held by the reader attempting to enqueue: about
4 MiB per active upload before HTTP, hashing, metadata, and kernel buffers.

The default `max_connections` is 4,096 and there is no independent upload or
memory semaphore. A worst-case wave of large uploads can therefore demand on
the order of 16 GiB just for application upload buffers. `sync.Pool` can also
retain a large post-spike buffer population until later garbage collections.

Recommendation:

1. Add a global byte-budget semaphore for upload buffers.
2. Separate `max_concurrent_uploads` from `max_connections`.
3. Evaluate 256 KiB or adaptive buffers; measure before choosing.
4. Export active uploads, borrowed buffer bytes, queue wait, and rejected or
   throttled uploads.

### P1 — Durable small-object writes pay too many independent sync operations

Every durable blob performs a file fsync and a leaf-directory fsync before the
metadata commit. New shard directories also sync their parents. Blob IDs use a
two-level, 256 × 256 random shard layout. With 5,000 fresh objects, roughly
4,800 leaf directories are expected to be new, so early small-object workloads
pay directory creation and parent synchronization for nearly every object.

The metadata group committer is useful, but it only coalesces the final bbolt
transaction. It has no short collection window and cannot coalesce the earlier
file and directory sync work.

Recommendation:

1. Instrument sync count and sync latency separately for blob files,
   directories, and metadata.
2. Measure a shallower initial shard layout or a strategy that creates/syncs
   shard directories in batches.
3. Add a configurable, sub-millisecond metadata commit window and record actual
   batch-size distributions.
4. Preserve the current ordering guarantee: data and directory entries must be
   durable before committed metadata references them.

### P1 — The benchmark cannot support latency or capacity decisions yet

`cmd/objex-bench` reports only aggregate operations and bytes per second. It
does not retain individual durations, expose p50/p95/p99, sample server CPU or
RSS, distinguish cold and warm reads, or capture disk statistics.

For every PUT request it also recomputes SHA-256 over the same payload inside
the timed worker. With the benchmark client on the server machine, client-side
hashing competes for CPU and memory bandwidth and especially distorts large PUT
measurements. Workloads always run in the same order, further coupling results
to cache and device temperature.

Recommendation:

- precompute the payload hash once per workload;
- record a latency histogram and error categories;
- support duration-based tests and unique-key versus overwrite modes;
- randomize or select workload order;
- add warm-cache, cold-cache, and direct-device reporting;
- collect server CPU, RSS, GC, goroutines, bbolt commit time, and disk latency;
- run client and server on separate machines for network tests.

### P2 — Verified full reads trade sendfile efficiency for integrity

Full-segment GETs use `copyVerified`, reading through a 256 KiB userspace
buffer and calculating CRC32C. This is a sensible integrity default, but it
prevents the sendfile-style path used by unverified ranges and adds CPU and
memory-copy work to every full GET. Warm-cache GET results therefore primarily
measure CPU/memory throughput.

Keep verified reads as the safe default. Add separate benchmark modes for
verified and unverified reads, and expose checksum CPU time or bytes verified.
Only change the default after a product-level durability decision.

### P2 — Metadata has a single-writer and JSON scaling ceiling

bbolt gives simple transactional correctness and fast ordered scans, but all
metadata mutations ultimately serialize through one writer. Object, upload,
and part records are JSON, requiring allocation and decoding during GET, list,
multipart completion, scrub, and GC. Large metadata databases may also incur
mmap growth stalls, and `NoFreelistSync` trades cheaper commits for additional
startup reconstruction work.

This is appropriate for the current single-node scope, but object-count and
multipart-count limits have not been measured. Add 1 million, 10 million, and
100 million metadata-only test plans, including startup time, DB size, list
latency, overwrite latency, and memory mapping behavior.

### P2 — Multipart completion bypasses the group committer

`CompleteMultipart` uses `s.db.Update` directly. It therefore pays its own
durable bbolt transaction and can block the grouped writer while validating and
removing as many as 10,000 part records. This is correct but creates a potential
latency spike for unrelated writes.

Measure completion with 1, 100, 1,000, and 10,000 parts while concurrent PUTs
run. Consider separating validation from the shortest possible publication
transaction or routing completion through a transaction mechanism designed for
large jobs without starving small commits.

### P3 — Per-request authentication and metadata costs need profiles

Each authenticated request derives a SigV4 signing key through several HMACs,
and each request gets a cryptographically random request ID. Requests carrying
an Origin header also read bucket metadata before normal routing. These are
reasonable implementations, but at tens of thousands of cached GETs per second
they may become visible.

Do not optimize these speculatively. Capture CPU and allocation profiles first;
if confirmed, cache signing keys by access key/date/region/service and avoid
duplicate bucket lookups within one request.

## Positive design choices

- Object bodies and multipart parts stream; whole objects are not buffered.
- Hashing overlaps file writes.
- Buffer pools reduce steady-state allocation churn.
- Metadata commits can group concurrent durable writes.
- Object copy and whole-part copy use hard links when the filesystem permits.
- Prefix listings seek past complete common-prefix subtrees.
- Read leases make overwrite/delete safe for active readers.
- Range reads can use the efficient file-copy path.
- Request XML is bounded to 4 MiB.
- Slow upload bodies receive a rolling 60-second read deadline.

## Recommended performance work order

1. Make the benchmark decision-grade and add production metrics.
2. Add upload memory/backpressure controls.
3. Instrument and reduce durable small-object sync operations.
4. Profile verified GET, SigV4, JSON decode, and request-ID costs.
5. Load-test metadata scale and multipart completion contention.
6. Establish Linux/ext4 or XFS baselines on the intended production hardware;
   macOS/APFS results should remain development guidance only.

