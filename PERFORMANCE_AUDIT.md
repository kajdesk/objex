# objex performance and efficiency audit

Audit date: 2026-09-30

Scope: the current working tree, including group-committed redb writes, blob
CRC32C verification, scheduled scrubbing, garbage collection, connection
limits, and `examples/bench.rs`. This is a local source audit plus a loopback
throughput baseline. It is not a production capacity certification.

## Executive summary

The current implementation delivers good single-node throughput in a warm,
local benchmark. With durability enabled it reached approximately 2,288 PUT/s
for 16 KiB objects, 768 MiB/s for 1 MiB PUTs, and 1.14 GiB/s for 64 MiB PUTs and
GETs. The group committer is clearly valuable.

The main risks appear under concurrency and large multipart objects rather than
in the happy-path throughput numbers:

1. Each GET occupies Tokio's shared blocking pool while it reads and may block
   there waiting for a slow client. Metadata operations use the same pool.
2. A multipart GET opens every segment before sending the response. A maximum
   10,000-part object therefore needs 10,000 file descriptors per request.
3. Physical deletion is performed serially in the request path after metadata
   commits, making overwrite, abort, and delete latency proportional to the
   number of segments removed.
4. GC and scrubbing are full-dataset scans with O(number of blobs) memory or
   metadata work and can overlap, producing periodic disk and latency spikes.
5. Object metadata stores the complete segment list as JSON. Listings and HEAD
   deserialize and copy data they usually do not need.

The first optimization should be an async, bounded read pipeline isolated from
metadata work. The second should be lazy segment opening plus asynchronous
physical reclamation.

## Measured baseline

The repository benchmark was compiled in release mode and run against a
temporary localhost server. The client and server shared the same machine and
filesystem, so GET results were heavily influenced by the page cache.

| Workload | Concurrency | Durable PUT | Durable GET | No-fsync PUT | No-fsync GET |
|---|---:|---:|---:|---:|---:|
| 5,000 × 16 KiB | 64 | 2,288 ops/s, 35.7 MiB/s | 27,255 ops/s, 425.9 MiB/s | 7,529 ops/s, 117.6 MiB/s | 21,427 ops/s, 334.8 MiB/s |
| 400 × 1 MiB | 32 | 768 ops/s, 768.4 MiB/s | 3,476 ops/s, 3,475.7 MiB/s | 837 ops/s, 837.2 MiB/s | 6,358 ops/s, 6,357.7 MiB/s |
| 16 × 64 MiB | 8 | 18 ops/s, 1,143.5 MiB/s | 18 ops/s, 1,138.2 MiB/s | 21 ops/s, 1,348.8 MiB/s | 20 ops/s, 1,285.1 MiB/s |

Interpretation:

- Durable small-object PUT throughput was about 30% of no-fsync throughput.
  File and directory synchronization dominates at this size despite metadata
  group commit.
- The durability penalty shrank substantially for larger objects because data
  transfer and hashing dominate fixed commit costs.
- GET differences between the two runs are benchmark noise/cache effects;
  `fsync` does not alter the GET path.
- Multi-GiB/s cached GET numbers are useful for detecting regressions, but they
  are not estimates of cold-disk, network, or multi-tenant throughput.

## Prioritized findings

### PERF-001 — Slow GETs can exhaust the shared blocking pool

**Priority:** P0  
**Impact:** throughput collapse and cross-request latency  
**Locations:** `src/storage/mod.rs:243-300`, `src/storage/local.rs:593-596`

Every object stream runs `ObjectReader::pump` in `tokio::task::spawn_blocking`.
The pump uses `blocking_send` into a four-item channel. If the client is slow,
the blocking task remains occupied waiting for channel capacity.

Metadata reads, redb scans, blob opens, and integrity work also use Tokio's same
blocking pool. Enough slow downloads can therefore occupy the pool and queue
unrelated HEAD, PUT, list, authorization-related bucket reads, and new GET file
opens. The connection limit does not prevent this: its default of 4,096 is well
above the blocking pool's practical thread count, and HTTP/2 can carry many
streams on one connection.

**Recommended change:** make the read pipeline asynchronous. Use
`tokio::fs::File`/`AsyncRead` and await channel capacity rather than blocking a
thread. If synchronous files are retained, use a dedicated, bounded I/O pool
and an async handoff so slow network consumers do not retain worker threads.
Keep metadata blocking work on a separate executor or dedicated redb thread.

**Measure:** concurrency sweep with fast and deliberately throttled GET clients;
track HEAD/PUT p99 latency while 100, 500, and 1,000 slow GETs are active.

### PERF-002 — Multipart GET eagerly opens every segment

**Priority:** P0  
**Impact:** file-descriptor exhaustion, high first-byte latency, failed reads  
**Location:** `src/storage/local.rs:650-660`

`open_record` opens and size-checks every blob before constructing the response.
A legal 10,000-part object consumes 10,000 file descriptors for one GET, even
for a range that touches one part. It also performs one `spawn_blocking` file
open per segment, sequentially, before the first response byte is available.
Concurrent multipart reads multiply this cost.

**Recommended change:** map the requested byte range to only the required
segments and open them lazily or in a small look-ahead window. Preserving reads
across concurrent deletion needs an explicit lifetime strategy: deferred blob
reclamation, reference leases, or a grace-period deletion queue. Deferred
reclamation pairs naturally with PERF-003.

**Measure:** first-byte latency and peak open descriptors for 1, 100, 1,000, and
10,000-part objects, including single-part ranges.

### PERF-003 — Blob deletion is serial and blocks request completion

**Priority:** P1  
**Impact:** long-tail latency for deletes, overwrites, aborts, and completion  
**Locations:** `src/storage/local.rs:619-627`, `src/storage/local.rs:680-723`,
`src/storage/local.rs:985-1008`, `src/storage/local.rs:1193-1205`

After metadata has committed, `discard` awaits one filesystem deletion at a
time. An overwrite of a large multipart object, a 1,000-key batch deletion, or
an aborted 10,000-part upload does not respond until all corresponding unlink
operations finish. Metadata already makes the blobs unreachable, and the GC
already provides eventual cleanup, so this synchronous work adds latency
without improving API visibility semantics.

**Recommended change:** enqueue obsolete blob IDs onto a durable or
reconstructable deletion queue and respond after the metadata commit. Drain the
queue with bounded concurrency and I/O-rate limiting. The existing full GC can
remain a repair mechanism rather than the primary deletion path.

### PERF-004 — GC and scrub scale as full-dataset jobs and can overlap

**Priority:** P1  
**Impact:** periodic memory growth, cache eviction, disk saturation, latency spikes  
**Locations:** `src/storage/local.rs:383-415`, `src/storage/local.rs:510-555`,
`src/server.rs:109-158`

GC walks every blob file, performs metadata calls for each filesystem entry,
then deserializes all object and part records into an in-memory `HashSet` of
referenced IDs. Scrub first materializes a vector for every referenced blob and
then reads every byte sequentially. Neither job is rate-limited.

With the default weekly scrub and hourly GC, both timers begin at server start;
their schedules can coincide every 168 hours. At scale this can create a large
memory allocation and simultaneous metadata, directory, and data scans.

**Recommended change:**

- maintain an incremental orphan/deletion journal so normal GC is proportional
  to recent mutations;
- stream metadata references rather than materializing all of them;
- process scrub entries in bounded pages;
- rate-limit scrub bandwidth and expose pause/resume controls;
- coordinate maintenance jobs so scrub and GC cannot overlap;
- persist progress so restarts do not repeatedly restart a full scrub.

### PERF-005 — Metadata layout makes cheap operations process full segment lists

**Priority:** P1  
**Impact:** high CPU, allocation, and read amplification for listings and HEAD  
**Locations:** `src/storage/local.rs:190-229`, `src/storage/local.rs:638-648`,
`src/storage/local.rs:1055-1118`

Each object is one JSON value containing its complete segment list. Reading the
record parses and allocates every blob ID. `ObjectRecord::info` also creates a
`Vec<u64>` of part sizes for multipart objects. ListObjects does this for as many
as 1,000 results even though its response needs only key, size, ETag, timestamp,
checksum summary, and storage class.

For 1,000 objects with 10,000 parts, a single listing can parse millions of
segment fields and copy tens of megabytes of part sizes that are never rendered.
JSON further increases stored bytes and parse cost.

**Recommended change:** split compact object summary metadata from segment
metadata. Keep listing fields in the primary object table and store parts in a
separate range-addressable table. At minimum, add a listing-specific projection
that does not build `ObjectInfo.parts`; this removes one avoidable allocation
but not JSON parse amplification. Consider a compact versioned binary encoding
after the table split.

### PERF-006 — Group commit is unbounded and only opportunistically batches

**Priority:** P1  
**Impact:** memory growth under overload and inconsistent small-write batching  
**Locations:** `src/storage/local.rs:318-369`, `src/storage/local.rs:579-616`

The metadata queue is an unbounded `std::sync::mpsc::channel`. Producers have no
backpressure if the disk or committer falls behind. Each queued closure retains
request-owned data until it runs.

After receiving the first job, the committer drains only jobs already present
with `try_recv`; it does not wait for a short batching window. Bursty arrivals
that miss that instant pay separate transactions and syncs. The measured 3.3×
small-PUT difference between durable and no-fsync modes shows that this path is
worth tuning.

**Recommended change:** use a bounded queue sized from a documented memory and
latency budget. Add a small configurable group-commit window (for example,
100–500 microseconds) or commit when either the window or batch-size limit is
reached. Reject or backpressure before accepting more upload work when the
queue is full. Benchmark p50/p99 latency as well as throughput; the best window
depends on the product latency target.

### PERF-007 — GET buffering allocates roughly a megabyte per active stream

**Priority:** P2  
**Impact:** allocation churn and multi-gigabyte memory potential at configured scale  
**Location:** `src/storage/mod.rs:215-300`

Each read allocates a fresh zeroed buffer for every 256 KiB chunk. The channel
holds up to four chunks, and the checksum design holds back another chunk; the
producer can also retain data while blocked. An active slow GET therefore
accounts for roughly 1–1.5 MiB of application buffering before Hyper/socket
buffers. At 4,096 connections, the theoretical application buffer footprint is
several GiB.

**Recommended change:** set explicit global and per-connection in-flight byte
budgets, reduce or adapt channel depth, and use a reusable bounded buffer pool.
Avoid zero-initialization when the subsequent read overwrites the entire slice,
using a safe initialized-buffer abstraction or vetted helper.

### PERF-008 — Integrity hashing duplicates work in common upload modes

**Priority:** P2  
**Impact:** avoidable CPU use on large PUTs and all full GETs  
**Locations:** `src/storage/mod.rs:105-188`, `src/storage/mod.rs:267-292`

Every upload computes MD5 plus the internal CRC32C. If the S3 checksum is also
CRC32C, the same CRC is computed a second time through `ChecksumHasher`. Signed
payloads may add SHA-256 as a third full pass. Every complete GET also computes
CRC32C, which is a deliberate integrity/throughput tradeoff.

**Recommended change:** reuse the internal CRC32C result when the requested S3
algorithm is CRC32C. Keep a single update pass that feeds only distinct hash
states. Verify that the chosen CRC implementation uses hardware acceleration on
supported targets. Preserve always-on read verification by default unless
product requirements explicitly favor throughput over corruption detection.

### PERF-009 — Large multipart copy and completion perform many serial operations

**Priority:** P2  
**Impact:** high latency for maximum-part objects  
**Locations:** `src/storage/local.rs:726-800`, `src/storage/local.rs:1011-1052`

CopyObject duplicates segments one at a time, and each hardlink placement may
sync a directory. Completing an upload reads and decodes each selected part,
then the commit transaction reads and decodes every selected part again to
preserve race safety. These are O(parts) as required, but with high constant
cost and no bounded parallelism.

**Recommended change:** batch hardlink creation and sync each affected directory
once. For completion, maintain an upload generation counter incremented by part
replacement; build from one consistent snapshot and validate the generation
once in the commit transaction. Retain the current per-part recheck until the
generation design is proven race-safe.

### PERF-010 — Authentication and CORS contain avoidable per-request linear work

**Priority:** P3  
**Impact:** CPU and metadata overhead at high request rates  
**Locations:** `src/auth.rs:280-321`, `src/config.rs:53-56`,
`src/s3/mod.rs:543-548`

Access keys are found with a linear scan, and each key's allowed buckets are
also scanned linearly. Requests carrying an Origin header perform an additional
bucket metadata read after the operation solely to apply CORS headers; anonymous
public reads may already have fetched the same bucket during authorization.

**Recommended change:** publish an immutable authentication snapshot containing
a hash map keyed by access key and sets for bucket restrictions. Carry bucket
metadata already read during authorization/execution into response decoration,
or add a small generation-invalidated bucket configuration cache.

## Benchmark and observability gaps

`examples/bench.rs` is useful as a smoke/regression benchmark, but it:

- reports one aggregate sample without warmup or repeated trials;
- records no p50, p95, p99, or maximum latency;
- runs client and server on the same host;
- collects each GET fully into client memory;
- spawns every operation as a Tokio task before the phase completes;
- does not separate cold-cache and warm-cache reads;
- records no server CPU, RSS, open descriptors, disk latency, sync latency,
  queue depth, blocking-pool saturation, or bytes in flight;
- does not test mixed read/write/delete/list traffic or slow clients;
- leaves access logging enabled, which adds formatting and output work.

Add a repeatable benchmark suite with:

1. concurrency sweeps rather than one chosen concurrency;
2. latency histograms and error counts;
3. warm-cache and cache-cold datasets larger than RAM;
4. small-object metadata-bound and large-object bandwidth-bound workloads;
5. multipart objects with 1, 100, 1,000, and 10,000 parts;
6. mixed workloads plus throttled clients;
7. CPU, RSS, allocation, file-descriptor, disk-I/O, and committer-queue metrics;
8. durable and no-fsync modes clearly reported as different products.

## Recommended optimization sequence

1. Replace blocking GET pumps with an async, bounded streaming design.
2. Open only range-relevant multipart segments with a small look-ahead window.
3. Move post-commit unlink work to a bounded background deletion queue.
4. Bound and deliberately time the metadata group-commit queue.
5. Split object summary metadata from multipart segment metadata.
6. Page and throttle GC/scrub, and prevent maintenance overlap.
7. Deduplicate CRC32C hashing and tune buffer size/pooling from profiles.
8. Index access keys and remove duplicate bucket reads.
9. Expand the benchmark and add operational metrics before lower-level tuning.

## Decisions needed before implementation

1. **Read integrity versus maximum throughput:** keep verification on every full
   read (recommended for object storage), or make it configurable in addition to
   scheduled scrub.
2. **Deletion semantics:** acknowledge after the metadata commit and reclaim
   asynchronously (recommended), or retain synchronous physical reclamation.
3. **Small-write latency target:** choose an acceptable group-commit delay so
   batching can be tuned against p99 latency.
4. **Capacity target:** define expected object count, average object size,
   multipart part count, concurrent requests, network speed, storage type, and
   minimum host memory. These determine sensible queue, buffer, and connection
   defaults.

## Validation status

- `cargo build --release --all-targets`: passed.
- `cargo test --all-targets`: passed — 32 unit tests and 8 integration tests.
- Both benchmark runs used temporary data directories that were removed after
  completion.
- Existing implementation changes were not modified by this audit.
