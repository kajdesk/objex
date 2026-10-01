# Efficiency audit

Audit date: 2026-09-30  
Revision: `4b6de0a`

This audit covers memory, CPU, disk layout, background work, concurrency, test
efficiency, and operational behavior. Measured throughput is documented in
[`PERFORMANCE_AUDIT.md`](PERFORMANCE_AUDIT.md).

## Overall assessment

The implementation is compact and generally idiomatic for a single-node Go
service. It makes good use of streaming, immutable blobs, bbolt transactions,
pooled buffers, hard links, read leases, and bounded request bodies. The main
efficiency concern is that resource use is bounded by connection count rather
than by explicit byte, upload, and maintenance budgets.

| Area | Assessment | Main reason |
|---|---|---|
| Binary/container | Good | Static stripped image is about 8.6 MiB |
| Object streaming | Good | No whole-object buffering |
| Upload memory | High risk | Up to roughly 4 MiB of pipeline buffers per active upload |
| Read memory | Good | 256 KiB pooled buffer per verified full read |
| Small durable writes | Needs work | Per-object file/directory sync and new shard directories |
| Metadata writes | Good for one node | Group commit, but still one bbolt writer |
| Metadata representation | Moderate | JSON is simple but allocation- and space-heavy at scale |
| Reclamation | Moderate risk | Polls leased garbage every 200 ms; pending queue is unbounded |
| Scrub/GC | High operational risk | Scrub pacing is bursty and not cancellable during shutdown |
| Observability | Weak | No native latency, queue, memory-budget, bbolt, or disk metrics |
| Tests | Good baseline | Race suite passes; whole-module coverage is 69.2% |
| Performance regression tests | Missing | No `Benchmark*` functions or stable performance thresholds |

## Findings

### E1 — Add explicit resource budgets instead of relying on connections

The 4,096 default connection cap protects file descriptors and goroutine count,
but it is not a memory limit. A large PUT can occupy approximately four 1 MiB
buffers in the read/write pipeline. GETs, metadata operations, and idle clients
have very different costs but consume the same connection token.

Add independent limits for:

- active upload count;
- total upload-buffer bytes;
- active verified-read count or bytes in flight;
- maintenance bandwidth and IOPS;
- queued metadata commits.

Until those exist, do not treat 4,096 as a validated safe concurrency setting.
Choose a lower deployment value from workload-specific load tests; 256 is a
reasonable conservative starting point, not a universal recommendation.

### E2 — The upload pipeline can retain too much memory after bursts

The 1 MiB write buffers live in a global `sync.Pool`. Pooling is efficient at a
stable concurrency level, but a large spike can populate the pool with many
large arrays. Go may keep them until a later garbage collection, so RSS need
not fall immediately when traffic subsides.

Prefer a bounded buffer pool tied to a byte semaphore. This makes memory use
predictable and turns overload into queueing or explicit rejection instead of
heap growth. Test 256 KiB, 512 KiB, and 1 MiB buffers on both fast NVMe and
network-limited clients.

### E3 — Scrub rate limiting is bursty and shutdown-unfriendly

Scrub reads an entire blob at full speed, then calls `time.Sleep` to restore the
configured average rate. A 5 GiB segment at the default 64 MiB/s can lead to an
approximately 80-second pacing interval. At 1 MiB/s, the interval can approach
85 minutes. The sleep and blob verification do not observe server cancellation.

`Server.Serve` waits for background jobs after stopping them, so shutdown can
remain blocked well beyond its 30-second HTTP drain timeout when a scrub is in
progress.

Use a context-aware, token-bucket reader that throttles during the copy rather
than after a full blob. Propagate cancellation through `Scrub`, `verifyBlob`,
and pacing waits. Give maintenance shutdown its own bounded deadline.

### E4 — Reclamation repeatedly scans leased garbage

When overwritten blobs are still held by readers, the reclaimer puts their IDs
back into an unbounded pending slice and retries the whole set every 200 ms.
Long-lived or slow downloads can therefore cause repeated locking and scanning,
and continued overwrites can grow the queue indefinitely.

Signal the reclaimer when a lease count reaches zero. Keep leased IDs in a
deduplicated waiting set, use backoff for retry failures, and expose pending,
leased, deleted, and failed counts. A bounded queue should fall back to the
existing on-disk GC mechanism rather than block or consume unbounded RAM.

### E5 — Sharding favors huge scale at the expense of fresh small-object cost

The 256 × 256 directory layout caps files per leaf at very large object counts,
but it creates almost one new leaf directory per object in a fresh 5,000-object
test. Durable creation synchronizes parent directories, so the layout amplifies
small-object IOPS before shards are populated. Empty shard directories also
remain after object deletion, although their count is bounded.

Benchmark alternative layouts and batched directory preparation. Any change
must include upgrade/migration handling and must preserve crash ordering.

### E6 — Background GC performs many filesystem and metadata operations

GC walks every shard and stats candidate files. It opens a separate bbolt read
transaction for each non-empty leaf batch. At large object counts this becomes
an O(files + directories) periodic workload that competes with foreground I/O.
Some directory read errors are skipped silently.

Add progress/error metrics, a context, an IOPS limiter, and incremental cursor
state. Consider processing larger metadata batches or maintaining a durable
orphan journal so normal reclamation does not require full-tree scans.

### E7 — Integrity and compatibility intentionally spend CPU

A normal signed single-part PUT may calculate MD5 for the ETag, CRC32C for
internal integrity, and SHA-256 for SigV4 payload verification. An additional
S3 checksum can add another pass. A verified GET computes CRC32C while copying.
These costs are legitimate, but they make memory bandwidth and hashing likely
bottlenecks once storage is cached or very fast.

Preserve correctness. Use CPU profiles to decide whether checksum combination,
hardware acceleration, larger batches, or selective reuse of an already
computed compatible digest is worthwhile.

### E8 — bbolt and JSON need an object-count envelope

bbolt is an excellent low-complexity choice for a single process, but it has one
writer and memory-maps the metadata file. JSON records are easy to inspect and
upgrade, but use more bytes and allocations than a versioned binary encoding.
`NoFreelistSync` reduces commit traffic but increases startup reconstruction
work as the database grows.

Define and measure the supported envelope before changing formats. Required
tests include DB size, RSS, startup time, list latency, write latency, and scrub
cost at 1 million and 10 million objects; extrapolate cautiously before a
100-million-object run.

### E9 — Observability is insufficient for efficient production tuning

Access logs provide method, path, status, bytes, and whole-request duration,
but the system cannot currently distinguish authentication, upload receive,
hashing, blob sync, directory sync, commit queue, bbolt commit, response copy,
or maintenance time.

Add low-cardinality metrics for:

- request count and latency by S3 operation/status;
- active and queued uploads/downloads;
- bytes in flight and buffer-pool usage;
- commit batch size, queue delay, transaction time, and failures;
- file, directory, and metadata sync latency;
- reclaim queue size and deletion failures;
- GC/scrub progress, bandwidth, problems, and cancellation;
- Go heap, RSS, goroutines, file descriptors, and GC pauses.

Avoid bucket and object names as metric labels.

### E10 — Tests are solid but do not guard efficiency regressions

The race-enabled suite passes and combined coverage is 69.2%. Storage tests
cover concurrent durable writers, read/delete races, integrity, GC, multipart,
copy, listing, and data-directory exclusivity. End-to-end tests exercise the
official AWS SDK.

Missing performance safeguards include allocation tests, high-concurrency
slow-client tests, cancellable maintenance tests, 10,000-part completion under
load, metadata-scale tests, and reproducible Go benchmarks. Add `Benchmark*`
functions for hashing, record encode/decode, list scans, group commit, full and
range reads, and multipart completion, with `-benchmem` baselines in CI or a
dedicated performance workflow.

## Recommended efficiency work order

1. Bound upload memory and concurrency.
2. Make scrub and GC context-aware, smooth, and observable.
3. Replace leased-garbage polling with release-driven reclamation.
4. Instrument sync and commit behavior, then optimize small durable writes.
5. Add profiling and stable benchmark coverage.
6. Establish the supported metadata/object-count envelope.

