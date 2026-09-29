# objex code audit

Audit date: 2026-09-30

Scope: the Rust server, S3 request handling, authentication, local blob storage,
metadata transactions, multipart uploads, checksums, configuration, and the
existing test suite. This is a source review and local test pass, not a formal
penetration test or filesystem power-failure test.

## Executive summary

The normal-path implementation is in good shape for an early single-node
server: all 27 unit tests and all 8 end-to-end tests pass. The most important
remaining problem is a multipart completion race that can publish an object
whose blob has already been deleted. There are also two high-priority local
deployment issues: secret-bearing config files are created with permissive file
permissions, and starting a second process against the same data directory can
delete the first process's active temporary uploads before database locking has
a chance to reject it.

Recommended order:

1. Make multipart completion and part replacement transactionally safe.
2. Enforce a single owner of the data directory before touching `tmp/`.
3. Create and replace configuration files with owner-only permissions.
4. Close the directory-fsync crash-consistency gap.
5. Add read verification/scrubbing and fix the smaller protocol issues.

## Findings

### OBJ-001 — Multipart completion can publish deleted data

**Severity:** Critical (data integrity)  
**Status:** Confirmed by source-level concurrency analysis  
**Locations:** `src/storage/local.rs:458-486`, `src/storage/local.rs:830-899`

`complete_multipart` reads the upload and selected `PartRecord`s in separate read
transactions, constructs an `ObjectRecord`, and only later calls
`commit_object`. The commit transaction verifies that the upload still exists,
but it does not verify that each selected part still maps to the same blob.

A damaging interleaving is:

1. Completion reads part 1 as blob A.
2. A concurrent `UploadPart` replaces part 1 with blob B and commits.
3. Part replacement deletes blob A.
4. Completion commits an object whose segment still references blob A.
5. Completion removes blob B as an unused part.

The completion request can therefore succeed while the resulting object is
unreadable. Garbage collection cannot repair it because the missing blob is
referenced by object metadata.

**Recommended fix:** perform selection validation and object publication in one
write transaction. At minimum, re-read every requested part inside the final
transaction and compare part number, ETag, checksum, size, and blob ID with the
records used to construct the object. A cleaner design is to build the final
record from the part records read inside that transaction. Delete obsolete
blobs only after the transaction commits.

**Regression test:** use a controllable/fake `BlobStore` or transaction barrier
to pause completion after its initial read, replace a selected part, then resume.
The result must be either the newer consistent part set or an `InvalidPart`/
conflict response; it must never reference a deleted blob.

### OBJ-002 — A second startup can destroy active temporary uploads

**Severity:** High (availability and in-flight data loss)  
**Status:** Confirmed by source review  
**Location:** `src/storage/local.rs:31-38`, before `Database::create` at
`src/storage/local.rs:330`

`FileBlobStore::new` unconditionally removes the entire shared `tmp/` directory.
This happens before redb is opened. If one server is running and another server
is accidentally started with the same data directory, the second process can
delete files being written by the first process and only afterward fail to open
the locked metadata database.

**Recommended fix:** acquire an exclusive data-directory/process lock before
any cleanup or mutation. Keep the lock for the server lifetime. Prefer unique
per-process/per-upload temp directories, and clean only stale files after the
exclusive lock is held.

**Regression test:** hold the data directory open in one engine, create an
in-flight temp file, and attempt to open a second engine. The second open must
fail without changing the temp file.

### OBJ-003 — Access-key config is created world-readable

**Severity:** High on shared Unix hosts (credential disclosure)  
**Status:** Reproduced locally  
**Locations:** `src/config.rs:123-127`, `src/config.rs:139-142`

Both initialization and atomic key updates use `std::fs::write`, so permissions
come from the process umask. With the common `022` umask, an `objex.toml`
containing `secret_key` values is created as `0644`. The audit reproduced:

```text
-rw-r--r-- 644 objex.toml
```

The temporary replacement file also contains every secret and is created the
same way.

**Recommended fix:** on Unix, create the initial file and every temporary file
with mode `0600` using `OpenOptionsExt::mode(0o600)`, and explicitly reduce the
mode of an existing destination during a key update. Document equivalent ACL
requirements on non-Unix systems. Fsync the file and parent directory as part of
the atomic replacement.

**Regression test:** set a permissive umask in a subprocess, run `init` and
`key add`, and assert that both final files are owner-readable/writable only.

### OBJ-004 — “Durable by default” has a directory-fsync gap

**Severity:** High/Medium depending on filesystem and crash model  
**Status:** Confirmed design gap; requires power-failure testing to reproduce  
**Location:** `src/storage/local.rs:50-63`

The blob file is synced and, after rename, only its immediate leaf directory is
synced. `create_dir_all` may have just created one or both sharding directories
(`blobs/ab` and `blobs/ab/cd`). Their entries in their parent directories are
not synced. On filesystems that require parent-directory fsync for durable
directory creation, a crash can lose part of the directory path after the
server has acknowledged the write and committed metadata.

**Recommended fix:** create shard directories in a helper that detects newly
created components and fsyncs each directory and its parent in bottom-up order.
Document the exact durability guarantee by platform/filesystem. Add a
fault-injection or VM power-cut test before retaining the unconditional
“durable by default” claim.

### OBJ-005 — Stored checksums are not used to detect corruption on reads

**Severity:** Medium (silent data corruption)  
**Status:** Confirmed by source review  
**Locations:** `src/storage/mod.rs:223-258`, `src/storage/local.rs:391-396`

Uploads verify and store checksums, but GET streams bytes directly from blob
files without verifying length, ETag, or a stored checksum. A same-length disk
corruption is returned as successful data. When checksum mode is enabled, the
response can include the original stored checksum even though the returned
bytes no longer match it; only a diligent client will detect the mismatch.

**Recommended fix:** decide on an integrity policy. Options include verification
on every full GET, a background scrubber plus verification on suspicious reads,
or storing a fast per-blob checksum distinct from optional S3 checksums. At a
minimum, expose a scrub command/metric and turn missing or truncated blobs into
a clearly logged integrity error.

### OBJ-006 — CompleteMultipartUpload accepts malformed checksum suffixes

**Severity:** Medium/Low (integrity-validation and compatibility)  
**Status:** Confirmed by source review  
**Location:** `src/storage/local.rs:888-894`

The supplied and computed checksum strings are compared only up to the first
`-`. Consequently, a correct digest followed by an arbitrary suffix is accepted
(for example, `digest-not-the-part-count`). A full-object checksum with an
invented suffix is accepted as well. The suffix is meaningful for composite
multipart checksums and should not be discarded.

**Recommended fix:** validate base64 and compare the complete canonical value.
For composite checksums, require and validate the exact `-N` suffix. For
full-object checksums, reject a suffix.

### OBJ-007 — Range requests against empty objects return 200 instead of 416

**Severity:** Low (S3/HTTP compatibility)  
**Status:** Confirmed by control flow  
**Location:** `src/s3/object.rs:127-152`

Range resolution is guarded by `size > 0`. Any syntactically valid range on a
zero-byte object falls through to a normal full response with status 200. No
byte range is satisfiable for an empty representation, so this should return
`InvalidRange`/416 with `Content-Range: bytes */0`.

**Recommended fix:** resolve every parsed range regardless of object size and
let `RangeSpec::resolve(0)` return `InvalidRange`. Add GET and HEAD integration
tests for an empty object.

### OBJ-008 — The server has no built-in transport security or resource limits

**Severity:** Medium operational risk  
**Status:** Deployment limitation  
**Locations:** `src/server.rs`, README configuration examples

The listener serves plaintext HTTP and has no configured request-header/body
timeouts, connection cap, per-key concurrency limit, bandwidth limit, or storage
quota. Object bodies are streamed and XML is capped, which helps memory use, but
slow clients and many concurrent requests can still hold sockets, tasks, file
descriptors, and temporary disk space indefinitely.

**Recommended fix:** for the first release, explicitly require a TLS-terminating
reverse proxy and document tested timeout/body/connection settings. Add server
side concurrency and temporary-storage limits so a proxy is not the sole
resource boundary. Consider per-key/bucket quotas before multi-tenant use.

## Test and tooling results

- `cargo test --all-targets`: passed — 27 unit tests and 8 integration tests.
- `cargo check --all-targets`: passed.
- The integration tests require permission to bind local TCP ports; their first
  sandboxed run failed at listener creation, then passed outside the sandbox.
- `cargo clippy --all-targets -- -D warnings`: not run because the installed
  toolchain does not include the Clippy component.
- `cargo fmt --all -- --check`: not run because the installed toolchain does not
  include the rustfmt component.
- No dependency-vulnerability scan or destructive power-failure test was run.

## Missing regression coverage

The current tests cover normal SDK operations well, but should add:

- concurrent UploadPart versus CompleteMultipartUpload;
- a second process opening the same data directory;
- crash/fault injection between file sync, directory creation, rename, and
  metadata commit;
- altered, missing, and truncated blob files;
- empty-object ranges;
- strict multipart checksum suffix and base64 validation;
- secret-file permissions;
- slow-client and concurrency/resource-limit behavior.

## Decisions needed before implementation

1. **Single-process contract:** enforce a process lock (recommended for the
   current architecture), or intentionally support multiple processes sharing a
   data directory. The latter needs substantially more coordination than redb's
   database lock.
2. **Integrity policy:** verify every read, provide scheduled scrubbing, or use a
   hybrid. A hybrid—cheap per-blob verification plus scheduled full scrubs—is a
   reasonable starting point.
3. **TLS boundary:** keep TLS at a required reverse proxy for the first release
   (recommended), or add native TLS and certificate lifecycle management.
4. **Durability claim:** either implement and test full directory durability or
   narrow the README claim to the tested filesystems and failure model.
