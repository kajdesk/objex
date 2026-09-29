//! Storage abstraction.
//!
//! The S3 API layer talks only to [`ObjectLayer`]. The single-node implementation
//! ([`local::LocalEngine`]) composes a redb metadata store with a [`BlobStore`]. A
//! distributed deployment replaces the blob store with an erasure-coded one that places
//! shards on remote nodes, and the metadata store with a replicated one. The API layer
//! does not change.

pub mod local;

use std::collections::BTreeMap;
use std::io::{self, SeekFrom};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, SubsecRound, Utc};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

use crate::checksum::{Checksum, ChecksumAlgo, ChecksumType};
use crate::error::{ErrorCode, S3Error, S3Result};

pub const MAX_OBJECT_SIZE: u64 = 5 * 1024 * 1024 * 1024 * 1024; // 5 TiB
pub const MAX_PUT_SIZE: u64 = 5 * 1024 * 1024 * 1024; // 5 GiB
pub const MIN_PART_SIZE: u64 = 5 * 1024 * 1024; // 5 MiB
pub const MAX_PART_NUMBER: u32 = 10_000;
pub const MAX_KEY_LEN: usize = 1024;

// ---------------------------------------------------------------------------
// Data sources
// ---------------------------------------------------------------------------

/// A stream of request body chunks.
#[async_trait]
pub trait ByteSource: Send {
    async fn next_chunk(&mut self) -> S3Result<Option<Bytes>>;

    /// A checksum sent as an HTTP trailer (aws-chunked). Only meaningful after
    /// `next_chunk` has returned `None`.
    fn trailing_checksum(&self) -> Option<Checksum> {
        None
    }
}

/// In-memory source, mostly for tests.
pub struct BytesSource(pub Option<Bytes>);

#[async_trait]
impl ByteSource for BytesSource {
    async fn next_chunk(&mut self) -> S3Result<Option<Bytes>> {
        Ok(self.0.take().filter(|b| !b.is_empty()))
    }
}

/// Source fed by an object read stream (used for server-side copies).
pub struct ChannelSource(pub mpsc::Receiver<io::Result<Bytes>>);

#[async_trait]
impl ByteSource for ChannelSource {
    async fn next_chunk(&mut self) -> S3Result<Option<Bytes>> {
        match self.0.recv().await {
            Some(Ok(b)) => Ok(Some(b)),
            Some(Err(e)) => Err(S3Error::internal(e)),
            None => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Blob storage
// ---------------------------------------------------------------------------

/// How a blob is laid out. Single-node blobs are 1 data shard + 0 parity on set 0.
/// Distributed mode spreads `data + parity` shards across the nodes of an erasure set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErasureLayout {
    #[serde(rename = "d")]
    pub data: u8,
    #[serde(rename = "p")]
    pub parity: u8,
    /// Erasure set (group of nodes/drives) holding the shards.
    #[serde(rename = "s")]
    pub set: u32,
}

impl Default for ErasureLayout {
    fn default() -> Self {
        ErasureLayout { data: 1, parity: 0, set: 0 }
    }
}

/// Reference to an immutable blob of object data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobRef {
    /// 128-bit random id, lowercase hex.
    pub id: String,
    #[serde(default, rename = "ec")]
    pub layout: ErasureLayout,
}

/// An open, readable blob. Reads are async so a reader waiting on a slow client
/// holds no thread.
pub trait BlobRead: AsyncRead + AsyncSeek + Send + Unpin {}
impl<T: AsyncRead + AsyncSeek + Send + Unpin> BlobRead for T {}

/// Streaming hash state for writes: MD5 (ETag), optional additional checksum,
/// and verification of client-supplied digests.
pub struct PutHasher {
    md5: md5::Md5,
    /// Internal integrity checksum, stored with every blob.
    crc32c: u32,
    sha256: Option<(sha2::Sha256, [u8; 32])>,
    content_md5: Option<[u8; 16]>,
    /// Requested S3 checksum algorithm.
    algo: Option<ChecksumAlgo>,
    /// Its hasher; None for CRC32C, which reuses the internal `crc32c`.
    checksum: Option<crate::checksum::ChecksumHasher>,
    expected_checksum: Option<Checksum>,
    expected_size: Option<u64>,
    pub size: u64,
}

pub struct PutDigest {
    pub md5: [u8; 16],
    pub crc32c: u32,
    pub size: u64,
    pub checksum: Option<Checksum>,
}

impl PutHasher {
    pub fn new(expect: &PutExpect) -> Self {
        use sha2::Digest;
        let algo = expect.checksum.as_ref().map(|c| c.algo).or(expect.checksum_algo);
        PutHasher {
            md5: md5::Md5::new(),
            crc32c: 0,
            sha256: expect.sha256.map(|h| (sha2::Sha256::new(), h)),
            content_md5: expect.content_md5,
            algo,
            checksum: algo.filter(|a| *a != ChecksumAlgo::Crc32c).map(crate::checksum::ChecksumHasher::new),
            expected_checksum: expect.checksum.clone(),
            expected_size: expect.size,
            size: 0,
        }
    }

    pub fn update(&mut self, data: &[u8]) {
        use sha2::Digest;
        self.md5.update(data);
        self.crc32c = crc32c::crc32c_append(self.crc32c, data);
        if let Some((h, _)) = &mut self.sha256 {
            h.update(data);
        }
        if let Some(c) = &mut self.checksum {
            c.update(data);
        }
        self.size += data.len() as u64;
    }

    /// Finish hashing and verify all expectations. `trailing` is a checksum that
    /// arrived in an HTTP trailer after the body.
    pub fn finish(self, trailing: Option<Checksum>) -> S3Result<PutDigest> {
        use sha2::Digest;
        if let Some(n) = self.expected_size
            && n != self.size
        {
            return Err(ErrorCode::IncompleteBody.into());
        }
        let md5: [u8; 16] = self.md5.finalize().into();
        if let Some(exp) = self.content_md5
            && exp != md5
        {
            return Err(ErrorCode::BadDigest.into());
        }
        if let Some((h, exp)) = self.sha256 {
            let got: [u8; 32] = h.finalize().into();
            if got != exp {
                return Err(ErrorCode::XAmzContentSHA256Mismatch.into());
            }
        }
        let mut checksum = None;
        let computed = match self.checksum {
            Some(h) => Some(h.finish()),
            None if self.algo == Some(ChecksumAlgo::Crc32c) => {
                use base64::Engine as _;
                let value = base64::engine::general_purpose::STANDARD.encode(self.crc32c.to_be_bytes());
                Some(Checksum { algo: ChecksumAlgo::Crc32c, value })
            }
            None => None,
        };
        if let Some(got) = computed {
            let expected = trailing.or(self.expected_checksum);
            if let Some(exp) = expected {
                if exp.algo != got.algo {
                    return Err(S3Error::msg(ErrorCode::InvalidRequest, "Checksum algorithm mismatch"));
                }
                if exp.value != got.value {
                    return Err(S3Error::msg(
                        ErrorCode::BadDigest,
                        format!("The {} you specified did not match the calculated checksum.", got.algo.name()),
                    ));
                }
            }
            checksum = Some(got);
        }
        Ok(PutDigest { md5, crc32c: self.crc32c, size: self.size, checksum })
    }
}

/// Storage of immutable data blobs. Local disk today; erasure-coded shards across
/// nodes in distributed mode.
#[async_trait]
pub trait BlobStore: Send + Sync + 'static {
    /// Stream `src` into a new blob, hashing as it goes. The blob is durable and
    /// verified when this returns; if anything fails nothing is left behind.
    async fn put(&self, src: &mut dyn ByteSource, hasher: PutHasher, max_size: u64) -> S3Result<(BlobRef, PutDigest)>;
    async fn open(&self, blob: &BlobRef) -> io::Result<Box<dyn BlobRead>>;
    async fn delete(&self, blob: &BlobRef) -> io::Result<()>;
    /// Make an independent copy of a blob (a hardlink when possible).
    async fn duplicate(&self, blob: &BlobRef) -> io::Result<BlobRef>;

    /// Duplicate several blobs. All or nothing: on error no copies remain.
    async fn duplicate_many(&self, blobs: &[BlobRef]) -> io::Result<Vec<BlobRef>> {
        let mut out = Vec::with_capacity(blobs.len());
        for b in blobs {
            match self.duplicate(b).await {
                Ok(d) => out.push(d),
                Err(e) => {
                    for d in &out {
                        let _ = self.delete(d).await;
                    }
                    return Err(e);
                }
            }
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// Object reads
// ---------------------------------------------------------------------------

const READ_CHUNK: usize = 256 * 1024;
/// Chunks buffered between the reader task and the HTTP response.
const STREAM_DEPTH: usize = 2;

/// One segment (blob) of an object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentRef {
    pub blob: BlobRef,
    pub size: u64,
    /// Expected CRC32C of the whole blob, when recorded.
    pub crc32c: Option<u32>,
}

/// Global limit on object bytes read from disk but not yet written to a socket.
/// Each chunk holds its share until the HTTP layer drops it, so slow clients push
/// back on disk reads instead of growing memory.
#[derive(Clone)]
pub struct ReadBudget {
    sem: Arc<Semaphore>,
    /// Size in KiB.
    total: u32,
}

impl ReadBudget {
    pub fn new(bytes: usize) -> Self {
        let total = bytes.div_ceil(1024).clamp(1, u32::MAX as usize >> 4) as u32;
        ReadBudget { sem: Arc::new(Semaphore::new(total as usize)), total }
    }

    async fn acquire(&self, bytes: usize) -> io::Result<OwnedSemaphorePermit> {
        // A chunk larger than the whole budget takes all of it rather than waiting forever.
        let kib = (bytes.div_ceil(1024) as u32).clamp(1, self.total);
        self.sem.clone().acquire_many_owned(kib).await.map_err(io::Error::other)
    }
}

/// A chunk of object data that returns its budget when dropped.
struct Budgeted {
    data: Bytes,
    _permit: OwnedSemaphorePermit,
}

impl AsRef<[u8]> for Budgeted {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

pub fn integrity_error(id: &str, what: &str) -> io::Error {
    tracing::error!("integrity: blob {id}: {what}");
    io::Error::other(format!("integrity error in blob {id}: {what}"))
}

/// An object's data, ready to stream. Segments are opened lazily, only those the
/// requested range touches, one at a time. `lease` keeps the blobs from being
/// reclaimed until the reader is dropped, even if the object is overwritten.
pub struct ObjectReader {
    blobs: Arc<dyn BlobStore>,
    segments: Vec<SegmentRef>,
    /// Offset of each segment within the object.
    starts: Vec<u64>,
    budget: ReadBudget,
    _lease: Option<Box<dyn Send + Sync>>,
}

impl ObjectReader {
    pub fn new(blobs: Arc<dyn BlobStore>, segments: Vec<SegmentRef>, budget: ReadBudget, lease: Option<Box<dyn Send + Sync>>) -> Self {
        let mut starts = Vec::with_capacity(segments.len());
        let mut at = 0;
        for s in &segments {
            starts.push(at);
            at += s.size;
        }
        ObjectReader { blobs, segments, starts, budget, _lease: lease }
    }

    /// Byte range (start, len) of part `n` (1-based) of a multipart object.
    pub fn part_range(&self, n: u32) -> Option<(u64, u64)> {
        let i = (n as usize).checked_sub(1)?;
        Some((*self.starts.get(i)?, self.segments[i].size))
    }

    /// Segment containing byte `pos`, and the offset within it.
    fn locate(&self, pos: u64) -> (usize, u64) {
        let i = self.starts.partition_point(|s| *s <= pos).saturating_sub(1);
        (i, pos - self.starts.get(i).copied().unwrap_or(0))
    }

    async fn open_segment(&self, i: usize, offset: u64) -> io::Result<Box<dyn BlobRead>> {
        let seg = &self.segments[i];
        let mut f = self.blobs.open(&seg.blob).await.map_err(|e| {
            if e.kind() == io::ErrorKind::NotFound { integrity_error(&seg.blob.id, "blob file is missing") } else { e }
        })?;
        let len = f.seek(SeekFrom::End(0)).await?;
        if len != seg.size {
            return Err(integrity_error(&seg.blob.id, &format!("size is {len}, expected {}", seg.size)));
        }
        f.seek(SeekFrom::Start(offset)).await?;
        Ok(f)
    }

    /// Start streaming `len` bytes from `start`. The first segment is opened before
    /// this returns, so a missing or truncated blob fails the request up front.
    /// Segments read in full are verified against their CRC32C; the last chunk of a
    /// segment is only sent once it verifies, so corrupt data never arrives complete.
    pub async fn open(self, start: u64, len: u64) -> io::Result<mpsc::Receiver<io::Result<Bytes>>> {
        let (tx, rx) = mpsc::channel(STREAM_DEPTH);
        if len == 0 || self.segments.is_empty() {
            return Ok(rx);
        }
        let (idx, offset) = self.locate(start);
        let first = self.open_segment(idx, offset).await?;
        tokio::spawn(async move {
            if let Err(e) = self.pump(first, idx, offset, len, &tx).await {
                let _ = tx.send(Err(e)).await;
            }
        });
        Ok(rx)
    }

    async fn pump(&self, first: Box<dyn BlobRead>, mut idx: usize, mut offset: u64, mut remaining: u64, tx: &mpsc::Sender<io::Result<Bytes>>) -> io::Result<()> {
        let mut next = Some(first);
        while remaining > 0 && idx < self.segments.len() {
            let seg = &self.segments[idx];
            let mut f = match next.take() {
                Some(f) => f,
                None => self.open_segment(idx, offset).await?,
            };
            let mut left = (seg.size - offset).min(remaining);
            let verify = seg.crc32c.filter(|_| offset == 0 && left == seg.size);
            let mut crc = 0u32;
            while left > 0 {
                let n = (left as usize).min(READ_CHUNK);
                // Never wait for budget while holding a budgeted chunk back: every
                // chunk is sent before the next is acquired, so readers cannot
                // deadlock each other (or themselves) on the shared budget.
                let permit = self.budget.acquire(n).await?;
                let mut buf = BytesMut::with_capacity(n);
                while buf.len() < n {
                    let want = (n - buf.len()) as u64;
                    if (&mut f).take(want).read_buf(&mut buf).await? == 0 {
                        return Err(integrity_error(&seg.blob.id, "blob is truncated"));
                    }
                }
                left -= n as u64;
                remaining -= n as u64;
                if let Some(want) = verify {
                    crc = crc32c::crc32c_append(crc, &buf);
                    // The segment's final chunk goes out only once the whole segment
                    // verifies, so corrupt data never arrives complete.
                    if left == 0 && crc != want {
                        return Err(integrity_error(&seg.blob.id, "checksum mismatch"));
                    }
                }
                let chunk = Bytes::from_owner(Budgeted { data: buf.freeze(), _permit: permit });
                if tx.send(Ok(chunk)).await.is_err() {
                    return Ok(()); // client went away
                }
            }
            idx += 1;
            offset = 0;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct CorsRule {
    pub id: Option<String>,
    pub allowed_origins: Vec<String>,
    pub allowed_methods: Vec<String>,
    pub allowed_headers: Vec<String>,
    pub expose_headers: Vec<String>,
    pub max_age_seconds: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketInfo {
    pub name: String,
    pub created: DateTime<Utc>,
    #[serde(default)]
    pub public_read: bool,
    #[serde(default)]
    pub cors: Option<Vec<CorsRule>>,
}

pub enum BucketUpdate {
    PublicRead(bool),
    Cors(Option<Vec<CorsRule>>),
}

/// Standard + user metadata stored with an object.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ObjectMetadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_encoding: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_disposition: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_language: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_control: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires: Option<String>,
    /// x-amz-meta-* (names lowercased, without the prefix)
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub user: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct ObjectInfo {
    pub key: String,
    pub size: u64,
    /// Hex MD5, or "hex-N" for multipart objects. Without quotes.
    pub etag: String,
    pub last_modified: DateTime<Utc>,
    pub meta: ObjectMetadata,
    pub checksum: Option<Checksum>,
    /// Number of parts for multipart objects, 0 otherwise. Part sizes are available
    /// from [`ObjectReader::part_range`].
    pub parts_count: u32,
}

#[derive(Debug, Clone, Default)]
pub struct PutExpect {
    pub content_md5: Option<[u8; 16]>,
    pub sha256: Option<[u8; 32]>,
    /// Checksum value supplied in a header.
    pub checksum: Option<Checksum>,
    /// Algorithm to compute (value may arrive in a trailer, or not at all).
    pub checksum_algo: Option<ChecksumAlgo>,
    pub size: Option<u64>,
}

/// Conditional-write headers (If-Match / If-None-Match on PUT and CompleteMultipartUpload).
#[derive(Debug, Clone, Default)]
pub struct WriteConditions {
    pub if_match: Option<String>,
    pub if_none_match: Option<String>,
}

impl WriteConditions {
    pub fn check(&self, existing: Option<&str>) -> S3Result<()> {
        if let Some(im) = &self.if_match {
            match existing {
                None => return Err(ErrorCode::NoSuchKey.into()),
                Some(etag) if !etag_matches(im, etag) => return Err(ErrorCode::PreconditionFailed.into()),
                _ => {}
            }
        }
        if let Some(inm) = &self.if_none_match
            && let Some(etag) = existing
            && etag_matches(inm, etag)
        {
            return Err(ErrorCode::PreconditionFailed.into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct PutOptions {
    pub meta: ObjectMetadata,
    pub expect: PutExpect,
    pub cond: WriteConditions,
}

/// Conditional-read headers (GET/HEAD, and x-amz-copy-source-if-*).
#[derive(Debug, Clone, Default)]
pub struct ReadConditions {
    pub if_match: Option<String>,
    pub if_none_match: Option<String>,
    pub if_modified_since: Option<DateTime<Utc>>,
    pub if_unmodified_since: Option<DateTime<Utc>>,
}

impl ReadConditions {
    /// Evaluate per RFC 7232 as S3 does. For copy sources, "not modified" outcomes
    /// become PreconditionFailed.
    pub fn check(&self, info: &ObjectInfo, copy_source: bool) -> S3Result<()> {
        let modified = info.last_modified.trunc_subsecs(0);
        if let Some(im) = &self.if_match {
            if !etag_matches(im, &info.etag) {
                return Err(ErrorCode::PreconditionFailed.into());
            }
        } else if let Some(t) = self.if_unmodified_since
            && modified > t
        {
            return Err(ErrorCode::PreconditionFailed.into());
        }
        let not_modified = if copy_source { ErrorCode::PreconditionFailed } else { ErrorCode::NotModified };
        if let Some(inm) = &self.if_none_match {
            if etag_matches(inm, &info.etag) {
                return Err(not_modified.into());
            }
        } else if let Some(t) = self.if_modified_since
            && modified <= t
        {
            return Err(not_modified.into());
        }
        Ok(())
    }
}

/// Match an If-Match / If-None-Match header value against an ETag.
pub fn etag_matches(header: &str, etag: &str) -> bool {
    header.split(',').map(|s| s.trim()).any(|t| {
        t == "*" || t.trim_start_matches("W/").trim_matches('"') == etag
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeSpec {
    /// bytes=first-[last]
    FromTo(u64, Option<u64>),
    /// bytes=-n
    Suffix(u64),
}

impl RangeSpec {
    /// Parse a Range header. Unsupported/malformed ranges yield None (ignored, as S3 does).
    pub fn parse(h: &str) -> Option<RangeSpec> {
        let spec = h.trim().strip_prefix("bytes=")?;
        if spec.contains(',') {
            return None;
        }
        let (a, b) = spec.split_once('-')?;
        let (a, b) = (a.trim(), b.trim());
        if a.is_empty() {
            return b.parse().ok().map(RangeSpec::Suffix);
        }
        let first: u64 = a.parse().ok()?;
        if b.is_empty() {
            return Some(RangeSpec::FromTo(first, None));
        }
        let last: u64 = b.parse().ok()?;
        (last >= first).then_some(RangeSpec::FromTo(first, Some(last)))
    }

    /// Resolve against an object size to (start, length).
    pub fn resolve(self, size: u64) -> S3Result<(u64, u64)> {
        match self {
            RangeSpec::FromTo(first, last) => {
                if first >= size {
                    return Err(ErrorCode::InvalidRange.into());
                }
                let last = last.unwrap_or(u64::MAX).min(size - 1);
                Ok((first, last - first + 1))
            }
            RangeSpec::Suffix(n) => {
                if n == 0 || size == 0 {
                    return Err(ErrorCode::InvalidRange.into());
                }
                let n = n.min(size);
                Ok((size - n, n))
            }
        }
    }
}

pub struct CopyOptions {
    /// None = copy the source metadata; Some = replace it.
    pub replace_meta: Option<ObjectMetadata>,
    pub src_cond: ReadConditions,
    pub dst_cond: WriteConditions,
}

#[derive(Debug, Clone, Default)]
pub struct ListQuery {
    pub prefix: String,
    pub delimiter: Option<String>,
    /// Exclusive start key.
    pub marker: String,
    pub max_keys: usize,
}

#[derive(Debug, Default)]
pub struct ListResult {
    pub objects: Vec<ObjectInfo>,
    pub prefixes: Vec<String>,
    pub truncated: bool,
    /// Last key or common prefix returned, when truncated.
    pub next_marker: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PartInfo {
    pub number: u32,
    pub etag: String,
    pub size: u64,
    pub last_modified: DateTime<Utc>,
    pub checksum: Option<Checksum>,
}

#[derive(Debug, Clone)]
pub struct UploadInfo {
    pub key: String,
    pub upload_id: String,
    pub initiated: DateTime<Utc>,
    pub checksum_algo: Option<ChecksumAlgo>,
    pub checksum_type: Option<ChecksumType>,
}

#[derive(Debug)]
pub struct ListPartsResult {
    pub upload: UploadInfo,
    pub parts: Vec<PartInfo>,
    pub truncated: bool,
    pub next_marker: u32,
}

#[derive(Debug, Clone, Default)]
pub struct ListUploadsQuery {
    pub prefix: String,
    pub delimiter: Option<String>,
    pub key_marker: String,
    pub upload_id_marker: String,
    pub max_uploads: usize,
}

#[derive(Debug, Default)]
pub struct ListUploadsResult {
    pub uploads: Vec<UploadInfo>,
    pub prefixes: Vec<String>,
    pub truncated: bool,
    pub next_key_marker: String,
    pub next_upload_id_marker: String,
}

pub struct CompletePart {
    pub number: u32,
    pub etag: String,
    /// Part checksum as listed by the client, verified against the stored one.
    pub checksum: Option<Checksum>,
}

#[derive(Debug, Clone, Default)]
pub struct CompleteOptions {
    pub cond: WriteConditions,
    /// Full-object checksum supplied by the client (x-amz-checksum-* header).
    pub checksum: Option<Checksum>,
}

// ---------------------------------------------------------------------------
// The object layer
// ---------------------------------------------------------------------------

#[async_trait]
pub trait ObjectLayer: Send + Sync + 'static {
    async fn list_buckets(&self) -> S3Result<Vec<BucketInfo>>;
    async fn create_bucket(&self, name: &str, public_read: bool) -> S3Result<()>;
    async fn get_bucket(&self, name: &str) -> S3Result<BucketInfo>;
    async fn update_bucket(&self, name: &str, update: BucketUpdate) -> S3Result<()>;
    async fn delete_bucket(&self, name: &str) -> S3Result<()>;

    async fn put_object(&self, bucket: &str, key: &str, src: &mut dyn ByteSource, opts: PutOptions) -> S3Result<ObjectInfo>;
    async fn head_object(&self, bucket: &str, key: &str) -> S3Result<ObjectInfo>;
    /// Returns the object info plus a reader for its data (nothing is opened until
    /// [`ObjectReader::open`]); conditions/ranges are evaluated by the caller.
    async fn get_object(&self, bucket: &str, key: &str) -> S3Result<(ObjectInfo, ObjectReader)>;
    async fn delete_object(&self, bucket: &str, key: &str) -> S3Result<()>;
    async fn delete_objects(&self, bucket: &str, keys: &[String]) -> S3Result<Vec<S3Result<()>>>;
    async fn copy_object(&self, src_bucket: &str, src_key: &str, bucket: &str, key: &str, opts: CopyOptions) -> S3Result<ObjectInfo>;
    async fn list_objects(&self, bucket: &str, query: ListQuery) -> S3Result<ListResult>;

    async fn create_multipart(&self, bucket: &str, key: &str, meta: ObjectMetadata, checksum: Option<(ChecksumAlgo, ChecksumType)>) -> S3Result<String>;
    async fn upload_part(&self, bucket: &str, key: &str, upload_id: &str, part: u32, src: &mut dyn ByteSource, expect: PutExpect) -> S3Result<PartInfo>;
    /// `range` is an inclusive (first, last) byte range of the source.
    async fn upload_part_copy(&self, src_bucket: &str, src_key: &str, range: Option<(u64, u64)>, cond: ReadConditions, bucket: &str, key: &str, upload_id: &str, part: u32) -> S3Result<(PartInfo, ObjectInfo)>;
    async fn complete_multipart(&self, bucket: &str, key: &str, upload_id: &str, parts: Vec<CompletePart>, opts: CompleteOptions) -> S3Result<ObjectInfo>;
    async fn abort_multipart(&self, bucket: &str, key: &str, upload_id: &str) -> S3Result<()>;
    async fn list_parts(&self, bucket: &str, key: &str, upload_id: &str, marker: u32, max_parts: usize) -> S3Result<ListPartsResult>;
    async fn list_uploads(&self, bucket: &str, query: ListUploadsQuery) -> S3Result<ListUploadsResult>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranges() {
        assert_eq!(RangeSpec::parse("bytes=0-9").unwrap().resolve(100).unwrap(), (0, 10));
        assert_eq!(RangeSpec::parse("bytes=90-").unwrap().resolve(100).unwrap(), (90, 10));
        assert_eq!(RangeSpec::parse("bytes=-10").unwrap().resolve(100).unwrap(), (90, 10));
        assert_eq!(RangeSpec::parse("bytes=50-500").unwrap().resolve(100).unwrap(), (50, 50));
        assert_eq!(RangeSpec::parse("bytes=-500").unwrap().resolve(100).unwrap(), (0, 100));
        assert!(RangeSpec::parse("bytes=100-").unwrap().resolve(100).is_err());
        assert!(RangeSpec::parse("bytes=5-1").is_none());
        assert!(RangeSpec::parse("bytes=0-1,5-6").is_none());
    }

    #[test]
    fn etags() {
        assert!(etag_matches("\"abc\"", "abc"));
        assert!(etag_matches("\"x\", \"abc\"", "abc"));
        assert!(etag_matches("*", "abc"));
        assert!(!etag_matches("\"abd\"", "abc"));
    }
}
