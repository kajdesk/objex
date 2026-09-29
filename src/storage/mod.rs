//! Storage abstraction.
//!
//! The S3 API layer talks only to [`ObjectLayer`]. The single-node implementation
//! ([`local::LocalEngine`]) composes a redb metadata store with a [`BlobStore`]. A
//! distributed deployment replaces the blob store with an erasure-coded one that places
//! shards on remote nodes, and the metadata store with a replicated one. The API layer
//! does not change.

pub mod local;

use std::collections::BTreeMap;
use std::io::{self, Read, Seek, SeekFrom};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, SubsecRound, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

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

/// A readable handle to one blob. Opened before a response starts so that
/// concurrent overwrites/deletes cannot pull the data out from under a reader.
pub trait BlobRead: Read + Seek + Send {}
impl<T: Read + Seek + Send> BlobRead for T {}

/// Streaming hash state for writes: MD5 (ETag), optional additional checksum,
/// and verification of client-supplied digests.
pub struct PutHasher {
    md5: md5::Md5,
    /// Internal integrity checksum, stored with every blob.
    crc32c: u32,
    sha256: Option<(sha2::Sha256, [u8; 32])>,
    content_md5: Option<[u8; 16]>,
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
            checksum: algo.map(crate::checksum::ChecksumHasher::new),
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
        if let Some(h) = self.checksum {
            let got = h.finish();
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
}

// ---------------------------------------------------------------------------
// Object reads
// ---------------------------------------------------------------------------

const READ_CHUNK: usize = 256 * 1024;

/// One opened blob of an object.
pub struct ReadSegment {
    pub blob: Box<dyn BlobRead>,
    pub size: u64,
    /// Expected CRC32C of the whole blob, when recorded.
    pub crc32c: Option<u32>,
    /// Blob id, for integrity error reports.
    pub id: String,
}

/// Opened object data, ready to stream. Holding it keeps the data readable even
/// if the object is overwritten or deleted meanwhile.
pub struct ObjectReader {
    segments: Vec<ReadSegment>,
}

pub fn integrity_error(id: &str, what: &str) -> io::Error {
    tracing::error!("integrity: blob {id}: {what}");
    io::Error::other(format!("integrity error in blob {id}: {what}"))
}

impl ObjectReader {
    pub fn new(segments: Vec<ReadSegment>) -> Self {
        ObjectReader { segments }
    }

    /// Stream `len` bytes starting at `start` into a channel from a blocking thread.
    /// Segments read in full are verified against their CRC32C; the last chunk of a
    /// segment is only sent once it verifies, so corrupt data never arrives complete.
    pub fn stream(self, start: u64, len: u64) -> mpsc::Receiver<io::Result<Bytes>> {
        let (tx, rx) = mpsc::channel(4);
        tokio::task::spawn_blocking(move || {
            if let Err(e) = self.pump(start, len, &tx) {
                let _ = tx.blocking_send(Err(e));
            }
        });
        rx
    }

    fn pump(self, mut start: u64, mut remaining: u64, tx: &mpsc::Sender<io::Result<Bytes>>) -> io::Result<()> {
        for mut seg in self.segments {
            if remaining == 0 {
                break;
            }
            if start >= seg.size {
                start -= seg.size;
                continue;
            }
            seg.blob.seek(SeekFrom::Start(start))?;
            let mut left = (seg.size - start).min(remaining);
            let verify = seg.crc32c.filter(|_| start == 0 && left == seg.size);
            start = 0;
            let mut crc = 0u32;
            let mut held: Option<Bytes> = None;
            while left > 0 {
                let n = (left as usize).min(READ_CHUNK);
                let mut buf = BytesMut::zeroed(n);
                seg.blob.read_exact(&mut buf).map_err(|e| {
                    if e.kind() == io::ErrorKind::UnexpectedEof { integrity_error(&seg.id, "blob is truncated") } else { e }
                })?;
                left -= n as u64;
                remaining -= n as u64;
                if verify.is_some() {
                    crc = crc32c::crc32c_append(crc, &buf);
                }
                if let Some(prev) = held.take()
                    && tx.blocking_send(Ok(prev)).is_err()
                {
                    return Ok(()); // client went away
                }
                held = Some(buf.freeze());
            }
            if let Some(want) = verify
                && crc != want
            {
                return Err(integrity_error(&seg.id, "checksum mismatch"));
            }
            if let Some(last) = held
                && tx.blocking_send(Ok(last)).is_err()
            {
                return Ok(());
            }
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
    /// Sizes of the parts for multipart objects, empty otherwise.
    pub parts: Vec<u64>,
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
    /// Returns the object info plus opened data; conditions/ranges are evaluated by the caller.
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
