//! Single-node engine: metadata in redb, object data in immutable blob files.

use std::collections::HashSet;
use std::io;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition, WriteTransaction};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

use super::*;
use crate::checksum::{self, ChecksumType};
use crate::util::{random_hex, valid_bucket_name};

// ---------------------------------------------------------------------------
// Blob files
// ---------------------------------------------------------------------------

/// Flush a file or directory to the storage device.
///
/// On Apple platforms this is a plain `fsync()` rather than `F_FULLFSYNC` (which is
/// what `File::sync_all` issues there, and which flushes the whole drive cache and
/// serializes across the machine). Durability comes from the metadata commit that
/// always follows: redb syncs with `F_FULLFSYNC`, which also flushes everything
/// written to the drive before it.
fn sync_path_or_file(f: &std::fs::File) -> io::Result<()> {
    #[cfg(target_vendor = "apple")]
    {
        use std::os::fd::AsRawFd;
        // SAFETY: fsync on a valid, open descriptor owned by `f`.
        if unsafe { libc::fsync(f.as_raw_fd()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
    #[cfg(not(target_vendor = "apple"))]
    f.sync_all()
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    sync_path_or_file(&std::fs::File::open(dir)?)
}

/// Blobs stored as files under `<root>/blobs/ab/cd/<id>`, written via `<root>/tmp`.
pub struct FileBlobStore {
    root: PathBuf,
    fsync: bool,
    /// Shard directories whose entry in their parent is known to be durable.
    durable_dirs: Arc<std::sync::Mutex<HashSet<PathBuf>>>,
}

impl FileBlobStore {
    pub fn new(root: &Path, fsync: bool) -> io::Result<Self> {
        std::fs::create_dir_all(root.join("blobs"))?;
        let tmp = root.join("tmp");
        // Nothing can be in flight at startup, so leftovers are garbage.
        if tmp.exists() {
            std::fs::remove_dir_all(&tmp)?;
        }
        std::fs::create_dir_all(&tmp)?;
        if fsync {
            sync_dir(root)?;
        }
        Ok(FileBlobStore { root: root.to_path_buf(), fsync, durable_dirs: Default::default() })
    }

    fn path(&self, id: &str) -> PathBuf {
        self.root.join("blobs").join(&id[0..2]).join(&id[2..4]).join(id)
    }

    fn blobs_dir(&self) -> PathBuf {
        self.root.join("blobs")
    }

    /// Move (or link) `from` into place as blob `id`. With fsync, every shard
    /// directory on the path, and the new entry itself, is durable on return.
    async fn place(&self, from: &Path, id: &str, link: bool) -> io::Result<()> {
        let (from, dst, blobs) = (from.to_path_buf(), self.path(id), self.blobs_dir());
        let (fsync, durable) = (self.fsync, self.durable_dirs.clone());
        tokio::task::spawn_blocking(move || {
            let leaf = dst.parent().unwrap().to_path_buf();
            let mid = leaf.parent().unwrap().to_path_buf();
            for (dir, parent) in [(&mid, &blobs), (&leaf, &mid)] {
                if durable.lock().unwrap().contains(dir) {
                    continue;
                }
                match std::fs::create_dir(dir) {
                    Err(e) if e.kind() != io::ErrorKind::AlreadyExists => return Err(e),
                    _ => {}
                }
                // Sync the parent even when another writer created the directory:
                // it may not have finished syncing it yet.
                if fsync {
                    sync_dir(parent)?;
                }
                durable.lock().unwrap().insert(dir.clone());
            }
            if link {
                if std::fs::hard_link(&from, &dst).is_err() {
                    let copied = std::fs::copy(&from, &dst);
                    if fsync && copied.is_ok() {
                        sync_path_or_file(&std::fs::File::open(&dst)?)?;
                    }
                    copied?;
                }
            } else {
                std::fs::rename(&from, &dst)?;
            }
            if fsync {
                sync_dir(&leaf)?;
            }
            Ok(())
        })
        .await?
    }
}

#[async_trait]
impl BlobStore for FileBlobStore {
    async fn put(&self, src: &mut dyn ByteSource, mut hasher: PutHasher, max_size: u64) -> S3Result<(BlobRef, PutDigest)> {
        let id = random_hex(16);
        let tmp = self.root.join("tmp").join(&id);
        let mut file = tokio::fs::File::create(&tmp).await?;
        let written: S3Result<PutDigest> = async {
            while let Some(chunk) = src.next_chunk().await? {
                if hasher.size + chunk.len() as u64 > max_size {
                    return Err(ErrorCode::EntityTooLarge.into());
                }
                hasher.update(&chunk);
                file.write_all(&chunk).await?;
            }
            file.flush().await?;
            let digest = hasher.finish(src.trailing_checksum())?;
            if self.fsync {
                let f = file.into_std().await;
                tokio::task::spawn_blocking(move || sync_path_or_file(&f)).await??;
            }
            Ok(digest)
        }
        .await;
        let placed = match written {
            Ok(digest) => self.place(&tmp, &id, false).await.map(|_| digest).map_err(S3Error::from),
            Err(e) => Err(e),
        };
        match placed {
            Ok(digest) => Ok((BlobRef { id, layout: ErasureLayout::default() }, digest)),
            Err(e) => {
                let _ = tokio::fs::remove_file(&tmp).await;
                Err(e)
            }
        }
    }

    async fn open(&self, blob: &BlobRef) -> io::Result<Box<dyn BlobRead>> {
        let path = self.path(&blob.id);
        tokio::task::spawn_blocking(move || Ok(Box::new(std::fs::File::open(path)?) as Box<dyn BlobRead>)).await?
    }

    async fn delete(&self, blob: &BlobRef) -> io::Result<()> {
        match tokio::fs::remove_file(self.path(&blob.id)).await {
            Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
            _ => Ok(()),
        }
    }

    async fn duplicate(&self, blob: &BlobRef) -> io::Result<BlobRef> {
        let id = random_hex(16);
        self.place(&self.path(&blob.id), &id, true).await?;
        Ok(BlobRef { id, layout: blob.layout })
    }
}

// ---------------------------------------------------------------------------
// Metadata records
// ---------------------------------------------------------------------------

const BUCKETS: TableDefinition<&str, &[u8]> = TableDefinition::new("buckets");
/// (bucket, key) -> ObjectRecord
const OBJECTS: TableDefinition<(&str, &str), &[u8]> = TableDefinition::new("objects");
/// (bucket, key, upload id) -> UploadRecord
const UPLOADS: TableDefinition<(&str, &str, &str), &[u8]> = TableDefinition::new("uploads");
/// (upload id, part number) -> PartRecord
const PARTS: TableDefinition<(&str, u32), &[u8]> = TableDefinition::new("parts");

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Segment {
    blob: BlobRef,
    size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ObjectRecord {
    size: u64,
    etag: String,
    mtime: DateTime<Utc>,
    #[serde(default)]
    meta: ObjectMetadata,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    checksum: Option<Checksum>,
    #[serde(default)]
    multipart: bool,
    segments: Vec<Segment>,
}

impl ObjectRecord {
    fn info(&self, key: &str) -> ObjectInfo {
        ObjectInfo {
            key: key.to_string(),
            size: self.size,
            etag: self.etag.clone(),
            last_modified: self.mtime,
            meta: self.meta.clone(),
            checksum: self.checksum.clone(),
            parts: if self.multipart { self.segments.iter().map(|s| s.size).collect() } else { Vec::new() },
        }
    }

    fn blobs(&self) -> impl Iterator<Item = &BlobRef> {
        self.segments.iter().map(|s| &s.blob)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UploadRecord {
    initiated: DateTime<Utc>,
    #[serde(default)]
    meta: ObjectMetadata,
    #[serde(default)]
    checksum_algo: Option<ChecksumAlgo>,
    #[serde(default)]
    checksum_type: Option<ChecksumType>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PartRecord {
    etag: String,
    size: u64,
    mtime: DateTime<Utc>,
    #[serde(default)]
    checksum: Option<Checksum>,
    blob: BlobRef,
}

impl PartRecord {
    fn info(&self, number: u32) -> PartInfo {
        PartInfo { number, etag: self.etag.clone(), size: self.size, last_modified: self.mtime, checksum: self.checksum.clone() }
    }
}

fn enc<T: Serialize>(v: &T) -> Vec<u8> {
    serde_json::to_vec(v).expect("record serialization")
}

fn dec<T: for<'de> Deserialize<'de>>(b: &[u8]) -> S3Result<T> {
    serde_json::from_slice(b).map_err(S3Error::internal)
}

macro_rules! internal_from {
    ($($t:ty),*) => { $( impl From<$t> for S3Error { fn from(e: $t) -> Self { S3Error::internal(e) } } )* };
}
internal_from!(redb::Error, redb::DatabaseError, redb::TransactionError, redb::TableError, redb::StorageError, redb::CommitError, redb::SetDurabilityError, tokio::task::JoinError);

/// Seconds-precision "now" with millisecond resolution, as S3 reports it.
fn now() -> DateTime<Utc> {
    let t = Utc::now();
    DateTime::from_timestamp_millis(t.timestamp_millis()).unwrap_or(t)
}

fn md5_hex(d: &PutDigest) -> String {
    hex::encode(d.md5)
}

/// Common prefix of `key` after `prefix` up to and including the delimiter, if any.
fn common_prefix<'a>(key: &'a str, prefix: &str, delimiter: Option<&str>) -> Option<&'a str> {
    let d = delimiter.filter(|d| !d.is_empty())?;
    let rest = &key[prefix.len()..];
    rest.find(d).map(|i| &key[..prefix.len() + i + d.len()])
}

/// A key just past every key that starts with `p` (modulo keys containing U+10FFFF,
/// which callers skip explicitly).
fn past_prefix(p: &str) -> String {
    format!("{p}\u{10FFFF}")
}

fn validate_key(key: &str) -> S3Result<()> {
    if key.is_empty() {
        return Err(S3Error::msg(ErrorCode::InvalidArgument, "Object key must not be empty"));
    }
    if key.len() > MAX_KEY_LEN {
        return Err(ErrorCode::KeyTooLongError.into());
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

pub struct LocalEngine<B: BlobStore = FileBlobStore> {
    db: Arc<Database>,
    blobs: Arc<B>,
    committer: std::sync::mpsc::Sender<Job>,
}

/// A metadata write, run inside a shared transaction. It returns whether it failed
/// with an internal error (the transaction may be dirty and must be aborted), and a
/// callback that receives the commit outcome.
type Job = Box<dyn FnOnce(&WriteTransaction) -> (bool, Done) + Send>;
type Done = Box<dyn FnOnce(S3Result<()>) + Send>;

/// Most writes folded into one transaction.
const MAX_BATCH: usize = 256;

/// Group commit: concurrent metadata writes share one redb transaction and so one
/// fsync. redb allows a single writer, so committing each write separately would
/// serialize every upload behind a disk flush.
///
/// Jobs must return client errors (precondition failed, missing upload...) before
/// modifying any table, so a failed request never leaves partial changes in a batch.
fn run_committer(db: Arc<Database>, fsync: bool, jobs: std::sync::mpsc::Receiver<Job>) {
    while let Ok(first) = jobs.recv() {
        let mut batch = vec![first];
        while batch.len() < MAX_BATCH
            && let Ok(j) = jobs.try_recv()
        {
            batch.push(j);
        }
        let mut txn = match db.begin_write() {
            Ok(t) => t,
            Err(e) => {
                // Dropping the jobs unblocks their waiters with an internal error.
                tracing::error!("begin_write: {e}");
                continue;
            }
        };
        if !fsync && let Err(e) = txn.set_durability(Durability::None) {
            tracing::error!("set_durability: {e}");
        }
        let mut done = Vec::with_capacity(batch.len());
        let mut dirty = false;
        for job in batch {
            let (failed, d) = job(&txn);
            dirty |= failed;
            done.push(d);
        }
        let outcome = if dirty {
            let _ = txn.abort();
            Err(S3Error::internal("metadata batch aborted after an internal error"))
        } else {
            txn.commit().map_err(S3Error::internal)
        };
        for d in done {
            d(outcome.clone());
        }
    }
}

impl LocalEngine<FileBlobStore> {
    pub fn open(data_dir: &Path, fsync: bool) -> S3Result<Self> {
        std::fs::create_dir_all(data_dir)?;
        let blobs = FileBlobStore::new(data_dir, fsync)?;
        LocalEngine::with_blobs(&data_dir.join("meta.redb"), blobs, fsync)
    }

    /// Delete blob files that no metadata references. Blobs changed within `grace`
    /// are skipped: they may belong to a write that has not committed yet.
    pub async fn gc(&self, grace: Duration) -> S3Result<usize> {
        let root = self.blobs.blobs_dir();
        let cutoff = SystemTime::now() - grace;
        // Walk first, snapshot metadata second: any blob older than the grace period
        // that is referenced at all is referenced in the snapshot.
        let candidates = tokio::task::spawn_blocking(move || collect_blobs(&root, cutoff)).await??;
        if candidates.is_empty() {
            return Ok(0);
        }
        let referenced = self
            .blocking(|db| {
                let txn = db.begin_read()?;
                let mut ids = HashSet::new();
                for e in txn.open_table(OBJECTS)?.iter()? {
                    let rec: ObjectRecord = dec(e?.1.value())?;
                    ids.extend(rec.segments.into_iter().map(|s| s.blob.id));
                }
                for e in txn.open_table(PARTS)?.iter()? {
                    let rec: PartRecord = dec(e?.1.value())?;
                    ids.insert(rec.blob.id);
                }
                Ok(ids)
            })
            .await?;
        let mut removed = 0;
        for id in candidates.into_iter().filter(|id| !referenced.contains(id)) {
            if self.blobs.delete(&BlobRef { id, layout: ErasureLayout::default() }).await.is_ok() {
                removed += 1;
            }
        }
        Ok(removed)
    }
}

fn collect_blobs(root: &Path, cutoff: SystemTime) -> io::Result<Vec<String>> {
    let mut out = Vec::new();
    for a in std::fs::read_dir(root)? {
        for b in std::fs::read_dir(a?.path())? {
            for f in std::fs::read_dir(b?.path())? {
                let f = f?;
                let md = f.metadata()?;
                if changed(&md) < cutoff {
                    out.push(f.file_name().to_string_lossy().into_owned());
                }
            }
        }
    }
    Ok(out)
}

/// Last inode change. On unix this is ctime, which hard-linking and renaming update,
/// so fresh copies are never mistaken for old garbage.
fn changed(md: &std::fs::Metadata) -> SystemTime {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        SystemTime::UNIX_EPOCH + Duration::new(md.ctime().max(0) as u64, md.ctime_nsec().clamp(0, 999_999_999) as u32)
    }
    #[cfg(not(unix))]
    {
        md.modified().unwrap_or(SystemTime::now())
    }
}

impl<B: BlobStore> LocalEngine<B> {
    pub fn with_blobs(meta_path: &Path, blobs: B, fsync: bool) -> S3Result<Self> {
        let db = Arc::new(Database::create(meta_path)?);
        let txn = db.begin_write()?;
        txn.open_table(BUCKETS)?;
        txn.open_table(OBJECTS)?;
        txn.open_table(UPLOADS)?;
        txn.open_table(PARTS)?;
        txn.commit()?;
        let (committer, jobs) = std::sync::mpsc::channel();
        let cdb = db.clone();
        std::thread::Builder::new().name("objex-commit".into()).spawn(move || run_committer(cdb, fsync, jobs))?;
        Ok(LocalEngine { db, blobs: Arc::new(blobs), committer })
    }

    async fn blocking<T: Send + 'static>(&self, f: impl FnOnce(&Database) -> S3Result<T> + Send + 'static) -> S3Result<T> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || f(&db)).await?
    }

    /// Run `f` in a write transaction and commit it.
    /// Run `f` in a write transaction (shared with concurrent writes) and wait until
    /// it is committed.
    async fn write<T: Send + 'static>(&self, f: impl FnOnce(&WriteTransaction) -> S3Result<T> + Send + 'static) -> S3Result<T> {
        let (tx, rx) = tokio::sync::oneshot::channel::<S3Result<T>>();
        let job: Job = Box::new(move |txn| {
            let out = f(txn);
            let failed = matches!(&out, Err(e) if e.code == ErrorCode::InternalError);
            let done: Done = Box::new(move |commit| {
                let _ = tx.send(match (out, commit) {
                    (Err(e), _) => Err(e),
                    (Ok(_), Err(e)) => Err(e),
                    (Ok(v), Ok(())) => Ok(v),
                });
            });
            (failed, done)
        });
        self.committer.send(job).map_err(|_| S3Error::internal("metadata committer stopped"))?;
        rx.await.map_err(|_| S3Error::internal("metadata write dropped"))?
    }

    /// Best-effort deletion of blobs that metadata no longer references. Failures are
    /// left for the garbage collector.
    async fn discard(&self, blobs: impl IntoIterator<Item = BlobRef>) {
        for b in blobs {
            if let Err(e) = self.blobs.delete(&b).await {
                tracing::warn!("deleting blob {}: {e}", b.id);
            }
        }
    }

    async fn require_bucket(&self, bucket: &str) -> S3Result<()> {
        let bucket = bucket.to_string();
        self.blocking(move |db| {
            let txn = db.begin_read()?;
            bucket_exists_in(&txn.open_table(BUCKETS)?, &bucket)
        })
        .await
    }

    async fn read_object(&self, bucket: &str, key: &str) -> S3Result<ObjectRecord> {
        let (bucket, key) = (bucket.to_string(), key.to_string());
        self.blocking(move |db| {
            let txn = db.begin_read()?;
            bucket_exists_in(&txn.open_table(BUCKETS)?, &bucket)?;
            let t = txn.open_table(OBJECTS)?;
            let v = t.get((bucket.as_str(), key.as_str()))?.ok_or(ErrorCode::NoSuchKey)?;
            dec(v.value())
        })
        .await
    }

    async fn open_record(&self, rec: &ObjectRecord) -> io::Result<ObjectReader> {
        let mut segments = Vec::with_capacity(rec.segments.len());
        for s in &rec.segments {
            segments.push((self.blobs.open(&s.blob).await?, s.size));
        }
        Ok(ObjectReader::new(segments))
    }

    /// Read an object's record and open its data. Retries if a concurrent overwrite
    /// deletes the blobs between the two steps.
    async fn open_object(&self, bucket: &str, key: &str) -> S3Result<(ObjectRecord, ObjectReader)> {
        let mut attempts = 0;
        loop {
            let rec = self.read_object(bucket, key).await?;
            match self.open_record(&rec).await {
                Ok(r) => return Ok((rec, r)),
                Err(e) if e.kind() == io::ErrorKind::NotFound && attempts < 5 => attempts += 1,
                Err(e) => return Err(S3Error::internal(format!("opening {bucket}/{key}: {e}"))),
            }
        }
    }

    /// Commit `rec` as bucket/key, subject to `cond`, then drop the replaced object's
    /// data. On failure the caller still owns the new data. With `upload`, the
    /// multipart upload is removed in the same transaction.
    async fn commit_object(&self, bucket: &str, key: &str, rec: ObjectRecord, cond: WriteConditions, upload: Option<String>) -> S3Result<ObjectInfo> {
        let info = rec.info(key);
        let (b, k) = (bucket.to_string(), key.to_string());
        let garbage = self
            .write(move |txn| {
                bucket_exists_in(&txn.open_table(BUCKETS)?, &b)?;
                let mut objects = txn.open_table(OBJECTS)?;
                let old: Option<ObjectRecord> = match objects.get((b.as_str(), k.as_str()))? {
                    Some(v) => Some(dec(v.value())?),
                    None => None,
                };
                cond.check(old.as_ref().map(|o| o.etag.as_str()))?;
                let mut garbage: Vec<BlobRef> = old.map(|o| o.segments.into_iter().map(|s| s.blob).collect()).unwrap_or_default();
                if let Some(id) = upload {
                    if txn.open_table(UPLOADS)?.remove((b.as_str(), k.as_str(), id.as_str()))?.is_none() {
                        return Err(ErrorCode::NoSuchUpload.into());
                    }
                    // Parts the object does not use become garbage.
                    let used: HashSet<&str> = rec.blobs().map(|b| b.id.as_str()).collect();
                    let parts = remove_parts(&mut txn.open_table(PARTS)?, &id)?;
                    garbage.extend(parts.into_iter().filter(|p| !used.contains(p.id.as_str())));
                }
                objects.insert((b.as_str(), k.as_str()), enc(&rec).as_slice())?;
                Ok(garbage)
            })
            .await?;
        self.discard(garbage).await;
        Ok(info)
    }

    async fn read_upload(&self, bucket: &str, key: &str, upload_id: &str) -> S3Result<UploadRecord> {
        let (b, k, u) = (bucket.to_string(), key.to_string(), upload_id.to_string());
        self.blocking(move |db| {
            let txn = db.begin_read()?;
            bucket_exists_in(&txn.open_table(BUCKETS)?, &b)?;
            let t = txn.open_table(UPLOADS)?;
            let v = t.get((b.as_str(), k.as_str(), u.as_str()))?.ok_or(ErrorCode::NoSuchUpload)?;
            dec(v.value())
        })
        .await
    }

    /// Store a part blob, replacing any earlier upload of the same part number.
    async fn commit_part(&self, bucket: &str, key: &str, upload_id: &str, number: u32, rec: PartRecord) -> S3Result<PartInfo> {
        let info = rec.info(number);
        let blob = rec.blob.clone();
        let (b, k, u) = (bucket.to_string(), key.to_string(), upload_id.to_string());
        let res = self
            .write(move |txn| {
                if txn.open_table(UPLOADS)?.get((b.as_str(), k.as_str(), u.as_str()))?.is_none() {
                    return Err(ErrorCode::NoSuchUpload.into());
                }
                let mut parts = txn.open_table(PARTS)?;
                let old = match parts.insert((u.as_str(), number), enc(&rec).as_slice())? {
                    Some(v) => Some(dec::<PartRecord>(v.value())?.blob),
                    None => None,
                };
                Ok(old)
            })
            .await;
        match res {
            Ok(old) => {
                self.discard(old).await;
                Ok(info)
            }
            Err(e) => {
                self.discard([blob]).await;
                Err(e)
            }
        }
    }
}

fn bucket_exists_in(t: &impl ReadableTable<&'static str, &'static [u8]>, bucket: &str) -> S3Result<()> {
    if t.get(bucket)?.is_none() {
        return Err(ErrorCode::NoSuchBucket.into());
    }
    Ok(())
}

fn single_record(blob: BlobRef, digest: PutDigest, meta: ObjectMetadata) -> ObjectRecord {
    ObjectRecord {
        size: digest.size,
        etag: md5_hex(&digest),
        mtime: now(),
        meta,
        checksum: digest.checksum,
        multipart: false,
        segments: vec![Segment { blob, size: digest.size }],
    }
}

#[async_trait]
impl<B: BlobStore> ObjectLayer for LocalEngine<B> {
    async fn list_buckets(&self) -> S3Result<Vec<BucketInfo>> {
        self.blocking(|db| {
            let txn = db.begin_read()?;
            let t = txn.open_table(BUCKETS)?;
            t.iter()?.map(|e| dec(e?.1.value())).collect()
        })
        .await
    }

    async fn create_bucket(&self, name: &str, public_read: bool) -> S3Result<()> {
        if !valid_bucket_name(name) {
            return Err(ErrorCode::InvalidBucketName.into());
        }
        let info = BucketInfo { name: name.to_string(), created: now(), public_read, cors: None };
        self.write(move |txn| {
            let mut t = txn.open_table(BUCKETS)?;
            if t.get(info.name.as_str())?.is_some() {
                return Err(ErrorCode::BucketAlreadyOwnedByYou.into());
            }
            t.insert(info.name.as_str(), enc(&info).as_slice())?;
            Ok(())
        })
        .await
    }

    async fn get_bucket(&self, name: &str) -> S3Result<BucketInfo> {
        let name = name.to_string();
        self.blocking(move |db| {
            let txn = db.begin_read()?;
            let t = txn.open_table(BUCKETS)?;
            let v = t.get(name.as_str())?.ok_or(ErrorCode::NoSuchBucket)?;
            dec(v.value())
        })
        .await
    }

    async fn update_bucket(&self, name: &str, update: BucketUpdate) -> S3Result<()> {
        let name = name.to_string();
        self.write(move |txn| {
            let mut t = txn.open_table(BUCKETS)?;
            let mut info: BucketInfo = match t.get(name.as_str())? {
                Some(v) => dec(v.value())?,
                None => return Err(ErrorCode::NoSuchBucket.into()),
            };
            match update {
                BucketUpdate::PublicRead(p) => info.public_read = p,
                BucketUpdate::Cors(c) => info.cors = c,
            }
            t.insert(name.as_str(), enc(&info).as_slice())?;
            Ok(())
        })
        .await
    }

    async fn delete_bucket(&self, name: &str) -> S3Result<()> {
        let name = name.to_string();
        let garbage = self
            .write(move |txn| {
                let mut buckets = txn.open_table(BUCKETS)?;
                if buckets.get(name.as_str())?.is_none() {
                    return Err(ErrorCode::NoSuchBucket.into());
                }
                let objects = txn.open_table(OBJECTS)?;
                if let Some(e) = objects.range((name.as_str(), "")..)?.next() {
                    if e?.0.value().0 == name {
                        return Err(ErrorCode::BucketNotEmpty.into());
                    }
                }
                // In-progress multipart uploads are aborted with the bucket.
                let mut uploads = txn.open_table(UPLOADS)?;
                let mut parts = txn.open_table(PARTS)?;
                let mut ids = Vec::new();
                for e in uploads.range((name.as_str(), "", "")..)? {
                    let (k, _) = e?;
                    let (b, key, id) = k.value();
                    if b != name {
                        break;
                    }
                    ids.push((key.to_string(), id.to_string()));
                }
                let mut garbage = Vec::new();
                for (key, id) in ids {
                    uploads.remove((name.as_str(), key.as_str(), id.as_str()))?;
                    garbage.extend(remove_parts(&mut parts, &id)?);
                }
                buckets.remove(name.as_str())?;
                Ok(garbage)
            })
            .await?;
        self.discard(garbage).await;
        Ok(())
    }

    async fn put_object(&self, bucket: &str, key: &str, src: &mut dyn ByteSource, opts: PutOptions) -> S3Result<ObjectInfo> {
        validate_key(key)?;
        self.require_bucket(bucket).await?;
        let (blob, digest) = self.blobs.put(src, PutHasher::new(&opts.expect), MAX_PUT_SIZE).await?;
        let rec = single_record(blob.clone(), digest, opts.meta);
        let res = self.commit_object(bucket, key, rec, opts.cond, None).await;
        if res.is_err() {
            self.discard([blob]).await;
        }
        res
    }

    async fn head_object(&self, bucket: &str, key: &str) -> S3Result<ObjectInfo> {
        Ok(self.read_object(bucket, key).await?.info(key))
    }

    async fn get_object(&self, bucket: &str, key: &str) -> S3Result<(ObjectInfo, ObjectReader)> {
        let (rec, reader) = self.open_object(bucket, key).await?;
        Ok((rec.info(key), reader))
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> S3Result<()> {
        self.delete_objects(bucket, &[key.to_string()]).await?.pop().unwrap_or(Ok(()))
    }

    async fn delete_objects(&self, bucket: &str, keys: &[String]) -> S3Result<Vec<S3Result<()>>> {
        let (b, keys) = (bucket.to_string(), keys.to_vec());
        let (results, garbage) = self
            .write(move |txn| {
                bucket_exists_in(&txn.open_table(BUCKETS)?, &b)?;
                let mut objects = txn.open_table(OBJECTS)?;
                let mut results = Vec::with_capacity(keys.len());
                let mut garbage = Vec::new();
                for k in &keys {
                    if let Err(e) = validate_key(k) {
                        results.push(Err(e));
                        continue;
                    }
                    if let Some(v) = objects.remove((b.as_str(), k.as_str()))? {
                        let rec: ObjectRecord = dec(v.value())?;
                        garbage.extend(rec.segments.into_iter().map(|s| s.blob));
                    }
                    results.push(Ok(()));
                }
                Ok((results, garbage))
            })
            .await?;
        self.discard(garbage).await;
        Ok(results)
    }

    async fn copy_object(&self, src_bucket: &str, src_key: &str, bucket: &str, key: &str, opts: CopyOptions) -> S3Result<ObjectInfo> {
        validate_key(key)?;
        if src_bucket == bucket && src_key == key && opts.replace_meta.is_none() {
            return Err(S3Error::msg(
                ErrorCode::InvalidRequest,
                "This copy request is illegal because it is trying to copy an object to itself without changing the object's metadata, storage class, website redirect location or encryption attributes.",
            ));
        }
        self.require_bucket(bucket).await?;
        let mut attempts = 0;
        let src = loop {
            let src = self.read_object(src_bucket, src_key).await?;
            opts.src_cond.check(&src.info(src_key), true)?;
            let mut dups = Vec::with_capacity(src.segments.len());
            let mut failed = None;
            for s in &src.segments {
                match self.blobs.duplicate(&s.blob).await {
                    Ok(b) => dups.push(Segment { blob: b, size: s.size }),
                    Err(e) => {
                        failed = Some(e);
                        break;
                    }
                }
            }
            match failed {
                None => break ObjectRecord { segments: dups, ..src },
                Some(e) => {
                    self.discard(dups.into_iter().map(|s| s.blob)).await;
                    if e.kind() != io::ErrorKind::NotFound || attempts >= 5 {
                        return Err(S3Error::internal(e));
                    }
                    attempts += 1;
                }
            }
        };
        let rec = ObjectRecord { mtime: now(), meta: opts.replace_meta.unwrap_or(src.meta.clone()), ..src };
        let blobs: Vec<BlobRef> = rec.blobs().cloned().collect();
        let res = self.commit_object(bucket, key, rec, opts.dst_cond, None).await;
        if res.is_err() {
            self.discard(blobs).await;
        }
        res
    }

    async fn list_objects(&self, bucket: &str, q: ListQuery) -> S3Result<ListResult> {
        let b = bucket.to_string();
        self.blocking(move |db| {
            let txn = db.begin_read()?;
            bucket_exists_in(&txn.open_table(BUCKETS)?, &b)?;
            let t = txn.open_table(OBJECTS)?;
            let mut res = ListResult::default();
            if q.max_keys == 0 {
                return Ok(res);
            }
            let delim = q.delimiter.as_deref();
            let mut lower = if q.marker.as_str() >= q.prefix.as_str() {
                Bound::Excluded(q.marker.clone())
            } else {
                Bound::Included(q.prefix.clone())
            };
            let mut last_prefix: Option<String> = None;
            'scan: loop {
                let from = match &lower {
                    Bound::Included(s) => Bound::Included((b.as_str(), s.as_str())),
                    Bound::Excluded(s) => Bound::Excluded((b.as_str(), s.as_str())),
                    Bound::Unbounded => unreachable!(),
                };
                for e in t.range::<(&str, &str)>((from, Bound::Unbounded))? {
                    let (k, v) = e?;
                    let (kb, key) = k.value();
                    if kb != b || !key.starts_with(q.prefix.as_str()) {
                        break 'scan;
                    }
                    if let Some(cp) = common_prefix(key, &q.prefix, delim) {
                        if last_prefix.as_deref() == Some(cp) || cp <= q.marker.as_str() {
                            if last_prefix.as_deref() != Some(cp) {
                                last_prefix = Some(cp.to_string());
                                lower = Bound::Included(past_prefix(cp));
                                continue 'scan;
                            }
                            continue;
                        }
                        if res.objects.len() + res.prefixes.len() >= q.max_keys {
                            res.truncated = true;
                            break 'scan;
                        }
                        res.prefixes.push(cp.to_string());
                        res.next_marker = Some(cp.to_string());
                        last_prefix = Some(cp.to_string());
                        lower = Bound::Included(past_prefix(cp));
                        continue 'scan;
                    }
                    if res.objects.len() + res.prefixes.len() >= q.max_keys {
                        res.truncated = true;
                        break 'scan;
                    }
                    let rec: ObjectRecord = dec(v.value())?;
                    res.objects.push(rec.info(key));
                    res.next_marker = Some(key.to_string());
                }
                break;
            }
            if !res.truncated {
                res.next_marker = None;
            }
            Ok(res)
        })
        .await
    }

    async fn create_multipart(&self, bucket: &str, key: &str, meta: ObjectMetadata, checksum: Option<(ChecksumAlgo, ChecksumType)>) -> S3Result<String> {
        validate_key(key)?;
        if let Some((algo, ChecksumType::FullObject)) = checksum
            && !algo.is_crc()
        {
            return Err(S3Error::msg(ErrorCode::InvalidRequest, format!("The FULL_OBJECT checksum type is not supported for {}", algo.name())));
        }
        if let Some((ChecksumAlgo::Crc64nvme, ChecksumType::Composite)) = checksum {
            return Err(S3Error::msg(ErrorCode::InvalidRequest, "The COMPOSITE checksum type is not supported for CRC64NVME"));
        }
        let id = random_hex(16);
        let rec = UploadRecord { initiated: now(), meta, checksum_algo: checksum.map(|c| c.0), checksum_type: checksum.map(|c| c.1) };
        let (b, k, u) = (bucket.to_string(), key.to_string(), id.clone());
        self.write(move |txn| {
            bucket_exists_in(&txn.open_table(BUCKETS)?, &b)?;
            txn.open_table(UPLOADS)?.insert((b.as_str(), k.as_str(), u.as_str()), enc(&rec).as_slice())?;
            Ok(())
        })
        .await?;
        Ok(id)
    }

    async fn upload_part(&self, bucket: &str, key: &str, upload_id: &str, part: u32, src: &mut dyn ByteSource, mut expect: PutExpect) -> S3Result<PartInfo> {
        if !(1..=MAX_PART_NUMBER).contains(&part) {
            return Err(S3Error::msg(ErrorCode::InvalidArgument, "Part number must be an integer between 1 and 10000, inclusive"));
        }
        let upload = self.read_upload(bucket, key, upload_id).await?;
        if let Some(algo) = upload.checksum_algo {
            let sent = expect.checksum.as_ref().map(|c| c.algo).or(expect.checksum_algo);
            match sent {
                Some(a) if a != algo => {
                    return Err(S3Error::msg(ErrorCode::InvalidRequest, format!("Checksum Type mismatch occurred, expected checksum Type: {}, actual checksum Type: {}", algo.name().to_lowercase(), a.name().to_lowercase())));
                }
                _ => expect.checksum_algo = Some(algo),
            }
        }
        let (blob, digest) = self.blobs.put(src, PutHasher::new(&expect), MAX_PUT_SIZE).await?;
        let rec = PartRecord { etag: md5_hex(&digest), size: digest.size, mtime: now(), checksum: digest.checksum, blob };
        self.commit_part(bucket, key, upload_id, part, rec).await
    }

    async fn upload_part_copy(&self, src_bucket: &str, src_key: &str, range: Option<(u64, u64)>, cond: ReadConditions, bucket: &str, key: &str, upload_id: &str, part: u32) -> S3Result<(PartInfo, ObjectInfo)> {
        if !(1..=MAX_PART_NUMBER).contains(&part) {
            return Err(S3Error::msg(ErrorCode::InvalidArgument, "Part number must be an integer between 1 and 10000, inclusive"));
        }
        let upload = self.read_upload(bucket, key, upload_id).await?;
        let (src, reader) = self.open_object(src_bucket, src_key).await?;
        let src_info = src.info(src_key);
        cond.check(&src_info, true)?;
        let (start, len) = match range {
            None => (0, src.size),
            Some((first, last)) => {
                if first > last || last >= src.size {
                    return Err(S3Error::msg(ErrorCode::InvalidArgument, format!("Range specified is not valid for source object of size: {}", src.size)));
                }
                (first, last - first + 1)
            }
        };
        let mut source = ChannelSource(reader.stream(start, len));
        let expect = PutExpect { checksum_algo: upload.checksum_algo, size: Some(len), ..Default::default() };
        let (blob, digest) = self.blobs.put(&mut source, PutHasher::new(&expect), MAX_PUT_SIZE).await?;
        let rec = PartRecord { etag: md5_hex(&digest), size: digest.size, mtime: now(), checksum: digest.checksum, blob };
        let info = self.commit_part(bucket, key, upload_id, part, rec).await?;
        Ok((info, src_info))
    }

    async fn complete_multipart(&self, bucket: &str, key: &str, upload_id: &str, parts: Vec<CompletePart>, opts: CompleteOptions) -> S3Result<ObjectInfo> {
        if parts.is_empty() {
            return Err(S3Error::msg(ErrorCode::MalformedXML, "You must specify at least one part"));
        }
        if parts.windows(2).any(|w| w[0].number >= w[1].number) {
            return Err(ErrorCode::InvalidPartOrder.into());
        }
        let upload = self.read_upload(bucket, key, upload_id).await?;
        let u = upload_id.to_string();
        let stored: Vec<Option<PartRecord>> = {
            let numbers: Vec<u32> = parts.iter().map(|p| p.number).collect();
            self.blocking(move |db| {
                let txn = db.begin_read()?;
                let t = txn.open_table(PARTS)?;
                numbers
                    .into_iter()
                    .map(|n| match t.get((u.as_str(), n))? {
                        Some(v) => Ok(Some(dec(v.value())?)),
                        None => Ok(None),
                    })
                    .collect()
            })
            .await?
        };
        let count = parts.len();
        let mut segments = Vec::with_capacity(count);
        let mut md5s = Vec::with_capacity(count * 16);
        let mut checksums = Vec::new();
        for (i, (want, have)) in parts.iter().zip(stored).enumerate() {
            let have = have.ok_or(ErrorCode::InvalidPart)?;
            if want.etag.trim_matches('"') != have.etag {
                return Err(ErrorCode::InvalidPart.into());
            }
            if let Some(c) = &want.checksum
                && have.checksum.as_ref() != Some(c)
            {
                return Err(S3Error::msg(ErrorCode::InvalidPart, format!("The checksum for part {} did not match", want.number)));
            }
            if i + 1 < count && have.size < MIN_PART_SIZE {
                return Err(ErrorCode::EntityTooSmall.into());
            }
            md5s.extend(hex::decode(&have.etag).map_err(S3Error::internal)?);
            if let Some(c) = &have.checksum {
                checksums.push((c.clone(), have.size));
            }
            segments.push(Segment { blob: have.blob, size: have.size });
        }
        let size: u64 = segments.iter().map(|s| s.size).sum();
        if size > MAX_OBJECT_SIZE {
            return Err(ErrorCode::EntityTooLarge.into());
        }
        let checksum = match upload.checksum_algo {
            Some(algo) if checksums.len() == count => match upload.checksum_type.unwrap_or(algo.default_type()) {
                ChecksumType::Composite => checksum::composite(algo, &checksums.iter().map(|c| c.0.clone()).collect::<Vec<_>>()),
                ChecksumType::FullObject => checksum::combine_full(algo, &checksums),
            },
            _ => None,
        };
        if let Some(want) = &opts.checksum {
            match &checksum {
                Some(got) if got.algo == want.algo && got.value.split('-').next() == want.value.split('-').next() => {}
                _ => {
                    return Err(S3Error::msg(ErrorCode::BadDigest, format!("The {} you specified did not match the calculated checksum.", want.algo.name())));
                }
            }
        }
        use md5::Digest;
        let etag = format!("{}-{count}", hex::encode(md5::Md5::digest(&md5s)));
        let rec = ObjectRecord { size, etag, mtime: now(), meta: upload.meta, checksum, multipart: true, segments };
        self.commit_object(bucket, key, rec, opts.cond, Some(upload_id.to_string())).await
    }

    async fn abort_multipart(&self, bucket: &str, key: &str, upload_id: &str) -> S3Result<()> {
        let (b, k, u) = (bucket.to_string(), key.to_string(), upload_id.to_string());
        let garbage = self
            .write(move |txn| {
                bucket_exists_in(&txn.open_table(BUCKETS)?, &b)?;
                if txn.open_table(UPLOADS)?.remove((b.as_str(), k.as_str(), u.as_str()))?.is_none() {
                    return Err(ErrorCode::NoSuchUpload.into());
                }
                remove_parts(&mut txn.open_table(PARTS)?, &u)
            })
            .await?;
        self.discard(garbage).await;
        Ok(())
    }

    async fn list_parts(&self, bucket: &str, key: &str, upload_id: &str, marker: u32, max_parts: usize) -> S3Result<ListPartsResult> {
        let upload = self.read_upload(bucket, key, upload_id).await?;
        let u = upload_id.to_string();
        let (parts, truncated) = self
            .blocking(move |db| {
                let txn = db.begin_read()?;
                let t = txn.open_table(PARTS)?;
                let mut parts = Vec::new();
                let mut truncated = false;
                let lower = marker.saturating_add(1);
                if marker == u32::MAX {
                    return Ok((parts, false));
                }
                for e in t.range((u.as_str(), lower)..=(u.as_str(), u32::MAX))? {
                    let (k, v) = e?;
                    if parts.len() >= max_parts {
                        truncated = true;
                        break;
                    }
                    parts.push(dec::<PartRecord>(v.value())?.info(k.value().1));
                }
                Ok((parts, truncated))
            })
            .await?;
        let next_marker = parts.last().map(|p| p.number).unwrap_or(0);
        Ok(ListPartsResult {
            upload: UploadInfo {
                key: key.to_string(),
                upload_id: upload_id.to_string(),
                initiated: upload.initiated,
                checksum_algo: upload.checksum_algo,
                checksum_type: upload.checksum_type,
            },
            parts,
            truncated,
            next_marker,
        })
    }

    async fn list_uploads(&self, bucket: &str, q: ListUploadsQuery) -> S3Result<ListUploadsResult> {
        let b = bucket.to_string();
        self.blocking(move |db| {
            let txn = db.begin_read()?;
            bucket_exists_in(&txn.open_table(BUCKETS)?, &b)?;
            let t = txn.open_table(UPLOADS)?;
            let mut res = ListUploadsResult::default();
            if q.max_uploads == 0 {
                return Ok(res);
            }
            let delim = q.delimiter.as_deref();
            // Lower bound as (key, upload id, inclusive).
            let mut lower: (String, String, bool) = if q.key_marker.is_empty() || q.key_marker < q.prefix {
                (q.prefix.clone(), String::new(), true)
            } else if q.upload_id_marker.is_empty() {
                // Skip every upload of key_marker.
                (format!("{}\0", q.key_marker), String::new(), true)
            } else {
                (q.key_marker.clone(), q.upload_id_marker.clone(), false)
            };
            let mut last_prefix: Option<String> = None;
            'scan: loop {
                let key = (b.as_str(), lower.0.as_str(), lower.1.as_str());
                let from = if lower.2 { Bound::Included(key) } else { Bound::Excluded(key) };
                for e in t.range::<(&str, &str, &str)>((from, Bound::Unbounded))? {
                    let (k, v) = e?;
                    let (kb, key, id) = k.value();
                    if kb != b || !key.starts_with(q.prefix.as_str()) {
                        break 'scan;
                    }
                    if let Some(cp) = common_prefix(key, &q.prefix, delim) {
                        if last_prefix.as_deref() == Some(cp) {
                            continue;
                        }
                        if !q.key_marker.is_empty() && cp <= q.key_marker.as_str() {
                            last_prefix = Some(cp.to_string());
                            lower = (past_prefix(cp), String::new(), true);
                            continue 'scan;
                        }
                        if res.uploads.len() + res.prefixes.len() >= q.max_uploads {
                            res.truncated = true;
                            break 'scan;
                        }
                        res.prefixes.push(cp.to_string());
                        res.next_key_marker = cp.to_string();
                        res.next_upload_id_marker = String::new();
                        last_prefix = Some(cp.to_string());
                        lower = (past_prefix(cp), String::new(), true);
                        continue 'scan;
                    }
                    if res.uploads.len() + res.prefixes.len() >= q.max_uploads {
                        res.truncated = true;
                        break 'scan;
                    }
                    let rec: UploadRecord = dec(v.value())?;
                    res.uploads.push(UploadInfo {
                        key: key.to_string(),
                        upload_id: id.to_string(),
                        initiated: rec.initiated,
                        checksum_algo: rec.checksum_algo,
                        checksum_type: rec.checksum_type,
                    });
                    res.next_key_marker = key.to_string();
                    res.next_upload_id_marker = id.to_string();
                }
                break;
            }
            Ok(res)
        })
        .await
    }
}

fn remove_parts(parts: &mut redb::Table<(&'static str, u32), &'static [u8]>, upload_id: &str) -> S3Result<Vec<BlobRef>> {
    let mut garbage = Vec::new();
    let mut numbers = Vec::new();
    for e in parts.range((upload_id, 0)..=(upload_id, u32::MAX))? {
        let (k, v) = e?;
        garbage.push(dec::<PartRecord>(v.value())?.blob);
        numbers.push(k.value().1);
    }
    for n in numbers {
        parts.remove((upload_id, n))?;
    }
    Ok(garbage)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpdir() -> PathBuf {
        let d = std::env::temp_dir().join(format!("objex-test-{}", random_hex(8)));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    async fn engine() -> (LocalEngine, PathBuf) {
        let d = tmpdir();
        (LocalEngine::open(&d, false).unwrap(), d)
    }

    async fn put(e: &LocalEngine, b: &str, k: &str, data: &[u8]) -> ObjectInfo {
        let mut src = BytesSource(Some(Bytes::copy_from_slice(data)));
        e.put_object(b, k, &mut src, PutOptions::default()).await.unwrap()
    }

    async fn read_all(e: &LocalEngine, b: &str, k: &str) -> Vec<u8> {
        let (info, r) = e.get_object(b, k).await.unwrap();
        let mut rx = r.stream(0, info.size);
        let mut out = Vec::new();
        while let Some(c) = rx.recv().await {
            out.extend_from_slice(&c.unwrap());
        }
        out
    }

    fn blob_count(d: &Path) -> usize {
        collect_blobs(&d.join("blobs"), SystemTime::now() + Duration::from_secs(60)).unwrap().len()
    }

    #[tokio::test]
    async fn put_get_overwrite_delete() {
        let (e, d) = engine().await;
        e.create_bucket("bk1", false).await.unwrap();
        assert_eq!(e.create_bucket("bk1", false).await.unwrap_err().code, ErrorCode::BucketAlreadyOwnedByYou);
        let info = put(&e, "bk1", "a/b.txt", b"hello").await;
        assert_eq!(info.etag, "5d41402abc4b2a76b9719d911017c592");
        assert_eq!(read_all(&e, "bk1", "a/b.txt").await, b"hello");
        put(&e, "bk1", "a/b.txt", b"world!").await;
        assert_eq!(read_all(&e, "bk1", "a/b.txt").await, b"world!");
        assert_eq!(blob_count(&d), 1);
        assert_eq!(e.delete_bucket("bk1").await.unwrap_err().code, ErrorCode::BucketNotEmpty);
        e.delete_object("bk1", "a/b.txt").await.unwrap();
        e.delete_object("bk1", "missing").await.unwrap();
        assert_eq!(e.head_object("bk1", "a/b.txt").await.unwrap_err().code, ErrorCode::NoSuchKey);
        assert_eq!(blob_count(&d), 0);
        e.delete_bucket("bk1").await.unwrap();
        assert_eq!(e.head_object("bk1", "x").await.unwrap_err().code, ErrorCode::NoSuchBucket);
    }

    #[tokio::test]
    async fn conditional_put() {
        let (e, _d) = engine().await;
        e.create_bucket("bk1", false).await.unwrap();
        let opts = |inm: &str| PutOptions { cond: WriteConditions { if_none_match: Some(inm.into()), if_match: None }, ..Default::default() };
        let mut src = BytesSource(Some(Bytes::from_static(b"1")));
        e.put_object("bk1", "k", &mut src, opts("*")).await.unwrap();
        let mut src = BytesSource(Some(Bytes::from_static(b"2")));
        assert_eq!(e.put_object("bk1", "k", &mut src, opts("*")).await.unwrap_err().code, ErrorCode::PreconditionFailed);
        assert_eq!(read_all(&e, "bk1", "k").await, b"1");
    }

    #[tokio::test]
    async fn listing() {
        let (e, _d) = engine().await;
        e.create_bucket("bk1", false).await.unwrap();
        e.create_bucket("bk0", false).await.unwrap();
        e.create_bucket("bk2", false).await.unwrap();
        put(&e, "bk0", "zzz", b"").await;
        put(&e, "bk2", "aaa", b"").await;
        for k in ["a", "b/1", "b/2", "b/3/x", "c/1", "d"] {
            put(&e, "bk1", k, b"x").await;
        }
        let q = |prefix: &str, delim: Option<&str>, marker: &str, max: usize| ListQuery {
            prefix: prefix.into(),
            delimiter: delim.map(Into::into),
            marker: marker.into(),
            max_keys: max,
        };
        let keys = |r: &ListResult| r.objects.iter().map(|o| o.key.clone()).collect::<Vec<_>>();

        let r = e.list_objects("bk1", q("", None, "", 1000)).await.unwrap();
        assert_eq!(keys(&r), ["a", "b/1", "b/2", "b/3/x", "c/1", "d"]);
        assert!(!r.truncated);

        let r = e.list_objects("bk1", q("", Some("/"), "", 1000)).await.unwrap();
        assert_eq!(keys(&r), ["a", "d"]);
        assert_eq!(r.prefixes, ["b/", "c/"]);

        let r = e.list_objects("bk1", q("b/", Some("/"), "", 1000)).await.unwrap();
        assert_eq!(keys(&r), ["b/1", "b/2"]);
        assert_eq!(r.prefixes, ["b/3/"]);

        // paginate with delimiter, one entry at a time
        let mut marker = String::new();
        let mut seen = Vec::new();
        loop {
            let r = e.list_objects("bk1", q("", Some("/"), &marker, 1)).await.unwrap();
            seen.extend(keys(&r));
            seen.extend(r.prefixes.clone());
            if !r.truncated {
                break;
            }
            marker = r.next_marker.unwrap();
        }
        assert_eq!(seen, ["a", "b/", "c/", "d"]);

        let r = e.list_objects("bk1", q("", None, "b/2", 2)).await.unwrap();
        assert_eq!(keys(&r), ["b/3/x", "c/1"]);
        assert!(r.truncated);
    }

    fn big(n: usize, seed: u8) -> Vec<u8> {
        (0..n).map(|i| (i as u8).wrapping_mul(7).wrapping_add(seed)).collect()
    }

    #[tokio::test]
    async fn multipart() {
        let (e, d) = engine().await;
        e.create_bucket("bk1", false).await.unwrap();
        let id = e.create_multipart("bk1", "big", ObjectMetadata::default(), Some((ChecksumAlgo::Crc32, ChecksumType::FullObject))).await.unwrap();
        let p1 = big(MIN_PART_SIZE as usize, 1);
        let p2 = big(1000, 2);
        let mut parts = Vec::new();
        for (n, data) in [(1u32, &p1), (2, &p2), (3, &p2)] {
            let mut src = BytesSource(Some(Bytes::copy_from_slice(data)));
            let info = e.upload_part("bk1", "big", &id, n, &mut src, PutExpect::default()).await.unwrap();
            assert!(info.checksum.is_some());
            parts.push(info);
        }
        // re-upload part 1
        let mut src = BytesSource(Some(Bytes::copy_from_slice(&p1)));
        e.upload_part("bk1", "big", &id, 1, &mut src, PutExpect::default()).await.unwrap();
        let listed = e.list_parts("bk1", "big", &id, 0, 1000).await.unwrap();
        assert_eq!(listed.parts.len(), 3);

        let up = e.list_uploads("bk1", ListUploadsQuery { max_uploads: 10, ..Default::default() }).await.unwrap();
        assert_eq!(up.uploads.len(), 1);

        let complete = |nums: &[u32]| nums.iter().map(|n| CompletePart { number: *n, etag: parts[*n as usize - 1].etag.clone(), checksum: None }).collect::<Vec<_>>();
        let err = e.complete_multipart("bk1", "big", &id, complete(&[2, 1]), CompleteOptions::default()).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidPartOrder);
        let err = e.complete_multipart("bk1", "big", &id, complete(&[2, 3]), CompleteOptions::default()).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::EntityTooSmall);

        let info = e.complete_multipart("bk1", "big", &id, complete(&[1, 2]), CompleteOptions::default()).await.unwrap();
        assert!(info.etag.ends_with("-2"));
        assert_eq!(info.parts, vec![p1.len() as u64, p2.len() as u64]);
        let mut all = p1.clone();
        all.extend_from_slice(&p2);
        assert_eq!(read_all(&e, "bk1", "big").await, all);
        let mut h = crate::checksum::ChecksumHasher::new(ChecksumAlgo::Crc32);
        h.update(&all);
        assert_eq!(info.checksum.unwrap(), h.finish());
        // part 3 and the replaced part 1 are gone
        assert_eq!(blob_count(&d), 2);
        assert_eq!(e.list_parts("bk1", "big", &id, 0, 10).await.unwrap_err().code, ErrorCode::NoSuchUpload);

        // ranged read across the part boundary
        let (_, r) = e.get_object("bk1", "big").await.unwrap();
        let start = p1.len() as u64 - 10;
        let mut rx = r.stream(start, 20);
        let mut out = Vec::new();
        while let Some(c) = rx.recv().await {
            out.extend_from_slice(&c.unwrap());
        }
        assert_eq!(out, &all[start as usize..start as usize + 20]);
    }

    #[tokio::test]
    async fn copy_and_gc() {
        let (e, d) = engine().await;
        e.create_bucket("bk1", false).await.unwrap();
        put(&e, "bk1", "src", b"copy me").await;
        let opts = || CopyOptions { replace_meta: None, src_cond: Default::default(), dst_cond: Default::default() };
        e.copy_object("bk1", "src", "bk1", "dst", opts()).await.unwrap();
        assert!(e.copy_object("bk1", "src", "bk1", "src", opts()).await.is_err());
        e.delete_object("bk1", "src").await.unwrap();
        assert_eq!(read_all(&e, "bk1", "dst").await, b"copy me");

        let id = e.create_multipart("bk1", "mp", ObjectMetadata::default(), None).await.unwrap();
        let (part, _) = e.upload_part_copy("bk1", "dst", Some((0, 3)), Default::default(), "bk1", "mp", &id, 1).await.unwrap();
        assert_eq!(part.size, 4);

        // an orphan blob, as left by a crash between write and commit
        let orphan = d.join("blobs/00/00/0000aaaa");
        std::fs::create_dir_all(orphan.parent().unwrap()).unwrap();
        std::fs::write(&orphan, b"junk").unwrap();
        assert_eq!(e.gc(Duration::from_secs(3600)).await.unwrap(), 0);
        assert_eq!(e.gc(Duration::ZERO).await.unwrap(), 1);
        assert!(!orphan.exists());
        assert_eq!(read_all(&e, "bk1", "dst").await, b"copy me");
        e.abort_multipart("bk1", "mp", &id).await.unwrap();
        assert_eq!(blob_count(&d), 1);
    }
}
