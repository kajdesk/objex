//! Single-node engine: metadata in redb, object data in immutable blob files.
//!
//! - `blobs`: the blob files.
//! - `meta`: tables, records and the group committer.
//! - `reclaim`: read leases and background deletion of unreferenced blobs.
//! - `maintenance`: garbage collection and integrity scrubbing.

mod blobs;
mod maintenance;
mod meta;
mod reclaim;

use std::collections::{HashMap, HashSet};
use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use redb::{Database, ReadableDatabase, ReadableTable, WriteTransaction};

use super::*;
use crate::checksum::{self, ChecksumType};
use crate::util::{random_hex, valid_bucket_name};
pub use blobs::FileBlobStore;
pub use maintenance::ScrubReport;
use meta::*;
use reclaim::{Leases, Reclaimer};

/// Tuning for [`LocalEngine`].
#[derive(Debug, Clone)]
pub struct EngineOptions {
    /// fsync data and metadata before acknowledging writes.
    pub fsync: bool,
    /// Group-commit window: how long the first write of a batch waits for others.
    pub commit_window: Duration,
    /// Most metadata writes queued at once.
    pub commit_queue: usize,
    /// Most object bytes buffered for GET responses across all requests.
    pub read_buffer: usize,
    /// How long deleted blobs are kept before their files are removed.
    pub reclaim_delay: Duration,
}

impl Default for EngineOptions {
    fn default() -> Self {
        EngineOptions {
            fsync: true,
            commit_window: Duration::ZERO,
            commit_queue: 4096,
            read_buffer: 256 * 1024 * 1024,
            reclaim_delay: Duration::from_secs(5),
        }
    }
}

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

fn bucket_exists_in(t: &impl ReadableTable<&'static str, &'static [u8]>, bucket: &str) -> S3Result<()> {
    if t.get(bucket)?.is_none() {
        return Err(ErrorCode::NoSuchBucket.into());
    }
    Ok(())
}

fn single_record(digest: &PutDigest, meta: ObjectMetadata) -> ObjectRecord {
    ObjectRecord {
        size: digest.size,
        etag: md5_hex(digest),
        mtime: now(),
        meta,
        checksum: digest.checksum.clone(),
        multipart: false,
        segments: Vec::new(),
        seg_id: None,
        nseg: 0,
    }
}

/// Bucket records, cached in memory: every request consults its bucket (existence,
/// public access, CORS) and they change rarely.
#[derive(Default)]
struct BucketCache {
    map: RwLock<HashMap<String, BucketInfo>>,
    /// Bumped on every bucket change, so a reader that loaded a record from an older
    /// snapshot never caches it.
    generation: AtomicU64,
}

impl BucketCache {
    fn get(&self, name: &str) -> Option<BucketInfo> {
        self.map.read().unwrap().get(name).cloned()
    }

    fn fill(&self, generation: u64, info: &BucketInfo) {
        let mut m = self.map.write().unwrap();
        if self.generation.load(Ordering::SeqCst) == generation {
            m.insert(info.name.clone(), info.clone());
        }
    }

    fn invalidate(&self, name: &str) {
        let mut m = self.map.write().unwrap();
        self.generation.fetch_add(1, Ordering::SeqCst);
        m.remove(name);
    }
}

/// What CompleteMultipartUpload checked before committing.
struct Completion {
    upload_id: String,
    /// Part number of each segment.
    numbers: Vec<u32>,
    /// Upload generation the parts were read at.
    generation: u64,
}

pub struct LocalEngine<B: BlobStore = FileBlobStore> {
    db: Arc<Database>,
    blobs: Arc<B>,
    committer: Committer,
    reclaimer: Reclaimer,
    leases: Arc<Leases>,
    budget: ReadBudget,
    buckets: BucketCache,
    /// Held by GC and by each scrub page, so the two never run at once.
    maintenance: tokio::sync::Mutex<()>,
    /// Exclusive lock on the data directory, held for the engine's lifetime.
    _lock: Option<std::fs::File>,
}

impl LocalEngine<FileBlobStore> {
    pub fn open(data_dir: &Path, fsync: bool) -> S3Result<Self> {
        Self::open_with(data_dir, EngineOptions { fsync, ..Default::default() })
    }

    pub fn open_with(data_dir: &Path, opts: EngineOptions) -> S3Result<Self> {
        std::fs::create_dir_all(data_dir)?;
        // Take ownership of the directory before touching anything in it: startup
        // clears tmp/, which would destroy another server's in-flight uploads.
        let lock = lock_data_dir(data_dir)?;
        let blobs = FileBlobStore::new(data_dir, opts.fsync)?;
        let mut engine = LocalEngine::with_blobs(&data_dir.join("meta.redb"), blobs, opts)?;
        engine._lock = Some(lock);
        Ok(engine)
    }
}

fn lock_data_dir(data_dir: &Path) -> S3Result<std::fs::File> {
    let path = data_dir.join("objex.lock");
    let f = std::fs::OpenOptions::new().create(true).truncate(false).write(true).open(&path)?;
    match f.try_lock() {
        Ok(()) => Ok(f),
        Err(std::fs::TryLockError::WouldBlock) => Err(S3Error::msg(
            ErrorCode::InternalError,
            format!("{} is in use by another objex process", data_dir.display()),
        )),
        Err(std::fs::TryLockError::Error(e)) => Err(S3Error::internal(format!("locking {}: {e}", path.display()))),
    }
}

impl<B: BlobStore> LocalEngine<B> {
    pub fn with_blobs(meta_path: &Path, blobs: B, opts: EngineOptions) -> S3Result<Self> {
        let db = Arc::new(Database::create(meta_path)?);
        meta::init(&db)?;
        let blobs = Arc::new(blobs);
        let leases = Arc::new(Leases::default());
        let committer = Committer::start(db.clone(), CommitOptions { fsync: opts.fsync, window: opts.commit_window, queue: opts.commit_queue })?;
        let reclaimer = Reclaimer::new(blobs.clone(), leases.clone(), opts.reclaim_delay);
        Ok(LocalEngine {
            db,
            blobs,
            committer,
            reclaimer,
            leases,
            budget: ReadBudget::new(opts.read_buffer),
            buckets: BucketCache::default(),
            maintenance: tokio::sync::Mutex::new(()),
            _lock: None,
        })
    }

    /// Delete queued blobs now instead of after the reclaim delay (tests, shutdown).
    pub async fn flush_reclaim(&self) {
        self.reclaimer.flush().await
    }

    async fn blocking<T: Send + 'static>(&self, f: impl FnOnce(&Database) -> S3Result<T> + Send + 'static) -> S3Result<T> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || f(&db)).await?
    }

    async fn write<T: Send + 'static>(&self, f: impl FnOnce(&WriteTransaction) -> S3Result<T> + Send + 'static) -> S3Result<T> {
        self.committer.write(f).await
    }

    async fn require_bucket(&self, bucket: &str) -> S3Result<()> {
        self.get_bucket(bucket).await.map(|_| ())
    }

    /// An object's summary record (no segment list).
    async fn read_object(&self, bucket: &str, key: &str) -> S3Result<ObjectRecord> {
        self.require_bucket(bucket).await?;
        let (bucket, key) = (bucket.to_string(), key.to_string());
        self.blocking(move |db| {
            let txn = db.begin_read()?;
            let t = txn.open_table(OBJECTS)?;
            let v = t.get((bucket.as_str(), key.as_str()))?.ok_or(ErrorCode::NoSuchKey)?;
            dec(v.value())
        })
        .await
    }

    /// An object's record and segments, leased so they stay readable until the
    /// lease is dropped. The lease is taken right after the snapshot is read, well
    /// inside the reclaim delay.
    async fn read_object_leased(&self, bucket: &str, key: &str) -> S3Result<(ObjectRecord, Vec<Segment>, reclaim::LeaseGuard)> {
        self.require_bucket(bucket).await?;
        let (bucket, key, leases) = (bucket.to_string(), key.to_string(), self.leases.clone());
        self.blocking(move |db| {
            let txn = db.begin_read()?;
            let v = txn.open_table(OBJECTS)?.get((bucket.as_str(), key.as_str()))?.ok_or(ErrorCode::NoSuchKey)?;
            let rec: ObjectRecord = dec(v.value())?;
            let segs = load_segments(&txn.open_table(SEGMENTS)?, &rec)?;
            let lease = leases.acquire(segs.iter().map(|s| s.blob.id.clone()).collect());
            Ok((rec, segs, lease))
        })
        .await
    }

    fn reader(&self, segments: Vec<Segment>, lease: reclaim::LeaseGuard) -> ObjectReader {
        let blobs: Arc<dyn BlobStore> = self.blobs.clone();
        ObjectReader::new(blobs, segments.into_iter().map(Into::into).collect(), self.budget.clone(), Some(Box::new(lease)))
    }

    /// Commit `rec` (with `segments`) as bucket/key, subject to `cond`. The replaced
    /// object's blobs are reclaimed in the background. On failure the caller still
    /// owns the new blobs. With `completion`, the multipart upload is consumed in the
    /// same transaction.
    async fn commit_object(&self, bucket: &str, key: &str, rec: ObjectRecord, segments: Vec<Segment>, cond: WriteConditions, completion: Option<Completion>) -> S3Result<ObjectInfo> {
        let mut info = rec.info(key);
        if rec.multipart {
            info.parts_count = segments.len() as u32;
        }
        let (b, k) = (bucket.to_string(), key.to_string());
        let garbage = self
            .write(move |txn| {
                // Validate everything before modifying anything (see Committer).
                bucket_exists_in(&txn.open_table(BUCKETS)?, &b)?;
                let mut objects = txn.open_table(OBJECTS)?;
                let old: Option<ObjectRecord> = match objects.get((b.as_str(), k.as_str()))? {
                    Some(v) => Some(dec(v.value())?),
                    None => None,
                };
                cond.check(old.as_ref().map(|o| o.etag.as_str()))?;
                if let Some(c) = &completion {
                    let current: UploadRecord = match txn.open_table(UPLOADS)?.get((b.as_str(), k.as_str(), c.upload_id.as_str()))? {
                        Some(v) => dec(v.value())?,
                        None => return Err(ErrorCode::NoSuchUpload.into()),
                    };
                    if current.generation != c.generation {
                        // Some part changed since completion read them. Each part used
                        // must still be the one the object was built from: a
                        // concurrent UploadPart may have replaced it.
                        let parts = txn.open_table(PARTS)?;
                        for (n, seg) in c.numbers.iter().zip(&segments) {
                            let current = match parts.get((c.upload_id.as_str(), *n))? {
                                Some(v) => Some(dec::<PartRecord>(v.value())?.blob.id),
                                None => None,
                            };
                            if current.as_deref() != Some(seg.blob.id.as_str()) {
                                return Err(S3Error::msg(ErrorCode::InvalidPart, format!("Part {n} was replaced while the upload was being completed")));
                            }
                        }
                    }
                }
                let mut segs = txn.open_table(SEGMENTS)?;
                let mut refs = txn.open_table(BLOB_REFS)?;
                let mut garbage = match &old {
                    Some(o) => release_object(&mut segs, &mut refs, o)?,
                    None => Vec::new(),
                };
                if let Some(c) = completion {
                    txn.open_table(UPLOADS)?.remove((b.as_str(), k.as_str(), c.upload_id.as_str()))?;
                    // Parts the object does not use become garbage.
                    let used: HashSet<&str> = segments.iter().map(|s| s.blob.id.as_str()).collect();
                    let parts = remove_parts(&mut txn.open_table(PARTS)?, &mut refs, &c.upload_id)?;
                    garbage.extend(parts.into_iter().filter(|p| !used.contains(p.id.as_str())));
                }
                store_object(&mut objects, &mut segs, &mut refs, &b, &k, rec, segments)?;
                Ok(garbage)
            })
            .await?;
        self.reclaimer.reclaim(garbage);
        Ok(info)
    }

    /// Validate a CompleteMultipartUpload request and build the object record plus
    /// the part numbers it uses, from one consistent snapshot. Nothing is written.
    async fn prepare_complete(&self, bucket: &str, key: &str, upload_id: &str, parts: Vec<CompletePart>, opts: CompleteOptions) -> S3Result<(ObjectRecord, Vec<Segment>, Completion)> {
        if parts.is_empty() {
            return Err(S3Error::msg(ErrorCode::MalformedXML, "You must specify at least one part"));
        }
        if parts.windows(2).any(|w| w[0].number >= w[1].number) {
            return Err(ErrorCode::InvalidPartOrder.into());
        }
        self.require_bucket(bucket).await?;
        let numbers: Vec<u32> = parts.iter().map(|p| p.number).collect();
        let (upload, stored) = {
            let (b, k, u, numbers) = (bucket.to_string(), key.to_string(), upload_id.to_string(), numbers.clone());
            self.blocking(move |db| {
                let txn = db.begin_read()?;
                let upload: UploadRecord = match txn.open_table(UPLOADS)?.get((b.as_str(), k.as_str(), u.as_str()))? {
                    Some(v) => dec(v.value())?,
                    None => return Err(ErrorCode::NoSuchUpload.into()),
                };
                let t = txn.open_table(PARTS)?;
                let stored = numbers
                    .into_iter()
                    .map(|n| match t.get((u.as_str(), n))? {
                        Some(v) => Ok(Some(dec::<PartRecord>(v.value())?)),
                        None => Ok(None),
                    })
                    .collect::<S3Result<Vec<_>>>()?;
                Ok((upload, stored))
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
            segments.push(Segment { blob: have.blob, size: have.size, crc32c: have.crc32c });
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
                // The whole value must match: a composite checksum carries its "-N"
                // part count and a full-object checksum carries no suffix.
                Some(got) if checksum::valid_value(want) && got == want => {}
                _ => {
                    return Err(S3Error::msg(ErrorCode::BadDigest, format!("The {} you specified did not match the calculated checksum.", want.algo.name())));
                }
            }
        }
        use md5::Digest;
        let etag = format!("{}-{count}", hex::encode(md5::Md5::digest(&md5s)));
        let rec = ObjectRecord { size, etag, mtime: now(), meta: upload.meta, checksum, multipart: true, segments: Vec::new(), seg_id: None, nseg: 0 };
        let completion = Completion { upload_id: upload_id.to_string(), numbers, generation: upload.generation };
        Ok((rec, segments, completion))
    }

    async fn read_upload(&self, bucket: &str, key: &str, upload_id: &str) -> S3Result<UploadRecord> {
        self.require_bucket(bucket).await?;
        let (b, k, u) = (bucket.to_string(), key.to_string(), upload_id.to_string());
        self.blocking(move |db| {
            let txn = db.begin_read()?;
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
                let mut uploads = txn.open_table(UPLOADS)?;
                let mut upload: UploadRecord = match uploads.get((b.as_str(), k.as_str(), u.as_str()))? {
                    Some(v) => dec(v.value())?,
                    None => return Err(ErrorCode::NoSuchUpload.into()),
                };
                upload.generation += 1;
                uploads.insert((b.as_str(), k.as_str(), u.as_str()), enc(&upload).as_slice())?;
                let mut refs = txn.open_table(BLOB_REFS)?;
                refs.insert(rec.blob.id.as_str(), ())?;
                let old = match txn.open_table(PARTS)?.insert((u.as_str(), number), enc(&rec).as_slice())? {
                    Some(v) => Some(dec::<PartRecord>(v.value())?.blob),
                    None => None,
                };
                if let Some(o) = &old {
                    refs.remove(o.id.as_str())?;
                }
                Ok(old)
            })
            .await;
        match res {
            Ok(old) => {
                self.reclaimer.reclaim(old.into_iter().collect());
                Ok(info)
            }
            Err(e) => {
                self.reclaimer.reclaim(vec![blob]);
                Err(e)
            }
        }
    }

    fn check_part_number(part: u32) -> S3Result<()> {
        if !(1..=MAX_PART_NUMBER).contains(&part) {
            return Err(S3Error::msg(ErrorCode::InvalidArgument, "Part number must be an integer between 1 and 10000, inclusive"));
        }
        Ok(())
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
        let res = self
            .write(move |txn| {
                let mut t = txn.open_table(BUCKETS)?;
                if t.get(info.name.as_str())?.is_some() {
                    return Err(ErrorCode::BucketAlreadyOwnedByYou.into());
                }
                t.insert(info.name.as_str(), enc(&info).as_slice())?;
                Ok(())
            })
            .await;
        self.buckets.invalidate(name);
        res
    }

    async fn get_bucket(&self, name: &str) -> S3Result<BucketInfo> {
        if let Some(info) = self.buckets.get(name) {
            return Ok(info);
        }
        let generation = self.buckets.generation.load(Ordering::SeqCst);
        let n = name.to_string();
        let info: BucketInfo = self
            .blocking(move |db| {
                let txn = db.begin_read()?;
                let t = txn.open_table(BUCKETS)?;
                let v = t.get(n.as_str())?.ok_or(ErrorCode::NoSuchBucket)?;
                dec(v.value())
            })
            .await?;
        self.buckets.fill(generation, &info);
        Ok(info)
    }

    async fn update_bucket(&self, name: &str, update: BucketUpdate) -> S3Result<()> {
        let n = name.to_string();
        let res = self
            .write(move |txn| {
                let mut t = txn.open_table(BUCKETS)?;
                let mut info: BucketInfo = match t.get(n.as_str())? {
                    Some(v) => dec(v.value())?,
                    None => return Err(ErrorCode::NoSuchBucket.into()),
                };
                match update {
                    BucketUpdate::PublicRead(p) => info.public_read = p,
                    BucketUpdate::Cors(c) => info.cors = c,
                }
                t.insert(n.as_str(), enc(&info).as_slice())?;
                Ok(())
            })
            .await;
        self.buckets.invalidate(name);
        res
    }

    async fn delete_bucket(&self, name: &str) -> S3Result<()> {
        let n = name.to_string();
        let res = self
            .write(move |txn| {
                let mut buckets = txn.open_table(BUCKETS)?;
                if buckets.get(n.as_str())?.is_none() {
                    return Err(ErrorCode::NoSuchBucket.into());
                }
                let objects = txn.open_table(OBJECTS)?;
                if let Some(e) = objects.range((n.as_str(), "")..)?.next()
                    && e?.0.value().0 == n
                {
                    return Err(ErrorCode::BucketNotEmpty.into());
                }
                // In-progress multipart uploads are aborted with the bucket.
                let mut uploads = txn.open_table(UPLOADS)?;
                let mut ids = Vec::new();
                for e in uploads.range((n.as_str(), "", "")..)? {
                    let (k, _) = e?;
                    let (b, key, id) = k.value();
                    if b != n {
                        break;
                    }
                    ids.push((key.to_string(), id.to_string()));
                }
                let mut parts = txn.open_table(PARTS)?;
                let mut refs = txn.open_table(BLOB_REFS)?;
                let mut garbage = Vec::new();
                for (key, id) in ids {
                    uploads.remove((n.as_str(), key.as_str(), id.as_str()))?;
                    garbage.extend(remove_parts(&mut parts, &mut refs, &id)?);
                }
                buckets.remove(n.as_str())?;
                Ok(garbage)
            })
            .await;
        self.buckets.invalidate(name);
        self.reclaimer.reclaim(res?);
        Ok(())
    }

    async fn put_object(&self, bucket: &str, key: &str, src: &mut dyn ByteSource, opts: PutOptions) -> S3Result<ObjectInfo> {
        validate_key(key)?;
        self.require_bucket(bucket).await?;
        let (blob, digest) = self.blobs.put(src, PutHasher::new(&opts.expect), MAX_PUT_SIZE).await?;
        let rec = single_record(&digest, opts.meta);
        let seg = Segment { blob: blob.clone(), size: digest.size, crc32c: Some(digest.crc32c) };
        let res = self.commit_object(bucket, key, rec, vec![seg], opts.cond, None).await;
        if res.is_err() {
            self.reclaimer.reclaim(vec![blob]);
        }
        res
    }

    async fn head_object(&self, bucket: &str, key: &str) -> S3Result<ObjectInfo> {
        Ok(self.read_object(bucket, key).await?.info(key))
    }

    async fn get_object(&self, bucket: &str, key: &str) -> S3Result<(ObjectInfo, ObjectReader)> {
        let (rec, segs, lease) = self.read_object_leased(bucket, key).await?;
        Ok((rec.info(key), self.reader(segs, lease)))
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
                let mut segs = txn.open_table(SEGMENTS)?;
                let mut refs = txn.open_table(BLOB_REFS)?;
                let mut results = Vec::with_capacity(keys.len());
                let mut garbage = Vec::new();
                for k in &keys {
                    if let Err(e) = validate_key(k) {
                        results.push(Err(e));
                        continue;
                    }
                    let removed = match objects.remove((b.as_str(), k.as_str()))? {
                        Some(v) => Some(dec::<ObjectRecord>(v.value())?),
                        None => None,
                    };
                    if let Some(rec) = removed {
                        garbage.extend(release_object(&mut segs, &mut refs, &rec)?);
                    }
                    results.push(Ok(()));
                }
                Ok((results, garbage))
            })
            .await?;
        self.reclaimer.reclaim(garbage);
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
        let (src, segs, lease) = self.read_object_leased(src_bucket, src_key).await?;
        opts.src_cond.check(&src.info(src_key), true)?;
        let originals: Vec<BlobRef> = segs.iter().map(|s| s.blob.clone()).collect();
        let dups = self.blobs.duplicate_many(&originals).await.map_err(S3Error::internal)?;
        drop(lease);
        let segments: Vec<Segment> = segs.into_iter().zip(&dups).map(|(s, d)| Segment { blob: d.clone(), ..s }).collect();
        let rec = ObjectRecord { mtime: now(), meta: opts.replace_meta.unwrap_or_else(|| src.meta.clone()), ..src };
        let res = self.commit_object(bucket, key, rec, segments, opts.dst_cond, None).await;
        if res.is_err() {
            self.reclaimer.reclaim(dups);
        }
        res
    }

    async fn list_objects(&self, bucket: &str, q: ListQuery) -> S3Result<ListResult> {
        self.require_bucket(bucket).await?;
        let b = bucket.to_string();
        self.blocking(move |db| {
            let txn = db.begin_read()?;
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
        let rec = UploadRecord { initiated: now(), meta, checksum_algo: checksum.map(|c| c.0), checksum_type: checksum.map(|c| c.1), generation: 0 };
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
        Self::check_part_number(part)?;
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
        let rec = PartRecord { etag: md5_hex(&digest), size: digest.size, mtime: now(), crc32c: Some(digest.crc32c), checksum: digest.checksum, blob };
        self.commit_part(bucket, key, upload_id, part, rec).await
    }

    async fn upload_part_copy(&self, src_bucket: &str, src_key: &str, range: Option<(u64, u64)>, cond: ReadConditions, bucket: &str, key: &str, upload_id: &str, part: u32) -> S3Result<(PartInfo, ObjectInfo)> {
        Self::check_part_number(part)?;
        let upload = self.read_upload(bucket, key, upload_id).await?;
        let (src, segs, lease) = self.read_object_leased(src_bucket, src_key).await?;
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
        // A whole single-part source already has the part's digests: link its blob
        // instead of copying the bytes.
        let reusable = !src.multipart
            && segs.len() == 1
            && start == 0
            && len == src.size
            && segs[0].crc32c.is_some()
            && upload.checksum_algo.is_none_or(|a| src.checksum.as_ref().is_some_and(|c| c.algo == a));
        let rec = if reusable {
            let seg = &segs[0];
            let blob = self.blobs.duplicate(&seg.blob).await.map_err(S3Error::internal)?;
            let checksum = upload.checksum_algo.and(src.checksum.clone());
            PartRecord { etag: src.etag.clone(), size: seg.size, mtime: now(), crc32c: seg.crc32c, checksum, blob }
        } else {
            let reader = self.reader(segs, lease);
            let mut source = ChannelSource(reader.open(start, len).await.map_err(S3Error::internal)?);
            let expect = PutExpect { checksum_algo: upload.checksum_algo, size: Some(len), ..Default::default() };
            let (blob, digest) = self.blobs.put(&mut source, PutHasher::new(&expect), MAX_PUT_SIZE).await?;
            PartRecord { etag: md5_hex(&digest), size: digest.size, mtime: now(), crc32c: Some(digest.crc32c), checksum: digest.checksum, blob }
        };
        let info = self.commit_part(bucket, key, upload_id, part, rec).await?;
        Ok((info, src_info))
    }

    async fn complete_multipart(&self, bucket: &str, key: &str, upload_id: &str, parts: Vec<CompletePart>, opts: CompleteOptions) -> S3Result<ObjectInfo> {
        let cond = opts.cond.clone();
        let (rec, segments, completion) = self.prepare_complete(bucket, key, upload_id, parts, opts).await?;
        self.commit_object(bucket, key, rec, segments, cond, Some(completion)).await
    }

    async fn abort_multipart(&self, bucket: &str, key: &str, upload_id: &str) -> S3Result<()> {
        let (b, k, u) = (bucket.to_string(), key.to_string(), upload_id.to_string());
        let garbage = self
            .write(move |txn| {
                bucket_exists_in(&txn.open_table(BUCKETS)?, &b)?;
                if txn.open_table(UPLOADS)?.remove((b.as_str(), k.as_str(), u.as_str()))?.is_none() {
                    return Err(ErrorCode::NoSuchUpload.into());
                }
                remove_parts(&mut txn.open_table(PARTS)?, &mut txn.open_table(BLOB_REFS)?, &u)
            })
            .await?;
        self.reclaimer.reclaim(garbage);
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
                if marker == u32::MAX {
                    return Ok((parts, false));
                }
                for e in t.range((u.as_str(), marker + 1)..=(u.as_str(), u32::MAX))? {
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
        self.require_bucket(bucket).await?;
        let b = bucket.to_string();
        self.blocking(move |db| {
            let txn = db.begin_read()?;
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

#[cfg(test)]
mod tests;
