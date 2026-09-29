//! Metadata tables, records, and the group committer.

use std::sync::Arc;
use std::sync::mpsc::RecvTimeoutError;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use redb::{Database, Durability, ReadableTable, Table, TableDefinition, WriteTransaction};
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

use super::super::*;
use crate::checksum::ChecksumType;

pub(super) const BUCKETS: TableDefinition<&str, &[u8]> = TableDefinition::new("buckets");
/// (bucket, key) -> ObjectRecord
pub(super) const OBJECTS: TableDefinition<(&str, &str), &[u8]> = TableDefinition::new("objects");
/// (segment list id, index) -> Segment, for multipart objects
pub(super) const SEGMENTS: TableDefinition<(u128, u32), &[u8]> = TableDefinition::new("segments");
/// (bucket, key, upload id) -> UploadRecord
pub(super) const UPLOADS: TableDefinition<(&str, &str, &str), &[u8]> = TableDefinition::new("uploads");
/// (upload id, part number) -> PartRecord
pub(super) const PARTS: TableDefinition<(&str, u32), &[u8]> = TableDefinition::new("parts");
/// Every blob id referenced by an object or part. Lets GC check a file with a point
/// lookup instead of loading every reference into memory.
pub(super) const BLOB_REFS: TableDefinition<&str, ()> = TableDefinition::new("blob_refs");
/// Engine state: schema version, scrub progress.
pub(super) const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");

const SCHEMA_KEY: &str = "schema";
/// 2: BLOB_REFS index; multipart segments in SEGMENTS.
const SCHEMA_VERSION: u32 = 2;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct Segment {
    pub blob: BlobRef,
    pub size: u64,
    /// Internal integrity checksum of the blob (absent in records from before it).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub crc32c: Option<u32>,
}

impl From<Segment> for SegmentRef {
    fn from(s: Segment) -> Self {
        SegmentRef { blob: s.blob, size: s.size, crc32c: s.crc32c }
    }
}

/// An object's summary. Kept small: listings and HEAD decode one per key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ObjectRecord {
    pub size: u64,
    pub etag: String,
    pub mtime: DateTime<Utc>,
    #[serde(default)]
    pub meta: ObjectMetadata,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checksum: Option<Checksum>,
    #[serde(default)]
    pub multipart: bool,
    /// Segments stored inline: single-part objects, and multipart records written
    /// before schema 2.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub segments: Vec<Segment>,
    /// Multipart objects: their segments are SEGMENTS rows (seg_id, 0..nseg).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seg_id: Option<u128>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub nseg: u32,
}

fn is_zero(n: &u32) -> bool {
    *n == 0
}

impl ObjectRecord {
    pub fn info(&self, key: &str) -> ObjectInfo {
        ObjectInfo {
            key: key.to_string(),
            size: self.size,
            etag: self.etag.clone(),
            last_modified: self.mtime,
            meta: self.meta.clone(),
            checksum: self.checksum.clone(),
            parts_count: if !self.multipart {
                0
            } else if self.seg_id.is_some() {
                self.nseg
            } else {
                self.segments.len() as u32
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct UploadRecord {
    pub initiated: DateTime<Utc>,
    #[serde(default)]
    pub meta: ObjectMetadata,
    #[serde(default)]
    pub checksum_algo: Option<ChecksumAlgo>,
    #[serde(default)]
    pub checksum_type: Option<ChecksumType>,
    /// Bumped whenever a part is stored, so completion can tell with one read that
    /// no part changed since it looked.
    #[serde(default)]
    pub generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct PartRecord {
    pub etag: String,
    pub size: u64,
    pub mtime: DateTime<Utc>,
    #[serde(default)]
    pub checksum: Option<Checksum>,
    pub blob: BlobRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub crc32c: Option<u32>,
}

impl PartRecord {
    pub fn info(&self, number: u32) -> PartInfo {
        PartInfo { number, etag: self.etag.clone(), size: self.size, last_modified: self.mtime, checksum: self.checksum.clone() }
    }
}

pub(super) fn enc<T: Serialize>(v: &T) -> Vec<u8> {
    serde_json::to_vec(v).expect("record serialization")
}

pub(super) fn dec<T: for<'de> Deserialize<'de>>(b: &[u8]) -> S3Result<T> {
    serde_json::from_slice(b).map_err(S3Error::internal)
}

macro_rules! internal_from {
    ($($t:ty),*) => { $( impl From<$t> for S3Error { fn from(e: $t) -> Self { S3Error::internal(e) } } )* };
}
internal_from!(redb::Error, redb::DatabaseError, redb::TransactionError, redb::TableError, redb::StorageError, redb::CommitError, redb::SetDurabilityError, tokio::task::JoinError);

// ---------------------------------------------------------------------------
// Object data helpers, shared by every write path so BLOB_REFS stays exact.
// ---------------------------------------------------------------------------

type SegTable<'t> = Table<'t, (u128, u32), &'static [u8]>;
type RefTable<'t> = Table<'t, &'static str, ()>;

/// Segments of `rec`, from the record or the SEGMENTS table.
pub(super) fn load_segments(t: &impl ReadableTable<(u128, u32), &'static [u8]>, rec: &ObjectRecord) -> S3Result<Vec<Segment>> {
    let Some(id) = rec.seg_id else { return Ok(rec.segments.clone()) };
    let mut out = Vec::with_capacity(rec.nseg as usize);
    for e in t.range((id, 0)..(id, rec.nseg))? {
        out.push(dec(e?.1.value())?);
    }
    if out.len() != rec.nseg as usize {
        return Err(S3Error::internal(format!("segment list {id:x} has {} of {} entries", out.len(), rec.nseg)));
    }
    Ok(out)
}

/// Store `rec` with `segments`: inline for single-part objects, as SEGMENTS rows for
/// multipart ones. Registers the blobs in BLOB_REFS.
pub(super) fn store_object(objects: &mut Table<(&'static str, &'static str), &'static [u8]>, segs: &mut SegTable, refs: &mut RefTable, bucket: &str, key: &str, mut rec: ObjectRecord, segments: Vec<Segment>) -> S3Result<()> {
    for s in &segments {
        refs.insert(s.blob.id.as_str(), ())?;
    }
    if rec.multipart {
        let id = u128::from_be_bytes(rand::random());
        for (i, s) in segments.iter().enumerate() {
            segs.insert((id, i as u32), enc(s).as_slice())?;
        }
        rec.seg_id = Some(id);
        rec.nseg = segments.len() as u32;
        rec.segments = Vec::new();
    } else {
        rec.segments = segments;
        rec.seg_id = None;
        rec.nseg = 0;
    }
    objects.insert((bucket, key), enc(&rec).as_slice())?;
    Ok(())
}

/// Drop `rec`'s data references (not the OBJECTS row itself); returns its blobs.
pub(super) fn release_object(segs: &mut SegTable, refs: &mut RefTable, rec: &ObjectRecord) -> S3Result<Vec<BlobRef>> {
    let blobs: Vec<BlobRef> = match rec.seg_id {
        Some(id) => {
            let mut out = Vec::with_capacity(rec.nseg as usize);
            for i in 0..rec.nseg {
                if let Some(v) = segs.remove((id, i))? {
                    out.push(dec::<Segment>(v.value())?.blob);
                }
            }
            out
        }
        None => rec.segments.iter().map(|s| s.blob.clone()).collect(),
    };
    for b in &blobs {
        refs.remove(b.id.as_str())?;
    }
    Ok(blobs)
}

/// Remove every part of an upload; returns their blobs.
pub(super) fn remove_parts(parts: &mut Table<(&'static str, u32), &'static [u8]>, refs: &mut RefTable, upload_id: &str) -> S3Result<Vec<BlobRef>> {
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
    for b in &garbage {
        refs.remove(b.id.as_str())?;
    }
    Ok(garbage)
}

/// Create tables and bring older databases up to the current schema.
pub(super) fn init(db: &Database) -> S3Result<()> {
    let txn = db.begin_write()?;
    {
        txn.open_table(BUCKETS)?;
        txn.open_table(UPLOADS)?;
        let objects = txn.open_table(OBJECTS)?;
        let parts = txn.open_table(PARTS)?;
        let segs = txn.open_table(SEGMENTS)?;
        let mut refs = txn.open_table(BLOB_REFS)?;
        let mut meta = txn.open_table(META)?;
        let version: u32 = match meta.get(SCHEMA_KEY)? {
            Some(v) => dec(v.value())?,
            None => 1,
        };
        if version < 2 {
            let mut n = 0u64;
            for e in objects.iter()? {
                let rec: ObjectRecord = dec(e?.1.value())?;
                for s in load_segments(&segs, &rec)? {
                    refs.insert(s.blob.id.as_str(), ())?;
                    n += 1;
                }
            }
            for e in parts.iter()? {
                let rec: PartRecord = dec(e?.1.value())?;
                refs.insert(rec.blob.id.as_str(), ())?;
                n += 1;
            }
            if n > 0 {
                tracing::info!("metadata upgraded to schema {SCHEMA_VERSION}: indexed {n} blob reference(s)");
            }
        }
        if version != SCHEMA_VERSION {
            meta.insert(SCHEMA_KEY, enc(&SCHEMA_VERSION).as_slice())?;
        }
    }
    txn.commit()?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Group commit
// ---------------------------------------------------------------------------

/// A metadata write, run inside a shared transaction. It returns whether it failed
/// with an internal error (the transaction may be dirty and must be aborted), and a
/// callback that receives the commit outcome.
type Job = Box<dyn FnOnce(&WriteTransaction) -> (bool, Done) + Send>;
type Done = Box<dyn FnOnce(S3Result<()>) + Send>;

/// Most writes folded into one transaction.
const MAX_BATCH: usize = 256;

#[derive(Debug, Clone)]
pub struct CommitOptions {
    pub fsync: bool,
    /// After the first write of a batch arrives, wait this long for more to join it.
    /// Zero batches only writes that are already queued.
    pub window: Duration,
    /// Most writes queued at once; further writers wait.
    pub queue: usize,
}

/// Group commit: concurrent metadata writes share one redb transaction and so one
/// fsync. redb allows a single writer, so committing each write separately would
/// serialize every upload behind a disk flush.
///
/// Jobs must return client errors (precondition failed, missing upload...) before
/// modifying any table, so a failed request never leaves partial changes in a batch.
pub(super) struct Committer {
    tx: std::sync::mpsc::Sender<Job>,
    /// Bounds the queue: a slot is held from submission until the job runs.
    slots: Arc<Semaphore>,
}

impl Committer {
    pub fn start(db: Arc<Database>, opts: CommitOptions) -> S3Result<Self> {
        let (tx, rx) = std::sync::mpsc::channel();
        let slots = Arc::new(Semaphore::new(opts.queue.max(1)));
        std::thread::Builder::new().name("objex-commit".into()).spawn(move || run(db, opts, rx))?;
        Ok(Committer { tx, slots })
    }

    /// Run `f` in a write transaction (shared with concurrent writes) and wait until
    /// it is committed.
    pub async fn write<T: Send + 'static>(&self, f: impl FnOnce(&WriteTransaction) -> S3Result<T> + Send + 'static) -> S3Result<T> {
        let slot = self.slots.clone().acquire_owned().await.map_err(S3Error::internal)?;
        let (tx, rx) = tokio::sync::oneshot::channel::<S3Result<T>>();
        let job: Job = Box::new(move |txn| {
            drop(slot);
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
        self.tx.send(job).map_err(|_| S3Error::internal("metadata committer stopped"))?;
        rx.await.map_err(|_| S3Error::internal("metadata write dropped"))?
    }
}

fn run(db: Arc<Database>, opts: CommitOptions, jobs: std::sync::mpsc::Receiver<Job>) {
    while let Ok(first) = jobs.recv() {
        let mut batch = vec![first];
        let deadline = Instant::now() + opts.window;
        while batch.len() < MAX_BATCH {
            let next = match deadline.checked_duration_since(Instant::now()) {
                Some(wait) if !wait.is_zero() => jobs.recv_timeout(wait),
                _ => jobs.try_recv().map_err(|_| RecvTimeoutError::Timeout),
            };
            match next {
                Ok(j) => batch.push(j),
                Err(_) => break,
            }
        }
        let mut txn = match db.begin_write() {
            Ok(t) => t,
            Err(e) => {
                // Dropping the jobs unblocks their waiters with an internal error.
                tracing::error!("begin_write: {e}");
                continue;
            }
        };
        if !opts.fsync
            && let Err(e) = txn.set_durability(Durability::None)
        {
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
