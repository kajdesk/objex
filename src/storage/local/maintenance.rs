//! Garbage collection and integrity scrubbing. Both work in bounded pages and take
//! the engine's maintenance lock, so they never run at the same time.

use std::io::SeekFrom;
use std::time::{Duration, Instant, SystemTime};

use redb::ReadableDatabase;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use super::blobs::{leaf_candidates, leaf_dirs};
use super::meta::*;
use super::*;

/// Records verified per scrub page (and per persisted progress step).
const SCRUB_PAGE: usize = 128;
const SCRUB_CURSOR_KEY: &str = "scrub_cursor";

/// Outcome of [`LocalEngine::scrub`].
#[derive(Debug, Default)]
pub struct ScrubReport {
    /// Blobs checked.
    pub blobs: u64,
    pub bytes: u64,
    /// Blobs without a recorded checksum (written before checksums were kept); only
    /// their size was checked.
    pub unverified: u64,
    /// Missing, truncated or corrupt blobs, described.
    pub problems: Vec<String>,
}

/// Where an interrupted scrub resumes. Persisted after every page.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Cursor {
    /// Past the object phase: scanning parts.
    parts: bool,
    /// Last (bucket, key) or (upload id, part) done.
    a: String,
    b: String,
    n: u32,
}

enum Owner {
    Object(String, String),
    Part(String, u32),
}

impl Owner {
    fn describe(&self) -> String {
        match self {
            Owner::Object(b, k) => format!("object {b}/{k}"),
            Owner::Part(u, n) => format!("upload {u} part {n}"),
        }
    }
}

/// Paces reads to a byte rate.
struct Pacer {
    start: Instant,
    bytes: u64,
    rate: Option<u64>,
}

impl Pacer {
    async fn consumed(&mut self, n: usize) {
        self.bytes += n as u64;
        if let Some(rate) = self.rate.filter(|r| *r > 0) {
            let due = Duration::from_secs_f64(self.bytes as f64 / rate as f64);
            if let Some(wait) = due.checked_sub(self.start.elapsed()) {
                tokio::time::sleep(wait).await;
            }
        }
    }
}

impl LocalEngine<FileBlobStore> {
    /// Delete blob files that no metadata references, one shard directory at a time.
    /// Blobs changed within `grace` are skipped (they may belong to a write that has
    /// not committed yet), as are blobs leased by readers. Normal deletes go through
    /// the reclaimer; this repairs what a crash or failed delete left behind.
    pub async fn gc(&self, grace: Duration) -> S3Result<usize> {
        let _m = self.maintenance.lock().await;
        let root = self.blobs.blobs_dir();
        let cutoff = SystemTime::now() - grace;
        let leaves = tokio::task::spawn_blocking(move || leaf_dirs(&root)).await??;
        let mut removed = 0;
        for leaf in leaves {
            let candidates = match tokio::task::spawn_blocking(move || leaf_candidates(&leaf, cutoff)).await? {
                Ok(c) => c,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            };
            if candidates.is_empty() {
                continue;
            }
            let unreferenced = self
                .blocking(move |db| {
                    let txn = db.begin_read()?;
                    let refs = txn.open_table(BLOB_REFS)?;
                    let mut out = Vec::new();
                    for id in candidates {
                        if refs.get(id.as_str())?.is_none() {
                            out.push(id);
                        }
                    }
                    Ok(out)
                })
                .await?;
            for id in unreferenced {
                if self.leases.is_leased(&id) {
                    continue;
                }
                if self.blobs.delete(&BlobRef { id, layout: ErasureLayout::default() }).await.is_ok() {
                    removed += 1;
                }
            }
        }
        Ok(removed)
    }
}

impl<B: BlobStore> LocalEngine<B> {
    /// Whether a scrub was interrupted and will resume where it stopped.
    pub async fn scrub_in_progress(&self) -> S3Result<bool> {
        self.blocking(|db| Ok(db.begin_read()?.open_table(META)?.get(SCRUB_CURSOR_KEY)?.is_some())).await
    }

    /// Verify every referenced blob against its size and checksum, reading at most
    /// `rate` bytes/second. Works in pages, persisting progress after each, so an
    /// interrupted scrub resumes instead of starting over. Problems are logged and
    /// reported; a blob that changes owner while being checked is not a problem.
    pub async fn scrub(&self, rate: Option<u64>) -> S3Result<ScrubReport> {
        let mut cursor: Cursor = self
            .blocking(|db| match db.begin_read()?.open_table(META)?.get(SCRUB_CURSOR_KEY)? {
                Some(v) => dec(v.value()),
                None => Ok(Cursor::default()),
            })
            .await?;
        let mut report = ScrubReport::default();
        let mut pacer = Pacer { start: Instant::now(), bytes: 0, rate };
        loop {
            let _m = self.maintenance.lock().await;
            let (page, next) = self.scrub_page(cursor.clone()).await?;
            for (owner, segs) in page {
                for seg in segs {
                    report.blobs += 1;
                    report.bytes += seg.size;
                    if seg.crc32c.is_none() {
                        report.unverified += 1;
                    }
                    if let Err(problem) = self.verify(&seg, &mut pacer).await
                        && self.still_owns(&owner, &seg.blob.id).await?
                    {
                        let msg = format!("{}: blob {}: {problem}", owner.describe(), seg.blob.id);
                        tracing::error!("scrub: {msg}");
                        report.problems.push(msg);
                    }
                }
            }
            let done = next.is_none();
            let saved = next.clone();
            self.write(move |txn| {
                let mut meta = txn.open_table(META)?;
                match &saved {
                    Some(c) => meta.insert(SCRUB_CURSOR_KEY, enc(c).as_slice())?,
                    None => meta.remove(SCRUB_CURSOR_KEY)?,
                };
                Ok(())
            })
            .await?;
            match next {
                Some(c) => cursor = c,
                None if done => return Ok(report),
                None => unreachable!(),
            }
        }
    }

    /// Up to SCRUB_PAGE records after `c`, with their segments, and the cursor to
    /// continue from (None when everything has been covered).
    async fn scrub_page(&self, c: Cursor) -> S3Result<(Vec<(Owner, Vec<Segment>)>, Option<Cursor>)> {
        self.blocking(move |db| {
            let txn = db.begin_read()?;
            let mut page = Vec::new();
            if !c.parts {
                let objects = txn.open_table(OBJECTS)?;
                let segs = txn.open_table(SEGMENTS)?;
                let from = if c.a.is_empty() && c.b.is_empty() {
                    Bound::Unbounded
                } else {
                    Bound::Excluded((c.a.as_str(), c.b.as_str()))
                };
                for e in objects.range::<(&str, &str)>((from, Bound::Unbounded))? {
                    let (k, v) = e?;
                    let (b, key) = k.value();
                    let rec: ObjectRecord = dec(v.value())?;
                    page.push((Owner::Object(b.to_string(), key.to_string()), load_segments(&segs, &rec)?));
                    if page.len() >= SCRUB_PAGE {
                        return Ok((page, Some(Cursor { parts: false, a: b.to_string(), b: key.to_string(), n: 0 })));
                    }
                }
                return Ok((page, Some(Cursor { parts: true, ..Default::default() })));
            }
            let parts = txn.open_table(PARTS)?;
            let from = if c.a.is_empty() { Bound::Unbounded } else { Bound::Excluded((c.a.as_str(), c.n)) };
            for e in parts.range::<(&str, u32)>((from, Bound::Unbounded))? {
                let (k, v) = e?;
                let (u, n) = k.value();
                let rec: PartRecord = dec(v.value())?;
                page.push((Owner::Part(u.to_string(), n), vec![Segment { blob: rec.blob, size: rec.size, crc32c: rec.crc32c }]));
                if page.len() >= SCRUB_PAGE {
                    return Ok((page, Some(Cursor { parts: true, a: u.to_string(), b: String::new(), n })));
                }
            }
            Ok((page, None))
        })
        .await
    }

    /// Read a whole blob, checking its size and (when known) CRC32C.
    async fn verify(&self, seg: &Segment, pacer: &mut Pacer) -> Result<(), String> {
        let mut f = match self.blobs.open(&seg.blob).await {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Err("blob file is missing".into()),
            Err(e) => return Err(e.to_string()),
        };
        let len = f.seek(SeekFrom::End(0)).await.map_err(|e| e.to_string())?;
        if len != seg.size {
            return Err(format!("size is {len}, expected {}", seg.size));
        }
        let Some(want) = seg.crc32c else { return Ok(()) };
        f.seek(SeekFrom::Start(0)).await.map_err(|e| e.to_string())?;
        let mut buf = vec![0u8; 1 << 20];
        let mut got = 0u32;
        loop {
            let n = f.read(&mut buf).await.map_err(|e| e.to_string())?;
            if n == 0 {
                break;
            }
            got = crc32c::crc32c_append(got, &buf[..n]);
            pacer.consumed(n).await;
        }
        if got != want { Err("checksum mismatch".into()) } else { Ok(()) }
    }

    async fn still_owns(&self, owner: &Owner, id: &str) -> S3Result<bool> {
        let owner = match owner {
            Owner::Object(b, k) => Owner::Object(b.clone(), k.clone()),
            Owner::Part(u, n) => Owner::Part(u.clone(), *n),
        };
        let id = id.to_string();
        self.blocking(move |db| {
            let txn = db.begin_read()?;
            Ok(match owner {
                Owner::Object(b, k) => match txn.open_table(OBJECTS)?.get((b.as_str(), k.as_str()))? {
                    Some(v) => {
                        let rec: ObjectRecord = dec(v.value())?;
                        load_segments(&txn.open_table(SEGMENTS)?, &rec)?.iter().any(|s| s.blob.id == id)
                    }
                    None => false,
                },
                Owner::Part(u, n) => match txn.open_table(PARTS)?.get((u.as_str(), n))? {
                    Some(v) => dec::<PartRecord>(v.value())?.blob.id == id,
                    None => false,
                },
            })
        })
        .await
    }
}
