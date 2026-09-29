use std::path::PathBuf;
use std::time::SystemTime;

use bytes::Bytes;

use std::sync::atomic::Ordering;

use async_trait::async_trait;
use redb::{Database, ReadableTableMetadata};

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

async fn drain(mut rx: tokio::sync::mpsc::Receiver<io::Result<Bytes>>) -> Vec<u8> {
    let mut out = Vec::new();
    while let Some(c) = rx.recv().await {
        out.extend_from_slice(&c.unwrap());
    }
    out
}

async fn read_all<B: BlobStore>(e: &LocalEngine<B>, b: &str, k: &str) -> Vec<u8> {
    let (info, r) = e.get_object(b, k).await.unwrap();
    drain(r.open(0, info.size).await.unwrap()).await
}

fn all_blobs(d: &Path) -> Vec<String> {
    let future = SystemTime::now() + Duration::from_secs(60);
    blobs::leaf_dirs(&d.join("blobs")).unwrap().iter().flat_map(|l| blobs::leaf_candidates(l, future).unwrap()).collect()
}

/// Blob files on disk, once pending reclamation has run.
async fn blob_count<B: BlobStore>(e: &LocalEngine<B>, d: &Path) -> usize {
    e.flush_reclaim().await;
    all_blobs(d).len()
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
    assert_eq!(blob_count(&e, &d).await, 1);
    assert_eq!(e.delete_bucket("bk1").await.unwrap_err().code, ErrorCode::BucketNotEmpty);
    e.delete_object("bk1", "a/b.txt").await.unwrap();
    e.delete_object("bk1", "missing").await.unwrap();
    assert_eq!(e.head_object("bk1", "a/b.txt").await.unwrap_err().code, ErrorCode::NoSuchKey);
    assert_eq!(blob_count(&e, &d).await, 0);
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
    assert_eq!(info.parts_count, 2);
    let mut all = p1.clone();
    all.extend_from_slice(&p2);
    assert_eq!(read_all(&e, "bk1", "big").await, all);
    let mut h = crate::checksum::ChecksumHasher::new(ChecksumAlgo::Crc32);
    h.update(&all);
    assert_eq!(info.checksum.unwrap(), h.finish());
    // part 3 and the replaced part 1 are gone
    assert_eq!(blob_count(&e, &d).await, 2);
    assert_eq!(e.list_parts("bk1", "big", &id, 0, 10).await.unwrap_err().code, ErrorCode::NoSuchUpload);

    // ranged read across the part boundary
    let (_, r) = e.get_object("bk1", "big").await.unwrap();
    assert_eq!(r.part_range(2), Some((p1.len() as u64, p2.len() as u64)));
    let start = p1.len() as u64 - 10;
    let out = drain(r.open(start, 20).await.unwrap()).await;
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
    // Deletes are reclaimed in the background; let the source's blob go first so
    // GC only finds the orphan.
    e.flush_reclaim().await;
    assert_eq!(e.gc(Duration::from_secs(3600)).await.unwrap(), 0);
    assert_eq!(e.gc(Duration::ZERO).await.unwrap(), 1);
    assert!(!orphan.exists());
    assert_eq!(read_all(&e, "bk1", "dst").await, b"copy me");
    e.abort_multipart("bk1", "mp", &id).await.unwrap();
    assert_eq!(blob_count(&e, &d).await, 1);
}

/// OBJ-001: a part replaced between completion's reads and its commit must not
/// yield an object that references the replaced (deleted) blob.
#[tokio::test]
async fn complete_races_part_replacement() {
    let (e, _d) = engine().await;
    e.create_bucket("bk1", false).await.unwrap();
    let id = e.create_multipart("bk1", "k", ObjectMetadata::default(), None).await.unwrap();
    let upload = |data: &'static [u8]| {
        let (e, id) = (&e, id.clone());
        async move {
            let mut src = BytesSource(Some(Bytes::from_static(data)));
            e.upload_part("bk1", "k", &id, 1, &mut src, PutExpect::default()).await.unwrap()
        }
    };
    let a = upload(b"version A").await;
    let parts = vec![CompletePart { number: 1, etag: a.etag.clone(), checksum: None }];
    let (rec, segs, completion) = e.prepare_complete("bk1", "k", &id, parts, CompleteOptions::default()).await.unwrap();

    let b = upload(b"version B").await; // deletes blob A

    let err = e.commit_object("bk1", "k", rec, segs, WriteConditions::default(), Some(completion)).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::InvalidPart);
    assert_eq!(e.head_object("bk1", "k").await.unwrap_err().code, ErrorCode::NoSuchKey);

    // The upload is intact and completes with the new part.
    let parts = vec![CompletePart { number: 1, etag: b.etag, checksum: None }];
    e.complete_multipart("bk1", "k", &id, parts, CompleteOptions::default()).await.unwrap();
    assert_eq!(read_all(&e, "bk1", "k").await, b"version B");
}

/// OBJ-006: the client's full checksum value must match exactly, suffix included.
#[tokio::test]
async fn complete_checksum_suffix_is_strict() {
    let (e, _d) = engine().await;
    e.create_bucket("bk1", false).await.unwrap();
    let mut good = None;
    for attempt in ["-2", "-x", "", "-1"] {
        let id = e.create_multipart("bk1", "k", ObjectMetadata::default(), Some((ChecksumAlgo::Crc32, ChecksumType::Composite))).await.unwrap();
        let mut src = BytesSource(Some(Bytes::from_static(b"data")));
        let part = e.upload_part("bk1", "k", &id, 1, &mut src, PutExpect::default()).await.unwrap();
        let composite = crate::checksum::composite(ChecksumAlgo::Crc32, &[part.checksum.clone().unwrap()]).unwrap();
        let digest = composite.value.split('-').next().unwrap().to_string();
        let opts = CompleteOptions { checksum: Some(Checksum { algo: ChecksumAlgo::Crc32, value: format!("{digest}{attempt}") }), ..Default::default() };
        let parts = vec![CompletePart { number: 1, etag: part.etag, checksum: None }];
        let r = e.complete_multipart("bk1", "k", &id, parts, opts).await;
        if attempt == "-1" {
            good = Some(r.unwrap());
        } else {
            assert_eq!(r.unwrap_err().code, ErrorCode::BadDigest, "suffix {attempt:?}");
        }
    }
    assert!(good.unwrap().checksum.unwrap().value.ends_with("-1"));
}

/// OBJ-002: a second engine on the same directory fails without touching tmp/.
#[tokio::test]
async fn data_dir_is_exclusive() {
    let (_e, d) = engine().await;
    let inflight = d.join("tmp").join("upload-in-progress");
    std::fs::write(&inflight, b"partial").unwrap();
    let err = LocalEngine::open(&d, false).err().expect("second open must fail");
    assert!(err.message.contains("in use"), "{err}");
    assert_eq!(std::fs::read(&inflight).unwrap(), b"partial");
}

/// OBJ-005: corrupt and truncated blobs are caught on read and by scrub.
#[tokio::test]
async fn integrity_checks() {
    let (e, d) = engine().await;
    e.create_bucket("bk1", false).await.unwrap();
    let data = big(600_000, 4);
    let mut src = BytesSource(Some(Bytes::copy_from_slice(&data)));
    e.put_object("bk1", "k", &mut src, PutOptions::default()).await.unwrap();
    put(&e, "bk1", "fine", b"untouched").await;
    assert!(e.scrub(None).await.unwrap().problems.is_empty());

    let path = all_blobs(&d).iter().map(|id| e.blobs.path(id)).find(|p| std::fs::metadata(p).unwrap().len() == data.len() as u64).unwrap();
    // Flip one byte, keeping the length.
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[500_000] ^= 0xff;
    std::fs::write(&path, &bytes).unwrap();

    let (info, r) = e.get_object("bk1", "k").await.unwrap();
    let mut rx = r.open(0, info.size).await.unwrap();
    let mut received = 0;
    let mut failed = false;
    while let Some(c) = rx.recv().await {
        match c {
            Ok(b) => received += b.len(),
            Err(_) => failed = true,
        }
    }
    assert!(failed, "corruption must fail the stream");
    assert!(received < data.len(), "corrupt data must never arrive complete");
    // A range that doesn't cover the whole blob is served unverified.
    let (_, r) = e.get_object("bk1", "k").await.unwrap();
    let mut rx = r.open(0, 10).await.unwrap();
    assert!(rx.recv().await.unwrap().is_ok());

    let report = e.scrub(None).await.unwrap();
    assert_eq!(report.blobs, 2);
    assert_eq!(report.problems.len(), 1, "{:?}", report.problems);
    assert!(report.problems[0].contains("bk1/k") && report.problems[0].contains("mismatch"));

    // Truncation is caught when the data is opened, before any response starts.
    std::fs::write(&path, &bytes[..1000]).unwrap();
    let (info, r) = e.get_object("bk1", "k").await.unwrap();
    assert!(r.open(0, info.size).await.err().unwrap().to_string().contains("size is 1000"));
    assert!(e.scrub(None).await.unwrap().problems[0].contains("size is 1000"));
}

/// Counts blob opens, to check segments are opened lazily.
struct Counting(FileBlobStore, Arc<std::sync::atomic::AtomicUsize>);

#[async_trait]
impl BlobStore for Counting {
    async fn put(&self, src: &mut dyn ByteSource, hasher: PutHasher, max: u64) -> S3Result<(BlobRef, PutDigest)> {
        self.0.put(src, hasher, max).await
    }
    async fn open(&self, blob: &BlobRef) -> io::Result<Box<dyn BlobRead>> {
        self.1.fetch_add(1, Ordering::SeqCst);
        self.0.open(blob).await
    }
    async fn delete(&self, blob: &BlobRef) -> io::Result<()> {
        self.0.delete(blob).await
    }
    async fn duplicate(&self, blob: &BlobRef) -> io::Result<BlobRef> {
        self.0.duplicate(blob).await
    }
}

async fn multipart_object<B: BlobStore>(e: &LocalEngine<B>, key: &str, parts: &[Vec<u8>]) -> ObjectInfo {
    let id = e.create_multipart("bk1", key, ObjectMetadata::default(), None).await.unwrap();
    let mut done = Vec::new();
    for (i, p) in parts.iter().enumerate() {
        let mut src = BytesSource(Some(Bytes::copy_from_slice(p)));
        let info = e.upload_part("bk1", key, &id, i as u32 + 1, &mut src, PutExpect::default()).await.unwrap();
        done.push(CompletePart { number: info.number, etag: info.etag, checksum: None });
    }
    e.complete_multipart("bk1", key, &id, done, CompleteOptions::default()).await.unwrap()
}

/// PERF-002: a ranged GET opens only the segments it touches, and no file is
/// opened before the stream starts.
#[tokio::test]
async fn segments_open_lazily() {
    let d = tmpdir();
    let opens = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let store = Counting(FileBlobStore::new(&d, false).unwrap(), opens.clone());
    let e = LocalEngine::with_blobs(&d.join("meta.redb"), store, EngineOptions { fsync: false, ..Default::default() }).unwrap();
    e.create_bucket("bk1", false).await.unwrap();
    let parts = vec![big(MIN_PART_SIZE as usize, 1), big(MIN_PART_SIZE as usize, 2), big(100, 3)];
    multipart_object(&e, "mp", &parts).await;

    let (info, r) = e.get_object("bk1", "mp").await.unwrap();
    assert_eq!(opens.load(Ordering::SeqCst), 0, "get_object must not open files");
    let start = MIN_PART_SIZE + 5;
    let got = drain(r.open(start, 10).await.unwrap()).await;
    assert_eq!(got, &parts[1][5..15]);
    assert_eq!(opens.load(Ordering::SeqCst), 1, "only the touched segment is opened");

    let (_, r) = e.get_object("bk1", "mp").await.unwrap();
    let all = drain(r.open(0, info.size).await.unwrap()).await;
    assert_eq!(all, parts.concat());
}

/// PERF-003: a reader keeps an overwritten object's blobs until it is done, then
/// they are reclaimed in the background.
#[tokio::test]
async fn reads_survive_overwrite_and_delete() {
    let (e, d) = engine().await;
    e.create_bucket("bk1", false).await.unwrap();
    let parts = vec![big(MIN_PART_SIZE as usize, 1), big(4096, 2)];
    let info = multipart_object(&e, "mp", &parts).await;
    let (_, reader) = e.get_object("bk1", "mp").await.unwrap();
    put(&e, "bk1", "mp", b"replacement").await;
    e.flush_reclaim().await;
    assert_eq!(all_blobs(&d).len(), 3, "leased blobs are kept");
    assert_eq!(drain(reader.open(0, info.size).await.unwrap()).await, parts.concat());
    assert_eq!(blob_count(&e, &d).await, 1, "released once the reader is gone");
    assert_eq!(read_all(&e, "bk1", "mp").await, b"replacement");
}

/// PERF-004: GC finds orphans through the reference index, leaving every
/// referenced blob (objects, multipart segments, pending parts) alone.
#[tokio::test]
async fn gc_uses_reference_index() {
    let (e, d) = engine().await;
    e.create_bucket("bk1", false).await.unwrap();
    put(&e, "bk1", "a", b"one").await;
    multipart_object(&e, "mp", &[big(MIN_PART_SIZE as usize, 1), big(10, 2)]).await;
    let id = e.create_multipart("bk1", "pending", ObjectMetadata::default(), None).await.unwrap();
    let mut src = BytesSource(Some(Bytes::from_static(b"part")));
    e.upload_part("bk1", "pending", &id, 1, &mut src, PutExpect::default()).await.unwrap();
    e.copy_object("bk1", "mp", "bk1", "mp2", CopyOptions { replace_meta: None, src_cond: Default::default(), dst_cond: Default::default() }).await.unwrap();
    assert_eq!(blob_count(&e, &d).await, 6);
    assert_eq!(e.gc(Duration::ZERO).await.unwrap(), 0);
    let orphan = d.join("blobs/ff/ff/ffff0000");
    std::fs::create_dir_all(orphan.parent().unwrap()).unwrap();
    std::fs::write(&orphan, b"junk").unwrap();
    assert_eq!(e.gc(Duration::ZERO).await.unwrap(), 1);
    assert_eq!(read_all(&e, "bk1", "mp2").await.len(), MIN_PART_SIZE as usize + 10);
    e.delete_objects("bk1", &["a".into(), "mp".into(), "mp2".into()]).await.unwrap();
    e.abort_multipart("bk1", "pending", &id).await.unwrap();
    assert_eq!(blob_count(&e, &d).await, 0);
    let refs = e.blocking(|db| Ok(db.begin_read()?.open_table(BLOB_REFS)?.len()?)).await.unwrap();
    assert_eq!(refs, 0, "reference index is empty once everything is deleted");
}

/// PERF-008: a CRC32C checksum reuses the internal CRC and is still correct.
#[tokio::test]
async fn crc32c_checksum_reuses_internal_crc() {
    let (e, _d) = engine().await;
    e.create_bucket("bk1", false).await.unwrap();
    let data = big(300_000, 9);
    let mut h = crate::checksum::ChecksumHasher::new(ChecksumAlgo::Crc32c);
    h.update(&data);
    let want = h.finish();
    let opts = PutOptions { expect: PutExpect { checksum: Some(want.clone()), ..Default::default() }, ..Default::default() };
    let mut src = BytesSource(Some(Bytes::copy_from_slice(&data)));
    let info = e.put_object("bk1", "k", &mut src, opts).await.unwrap();
    assert_eq!(info.checksum, Some(want.clone()));
    let bad = PutOptions { expect: PutExpect { checksum: Some(Checksum { value: "AAAAAA==".into(), ..want }), ..Default::default() }, ..Default::default() };
    let mut src = BytesSource(Some(Bytes::copy_from_slice(&data)));
    assert_eq!(e.put_object("bk1", "k2", &mut src, bad).await.unwrap_err().code, ErrorCode::BadDigest);
}

/// PERF-007: streaming works with a read budget far smaller than one chunk.
#[tokio::test]
async fn tiny_read_budget() {
    let d = tmpdir();
    let e = LocalEngine::open_with(&d, EngineOptions { fsync: false, read_buffer: 1, ..Default::default() }).unwrap();
    e.create_bucket("bk1", false).await.unwrap();
    let data = big(2_000_000, 5);
    put(&e, "bk1", "k", &data).await;
    let (a, b) = tokio::join!(read_all(&e, "bk1", "k"), read_all(&e, "bk1", "k"));
    assert_eq!(a, data);
    assert_eq!(b, data);
}

/// PERF-004: scrub pages through everything and clears its progress when done.
#[tokio::test]
async fn scrub_pages() {
    let (e, _d) = engine().await;
    e.create_bucket("bk1", false).await.unwrap();
    for i in 0..300 {
        put(&e, "bk1", &format!("k{i:03}"), b"data").await;
    }
    let id = e.create_multipart("bk1", "up", ObjectMetadata::default(), None).await.unwrap();
    let mut src = BytesSource(Some(Bytes::from_static(b"part")));
    e.upload_part("bk1", "up", &id, 1, &mut src, PutExpect::default()).await.unwrap();
    let r = e.scrub(Some(100 << 20)).await.unwrap();
    assert_eq!(r.blobs, 301);
    assert!(r.problems.is_empty());
    assert!(!e.scrub_in_progress().await.unwrap());
}

/// PERF-006: a tiny commit queue and a batching window still complete every write.
#[tokio::test]
async fn bounded_commit_queue() {
    let d = tmpdir();
    let opts = EngineOptions { fsync: false, commit_queue: 2, commit_window: Duration::from_micros(300), ..Default::default() };
    let e = Arc::new(LocalEngine::open_with(&d, opts).unwrap());
    e.create_bucket("bk1", false).await.unwrap();
    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..64 {
        let e = e.clone();
        tasks.spawn(async move { put(&e, "bk1", &format!("k{i}"), b"x").await });
    }
    while let Some(r) = tasks.join_next().await {
        r.unwrap();
    }
    let r = e.list_objects("bk1", ListQuery { max_keys: 1000, ..Default::default() }).await.unwrap();
    assert_eq!(r.objects.len(), 64);
}

/// Databases from before schema 2 (inline multipart segments, no reference index)
/// are indexed on open and stay readable.
#[tokio::test]
async fn upgrades_schema_1() {
    let d = tmpdir();
    let blob = BlobRef { id: "abcd0000000000000000000000000001".into(), layout: ErasureLayout::default() };
    let path = d.join("blobs/ab/cd").join(&blob.id);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, b"legacy").unwrap();
    {
        let db = Database::create(d.join("meta.redb")).unwrap();
        let txn = db.begin_write().unwrap();
        txn.open_table(BUCKETS).unwrap().insert("bk1", enc(&BucketInfo { name: "bk1".into(), created: now(), public_read: false, cors: None }).as_slice()).unwrap();
        let rec = serde_json::json!({
            "size": 6, "etag": "x-1", "mtime": now(), "multipart": true,
            "segments": [{ "blob": blob, "size": 6 }]
        });
        txn.open_table(OBJECTS).unwrap().insert(("bk1", "old"), serde_json::to_vec(&rec).unwrap().as_slice()).unwrap();
        txn.commit().unwrap();
    }
    let e = LocalEngine::open(&d, false).unwrap();
    assert_eq!(read_all(&e, "bk1", "old").await, b"legacy");
    assert_eq!(e.head_object("bk1", "old").await.unwrap().parts_count, 1);
    assert_eq!(e.gc(Duration::ZERO).await.unwrap(), 0, "legacy blob is indexed as referenced");
    e.delete_object("bk1", "old").await.unwrap();
    assert_eq!(blob_count(&e, &d).await, 0);
}
