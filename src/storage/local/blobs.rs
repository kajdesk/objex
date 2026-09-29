//! Blob files: `<root>/blobs/ab/cd/<id>`, written via `<root>/tmp`.

use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::io::AsyncWriteExt;

use super::super::*;
use crate::util::random_hex;

/// Flush a file or directory to the storage device.
///
/// On Apple platforms this is a plain `fsync()` rather than `F_FULLFSYNC` (which is
/// what `File::sync_all` issues there, and which flushes the whole drive cache and
/// serializes across the machine). Durability comes from the metadata commit that
/// always follows: redb syncs with `F_FULLFSYNC`, which also flushes everything
/// written to the drive before it.
pub(super) fn sync_path_or_file(f: &std::fs::File) -> io::Result<()> {
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

pub struct FileBlobStore {
    root: PathBuf,
    fsync: bool,
    /// Shard directories whose entry in their parent is known to be durable.
    durable_dirs: Arc<Mutex<HashSet<PathBuf>>>,
}

/// How [`FileBlobStore::place`] puts a file in position.
#[derive(Clone, Copy, PartialEq)]
enum Placement {
    Rename,
    Link,
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

    pub(super) fn path(&self, id: &str) -> PathBuf {
        self.root.join("blobs").join(&id[0..2]).join(&id[2..4]).join(id)
    }

    pub(super) fn blobs_dir(&self) -> PathBuf {
        self.root.join("blobs")
    }

    /// Move or link each `(from, id)` into place. With fsync, every shard directory
    /// involved, and the new entries, are durable on return; each directory is
    /// synced once however many entries it gained. All or nothing: on error, entries
    /// already placed are removed (links) or moved back (renames).
    async fn place(&self, items: Vec<(PathBuf, String)>, how: Placement) -> io::Result<()> {
        let dsts: Vec<PathBuf> = items.iter().map(|(_, id)| self.path(id)).collect();
        let (blobs, fsync, durable) = (self.blobs_dir(), self.fsync, self.durable_dirs.clone());
        tokio::task::spawn_blocking(move || {
            let mut done: Vec<usize> = Vec::with_capacity(items.len());
            let mut leaves: HashSet<PathBuf> = HashSet::new();
            let result = (|| {
                for (i, ((from, _), dst)) in items.iter().zip(&dsts).enumerate() {
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
                        // Sync the parent even when another writer created the
                        // directory: it may not have finished syncing it yet.
                        if fsync {
                            sync_dir(parent)?;
                        }
                        durable.lock().unwrap().insert(dir.clone());
                    }
                    match how {
                        Placement::Rename => std::fs::rename(from, dst)?,
                        Placement::Link => {
                            if std::fs::hard_link(from, dst).is_err() {
                                std::fs::copy(from, dst)?;
                                if fsync {
                                    sync_path_or_file(&std::fs::File::open(dst)?)?;
                                }
                            }
                        }
                    }
                    done.push(i);
                    leaves.insert(leaf);
                }
                if fsync {
                    for leaf in &leaves {
                        sync_dir(leaf)?;
                    }
                }
                Ok(())
            })();
            if result.is_err() {
                for i in done {
                    let _ = match how {
                        Placement::Link => std::fs::remove_file(&dsts[i]),
                        Placement::Rename => std::fs::rename(&dsts[i], &items[i].0),
                    };
                }
            }
            result
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
            Ok(digest) => self.place(vec![(tmp.clone(), id.clone())], Placement::Rename).await.map(|_| digest).map_err(S3Error::from),
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
        Ok(Box::new(tokio::fs::File::open(self.path(&blob.id)).await?))
    }

    async fn delete(&self, blob: &BlobRef) -> io::Result<()> {
        match tokio::fs::remove_file(self.path(&blob.id)).await {
            Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
            _ => Ok(()),
        }
    }

    async fn duplicate(&self, blob: &BlobRef) -> io::Result<BlobRef> {
        Ok(self.duplicate_many(std::slice::from_ref(blob)).await?.remove(0))
    }

    async fn duplicate_many(&self, blobs: &[BlobRef]) -> io::Result<Vec<BlobRef>> {
        let out: Vec<BlobRef> = blobs.iter().map(|b| BlobRef { id: random_hex(16), layout: b.layout }).collect();
        let items = blobs.iter().zip(&out).map(|(src, dst)| (self.path(&src.id), dst.id.clone())).collect();
        self.place(items, Placement::Link).await?;
        Ok(out)
    }
}

/// Blob ids in one leaf directory (`blobs/ab/cd`) last changed before `cutoff`.
pub(super) fn leaf_candidates(leaf: &Path, cutoff: std::time::SystemTime) -> io::Result<Vec<String>> {
    let mut out = Vec::new();
    for f in std::fs::read_dir(leaf)? {
        let f = f?;
        if changed(&f.metadata()?) < cutoff {
            out.push(f.file_name().to_string_lossy().into_owned());
        }
    }
    Ok(out)
}

/// All leaf directories under `blobs/`, in order.
pub(super) fn leaf_dirs(blobs: &Path) -> io::Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    let mut mids: Vec<PathBuf> = std::fs::read_dir(blobs)?.map(|e| e.map(|e| e.path())).collect::<io::Result<_>>()?;
    mids.sort();
    for m in mids {
        let mut leaves: Vec<PathBuf> = std::fs::read_dir(&m)?.map(|e| e.map(|e| e.path())).collect::<io::Result<_>>()?;
        leaves.sort();
        out.extend(leaves);
    }
    Ok(out)
}

/// Last inode change. On unix this is ctime, which hard-linking and renaming update,
/// so fresh copies are never mistaken for old garbage.
fn changed(md: &std::fs::Metadata) -> std::time::SystemTime {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        std::time::SystemTime::UNIX_EPOCH
            + std::time::Duration::new(md.ctime().max(0) as u64, md.ctime_nsec().clamp(0, 999_999_999) as u32)
    }
    #[cfg(not(unix))]
    {
        md.modified().unwrap_or(std::time::SystemTime::now())
    }
}
