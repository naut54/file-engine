use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;

use crate::analyze::{walk_blocking, walkdir_error};
use crate::engine::FileEngine;
use crate::error::{from_io, FileEngineError, Result};
use crate::handle::Handle;
use crate::operations::copy::copy_file;

/// `sync` is a one-directional mirror: it makes `dst` look like `src`, not
/// a bidirectional reconciliation. See design doc §8.5.
#[derive(Debug, Clone, Copy, Default)]
pub struct SyncSummary {
    pub copied: u64,
    pub updated: u64,
    pub deleted: u64,
    pub skipped: u64,
}

struct EntryMeta {
    is_dir: bool,
    size: u64,
    modified: Option<SystemTime>,
}

/// Walks `root` using the same traversal `analyze` uses. `tolerate_missing`
/// is set for `dst`, which legitimately may not exist yet on a first sync —
/// `src` missing is a real error and is left to propagate naturally (a
/// nonexistent `WalkDir` root surfaces as `SourceNotFound` via
/// `walkdir_error`/`from_io`).
fn walk_entries(
    root: &Path,
    follow_symlinks: bool,
    tolerate_missing: bool,
) -> Result<HashMap<PathBuf, EntryMeta>> {
    if tolerate_missing && !root.exists() {
        return Ok(HashMap::new());
    }

    let mut map = HashMap::new();
    for entry in walk_blocking(root, true, follow_symlinks) {
        let entry = entry.map_err(walkdir_error)?;
        if entry.path() == root {
            continue;
        }

        let metadata = entry.metadata().map_err(walkdir_error)?;
        let rel = entry
            .path()
            .strip_prefix(root)
            .unwrap_or(entry.path())
            .to_path_buf();

        map.insert(
            rel,
            EntryMeta {
                is_dir: metadata.is_dir(),
                size: metadata.len(),
                modified: metadata.modified().ok(),
            },
        );
    }

    Ok(map)
}

#[cfg(feature = "checksum")]
async fn hashes_differ(src_path: &Path, dst_path: &Path) -> Result<bool> {
    let src_path = src_path.to_path_buf();
    let dst_path = dst_path.to_path_buf();
    tokio::task::spawn_blocking(move || -> Result<bool> {
        let src_hash = crate::analyze::hash_file(&src_path)?;
        let dst_hash = crate::analyze::hash_file(&dst_path)?;
        Ok(src_hash != dst_hash)
    })
    .await
    .map_err(|e| FileEngineError::Io {
        path: PathBuf::new(),
        source: std::io::Error::other(e),
    })?
}

pub struct SyncBuilder {
    src: PathBuf,
    dst: PathBuf,
    delete_extraneous: bool,
    buffer_size: usize,
    follow_symlinks: bool,
    #[cfg(feature = "checksum")]
    compare_by_hash: bool,
    cancel_token: Option<CancellationToken>,
}

impl SyncBuilder {
    pub(crate) fn new(
        src: PathBuf,
        dst: PathBuf,
        buffer_size: usize,
        follow_symlinks: bool,
    ) -> Self {
        Self {
            src,
            dst,
            delete_extraneous: false,
            buffer_size,
            follow_symlinks,
            #[cfg(feature = "checksum")]
            compare_by_hash: false,
            cancel_token: None,
        }
    }

    /// Default: false. When true, files present in `dst` but not in `src`
    /// are removed — full mirror rather than additive-only sync.
    pub fn delete_extraneous(mut self, enabled: bool) -> Self {
        self.delete_extraneous = enabled;
        self
    }

    pub fn cancellation_token(mut self, token: CancellationToken) -> Self {
        self.cancel_token = Some(token);
        self
    }

    pub fn start(self) -> Result<Handle<SyncSummary>> {
        let cancel_token = self.cancel_token.unwrap_or_default();
        let (progress_tx, progress_rx) = tokio::sync::mpsc::unbounded_channel();

        let task_cancel_token = cancel_token.clone();
        let src = self.src;
        let dst = self.dst;
        let delete_extraneous = self.delete_extraneous;
        let buffer_size = self.buffer_size;
        let follow_symlinks = self.follow_symlinks;
        #[cfg(feature = "checksum")]
        let compare_by_hash = self.compare_by_hash;

        let join = tokio::spawn(async move {
            tokio::fs::create_dir_all(&dst)
                .await
                .map_err(|e| from_io(dst.clone(), e))?;

            let (src_for_walk, dst_for_walk) = (src.clone(), dst.clone());
            let (src_map, dst_map) = tokio::task::spawn_blocking(move || {
                let src_map = walk_entries(&src_for_walk, follow_symlinks, false);
                let dst_map = walk_entries(&dst_for_walk, follow_symlinks, true);
                (src_map, dst_map)
            })
            .await
            .map_err(|e| FileEngineError::Io {
                path: PathBuf::new(),
                source: std::io::Error::other(e),
            })?;
            let src_map = src_map?;
            let dst_map = dst_map?;

            let mut summary = SyncSummary::default();
            let files_total = src_map.len() as u64;
            let mut files_done = 0u64;

            for (rel, src_meta) in &src_map {
                if task_cancel_token.is_cancelled() {
                    return Err(FileEngineError::Cancelled);
                }

                let src_path = src.join(rel);
                let dst_path = dst.join(rel);

                if src_meta.is_dir {
                    tokio::fs::create_dir_all(&dst_path)
                        .await
                        .map_err(|e| from_io(dst_path.clone(), e))?;
                    continue;
                }

                let existing = dst_map.get(rel);
                let needs_copy = match existing {
                    None => true,
                    Some(dst_meta) => {
                        #[cfg(feature = "checksum")]
                        {
                            if compare_by_hash {
                                hashes_differ(&src_path, &dst_path).await?
                            } else {
                                src_meta.size != dst_meta.size
                                    || src_meta.modified != dst_meta.modified
                            }
                        }
                        #[cfg(not(feature = "checksum"))]
                        {
                            src_meta.size != dst_meta.size || src_meta.modified != dst_meta.modified
                        }
                    }
                };

                if needs_copy {
                    copy_file(
                        &src_path,
                        &dst_path,
                        buffer_size,
                        files_done,
                        files_total,
                        &progress_tx,
                        &task_cancel_token,
                    )
                    .await?;
                    if existing.is_some() {
                        summary.updated += 1;
                    } else {
                        summary.copied += 1;
                    }
                } else {
                    summary.skipped += 1;
                }

                files_done += 1;
            }

            if delete_extraneous {
                let mut extraneous: Vec<&PathBuf> = dst_map
                    .keys()
                    .filter(|rel| !src_map.contains_key(*rel))
                    .collect();
                extraneous.sort_by_key(|rel| rel.components().count());

                let mut removed_dirs: Vec<PathBuf> = Vec::new();
                for rel in extraneous {
                    if task_cancel_token.is_cancelled() {
                        return Err(FileEngineError::Cancelled);
                    }
                    if removed_dirs.iter().any(|d| rel.starts_with(d)) {
                        continue;
                    }

                    let dst_path = dst.join(rel);
                    let meta = &dst_map[rel];
                    if meta.is_dir {
                        tokio::fs::remove_dir_all(&dst_path)
                            .await
                            .map_err(|e| from_io(dst_path.clone(), e))?;
                        removed_dirs.push(rel.clone());
                    } else {
                        tokio::fs::remove_file(&dst_path)
                            .await
                            .map_err(|e| from_io(dst_path.clone(), e))?;
                    }
                    summary.deleted += 1;
                }
            }

            Ok(summary)
        });

        Ok(Handle {
            join,
            progress_rx: UnboundedReceiverStream::new(progress_rx),
            cancel_token,
        })
    }
}

/// `checksum` and `sync` are independent (both depend on `analyze` rather
/// than on each other, §4) but the combination is legal — see §8.5.
#[cfg(all(feature = "sync", feature = "checksum"))]
impl SyncBuilder {
    /// Default: false (size + mtime). When true, compares file content
    /// hashes instead — more expensive, immune to mtime-only changes.
    pub fn compare_by_hash(mut self, enabled: bool) -> Self {
        self.compare_by_hash = enabled;
        self
    }
}

#[cfg(feature = "sync")]
impl FileEngine {
    pub fn sync(&self, src: impl Into<PathBuf>, dst: impl Into<PathBuf>) -> SyncBuilder {
        let options = self.options();
        SyncBuilder::new(
            src.into(),
            dst.into(),
            options.buffer_size,
            options.follow_symlinks,
        )
    }
}
