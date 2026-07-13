use std::path::{Path, PathBuf};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc::UnboundedSender;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;

use crate::error::{from_io, FileEngineError, Result};
use crate::handle::Handle;
use crate::progress::Progress;

pub struct CopyBuilder {
    src: PathBuf,
    dst: PathBuf,
    overwrite: bool,
    buffer_size: usize,
    follow_symlinks: bool,
    #[cfg(feature = "permissions")]
    pub(crate) preserve_permissions: bool,
    cancel_token: Option<CancellationToken>,
}

impl CopyBuilder {
    pub(crate) fn new(
        src: PathBuf,
        dst: PathBuf,
        buffer_size: usize,
        follow_symlinks: bool,
    ) -> Self {
        Self {
            src,
            dst,
            overwrite: false,
            buffer_size,
            follow_symlinks,
            #[cfg(feature = "permissions")]
            preserve_permissions: false,
            cancel_token: None,
        }
    }

    pub fn overwrite(mut self, enabled: bool) -> Self {
        self.overwrite = enabled;
        self
    }

    pub fn cancellation_token(mut self, token: CancellationToken) -> Self {
        self.cancel_token = Some(token);
        self
    }

    pub fn start(self) -> Result<Handle<()>> {
        let cancel_token = self.cancel_token.unwrap_or_default();
        let (progress_tx, progress_rx) = tokio::sync::mpsc::unbounded_channel();

        let task_cancel_token = cancel_token.clone();
        let buffer_size = self.buffer_size;
        let follow_symlinks = self.follow_symlinks;
        let overwrite = self.overwrite;
        let src = self.src;
        let dst = self.dst;
        #[cfg(feature = "permissions")]
        let preserve_permissions = self.preserve_permissions;

        let join = tokio::spawn(async move {
            if !overwrite && tokio::fs::try_exists(&dst).await.unwrap_or(false) {
                return Err(FileEngineError::DestinationExists(dst));
            }

            let src_metadata = tokio::fs::symlink_metadata(&src)
                .await
                .map_err(|e| from_io(src.clone(), e))?;

            if src_metadata.is_dir() {
                copy_dir(
                    &src,
                    &dst,
                    buffer_size,
                    follow_symlinks,
                    &progress_tx,
                    &task_cancel_token,
                )
                .await?;
            } else if src_metadata.is_symlink() && !follow_symlinks {
                copy_symlink(&src, &dst).await?;
            } else {
                copy_file(
                    &src,
                    &dst,
                    buffer_size,
                    0,
                    1,
                    &progress_tx,
                    &task_cancel_token,
                )
                .await?;
            }

            #[cfg(feature = "permissions")]
            if preserve_permissions {
                preserve_permissions_recursive(&src, &dst).await?;
            }

            Ok(())
        });

        Ok(Handle {
            join,
            progress_rx: UnboundedReceiverStream::new(progress_rx),
            cancel_token,
        })
    }
}

/// Copies a single regular file `src` -> `dst` in `buffer_size` chunks,
/// emitting `Progress` after each chunk and checking `cancel_token` before
/// each read. `files_done`/`files_total` are pass-through context for
/// `Progress` — this function only knows about the one file, the caller
/// (a directory walk, or `sync`) knows where it sits in a larger set.
///
/// Preserves `src`'s mtime on `dst` after writing (best-effort — a failure
/// to set it is not a copy failure). This matters beyond cosmetics:
/// `sync`'s default change-detection (§8.5) compares size+mtime, which
/// would consider every file "changed" on every run if a plain copy always
/// stamped `dst` with the current time instead of carrying `src`'s mtime
/// forward.
///
/// `pub(crate)` so `move_op.rs` (cross-device fallback) and `sync.rs`
/// (copying new/changed files) can reuse it — both features require
/// `operations` at the manifest level (§4 of the design doc).
pub(crate) async fn copy_file(
    src: &Path,
    dst: &Path,
    buffer_size: usize,
    files_done: u64,
    files_total: u64,
    progress_tx: &UnboundedSender<Progress>,
    cancel_token: &CancellationToken,
) -> Result<()> {
    let src_metadata = tokio::fs::metadata(src)
        .await
        .map_err(|e| from_io(src.to_path_buf(), e))?;
    let bytes_total = src_metadata.len();

    if let Some(parent) = dst.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| from_io(parent.to_path_buf(), e))?;
    }

    let mut reader = tokio::fs::File::open(src)
        .await
        .map_err(|e| from_io(src.to_path_buf(), e))?;
    let mut writer = tokio::fs::File::create(dst)
        .await
        .map_err(|e| from_io(dst.to_path_buf(), e))?;

    let mut buf = vec![0u8; buffer_size.max(1)];
    let mut bytes_done = 0u64;

    loop {
        if cancel_token.is_cancelled() {
            return Err(FileEngineError::Cancelled);
        }

        let n = reader
            .read(&mut buf)
            .await
            .map_err(|e| from_io(src.to_path_buf(), e))?;
        if n == 0 {
            break;
        }

        writer
            .write_all(&buf[..n])
            .await
            .map_err(|e| from_io(dst.to_path_buf(), e))?;

        bytes_done += n as u64;
        let _ = progress_tx.send(Progress {
            bytes_done,
            bytes_total,
            files_done,
            files_total,
            current_file: Some(src.to_path_buf()),
        });
    }

    writer
        .flush()
        .await
        .map_err(|e| from_io(dst.to_path_buf(), e))?;
    drop(writer);

    if let Ok(modified) = src_metadata.modified() {
        let dst_owned = dst.to_path_buf();
        let _ = tokio::task::spawn_blocking(move || {
            std::fs::OpenOptions::new()
                .write(true)
                .open(&dst_owned)
                .and_then(|f| f.set_modified(modified))
        })
        .await;
    }

    Ok(())
}

/// Recreates a symlink at `dst` pointing to the same target `src` points
/// to, rather than copying through to the target's contents. Only called
/// when `follow_symlinks` is false.
pub(crate) async fn copy_symlink(src: &Path, dst: &Path) -> Result<()> {
    let target = tokio::fs::read_link(src)
        .await
        .map_err(|e| from_io(src.to_path_buf(), e))?;

    #[cfg(unix)]
    {
        tokio::fs::symlink(&target, dst)
            .await
            .map_err(|e| from_io(dst.to_path_buf(), e))?;
    }
    #[cfg(windows)]
    {
        let target_abs = src.parent().unwrap_or_else(|| Path::new(".")).join(&target);
        let target_is_dir = tokio::fs::metadata(&target_abs)
            .await
            .map(|m| m.is_dir())
            .unwrap_or(false);
        let result = if target_is_dir {
            tokio::fs::symlink_dir(&target, dst).await
        } else {
            tokio::fs::symlink_file(&target, dst).await
        };
        result.map_err(|e| from_io(dst.to_path_buf(), e))?;
    }

    Ok(())
}

/// Walks `src` (pre-pass to compute `files_total`, then a copy pass),
/// recreating its structure at `dst`. Iterative (an explicit stack of
/// relative directories), not recursive `async fn` calls, since Rust's
/// async fns can't recurse without boxing.
pub(crate) async fn copy_dir(
    src: &Path,
    dst: &Path,
    buffer_size: usize,
    follow_symlinks: bool,
    progress_tx: &UnboundedSender<Progress>,
    cancel_token: &CancellationToken,
) -> Result<()> {
    let mut entries: Vec<(PathBuf, bool)> = Vec::new();
    let mut dirs = vec![PathBuf::new()];

    while let Some(rel_dir) = dirs.pop() {
        let abs_dir = src.join(&rel_dir);
        let mut read_dir = tokio::fs::read_dir(&abs_dir)
            .await
            .map_err(|e| from_io(abs_dir.clone(), e))?;

        while let Some(entry) = read_dir
            .next_entry()
            .await
            .map_err(|e| from_io(abs_dir.clone(), e))?
        {
            let rel_path = rel_dir.join(entry.file_name());
            let file_type = entry
                .file_type()
                .await
                .map_err(|e| from_io(entry.path(), e))?;

            if file_type.is_dir() {
                dirs.push(rel_path);
            } else if file_type.is_symlink() && !follow_symlinks {
                entries.push((rel_path, true));
            } else {
                entries.push((rel_path, false));
            }
        }
    }

    let files_total = entries.len() as u64;
    tokio::fs::create_dir_all(dst)
        .await
        .map_err(|e| from_io(dst.to_path_buf(), e))?;

    for (files_done, (rel_path, is_symlink)) in entries.into_iter().enumerate() {
        if cancel_token.is_cancelled() {
            return Err(FileEngineError::Cancelled);
        }

        let entry_src = src.join(&rel_path);
        let entry_dst = dst.join(&rel_path);

        if is_symlink {
            if let Some(parent) = entry_dst.parent() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .map_err(|e| from_io(parent.to_path_buf(), e))?;
            }
            copy_symlink(&entry_src, &entry_dst).await?;
        } else {
            copy_file(
                &entry_src,
                &entry_dst,
                buffer_size,
                files_done as u64,
                files_total,
                progress_tx,
                cancel_token,
            )
            .await?;
        }
    }

    Ok(())
}

#[cfg(feature = "permissions")]
pub(crate) async fn preserve_permissions_recursive(src: &Path, dst: &Path) -> Result<()> {
    let src_metadata = tokio::fs::symlink_metadata(src)
        .await
        .map_err(|e| from_io(src.to_path_buf(), e))?;

    if !src_metadata.is_dir() {
        let perms = tokio::fs::metadata(src)
            .await
            .map_err(|e| from_io(src.to_path_buf(), e))?
            .permissions();
        return tokio::fs::set_permissions(dst, perms)
            .await
            .map_err(|e| from_io(dst.to_path_buf(), e));
    }

    let mut dirs = vec![PathBuf::new()];
    while let Some(rel_dir) = dirs.pop() {
        let abs_src_dir = src.join(&rel_dir);
        let abs_dst_dir = dst.join(&rel_dir);

        let perms = tokio::fs::metadata(&abs_src_dir)
            .await
            .map_err(|e| from_io(abs_src_dir.clone(), e))?
            .permissions();
        tokio::fs::set_permissions(&abs_dst_dir, perms)
            .await
            .map_err(|e| from_io(abs_dst_dir.clone(), e))?;

        let mut read_dir = tokio::fs::read_dir(&abs_src_dir)
            .await
            .map_err(|e| from_io(abs_src_dir.clone(), e))?;
        while let Some(entry) = read_dir
            .next_entry()
            .await
            .map_err(|e| from_io(abs_src_dir.clone(), e))?
        {
            let rel_path = rel_dir.join(entry.file_name());
            let file_type = entry
                .file_type()
                .await
                .map_err(|e| from_io(entry.path(), e))?;

            if file_type.is_dir() {
                dirs.push(rel_path);
            } else if file_type.is_file() {
                let entry_src = src.join(&rel_path);
                let entry_dst = dst.join(&rel_path);
                let perms = tokio::fs::metadata(&entry_src)
                    .await
                    .map_err(|e| from_io(entry_src.clone(), e))?
                    .permissions();
                tokio::fs::set_permissions(&entry_dst, perms)
                    .await
                    .map_err(|e| from_io(entry_dst.clone(), e))?;
            }
        }
    }

    Ok(())
}
