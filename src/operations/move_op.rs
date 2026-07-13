use std::path::PathBuf;

use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;

use crate::error::{from_io, FileEngineError, Result};
use crate::handle::Handle;
#[cfg(feature = "permissions")]
use crate::operations::copy::preserve_permissions_recursive;
use crate::operations::copy::{copy_dir, copy_file, copy_symlink};

pub struct MoveBuilder {
    src: PathBuf,
    dst: PathBuf,
    overwrite: bool,
    buffer_size: usize,
    follow_symlinks: bool,
    #[cfg(feature = "permissions")]
    pub(crate) preserve_permissions: bool,
    cancel_token: Option<CancellationToken>,
}

impl MoveBuilder {
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

            // Fast path: same-filesystem rename is atomic and correct for
            // both files and directories. Only fall back to copy+remove on
            // failure (e.g. a cross-device move, `EXDEV`).
            if tokio::fs::rename(&src, &dst).await.is_ok() {
                return Ok(());
            }

            let src_metadata = tokio::fs::symlink_metadata(&src)
                .await
                .map_err(|e| from_io(src.clone(), e))?;

            // Permissions must be applied (when requested) before `src` is
            // removed below — `copy_file`/`copy_dir` create `dst` with
            // default permissions, not `src`'s.
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
                #[cfg(feature = "permissions")]
                if preserve_permissions {
                    preserve_permissions_recursive(&src, &dst).await?;
                }
                tokio::fs::remove_dir_all(&src)
                    .await
                    .map_err(|e| from_io(src.clone(), e))?;
            } else if src_metadata.is_symlink() && !follow_symlinks {
                copy_symlink(&src, &dst).await?;
                tokio::fs::remove_file(&src)
                    .await
                    .map_err(|e| from_io(src.clone(), e))?;
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
                #[cfg(feature = "permissions")]
                if preserve_permissions {
                    preserve_permissions_recursive(&src, &dst).await?;
                }
                tokio::fs::remove_file(&src)
                    .await
                    .map_err(|e| from_io(src.clone(), e))?;
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
