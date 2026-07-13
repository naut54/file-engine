use std::path::PathBuf;

use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;

use crate::engine::FileEngine;
use crate::error::Result;
use crate::handle::Handle;

/// `sync` is a one-directional mirror: it makes `dst` look like `src`, not
/// a bidirectional reconciliation. See design doc §8.5.
#[derive(Debug, Clone, Copy, Default)]
pub struct SyncSummary {
    pub copied: u64,
    pub updated: u64,
    pub deleted: u64,
    pub skipped: u64,
}

pub struct SyncBuilder {
    src: PathBuf,
    dst: PathBuf,
    delete_extraneous: bool,
    #[cfg(feature = "checksum")]
    compare_by_hash: bool,
    cancel_token: Option<CancellationToken>,
}

impl SyncBuilder {
    pub(crate) fn new(src: PathBuf, dst: PathBuf) -> Self {
        Self {
            src,
            dst,
            delete_extraneous: false,
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
        let (_progress_tx, progress_rx) = tokio::sync::mpsc::unbounded_channel();

        let task_cancel_token = cancel_token.clone();
        let join = tokio::spawn(async move {
            let _ = &task_cancel_token;
            // TODO(implementation): walk `self.src` and `self.dst` (reusing
            // the `analyze` walk, §8.5), diff by size+mtime — or by hash
            // when `compare_by_hash` is set — copy new/changed files,
            // remove extraneous ones when `self.delete_extraneous`, and
            // accumulate the result into `SyncSummary`, reporting progress
            // via `_progress_tx` and observing `task_cancel_token`.
            let _ = (self.src, self.dst, self.delete_extraneous);
            #[cfg(feature = "checksum")]
            let _ = self.compare_by_hash;
            Ok(SyncSummary::default())
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
        SyncBuilder::new(src.into(), dst.into())
    }
}
