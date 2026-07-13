use std::path::PathBuf;

use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;

use crate::engine::FileEngine;
use crate::error::Result;
use crate::handle::Handle;

#[derive(Debug, Clone)]
pub enum FileKind {
    File,
    Directory,
    Symlink,
    Other,
}

#[derive(Debug, Clone)]
pub struct FileInfo {
    pub path: PathBuf,
    pub kind: FileKind,
    pub size: u64,
    #[cfg(feature = "checksum")]
    pub hash: Option<String>,
}

pub struct AnalyzeBuilder {
    path: PathBuf,
    recursive: bool,
    #[cfg(feature = "checksum")]
    with_hash: bool,
    cancel_token: Option<CancellationToken>,
}

impl AnalyzeBuilder {
    pub(crate) fn new(path: PathBuf) -> Self {
        Self {
            path,
            recursive: true,
            #[cfg(feature = "checksum")]
            with_hash: false,
            cancel_token: None,
        }
    }

    pub fn recursive(mut self, enabled: bool) -> Self {
        self.recursive = enabled;
        self
    }

    pub fn cancellation_token(mut self, token: CancellationToken) -> Self {
        self.cancel_token = Some(token);
        self
    }

    pub fn start(self) -> Result<Handle<Vec<FileInfo>>> {
        let cancel_token = self.cancel_token.unwrap_or_default();
        let (_progress_tx, progress_rx) = tokio::sync::mpsc::unbounded_channel();

        let task_cancel_token = cancel_token.clone();
        let join = tokio::spawn(async move {
            let _ = &task_cancel_token;
            // TODO(implementation): walk `self.path` (honoring `recursive`
            // and, when the `checksum` feature is enabled, `with_hash`),
            // reporting progress via `_progress_tx`.
            let _ = self.path;
            let _ = self.recursive;
            Ok(Vec::new())
        });

        Ok(Handle {
            join,
            progress_rx: UnboundedReceiverStream::new(progress_rx),
            cancel_token,
        })
    }
}

#[cfg(feature = "checksum")]
impl AnalyzeBuilder {
    pub fn with_hash(mut self, enabled: bool) -> Self {
        self.with_hash = enabled;
        self
    }
}

#[cfg(feature = "analyze")]
impl FileEngine {
    pub fn analyze(&self, path: impl Into<PathBuf>) -> AnalyzeBuilder {
        AnalyzeBuilder::new(path.into())
    }
}
