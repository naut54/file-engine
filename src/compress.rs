use std::path::{Path, PathBuf};

use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;

use crate::engine::FileEngine;
use crate::error::{FileEngineError, Result};
use crate::handle::Handle;

/// See design doc §8.4. `Zip` handles a single file or a directory tree;
/// `Gzip` is a single compressed stream and therefore only accepts a single
/// file as source (there is no `tar` dependency to fold a directory into a
/// `.tar.gz`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressFormat {
    Zip,
    Gzip,
}

fn infer_format(path: &Path) -> Option<CompressFormat> {
    match path.extension().and_then(|ext| ext.to_str()) {
        Some("zip") => Some(CompressFormat::Zip),
        Some("gz") => Some(CompressFormat::Gzip),
        _ => None,
    }
}

pub struct CompressBuilder {
    src: PathBuf,
    dst: PathBuf,
    format: Option<CompressFormat>,
    cancel_token: Option<CancellationToken>,
}

impl CompressBuilder {
    pub(crate) fn new(src: PathBuf, dst: PathBuf) -> Self {
        Self {
            src,
            dst,
            format: None,
            cancel_token: None,
        }
    }

    /// Optional — inferred from `dst`'s extension when omitted (`.zip` →
    /// `Zip`, `.gz` → `Gzip`).
    pub fn format(mut self, format: CompressFormat) -> Self {
        self.format = Some(format);
        self
    }

    pub fn cancellation_token(mut self, token: CancellationToken) -> Self {
        self.cancel_token = Some(token);
        self
    }

    pub fn start(self) -> Result<Handle<()>> {
        let cancel_token = self.cancel_token.unwrap_or_default();
        let (_progress_tx, progress_rx) = tokio::sync::mpsc::unbounded_channel();

        let task_cancel_token = cancel_token.clone();
        let join = tokio::spawn(async move {
            let _ = &task_cancel_token;

            let format = match self.format.or_else(|| infer_format(&self.dst)) {
                Some(format) => format,
                None => return Err(FileEngineError::UnknownCompressFormat(self.dst)),
            };

            if format == CompressFormat::Gzip && self.src.is_dir() {
                return Err(FileEngineError::GzipRequiresFile(self.src));
            }

            // TODO(implementation): write the archive — `zip::ZipWriter`
            // walking `self.src` for `Zip`, `flate2::write::GzEncoder`
            // wrapping a single file read for `Gzip` — reporting progress
            // via `_progress_tx` and observing `task_cancel_token`.
            let _ = (self.src, self.dst, format);
            Ok(())
        });

        Ok(Handle {
            join,
            progress_rx: UnboundedReceiverStream::new(progress_rx),
            cancel_token,
        })
    }
}

pub struct DecompressBuilder {
    src: PathBuf,
    dst: PathBuf,
    format: Option<CompressFormat>,
    cancel_token: Option<CancellationToken>,
}

impl DecompressBuilder {
    pub(crate) fn new(src: PathBuf, dst: PathBuf) -> Self {
        Self {
            src,
            dst,
            format: None,
            cancel_token: None,
        }
    }

    /// Optional — inferred from `src`'s extension when omitted.
    pub fn format(mut self, format: CompressFormat) -> Self {
        self.format = Some(format);
        self
    }

    pub fn cancellation_token(mut self, token: CancellationToken) -> Self {
        self.cancel_token = Some(token);
        self
    }

    pub fn start(self) -> Result<Handle<()>> {
        let cancel_token = self.cancel_token.unwrap_or_default();
        let (_progress_tx, progress_rx) = tokio::sync::mpsc::unbounded_channel();

        let task_cancel_token = cancel_token.clone();
        let join = tokio::spawn(async move {
            let _ = &task_cancel_token;

            let format = match self.format.or_else(|| infer_format(&self.src)) {
                Some(format) => format,
                None => return Err(FileEngineError::UnknownCompressFormat(self.src)),
            };

            // TODO(implementation): read the archive — `zip::ZipArchive`
            // extracting into `self.dst` for `Zip`,
            // `flate2::read::GzDecoder` writing a single file for `Gzip` —
            // reporting progress via `_progress_tx` and observing
            // `task_cancel_token`.
            let _ = (self.src, self.dst, format);
            Ok(())
        });

        Ok(Handle {
            join,
            progress_rx: UnboundedReceiverStream::new(progress_rx),
            cancel_token,
        })
    }
}

#[cfg(feature = "compress")]
impl FileEngine {
    pub fn compress(&self, src: impl Into<PathBuf>, dst: impl Into<PathBuf>) -> CompressBuilder {
        CompressBuilder::new(src.into(), dst.into())
    }

    pub fn decompress(
        &self,
        src: impl Into<PathBuf>,
        dst: impl Into<PathBuf>,
    ) -> DecompressBuilder {
        DecompressBuilder::new(src.into(), dst.into())
    }
}
