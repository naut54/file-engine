use std::io::Read;
use std::path::{Path, PathBuf};

use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;

use crate::engine::FileEngine;
use crate::error::{FileEngineError, Result};
use crate::handle::Handle;
use crate::progress::Progress;

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
    /// MIME type sniffed from the first bytes of the file's content, via
    /// `infer`. `None` for directories, symlinks, and files whose content
    /// doesn't match any known signature.
    pub content_type: Option<&'static str>,
    #[cfg(feature = "checksum")]
    pub hash: Option<String>,
}

pub struct AnalyzeBuilder {
    path: PathBuf,
    recursive: bool,
    follow_symlinks: bool,
    #[cfg(feature = "checksum")]
    with_hash: bool,
    cancel_token: Option<CancellationToken>,
}

impl AnalyzeBuilder {
    pub(crate) fn new(path: PathBuf, follow_symlinks: bool) -> Self {
        Self {
            path,
            recursive: true,
            follow_symlinks,
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
        let (progress_tx, progress_rx) = tokio::sync::mpsc::unbounded_channel();

        let task_cancel_token = cancel_token.clone();
        let path = self.path;
        let recursive = self.recursive;
        let follow_symlinks = self.follow_symlinks;
        #[cfg(feature = "checksum")]
        let with_hash = self.with_hash;

        let join = tokio::task::spawn_blocking(move || {
            let mut results = Vec::new();

            for (index, entry) in walk_blocking(&path, recursive, follow_symlinks).enumerate() {
                if task_cancel_token.is_cancelled() {
                    return Err(FileEngineError::Cancelled);
                }

                let entry = entry.map_err(walkdir_error)?;
                let metadata = entry.metadata().map_err(walkdir_error)?;

                let kind = if metadata.file_type().is_symlink() {
                    FileKind::Symlink
                } else if metadata.is_dir() {
                    FileKind::Directory
                } else if metadata.is_file() {
                    FileKind::File
                } else {
                    FileKind::Other
                };

                let size = metadata.len();
                let is_regular_file = matches!(kind, FileKind::File);

                let content_type = if is_regular_file {
                    sniff_content_type(entry.path())
                } else {
                    None
                };

                #[cfg(feature = "checksum")]
                let hash = if with_hash && is_regular_file {
                    Some(hash_file(entry.path())?)
                } else {
                    None
                };

                let files_done = index as u64 + 1;
                let _ = progress_tx.send(Progress {
                    bytes_done: files_done,
                    bytes_total: 0,
                    files_done,
                    files_total: 0,
                    current_file: Some(entry.path().to_path_buf()),
                });

                results.push(FileInfo {
                    path: entry.into_path(),
                    kind,
                    size,
                    content_type,
                    #[cfg(feature = "checksum")]
                    hash,
                });
            }

            Ok(results)
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

/// Shared walk configuration, also used by `sync.rs` (legal: `sync`
/// requires `analyze` at the manifest level, §4 of the design doc) to diff
/// two directory trees with the exact same traversal rules `analyze` uses.
pub(crate) fn walk_blocking(
    path: &Path,
    recursive: bool,
    follow_symlinks: bool,
) -> impl Iterator<Item = walkdir::Result<walkdir::DirEntry>> {
    let mut walker = walkdir::WalkDir::new(path).follow_links(follow_symlinks);
    if !recursive {
        walker = walker.max_depth(1);
    }
    walker.into_iter()
}

pub(crate) fn walkdir_error(err: walkdir::Error) -> FileEngineError {
    let path = err.path().map(Path::to_path_buf).unwrap_or_default();
    match err.into_io_error() {
        Some(io_err) => crate::error::from_io(path, io_err),
        None => FileEngineError::Io {
            path,
            source: std::io::Error::other("walk error with no underlying io::Error"),
        },
    }
}

fn sniff_content_type(path: &Path) -> Option<&'static str> {
    let mut file = std::fs::File::open(path).ok()?;
    let mut buf = [0u8; 8192];
    let n = file.read(&mut buf).ok()?;
    infer::get(&buf[..n]).map(|kind| kind.mime_type())
}

/// `pub(crate)` so `sync.rs`'s `compare_by_hash` (§8.5) can reuse it —
/// legal since that method only exists under `#[cfg(all(feature = "sync",
/// feature = "checksum"))]`, i.e. only when this is also compiled.
#[cfg(feature = "checksum")]
pub(crate) fn hash_file(path: &Path) -> Result<String> {
    let mut file =
        std::fs::File::open(path).map_err(|e| crate::error::from_io(path.to_path_buf(), e))?;
    let mut hasher = blake3::Hasher::new();
    hasher
        .update_reader(&mut file)
        .map_err(|e| crate::error::from_io(path.to_path_buf(), e))?;
    Ok(hasher.finalize().to_hex().to_string())
}

#[cfg(feature = "analyze")]
impl FileEngine {
    pub fn analyze(&self, path: impl Into<PathBuf>) -> AnalyzeBuilder {
        AnalyzeBuilder::new(path.into(), self.options().follow_symlinks)
    }
}
