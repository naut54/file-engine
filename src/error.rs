use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum FileEngineError {
    #[error("source not found: {0:?}")]
    SourceNotFound(PathBuf),

    #[error("destination already exists: {0:?}")]
    DestinationExists(PathBuf),

    #[error("operation cancelled")]
    Cancelled,

    #[error("insufficient disk space: needed {needed} bytes, available {available} bytes")]
    InsufficientSpace { needed: u64, available: u64 },

    #[error("permission denied: {0:?}")]
    PermissionDenied(PathBuf),

    #[error("io error on {path:?}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("could not infer compression format from destination: {0:?}")]
    UnknownCompressFormat(PathBuf),

    #[error("gzip compression requires a single file, got a directory: {0:?}")]
    GzipRequiresFile(PathBuf),
}

pub type Result<T> = std::result::Result<T, FileEngineError>;

/// Maps a raw `io::Error` to the closest `FileEngineError` variant, used by
/// every module that touches the filesystem directly.
///
/// `InsufficientSpace`'s `available` is always reported as `0`: there is no
/// disk-space-query dependency in this crate (out of scope for this pass),
/// so the only thing known for certain when `ErrorKind::StorageFull` occurs
/// is that the write failed, not how much space actually exists.
// Every feature that touches the filesystem uses this, but with every
// filesystem-touching feature off (`--no-default-features`) nothing calls
// it — `error.rs` itself has no feature gate (§5.1), so it's still compiled.
#[allow(dead_code)]
pub(crate) fn from_io(path: PathBuf, source: std::io::Error) -> FileEngineError {
    match source.kind() {
        std::io::ErrorKind::NotFound => FileEngineError::SourceNotFound(path),
        std::io::ErrorKind::PermissionDenied => FileEngineError::PermissionDenied(path),
        std::io::ErrorKind::StorageFull => FileEngineError::InsufficientSpace {
            needed: 0,
            available: 0,
        },
        _ => FileEngineError::Io { path, source },
    }
}

#[cfg(feature = "diagnostics")]
use error_engine::{EngineDiagnostic, Severity};

#[cfg(feature = "diagnostics")]
impl EngineDiagnostic for FileEngineError {
    fn code(&self) -> &'static str {
        match self {
            Self::SourceNotFound(_) => "FE_SOURCE_NOT_FOUND",
            Self::DestinationExists(_) => "FE_DEST_EXISTS",
            Self::Cancelled => "FE_CANCELLED",
            Self::InsufficientSpace { .. } => "FE_NO_SPACE",
            Self::PermissionDenied(_) => "FE_PERMISSION_DENIED",
            Self::Io { .. } => "FE_IO",
            Self::UnknownCompressFormat(_) => "FE_UNKNOWN_COMPRESS_FORMAT",
            Self::GzipRequiresFile(_) => "FE_GZIP_REQUIRES_FILE",
        }
    }

    fn severity(&self) -> Severity {
        Severity::Error
    }

    fn context(&self) -> Vec<(&'static str, String)> {
        match self {
            Self::SourceNotFound(p) => vec![("path", p.display().to_string())],
            Self::DestinationExists(p) => vec![("path", p.display().to_string())],
            Self::InsufficientSpace { needed, available } => vec![
                ("needed", needed.to_string()),
                ("available", available.to_string()),
            ],
            Self::PermissionDenied(p) => vec![("path", p.display().to_string())],
            Self::Io { path, .. } => vec![("path", path.display().to_string())],
            Self::Cancelled => vec![],
            Self::UnknownCompressFormat(p) => vec![("path", p.display().to_string())],
            Self::GzipRequiresFile(p) => vec![("path", p.display().to_string())],
        }
    }
}

/// file-engine's own diagnostic catalog, embedded at compile time.
/// Consuming apps merge it into their own catalog:
///
/// ```ignore
/// let catalog = error_engine::Catalog::load_or_fallback("errors.toml")
///     .merged_with(file_engine::catalog());
/// ```
#[cfg(feature = "diagnostics")]
pub fn catalog() -> error_engine::Catalog {
    error_engine::Catalog::from_str(include_str!("../errors.toml"))
        .expect("file-engine's own catalog is valid TOML — covered by tests")
}
