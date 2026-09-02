use std::io;
use std::path::PathBuf;

use crate::error::Error;

/// Duplicated from (rather than shared with) `profiler::scan`'s
/// identical helper: `profiler` is gated behind `operations`, which
/// `analyze` doesn't require, and this is a few lines, not worth
/// restructuring the feature boundary between the two modules for.
pub(crate) fn classify_io_error(err: io::Error, path: PathBuf) -> Error {
    match err.kind() {
        io::ErrorKind::NotFound => Error::SourceNotFound { path },
        io::ErrorKind::PermissionDenied => Error::PermissionDenied { path },
        _ => Error::Io { path, source: err },
    }
}

pub(crate) fn classify_walkdir_error(err: walkdir::Error) -> Error {
    let path = err.path().map(|p| p.to_path_buf());
    match err.into_io_error() {
        Some(io_err) => classify_io_error(io_err, path.unwrap_or_default()),
        None => Error::Io {
            path: path.unwrap_or_default(),
            source: io::Error::other("directory walk error"),
        },
    }
}

/// Default worker pool size for concurrent hashing during duplicate
/// detection. Duplicated from `operations::default_concurrency` for the
/// same reason as `classify_io_error` above — that helper lives behind
/// `operations`, which `checksum` doesn't require.
#[cfg(feature = "checksum")]
pub(crate) fn default_concurrency() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}
