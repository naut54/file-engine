use std::io;

use crate::error::{classify_io_error, Error};

pub(crate) fn classify_jwalk_error(err: jwalk::Error) -> Error {
    let path = err.path().map(|p| p.to_path_buf());
    match err.into_io_error() {
        Some(io_err) => classify_io_error(io_err, path.unwrap_or_default(), 0),
        None => Error::Io {
            path: path.unwrap_or_default(),
            source: io::Error::other("directory walk error"),
        },
    }
}

/// Default worker pool size for both the parallel tree walk (`walk.rs`)
/// and concurrent hashing during duplicate detection (`hash.rs`).
/// Duplicated from `operations::default_concurrency` — that helper lives
/// behind `operations`, which `analyze` doesn't require. Unlike error
/// classification (consolidated into `crate::error::classify_io_error`,
/// which lives outside any feature gate), this one has no
/// feature-independent home to move to without restructuring where
/// `default_concurrency` itself is defined.
pub(crate) fn default_concurrency() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}
