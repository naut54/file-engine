use std::io;
use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("source not found: {path}")]
    SourceNotFound { path: PathBuf },

    #[error("destination already exists: {path}")]
    DestExists { path: PathBuf },

    #[error("operation cancelled")]
    Cancelled,

    #[error("insufficient disk space: needed {needed} bytes, available {available} bytes")]
    NoSpace { needed: u64, available: u64 },

    #[error("permission denied: {path}")]
    PermissionDenied { path: PathBuf },

    #[error("io error on {path}: {source}")]
    Io { path: PathBuf, source: io::Error },

    #[cfg(feature = "compress")]
    #[error("could not infer compression format from destination: {path}")]
    UnknownCompressFormat { path: PathBuf },

    #[cfg(feature = "compress")]
    #[error("gzip compression requires a single file, got a directory: {path}")]
    GzipRequiresFile { path: PathBuf },

    // The four variants below back
    // dev-docs/design/filesystem-detection.md's pre-flight validation
    // (`profiler::validate`) — gated on `operations` since that's the
    // only feature that ever constructs them, matching the
    // `compress`-gated variants above.
    #[cfg(feature = "operations")]
    #[error("filename differs only by case from another entry, which the destination filesystem cannot represent: {path} collides with {other}")]
    CaseCollision { path: PathBuf, other: PathBuf },

    #[cfg(feature = "operations")]
    #[error("file exceeds the destination filesystem's maximum file size: {path} ({size} bytes, max {max} bytes)")]
    FileTooLargeForDest { path: PathBuf, size: u64, max: u64 },

    #[cfg(feature = "operations")]
    #[error("filename is reserved or invalid on the destination filesystem: {path}")]
    ReservedName { path: PathBuf },

    /// Unlike the three variants above (per-entry, governed by
    /// `ErrorStrategy` — see dev-docs/design/filesystem-detection.md,
    /// "Decisions"), this describes a whole-destination risk, not a
    /// property of any specific entry — `is_fatal` accordingly.
    #[cfg(feature = "operations")]
    #[error(
        "destination filesystem ({filesystem}) has a known write-integrity issue on this platform"
    )]
    FilesystemIntegrityRisk { filesystem: String },
}

impl Error {
    /// Fatal errors stop all remaining dispatch regardless of the
    /// configured `ErrorStrategy`; per-entry errors are handled per that
    /// strategy. See dev-docs/design/batching-engine.md for the rationale,
    /// including why `PermissionDenied`/`Io` default to per-entry despite
    /// being genuinely ambiguous.
    ///
    /// Only consumed by `operations`-gated code (`dispatcher.rs`,
    /// `move_path.rs`, `sync.rs`, `compress.rs`) — gated to match, since
    /// a `watch`-only build (which doesn't imply `operations`) would
    /// otherwise leave this genuinely dead and fail the crate's
    /// warnings-as-errors build (`.cargo/config.toml`).
    #[cfg(feature = "operations")]
    pub(crate) fn is_fatal(&self) -> bool {
        matches!(
            self,
            Error::Cancelled | Error::NoSpace { .. } | Error::FilesystemIntegrityRisk { .. }
        )
    }
}

pub type Result<T> = std::result::Result<T, Error>;
