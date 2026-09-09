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

    // Constructed by `analyze`'s `AnalysisFilter` and `remove`'s
    // `RemoveFilter` independently (neither implies the other) — gated
    // on the union of both, same reasoning as `classify_io_error` below.
    #[cfg(any(feature = "analyze", feature = "remove"))]
    #[error("invalid glob pattern {pattern:?}: {source}")]
    InvalidGlobPattern {
        pattern: String,
        source: globset::Error,
    },

    #[cfg(feature = "compress")]
    #[error("gzip compression requires a single file, got a directory: {path}")]
    GzipRequiresFile { path: PathBuf },

    // The four variants below back the pre-flight validation in
    // `profiler::validate` — gated on `operations` since that's the
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
    /// `ErrorStrategy`), this describes a whole-destination risk, not a
    /// property of any specific entry — `is_fatal` accordingly.
    #[cfg(feature = "operations")]
    #[error(
        "destination filesystem ({filesystem}) has a known write-integrity issue on this platform"
    )]
    FilesystemIntegrityRisk { filesystem: String },

    /// Whole-batch pre-flight validation for `MoveManyBuilder`, same
    /// spirit as `FilesystemIntegrityRisk` above: two sources sharing a
    /// basename is ambiguous ("moved into `dest`" would mean two
    /// different things), so it's caught before any source is touched
    /// rather than surfacing as a confusing overwrite of one by the
    /// other partway through. `is_fatal` accordingly.
    #[cfg(feature = "operations")]
    #[error("two sources would both move to the same destination name: {path} and {other}")]
    DuplicateSourceName { path: PathBuf, other: PathBuf },

    /// `MoveManyBuilder` pre-flight validation: a source with no final
    /// path component (`/`, `.`, `..`, ...) has nothing to name its
    /// destination entry after. `is_fatal`, same reasoning as
    /// `DuplicateSourceName`.
    #[cfg(feature = "operations")]
    #[error("source path has no file name to move under: {path}")]
    InvalidSourceName { path: PathBuf },

    /// `RemoveBuilder` pre-flight validation: `.start()` refuses to run
    /// with no filter criteria set at all, since an empty
    /// `RemoveFilter` matches every entry under the root — "delete
    /// everything" should never be the accidental default of an
    /// unconfigured builder. `.allow_unfiltered_delete(true)` is the
    /// explicit opt-in past this.
    #[cfg(feature = "remove")]
    #[error(
        "remove() called with no filter criteria set, which would match every entry under the root"
    )]
    RemoveCriteriaRequired,

    /// Per-entry: `RemoveBuilder` defaults to moving matched entries to
    /// the platform trash/recycle bin rather than unlinking them
    /// outright (see `RemoveBuilder::hard_delete`). Surfaced instead of
    /// silently falling back to a hard delete, since that fallback would
    /// defeat the point of trash being the safe default — a platform or
    /// environment with no trash service (e.g. a headless Linux box)
    /// must fail loudly here, not delete permanently without being
    /// asked to.
    #[cfg(feature = "remove")]
    #[error("could not move to trash: {path}: {source}")]
    TrashFailed { path: PathBuf, source: trash::Error },
}

impl Error {
    /// Fatal errors stop all remaining dispatch regardless of the
    /// configured `ErrorStrategy`; per-entry errors are handled per that
    /// strategy. `PermissionDenied`/`Io` default to per-entry despite
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
            Error::Cancelled
                | Error::NoSpace { .. }
                | Error::FilesystemIntegrityRisk { .. }
                | Error::DuplicateSourceName { .. }
                | Error::InvalidSourceName { .. }
        )
    }
}

/// Maps a raw `io::Error` onto the crate's `Error` variants — shared by
/// every module that turns a filesystem call's `io::Error` into one.
/// Previously duplicated ten times, nearly identically, across
/// `operations`/`profiler`/`analysis` (see
/// `dev-docs/design/error-classification-audit.md`'s "Research"
/// section); consolidated here specifically because `error.rs` is the
/// one module every feature combination compiles unconditionally —
/// `profiler::scan`'s copy of this was gated behind `operations`, which
/// is exactly why `analysis::util` couldn't reuse it and grew its own.
///
/// `needed` is only meaningful for the `StorageFull` arm (`NoSpace`'s
/// `needed` field); callers with no real figure to hand pass `0` rather
/// than fabricating one, matching every pre-consolidation call site
/// except `planner::action`'s (which has the entry's real size).
/// `available` isn't queried at this level either (would need an extra
/// statvfs-style syscall) so it's always reported as `0`.
///
/// Gated on the union of every feature that actually calls this
/// (`operations`, `watch`, `analyze` independently — none of the three
/// implies another) rather than left unconditional, so a build enabling
/// none of them (e.g. `--no-default-features --features diagnostics`,
/// exercised by CI's feature-powerset check) doesn't fail on dead code.
#[cfg(any(feature = "operations", feature = "watch", feature = "analyze"))]
pub(crate) fn classify_io_error(err: io::Error, path: PathBuf, needed: u64) -> Error {
    match err.kind() {
        io::ErrorKind::NotFound => Error::SourceNotFound { path },
        io::ErrorKind::PermissionDenied => Error::PermissionDenied { path },
        io::ErrorKind::StorageFull => Error::NoSpace {
            needed,
            available: 0,
        },
        _ => Error::Io { path, source: err },
    }
}

pub type Result<T> = std::result::Result<T, Error>;
