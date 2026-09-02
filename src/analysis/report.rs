use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use crate::error::Error;

/// A file discovered and matched by `AnalyzeBuilder`'s filters.
///
/// Deliberately a separate type from `profiler::Entry` rather than a
/// reuse: `profiler` (and everything in it) is gated behind the
/// `operations` feature, while `analyze` doesn't require `operations` —
/// reaching into `profiler::Entry` would force every `analyze`-only
/// build to pull in the whole copy/move/sync pipeline just for this
/// struct.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Entry {
    pub path: PathBuf,
    pub relative_path: PathBuf,
    pub size: u64,
    /// `None` when the platform/filesystem doesn't report mtimes.
    pub modified: Option<SystemTime>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExtensionStats {
    pub count: usize,
    pub total_size: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MimeStats {
    pub count: usize,
    pub total_size: u64,
}

/// Files bucketed by how long ago they were last modified, relative to a
/// single `now` captured once when the walk starts — not re-read per
/// entry, so results are deterministic within one run regardless of how
/// long the walk takes. Each entry lands in the first bucket whose
/// boundary it's younger than.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AgeBuckets {
    pub under_1_day: usize,
    pub under_1_week: usize,
    pub under_1_month: usize,
    pub under_1_year: usize,
    pub older: usize,
    /// Entries with no readable `modified` time on this platform/filesystem.
    pub unknown: usize,
}

/// A set of files sharing both size and blake3 content hash. Only
/// produced when `.detect_duplicates(true)` (feature `checksum`) is set.
#[cfg(feature = "checksum")]
#[derive(Debug, Clone)]
pub struct DuplicateGroup {
    pub hash: [u8; 32],
    pub size: u64,
    pub paths: Vec<PathBuf>,
}

/// Default cap on how many `(path, Error)` pairs `AnalysisReport::errors`
/// retains — see `AnalysisReport::errors_total` for the uncapped count.
/// Chosen to keep a report from a badly-permissioned tree bounded in
/// memory; override with `AnalyzeBuilder::max_reported_errors`.
pub const DEFAULT_MAX_REPORTED_ERRORS: usize = 1000;

/// Default cap on how many `DuplicateGroup`s `AnalysisReport::duplicates`
/// retains — see `AnalysisReport::duplicate_groups_total` and
/// `AnalysisReport::duplicate_bytes_wasted`, both uncapped, for the sums
/// that stay accurate even once the sample is truncated.
#[cfg(feature = "checksum")]
pub const DEFAULT_MAX_REPORTED_DUPLICATE_GROUPS: usize = 1000;

/// `#[non_exhaustive]`: a new aggregate (e.g. permission-mode breakdown)
/// is additive, not a breaking change for callers who construct nothing
/// and only read fields — mirrors `Progress`'s reasoning in
/// `src/progress.rs`.
#[derive(Debug)]
#[non_exhaustive]
pub struct AnalysisReport {
    pub file_count: usize,
    pub dir_count: usize,
    pub total_size: u64,
    /// Largest matched files, descending by size, capped at
    /// `AnalyzeBuilder::top_n_largest`.
    pub largest_files: Vec<Entry>,
    /// Keyed by lowercased extension without the leading dot; entries
    /// with no extension are grouped under the empty string `""`.
    pub by_extension: HashMap<String, ExtensionStats>,
    /// Populated only when `.detect_mime_types(true)` is set; empty
    /// otherwise. Keyed by the MIME type `infer` reports, or `"unknown"`
    /// when it can't classify the file's header.
    pub by_mime: HashMap<String, MimeStats>,
    pub age_buckets: AgeBuckets,
    /// Sample of encountered errors, first-N by walk order (not sorted
    /// by severity or size), capped at `AnalyzeBuilder::max_reported_errors`.
    pub errors: Vec<(PathBuf, Error)>,
    /// True count of errors encountered, independent of the `errors` cap.
    pub errors_total: usize,
    /// Sample of duplicate groups, first-N found, capped at
    /// `AnalyzeBuilder::max_reported_duplicates`. Empty unless
    /// `.detect_duplicates(true)` was set.
    #[cfg(feature = "checksum")]
    pub duplicates: Vec<DuplicateGroup>,
    /// True count of duplicate groups found, independent of the
    /// `duplicates` cap.
    #[cfg(feature = "checksum")]
    pub duplicate_groups_total: usize,
    /// Sum of `size * (paths.len() - 1)` over *every* duplicate group
    /// found, not just the capped sample — the number people actually
    /// want out of duplicate detection shouldn't degrade just because
    /// the detailed list got truncated.
    #[cfg(feature = "checksum")]
    pub duplicate_bytes_wasted: u64,
    pub duration: Duration,
}
