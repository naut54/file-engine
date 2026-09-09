use std::path::PathBuf;
use std::time::{Instant, SystemTime};

use tokio_util::sync::CancellationToken;

use crate::error::Result;

use super::error_strategy::AnalysisErrorStrategy;
use super::filter::AnalysisFilter;
use super::handle::AnalysisHandle;
use super::progress::AnalysisProgressReporter;
use super::report::{AnalysisReport, DEFAULT_MAX_REPORTED_ERRORS};
use super::util::default_concurrency;
use super::walk::{walk, WalkParams};

#[cfg(feature = "checksum")]
use super::hash::detect_duplicates;
#[cfg(feature = "checksum")]
use super::report::DEFAULT_MAX_REPORTED_DUPLICATE_GROUPS;

/// Default cap on `largest_files` — see `AnalyzeBuilder::top_n_largest`.
pub const DEFAULT_TOP_N_LARGEST: usize = 10;

pub struct AnalyzeBuilder {
    root: PathBuf,
    filter: AnalysisFilter,
    error_strategy: AnalysisErrorStrategy,
    max_depth: Option<usize>,
    follow_symlinks: bool,
    top_n_largest: usize,
    detect_mime_types: bool,
    max_reported_errors: usize,
    walk_concurrency: Option<usize>,
    estimate_total: bool,
    #[cfg(feature = "checksum")]
    detect_duplicates: bool,
    #[cfg(feature = "checksum")]
    hash_concurrency: Option<usize>,
    #[cfg(feature = "checksum")]
    max_reported_duplicates: usize,
}

impl AnalyzeBuilder {
    pub(crate) fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            filter: AnalysisFilter::default(),
            error_strategy: AnalysisErrorStrategy::default(),
            max_depth: None,
            follow_symlinks: false,
            top_n_largest: DEFAULT_TOP_N_LARGEST,
            detect_mime_types: false,
            max_reported_errors: DEFAULT_MAX_REPORTED_ERRORS,
            walk_concurrency: None,
            estimate_total: false,
            #[cfg(feature = "checksum")]
            detect_duplicates: false,
            #[cfg(feature = "checksum")]
            hash_concurrency: None,
            #[cfg(feature = "checksum")]
            max_reported_duplicates: DEFAULT_MAX_REPORTED_DUPLICATE_GROUPS,
        }
    }

    /// Only files with one of these extensions (case-insensitive,
    /// without the leading dot) are matched. Unset matches any
    /// extension, including files with none.
    ///
    /// Applied only after a file is already stat'd — unlike
    /// `.exclude_globs()`, this never prunes traversal. To skip a whole
    /// subtree without walking it, use `.exclude_globs()` instead.
    pub fn extensions(mut self, exts: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.filter.extensions = Some(exts.into_iter().map(Into::into).collect());
        self
    }

    /// Glob patterns (matched against the path relative to the analyzed
    /// root) that prune traversal entirely — an excluded directory is
    /// never descended into, not merely omitted from the report. The
    /// only filter here that actually skips the underlying I/O, rather
    /// than filtering after a file has already been stat'd.
    pub fn exclude_globs(mut self, patterns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.filter.exclude_patterns = patterns.into_iter().map(Into::into).collect();
        self
    }

    /// Applied only after a file is already stat'd — unlike
    /// `.exclude_globs()`, this never prunes traversal. To skip a whole
    /// subtree without walking it, use `.exclude_globs()` instead.
    pub fn min_size(mut self, bytes: u64) -> Self {
        self.filter.min_size = Some(bytes);
        self
    }

    /// Applied only after a file is already stat'd — unlike
    /// `.exclude_globs()`, this never prunes traversal. To skip a whole
    /// subtree without walking it, use `.exclude_globs()` instead.
    pub fn max_size(mut self, bytes: u64) -> Self {
        self.filter.max_size = Some(bytes);
        self
    }

    /// Only files modified at or after `t` are matched. A file with no
    /// readable modified time never matches once this is set.
    ///
    /// Applied only after a file is already stat'd — unlike
    /// `.exclude_globs()`, this never prunes traversal. To skip a whole
    /// subtree without walking it, use `.exclude_globs()` instead.
    pub fn modified_after(mut self, t: SystemTime) -> Self {
        self.filter.modified_after = Some(t);
        self
    }

    /// Only files modified at or before `t` are matched. A file with no
    /// readable modified time never matches once this is set.
    ///
    /// Applied only after a file is already stat'd — unlike
    /// `.exclude_globs()`, this never prunes traversal. To skip a whole
    /// subtree without walking it, use `.exclude_globs()` instead.
    pub fn modified_before(mut self, t: SystemTime) -> Self {
        self.filter.modified_before = Some(t);
        self
    }

    /// Bounds how far the walk descends: the analyzed root is depth 0,
    /// its immediate children depth 1, and so on — passed straight
    /// through to `jwalk`'s own `max_depth`, which prunes traversal past
    /// the bound rather than filtering after the fact.
    pub fn max_depth(mut self, depth: usize) -> Self {
        self.max_depth = Some(depth);
        self
    }

    /// Off by default. When enabled, `jwalk`'s own loop detection
    /// surfaces a symlink cycle as a per-entry error, handled like any
    /// other error via `.on_error()`.
    pub fn follow_symlinks(mut self, follow: bool) -> Self {
        self.follow_symlinks = follow;
        self
    }

    /// How many worker threads `jwalk` uses to read directories and stat
    /// entries concurrently while walking. Defaults to
    /// `available_parallelism()`, matching `hash_concurrency`.
    ///
    /// Traversal is still reported (and aggregated into `AnalysisReport`)
    /// in a single stream on the calling task — this only parallelizes
    /// the underlying directory reads and `stat` calls, which is where
    /// wall time actually goes on large trees or slow/network
    /// filesystems.
    pub fn walk_concurrency(mut self, n: usize) -> Self {
        self.walk_concurrency = Some(n);
        self
    }

    /// Off by default. When enabled, `.start()` walks the tree twice:
    /// once to count how many files match the configured filters, then
    /// again to actually build the report. That first pass reports its
    /// result as `AnalysisProgress::Started`'s `estimated_entries`
    /// before any `EntryAnalyzed` events, so a caller can render a
    /// determinate progress bar or ETA — at the cost of stat-ing every
    /// file in the tree twice.
    pub fn estimate_total(mut self, enable: bool) -> Self {
        self.estimate_total = enable;
        self
    }

    /// How many of the largest matched files to keep in
    /// `AnalysisReport::largest_files`. `0` disables the tracking
    /// entirely.
    pub fn top_n_largest(mut self, n: usize) -> Self {
        self.top_n_largest = n;
        self
    }

    /// Off by default — sniffing every matched file's header via `infer`
    /// is an extra read per file, on top of the metadata `walkdir`
    /// already reads for every entry.
    pub fn detect_mime_types(mut self, enable: bool) -> Self {
        self.detect_mime_types = enable;
        self
    }

    pub fn on_error(mut self, strategy: AnalysisErrorStrategy) -> Self {
        self.error_strategy = strategy;
        self
    }

    /// Caps `AnalysisReport::errors`; `AnalysisReport::errors_total`
    /// stays uncapped regardless of this setting.
    pub fn max_reported_errors(mut self, n: usize) -> Self {
        self.max_reported_errors = n;
        self
    }

    /// Off by default — hashing every matched file (even the pre-filtered
    /// size-collision candidates) is real I/O on top of the walk itself.
    #[cfg(feature = "checksum")]
    pub fn detect_duplicates(mut self, enable: bool) -> Self {
        self.detect_duplicates = enable;
        self
    }

    /// Defaults to `available_parallelism()`, matching
    /// `CopyBuilder::batch_concurrency`.
    #[cfg(feature = "checksum")]
    pub fn hash_concurrency(mut self, n: usize) -> Self {
        self.hash_concurrency = Some(n);
        self
    }

    /// Caps `AnalysisReport::duplicates`; `duplicate_groups_total` and
    /// `duplicate_bytes_wasted` stay uncapped regardless of this setting.
    #[cfg(feature = "checksum")]
    pub fn max_reported_duplicates(mut self, n: usize) -> Self {
        self.max_reported_duplicates = n;
        self
    }

    pub fn start(self) -> Result<AnalysisHandle> {
        let cancel = CancellationToken::new();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let reporter = AnalysisProgressReporter::new(tx);
        let cancel_for_task = cancel.clone();

        #[cfg(feature = "checksum")]
        let detect_duplicates_enabled = self.detect_duplicates;

        let params = WalkParams {
            root: self.root,
            filter: self.filter,
            error_strategy: self.error_strategy,
            max_depth: self.max_depth,
            follow_symlinks: self.follow_symlinks,
            top_n_largest: self.top_n_largest,
            detect_mime_types: self.detect_mime_types,
            #[cfg(feature = "checksum")]
            collect_duplicate_candidates: detect_duplicates_enabled,
            max_reported_errors: self.max_reported_errors,
            walk_concurrency: self.walk_concurrency.unwrap_or_else(default_concurrency),
            estimate_total: self.estimate_total,
        };

        #[cfg(feature = "checksum")]
        let error_strategy = self.error_strategy;
        #[cfg(feature = "checksum")]
        let hash_concurrency = self.hash_concurrency;
        #[cfg(feature = "checksum")]
        let max_reported_duplicates = self.max_reported_duplicates;
        #[cfg(feature = "checksum")]
        let max_reported_errors = self.max_reported_errors;

        let join_handle = tokio::spawn(async move {
            let started = Instant::now();
            let outcome = walk(params, cancel_for_task.clone(), reporter.clone()).await?;

            #[cfg(feature = "checksum")]
            let (duplicates, duplicate_groups_total, duplicate_bytes_wasted, errors, errors_total) = {
                let mut errors = outcome.errors;
                let mut errors_total = outcome.errors_total;
                if detect_duplicates_enabled {
                    let hash_outcome = detect_duplicates(
                        outcome.duplicate_candidates,
                        hash_concurrency,
                        max_reported_duplicates,
                        max_reported_errors.saturating_sub(errors.len()),
                        error_strategy,
                        &cancel_for_task,
                        &reporter,
                    )
                    .await?;
                    errors.extend(hash_outcome.errors);
                    errors_total += hash_outcome.errors_total;
                    (
                        hash_outcome.groups,
                        hash_outcome.groups_total,
                        hash_outcome.bytes_wasted,
                        errors,
                        errors_total,
                    )
                } else {
                    (Vec::new(), 0, 0, errors, errors_total)
                }
            };
            #[cfg(not(feature = "checksum"))]
            let (errors, errors_total) = (outcome.errors, outcome.errors_total);

            Ok(AnalysisReport {
                file_count: outcome.file_count,
                dir_count: outcome.dir_count,
                total_size: outcome.total_size,
                largest_files: outcome.largest_files,
                by_extension: outcome.by_extension,
                by_mime: outcome.by_mime,
                age_buckets: outcome.age_buckets,
                errors,
                errors_total,
                #[cfg(feature = "checksum")]
                duplicates,
                #[cfg(feature = "checksum")]
                duplicate_groups_total,
                #[cfg(feature = "checksum")]
                duplicate_bytes_wasted,
                duration: started.elapsed(),
            })
        });

        Ok(AnalysisHandle::new(join_handle, rx, cancel))
    }
}
