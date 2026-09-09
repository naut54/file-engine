use std::collections::{BinaryHeap, HashMap};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use globset::GlobSet;
use jwalk::{Parallelism, WalkDir};

use crate::error::{classify_io_error, Error, Result};

use super::error_strategy::AnalysisErrorStrategy;
use super::filter::AnalysisFilter;
use super::progress::{AnalysisProgress, AnalysisProgressReporter};
use super::report::{AgeBuckets, Entry, ExtensionStats, MimeStats};
use super::util::classify_jwalk_error;

pub(crate) struct WalkParams {
    pub(crate) root: PathBuf,
    pub(crate) filter: AnalysisFilter,
    pub(crate) error_strategy: AnalysisErrorStrategy,
    pub(crate) max_depth: Option<usize>,
    pub(crate) follow_symlinks: bool,
    pub(crate) top_n_largest: usize,
    pub(crate) detect_mime_types: bool,
    #[cfg(feature = "checksum")]
    pub(crate) collect_duplicate_candidates: bool,
    pub(crate) max_reported_errors: usize,
    /// Directory-read/stat worker count for the underlying `jwalk`
    /// traversal — see `AnalyzeBuilder::walk_concurrency`.
    pub(crate) walk_concurrency: usize,
    /// See `AnalyzeBuilder::estimate_total`.
    pub(crate) estimate_total: bool,
}

pub(crate) struct WalkOutcome {
    pub(crate) file_count: usize,
    pub(crate) dir_count: usize,
    pub(crate) total_size: u64,
    pub(crate) largest_files: Vec<Entry>,
    pub(crate) by_extension: HashMap<String, ExtensionStats>,
    pub(crate) by_mime: HashMap<String, MimeStats>,
    pub(crate) age_buckets: AgeBuckets,
    pub(crate) errors: Vec<(PathBuf, Error)>,
    pub(crate) errors_total: usize,
    /// Only populated when `collect_duplicate_candidates` is set —
    /// matched entries kept in full so `hash.rs` can bucket them by
    /// size. Not merely feature-gated but genuinely absent without
    /// `checksum` (rather than an always-empty `Vec`), so an
    /// `analyze`-only build never carries this around at all.
    #[cfg(feature = "checksum")]
    pub(crate) duplicate_candidates: Vec<Entry>,
}

/// Order by size so the heap's *smallest* kept entry sits at the top —
/// `BinaryHeap` is a max-heap, so wrapping in `Reverse` at push time
/// would work too, but comparing on size directly here keeps the heap
/// itself expressing "smallest of the largest N" without an extra
/// wrapper type threading through push/pop.
struct BySize(Entry);

impl PartialEq for BySize {
    fn eq(&self, other: &Self) -> bool {
        self.0.size == other.0.size
    }
}
impl Eq for BySize {}
impl PartialOrd for BySize {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for BySize {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reversed: the heap's *top* (`peek`/`pop`) is the smallest
        // entry currently kept, so a new larger entry can evict it in
        // O(log n) without scanning the whole top-N.
        other.0.size.cmp(&self.0.size)
    }
}

struct Aggregator {
    now: SystemTime,
    top_n: usize,
    detect_mime_types: bool,
    #[cfg(feature = "checksum")]
    collect_duplicate_candidates: bool,
    file_count: usize,
    dir_count: usize,
    total_size: u64,
    largest: BinaryHeap<BySize>,
    by_extension: HashMap<String, ExtensionStats>,
    by_mime: HashMap<String, MimeStats>,
    age_buckets: AgeBuckets,
    #[cfg(feature = "checksum")]
    duplicate_candidates: Vec<Entry>,
}

struct AggregatorOutcome {
    file_count: usize,
    dir_count: usize,
    total_size: u64,
    largest_files: Vec<Entry>,
    by_extension: HashMap<String, ExtensionStats>,
    by_mime: HashMap<String, MimeStats>,
    age_buckets: AgeBuckets,
    #[cfg(feature = "checksum")]
    duplicate_candidates: Vec<Entry>,
}

impl Aggregator {
    fn new(params: &WalkParams, now: SystemTime) -> Self {
        Self {
            now,
            top_n: params.top_n_largest,
            detect_mime_types: params.detect_mime_types,
            #[cfg(feature = "checksum")]
            collect_duplicate_candidates: params.collect_duplicate_candidates,
            file_count: 0,
            dir_count: 0,
            total_size: 0,
            largest: BinaryHeap::new(),
            by_extension: HashMap::new(),
            by_mime: HashMap::new(),
            age_buckets: AgeBuckets::default(),
            #[cfg(feature = "checksum")]
            duplicate_candidates: Vec::new(),
        }
    }

    fn record(&mut self, entry: Entry) {
        self.file_count += 1;
        self.total_size += entry.size;

        // Decided up front, from `entry.size` alone, so the heap and
        // duplicate-candidate pushes below can move `entry` instead of
        // cloning it in the common case where only one of them ends up
        // wanting it.
        let keep_in_heap = self.top_n > 0
            && (self.largest.len() < self.top_n
                || self
                    .largest
                    .peek()
                    .is_some_and(|smallest| entry.size > smallest.0.size));
        if keep_in_heap && self.largest.len() >= self.top_n {
            self.largest.pop();
        }

        let ext = entry
            .relative_path
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_ascii_lowercase())
            .unwrap_or_default();
        let stats = self.by_extension.entry(ext).or_default();
        stats.count += 1;
        stats.total_size += entry.size;

        if self.detect_mime_types {
            let mime = infer::get_from_path(&entry.path)
                .ok()
                .flatten()
                .map(|t| t.mime_type().to_string())
                .unwrap_or_else(|| "unknown".to_string());
            let stats = self.by_mime.entry(mime).or_default();
            stats.count += 1;
            stats.total_size += entry.size;
        }

        match entry.modified {
            None => self.age_buckets.unknown += 1,
            Some(modified) => {
                let age = self.now.duration_since(modified).unwrap_or(Duration::ZERO);
                if age < Duration::from_secs(24 * 60 * 60) {
                    self.age_buckets.under_1_day += 1;
                } else if age < Duration::from_secs(7 * 24 * 60 * 60) {
                    self.age_buckets.under_1_week += 1;
                } else if age < Duration::from_secs(30 * 24 * 60 * 60) {
                    self.age_buckets.under_1_month += 1;
                } else if age < Duration::from_secs(365 * 24 * 60 * 60) {
                    self.age_buckets.under_1_year += 1;
                } else {
                    self.age_buckets.older += 1;
                }
            }
        }

        #[cfg(feature = "checksum")]
        {
            let goes_to_duplicates = self.collect_duplicate_candidates;
            if keep_in_heap && goes_to_duplicates {
                self.largest.push(BySize(entry.clone()));
                self.duplicate_candidates.push(entry);
            } else if keep_in_heap {
                self.largest.push(BySize(entry));
            } else if goes_to_duplicates {
                self.duplicate_candidates.push(entry);
            }
        }
        #[cfg(not(feature = "checksum"))]
        if keep_in_heap {
            self.largest.push(BySize(entry));
        }
    }

    fn finish(self) -> AggregatorOutcome {
        let mut largest_files: Vec<Entry> = self.largest.into_iter().map(|b| b.0).collect();
        largest_files.sort_unstable_by_key(|entry| std::cmp::Reverse(entry.size));
        AggregatorOutcome {
            file_count: self.file_count,
            dir_count: self.dir_count,
            total_size: self.total_size,
            largest_files,
            by_extension: self.by_extension,
            by_mime: self.by_mime,
            age_buckets: self.age_buckets,
            #[cfg(feature = "checksum")]
            duplicate_candidates: self.duplicate_candidates,
        }
    }
}

impl AggregatorOutcome {
    fn into_walk_outcome(self, errors: Vec<(PathBuf, Error)>, errors_total: usize) -> WalkOutcome {
        WalkOutcome {
            file_count: self.file_count,
            dir_count: self.dir_count,
            total_size: self.total_size,
            largest_files: self.largest_files,
            by_extension: self.by_extension,
            by_mime: self.by_mime,
            age_buckets: self.age_buckets,
            errors,
            errors_total,
            #[cfg(feature = "checksum")]
            duplicate_candidates: self.duplicate_candidates,
        }
    }
}

pub(crate) async fn walk(
    params: WalkParams,
    cancel: tokio_util::sync::CancellationToken,
    reporter: AnalysisProgressReporter,
) -> Result<WalkOutcome> {
    tokio::task::spawn_blocking(move || walk_blocking(params, cancel, reporter))
        .await
        .expect("walk blocking task panicked")
}

/// Builds the `jwalk` walker shared by the counting pass and the real
/// aggregation pass, so the two can never drift in what they consider
/// "the tree" (same exclude pruning, same symlink/depth handling).
fn build_walker(params: &WalkParams, excludes: Option<GlobSet>) -> WalkDir {
    let exclude_root = params.root.clone();

    let mut walker = WalkDir::new(&params.root)
        .skip_hidden(false)
        .follow_links(params.follow_symlinks)
        .parallelism(Parallelism::RayonNewPool(params.walk_concurrency.max(1)))
        .process_read_dir(move |_depth, _read_dir_path, _state, children| {
            let Some(excludes) = &excludes else {
                return;
            };
            children.retain(|child| {
                let Ok(child) = child else {
                    return true;
                };
                // The root itself (depth 0) is never excluded — only its
                // descendants.
                if child.depth == 0 {
                    return true;
                }
                let path = child.path();
                let relative = path.strip_prefix(&exclude_root).unwrap_or(&path);
                !excludes.is_match(relative)
            });
        });
    if let Some(max_depth) = params.max_depth {
        walker = walker.max_depth(max_depth);
    }
    walker
}

/// A full extra pass over the tree, counting how many entries would
/// match the same filters the real walk applies — see
/// `AnalyzeBuilder::estimate_total`. Walk errors are swallowed here
/// (this is only an estimate; the real pass reports them properly)
/// rather than duplicating `error_strategy` handling for a number that's
/// allowed to be approximate.
fn count_matching_entries(
    params: &WalkParams,
    excludes: Option<GlobSet>,
    cancel: &tokio_util::sync::CancellationToken,
) -> Result<usize> {
    let root: &Path = &params.root;
    let mut count = 0usize;

    for result in build_walker(params, excludes) {
        if cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }

        let Ok(walk_entry) = result else {
            continue;
        };
        if !walk_entry.file_type().is_file() {
            continue;
        }
        let Ok(metadata) = walk_entry.metadata() else {
            continue;
        };

        let full_path = walk_entry.path();
        let relative_path = full_path.strip_prefix(root).unwrap_or(&full_path);
        if params
            .filter
            .matches(relative_path, metadata.len(), metadata.modified().ok())
        {
            count += 1;
        }
    }

    Ok(count)
}

fn walk_blocking(
    params: WalkParams,
    cancel: tokio_util::sync::CancellationToken,
    reporter: AnalysisProgressReporter,
) -> Result<WalkOutcome> {
    let excludes = params.filter.compiled_excludes()?;
    let now = SystemTime::now();
    let mut aggregator = Aggregator::new(&params, now);
    let mut errors: Vec<(PathBuf, Error)> = Vec::new();
    let mut errors_total = 0usize;

    let root_metadata = std::fs::metadata(&params.root)
        .map_err(|e| classify_io_error(e, params.root.clone(), 0))?;

    if root_metadata.is_file() {
        // A single file is always its own trivial "total" — not worth a
        // second pass just to confirm what's already known.
        reporter.send(AnalysisProgress::Started {
            estimated_entries: None,
        });

        let relative_path = params
            .root
            .file_name()
            .map(PathBuf::from)
            .unwrap_or_else(|| params.root.clone());
        let size = root_metadata.len();
        let modified = root_metadata.modified().ok();
        if params.filter.matches(&relative_path, size, modified) {
            let entry = Entry {
                path: params.root.clone(),
                relative_path,
                size,
                modified,
            };
            reporter.send(AnalysisProgress::EntryAnalyzed {
                path: entry.path.clone(),
            });
            aggregator.record(entry);
        }
        return Ok(aggregator.finish().into_walk_outcome(errors, errors_total));
    }

    let estimated_entries = if params.estimate_total {
        Some(count_matching_entries(&params, excludes.clone(), &cancel)?)
    } else {
        None
    };
    reporter.send(AnalysisProgress::Started { estimated_entries });

    let root = params.root.clone();

    for result in build_walker(&params, excludes) {
        if cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }

        let walk_entry = match result {
            Ok(e) => e,
            Err(err) => {
                let err = classify_jwalk_error(err);
                let path = err_path(&err);
                if params.error_strategy == AnalysisErrorStrategy::AbortOnError {
                    return Err(err);
                }
                errors_total += 1;
                if errors.len() < params.max_reported_errors {
                    errors.push((path, err));
                }
                continue;
            }
        };

        let file_type = walk_entry.file_type();
        let full_path = walk_entry.path();
        let relative_path = full_path
            .strip_prefix(&root)
            .unwrap_or(&full_path)
            .to_path_buf();

        if file_type.is_dir() {
            aggregator.dir_count += 1;
            continue;
        }
        if !file_type.is_file() {
            // Symlinks not followed (`follow_symlinks(false)`, the
            // default) land here and are skipped, mirroring
            // `profiler::scan`'s behavior.
            continue;
        }

        let metadata = match walk_entry.metadata() {
            Ok(m) => m,
            Err(err) => {
                let err = classify_jwalk_error(err);
                if params.error_strategy == AnalysisErrorStrategy::AbortOnError {
                    return Err(err);
                }
                errors_total += 1;
                if errors.len() < params.max_reported_errors {
                    errors.push((full_path, err));
                }
                continue;
            }
        };

        let size = metadata.len();
        let modified = metadata.modified().ok();
        if !params.filter.matches(&relative_path, size, modified) {
            continue;
        }

        let entry = Entry {
            path: full_path,
            relative_path,
            size,
            modified,
        };
        reporter.send(AnalysisProgress::EntryAnalyzed {
            path: entry.path.clone(),
        });
        aggregator.record(entry);
    }

    Ok(aggregator.finish().into_walk_outcome(errors, errors_total))
}

fn err_path(err: &Error) -> PathBuf {
    match err {
        Error::SourceNotFound { path }
        | Error::DestExists { path }
        | Error::PermissionDenied { path }
        | Error::Io { path, .. } => path.clone(),
        _ => PathBuf::new(),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;
    use tokio_util::sync::CancellationToken;

    use super::*;

    fn params(root: PathBuf) -> WalkParams {
        WalkParams {
            root,
            filter: AnalysisFilter::default(),
            error_strategy: AnalysisErrorStrategy::ContinueAndCollect,
            max_depth: None,
            follow_symlinks: false,
            top_n_largest: 10,
            detect_mime_types: false,
            #[cfg(feature = "checksum")]
            collect_duplicate_candidates: false,
            max_reported_errors: 1000,
            walk_concurrency: 2,
            estimate_total: false,
        }
    }

    fn noop_reporter() -> AnalysisProgressReporter {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        AnalysisProgressReporter::new(tx)
    }

    #[tokio::test]
    async fn basic_stats_over_a_small_tree() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("a.txt"), vec![0u8; 10]).unwrap();
        fs::write(dir.path().join("b.txt"), vec![0u8; 20]).unwrap();
        fs::create_dir(dir.path().join("nested")).unwrap();
        fs::write(dir.path().join("nested").join("c.rs"), vec![0u8; 30]).unwrap();

        let outcome = walk(
            params(dir.path().to_path_buf()),
            CancellationToken::new(),
            noop_reporter(),
        )
        .await
        .unwrap();

        assert_eq!(outcome.file_count, 3);
        assert_eq!(outcome.total_size, 60);
        assert_eq!(outcome.largest_files[0].size, 30);
        assert_eq!(outcome.by_extension["txt"].count, 2);
        assert_eq!(outcome.by_extension["rs"].count, 1);
    }

    #[tokio::test]
    async fn exclude_glob_prunes_the_whole_subtree() {
        let dir = tempdir().unwrap();
        fs::create_dir(dir.path().join("node_modules")).unwrap();
        fs::write(dir.path().join("node_modules").join("pkg.js"), b"x").unwrap();
        fs::write(dir.path().join("main.rs"), b"x").unwrap();

        let mut p = params(dir.path().to_path_buf());
        p.filter.exclude_patterns = vec!["**/node_modules/**".to_string()];

        let outcome = walk(p, CancellationToken::new(), noop_reporter())
            .await
            .unwrap();

        assert_eq!(outcome.file_count, 1);
    }

    #[tokio::test]
    async fn single_file_root_produces_one_entry() {
        let dir = tempdir().unwrap();
        let file = dir.path().join("only.txt");
        fs::write(&file, vec![0u8; 5]).unwrap();

        let outcome = walk(params(file), CancellationToken::new(), noop_reporter())
            .await
            .unwrap();

        assert_eq!(outcome.file_count, 1);
        assert_eq!(outcome.total_size, 5);
        assert_eq!(outcome.dir_count, 0);
    }

    #[tokio::test]
    async fn max_depth_limits_the_walk() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("top.txt"), b"x").unwrap();
        fs::create_dir_all(dir.path().join("a").join("b")).unwrap();
        fs::write(dir.path().join("a").join("deep.txt"), b"x").unwrap();
        fs::write(dir.path().join("a").join("b").join("deeper.txt"), b"x").unwrap();

        let mut p = params(dir.path().to_path_buf());
        p.max_depth = Some(1);

        let outcome = walk(p, CancellationToken::new(), noop_reporter())
            .await
            .unwrap();

        // depth 0 = root, depth 1 = top.txt and `a/` itself.
        assert_eq!(outcome.file_count, 1);
    }

    #[tokio::test]
    async fn estimate_total_reports_the_matching_count_before_any_entries() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("a.txt"), b"x").unwrap();
        fs::write(dir.path().join("b.log"), b"x").unwrap();

        let mut p = params(dir.path().to_path_buf());
        p.estimate_total = true;
        p.filter.extensions = Some(vec!["txt".to_string()]);

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let reporter = AnalysisProgressReporter::new(tx);

        let outcome = walk(p, CancellationToken::new(), reporter).await.unwrap();
        assert_eq!(outcome.file_count, 1);

        match rx.recv().await {
            Some(AnalysisProgress::Started { estimated_entries }) => {
                assert_eq!(estimated_entries, Some(1));
            }
            other => panic!("expected Started first, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn estimate_total_off_reports_no_estimate() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("a.txt"), b"x").unwrap();

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let reporter = AnalysisProgressReporter::new(tx);

        walk(
            params(dir.path().to_path_buf()),
            CancellationToken::new(),
            reporter,
        )
        .await
        .unwrap();

        match rx.recv().await {
            Some(AnalysisProgress::Started { estimated_entries }) => {
                assert_eq!(estimated_entries, None);
            }
            other => panic!("expected Started first, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn nonexistent_path_returns_error() {
        let dir = tempdir().unwrap();
        let missing = dir.path().join("does-not-exist");

        let result = walk(params(missing), CancellationToken::new(), noop_reporter()).await;
        assert!(matches!(result, Err(Error::SourceNotFound { .. })));
    }

    // Mirrors `profiler::scan`'s three symlink tests — `analysis::walk`
    // wires `follow_symlinks` into `jwalk` the same way `profiler::scan`
    // wires it into `walkdir`, but until now nothing actually exercised
    // it here.

    #[cfg(unix)]
    #[tokio::test]
    async fn symlinks_are_skipped_not_followed() {
        let dir = tempdir().unwrap();
        let target = dir.path().join("real.txt");
        fs::write(&target, vec![0u8; 10]).unwrap();

        let link = dir.path().join("link.txt");
        std::os::unix::fs::symlink(&target, &link).unwrap();

        let outcome = walk(
            params(dir.path().to_path_buf()),
            CancellationToken::new(),
            noop_reporter(),
        )
        .await
        .unwrap();

        assert_eq!(outcome.file_count, 1);
        assert_eq!(
            outcome.largest_files[0].relative_path,
            PathBuf::from("real.txt")
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn follow_symlinks_true_walks_into_a_symlinked_directory() {
        let real_dir = tempdir().unwrap();
        fs::write(real_dir.path().join("inside.txt"), b"a").unwrap();

        let root_dir = tempdir().unwrap();
        let link = root_dir.path().join("link");
        std::os::unix::fs::symlink(real_dir.path(), &link).unwrap();

        let mut p = params(root_dir.path().to_path_buf());
        p.follow_symlinks = true;

        let outcome = walk(p, CancellationToken::new(), noop_reporter())
            .await
            .unwrap();

        assert!(outcome
            .largest_files
            .iter()
            .any(|e| e.relative_path == PathBuf::from("link").join("inside.txt")));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn follow_symlinks_true_reports_a_cycle_as_an_error_instead_of_hanging() {
        let dir = tempdir().unwrap();
        let subdir = dir.path().join("nested");
        fs::create_dir(&subdir).unwrap();
        let cycle_link = subdir.join("back_to_root");
        std::os::unix::fs::symlink(dir.path(), &cycle_link).unwrap();

        let mut p = params(dir.path().to_path_buf());
        p.follow_symlinks = true;
        p.error_strategy = AnalysisErrorStrategy::AbortOnError;

        let result = walk(p, CancellationToken::new(), noop_reporter()).await;

        assert!(result.is_err(), "a symlink cycle must not hang the walk");
    }
}
