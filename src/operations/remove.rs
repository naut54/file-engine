use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::time::{Instant, SystemTime};

use tokio_util::sync::CancellationToken;

use crate::error::{classify_io_error, Error, Result};
use crate::handle::Handle;
use crate::planner::{
    dispatch, plan, BatchConfig, EntryAction, EntryOutcome, ErrorStrategy, StopReason,
};
use crate::profiler::{scan, Entry, ScanOptions, Workload};
use crate::progress::ProgressReporter;

use super::default_concurrency;
use super::remove_filter::RemoveFilter;

/// Result of a `RemoveBuilder` run.
///
/// `#[non_exhaustive]`: an output type built by this crate and read by
/// the caller, so adding a field later isn't a breaking change for
/// exhaustive struct literals — same reasoning as `OperationOutcome`.
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct RemoveOutcome {
    /// Entries actually removed. Empty when `.dry_run(true)` (the
    /// default) — see `previewed` instead.
    pub succeeded: Vec<Entry>,
    /// Entries that matched the filter but were never touched, because
    /// `.dry_run(true)` (the default) was in effect. Always empty
    /// otherwise.
    pub previewed: Vec<Entry>,
    pub failed: Vec<(Entry, Error)>,
    pub stopped_early: Option<StopReason>,
    pub duration: std::time::Duration,
}

/// Deletes files under a root that match a set of criteria (extension,
/// size range, modified-time range, exclude globs), rather than an
/// entire path outright — the destructive counterpart to `analyze()`,
/// built on the same filter shape.
///
/// Two defaults set this apart from every other builder in the crate,
/// both deliberately biased toward safety over convenience given this
/// is the one irreversible operation the crate offers:
///
/// - `.dry_run()` defaults to `true`: `.start()` reports what *would*
///   be removed (`RemoveOutcome::previewed`) without touching anything,
///   until told otherwise.
/// - `.hard_delete()` defaults to `false`: matched entries go to the
///   platform trash/recycle bin, not straight to `unlink`.
pub struct RemoveBuilder {
    root: PathBuf,
    filter: RemoveFilter,
    max_depth: Option<usize>,
    follow_symlinks: bool,
    dry_run: bool,
    hard_delete: bool,
    allow_unfiltered_delete: bool,
    batch_config: BatchConfig,
    concurrency: Option<usize>,
}

impl RemoveBuilder {
    pub(crate) fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            filter: RemoveFilter::default(),
            max_depth: None,
            follow_symlinks: false,
            dry_run: true,
            hard_delete: false,
            allow_unfiltered_delete: false,
            batch_config: BatchConfig::default(),
            concurrency: None,
        }
    }

    /// Only files with one of these extensions (case-insensitive,
    /// without the leading dot) are matched. Unset matches any
    /// extension, including files with none.
    pub fn extensions(mut self, exts: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.filter.extensions = Some(exts.into_iter().map(Into::into).collect());
        self
    }

    /// Glob patterns (matched against the path relative to `root`) that
    /// exclude an otherwise-matching entry from removal.
    pub fn exclude(mut self, patterns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.filter.exclude_patterns = patterns.into_iter().map(Into::into).collect();
        self
    }

    pub fn min_size(mut self, bytes: u64) -> Self {
        self.filter.min_size = Some(bytes);
        self
    }

    pub fn max_size(mut self, bytes: u64) -> Self {
        self.filter.max_size = Some(bytes);
        self
    }

    /// Only files modified at or after `t` are matched. A file with no
    /// readable modified time never matches once this is set.
    pub fn modified_after(mut self, t: SystemTime) -> Self {
        self.filter.modified_after = Some(t);
        self
    }

    /// Only files modified at or before `t` are matched. A file with no
    /// readable modified time never matches once this is set.
    pub fn modified_before(mut self, t: SystemTime) -> Self {
        self.filter.modified_before = Some(t);
        self
    }

    /// Bounds how far the walk descends: the removed root is depth 0,
    /// its immediate children depth 1, and so on — passed straight
    /// through to `walkdir`'s own `max_depth`, which prunes traversal
    /// past the bound rather than filtering after the fact. Same
    /// semantics as `AnalyzeBuilder::max_depth`.
    pub fn max_depth(mut self, depth: usize) -> Self {
        self.max_depth = Some(depth);
        self
    }

    /// Off by default — a symlink (to a file or a directory) is skipped,
    /// not followed. When enabled, a symlinked directory is walked into
    /// and its contents become eligible for removal, and a symlink
    /// cycle surfaces as a per-entry error rather than an infinite
    /// walk. Same semantics as `AnalyzeBuilder::follow_symlinks`.
    pub fn follow_symlinks(mut self, follow: bool) -> Self {
        self.follow_symlinks = follow;
        self
    }

    /// Defaults to `true`: `.start()` walks and filters `root` exactly
    /// as it would for real, but returns the matches in
    /// `RemoveOutcome::previewed` instead of removing anything. Pass
    /// `false` to actually remove.
    pub fn dry_run(mut self, dry_run: bool) -> Self {
        self.dry_run = dry_run;
        self
    }

    /// Defaults to `false`: matched entries are moved to the platform
    /// trash/recycle bin. Pass `true` to unlink them permanently
    /// instead. A platform/environment with no trash service available
    /// fails per-entry with `Error::TrashFailed` rather than silently
    /// falling back to a hard delete — that fallback would defeat the
    /// point of trash being the safe default.
    pub fn hard_delete(mut self, hard: bool) -> Self {
        self.hard_delete = hard;
        self
    }

    /// Escape hatch for removing everything under `root` with no filter
    /// at all. Without this, `.start()`'s `Handle` resolves to
    /// `Err(Error::RemoveCriteriaRequired)` before anything is touched
    /// if no criterion (`.extensions()`, `.min_size()`, ...) has been
    /// set — an unconfigured filter matching *everything* should never
    /// be an accident.
    pub fn allow_unfiltered_delete(mut self, allow: bool) -> Self {
        self.allow_unfiltered_delete = allow;
        self
    }

    /// `ErrorStrategy::Undo` stops the batch on the first failure like
    /// it does for copy/move, but cannot roll back entries already
    /// removed — a hard-deleted file is gone, and trashed files have no
    /// reliable cross-platform restore. Prefer `.dry_run(true)` (the
    /// default) to check what would be removed before committing.
    pub fn on_error(mut self, strategy: ErrorStrategy) -> Self {
        self.batch_config.error_strategy = strategy;
        self
    }

    pub fn batch_concurrency(mut self, n: usize) -> Self {
        self.concurrency = Some(n);
        self
    }

    pub fn start(self) -> Result<Handle<RemoveOutcome>> {
        let cancel = CancellationToken::new();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let reporter = ProgressReporter::new(tx);

        let concurrency = self.concurrency.unwrap_or_else(default_concurrency);
        let cancel_for_task = cancel.clone();

        let join_handle = tokio::spawn(async move {
            let started = Instant::now();

            if self.filter.is_empty() && !self.allow_unfiltered_delete {
                return Err(Error::RemoveCriteriaRequired);
            }

            let mut outcome = run_remove(
                &self.root,
                &self.filter,
                ScanOptions {
                    max_depth: self.max_depth,
                    follow_symlinks: self.follow_symlinks,
                },
                self.dry_run,
                self.hard_delete,
                &self.batch_config,
                concurrency,
                cancel_for_task,
                reporter,
            )
            .await?;
            outcome.duration = started.elapsed();
            Ok(outcome)
        });

        Ok(Handle::new(join_handle, rx, cancel))
    }
}

/// Scans `root` (via `scan()`, the same profiler pass copy/move use),
/// filters the result, and either previews or dispatches the removal.
///
/// `scan()` is called with `u64::MAX` as its small/large threshold: a
/// delete's cost doesn't scale with bytes the way a copy/move's does
/// (there's nothing to stream), so every matched entry is treated as a
/// batch unit — no per-entry progress sampling, no calibration stream —
/// while still reusing `plan()`/`dispatch()` for concurrency and
/// `ErrorStrategy` handling.
#[allow(clippy::too_many_arguments)]
async fn run_remove(
    root: &Path,
    filter: &RemoveFilter,
    scan_options: ScanOptions,
    dry_run: bool,
    hard_delete: bool,
    config: &BatchConfig,
    concurrency: usize,
    cancel: CancellationToken,
    reporter: ProgressReporter,
) -> Result<RemoveOutcome> {
    let workload = scan(root, u64::MAX, scan_options).await?;
    let excludes = filter.compiled_excludes()?;

    let matched: Vec<Entry> = workload
        .small
        .into_iter()
        .filter(|entry| {
            filter.matches(
                &entry.relative_path,
                entry.size,
                entry.modified,
                excludes.as_ref(),
            )
        })
        .collect();

    if dry_run {
        return Ok(RemoveOutcome {
            previewed: matched,
            ..RemoveOutcome::default()
        });
    }

    let execution_plan = plan(
        Workload {
            small: matched,
            large: Vec::new(),
            directories: Vec::new(),
        },
        config,
    );
    let action = RemoveAction { hard_delete };
    // `dest_root` is meaningless for removal — `RemoveAction` never
    // reads it — but `dispatch()` is shared with copy/move, which do.
    // `root` is passed through unused rather than threading an
    // `Option<&Path>` through `dispatch()` for this one caller.
    let dispatch_outcome = dispatch(
        execution_plan,
        action,
        root,
        config.error_strategy,
        concurrency,
        cancel,
        reporter,
    )
    .await;

    Ok(RemoveOutcome {
        succeeded: dispatch_outcome.succeeded,
        failed: dispatch_outcome.failed,
        stopped_early: dispatch_outcome.stopped_early,
        previewed: Vec::new(),
        duration: std::time::Duration::ZERO,
    })
}

struct RemoveAction {
    hard_delete: bool,
}

impl EntryAction for RemoveAction {
    fn execute<'a>(
        &'a self,
        entry: &'a Entry,
        _dest_root: &'a Path,
    ) -> Pin<Box<dyn Future<Output = Result<EntryOutcome>> + Send + 'a>> {
        Box::pin(async move {
            if self.hard_delete {
                tokio::fs::remove_file(&entry.path)
                    .await
                    .map_err(|e| classify_io_error(e, entry.path.clone(), entry.size))?;
            } else {
                let path = entry.path.clone();
                tokio::task::spawn_blocking(move || trash::delete(&path))
                    .await
                    .expect("trash delete task panicked")
                    .map_err(|source| Error::TrashFailed {
                        path: entry.path.clone(),
                        source,
                    })?;
            }
            Ok(EntryOutcome::Written)
        })
    }

    /// A no-op: removal can't be undone. A hard-deleted file is gone,
    /// and even a trashed one has no reliable cross-platform restore
    /// API (`trash`'s `os_limited::restore_all` covers Linux/Windows
    /// but not macOS). `ErrorStrategy::Undo` still stops the batch on
    /// the first failure the same as it does for copy/move — see
    /// `RemoveBuilder::on_error` — it just can't roll back what already
    /// succeeded.
    fn undo<'a>(
        &'a self,
        _entry: &'a Entry,
        _dest_root: &'a Path,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move { Ok(()) })
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::*;

    fn read_matched(outcome: &RemoveOutcome) -> Vec<PathBuf> {
        let mut paths: Vec<PathBuf> = outcome
            .previewed
            .iter()
            .chain(outcome.succeeded.iter())
            .map(|e| e.path.clone())
            .collect();
        paths.sort();
        paths
    }

    #[tokio::test]
    async fn dry_run_defaults_to_true_and_touches_nothing() {
        let dir = tempdir().unwrap();
        let target = dir.path().join("a.tmp");
        fs::write(&target, b"x").unwrap();

        let outcome = crate::FileEngine::new()
            .remove(dir.path())
            .extensions(["tmp"])
            .start()
            .unwrap()
            .await
            .unwrap();

        assert!(outcome.succeeded.is_empty());
        assert_eq!(read_matched(&outcome), vec![target.clone()]);
        assert!(target.exists(), "dry run must not remove anything");
    }

    #[tokio::test]
    async fn unfiltered_start_is_rejected_without_the_explicit_opt_in() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("a.txt"), b"x").unwrap();

        let result = crate::FileEngine::new()
            .remove(dir.path())
            .dry_run(false)
            .start()
            .unwrap()
            .await;

        assert!(matches!(result, Err(Error::RemoveCriteriaRequired)));
        assert!(dir.path().join("a.txt").exists());
    }

    #[tokio::test]
    async fn hard_delete_removes_matching_files_and_spares_others() {
        let dir = tempdir().unwrap();
        let doomed = dir.path().join("cache.tmp");
        let spared = dir.path().join("keep.txt");
        fs::write(&doomed, b"x").unwrap();
        fs::write(&spared, b"y").unwrap();

        let outcome = crate::FileEngine::new()
            .remove(dir.path())
            .extensions(["tmp"])
            .dry_run(false)
            .hard_delete(true)
            .start()
            .unwrap()
            .await
            .unwrap();

        assert_eq!(outcome.succeeded.len(), 1);
        assert!(outcome.failed.is_empty());
        assert!(!doomed.exists());
        assert!(spared.exists());
    }

    #[tokio::test]
    async fn exclude_pattern_spares_an_otherwise_matching_entry() {
        let dir = tempdir().unwrap();
        let doomed = dir.path().join("a.tmp");
        let excluded = dir.path().join("important.tmp");
        fs::write(&doomed, b"x").unwrap();
        fs::write(&excluded, b"y").unwrap();

        let outcome = crate::FileEngine::new()
            .remove(dir.path())
            .extensions(["tmp"])
            .exclude(["important.tmp"])
            .dry_run(false)
            .hard_delete(true)
            .start()
            .unwrap()
            .await
            .unwrap();

        assert_eq!(outcome.succeeded.len(), 1);
        assert!(!doomed.exists());
        assert!(excluded.exists());
    }

    #[tokio::test]
    async fn size_and_extension_criteria_combine_with_and_semantics() {
        let dir = tempdir().unwrap();
        let too_small = dir.path().join("small.tmp");
        let right_size = dir.path().join("big.tmp");
        fs::write(&too_small, vec![0u8; 1]).unwrap();
        fs::write(&right_size, vec![0u8; 100]).unwrap();

        let outcome = crate::FileEngine::new()
            .remove(dir.path())
            .extensions(["tmp"])
            .min_size(50)
            .start()
            .unwrap()
            .await
            .unwrap();

        assert_eq!(read_matched(&outcome), vec![right_size]);
    }

    #[tokio::test]
    async fn max_depth_excludes_matches_past_the_bound() {
        let dir = tempdir().unwrap();
        let shallow = dir.path().join("a.tmp");
        fs::create_dir(dir.path().join("nested")).unwrap();
        let deep = dir.path().join("nested").join("b.tmp");
        fs::write(&shallow, b"x").unwrap();
        fs::write(&deep, b"y").unwrap();

        let outcome = crate::FileEngine::new()
            .remove(dir.path())
            .extensions(["tmp"])
            .max_depth(1)
            .start()
            .unwrap()
            .await
            .unwrap();

        assert_eq!(read_matched(&outcome), vec![shallow]);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn follow_symlinks_defaults_to_false_and_spares_a_symlinked_directorys_contents() {
        let real_dir = tempdir().unwrap();
        let inside = real_dir.path().join("inside.tmp");
        fs::write(&inside, b"x").unwrap();

        let root_dir = tempdir().unwrap();
        std::os::unix::fs::symlink(real_dir.path(), root_dir.path().join("link")).unwrap();

        let outcome = crate::FileEngine::new()
            .remove(root_dir.path())
            .extensions(["tmp"])
            .start()
            .unwrap()
            .await
            .unwrap();

        assert!(
            outcome.previewed.is_empty(),
            "a symlinked directory should not be walked into by default"
        );
        assert!(inside.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn follow_symlinks_true_makes_a_symlinked_directorys_contents_eligible() {
        let real_dir = tempdir().unwrap();
        let inside = real_dir.path().join("inside.tmp");
        fs::write(&inside, b"x").unwrap();

        let root_dir = tempdir().unwrap();
        std::os::unix::fs::symlink(real_dir.path(), root_dir.path().join("link")).unwrap();

        let outcome = crate::FileEngine::new()
            .remove(root_dir.path())
            .extensions(["tmp"])
            .follow_symlinks(true)
            .dry_run(false)
            .hard_delete(true)
            .start()
            .unwrap()
            .await
            .unwrap();

        assert_eq!(outcome.succeeded.len(), 1);
        assert!(!inside.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn continue_and_collect_keeps_removing_after_one_failure() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempdir().unwrap();
        let locked_dir = dir.path().join("locked");
        fs::create_dir(&locked_dir).unwrap();
        let undeletable = locked_dir.join("a.tmp");
        let real = dir.path().join("b.tmp");
        fs::write(&undeletable, b"x").unwrap();
        fs::write(&real, b"y").unwrap();
        // Removing a file requires write permission on its *containing*
        // directory, not the file itself — this makes `undeletable`'s
        // `remove_file` genuinely fail with `PermissionDenied` while
        // leaving `real` (in an unaffected directory) removable.
        fs::set_permissions(&locked_dir, fs::Permissions::from_mode(0o500)).unwrap();

        let outcome = crate::FileEngine::new()
            .remove(dir.path())
            .extensions(["tmp"])
            .dry_run(false)
            .hard_delete(true)
            .start()
            .unwrap()
            .await
            .unwrap();

        fs::set_permissions(&locked_dir, fs::Permissions::from_mode(0o700)).unwrap();

        assert_eq!(outcome.succeeded.len(), 1);
        assert_eq!(outcome.succeeded[0].path, real);
        assert_eq!(outcome.failed.len(), 1);
        assert_eq!(outcome.failed[0].0.path, undeletable);
        assert!(matches!(
            outcome.failed[0].1,
            Error::PermissionDenied { .. }
        ));
        assert!(!real.exists());
        assert!(undeletable.exists(), "the locked file should remain");
    }
}
