use std::io;
use std::path::{Path, PathBuf};

use tokio_util::sync::CancellationToken;

use crate::error::{Error, Result};
use crate::planner::{BatchConfig, ErrorStrategy, OperationOutcome, StopReason};
use crate::profiler::{probe_fs_caps, Entry, Workload, DEFAULT_SMALL_FILE_THRESHOLD};
use crate::progress::{Progress, ProgressReporter};

use super::default_concurrency;
use super::diff::{diff, DiffStrategy};
use super::pipeline::run_workload_pipeline;

/// `to_copy`'s outcome and `to_delete`'s outcome, reported separately
/// rather than merged: sync's delete phase acts on dest-only orphans, a
/// genuinely different entry set from what the copy phase touched
/// (unlike move's sweep, which deletes the entries it just copied). See
/// dev-docs/design/batching-engine.md, "Sync's outcome shape".
#[derive(Debug, Default)]
pub struct SyncOutcome {
    pub copy: OperationOutcome,
    pub delete: OperationOutcome,
}

pub struct SyncBuilder {
    source: PathBuf,
    dest: PathBuf,
    overwrite: bool,
    preserve_permissions: bool,
    allow_filesystem_integrity_risk: bool,
    small_file_threshold: Option<u64>,
    batch_config: BatchConfig,
    concurrency: Option<usize>,
    diff_strategy: DiffStrategy,
}

impl SyncBuilder {
    pub(crate) fn new(source: impl Into<PathBuf>, dest: impl Into<PathBuf>) -> Self {
        Self {
            source: source.into(),
            dest: dest.into(),
            overwrite: true,
            preserve_permissions: false,
            allow_filesystem_integrity_risk: false,
            small_file_threshold: None,
            batch_config: BatchConfig::default(),
            concurrency: None,
            diff_strategy: DiffStrategy::default(),
        }
    }

    pub fn overwrite(mut self, overwrite: bool) -> Self {
        self.overwrite = overwrite;
        self
    }

    /// Only affects `sync`'s copy phase — see dev-docs/design/permissions.md
    /// for why `diff.rs` doesn't (yet) diff directory permissions at all.
    #[cfg(all(unix, feature = "permissions"))]
    pub fn preserve_permissions(mut self, preserve: bool) -> Self {
        self.preserve_permissions = preserve;
        self
    }

    /// See `CopyBuilder::allow_filesystem_integrity_risk` and
    /// dev-docs/design/filesystem-detection.md. Without this, `sync` fails
    /// before any write happens: `diff()` still runs first (it's
    /// read-only), but `run_workload_pipeline`'s `validate()` call
    /// rejects the copy phase outright — same single destination probe
    /// that feeds `diff.rs`'s mtime tolerance also carries this flag's
    /// answer.
    pub fn allow_filesystem_integrity_risk(mut self, allow: bool) -> Self {
        self.allow_filesystem_integrity_risk = allow;
        self
    }

    pub fn small_file_threshold(mut self, bytes: u64) -> Self {
        self.small_file_threshold = Some(bytes);
        self
    }

    pub fn on_error(mut self, strategy: ErrorStrategy) -> Self {
        self.batch_config.error_strategy = strategy;
        self
    }

    pub fn batch_concurrency(mut self, n: usize) -> Self {
        self.concurrency = Some(n);
        self
    }

    pub fn diff_strategy(mut self, strategy: DiffStrategy) -> Self {
        self.diff_strategy = strategy;
        self
    }

    pub fn start(self) -> Result<crate::handle::Handle<SyncOutcome>> {
        let cancel = CancellationToken::new();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let reporter = ProgressReporter::new(tx);

        let concurrency = self.concurrency.unwrap_or_else(default_concurrency);
        let threshold = self
            .small_file_threshold
            .unwrap_or(DEFAULT_SMALL_FILE_THRESHOLD);
        let cancel_for_task = cancel.clone();

        let join_handle = tokio::spawn(async move {
            sync(
                &self.source,
                &self.dest,
                self.overwrite,
                self.preserve_permissions,
                self.allow_filesystem_integrity_risk,
                threshold,
                &self.batch_config,
                concurrency,
                cancel_for_task,
                reporter,
                self.diff_strategy,
            )
            .await
        });

        Ok(crate::handle::Handle::new(join_handle, rx, cancel))
    }
}

/// 1. Diff `source` against `dest`. 2. Run `to_copy` through
/// `pipeline::run_workload_pipeline` (skips re-scanning — `diff` already
/// produced the entry list). 3. If the copy phase completed without
/// stopping early, delete every `to_delete` entry (dest-only orphans).
/// See dev-docs/design/batching-engine.md, "sync.rs and diff.rs".
///
/// The copy-phase gate is a judgment call the design doc didn't spell
/// out: if the copy phase was aborted, cancelled, or hit a fatal error
/// partway (`stopped_early.is_some()`), the delete phase is skipped
/// entirely rather than removing dest-only orphans while the copy side
/// is in a known-incomplete state — deleting real (if stale) data when
/// the sync itself didn't finish is a worse failure mode than leaving an
/// orphan in place for the next run to catch. Per-entry failures under
/// `ContinueAndCollect` don't set `stopped_early`, so a normal partial
/// failure still lets the delete phase run.
#[allow(clippy::too_many_arguments)]
async fn sync(
    source: &Path,
    dest: &Path,
    overwrite: bool,
    preserve_permissions: bool,
    allow_filesystem_integrity_risk: bool,
    small_file_threshold: u64,
    config: &BatchConfig,
    concurrency: usize,
    cancel: CancellationToken,
    reporter: ProgressReporter,
    diff_strategy: DiffStrategy,
) -> Result<SyncOutcome> {
    // Probed once and shared: `diff.rs` needs `timestamp_granularity` for
    // its mtime tolerance, `run_workload_pipeline` needs the full
    // capabilities for `validate()` — both describe the same destination
    // volume, which hasn't changed between the two uses. See
    // dev-docs/design/filesystem-detection.md, item 4.
    let dest_caps = probe_fs_caps(dest).await?;

    let sync_plan = diff(source, dest, diff_strategy, dest_caps.timestamp_granularity).await?;

    let copy_workload = Workload::partition(sync_plan.to_copy, small_file_threshold);
    let copy = run_workload_pipeline(
        copy_workload,
        dest,
        &dest_caps,
        overwrite,
        preserve_permissions,
        allow_filesystem_integrity_risk,
        config,
        concurrency,
        cancel,
        reporter.clone(),
    )
    .await?;

    let delete = if copy.stopped_early.is_none() {
        delete_sweep(sync_plan.to_delete, config.error_strategy, reporter).await
    } else {
        OperationOutcome::default()
    };

    Ok(SyncOutcome { copy, delete })
}

/// Deletes each dest-only orphan entry directly. Sequential, like
/// `move_path.rs`'s sweep, and for the same reason — deletions are cheap
/// metadata operations, not data transfer, so the syscall-overload
/// concern that motivates batching doesn't apply here.
///
/// Unlike `move_path.rs`'s sweep, there's no source-side copy to restore
/// an orphan from if its deletion fails partway through an
/// `ErrorStrategy::Undo` run — an orphan deletion is not reversible
/// without a backup this design doesn't keep. `Undo` is therefore treated
/// identically to `AbortOnError` here: stop on the triggering failure,
/// attempt no rollback (there being nothing coherent to roll back to).
async fn delete_sweep(
    entries: Vec<Entry>,
    error_strategy: ErrorStrategy,
    reporter: ProgressReporter,
) -> OperationOutcome {
    let mut outcome = OperationOutcome::default();

    reporter.send(Progress::Started {
        bytes_total: None,
        entries_total: entries.len(),
    });

    for entry in entries {
        reporter.send(Progress::EntryStarted {
            entry: entry.clone(),
        });

        match remove_path(&entry.path).await {
            Ok(()) => {
                reporter.send(Progress::EntryCompleted {
                    entry: entry.clone(),
                });
                outcome.succeeded.push(entry);
            }
            Err(err) => {
                reporter.send(Progress::EntryFailed {
                    entry: entry.clone(),
                });

                let fatal = err.is_fatal();
                let reason = if fatal {
                    Some(StopReason::Fatal)
                } else {
                    match error_strategy {
                        ErrorStrategy::ContinueAndCollect => None,
                        ErrorStrategy::AbortOnError | ErrorStrategy::Undo => {
                            Some(StopReason::AbortOnError)
                        }
                    }
                };

                outcome.failed.push((entry, err));

                if let Some(reason) = reason {
                    if outcome.stopped_early.is_none() {
                        outcome.stopped_early = Some(reason);
                    }
                    break;
                }
            }
        }
    }

    outcome
}

async fn remove_path(path: &Path) -> Result<()> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(classify_error(e, path)),
    }
}

fn classify_error(err: io::Error, path: &Path) -> Error {
    match err.kind() {
        io::ErrorKind::NotFound => Error::SourceNotFound {
            path: path.to_path_buf(),
        },
        io::ErrorKind::PermissionDenied => Error::PermissionDenied {
            path: path.to_path_buf(),
        },
        io::ErrorKind::StorageFull => Error::NoSpace {
            needed: 0,
            available: 0,
        },
        _ => Error::Io {
            path: path.to_path_buf(),
            source: err,
        },
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::*;

    #[tokio::test]
    async fn both_phases_run_and_are_reported_independently() {
        let source = tempdir().unwrap();
        let dest = tempdir().unwrap();

        fs::write(source.path().join("new.txt"), b"new").unwrap();
        fs::write(dest.path().join("orphan.txt"), b"stale").unwrap();

        let outcome = sync(
            source.path(),
            dest.path(),
            false,
            false,
            false,
            256,
            &BatchConfig::default(),
            2,
            CancellationToken::new(),
            ProgressReporter::noop(),
            DiffStrategy::default(),
        )
        .await
        .unwrap();

        assert_eq!(outcome.copy.succeeded.len(), 1);
        assert_eq!(
            outcome.copy.succeeded[0].relative_path,
            PathBuf::from("new.txt")
        );
        assert!(outcome.copy.failed.is_empty());

        assert_eq!(outcome.delete.succeeded.len(), 1);
        assert_eq!(
            outcome.delete.succeeded[0].relative_path,
            PathBuf::from("orphan.txt")
        );
        assert!(outcome.delete.failed.is_empty());

        assert_eq!(fs::read(dest.path().join("new.txt")).unwrap(), b"new");
        assert!(!dest.path().join("orphan.txt").exists());
    }

    #[tokio::test]
    async fn copy_failures_and_deletion_failures_dont_cross_contaminate() {
        let source = tempdir().unwrap();
        let dest = tempdir().unwrap();

        // Copy will fail: source file, but a same-name directory already
        // sits at the destination path, so writing the file there fails.
        fs::write(source.path().join("conflict.txt"), b"data").unwrap();
        fs::create_dir(dest.path().join("conflict.txt")).unwrap();

        fs::write(dest.path().join("orphan.txt"), b"stale").unwrap();

        let outcome = sync(
            source.path(),
            dest.path(),
            true,
            false,
            false,
            256,
            &BatchConfig::default(),
            2,
            CancellationToken::new(),
            ProgressReporter::noop(),
            DiffStrategy::default(),
        )
        .await
        .unwrap();

        assert_eq!(outcome.copy.failed.len(), 1);
        assert_eq!(
            outcome.copy.failed[0].0.relative_path,
            PathBuf::from("conflict.txt")
        );

        // Under ContinueAndCollect (the default), the copy phase doesn't
        // stop early even with a per-entry failure, so the delete phase
        // still runs and is unaffected by the copy-side failure.
        assert_eq!(outcome.delete.succeeded.len(), 1);
        assert!(outcome.delete.failed.is_empty());
        assert!(!dest.path().join("orphan.txt").exists());
    }

    #[tokio::test]
    async fn delete_phase_is_skipped_when_copy_phase_stops_early() {
        let source = tempdir().unwrap();
        let dest = tempdir().unwrap();

        fs::write(source.path().join("a.txt"), b"a").unwrap();
        fs::create_dir(dest.path().join("a.txt")).unwrap(); // forces a copy failure
        fs::write(dest.path().join("orphan.txt"), b"stale").unwrap();

        let mut config = BatchConfig::default();
        config.error_strategy = ErrorStrategy::AbortOnError;

        let outcome = sync(
            source.path(),
            dest.path(),
            true,
            false,
            false,
            256,
            &config,
            2,
            CancellationToken::new(),
            ProgressReporter::noop(),
            DiffStrategy::default(),
        )
        .await
        .unwrap();

        assert!(outcome.copy.stopped_early.is_some());
        assert!(outcome.delete.succeeded.is_empty());
        assert!(outcome.delete.failed.is_empty());
        assert!(
            dest.path().join("orphan.txt").exists(),
            "orphan should be left in place when the copy phase didn't complete"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn continue_and_collect_delete_phase_keeps_going_after_one_failure() {
        use std::os::unix::fs::PermissionsExt;

        let dest = tempdir().unwrap();
        let locked_dir = dest.path().join("locked");
        fs::create_dir(&locked_dir).unwrap();
        let a = locked_dir.join("a.txt");
        fs::write(&a, b"a").unwrap();
        let b = dest.path().join("b.txt");
        fs::write(&b, b"b").unwrap();

        let entry_a = Entry {
            path: a.clone(),
            relative_path: PathBuf::from("locked/a.txt"),
            size: 1,
            modified: None,
        };
        let entry_b = Entry {
            path: b.clone(),
            relative_path: PathBuf::from("b.txt"),
            size: 1,
            modified: None,
        };

        fs::set_permissions(&locked_dir, fs::Permissions::from_mode(0o555)).unwrap();
        let outcome = delete_sweep(
            vec![entry_a, entry_b],
            ErrorStrategy::ContinueAndCollect,
            ProgressReporter::noop(),
        )
        .await;
        fs::set_permissions(&locked_dir, fs::Permissions::from_mode(0o755)).unwrap();

        assert_eq!(outcome.failed.len(), 1);
        assert_eq!(outcome.failed[0].0.path, a);
        assert_eq!(outcome.succeeded.len(), 1);
        assert_eq!(outcome.succeeded[0].path, b);
        assert!(a.exists());
        assert!(!b.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn abort_on_error_delete_phase_stops_after_first_failure() {
        use std::os::unix::fs::PermissionsExt;

        let dest = tempdir().unwrap();
        let locked_dir = dest.path().join("locked");
        fs::create_dir(&locked_dir).unwrap();
        let a = locked_dir.join("a.txt");
        fs::write(&a, b"a").unwrap();
        let b = dest.path().join("b.txt");
        fs::write(&b, b"b").unwrap();

        let entry_a = Entry {
            path: a.clone(),
            relative_path: PathBuf::from("locked/a.txt"),
            size: 1,
            modified: None,
        };
        let entry_b = Entry {
            path: b.clone(),
            relative_path: PathBuf::from("b.txt"),
            size: 1,
            modified: None,
        };

        fs::set_permissions(&locked_dir, fs::Permissions::from_mode(0o555)).unwrap();
        let outcome = delete_sweep(
            vec![entry_a, entry_b],
            ErrorStrategy::AbortOnError,
            ProgressReporter::noop(),
        )
        .await;
        fs::set_permissions(&locked_dir, fs::Permissions::from_mode(0o755)).unwrap();

        assert_eq!(outcome.stopped_early, Some(StopReason::AbortOnError));
        assert!(a.exists());
        assert!(
            b.exists(),
            "b comes after the triggering failure and should never be attempted"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn undo_delete_phase_stops_like_abort_on_error_with_no_rollback_attempted() {
        use std::os::unix::fs::PermissionsExt;

        let dest = tempdir().unwrap();
        let locked_dir = dest.path().join("locked");
        fs::create_dir(&locked_dir).unwrap();
        let a = locked_dir.join("a.txt");
        fs::write(&a, b"a").unwrap();
        let b = dest.path().join("b.txt");
        fs::write(&b, b"b").unwrap();

        let entry_a = Entry {
            path: a.clone(),
            relative_path: PathBuf::from("locked/a.txt"),
            size: 1,
            modified: None,
        };
        let entry_b = Entry {
            path: b.clone(),
            relative_path: PathBuf::from("b.txt"),
            size: 1,
            modified: None,
        };

        fs::set_permissions(&locked_dir, fs::Permissions::from_mode(0o555)).unwrap();
        let outcome = delete_sweep(
            vec![entry_a, entry_b],
            ErrorStrategy::Undo,
            ProgressReporter::noop(),
        )
        .await;
        fs::set_permissions(&locked_dir, fs::Permissions::from_mode(0o755)).unwrap();

        assert_eq!(outcome.stopped_early, Some(StopReason::AbortOnError));
        assert!(a.exists());
        assert!(b.exists(), "b was never attempted, nothing to roll back");
    }

    #[tokio::test]
    async fn empty_diff_produces_an_empty_sync_outcome() {
        let source = tempdir().unwrap();
        let dest = tempdir().unwrap();
        let t = std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_000);

        fs::write(source.path().join("same.txt"), b"same").unwrap();
        fs::write(dest.path().join("same.txt"), b"same").unwrap();
        filetime::set_file_mtime(
            source.path().join("same.txt"),
            filetime::FileTime::from_system_time(t),
        )
        .unwrap();
        filetime::set_file_mtime(
            dest.path().join("same.txt"),
            filetime::FileTime::from_system_time(t),
        )
        .unwrap();

        let outcome = sync(
            source.path(),
            dest.path(),
            false,
            false,
            false,
            256,
            &BatchConfig::default(),
            2,
            CancellationToken::new(),
            ProgressReporter::noop(),
            DiffStrategy::default(),
        )
        .await
        .unwrap();

        assert!(outcome.copy.succeeded.is_empty());
        assert!(outcome.delete.succeeded.is_empty());
        assert_eq!(outcome.copy.stopped_early, None);
    }
}
