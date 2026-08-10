use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};

use tokio_util::sync::CancellationToken;

use crate::error::{Error, Result};
use crate::planner::{
    BatchConfig, CopyAction, EntryAction, ErrorStrategy, OperationOutcome, StopReason,
};
use crate::profiler::{Entry, DEFAULT_SMALL_FILE_THRESHOLD};
use crate::progress::{Progress, ProgressReporter};

use super::default_concurrency;
use super::pipeline::run_copy_pipeline;

pub struct MoveBuilder {
    source: PathBuf,
    dest: PathBuf,
    overwrite: bool,
    preserve_permissions: bool,
    allow_filesystem_integrity_risk: bool,
    small_file_threshold: Option<u64>,
    batch_config: BatchConfig,
    concurrency: Option<usize>,
}

impl MoveBuilder {
    pub(crate) fn new(source: impl Into<PathBuf>, dest: impl Into<PathBuf>) -> Self {
        Self {
            source: source.into(),
            dest: dest.into(),
            overwrite: false,
            preserve_permissions: false,
            allow_filesystem_integrity_risk: false,
            small_file_threshold: None,
            batch_config: BatchConfig::default(),
            concurrency: None,
        }
    }

    pub fn overwrite(mut self, overwrite: bool) -> Self {
        self.overwrite = overwrite;
        self
    }

    /// Only meaningful for the cross-device fallback (which reuses
    /// `CopyAction`) — the atomic-rename fast path already preserves
    /// everything about the source, permissions included, for free.
    #[cfg(all(unix, feature = "permissions"))]
    pub fn preserve_permissions(mut self, preserve: bool) -> Self {
        self.preserve_permissions = preserve;
        self
    }

    /// Only meaningful for the cross-device fallback, for the same
    /// reason `.preserve_permissions()` is — the atomic-rename fast path
    /// never touches `dest`'s filesystem capabilities at all. See
    /// `CopyBuilder::allow_filesystem_integrity_risk` and
    /// dev-docs/design/filesystem-detection.md.
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

    pub fn start(self) -> Result<crate::handle::Handle<OperationOutcome>> {
        let cancel = CancellationToken::new();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let reporter = ProgressReporter::new(tx);

        let concurrency = self.concurrency.unwrap_or_else(default_concurrency);
        let threshold = self
            .small_file_threshold
            .unwrap_or(DEFAULT_SMALL_FILE_THRESHOLD);
        let cancel_for_task = cancel.clone();

        let join_handle = tokio::spawn(async move {
            move_path(
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
                &TokioRenamer,
            )
            .await
        });

        Ok(crate::handle::Handle::new(join_handle, rx, cancel))
    }
}

/// Pure classification, unit-testable with synthetic `io::Error` values —
/// no real cross-device filesystem needed. See
/// dev-docs/design/batching-engine.md, "Move" / "EXDEV test seam".
pub(crate) fn is_cross_device(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::CrossesDevices
}

/// Injectable rename seam: production code uses `TokioRenamer`, tests
/// inject a fake that deterministically returns a synthetic cross-device
/// error to exercise the fallback wiring without a second filesystem.
pub(crate) trait Renamer {
    async fn rename(&self, source: &Path, dest: &Path) -> io::Result<()>;
}

pub(crate) struct TokioRenamer;

impl Renamer for TokioRenamer {
    async fn rename(&self, source: &Path, dest: &Path) -> io::Result<()> {
        tokio::fs::rename(source, dest).await
    }
}

/// 1. Attempt a single atomic rename, deferring entirely to the OS's
/// native rename semantics — no synthesized top-level overwrite check
/// here, since for a directory move `dest` legitimately pre-exists as the
/// directory being moved *into* (matching `CopyAction`'s
/// `dest_root.join(relative_path)` placement, which mirrors contents into
/// an existing directory rather than nesting a new one under it); a
/// pre-check for "dest exists" would reject that normal case. Per-file
/// overwrite conflicts are still caught correctly, just per-entry, by
/// `CopyAction` in the fallback path below. 2. On cross-device failure,
/// fall back to `pipeline::run_copy_pipeline` (unmodified — no dedicated
/// `EntryAction` for move). 3. Any other rename error surfaces directly.
/// 4. Once the copy phase resolves, run the deferred deletion sweep over
/// `succeeded`, governed by the same `ErrorStrategy`. See
/// dev-docs/design/batching-engine.md, "Move".
#[allow(clippy::too_many_arguments)]
async fn move_path<R: Renamer>(
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
    renamer: &R,
) -> Result<OperationOutcome> {
    match renamer.rename(source, dest).await {
        // Trivially "everything succeeded" without ever enumerating
        // individual entries, so no progress events are emitted either
        // — see dev-docs/design/batching-engine.md, "Move".
        Ok(()) => return Ok(OperationOutcome::default()),
        Err(err) if is_cross_device(&err) => {}
        Err(err) => return Err(classify_error(err, source)),
    }

    let mut outcome = run_copy_pipeline(
        source,
        dest,
        overwrite,
        preserve_permissions,
        allow_filesystem_integrity_risk,
        small_file_threshold,
        config,
        concurrency,
        cancel,
        reporter.clone(),
    )
    .await?;

    sweep(&mut outcome, dest, config.error_strategy, reporter).await;

    Ok(outcome)
}

/// Deletes each `succeeded` entry's original source. Sequential — no
/// batching/concurrency of its own, since deletions are cheap metadata
/// operations, not data transfer.
async fn sweep(
    outcome: &mut OperationOutcome,
    dest_root: &Path,
    error_strategy: ErrorStrategy,
    reporter: ProgressReporter,
) {
    if outcome.succeeded.is_empty() {
        return;
    }

    let entries = outcome.succeeded.clone();
    let mut deleted_paths: HashSet<PathBuf> = HashSet::new();

    reporter.send(Progress::Started {
        bytes_total: None,
        entries_total: entries.len(),
    });

    for entry in &entries {
        reporter.send(Progress::EntryStarted {
            entry: entry.clone(),
        });

        match remove_source(entry).await {
            Ok(()) => {
                reporter.send(Progress::EntryCompleted {
                    entry: entry.clone(),
                });
                deleted_paths.insert(entry.path.clone());
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
                        ErrorStrategy::AbortOnError => Some(StopReason::AbortOnError),
                        ErrorStrategy::Undo => Some(StopReason::Undo),
                    }
                };

                outcome.cleanup_failed.push((entry.clone(), err));

                if let Some(reason) = reason {
                    if outcome.stopped_early.is_none() {
                        outcome.stopped_early = Some(reason);
                    }

                    if matches!(error_strategy, ErrorStrategy::Undo) {
                        rollback(&entries, &deleted_paths, dest_root).await;
                        outcome.succeeded.clear();
                        outcome.cleanup_failed.clear();
                    }

                    break;
                }
            }
        }
    }
}

/// Restores every entry to its pre-operation state: already-deleted
/// sources are restored from the destination copy then that copy is
/// removed; sources that were never touched just have their destination
/// copy removed (identical to `CopyAction::undo`, reused directly rather
/// than reimplemented).
async fn rollback(entries: &[Entry], deleted_paths: &HashSet<PathBuf>, dest_root: &Path) {
    let copy_action = CopyAction { overwrite: true };
    for entry in entries.iter().rev() {
        if deleted_paths.contains(&entry.path) {
            let _ = restore_source(entry, dest_root).await;
        } else {
            let _ = copy_action.undo(entry, dest_root).await;
        }
    }
}

async fn remove_source(entry: &Entry) -> Result<()> {
    match tokio::fs::remove_file(&entry.path).await {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(classify_error(e, &entry.path)),
    }
}

async fn restore_source(entry: &Entry, dest_root: &Path) -> Result<()> {
    let dest_path = dest_root.join(&entry.relative_path);
    tokio::fs::copy(&dest_path, &entry.path)
        .await
        .map_err(|e| classify_error(e, &entry.path))?;
    tokio::fs::remove_file(&dest_path)
        .await
        .map_err(|e| classify_error(e, &dest_path))?;
    Ok(())
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

    #[test]
    fn is_cross_device_true_for_crosses_devices_kind() {
        let err = io::Error::from(io::ErrorKind::CrossesDevices);
        assert!(is_cross_device(&err));
    }

    #[test]
    fn is_cross_device_false_for_other_kinds() {
        for kind in [
            io::ErrorKind::PermissionDenied,
            io::ErrorKind::NotFound,
            io::ErrorKind::Other,
        ] {
            let err = io::Error::from(kind);
            assert!(
                !is_cross_device(&err),
                "{kind:?} should not be classified as cross-device"
            );
        }
    }

    struct AlwaysCrossDevice;
    impl Renamer for AlwaysCrossDevice {
        async fn rename(&self, _source: &Path, _dest: &Path) -> io::Result<()> {
            Err(io::Error::from(io::ErrorKind::CrossesDevices))
        }
    }

    struct AlwaysPermissionDenied;
    impl Renamer for AlwaysPermissionDenied {
        async fn rename(&self, _source: &Path, _dest: &Path) -> io::Result<()> {
            Err(io::Error::from(io::ErrorKind::PermissionDenied))
        }
    }

    // Only referenced by the `#[cfg(unix)]` tests below (they need a
    // deferred-deletion sweep to exercise, which only exists on the
    // cross-device fallback path) — undetected until a Windows
    // cross-compile of `--tests` was actually run.
    #[cfg(unix)]
    fn entry(path: PathBuf, relative_path: PathBuf, size: u64) -> Entry {
        Entry {
            path,
            relative_path,
            size,
            modified: None,
        }
    }

    #[tokio::test]
    async fn same_filesystem_move_uses_rename_and_skips_pipeline() {
        let root = tempdir().unwrap();
        let source = root.path().join("src.txt");
        let dest = root.path().join("dst.txt");
        fs::write(&source, b"hello").unwrap();

        let outcome = move_path(
            &source,
            &dest,
            false,
            false,
            false,
            256,
            &BatchConfig::default(),
            2,
            CancellationToken::new(),
            ProgressReporter::noop(),
            &TokioRenamer,
        )
        .await
        .unwrap();

        assert!(
            outcome.succeeded.is_empty(),
            "fast path doesn't enumerate entries"
        );
        assert!(!source.exists());
        assert_eq!(fs::read(&dest).unwrap(), b"hello");
    }

    #[tokio::test]
    async fn cross_device_fallback_copies_then_deletes_sources() {
        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();
        fs::write(src_dir.path().join("a.txt"), b"a").unwrap();
        fs::write(src_dir.path().join("b.txt"), b"b").unwrap();

        let outcome = move_path(
            src_dir.path(),
            dest_dir.path(),
            false,
            false,
            false,
            256,
            &BatchConfig::default(),
            2,
            CancellationToken::new(),
            ProgressReporter::noop(),
            &AlwaysCrossDevice,
        )
        .await
        .unwrap();

        assert_eq!(outcome.succeeded.len(), 2);
        assert!(outcome.failed.is_empty());
        assert!(outcome.cleanup_failed.is_empty());

        assert!(!src_dir.path().join("a.txt").exists());
        assert!(!src_dir.path().join("b.txt").exists());
        assert_eq!(fs::read(dest_dir.path().join("a.txt")).unwrap(), b"a");
        assert_eq!(fs::read(dest_dir.path().join("b.txt")).unwrap(), b"b");
    }

    #[tokio::test]
    async fn non_cross_device_rename_error_surfaces_directly_without_fallback() {
        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();
        fs::write(src_dir.path().join("a.txt"), b"a").unwrap();

        let result = move_path(
            src_dir.path(),
            dest_dir.path(),
            false,
            false,
            false,
            256,
            &BatchConfig::default(),
            2,
            CancellationToken::new(),
            ProgressReporter::noop(),
            &AlwaysPermissionDenied,
        )
        .await;

        assert!(matches!(result, Err(Error::PermissionDenied { .. })));
        assert!(!dest_dir.path().join("a.txt").exists());
        assert!(src_dir.path().join("a.txt").exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn continue_and_collect_sweep_keeps_deleting_after_one_failure() {
        use std::os::unix::fs::PermissionsExt;

        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();

        let locked_dir = src_dir.path().join("locked");
        fs::create_dir(&locked_dir).unwrap();
        let a = locked_dir.join("a.txt");
        fs::write(&a, b"a").unwrap();

        let b = src_dir.path().join("b.txt");
        fs::write(&b, b"b").unwrap();

        fs::create_dir_all(dest_dir.path().join("locked")).unwrap();
        fs::write(dest_dir.path().join("locked").join("a.txt"), b"a").unwrap();
        fs::write(dest_dir.path().join("b.txt"), b"b").unwrap();

        let entry_a = entry(a.clone(), PathBuf::from("locked/a.txt"), 1);
        let entry_b = entry(b.clone(), PathBuf::from("b.txt"), 1);

        let mut outcome = OperationOutcome::default();
        outcome.succeeded = vec![entry_a.clone(), entry_b.clone()];

        // unlink needs write+execute on the containing directory, so
        // locking it down makes deleting `a` fail with permission denied.
        fs::set_permissions(&locked_dir, fs::Permissions::from_mode(0o555)).unwrap();
        sweep(
            &mut outcome,
            dest_dir.path(),
            ErrorStrategy::ContinueAndCollect,
            ProgressReporter::noop(),
        )
        .await;
        fs::set_permissions(&locked_dir, fs::Permissions::from_mode(0o755)).unwrap();

        assert_eq!(outcome.cleanup_failed.len(), 1);
        assert_eq!(outcome.cleanup_failed[0].0.path, a);
        assert!(
            a.exists(),
            "a's deletion should have failed, leaving it in place"
        );
        assert!(!b.exists(), "b's deletion should still have succeeded");
        assert_eq!(outcome.stopped_early, None);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn abort_on_error_sweep_stops_after_first_deletion_failure() {
        use std::os::unix::fs::PermissionsExt;

        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();

        let locked_dir = src_dir.path().join("locked");
        fs::create_dir(&locked_dir).unwrap();
        let a = locked_dir.join("a.txt");
        fs::write(&a, b"a").unwrap();

        let b = src_dir.path().join("b.txt");
        fs::write(&b, b"b").unwrap();
        let c = src_dir.path().join("c.txt");
        fs::write(&c, b"c").unwrap();

        fs::write(dest_dir.path().join("b.txt"), b"b").unwrap();
        fs::write(dest_dir.path().join("c.txt"), b"c").unwrap();

        let entry_a = entry(a.clone(), PathBuf::from("locked/a.txt"), 1);
        let entry_b = entry(b.clone(), PathBuf::from("b.txt"), 1);
        let entry_c = entry(c.clone(), PathBuf::from("c.txt"), 1);

        let mut outcome = OperationOutcome::default();
        outcome.succeeded = vec![entry_a, entry_b, entry_c];

        fs::set_permissions(&locked_dir, fs::Permissions::from_mode(0o555)).unwrap();
        sweep(
            &mut outcome,
            dest_dir.path(),
            ErrorStrategy::AbortOnError,
            ProgressReporter::noop(),
        )
        .await;
        fs::set_permissions(&locked_dir, fs::Permissions::from_mode(0o755)).unwrap();

        assert_eq!(outcome.stopped_early, Some(StopReason::AbortOnError));
        assert!(a.exists());
        assert!(
            b.exists(),
            "b comes after the triggering failure, so it should never be attempted"
        );
        assert!(
            c.exists(),
            "c comes after the triggering failure, so it should never be attempted"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn undo_sweep_restores_everything_on_deletion_failure() {
        use std::os::unix::fs::PermissionsExt;

        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();

        let locked_dir = src_dir.path().join("locked");
        fs::create_dir(&locked_dir).unwrap();
        let a = locked_dir.join("a.txt");
        fs::write(&a, b"a").unwrap();

        let b = src_dir.path().join("b.txt");
        fs::write(&b, b"b").unwrap();

        fs::create_dir_all(dest_dir.path().join("locked")).unwrap();
        fs::write(dest_dir.path().join("locked").join("a.txt"), b"a").unwrap();
        fs::write(dest_dir.path().join("b.txt"), b"b").unwrap();

        let entry_a = entry(a.clone(), PathBuf::from("locked/a.txt"), 1);
        let entry_b = entry(b.clone(), PathBuf::from("b.txt"), 1);

        // b first (its deletion succeeds), then a (its deletion fails and
        // triggers rollback of everything, including b).
        let mut outcome = OperationOutcome::default();
        outcome.succeeded = vec![entry_b, entry_a];

        fs::set_permissions(&locked_dir, fs::Permissions::from_mode(0o555)).unwrap();
        sweep(
            &mut outcome,
            dest_dir.path(),
            ErrorStrategy::Undo,
            ProgressReporter::noop(),
        )
        .await;
        fs::set_permissions(&locked_dir, fs::Permissions::from_mode(0o755)).unwrap();

        assert!(outcome.succeeded.is_empty());
        assert!(outcome.cleanup_failed.is_empty());
        assert_eq!(outcome.stopped_early, Some(StopReason::Undo));

        assert!(
            a.exists(),
            "a's source was never removed, since its deletion failed"
        );
        assert!(b.exists(), "b's source should have been restored from dest");
        assert_eq!(fs::read(&b).unwrap(), b"b");

        assert!(!dest_dir.path().join("locked").join("a.txt").exists());
        assert!(!dest_dir.path().join("b.txt").exists());
    }
}
