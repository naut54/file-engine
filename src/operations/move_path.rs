use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Instant;

use tokio_util::sync::CancellationToken;

use crate::error::{classify_io_error, Error, Result};
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
    skip_if_identical: bool,
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
            skip_if_identical: false,
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

    /// Only consulted when `.overwrite(false)` (the default) *and* the
    /// destination already exists: instead of failing with
    /// `Error::DestExists`, compares content and leaves an already-
    /// identical destination alone — the source is still removed, since
    /// that's still what "moved" means, it just skips redundantly
    /// rewriting a destination that already matches. A differing
    /// destination still fails exactly as without this. See
    /// `CopyBuilder::skip_if_identical` for the full rationale; applies
    /// equally to this builder's atomic-rename fast path and its
    /// cross-device fallback.
    #[cfg(feature = "checksum")]
    pub fn skip_if_identical(mut self, skip: bool) -> Self {
        self.skip_if_identical = skip;
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
    /// `CopyBuilder::allow_filesystem_integrity_risk`.
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
            let started = Instant::now();
            let mut outcome = move_path(
                &self.source,
                &self.dest,
                self.overwrite,
                self.skip_if_identical,
                self.preserve_permissions,
                self.allow_filesystem_integrity_risk,
                threshold,
                &self.batch_config,
                concurrency,
                cancel_for_task,
                reporter,
                &TokioRenamer,
            )
            .await?;
            outcome.duration = started.elapsed();
            Ok(outcome)
        });

        Ok(crate::handle::Handle::new(join_handle, rx, cancel))
    }
}

/// Pure classification, unit-testable with synthetic `io::Error` values —
/// no real cross-device filesystem needed.
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

/// 1. If `dest` already exists and `source` is a single file, resolve
///    the conflict up front (see `resolve_existing_dest_conflict`) —
///    `rename(2)` would otherwise silently replace it regardless of
///    `overwrite`. Directory sources are left alone here: `dest`
///    legitimately pre-exists as the directory being moved *into*
///    (matching `CopyAction`'s `dest_root.join(relative_path)`
///    placement, which mirrors contents into an existing directory
///    rather than nesting a new one under it), so a blanket
///    "dest exists" pre-check would reject that normal case. Per-file
///    overwrite conflicts inside a moved directory are still caught
///    correctly, just per-entry, by `CopyAction` in the fallback path
///    below.
/// 2. Attempt a single atomic rename.
/// 3. On cross-device failure, fall back to
///    `pipeline::run_copy_pipeline` (unmodified — no dedicated
///    `EntryAction` for move).
/// 4. Any other rename error surfaces directly.
/// 5. Once the copy phase resolves, run the deferred deletion sweep over
///    `succeeded`, governed by the same `ErrorStrategy`.
#[allow(clippy::too_many_arguments)]
async fn move_path<R: Renamer>(
    source: &Path,
    dest: &Path,
    overwrite: bool,
    skip_if_identical: bool,
    preserve_permissions: bool,
    allow_filesystem_integrity_risk: bool,
    small_file_threshold: u64,
    config: &BatchConfig,
    concurrency: usize,
    cancel: CancellationToken,
    reporter: ProgressReporter,
    renamer: &R,
) -> Result<OperationOutcome> {
    // `rename(2)` fails with `NotFound` if any component of `dest`'s
    // parent chain is missing — not just if `source` is missing — so
    // without this, moving into a not-yet-created destination directory
    // surfaces as a misleading `SourceNotFound` (see the `err` arm
    // below, which blames `source` for every non-cross-device failure)
    // and never reaches the copy-pipeline fallback, which *would* have
    // created it. `create_dir_all` is a no-op when `parent` already
    // exists, so this is safe to run unconditionally on every move.
    if let Some(parent) = dest.parent() {
        if let Err(err) = tokio::fs::create_dir_all(parent).await {
            return Err(classify_io_error(err, dest.to_path_buf(), 0));
        }
    }

    if !overwrite && resolve_existing_dest_conflict(source, dest, skip_if_identical).await? {
        return Ok(OperationOutcome::default());
    }

    match renamer.rename(source, dest).await {
        // Trivially "everything succeeded" without ever enumerating
        // individual entries, so no progress events are emitted either.
        Ok(()) => return Ok(OperationOutcome::default()),
        Err(err) if is_cross_device(&err) => {}
        Err(err) => return Err(classify_io_error(err, source.to_path_buf(), 0)),
    }

    let mut outcome = run_copy_pipeline(
        source,
        dest,
        overwrite,
        skip_if_identical,
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

/// Guards the atomic-rename fast path against `rename(2)`'s native
/// overwrite semantics: on Unix (and Windows' `MoveFileEx` equivalent),
/// `rename(source, dest)` happily replaces an existing destination
/// *file* with no error, so without this check `overwrite=false` is
/// silently unenforced here even though the cross-device fallback below
/// (via `CopyAction`) enforces it correctly. Only applies when `source`
/// is a single file — a directory `dest` pre-existing is the normal
/// "move into" case (see the `move_path` doc comment), and there's no
/// single-file checksum to compare a directory against anyway.
///
/// `Ok(false)` means "no conflict (or not applicable) — proceed with the
/// rename exactly as before", the zero-added-cost path for the common
/// case of moving to a location that doesn't exist yet. `Ok(true)` means
/// the move is already fully resolved (dest was identical, so `source`
/// was removed and nothing else needs to happen) without ever calling
/// `rename`. `Err` is either the conflict itself (`Error::DestExists`)
/// or a genuine I/O failure reaching either file's metadata.
///
/// `pub(crate)` (not private): `move_many.rs` runs this same per-source
/// check ahead of its own rename attempt, for the same reason.
pub(crate) async fn resolve_existing_dest_conflict(
    source: &Path,
    dest: &Path,
    skip_if_identical: bool,
) -> Result<bool> {
    let dest_meta = match tokio::fs::metadata(dest).await {
        Ok(meta) => meta,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(classify_io_error(e, dest.to_path_buf(), 0)),
    };

    let source_meta = tokio::fs::metadata(source)
        .await
        .map_err(|e| classify_io_error(e, source.to_path_buf(), 0))?;
    if !source_meta.is_file() {
        return Ok(false);
    }

    if identical_to_existing(
        source,
        dest,
        source_meta.len(),
        dest_meta.len(),
        skip_if_identical,
    )
    .await?
    {
        tokio::fs::remove_file(source)
            .await
            .map_err(|e| classify_io_error(e, source.to_path_buf(), 0))?;
        return Ok(true);
    }

    Err(Error::DestExists {
        path: dest.to_path_buf(),
    })
}

/// No-op fallback when `checksum` is disabled, so `enabled` (always
/// `false` in that build, since the one builder method that can set it
/// is itself `checksum`-gated) never needs its own `#[cfg]` at the call
/// site — same pattern as `planner::action::CopyAction::identical_to_existing`.
#[cfg(feature = "checksum")]
async fn identical_to_existing(
    source: &Path,
    dest: &Path,
    source_len: u64,
    dest_len: u64,
    enabled: bool,
) -> Result<bool> {
    if !enabled {
        return Ok(false);
    }
    crate::checksum::files_identical(source, dest, source_len, dest_len).await
}

#[cfg(not(feature = "checksum"))]
async fn identical_to_existing(
    _source: &Path,
    _dest: &Path,
    _source_len: u64,
    _dest_len: u64,
    _enabled: bool,
) -> Result<bool> {
    Ok(false)
}

/// Deletes each `succeeded` entry's original source. Sequential — no
/// batching/concurrency of its own, since deletions are cheap metadata
/// operations, not data transfer.
///
/// `pub(crate)` (not private): `move_many.rs` reuses this verbatim over
/// its own merged multi-source outcome — the same "delete every
/// successfully-copied source, roll back on failure under `Undo`" logic
/// applies regardless of whether the entries came from one source tree
/// or several concatenated ones.
pub(crate) async fn sweep(
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
    let copy_action = CopyAction {
        overwrite: true,
        skip_if_identical: false,
    };
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
        Err(e) => Err(classify_io_error(e, entry.path.clone(), 0)),
    }
}

async fn restore_source(entry: &Entry, dest_root: &Path) -> Result<()> {
    let dest_path = dest_root.join(&entry.relative_path);
    tokio::fs::copy(&dest_path, &entry.path)
        .await
        .map_err(|e| classify_io_error(e, entry.path.clone(), 0))?;
    tokio::fs::remove_file(&dest_path)
        .await
        .map_err(|e| classify_io_error(e, dest_path, 0))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use crate::error::Error;

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
            false, // skip_if_identical
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
    async fn same_filesystem_move_creates_missing_dest_parent_dirs() {
        let root = tempdir().unwrap();
        let source = root.path().join("src.txt");
        let dest = root
            .path()
            .join("does")
            .join("not")
            .join("exist")
            .join("dst.txt");
        fs::write(&source, b"hello").unwrap();

        let outcome = move_path(
            &source,
            &dest,
            false,
            false, // skip_if_identical
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

        assert!(outcome.succeeded.is_empty());
        assert!(!source.exists());
        assert_eq!(fs::read(&dest).unwrap(), b"hello");
    }

    #[cfg(feature = "checksum")]
    #[tokio::test]
    async fn same_filesystem_move_without_overwrite_fails_on_a_differing_destination() {
        let root = tempdir().unwrap();
        let source = root.path().join("src.txt");
        let dest = root.path().join("dst.txt");
        fs::write(&source, b"new content").unwrap();
        fs::write(&dest, b"old content").unwrap();

        let result = move_path(
            &source,
            &dest,
            false,
            false, // skip_if_identical
            false,
            false,
            256,
            &BatchConfig::default(),
            2,
            CancellationToken::new(),
            ProgressReporter::noop(),
            &TokioRenamer,
        )
        .await;

        assert!(matches!(result, Err(Error::DestExists { .. })));
        assert!(
            source.exists(),
            "a rejected move must leave source in place"
        );
        assert_eq!(fs::read(&dest).unwrap(), b"old content");
    }

    #[cfg(feature = "checksum")]
    #[tokio::test]
    async fn same_filesystem_move_skips_an_identical_destination_but_still_removes_source() {
        let root = tempdir().unwrap();
        let source = root.path().join("src.txt");
        let dest = root.path().join("dst.txt");
        fs::write(&source, b"same content").unwrap();
        fs::write(&dest, b"same content").unwrap();

        let outcome = move_path(
            &source,
            &dest,
            false,
            true, // skip_if_identical
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

        assert!(outcome.succeeded.is_empty());
        assert!(
            outcome.skipped.is_empty(),
            "the fast path never enumerates entries"
        );
        assert!(
            !source.exists(),
            "the move should still complete by removing the now-redundant source"
        );
        assert_eq!(fs::read(&dest).unwrap(), b"same content");
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
            false, // skip_if_identical
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
            false, // skip_if_identical
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

        let mut outcome = OperationOutcome {
            succeeded: vec![entry_a.clone(), entry_b.clone()],
            ..OperationOutcome::default()
        };

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

        let mut outcome = OperationOutcome {
            succeeded: vec![entry_a, entry_b, entry_c],
            ..OperationOutcome::default()
        };

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
        let mut outcome = OperationOutcome {
            succeeded: vec![entry_b, entry_a],
            ..OperationOutcome::default()
        };

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
