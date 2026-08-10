use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::error::{Error, Result};
use crate::planner::{
    dispatch, plan, BatchConfig, CopyAction, ErrorStrategy, OperationOutcome, StopReason,
};
use crate::profiler::{
    probe_fs_caps, scan, validate, DirEntry, Entry, FilesystemCapabilities, Workload,
};
use crate::progress::{Progress, ProgressReporter};

/// Shared orchestration used by both `copy` and `move_path`'s
/// cross-device fallback: Profiler -> Planner -> Dispatcher(CopyAction).
/// Kept separate from `copy.rs` so neither operation file depends on the
/// other's internals.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_copy_pipeline(
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
) -> Result<OperationOutcome> {
    let workload = scan(source, small_file_threshold).await?;
    let dest_caps = probe_fs_caps(dest).await?;
    run_workload_pipeline(
        workload,
        dest,
        &dest_caps,
        overwrite,
        preserve_permissions,
        allow_filesystem_integrity_risk,
        config,
        concurrency,
        cancel,
        reporter,
    )
    .await
}

/// Plans and dispatches an already-built `Workload` directly, skipping
/// the scan step. `sync`'s `diff.rs` already produced the entry list that
/// needs copying — re-scanning `source` from disk to rediscover the same
/// entries would be redundant (and wrong, since diff's list is a filtered
/// subset of source, not all of it).
///
/// Takes `dest_caps` as a parameter rather than probing internally:
/// `sync`'s pipeline needs the same probe result for `diff.rs`'s mtime
/// tolerance — probing twice per `sync` call would just be redundant
/// syscalls against a destination that hasn't changed between the two
/// calls, so the one caller that already has a probe (`sync.rs`) passes it
/// straight through, and `run_copy_pipeline` (which has no other reason to
/// probe) does the single probe itself.
///
/// Fallible (unlike before filesystem-capability detection was wired in):
/// the write-integrity risk `validate()` can surface now propagates as a
/// genuine `Err`.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_workload_pipeline(
    mut workload: Workload,
    dest: &Path,
    dest_caps: &FilesystemCapabilities,
    overwrite: bool,
    preserve_permissions: bool,
    allow_filesystem_integrity_risk: bool,
    config: &BatchConfig,
    concurrency: usize,
    cancel: CancellationToken,
    reporter: ProgressReporter,
) -> Result<OperationOutcome> {
    let validation = validate(&mut workload, dest_caps, allow_filesystem_integrity_risk)?;

    // Taken out before `workload` is moved into `plan()`, which only
    // consumes `small`/`large` — directories aren't part of batching at
    // all, only the two directory-focused steps below.
    let directories = std::mem::take(&mut workload.directories);

    let mut outcome = OperationOutcome {
        failed: validation.rejected_entries,
        ..OperationOutcome::default()
    };
    let mut directories_failed: Vec<(PathBuf, Error)> = validation
        .rejected_directories
        .into_iter()
        .map(|(dir, err)| (dest_path_for(&dir, dest), err))
        .collect();

    // Case-collisions/oversized-files/reserved-names are per-entry
    // failures governed by `ErrorStrategy`, same as any failure
    // `dispatch()` itself would produce — not a bespoke
    // block-or-allow toggle. `ContinueAndCollect` proceeds with
    // whatever `validate()` left in the (now-smaller) workload;
    // `AbortOnError`/`Undo` stop before any work starts at all, the same
    // as if the first entry `dispatch()` touched had failed under that
    // strategy.
    let had_rejections = !outcome.failed.is_empty() || !directories_failed.is_empty();
    if had_rejections {
        let stop_reason = match config.error_strategy {
            ErrorStrategy::ContinueAndCollect => None,
            ErrorStrategy::AbortOnError => Some(StopReason::AbortOnError),
            ErrorStrategy::Undo => Some(StopReason::Undo),
        };
        if let Some(stop_reason) = stop_reason {
            outcome.stopped_early = Some(stop_reason);
            outcome.directories_failed = directories_failed;
            return Ok(outcome);
        }
    }

    // Unconditional, cross-platform, independent of `preserve_permissions`:
    // `CopyAction` only calls `create_dir_all` when copying a *file*
    // (using that file's parent path), so a directory subtree containing
    // no files anywhere inside it — an empty directory, or one that only
    // contains further empty directories — would otherwise never get
    // created at the destination at all, silently, with no failure
    // reported. Found via a real 4.4GB/36k-file copy where several
    // legitimately-empty legacy-app directories went missing with
    // `failed: 0`. Runs before dispatch: creating a directory with
    // default permissions first doesn't conflict with later tightening
    // them via `apply_directory_permissions`, which still has to run
    // after files are written for the reason documented on it below.
    //
    // Only directories with *no file anywhere in their subtree* actually
    // need an explicit call here — `create_dir_all` on a file's parent
    // already creates every ancestor directory as a side effect, so a
    // directory that's an ancestor of at least one (still-going-to-be-
    // dispatched) file gets created for free during the normal copy.
    // For a typical tree this cuts the pass from "every directory" down
    // to just the handful that are genuinely empty — the exact case this
    // fix exists for in the first place. Found necessary after a real
    // run against a USB-connected exFAT drive: bounding concurrency
    // alone (an earlier fix attempt) didn't help — per-directory latency
    // actually got slightly *worse* under concurrency, pointing at
    // serialization somewhere below this crate (the exFAT driver or the
    // USB mass-storage stack), not a lack of parallelism on our end —
    // so reducing the actual number of syscalls issued is what matters,
    // not how they're scheduled.
    let covered = directories_covered_by_files(&workload.small, &workload.large);
    let dirs_needing_creation: Vec<DirEntry> = directories
        .iter()
        .filter(|dir| !covered.contains(&dir.relative_path))
        .cloned()
        .collect();
    directories_failed.extend(
        ensure_directories_exist(&dirs_needing_creation, dest, concurrency, &reporter).await,
    );

    let execution_plan = plan(workload, config);
    let action = CopyAction { overwrite };
    let dispatch_outcome = dispatch(
        execution_plan,
        action,
        dest,
        config.error_strategy,
        concurrency,
        cancel,
        reporter,
    )
    .await;

    outcome.succeeded = dispatch_outcome.succeeded;
    outcome.failed.extend(dispatch_outcome.failed);
    outcome.cleanup_failed = dispatch_outcome.cleanup_failed;
    outcome.stopped_early = dispatch_outcome.stopped_early;

    // No `#[cfg]` on this block itself — `preserve_permissions` can only
    // ever be `true` when `.preserve_permissions()` exists to set it
    // (Unix + `permissions` feature), so the body is unreachable
    // otherwise; but it still needs to *compile* unconditionally, which
    // is what the no-op fallback `apply_directory_permissions` below is
    // for — keeps `preserve_permissions`/`directories`/`mut outcome`
    // genuinely used regardless of feature flags.
    if preserve_permissions && outcome.stopped_early.is_none() {
        directories_failed.extend(apply_directory_permissions(&directories, dest).await);
    }

    outcome.directories_failed = directories_failed;
    Ok(outcome)
}

/// The destination path a `DirEntry` (or any other captured
/// `relative_path`) maps to — shared by every directory-focused step
/// below and by `validate()`'s rejected-directory reporting, rather than
/// re-deriving "empty relative_path means the destination root itself"
/// in four places.
fn dest_path_for(dir: &DirEntry, dest_root: &Path) -> PathBuf {
    if dir.relative_path.as_os_str().is_empty() {
        dest_root.to_path_buf()
    } else {
        dest_root.join(&dir.relative_path)
    }
}

/// Every directory (relative path, including the root as an empty
/// `PathBuf`) that will get created as a side effect of dispatching at
/// least one of `small`/`large` — i.e. every proper ancestor of some
/// file's `relative_path`. `create_dir_all` creates a file's entire
/// missing ancestor chain, not just its immediate parent, so once one
/// entry marks a directory as covered, every shallower ancestor is
/// necessarily covered too — the early `break` below stops walking as
/// soon as it hits a directory a previous entry already covered, since
/// re-walking the same shared prefix for every file would be wasted
/// work at this scale (tens of thousands of entries).
fn directories_covered_by_files(small: &[Entry], large: &[Entry]) -> HashSet<PathBuf> {
    let mut covered = HashSet::new();
    for entry in small.iter().chain(large) {
        let mut current = entry.relative_path.parent();
        while let Some(dir) = current {
            if !covered.insert(dir.to_path_buf()) {
                break;
            }
            current = dir.parent();
        }
    }
    covered
}

/// Ensures every directory the Profiler discovered actually exists at the
/// destination, including the scanned root itself and directories with
/// no files anywhere in their subtree. Best-effort in the same spirit as
/// `apply_directory_permissions`: continues through every directory
/// regardless of individual failures, since a failure here is usually
/// about to be independently reproduced (and reported) by whichever file
/// entries would have landed inside it anyway.
///
/// Bounded-concurrent (same `concurrency` the dispatcher uses), not
/// sequential — a real run against a USB-connected exFAT drive with
/// ~7,700 directories spent about a minute here, one `create_dir_all`
/// awaited at a time, before any progress was visible at all (this
/// entire pass runs *before* `dispatch()`'s own `Progress::Started`).
/// Concurrent `create_dir_all` calls across overlapping parent paths are
/// safe — its create-if-missing behavior already tolerates the race,
/// the same fact `planner::action::tests::concurrent_parent_dir_creation_both_succeed`
/// already verifies for `CopyAction`'s own concurrent directory creation.
async fn ensure_directories_exist(
    directories: &[DirEntry],
    dest_root: &Path,
    concurrency: usize,
    reporter: &ProgressReporter,
) -> Vec<(PathBuf, Error)> {
    reporter.send(Progress::DirectoriesStarted {
        total: directories.len(),
    });

    let semaphore = Arc::new(Semaphore::new(concurrency.max(1)));
    let failures = Arc::new(Mutex::new(Vec::new()));
    let mut join_set: JoinSet<()> = JoinSet::new();

    for dir in directories {
        let dest_path = dest_path_for(dir, dest_root);
        let permit = Arc::clone(&semaphore)
            .acquire_owned()
            .await
            .expect("semaphore closed");
        let failures = Arc::clone(&failures);
        let reporter = reporter.clone();

        join_set.spawn(async move {
            let _permit = permit;
            match tokio::fs::create_dir_all(&dest_path).await {
                Ok(()) => reporter.send(Progress::DirectoryCompleted { path: dest_path }),
                Err(err) => {
                    reporter.send(Progress::DirectoryFailed {
                        path: dest_path.clone(),
                    });
                    failures
                        .lock()
                        .unwrap()
                        .push((dest_path.clone(), classify_error(err, &dest_path)));
                }
            }
        });
    }

    while let Some(result) = join_set.join_next().await {
        let _ = result;
    }

    Arc::try_unwrap(failures)
        .unwrap_or_else(|_| {
            panic!("ensure_directories_exist: failures has outstanding references after join")
        })
        .into_inner()
        .unwrap()
}

/// Best-effort: always runs through every directory regardless of
/// individual failures, never sets `stopped_early`. Applied after every
/// file entry has already been dispatched, never before: a source directory
/// whose mode doesn't permit writes (e.g. `0o500`) would otherwise lock the
/// copy out of its own destination if applied first.
#[cfg(all(unix, feature = "permissions"))]
async fn apply_directory_permissions(
    directories: &[DirEntry],
    dest_root: &Path,
) -> Vec<(PathBuf, Error)> {
    use std::os::unix::fs::PermissionsExt;

    let mut failures = Vec::new();
    for dir in directories {
        let Some(mode) = dir.mode else { continue };
        let dest_path = dest_path_for(dir, dest_root);
        if let Err(err) =
            tokio::fs::set_permissions(&dest_path, std::fs::Permissions::from_mode(mode)).await
        {
            failures.push((dest_path.clone(), classify_error(err, &dest_path)));
        }
    }
    failures
}

/// Unreachable at runtime (`preserve_permissions` can't be `true` without
/// Unix + `permissions`), but needs to exist so the call site above
/// compiles regardless of feature flags.
#[cfg(not(all(unix, feature = "permissions")))]
async fn apply_directory_permissions(
    _directories: &[DirEntry],
    _dest_root: &Path,
) -> Vec<(PathBuf, Error)> {
    Vec::new()
}

fn classify_error(err: std::io::Error, path: &Path) -> Error {
    match err.kind() {
        std::io::ErrorKind::NotFound => Error::SourceNotFound {
            path: path.to_path_buf(),
        },
        std::io::ErrorKind::PermissionDenied => Error::PermissionDenied {
            path: path.to_path_buf(),
        },
        std::io::ErrorKind::StorageFull => Error::NoSpace {
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

    use crate::error::Error;
    use crate::planner::ErrorStrategy;
    #[cfg(all(unix, feature = "permissions"))]
    use crate::profiler::DirEntry;

    use super::*;

    #[tokio::test]
    async fn copies_a_single_file() {
        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();

        let src_file = src_dir.path().join("file.txt");
        fs::write(&src_file, b"hello world").unwrap();

        let outcome = run_copy_pipeline(
            &src_file,
            dest_dir.path(),
            false,
            false,
            false,
            256,
            &BatchConfig::default(),
            2,
            CancellationToken::new(),
            ProgressReporter::noop(),
        )
        .await
        .unwrap();

        assert_eq!(outcome.succeeded.len(), 1);
        assert!(outcome.failed.is_empty());
        assert_eq!(
            fs::read(dest_dir.path().join("file.txt")).unwrap(),
            b"hello world"
        );
    }

    #[tokio::test]
    async fn copies_a_directory_tree_end_to_end() {
        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();

        // A mix of small and large-relative-to-threshold files, nested,
        // to exercise the profiler's classification, the planner's
        // batching, and the dispatcher's fan-out together.
        fs::create_dir_all(src_dir.path().join("nested")).unwrap();
        fs::write(src_dir.path().join("a.txt"), vec![1u8; 10]).unwrap();
        fs::write(src_dir.path().join("b.txt"), vec![2u8; 10]).unwrap();
        fs::write(
            src_dir.path().join("nested").join("big.bin"),
            vec![3u8; 1000],
        )
        .unwrap();

        let config = BatchConfig {
            max_bytes_per_batch: 1024,
            ..BatchConfig::default()
        };

        let outcome = run_copy_pipeline(
            src_dir.path(),
            dest_dir.path(),
            false,
            false,
            false,
            100, // threshold: a.txt/b.txt are small, big.bin is large
            &config,
            2,
            CancellationToken::new(),
            ProgressReporter::noop(),
        )
        .await
        .unwrap();

        assert_eq!(outcome.succeeded.len(), 3);
        assert!(outcome.failed.is_empty());

        assert_eq!(
            fs::read(dest_dir.path().join("a.txt")).unwrap(),
            vec![1u8; 10]
        );
        assert_eq!(
            fs::read(dest_dir.path().join("b.txt")).unwrap(),
            vec![2u8; 10]
        );
        assert_eq!(
            fs::read(dest_dir.path().join("nested").join("big.bin")).unwrap(),
            vec![3u8; 1000]
        );
    }

    #[cfg(all(unix, feature = "permissions"))]
    #[tokio::test]
    async fn preserve_permissions_true_preserves_directory_modes() {
        use std::os::unix::fs::PermissionsExt;

        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();

        let subdir = src_dir.path().join("nested");
        fs::create_dir(&subdir).unwrap();
        fs::set_permissions(&subdir, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(subdir.join("a.txt"), b"a").unwrap();

        let outcome = run_copy_pipeline(
            src_dir.path(),
            dest_dir.path(),
            true,
            true,
            false,
            256,
            &BatchConfig::default(),
            2,
            CancellationToken::new(),
            ProgressReporter::noop(),
        )
        .await
        .unwrap();

        assert!(outcome.directories_failed.is_empty());
        let dest_subdir_mode = fs::metadata(dest_dir.path().join("nested"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(dest_subdir_mode, 0o700);
    }

    #[cfg(all(unix, feature = "permissions"))]
    #[tokio::test]
    async fn preserve_permissions_true_preserves_root_mode_when_newly_created() {
        use std::os::unix::fs::PermissionsExt;

        let src_dir = tempdir().unwrap();
        fs::set_permissions(src_dir.path(), fs::Permissions::from_mode(0o750)).unwrap();
        fs::write(src_dir.path().join("a.txt"), b"a").unwrap();

        let out_dir = tempdir().unwrap();
        let dest_root = out_dir.path().join("new_root"); // does not exist yet

        let outcome = run_copy_pipeline(
            src_dir.path(),
            &dest_root,
            false,
            true,
            false,
            256,
            &BatchConfig::default(),
            2,
            CancellationToken::new(),
            ProgressReporter::noop(),
        )
        .await
        .unwrap();

        assert!(outcome.directories_failed.is_empty());
        let dest_root_mode = fs::metadata(&dest_root).unwrap().permissions().mode() & 0o777;
        assert_eq!(dest_root_mode, 0o750);
    }

    #[cfg(all(unix, feature = "permissions"))]
    #[tokio::test]
    async fn preserve_permissions_false_skips_directory_pass() {
        use std::os::unix::fs::PermissionsExt;

        let src_dir = tempdir().unwrap();
        let subdir = src_dir.path().join("nested");
        fs::create_dir(&subdir).unwrap();
        fs::set_permissions(&subdir, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(subdir.join("a.txt"), b"a").unwrap();

        let dest_dir = tempdir().unwrap();

        let outcome = run_copy_pipeline(
            src_dir.path(),
            dest_dir.path(),
            true,
            false,
            false,
            256,
            &BatchConfig::default(),
            2,
            CancellationToken::new(),
            ProgressReporter::noop(),
        )
        .await
        .unwrap();

        assert!(
            outcome.directories_failed.is_empty(),
            "pass never ran, so nothing failed either"
        );
        let dest_subdir_mode = fs::metadata(dest_dir.path().join("nested"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_ne!(
            dest_subdir_mode, 0o700,
            "directory pass should not have run"
        );
    }

    #[cfg(all(unix, feature = "permissions"))]
    #[tokio::test]
    async fn directory_pass_is_skipped_when_copy_phase_stopped_early() {
        use std::os::unix::fs::PermissionsExt;

        let src_dir = tempdir().unwrap();
        let subdir = src_dir.path().join("nested");
        fs::create_dir(&subdir).unwrap();
        fs::set_permissions(&subdir, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(subdir.join("a.txt"), b"a").unwrap();
        fs::write(src_dir.path().join("b.txt"), b"b").unwrap();

        let dest_dir = tempdir().unwrap();
        // Forces a copy failure for b.txt: a directory already sits where
        // the file needs to go.
        fs::create_dir(dest_dir.path().join("b.txt")).unwrap();

        let config = BatchConfig {
            error_strategy: ErrorStrategy::AbortOnError,
            ..BatchConfig::default()
        };

        let outcome = run_copy_pipeline(
            src_dir.path(),
            dest_dir.path(),
            false,
            true,
            false,
            256,
            &config,
            1,
            CancellationToken::new(),
            ProgressReporter::noop(),
        )
        .await
        .unwrap();

        assert!(outcome.stopped_early.is_some());
        assert!(
            outcome.directories_failed.is_empty(),
            "pass should never have been attempted"
        );

        // Whether `nested/` even got created at dest depends on dispatch
        // ordering (AbortOnError may stop before its file is attempted at
        // all) — only assert on its mode if it exists.
        if let Ok(metadata) = fs::metadata(dest_dir.path().join("nested")) {
            assert_ne!(
                metadata.permissions().mode() & 0o777,
                0o700,
                "directory pass should not have run"
            );
        }
    }

    #[cfg(all(unix, feature = "permissions"))]
    #[tokio::test]
    async fn directory_permissions_pass_continues_after_one_failure() {
        use std::os::unix::fs::PermissionsExt;

        let dest_dir = tempdir().unwrap();
        fs::create_dir(dest_dir.path().join("real")).unwrap();

        let missing = DirEntry {
            path: PathBuf::from("irrelevant"),
            relative_path: PathBuf::from("does-not-exist"),
            mode: Some(0o700),
        };
        let real = DirEntry {
            path: PathBuf::from("irrelevant"),
            relative_path: PathBuf::from("real"),
            mode: Some(0o700),
        };

        let failures = apply_directory_permissions(&[missing, real], dest_dir.path()).await;

        assert_eq!(failures.len(), 1);
        assert!(matches!(failures[0].1, Error::SourceNotFound { .. }));

        let real_mode = fs::metadata(dest_dir.path().join("real"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(real_mode, 0o700);
    }

    // The two tests below exercise the real `probe()` -> `validate()` ->
    // `ErrorStrategy` wiring end-to-end rather than a mocked capability,
    // which means they need a destination volume that genuinely is
    // case-insensitive — with a case-sensitive one there is no collision
    // to detect, and the assertions describe something that legitimately
    // never happened. That holds for the default macOS boot volume
    // (empirically confirmed via `pathconf(_PC_CASE_SENSITIVE)` while
    // building `fs_caps`) but not for Linux's ext4/tmpfs, so each probes
    // the destination first and skips when it can't host the scenario.
    //
    // Skipping is deliberate over mocking the capability: a mocked
    // `FilesystemCapabilities` would run everywhere while testing
    // strictly less — it would no longer prove that a real `probe()`
    // reports case-insensitivity in a form `validate()` acts on, which
    // is the only part of this that isn't already covered by
    // `validate.rs`'s unit tests.
    //
    // Built as a hand-constructed `Workload` fed straight to
    // `run_workload_pipeline`, bypassing `scan()`, rather than two real
    // `Report.txt`/`report.txt` source files: on this same
    // case-insensitive volume, the *source* side would collide too —
    // the second `fs::write` would just overwrite the first, leaving
    // only one real file to scan and nothing to detect. Both synthetic
    // entries point at the same one real source file, which is fine —
    // `validate()` rejects both before either would ever be read.
    //
    // The oversized-file and reserved-name checks aren't similarly
    // testable here — they'd need an actual FAT32/exFAT or
    // Windows-family destination, which isn't reliably available —
    // those stay covered at the `validate.rs` unit level only.

    #[tokio::test]
    async fn case_collision_is_rejected_but_other_entries_still_copy_under_continue_and_collect() {
        use crate::profiler::Entry;

        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();

        let shared_source = src_dir.path().join("source.txt");
        fs::write(&shared_source, b"shared").unwrap();
        let unrelated_source = src_dir.path().join("unrelated_source.txt");
        fs::write(&unrelated_source, b"three").unwrap();

        let workload = Workload {
            small: vec![
                Entry {
                    path: shared_source.clone(),
                    relative_path: PathBuf::from("Report.txt"),
                    size: 6,
                    modified: None,
                },
                Entry {
                    path: shared_source,
                    relative_path: PathBuf::from("report.txt"),
                    size: 6,
                    modified: None,
                },
                Entry {
                    path: unrelated_source,
                    relative_path: PathBuf::from("unrelated.txt"),
                    size: 5,
                    modified: None,
                },
            ],
            large: vec![],
            directories: vec![],
        };

        let dest_caps = probe_fs_caps(dest_dir.path()).await.unwrap();
        if dest_caps.case_sensitive {
            eprintln!(
                "skipped: destination volume is case-sensitive, so there is no case collision to detect"
            );
            return;
        }

        let outcome = run_workload_pipeline(
            workload,
            dest_dir.path(),
            &dest_caps,
            false,
            false,
            false,
            &BatchConfig::default(), // default: ErrorStrategy::ContinueAndCollect
            2,
            CancellationToken::new(),
            ProgressReporter::noop(),
        )
        .await
        .unwrap();

        assert_eq!(outcome.succeeded.len(), 1);
        assert_eq!(
            outcome.succeeded[0].relative_path,
            PathBuf::from("unrelated.txt")
        );
        assert_eq!(outcome.failed.len(), 2);
        for (_, err) in &outcome.failed {
            assert!(matches!(err, Error::CaseCollision { .. }));
        }
        assert_eq!(outcome.stopped_early, None);
        assert_eq!(
            fs::read(dest_dir.path().join("unrelated.txt")).unwrap(),
            b"three"
        );
    }

    #[tokio::test]
    async fn case_collision_aborts_before_any_dispatch_under_abort_on_error() {
        use crate::profiler::Entry;

        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();

        let shared_source = src_dir.path().join("source.txt");
        fs::write(&shared_source, b"shared").unwrap();
        let unrelated_source = src_dir.path().join("unrelated_source.txt");
        fs::write(&unrelated_source, b"three").unwrap();

        let workload = Workload {
            small: vec![
                Entry {
                    path: shared_source.clone(),
                    relative_path: PathBuf::from("Report.txt"),
                    size: 6,
                    modified: None,
                },
                Entry {
                    path: shared_source,
                    relative_path: PathBuf::from("report.txt"),
                    size: 6,
                    modified: None,
                },
                Entry {
                    path: unrelated_source,
                    relative_path: PathBuf::from("unrelated.txt"),
                    size: 5,
                    modified: None,
                },
            ],
            large: vec![],
            directories: vec![],
        };

        let config = BatchConfig {
            error_strategy: ErrorStrategy::AbortOnError,
            ..BatchConfig::default()
        };

        let dest_caps = probe_fs_caps(dest_dir.path()).await.unwrap();
        if dest_caps.case_sensitive {
            eprintln!(
                "skipped: destination volume is case-sensitive, so there is no case collision to detect"
            );
            return;
        }

        let outcome = run_workload_pipeline(
            workload,
            dest_dir.path(),
            &dest_caps,
            false,
            false,
            false,
            &config,
            2,
            CancellationToken::new(),
            ProgressReporter::noop(),
        )
        .await
        .unwrap();

        assert!(
            outcome.succeeded.is_empty(),
            "nothing should have been dispatched at all"
        );
        assert_eq!(outcome.failed.len(), 2);
        assert_eq!(
            outcome.stopped_early,
            Some(crate::planner::StopReason::AbortOnError)
        );
        assert!(
            !dest_dir.path().join("unrelated.txt").exists(),
            "an unrelated valid entry should not have been copied either — abort happens before dispatch starts"
        );
    }

    // A synthetic risky `FilesystemCapabilities`, rather than a real
    // exFAT-on-macOS destination — not reliably available in a test
    // environment. `run_workload_pipeline` taking `dest_caps` as a
    // parameter (rather than probing internally) is exactly what makes
    // this injectable without a second `EntryAction`/`Renamer`-style
    // test seam.
    fn risky_caps() -> crate::profiler::FilesystemCapabilities {
        crate::profiler::FilesystemCapabilities {
            name: "exfat".to_string(),
            case_sensitive: false,
            max_file_size: None,
            windows_naming_rules: true,
            timestamp_granularity: std::time::Duration::from_secs(2),
            write_integrity_risk: true,
        }
    }

    #[tokio::test]
    async fn write_integrity_risk_blocks_the_operation_by_default() {
        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();
        fs::write(src_dir.path().join("a.txt"), b"a").unwrap();

        let workload = scan(src_dir.path(), 256).await.unwrap();
        let result = run_workload_pipeline(
            workload,
            dest_dir.path(),
            &risky_caps(),
            false,
            false,
            false, // allow_filesystem_integrity_risk
            &BatchConfig::default(),
            2,
            CancellationToken::new(),
            ProgressReporter::noop(),
        )
        .await;

        assert!(matches!(result, Err(Error::FilesystemIntegrityRisk { .. })));
        assert!(
            !dest_dir.path().join("a.txt").exists(),
            "nothing should have been written"
        );
    }

    #[tokio::test]
    async fn allow_filesystem_integrity_risk_lets_the_operation_proceed() {
        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();
        fs::write(src_dir.path().join("a.txt"), b"a").unwrap();

        let workload = scan(src_dir.path(), 256).await.unwrap();
        let outcome = run_workload_pipeline(
            workload,
            dest_dir.path(),
            &risky_caps(),
            false,
            false,
            true, // allow_filesystem_integrity_risk
            &BatchConfig::default(),
            2,
            CancellationToken::new(),
            ProgressReporter::noop(),
        )
        .await
        .unwrap();

        assert_eq!(outcome.succeeded.len(), 1);
        assert_eq!(fs::read(dest_dir.path().join("a.txt")).unwrap(), b"a");
    }

    #[tokio::test]
    async fn ensure_directories_exist_reports_progress_for_every_directory() {
        let dest_dir = tempdir().unwrap();

        let dirs = vec![
            crate::profiler::DirEntry {
                path: PathBuf::from("irrelevant"),
                relative_path: PathBuf::from("a"),
                mode: None,
            },
            crate::profiler::DirEntry {
                path: PathBuf::from("irrelevant"),
                relative_path: PathBuf::from("b"),
                mode: None,
            },
            crate::profiler::DirEntry {
                path: PathBuf::from("irrelevant"),
                relative_path: PathBuf::from("c"),
                mode: None,
            },
        ];

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let reporter = ProgressReporter::new(tx);

        let failures = ensure_directories_exist(&dirs, dest_dir.path(), 2, &reporter).await;
        drop(reporter);

        assert!(failures.is_empty());
        assert!(dest_dir.path().join("a").is_dir());
        assert!(dest_dir.path().join("b").is_dir());
        assert!(dest_dir.path().join("c").is_dir());

        let mut events = Vec::new();
        while let Some(event) = rx.recv().await {
            events.push(event);
        }

        assert!(matches!(
            events[0],
            Progress::DirectoriesStarted { total: 3 }
        ));
        let completed = events
            .iter()
            .filter(|e| matches!(e, Progress::DirectoryCompleted { .. }))
            .count();
        assert_eq!(
            completed, 3,
            "one DirectoryCompleted per directory, regardless of concurrency"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn ensure_directories_exist_continues_and_reports_after_one_failure() {
        use std::os::unix::fs::PermissionsExt;

        let dest_dir = tempdir().unwrap();
        let locked_dir = dest_dir.path().join("locked");
        fs::create_dir(&locked_dir).unwrap();
        fs::set_permissions(&locked_dir, fs::Permissions::from_mode(0o555)).unwrap();

        let dirs = vec![
            crate::profiler::DirEntry {
                path: PathBuf::from("irrelevant"),
                relative_path: PathBuf::from("locked/nested"),
                mode: None,
            },
            crate::profiler::DirEntry {
                path: PathBuf::from("irrelevant"),
                relative_path: PathBuf::from("ok"),
                mode: None,
            },
        ];

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let reporter = ProgressReporter::new(tx);

        let failures = ensure_directories_exist(&dirs, dest_dir.path(), 2, &reporter).await;
        drop(reporter);
        fs::set_permissions(&locked_dir, fs::Permissions::from_mode(0o755)).unwrap();

        assert_eq!(failures.len(), 1);
        assert!(
            dest_dir.path().join("ok").is_dir(),
            "the other directory should still have been created"
        );

        let mut events = Vec::new();
        while let Some(event) = rx.recv().await {
            events.push(event);
        }
        assert!(events
            .iter()
            .any(|e| matches!(e, Progress::DirectoryFailed { .. })));
        assert!(events
            .iter()
            .any(|e| matches!(e, Progress::DirectoryCompleted { .. })));
    }

    #[test]
    fn directories_covered_by_files_marks_every_ancestor_including_the_root() {
        let small = vec![crate::profiler::Entry {
            path: PathBuf::from("irrelevant"),
            relative_path: PathBuf::from("a/b/file.txt"),
            size: 1,
            modified: None,
        }];

        let covered = directories_covered_by_files(&small, &[]);

        assert!(covered.contains(&PathBuf::from("a/b")));
        assert!(covered.contains(&PathBuf::from("a")));
        assert!(
            covered.contains(&PathBuf::new()),
            "the root itself is a proper ancestor too"
        );
        assert_eq!(covered.len(), 3);
    }

    #[test]
    fn directories_covered_by_files_does_not_cover_an_unrelated_sibling() {
        let small = vec![crate::profiler::Entry {
            path: PathBuf::from("irrelevant"),
            relative_path: PathBuf::from("a/b/file.txt"),
            size: 1,
            modified: None,
        }];

        let covered = directories_covered_by_files(&small, &[]);

        assert!(!covered.contains(&PathBuf::from("a/empty_sibling")));
    }

    #[tokio::test]
    async fn only_directories_with_no_files_anywhere_beneath_them_are_explicitly_created() {
        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();

        // "a" and "a/b" both contain (or are an ancestor of) a file, so
        // they'll be created for free by that file's own copy. Only
        // "empty_dir" has no file anywhere in its subtree.
        fs::create_dir_all(src_dir.path().join("a").join("b")).unwrap();
        fs::write(src_dir.path().join("a").join("b").join("file.txt"), b"x").unwrap();
        fs::create_dir_all(src_dir.path().join("empty_dir")).unwrap();

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let reporter = ProgressReporter::new(tx);

        let outcome = run_copy_pipeline(
            src_dir.path(),
            dest_dir.path(),
            false,
            false,
            false,
            256,
            &BatchConfig::default(),
            2,
            CancellationToken::new(),
            reporter,
        )
        .await
        .unwrap();

        assert!(outcome.failed.is_empty());
        assert!(dest_dir
            .path()
            .join("a")
            .join("b")
            .join("file.txt")
            .exists());
        assert!(dest_dir.path().join("empty_dir").is_dir());

        let mut events = Vec::new();
        while let Ok(event) = rx.try_recv() {
            events.push(event);
        }
        let directories_started = events
            .iter()
            .find_map(|e| {
                if let Progress::DirectoriesStarted { total } = e {
                    Some(*total)
                } else {
                    None
                }
            })
            .expect("DirectoriesStarted should have been emitted");

        assert_eq!(
            directories_started, 1,
            "only empty_dir should need an explicit create_dir_all — root and a/b are covered by the file copy"
        );
    }
}
