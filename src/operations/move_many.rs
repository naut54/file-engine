use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

use tokio_util::sync::CancellationToken;

use crate::error::{Error, Result};
use crate::planner::{BatchConfig, ErrorStrategy, OperationOutcome, StopReason};
use crate::profiler::{probe_fs_caps, scan, DEFAULT_SMALL_FILE_THRESHOLD};
use crate::progress::ProgressReporter;

use super::default_concurrency;
use super::move_path::{
    is_cross_device, resolve_existing_dest_conflict, sweep, Renamer, TokioRenamer,
};
use super::pipeline::{merge_workloads, prefix_workload, run_workload_pipeline};

/// Moves several independent sources into one destination directory in
/// a single operation, rather than requiring the caller to run
/// `MoveBuilder` once per source (and lose the ability to batch them
/// under one `ErrorStrategy`/concurrency pool/progress stream). `dest`
/// is always a directory sources land *inside* — each source keeps its
/// own basename under it — never a rename target the way `MoveBuilder`'s
/// `dest` can be.
pub struct MoveManyBuilder {
    sources: Vec<PathBuf>,
    dest: PathBuf,
    overwrite: bool,
    skip_if_identical: bool,
    preserve_permissions: bool,
    allow_filesystem_integrity_risk: bool,
    small_file_threshold: Option<u64>,
    batch_config: BatchConfig,
    concurrency: Option<usize>,
}

impl MoveManyBuilder {
    pub(crate) fn new(
        sources: impl IntoIterator<Item = impl Into<PathBuf>>,
        dest: impl Into<PathBuf>,
    ) -> Self {
        Self {
            sources: sources.into_iter().map(Into::into).collect(),
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

    /// See `MoveBuilder::skip_if_identical` — identical semantics,
    /// applied per source.
    #[cfg(feature = "checksum")]
    pub fn skip_if_identical(mut self, skip: bool) -> Self {
        self.skip_if_identical = skip;
        self
    }

    #[cfg(all(unix, feature = "permissions"))]
    pub fn preserve_permissions(mut self, preserve: bool) -> Self {
        self.preserve_permissions = preserve;
        self
    }

    pub fn allow_filesystem_integrity_risk(mut self, allow: bool) -> Self {
        self.allow_filesystem_integrity_risk = allow;
        self
    }

    pub fn small_file_threshold(mut self, bytes: u64) -> Self {
        self.small_file_threshold = Some(bytes);
        self
    }

    /// Governs the batch as a whole, not source-by-source: under
    /// `AbortOnError`/`Undo`, the first failure — whether a whole
    /// source's rename or a single file inside one of the cross-device
    /// fallback sources — stops every source not yet finished, not just
    /// the one that failed.
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
            let mut outcome = move_many(
                &self.sources,
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

/// Every source's basename, validated up front: no two sources may
/// share one (ambiguous — "moved into `dest`" would mean two different
/// things), and every source must have one at all (rules out `/`, `.`,
/// `..`, and similar). Both are whole-batch, caller-input problems, so
/// they're reported as a fatal `Err` before any source is touched,
/// rather than as a per-source failure discovered partway through —
/// same reasoning as `allow_filesystem_integrity_risk`'s pre-flight
/// check in `pipeline::run_workload_pipeline`.
fn basenames(sources: &[PathBuf]) -> Result<Vec<PathBuf>> {
    let mut seen: HashMap<PathBuf, &PathBuf> = HashMap::new();
    let mut basenames = Vec::with_capacity(sources.len());

    for source in sources {
        let basename =
            source
                .file_name()
                .map(PathBuf::from)
                .ok_or_else(|| Error::InvalidSourceName {
                    path: source.clone(),
                })?;

        if let Some(other) = seen.get(&basename) {
            return Err(Error::DuplicateSourceName {
                path: source.clone(),
                other: (*other).clone(),
            });
        }
        seen.insert(basename.clone(), source);
        basenames.push(basename);
    }

    Ok(basenames)
}

/// 1. Validate every source's basename (see `basenames`).
/// 2. `create_dir_all(dest)` — a not-yet-created destination directory
///    is normal, same reasoning as `move_path`'s equivalent fix.
/// 3. For each source in order, resolve any existing-destination
///    conflict and attempt an atomic rename (see
///    `move_path::resolve_existing_dest_conflict`, reused verbatim —
///    each source is independent, so the same per-source logic
///    `MoveBuilder` uses applies unchanged here). A same-device source
///    is fully done at this point, contributing nothing to `succeeded`
///    (matching `MoveBuilder`'s "trivial success" fast path). A
///    cross-device source is queued for step 4. Any other failure is a
///    whole-source failure: recorded in `sources_failed` under
///    `ContinueAndCollect`, or stops the entire batch under
///    `AbortOnError`/`Undo` (rolling back every source already renamed
///    in this loop, under `Undo`) — see `stop_reason_for`.
/// 4. Every cross-device source is scanned, its `Workload` re-rooted
///    under its own basename (`prefix_workload`), and all of them
///    merged (`merge_workloads`) into one combined `Workload` — planned
///    and dispatched in a single `run_workload_pipeline` call, sharing
///    one concurrency pool and one `ErrorStrategy` scope across every
///    remaining source instead of one pass per source.
/// 5. The merged phase's `succeeded` entries go through the same
///    deferred-deletion sweep `MoveBuilder` uses (`move_path::sweep`,
///    reused directly).
#[allow(clippy::too_many_arguments)]
async fn move_many<R: Renamer>(
    sources: &[PathBuf],
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
    let basenames = basenames(sources)?;

    if let Err(err) = tokio::fs::create_dir_all(dest).await {
        return Err(crate::error::classify_io_error(err, dest.to_path_buf(), 0));
    }

    let mut sources_failed: Vec<(PathBuf, Error)> = Vec::new();
    // (source, basename, is_file) — `is_file` decides how
    // `prefix_workload` needs to treat that source's scan result, see
    // its doc comment.
    let mut pending_fallback: Vec<(PathBuf, PathBuf, bool)> = Vec::new();
    // (source, dest_target) for every source already renamed in this
    // loop — only ever consulted to roll back under `Undo`.
    let mut renamed_ok: Vec<(PathBuf, PathBuf)> = Vec::new();
    let mut stopped_early = None;

    for (source, basename) in sources.iter().zip(basenames.iter()) {
        if cancel.is_cancelled() {
            stopped_early = Some(StopReason::Cancelled);
            break;
        }

        let dest_target = dest.join(basename);
        let result =
            try_move_one_source(source, &dest_target, overwrite, skip_if_identical, renamer).await;

        match result {
            Ok(MoveOneOutcome::Resolved) => {}
            Ok(MoveOneOutcome::Renamed) => {
                renamed_ok.push((source.clone(), dest_target));
            }
            Ok(MoveOneOutcome::CrossDevice) => {
                let is_file = tokio::fs::metadata(source)
                    .await
                    .map(|m| m.is_file())
                    .unwrap_or(false);
                pending_fallback.push((source.clone(), basename.clone(), is_file));
            }
            Err(err) => {
                let reason = if err.is_fatal() {
                    Some(StopReason::Fatal)
                } else {
                    match config.error_strategy {
                        ErrorStrategy::ContinueAndCollect => None,
                        ErrorStrategy::AbortOnError => Some(StopReason::AbortOnError),
                        ErrorStrategy::Undo => Some(StopReason::Undo),
                    }
                };

                sources_failed.push((source.clone(), err));

                if let Some(reason) = reason {
                    if matches!(reason, StopReason::Undo) {
                        for (src, dest_target) in renamed_ok.iter().rev() {
                            let _ = renamer.rename(dest_target, src).await;
                        }
                    }
                    stopped_early = Some(reason);
                    break;
                }
            }
        }
    }

    if let Some(reason) = stopped_early {
        return Ok(OperationOutcome {
            sources_failed,
            stopped_early: Some(reason),
            ..OperationOutcome::default()
        });
    }

    if pending_fallback.is_empty() {
        return Ok(OperationOutcome {
            sources_failed,
            ..OperationOutcome::default()
        });
    }

    let mut workloads = Vec::with_capacity(pending_fallback.len());
    for (source, basename, is_file) in &pending_fallback {
        let workload = scan(
            source,
            small_file_threshold,
            crate::profiler::ScanOptions::default(),
        )
        .await?;
        workloads.push(prefix_workload(workload, basename, *is_file));
    }
    let merged = merge_workloads(workloads);

    let dest_caps = probe_fs_caps(dest).await?;
    let mut outcome = run_workload_pipeline(
        merged,
        dest,
        &dest_caps,
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

    outcome.sources_failed = sources_failed;
    Ok(outcome)
}

enum MoveOneOutcome {
    /// Already fully handled (no conflict and renamed, or an
    /// identical-destination skip) — nothing more to do for this
    /// source.
    Resolved,
    Renamed,
    CrossDevice,
}

async fn try_move_one_source<R: Renamer>(
    source: &Path,
    dest_target: &Path,
    overwrite: bool,
    skip_if_identical: bool,
    renamer: &R,
) -> Result<MoveOneOutcome> {
    if !overwrite && resolve_existing_dest_conflict(source, dest_target, skip_if_identical).await? {
        return Ok(MoveOneOutcome::Resolved);
    }

    match renamer.rename(source, dest_target).await {
        Ok(()) => Ok(MoveOneOutcome::Renamed),
        Err(err) if is_cross_device(&err) => Ok(MoveOneOutcome::CrossDevice),
        Err(err) => Err(crate::error::classify_io_error(
            err,
            source.to_path_buf(),
            0,
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io;

    use tempfile::tempdir;

    use crate::planner::BatchConfig;

    use super::*;

    struct AlwaysCrossDevice;
    impl Renamer for AlwaysCrossDevice {
        async fn rename(&self, _source: &Path, _dest: &Path) -> io::Result<()> {
            Err(io::Error::from(io::ErrorKind::CrossesDevices))
        }
    }

    /// Cross-device for one specific source (by its basename), the
    /// atomic rename for everything else — exercises a batch where some
    /// sources take the fast path and others fall back, in one run.
    struct CrossDeviceFor(&'static str);
    impl Renamer for CrossDeviceFor {
        async fn rename(&self, source: &Path, dest: &Path) -> io::Result<()> {
            if source.file_name().and_then(|n| n.to_str()) == Some(self.0) {
                Err(io::Error::from(io::ErrorKind::CrossesDevices))
            } else {
                tokio::fs::rename(source, dest).await
            }
        }
    }

    #[tokio::test]
    async fn same_device_sources_are_moved_via_rename_and_removed() {
        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();
        let a = src_dir.path().join("a.txt");
        let b = src_dir.path().join("b.txt");
        fs::write(&a, b"a").unwrap();
        fs::write(&b, b"b").unwrap();

        let outcome = move_many(
            &[a.clone(), b.clone()],
            dest_dir.path(),
            false,
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

        assert!(outcome.succeeded.is_empty(), "fast path enumerates nothing");
        assert!(outcome.sources_failed.is_empty());
        assert!(!a.exists());
        assert!(!b.exists());
        assert_eq!(fs::read(dest_dir.path().join("a.txt")).unwrap(), b"a");
        assert_eq!(fs::read(dest_dir.path().join("b.txt")).unwrap(), b"b");
    }

    #[tokio::test]
    async fn duplicate_basenames_are_rejected_before_any_source_is_touched() {
        let root = tempdir().unwrap();
        let a = root.path().join("one").join("shared.txt");
        let b = root.path().join("two").join("shared.txt");
        fs::create_dir_all(a.parent().unwrap()).unwrap();
        fs::create_dir_all(b.parent().unwrap()).unwrap();
        fs::write(&a, b"a").unwrap();
        fs::write(&b, b"b").unwrap();
        let dest_dir = tempdir().unwrap();

        let result = move_many(
            &[a.clone(), b.clone()],
            dest_dir.path(),
            false,
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
        .await;

        assert!(matches!(result, Err(Error::DuplicateSourceName { .. })));
        // Neither source was touched — validation runs before any work.
        assert!(a.exists());
        assert!(b.exists());
    }

    #[tokio::test]
    async fn cross_device_sources_are_merged_into_one_pipeline_run() {
        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();

        let file_source = src_dir.path().join("notes.txt");
        fs::write(&file_source, b"notes").unwrap();

        let dir_source = src_dir.path().join("photos");
        fs::create_dir(&dir_source).unwrap();
        fs::write(dir_source.join("a.jpg"), b"a").unwrap();
        fs::create_dir(dir_source.join("nested")).unwrap();
        fs::write(dir_source.join("nested").join("b.jpg"), b"b").unwrap();

        let outcome = move_many(
            &[file_source.clone(), dir_source.clone()],
            dest_dir.path(),
            false,
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

        assert_eq!(outcome.succeeded.len(), 3);
        assert!(outcome.failed.is_empty());
        assert!(outcome.sources_failed.is_empty());

        assert!(!file_source.exists());
        // The sweep deletes individual files, not directories (matching
        // `move_path.rs`'s own cross-device sweep) — the now-empty
        // directory tree is left behind, only its files are gone.
        assert!(!dir_source.join("a.jpg").exists());
        assert!(!dir_source.join("nested").join("b.jpg").exists());
        assert_eq!(
            fs::read(dest_dir.path().join("notes.txt")).unwrap(),
            b"notes"
        );
        assert_eq!(
            fs::read(dest_dir.path().join("photos").join("a.jpg")).unwrap(),
            b"a"
        );
        assert_eq!(
            fs::read(dest_dir.path().join("photos").join("nested").join("b.jpg")).unwrap(),
            b"b"
        );
    }

    #[tokio::test]
    async fn a_mixed_batch_fast_renames_some_and_falls_back_for_others() {
        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();

        let fast = src_dir.path().join("fast.txt");
        let slow = src_dir.path().join("slow.txt");
        fs::write(&fast, b"fast").unwrap();
        fs::write(&slow, b"slow").unwrap();

        let outcome = move_many(
            &[fast.clone(), slow.clone()],
            dest_dir.path(),
            false,
            false,
            false,
            false,
            256,
            &BatchConfig::default(),
            2,
            CancellationToken::new(),
            ProgressReporter::noop(),
            &CrossDeviceFor("slow.txt"),
        )
        .await
        .unwrap();

        // Only the cross-device source is enumerated — the fast-renamed
        // one never goes through the pipeline.
        assert_eq!(outcome.succeeded.len(), 1);
        assert!(!fast.exists());
        assert!(!slow.exists());
        assert_eq!(fs::read(dest_dir.path().join("fast.txt")).unwrap(), b"fast");
        assert_eq!(fs::read(dest_dir.path().join("slow.txt")).unwrap(), b"slow");
    }

    #[tokio::test]
    async fn continue_and_collect_keeps_moving_other_sources_after_one_rename_failure() {
        struct FailOn(&'static str);
        impl Renamer for FailOn {
            async fn rename(&self, source: &Path, dest: &Path) -> io::Result<()> {
                if source.file_name().and_then(|n| n.to_str()) == Some(self.0) {
                    Err(io::Error::from(io::ErrorKind::PermissionDenied))
                } else {
                    tokio::fs::rename(source, dest).await
                }
            }
        }

        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();
        let a = src_dir.path().join("a.txt");
        let b = src_dir.path().join("b.txt");
        fs::write(&a, b"a").unwrap();
        fs::write(&b, b"b").unwrap();

        let outcome = move_many(
            &[a.clone(), b.clone()],
            dest_dir.path(),
            false,
            false,
            false,
            false,
            256,
            &BatchConfig::default(), // default: ContinueAndCollect
            2,
            CancellationToken::new(),
            ProgressReporter::noop(),
            &FailOn("a.txt"),
        )
        .await
        .unwrap();

        assert_eq!(outcome.sources_failed.len(), 1);
        assert_eq!(outcome.sources_failed[0].0, a);
        assert!(a.exists(), "a's move failed, so it should remain in place");
        assert!(!b.exists(), "b should still have been moved");
        assert_eq!(fs::read(dest_dir.path().join("b.txt")).unwrap(), b"b");
    }

    #[tokio::test]
    async fn abort_on_error_stops_before_touching_later_sources() {
        struct FailOn(&'static str);
        impl Renamer for FailOn {
            async fn rename(&self, source: &Path, dest: &Path) -> io::Result<()> {
                if source.file_name().and_then(|n| n.to_str()) == Some(self.0) {
                    Err(io::Error::from(io::ErrorKind::PermissionDenied))
                } else {
                    tokio::fs::rename(source, dest).await
                }
            }
        }

        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();
        let a = src_dir.path().join("a.txt");
        let b = src_dir.path().join("b.txt");
        fs::write(&a, b"a").unwrap();
        fs::write(&b, b"b").unwrap();

        let config = BatchConfig {
            error_strategy: ErrorStrategy::AbortOnError,
            ..BatchConfig::default()
        };

        let outcome = move_many(
            &[a.clone(), b.clone()],
            dest_dir.path(),
            false,
            false,
            false,
            false,
            256,
            &config,
            2,
            CancellationToken::new(),
            ProgressReporter::noop(),
            &FailOn("a.txt"),
        )
        .await
        .unwrap();

        assert_eq!(outcome.stopped_early, Some(StopReason::AbortOnError));
        assert_eq!(outcome.sources_failed.len(), 1);
        assert!(a.exists());
        assert!(
            b.exists(),
            "b comes after the triggering failure, so it should never be attempted"
        );
    }

    #[cfg(feature = "checksum")]
    #[tokio::test]
    async fn skip_if_identical_applies_per_source_on_the_fast_path() {
        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();

        let a = src_dir.path().join("a.txt");
        fs::write(&a, b"same").unwrap();
        fs::write(dest_dir.path().join("a.txt"), b"same").unwrap();

        let outcome = move_many(
            std::slice::from_ref(&a),
            dest_dir.path(),
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

        assert!(outcome.sources_failed.is_empty());
        assert!(!a.exists(), "the redundant source should still be removed");
        assert_eq!(fs::read(dest_dir.path().join("a.txt")).unwrap(), b"same");
    }
}
