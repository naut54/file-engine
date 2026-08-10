use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinSet;

use tokio_util::sync::CancellationToken;

use crate::error::{Error, Result};
use crate::planner::{plan, BatchConfig, ErrorStrategy, OperationOutcome, StopReason};
use crate::profiler::{scan, Entry, DEFAULT_SMALL_FILE_THRESHOLD};
use crate::progress::{Progress, ProgressReporter};

use super::default_concurrency;

pub struct CompressBuilder {
    source: PathBuf,
    dest: PathBuf,
    format: Option<CompressFormat>,
    small_file_threshold: Option<u64>,
    batch_config: BatchConfig,
    concurrency: Option<usize>,
}

impl CompressBuilder {
    pub(crate) fn new(source: impl Into<PathBuf>, dest: impl Into<PathBuf>) -> Self {
        Self {
            source: source.into(),
            dest: dest.into(),
            format: None,
            small_file_threshold: None,
            batch_config: BatchConfig::default(),
            concurrency: None,
        }
    }

    pub fn format(mut self, format: CompressFormat) -> Self {
        self.format = Some(format);
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
            compress(
                &self.source,
                &self.dest,
                self.format,
                threshold,
                &self.batch_config,
                concurrency,
                cancel_for_task,
                reporter,
            )
            .await
        });

        Ok(crate::handle::Handle::new(join_handle, rx, cancel))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressFormat {
    Zip,
    Gzip,
}

fn infer_format(dest: &Path) -> Result<CompressFormat> {
    match dest.extension().and_then(|e| e.to_str()) {
        Some("zip") => Ok(CompressFormat::Zip),
        Some("gz") => Ok(CompressFormat::Gzip),
        _ => Err(Error::UnknownCompressFormat {
            path: dest.to_path_buf(),
        }),
    }
}

/// Reuses the Profiler and `planner::plan`'s batching, but not
/// `planner::dispatcher` — archive writers need a single sequential writer.
/// One deviation from the original design worth flagging: workers here do
/// the *read* in parallel and hand raw bytes to the writer, rather than
/// compressing in the worker and handing over already-compressed bytes.
/// `zip`'s writer API compresses as you write to it; there's no
/// straightforward way to hand it pre-compressed bytes for a fresh entry
/// without fighting its API (that's meant for copying between archives, not
/// injecting arbitrary compressed streams). Compression is CPU-bound and
/// cheap relative to the disk I/O this design already parallelizes, so the
/// practical benefit lost is small; what's preserved is the actual
/// motivating concern — parallel reads instead of one file at a time.
// Matches `run_copy_pipeline`/`move_path`/`sync`: these pipeline entry
// points thread the same builder options through, and grouping them into
// a struct purely to satisfy the lint would add a type with no other use.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn compress(
    source: &Path,
    dest: &Path,
    format: Option<CompressFormat>,
    small_file_threshold: u64,
    config: &BatchConfig,
    concurrency: usize,
    cancel: CancellationToken,
    reporter: ProgressReporter,
) -> Result<OperationOutcome> {
    let format = match format {
        Some(f) => f,
        None => infer_format(dest)?,
    };

    if matches!(format, CompressFormat::Gzip) {
        let metadata = tokio::fs::metadata(source)
            .await
            .map_err(|e| classify_error(e, source))?;
        if metadata.is_dir() {
            return Err(Error::GzipRequiresFile {
                path: source.to_path_buf(),
            });
        }
        // A single blocking operation with no natural mid-flight
        // checkpoint — unlike compress_zip, cancellation isn't checked
        // here, the same way move_path.rs's atomic-rename fast path
        // doesn't check it either.
        return compress_gzip(source, dest, reporter).await;
    }

    compress_zip(
        source,
        dest,
        small_file_threshold,
        config,
        concurrency,
        cancel,
        reporter,
    )
    .await
}

async fn compress_gzip(
    source: &Path,
    dest: &Path,
    reporter: ProgressReporter,
) -> Result<OperationOutcome> {
    let source = source.to_path_buf();
    let dest = dest.to_path_buf();

    tokio::task::spawn_blocking(move || compress_gzip_blocking(&source, &dest, reporter))
        .await
        .expect("gzip compression task panicked")
}

fn compress_gzip_blocking(
    source: &Path,
    dest: &Path,
    reporter: ProgressReporter,
) -> Result<OperationOutcome> {
    let metadata = std::fs::metadata(source).map_err(|e| classify_error(e, source))?;
    let entry = Entry {
        path: source.to_path_buf(),
        relative_path: source.file_name().map(PathBuf::from).unwrap_or_default(),
        size: metadata.len(),
        modified: metadata.modified().ok(),
    };

    reporter.send(Progress::Started {
        bytes_total: Some(entry.size),
        entries_total: 1,
    });
    reporter.send(Progress::EntryStarted {
        entry: entry.clone(),
    });

    let result: Result<()> = (|| {
        let mut input = std::fs::File::open(source).map_err(|e| classify_error(e, source))?;
        let output = std::fs::File::create(dest).map_err(|e| classify_error(e, dest))?;
        let mut encoder = flate2::write::GzEncoder::new(output, flate2::Compression::default());

        std::io::copy(&mut input, &mut encoder).map_err(|e| classify_error(e, source))?;
        encoder.finish().map_err(|e| classify_error(e, dest))?;
        Ok(())
    })();

    match result {
        Ok(()) => {
            reporter.send(Progress::EntryCompleted {
                entry: entry.clone(),
            });
            Ok(OperationOutcome {
                succeeded: vec![entry],
                ..Default::default()
            })
        }
        Err(err) => {
            reporter.send(Progress::EntryFailed { entry });
            Err(err)
        }
    }
}

enum WriterMsg {
    Data { entry: Entry, bytes: Vec<u8> },
    Failed { entry: Entry, error: Error },
}

async fn compress_zip(
    source: &Path,
    dest: &Path,
    small_file_threshold: u64,
    config: &BatchConfig,
    concurrency: usize,
    cancel: CancellationToken,
    reporter: ProgressReporter,
) -> Result<OperationOutcome> {
    let workload = scan(source, small_file_threshold).await?;
    let execution_plan = plan(workload, config);

    let bytes_total: u64 = execution_plan
        .batches
        .iter()
        .map(|b| b.total_bytes)
        .sum::<u64>()
        + execution_plan
            .streams
            .iter()
            .map(|s| s.entry.size)
            .sum::<u64>();
    let entries_total: usize = execution_plan
        .batches
        .iter()
        .map(|b| b.entries.len())
        .sum::<usize>()
        + execution_plan.streams.len();

    let mut units: Vec<Vec<Entry>> =
        Vec::with_capacity(execution_plan.batches.len() + execution_plan.streams.len());
    units.extend(execution_plan.batches.into_iter().map(|b| b.entries));
    units.extend(execution_plan.streams.into_iter().map(|s| vec![s.entry]));

    reporter.send(Progress::Started {
        bytes_total: Some(bytes_total),
        entries_total,
    });

    let concurrency = concurrency.max(1);
    let (tx, rx) = mpsc::channel::<WriterMsg>(concurrency);
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let stop = Arc::new(AtomicBool::new(false));

    let error_strategy = config.error_strategy;
    let writer_handle = tokio::task::spawn_blocking({
        let stop = Arc::clone(&stop);
        let dest = dest.to_path_buf();
        let reporter = reporter.clone();
        move || write_archive(rx, dest, error_strategy, stop, reporter)
    });

    let mut join_set: JoinSet<()> = JoinSet::new();
    let mut stopped_by_cancel = false;

    for unit in units {
        if stop.load(Ordering::SeqCst) {
            break;
        }

        // Checked between units only, same rationale as
        // dispatcher.rs's cooperative cancellation contract: the batch
        // caps already bound how much work one unit represents, so this
        // keeps worst-case cancellation latency bounded without a
        // per-entry check inside every batch.
        let permit = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                stopped_by_cancel = true;
                break;
            }
            permit = Arc::clone(&semaphore).acquire_owned() => {
                match permit {
                    Ok(permit) => permit,
                    Err(_) => break,
                }
            }
        };

        if stop.load(Ordering::SeqCst) {
            break;
        }

        let tx = tx.clone();
        let stop = Arc::clone(&stop);
        let reporter = reporter.clone();

        join_set.spawn(async move {
            let _permit = permit;
            for entry in unit {
                if stop.load(Ordering::SeqCst) {
                    break;
                }

                reporter.send(Progress::EntryStarted {
                    entry: entry.clone(),
                });

                let msg = match tokio::fs::read(&entry.path).await {
                    Ok(bytes) => WriterMsg::Data { entry, bytes },
                    Err(e) => {
                        // Reported directly here, not by the writer —
                        // this entry never reaches it.
                        reporter.send(Progress::EntryFailed {
                            entry: entry.clone(),
                        });
                        let error = classify_error(e, &entry.path);
                        WriterMsg::Failed { entry, error }
                    }
                };

                // A send error means the writer already stopped and
                // dropped its receiver — cooperative shutdown, not a
                // failure to record.
                if tx.send(msg).await.is_err() {
                    break;
                }
            }
        });
    }

    // Drop the original sender so the channel can close once every
    // worker-held clone is also dropped (which happens as each spawned
    // task finishes below).
    drop(tx);

    while let Some(result) = join_set.join_next().await {
        let _ = result;
    }

    let mut outcome = writer_handle.await.expect("archive writer task panicked")?;

    // write_archive has no way to know cancellation happened — from its
    // side, the channel just closes once in-flight workers finish, which
    // looks identical to a normal, uninterrupted completion. Redo its
    // stop/cleanup decision here for the cancellation case specifically.
    if stopped_by_cancel && outcome.stopped_early.is_none() {
        outcome.stopped_early = Some(StopReason::Cancelled);
        if matches!(
            error_strategy,
            ErrorStrategy::AbortOnError | ErrorStrategy::Undo
        ) {
            let _ = std::fs::remove_file(dest);
            outcome.succeeded.clear();
        }
    }

    Ok(outcome)
}

fn write_archive(
    mut rx: mpsc::Receiver<WriterMsg>,
    dest: PathBuf,
    error_strategy: ErrorStrategy,
    stop: Arc<AtomicBool>,
    reporter: ProgressReporter,
) -> Result<OperationOutcome> {
    let file = std::fs::File::create(&dest).map_err(|e| classify_error(e, &dest))?;
    let mut zip = zip::ZipWriter::new(file);
    let options = zip::write::SimpleFileOptions::default();

    let mut outcome = OperationOutcome::default();

    while let Some(msg) = rx.blocking_recv() {
        match msg {
            WriterMsg::Data { entry, bytes } => {
                let name = entry.relative_path.to_string_lossy().replace('\\', "/");
                let write_result = zip
                    .start_file(name, options)
                    .and_then(|()| zip.write_all(&bytes).map_err(zip::result::ZipError::Io));

                match write_result {
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
                        let path = entry.path.clone();
                        let error = Error::Io {
                            path,
                            source: std::io::Error::other(err.to_string()),
                        };
                        record_failure(&mut outcome, entry, error, error_strategy, &stop);
                    }
                }
            }
            // Already reported by the worker that read it — this entry
            // never reached the writer to begin with.
            WriterMsg::Failed { entry, error } => {
                record_failure(&mut outcome, entry, error, error_strategy, &stop);
            }
        }

        if outcome.stopped_early.is_some() {
            break;
        }
    }

    // Explicitly stop draining — any workers still trying to send will
    // get a closed-channel error and shut down cooperatively.
    drop(rx);

    zip.finish().map_err(|err| Error::Io {
        path: dest.clone(),
        source: std::io::Error::other(err.to_string()),
    })?;

    if matches!(
        error_strategy,
        ErrorStrategy::AbortOnError | ErrorStrategy::Undo
    ) && outcome.stopped_early.is_some()
    {
        let _ = std::fs::remove_file(&dest);
        outcome.succeeded.clear();
    }

    Ok(outcome)
}

fn record_failure(
    outcome: &mut OperationOutcome,
    entry: Entry,
    error: Error,
    error_strategy: ErrorStrategy,
    stop: &AtomicBool,
) {
    let fatal = error.is_fatal();
    let reason = if fatal {
        Some(StopReason::Fatal)
    } else {
        match error_strategy {
            ErrorStrategy::ContinueAndCollect => None,
            ErrorStrategy::AbortOnError => Some(StopReason::AbortOnError),
            ErrorStrategy::Undo => Some(StopReason::Undo),
        }
    };

    outcome.failed.push((entry, error));

    if let Some(reason) = reason {
        if outcome.stopped_early.is_none() {
            outcome.stopped_early = Some(reason);
        }
        stop.store(true, Ordering::SeqCst);
    }
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
    use std::collections::BTreeMap;
    use std::fs;
    use std::io::Read;

    use tempfile::tempdir;

    use super::*;

    fn read_zip_entries(path: &Path) -> BTreeMap<String, Vec<u8>> {
        let file = fs::File::open(path).unwrap();
        let mut archive = zip::ZipArchive::new(file).unwrap();
        let mut out = BTreeMap::new();
        for i in 0..archive.len() {
            let mut zip_file = archive.by_index(i).unwrap();
            let mut buf = Vec::new();
            zip_file.read_to_end(&mut buf).unwrap();
            out.insert(zip_file.name().to_string(), buf);
        }
        out
    }

    #[tokio::test]
    async fn directory_of_small_files_round_trips_through_zip() {
        let src_dir = tempdir().unwrap();
        let out_dir = tempdir().unwrap();
        let dest = out_dir.path().join("archive.zip");

        fs::create_dir_all(src_dir.path().join("nested")).unwrap();
        fs::write(src_dir.path().join("a.txt"), b"aaa").unwrap();
        fs::write(src_dir.path().join("nested").join("b.txt"), b"bbb").unwrap();

        let outcome = compress(
            src_dir.path(),
            &dest,
            None,
            256 * 1024,
            &BatchConfig::default(),
            2,
            CancellationToken::new(),
            ProgressReporter::noop(),
        )
        .await
        .unwrap();

        assert_eq!(outcome.succeeded.len(), 2);
        assert!(outcome.failed.is_empty());

        let entries = read_zip_entries(&dest);
        assert_eq!(entries.get("a.txt").unwrap(), b"aaa");
        assert_eq!(entries.get("nested/b.txt").unwrap(), b"bbb");
    }

    #[tokio::test]
    async fn single_file_compresses_via_gzip() {
        let src_dir = tempdir().unwrap();
        let out_dir = tempdir().unwrap();
        let source = src_dir.path().join("file.txt");
        let dest = out_dir.path().join("file.txt.gz");
        fs::write(&source, b"hello gzip").unwrap();

        let outcome = compress(
            &source,
            &dest,
            None,
            256,
            &BatchConfig::default(),
            2,
            CancellationToken::new(),
            ProgressReporter::noop(),
        )
        .await
        .unwrap();

        assert_eq!(outcome.succeeded.len(), 1);
        assert!(dest.exists());

        let file = fs::File::open(&dest).unwrap();
        let mut decoder = flate2::read::GzDecoder::new(file);
        let mut contents = Vec::new();
        decoder.read_to_end(&mut contents).unwrap();
        assert_eq!(contents, b"hello gzip");
    }

    #[tokio::test]
    async fn single_file_compresses_via_zip_too() {
        let src_dir = tempdir().unwrap();
        let out_dir = tempdir().unwrap();
        let source = src_dir.path().join("file.txt");
        let dest = out_dir.path().join("file.zip");
        fs::write(&source, b"hello zip").unwrap();

        let outcome = compress(
            &source,
            &dest,
            None,
            256,
            &BatchConfig::default(),
            2,
            CancellationToken::new(),
            ProgressReporter::noop(),
        )
        .await
        .unwrap();

        assert_eq!(outcome.succeeded.len(), 1);
        let entries = read_zip_entries(&dest);
        assert_eq!(entries.get("file.txt").unwrap(), b"hello zip");
    }

    #[tokio::test]
    async fn gzip_on_a_directory_fails_before_any_work_starts() {
        let src_dir = tempdir().unwrap();
        let out_dir = tempdir().unwrap();
        fs::write(src_dir.path().join("a.txt"), b"a").unwrap();
        let dest = out_dir.path().join("archive.gz");

        let result = compress(
            src_dir.path(),
            &dest,
            Some(CompressFormat::Gzip),
            256,
            &BatchConfig::default(),
            2,
            CancellationToken::new(),
            ProgressReporter::noop(),
        )
        .await;

        assert!(matches!(result, Err(Error::GzipRequiresFile { .. })));
        assert!(!dest.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn continue_and_collect_keeps_compressing_after_one_unreadable_entry() {
        use std::os::unix::fs::PermissionsExt;

        let src_dir = tempdir().unwrap();
        let out_dir = tempdir().unwrap();
        let dest = out_dir.path().join("archive.zip");

        fs::write(src_dir.path().join("a.txt"), b"a").unwrap();
        fs::write(src_dir.path().join("b.txt"), b"b").unwrap();
        let unreadable = src_dir.path().join("c.txt");
        fs::write(&unreadable, b"c").unwrap();
        fs::set_permissions(&unreadable, fs::Permissions::from_mode(0o000)).unwrap();

        let outcome = compress(
            src_dir.path(),
            &dest,
            None,
            256,
            &BatchConfig::default(),
            2,
            CancellationToken::new(),
            ProgressReporter::noop(),
        )
        .await;

        fs::set_permissions(&unreadable, fs::Permissions::from_mode(0o644)).unwrap();

        let outcome = outcome.unwrap();
        assert_eq!(outcome.succeeded.len(), 2);
        assert_eq!(outcome.failed.len(), 1);
        assert_eq!(outcome.failed[0].0.relative_path, PathBuf::from("c.txt"));

        let entries = read_zip_entries(&dest);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries.get("a.txt").unwrap(), b"a");
        assert_eq!(entries.get("b.txt").unwrap(), b"b");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn abort_on_error_leaves_no_archive_file_on_disk() {
        use std::os::unix::fs::PermissionsExt;

        let src_dir = tempdir().unwrap();
        let out_dir = tempdir().unwrap();
        let dest = out_dir.path().join("archive.zip");

        let unreadable = src_dir.path().join("a.txt");
        fs::write(&unreadable, b"a").unwrap();
        fs::set_permissions(&unreadable, fs::Permissions::from_mode(0o000)).unwrap();
        fs::write(src_dir.path().join("b.txt"), b"b").unwrap();

        let config = BatchConfig {
            error_strategy: ErrorStrategy::AbortOnError,
            ..BatchConfig::default()
        };

        let outcome = compress(
            src_dir.path(),
            &dest,
            None,
            256,
            &config,
            1,
            CancellationToken::new(),
            ProgressReporter::noop(),
        )
        .await;

        fs::set_permissions(&unreadable, fs::Permissions::from_mode(0o644)).unwrap();

        let outcome = outcome.unwrap();
        assert_eq!(outcome.stopped_early, Some(StopReason::AbortOnError));
        assert!(!dest.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn undo_leaves_no_archive_file_on_disk() {
        use std::os::unix::fs::PermissionsExt;

        let src_dir = tempdir().unwrap();
        let out_dir = tempdir().unwrap();
        let dest = out_dir.path().join("archive.zip");

        let unreadable = src_dir.path().join("a.txt");
        fs::write(&unreadable, b"a").unwrap();
        fs::set_permissions(&unreadable, fs::Permissions::from_mode(0o000)).unwrap();
        fs::write(src_dir.path().join("b.txt"), b"b").unwrap();

        let config = BatchConfig {
            error_strategy: ErrorStrategy::Undo,
            ..BatchConfig::default()
        };

        let outcome = compress(
            src_dir.path(),
            &dest,
            None,
            256,
            &config,
            1,
            CancellationToken::new(),
            ProgressReporter::noop(),
        )
        .await;

        fs::set_permissions(&unreadable, fs::Permissions::from_mode(0o644)).unwrap();

        let outcome = outcome.unwrap();
        assert_eq!(outcome.stopped_early, Some(StopReason::Undo));
        assert!(!dest.exists());
    }

    /// Not the exact "instrumented fake writer" seam the design doc
    /// described (this architecture has no injectable writer), but a
    /// practical stand-in: forcing one entry per batch means many
    /// concurrent producers race a channel whose capacity (2) is far
    /// smaller than the entry count (50). If backpressure didn't work —
    /// unbounded buffering, a deadlock, or dropped messages — this would
    /// hang or lose entries instead of completing cleanly.
    #[tokio::test]
    async fn many_entries_exceeding_channel_capacity_all_complete_without_deadlock() {
        let src_dir = tempdir().unwrap();
        let out_dir = tempdir().unwrap();
        let dest = out_dir.path().join("archive.zip");

        for i in 0..50 {
            fs::write(src_dir.path().join(format!("f{i}.txt")), format!("data{i}")).unwrap();
        }

        let config = BatchConfig {
            max_files_per_batch: Some(1),
            ..BatchConfig::default()
        };

        let outcome = compress(
            src_dir.path(),
            &dest,
            None,
            256 * 1024,
            &config,
            2,
            CancellationToken::new(),
            ProgressReporter::noop(),
        )
        .await
        .unwrap();

        assert_eq!(outcome.succeeded.len(), 50);
        assert!(outcome.failed.is_empty());

        let entries = read_zip_entries(&dest);
        assert_eq!(entries.len(), 50);
    }

    /// No injectable delay seam exists for compress's workers (unlike
    /// dispatcher.rs's tests, which use a fake `EntryAction` with a
    /// controllable delay), so real file reads happen too fast to
    /// reliably test cancellation landing mid-run without flakiness.
    /// Cancelling *before* any work starts is still a deterministic,
    /// meaningful test of the same code path: `biased` `select!` on an
    /// already-cancelled token always wins over the semaphore permit,
    /// even though a permit is also immediately available.
    #[tokio::test]
    async fn cancellation_before_any_work_starts_stops_everything() {
        let src_dir = tempdir().unwrap();
        let out_dir = tempdir().unwrap();
        let dest = out_dir.path().join("archive.zip");

        fs::write(src_dir.path().join("a.txt"), b"a").unwrap();
        fs::write(src_dir.path().join("b.txt"), b"b").unwrap();

        let cancel = CancellationToken::new();
        cancel.cancel();

        let outcome = compress(
            src_dir.path(),
            &dest,
            None,
            256,
            &BatchConfig::default(),
            1,
            cancel,
            ProgressReporter::noop(),
        )
        .await
        .unwrap();

        assert!(outcome.succeeded.is_empty());
        assert_eq!(outcome.stopped_early, Some(StopReason::Cancelled));
        // ContinueAndCollect (the default) still finalizes the archive,
        // just an empty one — only AbortOnError/Undo delete it.
        assert!(dest.exists());
    }

    #[tokio::test]
    async fn cancellation_with_abort_on_error_leaves_no_archive_file() {
        let src_dir = tempdir().unwrap();
        let out_dir = tempdir().unwrap();
        let dest = out_dir.path().join("archive.zip");

        fs::write(src_dir.path().join("a.txt"), b"a").unwrap();

        let config = BatchConfig {
            error_strategy: ErrorStrategy::AbortOnError,
            ..BatchConfig::default()
        };

        let cancel = CancellationToken::new();
        cancel.cancel();

        let outcome = compress(
            src_dir.path(),
            &dest,
            None,
            256,
            &config,
            1,
            cancel,
            ProgressReporter::noop(),
        )
        .await
        .unwrap();

        assert_eq!(outcome.stopped_early, Some(StopReason::Cancelled));
        assert!(!dest.exists());
    }
}
