use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::profiler::Entry;
use crate::progress::{Progress, ProgressReporter};

use super::action::EntryAction;
use super::config::ErrorStrategy;
use super::outcome::{OperationOutcome, StopReason};
use super::plan::ExecutionPlan;

enum Unit {
    Batch(Vec<Entry>),
    Stream(Entry),
}

/// How often a streamed entry's destination file is sampled for
/// `Progress::EntryProgress`. Frequent enough that a slow transfer feels
/// live, sparse enough that the cost (one `metadata` call per in-flight
/// stream) stays irrelevant next to the copy itself.
const PROGRESS_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(250);

/// Reports how far a single in-flight streamed entry has got, by watching
/// its destination file grow, until `cancel` fires.
///
/// This exists because `tokio::fs::copy` is opaque while it runs. Copying
/// in chunks would give exact byte counts directly, but would forfeit the
/// kernel's accelerated copy paths (`clonefile` on APFS, `copy_file_range`
/// on Linux) that make same-volume copies effectively instant — measured
/// here at 2GB in under a millisecond. Sampling the destination keeps
/// those paths and costs one syscall per interval.
///
/// Silently reports nothing if it can't stat the destination (it may not
/// exist yet) — progress reporting must never be able to fail an
/// operation that is otherwise succeeding.
async fn report_entry_progress(
    path: std::path::PathBuf,
    entry: Entry,
    reporter: ProgressReporter,
    cancel: CancellationToken,
) {
    let mut last_reported = 0;

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(PROGRESS_POLL_INTERVAL) => {}
        }

        let Ok(metadata) = tokio::fs::metadata(&path).await else {
            continue;
        };

        // Clamped to the size recorded at scan time: the estimator treats
        // these as progress toward a known total, and a destination that
        // momentarily measures larger (a pre-existing file mid-truncation)
        // must not push reported progress past 100%.
        let bytes_copied = metadata.len().min(entry.size);
        if bytes_copied > last_reported {
            last_reported = bytes_copied;
            reporter.send(Progress::EntryProgress {
                entry: entry.clone(),
                bytes_copied,
            });
        }
    }
}

/// Executes an `ExecutionPlan` against a concurrency-limited worker pool,
/// applying `error_strategy` to per-entry failures and `Error::is_fatal()`
/// to decide when to stop regardless of strategy. Cancellation is checked
/// between batches/streams only, not inside an in-progress batch's entry
/// loop.
///
/// Rollback for `ErrorStrategy::Undo` calls `action.undo()` directly on
/// every entry in `succeeded`, in reverse order, rather than going
/// through `undo.rs`'s `UndoLog`/`UndoOp` — `EntryAction` already exposes
/// exactly the compensating action needed (see `action.rs`), so there's
/// nothing left for a separate op-log to add here. `undo.rs` remains for
/// `move_path.rs`'s deferred deletion sweep, which isn't an `EntryAction`
/// operation and needs a different compensating action (restore-then-
/// delete) that doesn't fit this trait.
pub(crate) async fn dispatch<A: EntryAction + 'static>(
    plan: ExecutionPlan,
    action: A,
    dest_root: &Path,
    error_strategy: ErrorStrategy,
    concurrency: usize,
    cancel: CancellationToken,
    reporter: ProgressReporter,
) -> OperationOutcome {
    let bytes_total: u64 = plan.batches.iter().map(|b| b.total_bytes).sum::<u64>()
        + plan.streams.iter().map(|s| s.entry.size).sum::<u64>();
    let entries_total: usize =
        plan.batches.iter().map(|b| b.entries.len()).sum::<usize>() + plan.streams.len();

    let mut streams = plan.streams;

    // One stream is pulled to the front of the queue as a calibration
    // sample. Everything behind it is unchanged.
    //
    // Streams used to run strictly after every batch, which meant that on
    // a workload with many small files and a few large ones, no large file
    // finished until the operation was nearly over — a real 2.7GB run put
    // ~56 batch units ahead of 6 streams and completed its first stream at
    // 95% elapsed. Nothing consuming `Progress` can observe a per-byte
    // transfer rate until a streamed entry completes (an in-flight one
    // reports nothing between `EntryStarted` and `EntryCompleted`), so
    // that ordering withheld the only true bandwidth measurement in the
    // run until it was useless. `EtaEstimator` is the consumer that
    // motivated this, but the measurement isn't specific to it.
    //
    // The *smallest* stream is chosen, not the largest: it still clears
    // the small-file threshold, so it's bandwidth-dominated rather than
    // per-file-overhead-dominated, but it completes soonest — and the
    // point is to get a usable sample early, not a perfect one.
    let calibration_stream = streams
        .iter()
        .enumerate()
        .min_by_key(|(_, stream)| stream.entry.size)
        .map(|(index, _)| index)
        .map(|index| streams.remove(index));

    let mut units: Vec<Unit> = Vec::with_capacity(plan.batches.len() + streams.len() + 1);
    units.extend(calibration_stream.map(|s| Unit::Stream(s.entry)));
    units.extend(plan.batches.into_iter().map(|b| Unit::Batch(b.entries)));
    units.extend(streams.into_iter().map(|s| Unit::Stream(s.entry)));

    reporter.send(Progress::Started {
        bytes_total: Some(bytes_total),
        entries_total,
    });

    let action = Arc::new(action);
    let dest_root = Arc::new(dest_root.to_path_buf());
    let semaphore = Arc::new(Semaphore::new(concurrency.max(1)));
    let outcome = Arc::new(Mutex::new(OperationOutcome::default()));
    let stop = Arc::new(AtomicBool::new(false));

    let mut join_set: JoinSet<()> = JoinSet::new();
    let mut stopped_by_cancel = false;

    for unit in units {
        if stop.load(Ordering::SeqCst) {
            break;
        }

        let permit = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                stopped_by_cancel = true;
                break;
            }
            permit = Arc::clone(&semaphore).acquire_owned() => {
                permit.expect("semaphore closed")
            }
        };

        // Acquiring a permit can block until an in-flight unit finishes
        // and releases one — by which point that unit may have set
        // `stop`. Re-check rather than spawning on stale information.
        if stop.load(Ordering::SeqCst) {
            break;
        }

        let action = Arc::clone(&action);
        let dest_root = Arc::clone(&dest_root);
        let outcome = Arc::clone(&outcome);
        let stop = Arc::clone(&stop);
        let reporter = reporter.clone();

        join_set.spawn(async move {
            let _permit = permit;
            // Only streamed entries are sampled: a batch's entries are
            // small by construction, so each finishes well inside one poll
            // interval and would be sampled zero times anyway — the polling
            // would be pure overhead.
            let (entries, sample_bytes) = match unit {
                Unit::Batch(entries) => (entries, false),
                Unit::Stream(entry) => (vec![entry], true),
            };

            for entry in entries {
                reporter.send(Progress::EntryStarted {
                    entry: entry.clone(),
                });

                let sampler = sample_bytes
                    .then(|| action.progress_target(&entry, &dest_root))
                    .flatten()
                    .map(|path| {
                        let token = CancellationToken::new();
                        let handle = tokio::spawn(report_entry_progress(
                            path,
                            entry.clone(),
                            reporter.clone(),
                            token.clone(),
                        ));
                        (token, handle)
                    });

                let result = action.execute(&entry, &dest_root).await;

                if let Some((token, handle)) = sampler {
                    token.cancel();
                    // A sampler panic (a runtime built without the time
                    // driver would produce one on its first sleep) is
                    // swallowed deliberately: losing intermediate progress
                    // must not fail a copy that succeeded.
                    let _ = handle.await;
                }

                match result {
                    Ok(()) => {
                        reporter.send(Progress::EntryCompleted {
                            entry: entry.clone(),
                        });
                        outcome.lock().unwrap().succeeded.push(entry);
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

                        {
                            let mut out = outcome.lock().unwrap();
                            out.failed.push((entry, err));
                            if let Some(reason) = reason {
                                if out.stopped_early.is_none() {
                                    out.stopped_early = Some(reason);
                                }
                            }
                        }

                        // Stop this unit's remaining entries too, not
                        // just future units — a triggering failure means
                        // "stop", not "stop everything except what I was
                        // already doing."
                        if reason.is_some() {
                            stop.store(true, Ordering::SeqCst);
                            break;
                        }
                    }
                }
            }
        });
    }

    while let Some(result) = join_set.join_next().await {
        let _ = result;
    }

    let mut outcome = Arc::try_unwrap(outcome)
        .unwrap_or_else(|_| panic!("dispatch: outcome has outstanding references after join"))
        .into_inner()
        .unwrap();

    if stopped_by_cancel && outcome.stopped_early.is_none() {
        outcome.stopped_early = Some(StopReason::Cancelled);
    }

    if matches!(error_strategy, ErrorStrategy::Undo) && outcome.stopped_early.is_some() {
        for entry in outcome.succeeded.iter().rev() {
            let _ = action.undo(entry, &dest_root).await;
        }
        outcome.succeeded.clear();
    }

    outcome
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::future::Future;
    use std::path::PathBuf;
    use std::pin::Pin;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    use tempfile::tempdir;

    use crate::error::{Error, Result};

    use super::super::batch::Batch;
    use super::super::plan::StreamJob;
    use super::*;

    fn entry(name: &str) -> Entry {
        Entry {
            path: PathBuf::from(name),
            relative_path: PathBuf::from(name),
            size: 1,
            modified: None,
        }
    }

    /// One single-entry batch per input entry, so dispatch order and
    /// per-unit granularity are deterministic and easy to reason about
    /// in tests, independent of `batch.rs`'s own packing logic.
    fn single_entry_plan(entries: &[Entry]) -> ExecutionPlan {
        ExecutionPlan {
            batches: entries
                .iter()
                .cloned()
                .map(|e| Batch {
                    entries: vec![e],
                    total_bytes: 1,
                })
                .collect(),
            streams: Vec::new(),
        }
    }

    struct FakeAction {
        log: Arc<Mutex<Vec<String>>>,
        active: Arc<AtomicUsize>,
        max_active: Arc<AtomicUsize>,
        delay_for: Arc<HashMap<PathBuf, Duration>>,
        /// `Some(true)` = fails fatally, `Some(false)` = fails per-entry,
        /// absent = succeeds.
        fail: Arc<HashMap<PathBuf, bool>>,
    }

    impl EntryAction for FakeAction {
        fn execute<'a>(
            &'a self,
            entry: &'a Entry,
            _dest_root: &'a Path,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
            Box::pin(async move {
                let current = self.active.fetch_add(1, Ordering::SeqCst) + 1;
                self.max_active.fetch_max(current, Ordering::SeqCst);
                self.log
                    .lock()
                    .unwrap()
                    .push(format!("start:{}", entry.path.display()));

                if let Some(d) = self.delay_for.get(&entry.path) {
                    tokio::time::sleep(*d).await;
                }

                self.active.fetch_sub(1, Ordering::SeqCst);
                self.log
                    .lock()
                    .unwrap()
                    .push(format!("end:{}", entry.path.display()));

                match self.fail.get(&entry.path) {
                    Some(true) => Err(Error::NoSpace {
                        needed: 0,
                        available: 0,
                    }),
                    Some(false) => Err(Error::SourceNotFound {
                        path: entry.path.clone(),
                    }),
                    None => Ok(()),
                }
            })
        }

        fn undo<'a>(
            &'a self,
            entry: &'a Entry,
            _dest_root: &'a Path,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
            Box::pin(async move {
                self.log
                    .lock()
                    .unwrap()
                    .push(format!("undo:{}", entry.path.display()));
                Ok(())
            })
        }
    }

    fn fake_action(
        delay_for: HashMap<PathBuf, Duration>,
        fail: HashMap<PathBuf, bool>,
    ) -> (FakeAction, Arc<Mutex<Vec<String>>>, Arc<AtomicUsize>) {
        let log = Arc::new(Mutex::new(Vec::new()));
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let action = FakeAction {
            log: Arc::clone(&log),
            active: Arc::clone(&active),
            max_active: Arc::clone(&max_active),
            delay_for: Arc::new(delay_for),
            fail: Arc::new(fail),
        };
        (action, log, max_active)
    }

    /// Writes its destination file in visible increments, so the sampler
    /// has something to observe growing — standing in for a real
    /// cross-device `fs::copy`, which is opaque while it runs.
    struct GrowingAction;

    impl EntryAction for GrowingAction {
        fn execute<'a>(
            &'a self,
            entry: &'a Entry,
            dest_root: &'a Path,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
            Box::pin(async move {
                let dest = dest_root.join(&entry.relative_path);
                for step in 1..=4u64 {
                    tokio::fs::write(&dest, vec![0u8; (entry.size / 4 * step) as usize])
                        .await
                        .unwrap();
                    tokio::time::sleep(Duration::from_millis(150)).await;
                }
                Ok(())
            })
        }

        fn undo<'a>(
            &'a self,
            _entry: &'a Entry,
            _dest_root: &'a Path,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
            Box::pin(async move { Ok(()) })
        }

        fn progress_target(&self, entry: &Entry, dest_root: &Path) -> Option<PathBuf> {
            Some(dest_root.join(&entry.relative_path))
        }
    }

    #[tokio::test]
    async fn a_streamed_entry_reports_partial_byte_progress_while_in_flight() {
        let big = Entry {
            size: 4000,
            ..entry("big")
        };
        let plan = ExecutionPlan {
            batches: Vec::new(),
            streams: vec![StreamJob { entry: big.clone() }],
        };

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let dest_root = tempdir().unwrap();
        let outcome = dispatch(
            plan,
            GrowingAction,
            dest_root.path(),
            ErrorStrategy::ContinueAndCollect,
            1,
            CancellationToken::new(),
            ProgressReporter::new(tx),
        )
        .await;

        assert_eq!(outcome.succeeded.len(), 1);

        let mut samples = Vec::new();
        while let Ok(event) = rx.try_recv() {
            if let Progress::EntryProgress { bytes_copied, .. } = event {
                samples.push(bytes_copied);
            }
        }

        assert!(
            !samples.is_empty(),
            "expected at least one in-flight sample over a ~600ms copy"
        );
        assert!(
            samples.windows(2).all(|w| w[0] < w[1]),
            "samples must be strictly increasing, got {samples:?}"
        );
        assert!(
            samples.iter().all(|&b| b <= big.size),
            "samples must never exceed the entry size, got {samples:?}"
        );
    }

    /// The sampler must stop when its entry does. A leaked poller would
    /// keep emitting events after the operation resolved.
    #[tokio::test]
    async fn sampling_stops_once_the_entry_completes() {
        let big = Entry {
            size: 4000,
            ..entry("big")
        };
        let plan = ExecutionPlan {
            batches: Vec::new(),
            streams: vec![StreamJob { entry: big }],
        };

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let dest_root = tempdir().unwrap();
        dispatch(
            plan,
            GrowingAction,
            dest_root.path(),
            ErrorStrategy::ContinueAndCollect,
            1,
            CancellationToken::new(),
            ProgressReporter::new(tx),
        )
        .await;

        while rx.try_recv().is_ok() {}
        tokio::time::sleep(PROGRESS_POLL_INTERVAL * 3).await;
        assert!(
            rx.try_recv().is_err(),
            "no events should be emitted after dispatch resolves"
        );
    }

    /// Guards the calibration ordering: without it, every batch is queued
    /// ahead of every stream, and on a many-small-files workload no
    /// per-byte transfer rate is observable until the run is nearly over.
    #[tokio::test]
    async fn the_smallest_stream_is_dispatched_before_any_batch() {
        let small: Vec<Entry> = (0..6).map(|i| entry(&format!("small{i}"))).collect();
        let mut plan = single_entry_plan(&small);
        plan.streams = [("big", 900), ("medium", 500), ("biggest", 1000)]
            .into_iter()
            .map(|(name, size)| StreamJob {
                entry: Entry {
                    size,
                    ..entry(name)
                },
            })
            .collect();

        // Concurrency of 1 makes the queue order directly observable in
        // the log, rather than being blurred by overlapping workers.
        let (action, log, _) = fake_action(HashMap::new(), HashMap::new());
        let dest_root = tempdir().unwrap();
        let outcome = dispatch(
            plan,
            action,
            dest_root.path(),
            ErrorStrategy::ContinueAndCollect,
            1,
            CancellationToken::new(),
            ProgressReporter::noop(),
        )
        .await;

        assert_eq!(outcome.succeeded.len(), 9);
        let log = log.lock().unwrap();
        let order: Vec<&str> = log
            .iter()
            .filter_map(|line| line.strip_prefix("start:"))
            .collect();
        assert_eq!(
            order.first(),
            Some(&"medium"),
            "the smallest stream must run first as a calibration sample; got {order:?}"
        );
        // The remaining streams stay behind the batches, as before.
        assert_eq!(
            &order[order.len() - 2..],
            &["big", "biggest"],
            "remaining streams should still trail the batches; got {order:?}"
        );
    }

    #[tokio::test]
    async fn no_more_than_concurrency_limit_active_at_once() {
        let entries: Vec<Entry> = (0..6).map(|i| entry(&format!("e{i}"))).collect();
        let plan = single_entry_plan(&entries);

        let delay_for = entries
            .iter()
            .map(|e| (e.path.clone(), Duration::from_millis(30)))
            .collect();
        let (action, _log, max_active) = fake_action(delay_for, HashMap::new());

        let dest_root = tempdir().unwrap();
        let outcome = dispatch(
            plan,
            action,
            dest_root.path(),
            ErrorStrategy::ContinueAndCollect,
            2,
            CancellationToken::new(),
            ProgressReporter::noop(),
        )
        .await;

        assert_eq!(outcome.succeeded.len(), 6);
        let observed_max = max_active.load(Ordering::SeqCst);
        assert!(
            observed_max <= 2,
            "observed {observed_max} concurrent workers, expected <= 2"
        );
        assert!(
            observed_max >= 2,
            "expected overlap to actually happen under the delay, observed {observed_max}"
        );
    }

    #[tokio::test]
    async fn every_entry_processed_exactly_once_when_no_failures() {
        let entries: Vec<Entry> = (0..5).map(|i| entry(&format!("e{i}"))).collect();
        let plan = single_entry_plan(&entries);
        let (action, _log, _max_active) = fake_action(HashMap::new(), HashMap::new());

        let dest_root = tempdir().unwrap();
        let outcome = dispatch(
            plan,
            action,
            dest_root.path(),
            ErrorStrategy::ContinueAndCollect,
            3,
            CancellationToken::new(),
            ProgressReporter::noop(),
        )
        .await;

        let mut succeeded_paths: Vec<_> =
            outcome.succeeded.iter().map(|e| e.path.clone()).collect();
        succeeded_paths.sort();
        let mut expected: Vec<_> = entries.iter().map(|e| e.path.clone()).collect();
        expected.sort();

        assert_eq!(succeeded_paths, expected);
        assert!(outcome.failed.is_empty());
        assert!(outcome.cleanup_failed.is_empty());
        assert_eq!(outcome.stopped_early, None);
    }

    #[tokio::test]
    async fn continue_and_collect_does_not_stop_other_units() {
        let entries: Vec<Entry> = (0..5).map(|i| entry(&format!("e{i}"))).collect();
        let plan = single_entry_plan(&entries);

        let mut fail = HashMap::new();
        fail.insert(entries[2].path.clone(), false);
        let (action, _log, _max_active) = fake_action(HashMap::new(), fail);

        let dest_root = tempdir().unwrap();
        let outcome = dispatch(
            plan,
            action,
            dest_root.path(),
            ErrorStrategy::ContinueAndCollect,
            3,
            CancellationToken::new(),
            ProgressReporter::noop(),
        )
        .await;

        assert_eq!(outcome.succeeded.len(), 4);
        assert_eq!(outcome.failed.len(), 1);
        assert_eq!(outcome.failed[0].0.path, entries[2].path);
        assert_eq!(outcome.stopped_early, None);
    }

    #[tokio::test]
    async fn abort_on_error_stops_queued_units_but_lets_in_flight_finish() {
        let a = entry("a");
        let b = entry("b");
        let c = entry("c");
        let plan = single_entry_plan(&[a.clone(), b.clone(), c.clone()]);

        let mut delay_for = HashMap::new();
        delay_for.insert(b.path.clone(), Duration::from_millis(60));
        let mut fail = HashMap::new();
        fail.insert(a.path.clone(), false);
        let (action, log, _max_active) = fake_action(delay_for, fail);

        let dest_root = tempdir().unwrap();
        let outcome = dispatch(
            plan,
            action,
            dest_root.path(),
            ErrorStrategy::AbortOnError,
            2,
            CancellationToken::new(),
            ProgressReporter::noop(),
        )
        .await;

        assert_eq!(outcome.failed.len(), 1);
        assert_eq!(outcome.failed[0].0.path, a.path);
        assert_eq!(outcome.succeeded.len(), 1);
        assert_eq!(outcome.succeeded[0].path, b.path);
        assert_eq!(outcome.stopped_early, Some(StopReason::AbortOnError));

        let log = log.lock().unwrap();
        assert!(!log.iter().any(|l| l.contains('c')));
    }

    #[tokio::test]
    async fn undo_strategy_rolls_back_completed_entries_in_reverse_order() {
        let a = entry("a");
        let b = entry("b");
        let c = entry("c");
        let plan = single_entry_plan(&[a.clone(), b.clone(), c.clone()]);

        let mut fail = HashMap::new();
        fail.insert(c.path.clone(), false);
        let (action, log, _max_active) = fake_action(HashMap::new(), fail);

        let dest_root = tempdir().unwrap();
        let outcome = dispatch(
            plan,
            action,
            dest_root.path(),
            ErrorStrategy::Undo,
            1,
            CancellationToken::new(),
            ProgressReporter::noop(),
        )
        .await;

        assert!(
            outcome.succeeded.is_empty(),
            "succeeded should be cleared after rollback"
        );
        assert_eq!(outcome.failed.len(), 1);
        assert_eq!(outcome.failed[0].0.path, c.path);
        assert_eq!(outcome.stopped_early, Some(StopReason::Undo));

        let log = log.lock().unwrap();
        let undo_b = log
            .iter()
            .position(|l| l == "undo:b")
            .expect("b should have been undone");
        let undo_a = log
            .iter()
            .position(|l| l == "undo:a")
            .expect("a should have been undone");
        assert!(
            undo_b < undo_a,
            "undo should replay in reverse completion order"
        );
    }

    #[tokio::test]
    async fn fatal_error_stops_dispatch_even_under_continue_and_collect() {
        let a = entry("a");
        let b = entry("b");
        let plan = single_entry_plan(&[a.clone(), b.clone()]);

        let mut fail = HashMap::new();
        fail.insert(a.path.clone(), true);
        let (action, _log, _max_active) = fake_action(HashMap::new(), fail);

        let dest_root = tempdir().unwrap();
        let outcome = dispatch(
            plan,
            action,
            dest_root.path(),
            ErrorStrategy::ContinueAndCollect,
            1,
            CancellationToken::new(),
            ProgressReporter::noop(),
        )
        .await;

        assert_eq!(outcome.failed.len(), 1);
        assert_eq!(outcome.failed[0].0.path, a.path);
        assert!(outcome.succeeded.is_empty());
        assert_eq!(outcome.stopped_early, Some(StopReason::Fatal));
    }

    #[tokio::test]
    async fn cancellation_lets_in_flight_finish_but_skips_queued() {
        let a = entry("a");
        let b = entry("b");
        let c = entry("c");
        let plan = single_entry_plan(&[a.clone(), b.clone(), c.clone()]);

        let mut delay_for = HashMap::new();
        delay_for.insert(a.path.clone(), Duration::from_millis(80));
        let (action, log, _max_active) = fake_action(delay_for, HashMap::new());

        let dest_root = tempdir().unwrap();
        let cancel = CancellationToken::new();
        let trigger = cancel.clone();

        let (outcome, ()) = tokio::join!(
            dispatch(
                plan,
                action,
                dest_root.path(),
                ErrorStrategy::ContinueAndCollect,
                1,
                cancel,
                ProgressReporter::noop()
            ),
            async {
                tokio::time::sleep(Duration::from_millis(20)).await;
                trigger.cancel();
            }
        );

        assert_eq!(outcome.succeeded.len(), 1);
        assert_eq!(outcome.succeeded[0].path, a.path);
        assert!(outcome.failed.is_empty());
        assert_eq!(outcome.stopped_early, Some(StopReason::Cancelled));

        let log = log.lock().unwrap();
        assert!(!log.iter().any(|l| l.contains('b') || l.contains('c')));
    }

    #[tokio::test]
    async fn cancellation_combined_with_undo_rolls_back_what_completed_before_it() {
        let a = entry("a");
        let b = entry("b");
        let plan = single_entry_plan(&[a.clone(), b.clone()]);

        let mut delay_for = HashMap::new();
        delay_for.insert(a.path.clone(), Duration::from_millis(80));
        let (action, log, _max_active) = fake_action(delay_for, HashMap::new());

        let dest_root = tempdir().unwrap();
        let cancel = CancellationToken::new();
        let trigger = cancel.clone();

        let (outcome, ()) = tokio::join!(
            dispatch(
                plan,
                action,
                dest_root.path(),
                ErrorStrategy::Undo,
                1,
                cancel,
                ProgressReporter::noop()
            ),
            async {
                tokio::time::sleep(Duration::from_millis(20)).await;
                trigger.cancel();
            }
        );

        assert!(outcome.succeeded.is_empty());
        assert_eq!(outcome.stopped_early, Some(StopReason::Cancelled));

        let log = log.lock().unwrap();
        assert!(log.contains(&"undo:a".to_string()));
        assert!(!log.iter().any(|l| l.contains('b')));
    }

    #[tokio::test]
    async fn cancel_after_completion_does_not_change_the_outcome() {
        let entries: Vec<Entry> = (0..3).map(|i| entry(&format!("e{i}"))).collect();
        let plan = single_entry_plan(&entries);
        let (action, _log, _max_active) = fake_action(HashMap::new(), HashMap::new());

        let dest_root = tempdir().unwrap();
        let cancel = CancellationToken::new();

        let outcome = dispatch(
            plan,
            action,
            dest_root.path(),
            ErrorStrategy::ContinueAndCollect,
            2,
            cancel.clone(),
            ProgressReporter::noop(),
        )
        .await;

        // dispatch() has already returned; cancelling now cannot retroactively
        // change a value it already produced.
        cancel.cancel();

        assert_eq!(outcome.succeeded.len(), 3);
        assert_eq!(outcome.stopped_early, None);
    }

    #[tokio::test]
    async fn empty_plan_completes_immediately_with_empty_outcome() {
        let plan = ExecutionPlan::default();
        let (action, _log, _max_active) = fake_action(HashMap::new(), HashMap::new());

        let dest_root = tempdir().unwrap();
        let outcome = dispatch(
            plan,
            action,
            dest_root.path(),
            ErrorStrategy::ContinueAndCollect,
            2,
            CancellationToken::new(),
            ProgressReporter::noop(),
        )
        .await;

        assert!(outcome.succeeded.is_empty());
        assert!(outcome.failed.is_empty());
        assert_eq!(outcome.stopped_early, None);
    }
}
