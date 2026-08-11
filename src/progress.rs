use std::path::PathBuf;

use tokio::sync::mpsc;

use crate::profiler::Entry;

/// Discrete per-entry events rather than a cumulative snapshot;
/// `EntryFailed` carries only the `Entry`, not the `Error` — `Error` isn't
/// `Clone` (it wraps `std::io::Error`), and the failure detail is already
/// available from the operation's final `OperationOutcome.failed` once the
/// handle resolves.
/// `#[non_exhaustive]`: adding a variant here is otherwise a breaking
/// change for any downstream exhaustive `match`, which is exactly what
/// adding `Planned` was. Marked now so the next addition isn't.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Progress {
    /// The shape of the work about to be performed, emitted once per
    /// phase *before* `DirectoriesStarted` and `Started` — i.e. before
    /// the directory pre-pass that `Started` doesn't cover.
    ///
    /// Exists for cost estimation (`EtaEstimator`): the small/large
    /// split is the difference between work whose cost is per-file
    /// (syscall-bound) and work whose cost is per-byte (bandwidth-bound),
    /// and `Started`'s single `bytes_total` can't distinguish them. A
    /// consumer that only wants a progress bar can ignore this variant
    /// entirely.
    ///
    /// Not emitted by the delete sweeps (`sync`'s orphan sweep,
    /// `move_path`'s source cleanup) — those are metadata-only phases
    /// with no byte-sized work to model, so they emit a bare `Started`.
    Planned {
        /// Directories needing an explicit `create_dir_all` — already
        /// filtered to those with no file beneath them (see
        /// `pipeline.rs`'s `directories_covered_by_files`), so this
        /// matches the `DirectoriesStarted { total }` that follows
        /// rather than the total directory count in the source tree.
        directories: usize,
        small_files: usize,
        small_bytes: u64,
        large_files: usize,
        large_bytes: u64,
        /// The threshold that produced the split above. Carried so a
        /// consumer can classify each subsequent `Entry` the same way
        /// the planner did, without having to know what the builder was
        /// configured with.
        small_file_threshold: u64,
    },
    /// Emitted once per phase, before any entries in that phase start.
    /// `bytes_total` is `None` for phases with nothing byte-sized to
    /// report (the delete sweeps). Can be emitted more than once per
    /// operation — `sync` emits it once per phase (copy, then delete).
    Started {
        bytes_total: Option<u64>,
        entries_total: usize,
    },
    EntryStarted {
        entry: Entry,
    },
    /// Bytes written so far for an entry still in flight, sampled by
    /// watching the destination file grow. Emitted only for large
    /// (streamed) entries, and only while they take long enough to be
    /// sampled at all — a copy that the filesystem satisfies by
    /// copy-on-write finishes before the first sample and emits none.
    ///
    /// `bytes_copied` is cumulative, not a delta, and is clamped to the
    /// entry's size. It is monotonically non-decreasing per entry.
    ///
    /// Exists because `tokio::fs::copy` is opaque while it runs: without
    /// this, a single large file emits `EntryStarted` and then nothing
    /// until it finishes, so its transfer rate is unmeasurable for exactly
    /// as long as it takes to copy.
    EntryProgress {
        entry: Entry,
        bytes_copied: u64,
    },
    EntryCompleted {
        entry: Entry,
    },
    EntryFailed {
        entry: Entry,
    },
    /// The directory-creation pre-pass (`operations/pipeline.rs`'s
    /// `ensure_directories_exist`), separate from `Started`/`Entry*`
    /// since it operates on `DirEntry`, not `Entry` — carrying only the
    /// destination path (not the full `DirEntry`) is enough for a
    /// caller to show "N/M directories created" without extending
    /// every `Entry`-typed variant to also accept `DirEntry`. Added
    /// after a real run against a USB-connected exFAT drive spent about
    /// a minute silently creating ~7,700 directories one at a time
    /// before `Started` (for the file-copy phase) was ever emitted.
    DirectoriesStarted {
        total: usize,
    },
    DirectoryCompleted {
        path: PathBuf,
    },
    DirectoryFailed {
        path: PathBuf,
    },
}

/// `Send + Sync + Clone` sender wrapper threaded into every execution
/// path that processes entries. Backed by an unbounded channel
/// specifically so `.send()` is synchronous, not `async` — needed
/// because `compress.rs`'s writer runs inside `spawn_blocking`, a
/// non-async context that can't await a bounded channel's backpressure.
#[derive(Clone)]
pub(crate) struct ProgressReporter {
    tx: mpsc::UnboundedSender<Progress>,
}

impl ProgressReporter {
    pub(crate) fn new(tx: mpsc::UnboundedSender<Progress>) -> Self {
        Self { tx }
    }

    /// A reporter whose receiver is immediately dropped — for tests that
    /// don't care about progress and don't want to plumb a receiver
    /// through just to ignore it.
    #[cfg(test)]
    pub(crate) fn noop() -> Self {
        let (tx, _rx) = mpsc::unbounded_channel();
        Self { tx }
    }

    /// A closed receiver means nobody's listening (the caller never
    /// called `.progress()`, or dropped the stream) — not a failure,
    /// just nothing to report to.
    pub(crate) fn send(&self, progress: Progress) {
        let _ = self.tx.send(progress);
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn entry(name: &str) -> Entry {
        Entry {
            path: PathBuf::from(name),
            relative_path: PathBuf::from(name),
            size: 1,
            modified: None,
        }
    }

    #[test]
    fn send_after_receiver_dropped_does_not_panic() {
        let (tx, rx) = mpsc::unbounded_channel();
        let reporter = ProgressReporter::new(tx);
        drop(rx);

        reporter.send(Progress::EntryStarted { entry: entry("a") });
    }

    #[tokio::test]
    async fn sends_are_received_in_order() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let reporter = ProgressReporter::new(tx);

        reporter.send(Progress::Started {
            bytes_total: Some(10),
            entries_total: 2,
        });
        reporter.send(Progress::EntryStarted { entry: entry("a") });
        reporter.send(Progress::EntryCompleted { entry: entry("a") });

        drop(reporter); // otherwise the last recv() below blocks forever

        assert!(matches!(rx.recv().await, Some(Progress::Started { .. })));
        assert!(matches!(
            rx.recv().await,
            Some(Progress::EntryStarted { .. })
        ));
        assert!(matches!(
            rx.recv().await,
            Some(Progress::EntryCompleted { .. })
        ));
        assert!(rx.recv().await.is_none());
    }
}
