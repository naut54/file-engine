use std::path::PathBuf;

use tokio::sync::mpsc;

use crate::profiler::Entry;

/// Discrete per-entry events rather than a cumulative snapshot;
/// `EntryFailed` carries only the `Entry`, not the `Error` — `Error` isn't
/// `Clone` (it wraps `std::io::Error`), and the failure detail is already
/// available from the operation's final `OperationOutcome.failed` once the
/// handle resolves.
#[derive(Debug, Clone)]
pub enum Progress {
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
