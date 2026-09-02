use std::path::PathBuf;

use tokio::sync::mpsc;

/// Deliberately a separate, smaller enum from `crate::progress::Progress`
/// rather than a reuse: `Progress` (and the `Handle<T>` it's threaded
/// through) live behind the `operations` feature, which `analyze`
/// doesn't require — see `AnalysisHandle`'s doc comment for the same
/// reasoning applied one level up.
///
/// `#[non_exhaustive]` for the same reason as `Progress`: adding a
/// variant later shouldn't be a breaking change for an exhaustive match.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum AnalysisProgress {
    /// Emitted once per matched entry (after filters, not per entry
    /// walked) as the tree is scanned.
    EntryAnalyzed { path: PathBuf },
    /// Emitted once per file as it finishes content-hashing during
    /// duplicate detection — the slow phase, so this is the only signal
    /// of liveness while it runs. Only emitted when
    /// `.detect_duplicates(true)` is set.
    #[cfg(feature = "checksum")]
    EntryHashed { path: PathBuf },
}

#[derive(Clone)]
pub(crate) struct AnalysisProgressReporter {
    tx: mpsc::UnboundedSender<AnalysisProgress>,
}

impl AnalysisProgressReporter {
    pub(crate) fn new(tx: mpsc::UnboundedSender<AnalysisProgress>) -> Self {
        Self { tx }
    }

    /// A closed receiver means nobody's listening — not a failure, just
    /// nothing to report to. Mirrors `ProgressReporter::send`.
    pub(crate) fn send(&self, progress: AnalysisProgress) {
        let _ = self.tx.send(progress);
    }
}
