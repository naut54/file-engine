use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock};

use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::checksum::hash_file;
use crate::error::{Error, Result};

use super::error_strategy::AnalysisErrorStrategy;
use super::progress::{AnalysisProgress, AnalysisProgressReporter};
use super::report::DuplicateGroup;
use super::util::default_concurrency;
use super::Entry;

/// Returned alongside the duplicate-group findings: hashing a candidate
/// file can itself fail (removed between the walk and this pass, a
/// permission change, ...), and those failures are reported through the
/// same `errors`/`errors_total` split as walk errors rather than
/// silently dropped or force-aborting duplicate detection regardless of
/// the caller's `AnalysisErrorStrategy`.
/// blake3's hash of the empty input, computed once. Every empty file is
/// byte-identical to every other by construction, so a size-0 group
/// never needs to actually be read and hashed — see its use in
/// `detect_duplicates` below.
static EMPTY_FILE_HASH: LazyLock<[u8; 32]> = LazyLock::new(|| *blake3::hash(b"").as_bytes());

pub(crate) struct HashOutcome {
    pub(crate) groups: Vec<DuplicateGroup>,
    pub(crate) groups_total: usize,
    pub(crate) bytes_wasted: u64,
    pub(crate) errors: Vec<(PathBuf, Error)>,
    pub(crate) errors_total: usize,
}

/// Groups `candidates` (already filtered to the caller's matched
/// entries) into duplicate sets by content hash, and returns the capped
/// sample plus the two uncapped totals `AnalysisReport` needs
/// (`duplicate_groups_total`, `duplicate_bytes_wasted`) — see
/// `report.rs`'s doc comments for why those stay uncapped.
///
/// Two-phase: first a free (no I/O) grouping by `size`, since two files
/// can only be byte-identical if they're the same size — this alone
/// rules out any file whose size is unique in the candidate set before a
/// single byte is hashed. Only files that survive that are actually
/// read and blake3-hashed, bounded to `concurrency` in flight at once
/// via the same `Arc<Semaphore>` pattern `planner/dispatcher.rs` and
/// `operations/pipeline.rs` use for their own worker pools.
pub(crate) async fn detect_duplicates(
    candidates: Vec<Entry>,
    concurrency: Option<usize>,
    max_reported_groups: usize,
    max_reported_errors: usize,
    error_strategy: AnalysisErrorStrategy,
    cancel: &CancellationToken,
    reporter: &AnalysisProgressReporter,
) -> Result<HashOutcome> {
    let mut by_size: HashMap<u64, Vec<Entry>> = HashMap::new();
    for entry in candidates {
        by_size.entry(entry.size).or_default().push(entry);
    }

    // Empty files are guaranteed byte-identical to one another without
    // reading them — fold a size-0 group straight into `by_hash` and
    // skip it in the spawn/hash loop below entirely.
    let empty_group = by_size.remove(&0).filter(|group| group.len() > 1);

    let to_hash: Vec<Entry> = by_size
        .into_values()
        .filter(|group| group.len() > 1)
        .flatten()
        .collect();

    let concurrency = concurrency.unwrap_or_else(default_concurrency).max(1);
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let mut tasks = JoinSet::new();

    for entry in to_hash {
        if cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let semaphore = Arc::clone(&semaphore);
        let reporter = reporter.clone();
        tasks.spawn(async move {
            let permit = semaphore
                .acquire_owned()
                .await
                .expect("semaphore is never closed");
            let path = entry.path.clone();
            let hash = tokio::task::spawn_blocking(move || hash_file(&path))
                .await
                .expect("hash task panicked");
            drop(permit);
            reporter.send(AnalysisProgress::EntryHashed {
                path: entry.path.clone(),
            });
            (entry, hash)
        });
    }

    let mut by_hash: HashMap<(u64, [u8; 32]), Vec<Entry>> = HashMap::new();
    if let Some(group) = empty_group {
        by_hash.insert((0, *EMPTY_FILE_HASH), group);
    }

    let mut errors = Vec::new();
    let mut errors_total = 0usize;
    while let Some(result) = tasks.join_next().await {
        if cancel.is_cancelled() {
            // Dropping `tasks` aborts every hash task still in flight,
            // rather than waiting for the rest to finish uselessly.
            // Results already pulled off the queue this call are
            // discarded along with them — cancellation means giving up
            // on the whole pass, not returning a partial one.
            return Err(Error::Cancelled);
        }

        let (entry, hash) = result.expect("hash task panicked");
        match hash {
            Ok(hash) => {
                by_hash.entry((entry.size, hash)).or_default().push(entry);
            }
            Err(err) => {
                if error_strategy == AnalysisErrorStrategy::AbortOnError {
                    return Err(err);
                }
                errors_total += 1;
                if errors.len() < max_reported_errors {
                    errors.push((entry.path, err));
                }
            }
        }
    }

    let mut groups_total = 0usize;
    let mut bytes_wasted = 0u64;
    let mut groups = Vec::new();
    for ((size, hash), entries) in by_hash {
        if entries.len() < 2 {
            continue;
        }
        groups_total += 1;
        bytes_wasted += size * (entries.len() as u64 - 1);
        if groups.len() < max_reported_groups {
            groups.push(DuplicateGroup {
                hash,
                size,
                paths: entries.into_iter().map(|e| e.path).collect(),
            });
        }
    }

    Ok(HashOutcome {
        groups,
        groups_total,
        bytes_wasted,
        errors,
        errors_total,
    })
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::time::Duration;

    use tempfile::tempdir;
    use tokio_util::sync::CancellationToken;

    use super::*;

    fn noop_reporter() -> AnalysisProgressReporter {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        AnalysisProgressReporter::new(tx)
    }

    fn entry(path: PathBuf, size: u64) -> Entry {
        Entry {
            relative_path: path.file_name().map(PathBuf::from).unwrap_or_default(),
            path,
            size,
            modified: None,
        }
    }

    #[tokio::test]
    async fn empty_files_are_grouped_as_duplicates_without_reading_them() {
        // Nonexistent paths: if `detect_duplicates` ever tried to hash
        // these, `hash_file` would fail to open them and the group would
        // come back empty instead of populated — proving the size-0
        // fast path never touches disk.
        let candidates = vec![
            entry(PathBuf::from("/does/not/exist/a"), 0),
            entry(PathBuf::from("/does/not/exist/b"), 0),
            entry(PathBuf::from("/does/not/exist/c"), 0),
        ];

        let outcome = detect_duplicates(
            candidates,
            None,
            1000,
            1000,
            AnalysisErrorStrategy::ContinueAndCollect,
            &CancellationToken::new(),
            &noop_reporter(),
        )
        .await
        .unwrap();

        assert_eq!(outcome.groups_total, 1);
        assert_eq!(outcome.groups.len(), 1);
        assert_eq!(outcome.groups[0].size, 0);
        assert_eq!(outcome.groups[0].paths.len(), 3);
        assert_eq!(outcome.errors_total, 0);
    }

    #[tokio::test]
    async fn a_lone_empty_file_is_not_reported_as_a_duplicate() {
        let candidates = vec![entry(PathBuf::from("/does/not/exist/a"), 0)];

        let outcome = detect_duplicates(
            candidates,
            None,
            1000,
            1000,
            AnalysisErrorStrategy::ContinueAndCollect,
            &CancellationToken::new(),
            &noop_reporter(),
        )
        .await
        .unwrap();

        assert_eq!(outcome.groups_total, 0);
    }

    #[tokio::test]
    async fn abort_on_error_stops_at_the_first_hash_failure() {
        let dir = tempdir().unwrap();
        let a = dir.path().join("a.txt");
        let b = dir.path().join("b.txt");
        fs::write(&a, b"same content").unwrap();
        fs::write(&b, b"same content").unwrap();
        let size = fs::metadata(&a).unwrap().len();

        let candidates = vec![entry(a.clone(), size), entry(b.clone(), size)];
        // Removed after listing but before hashing, so the size-based
        // grouping still queues both for a hash that then fails for `b`.
        fs::remove_file(&b).unwrap();

        let result = detect_duplicates(
            candidates,
            Some(1),
            1000,
            1000,
            AnalysisErrorStrategy::AbortOnError,
            &CancellationToken::new(),
            &noop_reporter(),
        )
        .await;

        assert!(matches!(
            result,
            Err(Error::SourceNotFound { .. } | Error::Io { .. })
        ));
    }

    #[tokio::test]
    async fn continue_and_collect_records_hash_failures_instead_of_aborting() {
        let dir = tempdir().unwrap();
        let a = dir.path().join("a.txt");
        let b = dir.path().join("b.txt");
        fs::write(&a, b"same content").unwrap();
        fs::write(&b, b"same content").unwrap();
        let size = fs::metadata(&a).unwrap().len();

        let candidates = vec![entry(a.clone(), size), entry(b.clone(), size)];
        fs::remove_file(&b).unwrap();

        let outcome = detect_duplicates(
            candidates,
            Some(1),
            1000,
            1000,
            AnalysisErrorStrategy::ContinueAndCollect,
            &CancellationToken::new(),
            &noop_reporter(),
        )
        .await
        .unwrap();

        assert_eq!(outcome.errors_total, 1);
        assert_eq!(outcome.errors[0].0, b);
        // `a` never found a surviving match to group with, so no
        // duplicate group is reported.
        assert_eq!(outcome.groups_total, 0);
    }

    #[tokio::test]
    async fn cancellation_is_observed_while_draining_completed_hashes() {
        let dir = tempdir().unwrap();
        let mut candidates = Vec::new();
        for i in 0..20 {
            let path = dir.path().join(format!("f{i}.bin"));
            fs::write(&path, vec![0u8; 4_000_000]).unwrap();
            candidates.push(entry(path, 4_000_000));
        }

        let cancel = CancellationToken::new();
        let trigger = cancel.clone();
        let reporter = noop_reporter();

        let (result, ()) = tokio::join!(
            detect_duplicates(
                candidates,
                Some(1),
                1000,
                1000,
                AnalysisErrorStrategy::ContinueAndCollect,
                &cancel,
                &reporter,
            ),
            async {
                tokio::time::sleep(Duration::from_millis(5)).await;
                trigger.cancel();
            }
        );

        assert!(matches!(result, Err(Error::Cancelled)));
    }
}
