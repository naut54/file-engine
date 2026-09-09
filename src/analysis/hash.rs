use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

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
    let mut errors = Vec::new();
    let mut errors_total = 0usize;
    while let Some(result) = tasks.join_next().await {
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
