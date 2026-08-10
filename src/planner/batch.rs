use crate::profiler::Entry;

use super::config::{BatchConfig, SortOrder};

#[derive(Debug, Clone, Default)]
pub struct Batch {
    pub entries: Vec<Entry>,
    pub total_bytes: u64,
}

/// Sorts `entries` by size (per `config.sort_order`) then greedily fills
/// batches under two hard caps (`max_bytes_per_batch`, resolved
/// `max_files_per_batch`) — see dev-docs/design/batching-engine.md, "batch.rs",
/// for why sorting first removes the reliance on statistical luck that a
/// pure average-based approach would have.
pub(crate) fn pack(mut entries: Vec<Entry>, config: &BatchConfig) -> Vec<Batch> {
    let max_files = config.resolved_max_files_per_batch(&entries);
    let max_bytes = config.max_bytes_per_batch;

    match config.sort_order {
        SortOrder::Ascending => entries.sort_by_key(|e| e.size),
        SortOrder::Descending => entries.sort_by_key(|e| std::cmp::Reverse(e.size)),
    }

    let mut batches = Vec::new();
    let mut current = Batch::default();

    for entry in entries {
        let would_exceed_bytes = current.total_bytes + entry.size > max_bytes;
        let would_exceed_count = current.entries.len() >= max_files;

        if !current.entries.is_empty() && (would_exceed_bytes || would_exceed_count) {
            batches.push(std::mem::take(&mut current));
        }

        current.total_bytes += entry.size;
        current.entries.push(entry);
    }

    if !current.entries.is_empty() {
        batches.push(current);
    }

    batches
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::*;

    fn entry(name: &str, size: u64) -> Entry {
        Entry {
            path: PathBuf::from(name),
            relative_path: PathBuf::from(name),
            size,
            modified: None,
        }
    }

    fn config(
        max_bytes_per_batch: u64,
        max_files_per_batch: Option<usize>,
        sort_order: SortOrder,
    ) -> BatchConfig {
        BatchConfig {
            max_bytes_per_batch,
            max_files_per_batch,
            sort_order,
            ..BatchConfig::default()
        }
    }

    fn all_entries(batches: &[Batch]) -> Vec<Entry> {
        let mut out: Vec<Entry> = batches
            .iter()
            .flat_map(|b| b.entries.iter().cloned())
            .collect();
        out.sort_by(|a, b| a.path.cmp(&b.path));
        out
    }

    fn sorted(mut entries: Vec<Entry>) -> Vec<Entry> {
        entries.sort_by(|a, b| a.path.cmp(&b.path));
        entries
    }

    // Distributions exercised across the assertions below.
    fn uniform() -> Vec<Entry> {
        (0..20)
            .map(|i| entry(&format!("uniform-{i}"), 1024))
            .collect()
    }

    fn all_tiny() -> Vec<Entry> {
        (0..500).map(|i| entry(&format!("tiny-{i}"), 1)).collect()
    }

    fn bimodal() -> Vec<Entry> {
        let mut entries: Vec<Entry> = (0..200).map(|i| entry(&format!("tiny-{i}"), 8)).collect();
        entries.extend((0..10).map(|i| entry(&format!("near-threshold-{i}"), 250_000)));
        entries
    }

    fn assert_caps_respected(entries: Vec<Entry>, config: &BatchConfig) -> Vec<Batch> {
        let batches = pack(entries.clone(), config);
        let max_files = config.resolved_max_files_per_batch(&entries);
        for batch in &batches {
            // A lone entry whose own size exceeds the budget is the one
            // documented exception: it still becomes its own batch of
            // one rather than being dropped or split.
            assert!(batch.total_bytes <= config.max_bytes_per_batch || batch.entries.len() == 1);
            assert!(batch.entries.len() <= max_files);
            let actual_bytes: u64 = batch.entries.iter().map(|e| e.size).sum();
            assert_eq!(actual_bytes, batch.total_bytes);
        }
        batches
    }

    #[test]
    fn no_batch_exceeds_caps_across_distributions_and_sort_orders() {
        for distribution in [uniform(), all_tiny(), bimodal()] {
            for sort_order in [SortOrder::Ascending, SortOrder::Descending] {
                let config = config(8 * 1024, Some(16), sort_order);
                assert_caps_respected(distribution.clone(), &config);
            }
        }
    }

    #[test]
    fn every_entry_appears_in_exactly_one_batch() {
        for distribution in [uniform(), all_tiny(), bimodal()] {
            let config = config(8 * 1024, Some(16), SortOrder::Descending);
            let batches = pack(distribution.clone(), &config);
            assert_eq!(all_entries(&batches), sorted(distribution));
        }
    }

    #[test]
    fn packing_is_deterministic() {
        let entries = bimodal();
        let config = config(8 * 1024, Some(16), SortOrder::Descending);

        let first = pack(entries.clone(), &config);
        let second = pack(entries, &config);

        let first: Vec<_> = first
            .iter()
            .map(|b| b.entries.iter().map(|e| e.path.clone()).collect::<Vec<_>>())
            .collect();
        let second: Vec<_> = second
            .iter()
            .map(|b| b.entries.iter().map(|e| e.path.clone()).collect::<Vec<_>>())
            .collect();
        assert_eq!(first, second);
    }

    #[test]
    fn ascending_fills_smallest_entries_first() {
        let entries = vec![entry("a", 300), entry("b", 100), entry("c", 200)];
        let config = config(u64::MAX, Some(1), SortOrder::Ascending);
        let batches = pack(entries, &config);
        assert_eq!(batches[0].entries[0].path, PathBuf::from("b"));
        assert_eq!(batches[1].entries[0].path, PathBuf::from("c"));
        assert_eq!(batches[2].entries[0].path, PathBuf::from("a"));
    }

    #[test]
    fn descending_fills_largest_entries_first() {
        let entries = vec![entry("a", 300), entry("b", 100), entry("c", 200)];
        let config = config(u64::MAX, Some(1), SortOrder::Descending);
        let batches = pack(entries, &config);
        assert_eq!(batches[0].entries[0].path, PathBuf::from("a"));
        assert_eq!(batches[1].entries[0].path, PathBuf::from("c"));
        assert_eq!(batches[2].entries[0].path, PathBuf::from("b"));
    }

    #[test]
    fn single_entry_larger_than_byte_budget_becomes_its_own_batch() {
        let entries = vec![
            entry("small", 100),
            entry("oversized", 10_000),
            entry("small-2", 100),
        ];
        let config = config(1_000, None, SortOrder::Descending);
        let batches = pack(entries, &config);

        let oversized_batch = batches
            .iter()
            .find(|b| b.entries.iter().any(|e| e.path == Path::new("oversized")))
            .expect("oversized entry should be in some batch");
        assert_eq!(oversized_batch.entries.len(), 1);
        assert_eq!(oversized_batch.total_bytes, 10_000);
    }

    #[test]
    fn empty_input_produces_no_batches() {
        let config = config(8 * 1024, None, SortOrder::Descending);
        assert!(pack(Vec::new(), &config).is_empty());
    }
}
