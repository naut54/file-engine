use crate::profiler::Entry;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortOrder {
    Ascending,
    Descending,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ErrorStrategy {
    #[default]
    ContinueAndCollect,
    AbortOnError,
    Undo,
}

pub const DEFAULT_MAX_BYTES_PER_BATCH: u64 = 8 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct BatchConfig {
    pub max_bytes_per_batch: u64,
    pub max_files_per_batch: Option<usize>,
    pub sort_order: SortOrder,
    pub error_strategy: ErrorStrategy,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_bytes_per_batch: DEFAULT_MAX_BYTES_PER_BATCH,
            max_files_per_batch: None,
            sort_order: SortOrder::Descending,
            error_strategy: ErrorStrategy::default(),
        }
    }
}

/// Used when there's no median to derive a count cap from (empty input,
/// or every entry is zero bytes) — still bounds a batch by count even
/// though the byte budget alone wouldn't constrain it.
const FALLBACK_MAX_FILES_PER_BATCH: usize = 1000;

impl BatchConfig {
    /// `max_files_per_batch`, resolved: the explicit override if set,
    /// otherwise `max_bytes_per_batch / median(entries' sizes)`, floored
    /// at 1 so a median larger than the byte budget still allows a batch
    /// of one rather than zero.
    pub(crate) fn resolved_max_files_per_batch(&self, entries: &[Entry]) -> usize {
        if let Some(n) = self.max_files_per_batch {
            return n;
        }

        let median = median_size(entries);
        if median == 0 {
            return FALLBACK_MAX_FILES_PER_BATCH;
        }

        ((self.max_bytes_per_batch / median) as usize).max(1)
    }
}

fn median_size(entries: &[Entry]) -> u64 {
    if entries.is_empty() {
        return 0;
    }

    let mut sizes: Vec<u64> = entries.iter().map(|e| e.size).collect();
    sizes.sort_unstable();

    let mid = sizes.len() / 2;
    if sizes.len() % 2 == 0 {
        (sizes[mid - 1] + sizes[mid]) / 2
    } else {
        sizes[mid]
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn entry(size: u64) -> Entry {
        Entry {
            path: PathBuf::from("x"),
            relative_path: PathBuf::from("x"),
            size,
            modified: None,
        }
    }

    #[test]
    fn explicit_override_wins_regardless_of_sizes() {
        let config = BatchConfig {
            max_files_per_batch: Some(7),
            ..BatchConfig::default()
        };
        let entries: Vec<Entry> = vec![entry(1), entry(1_000_000)];
        assert_eq!(config.resolved_max_files_per_batch(&entries), 7);
    }

    #[test]
    fn derived_from_median_with_even_length_input() {
        // Sizes 10, 20, 30, 40 -> median = (20 + 30) / 2 = 25.
        let entries: Vec<Entry> = vec![entry(40), entry(10), entry(30), entry(20)];
        let config = BatchConfig {
            max_bytes_per_batch: 250,
            max_files_per_batch: None,
            ..BatchConfig::default()
        };
        assert_eq!(config.resolved_max_files_per_batch(&entries), 10);
    }

    #[test]
    fn derivation_on_empty_entries_does_not_panic() {
        let config = BatchConfig {
            max_files_per_batch: None,
            ..BatchConfig::default()
        };
        assert_eq!(
            config.resolved_max_files_per_batch(&[]),
            FALLBACK_MAX_FILES_PER_BATCH
        );
    }

    #[test]
    fn derivation_on_all_zero_byte_entries_does_not_panic() {
        let entries: Vec<Entry> = vec![entry(0), entry(0), entry(0)];
        let config = BatchConfig {
            max_files_per_batch: None,
            ..BatchConfig::default()
        };
        assert_eq!(
            config.resolved_max_files_per_batch(&entries),
            FALLBACK_MAX_FILES_PER_BATCH
        );
    }

    #[test]
    fn error_strategy_defaults_to_continue_and_collect() {
        assert_eq!(BatchConfig::default().error_strategy, ErrorStrategy::ContinueAndCollect);
    }
}
