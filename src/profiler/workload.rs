use std::path::PathBuf;
use std::time::SystemTime;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Entry {
    pub path: PathBuf,
    pub relative_path: PathBuf,
    pub size: u64,
    /// `None` when the platform/filesystem doesn't report mtimes.
    /// Populated by `scan.rs`; only consumed by `sync`'s
    /// `DiffStrategy::SizeAndModifiedTime`, but kept here rather than in
    /// a sync-specific type since the Profiler already fetches it for
    /// every entry as part of the same `metadata()` call that gets size.
    pub modified: Option<SystemTime>,
}

/// A directory the Profiler discovered while walking — separate from
/// `Entry` because directories aren't part of size-based batching at all
/// (no bytes to transfer), only ever consumed by permission-preservation.
///
/// `Entry` deliberately has no equivalent `mode` field: `std::fs::copy`
/// (which `CopyAction` already uses) unconditionally copies the source
/// file's permission bits to the destination — verified empirically, not
/// assumed — so an explicit file-mode-preservation step would just
/// re-apply what `copy()` already did, from a captured-at-scan-time value
/// that's actually less current than what `copy()` reads live. Directory
/// creation (`create_dir_all`) has no equivalent built-in behavior, which
/// is what makes preserving *directory* permissions the one part of this
/// feature that does something. See dev-docs/design/permissions.md.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirEntry {
    pub path: PathBuf,
    /// Empty for the scanned root itself.
    pub relative_path: PathBuf,
    pub mode: Option<u32>,
}

#[derive(Debug, Clone, Default)]
pub struct Workload {
    pub small: Vec<Entry>,
    pub large: Vec<Entry>,
    pub directories: Vec<DirEntry>,
}

impl Workload {
    /// Entries exactly at `threshold` are classified as small.
    /// `directories` is left empty — set separately by `scan_blocking`,
    /// since directories aren't part of the size-based split this
    /// function performs.
    pub(crate) fn partition(entries: Vec<Entry>, threshold: u64) -> Self {
        let (small, large) = entries.into_iter().partition(|e| e.size <= threshold);
        Self { small, large, directories: Vec::new() }
    }
}
