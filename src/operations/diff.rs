use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::error::{Error, Result};
use crate::paths::normalize_for_comparison;
use crate::profiler::{scan, Entry};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DiffStrategy {
    #[default]
    SizeAndModifiedTime,
    #[cfg(feature = "checksum")]
    Checksum,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct SyncPlan {
    pub to_copy: Vec<Entry>,
    pub to_delete: Vec<Entry>,
}

/// Walks both `source` and `dest` via the Profiler and matches entries by
/// `relative_path` to produce a `SyncPlan`. See
/// dev-docs/design/batching-engine.md, "sync.rs and diff.rs".
///
/// `tolerance` absorbs a destination filesystem's coarse mtime
/// resolution (e.g. FAT/exFAT's 2-second granularity — see
/// `FilesystemCapabilities::timestamp_granularity`,
/// dev-docs/design/filesystem-detection.md item 4) so a source file that's
/// only marginally newer than what's already at the destination, purely
/// because the destination can't represent the sub-tolerance difference
/// in the first place, isn't treated as "changed" on every single sync
/// run. `Duration::ZERO` (native filesystems, sub-second resolution)
/// preserves exact comparison.
pub(crate) async fn diff(source: &Path, dest: &Path, strategy: DiffStrategy, tolerance: Duration) -> Result<SyncPlan> {
    let source_entries = flat_entries(source).await?;
    // Unlike `source`, a missing `dest` isn't an error here — it's the
    // normal shape of a first sync into a destination that doesn't exist
    // yet, and should just mean "everything needs copying," not fail.
    let dest_entries = match flat_entries(dest).await {
        Ok(entries) => entries,
        Err(Error::SourceNotFound { .. }) => Vec::new(),
        Err(err) => return Err(err),
    };

    // Keyed by normalized form, not the raw `relative_path`: a file
    // written as NFD by one filesystem's writer (e.g. HFS+/APFS) and
    // read back as NFC-normalized bytes from another (e.g. exFAT) is
    // still the same file — comparing raw bytes would treat it as
    // "only in source" and "only in dest" instead of matching it. See
    // dev-docs/design/filesystem-detection.md, item 6. The *source* entry's
    // own (unnormalized) `relative_path` is what actually gets used
    // downstream for the real copy — normalization here is comparison
    // only, never used to construct a path passed to a filesystem call.
    let mut dest_by_relative_path: HashMap<PathBuf, Entry> = dest_entries
        .into_iter()
        .map(|entry| (normalize_for_comparison(&entry.relative_path), entry))
        .collect();

    let mut to_copy = Vec::new();
    for source_entry in source_entries {
        let key = normalize_for_comparison(&source_entry.relative_path);
        match dest_by_relative_path.remove(&key) {
            None => to_copy.push(source_entry),
            Some(dest_entry) => {
                if changed(&source_entry, &dest_entry, strategy, tolerance).await? {
                    to_copy.push(source_entry);
                }
            }
        }
    }

    // Whatever's left in the map exists in `dest` but was never matched
    // against a `source` entry.
    let to_delete = dest_by_relative_path.into_values().collect();

    Ok(SyncPlan { to_copy, to_delete })
}

/// Diff doesn't care about the small/large split the Profiler produces
/// for batching purposes — flatten both buckets into one list.
async fn flat_entries(root: &Path) -> Result<Vec<Entry>> {
    let workload = scan(root, u64::MAX).await?;
    let mut entries = workload.small;
    entries.extend(workload.large);
    Ok(entries)
}

async fn changed(source: &Entry, dest: &Entry, strategy: DiffStrategy, tolerance: Duration) -> Result<bool> {
    match strategy {
        DiffStrategy::SizeAndModifiedTime => {
            // Both timestamps present is the common case, where the
            // tolerance actually matters; either missing falls back to
            // today's untolerant `>` (matches `Option<SystemTime>`'s
            // `PartialOrd`, which already treats `None < Some(_)`) —
            // there's no destination timestamp to be coarse *about* if
            // one side doesn't have one at all.
            let newer = match (source.modified, dest.modified) {
                (Some(source_modified), Some(dest_modified)) => source_modified > dest_modified + tolerance,
                _ => source.modified > dest.modified,
            };
            Ok(source.size != dest.size || newer)
        }
        #[cfg(feature = "checksum")]
        DiffStrategy::Checksum => {
            let source_hash = hash_file(&source.path).await?;
            let dest_hash = hash_file(&dest.path).await?;
            Ok(source_hash != dest_hash)
        }
    }
}

#[cfg(feature = "checksum")]
async fn hash_file(path: &Path) -> Result<blake3::Hash> {
    let bytes = tokio::fs::read(path)
        .await
        .map_err(|e| classify_error(e, path))?;
    Ok(blake3::hash(&bytes))
}

#[cfg(feature = "checksum")]
fn classify_error(err: std::io::Error, path: &Path) -> Error {
    match err.kind() {
        std::io::ErrorKind::NotFound => Error::SourceNotFound { path: path.to_path_buf() },
        std::io::ErrorKind::PermissionDenied => Error::PermissionDenied { path: path.to_path_buf() },
        _ => Error::Io { path: path.to_path_buf(), source: err },
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::{Duration, SystemTime};

    use tempfile::tempdir;

    use super::*;

    fn touch(path: &Path, contents: &[u8], modified: SystemTime) {
        fs::write(path, contents).unwrap();
        filetime::set_file_mtime(path, filetime::FileTime::from_system_time(modified)).unwrap();
    }

    #[tokio::test]
    async fn identical_trees_produce_an_empty_plan() {
        let source = tempdir().unwrap();
        let dest = tempdir().unwrap();
        let t = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);

        touch(&source.path().join("a.txt"), b"same", t);
        touch(&dest.path().join("a.txt"), b"same", t);

        let plan = diff(source.path(), dest.path(), DiffStrategy::SizeAndModifiedTime, Duration::ZERO).await.unwrap();

        assert!(plan.to_copy.is_empty());
        assert!(plan.to_delete.is_empty());
    }

    #[tokio::test]
    async fn entries_only_on_one_side_are_classified_correctly() {
        let source = tempdir().unwrap();
        let dest = tempdir().unwrap();

        fs::write(source.path().join("only_source.txt"), b"x").unwrap();
        fs::write(dest.path().join("only_dest.txt"), b"y").unwrap();

        let plan = diff(source.path(), dest.path(), DiffStrategy::SizeAndModifiedTime, Duration::ZERO).await.unwrap();

        assert_eq!(plan.to_copy.len(), 1);
        assert_eq!(plan.to_copy[0].relative_path, PathBuf::from("only_source.txt"));

        assert_eq!(plan.to_delete.len(), 1);
        assert_eq!(plan.to_delete[0].relative_path, PathBuf::from("only_dest.txt"));
    }

    #[tokio::test]
    async fn size_and_modified_time_flags_a_newer_source_but_not_an_identical_one() {
        let source = tempdir().unwrap();
        let dest = tempdir().unwrap();

        let older = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        let newer = SystemTime::UNIX_EPOCH + Duration::from_secs(2_000);

        touch(&source.path().join("newer.txt"), b"1234", newer);
        touch(&dest.path().join("newer.txt"), b"1234", older);

        touch(&source.path().join("unchanged.txt"), b"1234", older);
        touch(&dest.path().join("unchanged.txt"), b"1234", older);

        let plan = diff(source.path(), dest.path(), DiffStrategy::SizeAndModifiedTime, Duration::ZERO).await.unwrap();

        assert_eq!(plan.to_copy.len(), 1);
        assert_eq!(plan.to_copy[0].relative_path, PathBuf::from("newer.txt"));
        assert!(plan.to_delete.is_empty());
    }

    #[tokio::test]
    async fn tolerance_absorbs_a_difference_within_it() {
        let source = tempdir().unwrap();
        let dest = tempdir().unwrap();
        let base = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);

        // A source mtime 900ms newer than dest — real, but smaller than a
        // FAT/exFAT-style 2-second tolerance would ever be able to
        // represent in the first place.
        touch(&source.path().join("a.txt"), b"same size", base + Duration::from_millis(900));
        touch(&dest.path().join("a.txt"), b"same size", base);

        let plan = diff(source.path(), dest.path(), DiffStrategy::SizeAndModifiedTime, Duration::from_secs(2))
            .await
            .unwrap();

        assert!(plan.to_copy.is_empty(), "a sub-tolerance mtime difference should not be treated as changed");
    }

    #[tokio::test]
    async fn a_difference_beyond_tolerance_is_still_flagged_as_changed() {
        let source = tempdir().unwrap();
        let dest = tempdir().unwrap();
        let base = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);

        touch(&source.path().join("a.txt"), b"same size", base + Duration::from_secs(5));
        touch(&dest.path().join("a.txt"), b"same size", base);

        let plan = diff(source.path(), dest.path(), DiffStrategy::SizeAndModifiedTime, Duration::from_secs(2))
            .await
            .unwrap();

        assert_eq!(plan.to_copy.len(), 1, "a difference larger than the tolerance should still count as changed");
    }

    #[tokio::test]
    async fn zero_tolerance_preserves_exact_comparison() {
        let source = tempdir().unwrap();
        let dest = tempdir().unwrap();
        let base = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);

        touch(&source.path().join("a.txt"), b"same size", base + Duration::from_millis(1));
        touch(&dest.path().join("a.txt"), b"same size", base);

        let plan = diff(source.path(), dest.path(), DiffStrategy::SizeAndModifiedTime, Duration::ZERO).await.unwrap();

        assert_eq!(plan.to_copy.len(), 1, "with zero tolerance, any newer mtime at all should count as changed");
    }

    #[cfg(feature = "checksum")]
    #[tokio::test]
    async fn checksum_catches_a_content_change_that_size_and_mtime_would_miss() {
        let source = tempdir().unwrap();
        let dest = tempdir().unwrap();
        let t = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);

        // Same size, same mtime, different content.
        touch(&source.path().join("a.txt"), b"aaaa", t);
        touch(&dest.path().join("a.txt"), b"bbbb", t);

        let plan = diff(source.path(), dest.path(), DiffStrategy::Checksum, Duration::ZERO).await.unwrap();

        assert_eq!(plan.to_copy.len(), 1);
        assert_eq!(plan.to_copy[0].relative_path, PathBuf::from("a.txt"));
    }

    #[tokio::test]
    async fn every_entry_lands_in_exactly_one_bucket_or_neither() {
        let source = tempdir().unwrap();
        let dest = tempdir().unwrap();
        let t = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);

        touch(&source.path().join("unchanged.txt"), b"same", t);
        touch(&dest.path().join("unchanged.txt"), b"same", t);

        touch(&source.path().join("changed.txt"), b"new", SystemTime::UNIX_EPOCH + Duration::from_secs(2_000));
        touch(&dest.path().join("changed.txt"), b"old", t);

        fs::write(source.path().join("added.txt"), b"added").unwrap();
        fs::write(dest.path().join("removed.txt"), b"removed").unwrap();

        let plan = diff(source.path(), dest.path(), DiffStrategy::SizeAndModifiedTime, Duration::ZERO).await.unwrap();

        let mut copied: Vec<_> = plan.to_copy.iter().map(|e| e.relative_path.clone()).collect();
        copied.sort();
        assert_eq!(copied, vec![PathBuf::from("added.txt"), PathBuf::from("changed.txt")]);

        let mut deleted: Vec<_> = plan.to_delete.iter().map(|e| e.relative_path.clone()).collect();
        deleted.sort();
        assert_eq!(deleted, vec![PathBuf::from("removed.txt")]);
    }

    #[tokio::test]
    async fn missing_dest_means_everything_needs_copying_not_an_error() {
        let source = tempdir().unwrap();
        let dest = tempdir().unwrap();
        let missing_dest = dest.path().join("does-not-exist-yet");

        fs::write(source.path().join("a.txt"), b"a").unwrap();

        let plan = diff(source.path(), &missing_dest, DiffStrategy::SizeAndModifiedTime, Duration::ZERO).await.unwrap();

        assert_eq!(plan.to_copy.len(), 1);
        assert!(plan.to_delete.is_empty());
    }

    #[tokio::test]
    async fn nfc_and_nfd_forms_of_the_same_name_are_matched_not_treated_as_add_and_delete() {
        use unicode_normalization::UnicodeNormalization;

        let source = tempdir().unwrap();
        let dest = tempdir().unwrap();
        let t = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);

        let nfc: String = "café.txt".nfc().collect();
        let nfd: String = "café.txt".nfd().collect();
        assert_ne!(nfc.as_bytes(), nfd.as_bytes(), "test setup: these must start out byte-different");

        // Source writer preserves NFD (e.g. APFS/HFS+); dest writer
        // normalized to NFC on write (e.g. exFAT) — same content, same
        // mtime, genuinely the same file, different byte-level name.
        touch(&source.path().join(&nfd), b"same", t);
        touch(&dest.path().join(&nfc), b"same", t);

        let plan = diff(source.path(), dest.path(), DiffStrategy::SizeAndModifiedTime, Duration::ZERO).await.unwrap();

        assert!(plan.to_copy.is_empty(), "should be matched as the same file, not re-copied");
        assert!(plan.to_delete.is_empty(), "should be matched as the same file, not deleted as an orphan");
    }

    #[tokio::test]
    async fn missing_source_is_still_an_error() {
        let source = tempdir().unwrap();
        let dest = tempdir().unwrap();
        let missing_source = source.path().join("does-not-exist");

        let result = diff(&missing_source, dest.path(), DiffStrategy::SizeAndModifiedTime, Duration::ZERO).await;
        assert!(matches!(result, Err(Error::SourceNotFound { .. })));
    }
}
