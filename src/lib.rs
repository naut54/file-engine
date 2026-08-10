mod error;
#[cfg(feature = "operations")]
mod handle;
// `sync`/`compress` already imply `operations` via Cargo.toml, but
// `watch` deliberately doesn't (it never touches the
// Profiler/Planner/Dispatcher pipeline) — this needs its own condition
// rather than reusing the `operations` feature alone, or `watch`-only
// builds fail to find this module at all.
#[cfg(any(feature = "operations", feature = "watch"))]
mod operations;
// Also intended for `sync`'s `diff.rs` once that's wired up to use it
// too (dev-docs/design/filesystem-detection.md, item 6) — currently only
// called from `profiler::validate`.
#[cfg(feature = "operations")]
mod paths;
#[cfg(feature = "operations")]
mod planner;
#[cfg(feature = "operations")]
mod profiler;
#[cfg(feature = "operations")]
mod progress;
#[cfg(feature = "watch")]
mod watch_event;
#[cfg(feature = "watch")]
mod watch_handle;

pub use error::{Error, Result};
#[cfg(feature = "operations")]
pub use handle::Handle;
#[cfg(feature = "operations")]
pub use progress::Progress;
#[cfg(feature = "watch")]
pub use watch_event::{WatchEvent, WatchEventKind};
#[cfg(feature = "watch")]
pub use watch_handle::WatchHandle;

#[cfg(feature = "sync")]
pub use operations::diff::DiffStrategy;
#[cfg(feature = "operations")]
pub use operations::CopyBuilder;
#[cfg(feature = "operations")]
pub use operations::MoveBuilder;
#[cfg(feature = "watch")]
pub use operations::WatchBuilder;
#[cfg(feature = "compress")]
pub use operations::{CompressBuilder, CompressFormat};
#[cfg(feature = "sync")]
pub use operations::{SyncBuilder, SyncOutcome};

// These aren't just re-exports of convenience — `ErrorStrategy`,
// `SortOrder`, and `DiffStrategy` are parameter types on the builders'
// public methods (`on_error`, `batch_sort_order`, `diff_strategy`), and
// `OperationOutcome`/`StopReason`/`Entry` appear in the values those
// builders return. Without these, callers outside this crate can't name
// the types needed to call those methods or destructure the results,
// even though `planner`/`profiler` mark them `pub`.
#[cfg(feature = "operations")]
pub use planner::{ErrorStrategy, OperationOutcome, SortOrder, StopReason};
#[cfg(feature = "operations")]
pub use profiler::Entry;

pub struct FileEngine;

impl Default for FileEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl FileEngine {
    pub fn new() -> Self {
        FileEngine
    }

    #[cfg(feature = "operations")]
    pub fn copy(
        &self,
        source: impl Into<std::path::PathBuf>,
        dest: impl Into<std::path::PathBuf>,
    ) -> CopyBuilder {
        CopyBuilder::new(source, dest)
    }

    #[cfg(feature = "operations")]
    pub fn move_path(
        &self,
        source: impl Into<std::path::PathBuf>,
        dest: impl Into<std::path::PathBuf>,
    ) -> MoveBuilder {
        MoveBuilder::new(source, dest)
    }

    #[cfg(feature = "watch")]
    pub fn watch(&self, path: impl Into<std::path::PathBuf>) -> WatchBuilder {
        WatchBuilder::new(path)
    }

    #[cfg(feature = "sync")]
    pub fn sync(
        &self,
        source: impl Into<std::path::PathBuf>,
        dest: impl Into<std::path::PathBuf>,
    ) -> SyncBuilder {
        SyncBuilder::new(source, dest)
    }

    #[cfg(feature = "compress")]
    pub fn compress(
        &self,
        source: impl Into<std::path::PathBuf>,
        dest: impl Into<std::path::PathBuf>,
    ) -> CompressBuilder {
        CompressBuilder::new(source, dest)
    }
}

#[cfg(all(test, feature = "operations", feature = "sync", feature = "compress"))]
mod tests {
    use std::fs;

    use tempfile::tempdir;
    use tokio_stream::StreamExt;

    use super::*;

    /// Confirms the event sequence contract from
    /// dev-docs/design/handle-progress.md's `.start()` test list: a `Started`
    /// before any `EntryStarted`, its `entries_total` matching what
    /// actually ran, and one terminal event (`EntryCompleted` or
    /// `EntryFailed`) per entry.
    fn assert_well_formed(events: &[Progress], expected_entries: usize) {
        let started_at = events
            .iter()
            .position(|e| matches!(e, Progress::Started { .. }));
        assert!(started_at.is_some(), "expected a Started event");

        if let Some(first_entry_started) = events
            .iter()
            .position(|e| matches!(e, Progress::EntryStarted { .. }))
        {
            assert!(
                started_at.unwrap() < first_entry_started,
                "Started must come before any EntryStarted"
            );
        }

        let entries_total = events
            .iter()
            .find_map(|e| match e {
                Progress::Started { entries_total, .. } => Some(*entries_total),
                _ => None,
            })
            .unwrap();
        assert_eq!(entries_total, expected_entries);

        let terminal_count = events
            .iter()
            .filter(|e| {
                matches!(
                    e,
                    Progress::EntryCompleted { .. } | Progress::EntryFailed { .. }
                )
            })
            .count();
        assert_eq!(terminal_count, expected_entries);
    }

    #[tokio::test]
    async fn copy_end_to_end_through_the_public_api() {
        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();
        fs::write(src_dir.path().join("a.txt"), b"hello").unwrap();

        let engine = FileEngine::new();
        let mut handle = engine
            .copy(src_dir.path(), dest_dir.path())
            .start()
            .unwrap();

        let mut events = Vec::new();
        while let Some(event) = handle.progress().next().await {
            events.push(event);
        }

        let outcome = handle.await.unwrap();

        assert_eq!(outcome.succeeded.len(), 1);
        assert_eq!(fs::read(dest_dir.path().join("a.txt")).unwrap(), b"hello");
        assert_well_formed(&events, 1);
    }

    #[tokio::test]
    async fn move_end_to_end_through_the_public_api() {
        // Both paths under one tempdir, guaranteeing the same filesystem
        // so this exercises the atomic-rename fast path (matches
        // move_path.rs's own same-filesystem test).
        let root = tempdir().unwrap();
        let src_file = root.path().join("a.txt");
        let dest_file = root.path().join("dst.txt");
        fs::write(&src_file, b"hello").unwrap();

        let engine = FileEngine::new();
        let handle = engine
            .move_path(root.path().join("a.txt"), dest_file.clone())
            .start()
            .unwrap();
        let outcome = handle.await.unwrap();

        // Fast path enumerates no entries — matches move_path.rs's tests.
        assert!(outcome.succeeded.is_empty());
        assert!(!src_file.exists());
        assert_eq!(fs::read(&dest_file).unwrap(), b"hello");
    }

    #[tokio::test]
    async fn sync_end_to_end_through_the_public_api() {
        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();
        fs::write(src_dir.path().join("new.txt"), b"new").unwrap();
        fs::write(dest_dir.path().join("orphan.txt"), b"stale").unwrap();

        let engine = FileEngine::new();
        let handle = engine
            .sync(src_dir.path(), dest_dir.path())
            .start()
            .unwrap();
        let outcome = handle.await.unwrap();

        assert_eq!(outcome.copy.succeeded.len(), 1);
        assert_eq!(outcome.delete.succeeded.len(), 1);
        assert_eq!(fs::read(dest_dir.path().join("new.txt")).unwrap(), b"new");
        assert!(!dest_dir.path().join("orphan.txt").exists());
    }

    #[tokio::test]
    async fn compress_end_to_end_through_the_public_api() {
        let src_dir = tempdir().unwrap();
        let out_dir = tempdir().unwrap();
        fs::write(src_dir.path().join("a.txt"), b"a").unwrap();
        let dest = out_dir.path().join("archive.zip");

        let engine = FileEngine::new();
        let mut handle = engine.compress(src_dir.path(), &dest).start().unwrap();

        let mut events = Vec::new();
        while let Some(event) = handle.progress().next().await {
            events.push(event);
        }

        let outcome = handle.await.unwrap();

        assert_eq!(outcome.succeeded.len(), 1);
        assert!(dest.exists());
        assert_well_formed(&events, 1);
    }
}
