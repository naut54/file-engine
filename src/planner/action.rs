use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;

use crate::error::{classify_io_error, Error, Result};
use crate::profiler::Entry;

/// Lets the dispatcher stay generic over what happens to an entry (copy,
/// move-via-copy, ...) instead of hardcoding filesystem operations.
///
/// Dispatch is generic over `A: EntryAction` (static dispatch) rather than
/// `dyn EntryAction`, since a single plan execution only ever uses one
/// concrete action.
///
/// Methods return boxed futures explicitly, rather than using native
/// `async fn` in the trait: the dispatcher spawns these onto `JoinSet`
/// (which requires `Send` futures), and native async-fn-in-trait doesn't
/// carry a `Send` bound through a generic `A: EntryAction` — the compiler
/// can't prove it without either this, or a `-> impl Future + Send`
/// return type (unstable in traits as of this crate's MSRV) or an extra
/// dependency (`trait_variant`). Boxing costs one small heap allocation
/// per entry, which is negligible next to the I/O each call performs.
pub(crate) trait EntryAction: Send + Sync {
    fn execute<'a>(
        &'a self,
        entry: &'a Entry,
        dest_root: &'a Path,
    ) -> Pin<Box<dyn Future<Output = Result<EntryOutcome>> + Send + 'a>>;
    fn undo<'a>(
        &'a self,
        entry: &'a Entry,
        dest_root: &'a Path,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

    /// The file whose growth tracks this entry's progress, if the action
    /// has one. The dispatcher samples its size while a streamed entry is
    /// in flight, to emit `Progress::EntryProgress`.
    ///
    /// Defaulted to `None` — an action with no single growing destination
    /// file simply reports no intermediate progress, rather than being
    /// forced to invent one. Kept on the trait, rather than derived in the
    /// dispatcher, so that the mapping from entry to destination path
    /// stays owned by the action that performs the write.
    fn progress_target(&self, _entry: &Entry, _dest_root: &Path) -> Option<PathBuf> {
        None
    }
}

/// What `EntryAction::execute` actually did, distinct from a bare
/// success so the dispatcher (and `OperationOutcome`) can tell "wrote
/// the destination" apart from "left it alone because it already
/// matched" — the latter goes to `OperationOutcome::skipped`, not
/// `succeeded`, since no bytes were transferred.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EntryOutcome {
    Written,
    Skipped,
}

#[derive(Debug, Clone)]
pub(crate) struct CopyAction {
    pub overwrite: bool,
    /// Only ever `true` when the `checksum` feature is enabled (the one
    /// builder method that sets it is cfg-gated) — kept as a plain,
    /// unconditional `bool` field regardless, same reasoning as
    /// `preserve_permissions` elsewhere: a platform/feature-conditional
    /// struct shape isn't worth it for a single bool. Genuinely unread
    /// without `checksum` (the fallback `identical_to_existing` below
    /// never looks at it), hence the `allow`.
    #[cfg_attr(not(feature = "checksum"), allow(dead_code))]
    pub skip_if_identical: bool,
}

impl CopyAction {
    /// Whether `entry`'s source and the already-existing `dest_path`
    /// (whose length the caller already has from its own `metadata()`
    /// call) are byte-identical. Only ever consulted when `!overwrite`
    /// and `dest_path` exists — `skip_if_identical` is meaningless
    /// otherwise. No-op fallback when `checksum` is disabled, so
    /// `execute()` below doesn't need its own `#[cfg]` — same pattern as
    /// `operations/pipeline.rs`'s `apply_directory_permissions`.
    #[cfg(feature = "checksum")]
    async fn identical_to_existing(
        &self,
        entry: &Entry,
        dest_path: &Path,
        dest_len: u64,
    ) -> Result<bool> {
        if !self.skip_if_identical {
            return Ok(false);
        }
        crate::checksum::files_identical(&entry.path, dest_path, entry.size, dest_len).await
    }

    #[cfg(not(feature = "checksum"))]
    async fn identical_to_existing(
        &self,
        _entry: &Entry,
        _dest_path: &Path,
        _dest_len: u64,
    ) -> Result<bool> {
        Ok(false)
    }
}

impl EntryAction for CopyAction {
    fn execute<'a>(
        &'a self,
        entry: &'a Entry,
        dest_root: &'a Path,
    ) -> Pin<Box<dyn Future<Output = Result<EntryOutcome>> + Send + 'a>> {
        Box::pin(async move {
            let dest_path = dest_root.join(&entry.relative_path);

            // create_dir_all treats "already exists as a directory" as
            // success (including when another worker's concurrent call
            // raced to create it first), so no special AlreadyExists
            // handling is needed here.
            if let Some(parent) = dest_path.parent() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .map_err(|e| classify_io_error(e, parent.to_path_buf(), 0))?;
            }

            if !self.overwrite {
                match tokio::fs::metadata(&dest_path).await {
                    Ok(existing) => {
                        if self
                            .identical_to_existing(entry, &dest_path, existing.len())
                            .await?
                        {
                            return Ok(EntryOutcome::Skipped);
                        }
                        return Err(Error::DestExists { path: dest_path });
                    }
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    Err(e) => return Err(classify_io_error(e, dest_path, 0)),
                }
            }

            // `std::fs::copy` (which this wraps) already copies the
            // source's permission bits to the destination unconditionally
            // — verified empirically, not assumed. No separate
            // preserve-permissions step needed for files: that feature's
            // only real effect is on directories, not files.
            tokio::fs::copy(&entry.path, &dest_path)
                .await
                .map(|_| EntryOutcome::Written)
                .map_err(|e| classify_io_error(e, entry.path.clone(), entry.size))
        })
    }

    fn progress_target(&self, entry: &Entry, dest_root: &Path) -> Option<PathBuf> {
        Some(dest_root.join(&entry.relative_path))
    }

    fn undo<'a>(
        &'a self,
        entry: &'a Entry,
        dest_root: &'a Path,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let dest_path = dest_root.join(&entry.relative_path);
            match tokio::fs::remove_file(&dest_path).await {
                Ok(()) => Ok(()),
                Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
                Err(e) => Err(classify_io_error(e, dest_path, 0)),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use tempfile::tempdir;

    use super::*;

    fn entry(path: PathBuf, relative_path: PathBuf, size: u64) -> Entry {
        Entry {
            path,
            relative_path,
            size,
            modified: None,
        }
    }

    #[tokio::test]
    async fn execute_copies_bytes_and_creates_missing_parent_dirs() {
        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();

        let src_path = src_dir.path().join("file.txt");
        fs::write(&src_path, b"hello world").unwrap();

        let relative_path = PathBuf::from("nested/deep/file.txt");
        let e = entry(src_path, relative_path.clone(), 11);

        let action = CopyAction {
            overwrite: false,
            skip_if_identical: false,
        };
        action.execute(&e, dest_dir.path()).await.unwrap();

        let dest_path = dest_dir.path().join(&relative_path);
        assert_eq!(fs::read(&dest_path).unwrap(), b"hello world");
    }

    #[tokio::test]
    async fn execute_without_overwrite_fails_on_existing_destination() {
        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();

        let src_path = src_dir.path().join("file.txt");
        fs::write(&src_path, b"new content").unwrap();

        let relative_path = PathBuf::from("file.txt");
        let dest_path = dest_dir.path().join(&relative_path);
        fs::write(&dest_path, b"old content").unwrap();

        let e = entry(src_path, relative_path, 11);
        let action = CopyAction {
            overwrite: false,
            skip_if_identical: false,
        };

        let result = action.execute(&e, dest_dir.path()).await;
        assert!(matches!(result, Err(Error::DestExists { .. })));
        assert_eq!(fs::read(&dest_path).unwrap(), b"old content");
    }

    #[tokio::test]
    async fn execute_with_overwrite_replaces_existing_destination() {
        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();

        let src_path = src_dir.path().join("file.txt");
        fs::write(&src_path, b"new content").unwrap();

        let relative_path = PathBuf::from("file.txt");
        let dest_path = dest_dir.path().join(&relative_path);
        fs::write(&dest_path, b"old content").unwrap();

        let e = entry(src_path, relative_path, 11);
        let action = CopyAction {
            overwrite: true,
            skip_if_identical: false,
        };

        action.execute(&e, dest_dir.path()).await.unwrap();
        assert_eq!(fs::read(&dest_path).unwrap(), b"new content");
    }

    #[cfg(feature = "checksum")]
    #[tokio::test]
    async fn skip_if_identical_leaves_a_matching_destination_untouched() {
        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();

        let src_path = src_dir.path().join("file.txt");
        fs::write(&src_path, b"same content").unwrap();

        let relative_path = PathBuf::from("file.txt");
        let dest_path = dest_dir.path().join(&relative_path);
        fs::write(&dest_path, b"same content").unwrap();
        let dest_modified_before = fs::metadata(&dest_path).unwrap().modified().unwrap();

        let e = entry(src_path, relative_path, 12);
        let action = CopyAction {
            overwrite: false,
            skip_if_identical: true,
        };

        let outcome = action.execute(&e, dest_dir.path()).await.unwrap();
        assert_eq!(outcome, EntryOutcome::Skipped);
        assert_eq!(fs::read(&dest_path).unwrap(), b"same content");
        assert_eq!(
            fs::metadata(&dest_path).unwrap().modified().unwrap(),
            dest_modified_before,
            "an identical destination must not be rewritten"
        );
    }

    #[cfg(feature = "checksum")]
    #[tokio::test]
    async fn skip_if_identical_still_fails_on_a_genuinely_different_destination() {
        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();

        let src_path = src_dir.path().join("file.txt");
        fs::write(&src_path, b"new content").unwrap();

        let relative_path = PathBuf::from("file.txt");
        let dest_path = dest_dir.path().join(&relative_path);
        fs::write(&dest_path, b"old content").unwrap();

        let e = entry(src_path, relative_path, 11);
        let action = CopyAction {
            overwrite: false,
            skip_if_identical: true,
        };

        let result = action.execute(&e, dest_dir.path()).await;
        assert!(matches!(result, Err(Error::DestExists { .. })));
        assert_eq!(fs::read(&dest_path).unwrap(), b"old content");
    }

    #[tokio::test]
    async fn concurrent_parent_dir_creation_both_succeed() {
        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();

        let src_path_a = src_dir.path().join("a.txt");
        let src_path_b = src_dir.path().join("b.txt");
        fs::write(&src_path_a, b"a").unwrap();
        fs::write(&src_path_b, b"b").unwrap();

        let entry_a = entry(src_path_a, PathBuf::from("shared/a.txt"), 1);
        let entry_b = entry(src_path_b, PathBuf::from("shared/b.txt"), 1);

        let action = CopyAction {
            overwrite: false,
            skip_if_identical: false,
        };
        let dest_root = dest_dir.path().to_path_buf();

        let (result_a, result_b) = tokio::join!(
            action.execute(&entry_a, &dest_root),
            action.execute(&entry_b, &dest_root),
        );

        result_a.unwrap();
        result_b.unwrap();
        assert!(dest_dir.path().join("shared/a.txt").exists());
        assert!(dest_dir.path().join("shared/b.txt").exists());
    }

    #[tokio::test]
    async fn undo_removes_exactly_the_destination_it_created() {
        let src_dir = tempdir().unwrap();
        let dest_dir = tempdir().unwrap();

        let src_path = src_dir.path().join("file.txt");
        fs::write(&src_path, b"data").unwrap();

        let relative_path = PathBuf::from("file.txt");
        let e = entry(src_path, relative_path.clone(), 4);

        // A sibling file that undo must not touch.
        let sibling = dest_dir.path().join("sibling.txt");
        fs::write(&sibling, b"leave me alone").unwrap();

        let action = CopyAction {
            overwrite: false,
            skip_if_identical: false,
        };
        action.execute(&e, dest_dir.path()).await.unwrap();

        let dest_path = dest_dir.path().join(&relative_path);
        assert!(dest_path.exists());

        action.undo(&e, dest_dir.path()).await.unwrap();

        assert!(!dest_path.exists());
        assert!(sibling.exists());
    }
}
