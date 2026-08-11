use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;

use crate::error::{Error, Result};
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
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
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

#[derive(Debug, Clone)]
pub(crate) struct CopyAction {
    pub overwrite: bool,
}

impl EntryAction for CopyAction {
    fn execute<'a>(
        &'a self,
        entry: &'a Entry,
        dest_root: &'a Path,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let dest_path = dest_root.join(&entry.relative_path);

            // create_dir_all treats "already exists as a directory" as
            // success (including when another worker's concurrent call
            // raced to create it first), so no special AlreadyExists
            // handling is needed here.
            if let Some(parent) = dest_path.parent() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .map_err(|e| classify_error(e, parent, 0))?;
            }

            if !self.overwrite {
                match tokio::fs::metadata(&dest_path).await {
                    Ok(_) => return Err(Error::DestExists { path: dest_path }),
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    Err(e) => return Err(classify_error(e, &dest_path, 0)),
                }
            }

            // `std::fs::copy` (which this wraps) already copies the
            // source's permission bits to the destination unconditionally
            // — verified empirically, not assumed. No separate
            // preserve-permissions step needed for files: that feature's
            // only real effect is on directories, not files.
            tokio::fs::copy(&entry.path, &dest_path)
                .await
                .map(|_| ())
                .map_err(|e| classify_error(e, &entry.path, entry.size))
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
                Err(e) => Err(classify_error(e, &dest_path, 0)),
            }
        })
    }
}

/// Maps a raw `io::Error` onto the crate's `Error` variants. `needed` is
/// only meaningful for `StorageFull`; `available` isn't queried at this
/// level (would need an extra statvfs-style syscall) so it's reported as
/// 0 rather than fabricated.
fn classify_error(err: io::Error, path: &Path, needed: u64) -> Error {
    match err.kind() {
        io::ErrorKind::NotFound => Error::SourceNotFound {
            path: path.to_path_buf(),
        },
        io::ErrorKind::PermissionDenied => Error::PermissionDenied {
            path: path.to_path_buf(),
        },
        io::ErrorKind::StorageFull => Error::NoSpace {
            needed,
            available: 0,
        },
        _ => Error::Io {
            path: path.to_path_buf(),
            source: err,
        },
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

        let action = CopyAction { overwrite: false };
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
        let action = CopyAction { overwrite: false };

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
        let action = CopyAction { overwrite: true };

        action.execute(&e, dest_dir.path()).await.unwrap();
        assert_eq!(fs::read(&dest_path).unwrap(), b"new content");
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

        let action = CopyAction { overwrite: false };
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

        let action = CopyAction { overwrite: false };
        action.execute(&e, dest_dir.path()).await.unwrap();

        let dest_path = dest_dir.path().join(&relative_path);
        assert!(dest_path.exists());

        action.undo(&e, dest_dir.path()).await.unwrap();

        assert!(!dest_path.exists());
        assert!(sibling.exists());
    }
}
