use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use walkdir::WalkDir;

use crate::error::{Error, Result};

use super::workload::{DirEntry, Entry, Workload};

#[cfg(unix)]
fn mode_of(metadata: &fs::Metadata) -> Option<u32> {
    use std::os::unix::fs::PermissionsExt;
    Some(metadata.permissions().mode())
}

#[cfg(not(unix))]
fn mode_of(_metadata: &fs::Metadata) -> Option<u32> {
    None
}

pub const DEFAULT_SMALL_FILE_THRESHOLD: u64 = 256 * 1024;

/// Walks `root` and classifies every entry as small/large relative to
/// `threshold`. If `root` is a file rather than a directory, `walkdir` is
/// skipped and a one-entry `Workload` is returned directly — see
/// dev-docs/design/batching-engine.md, "scan.rs" — so single-file and
/// directory sources share the exact same downstream path.
pub(crate) async fn scan(root: &Path, threshold: u64) -> Result<Workload> {
    let root = root.to_path_buf();
    tokio::task::spawn_blocking(move || scan_blocking(&root, threshold))
        .await
        .expect("scan blocking task panicked")
}

fn scan_blocking(root: &Path, threshold: u64) -> Result<Workload> {
    // The root itself is resolved following symlinks (it's the explicit
    // starting point the caller gave, not something discovered mid-walk),
    // matching how tools like `cp`/`rsync` treat their top-level argument.
    let metadata = fs::metadata(root).map_err(|e| classify_io_error(e, root.to_path_buf()))?;

    if metadata.is_file() {
        let relative_path = root
            .file_name()
            .map(PathBuf::from)
            .unwrap_or_else(|| root.to_path_buf());
        let entry = Entry {
            path: root.to_path_buf(),
            relative_path,
            size: metadata.len(),
            modified: metadata.modified().ok(),
        };
        return Ok(Workload::partition(vec![entry], threshold));
    }

    // Symlinks (to files or directories) are skipped, not followed:
    // `DirEntry::file_type()` reports the entry's own type (symlink),
    // never the target's, so filtering on `is_file()`/`is_dir()`
    // naturally excludes them without special-casing — this also means
    // walkdir never recurses into a symlinked directory, avoiding cycles.
    let mut entries = Vec::new();
    let mut directories = Vec::new();
    for result in WalkDir::new(root).into_iter() {
        let walk_entry = result.map_err(classify_walkdir_error)?;
        let file_type = walk_entry.file_type();

        let relative_path = walk_entry
            .path()
            .strip_prefix(root)
            .unwrap_or_else(|_| walk_entry.path())
            .to_path_buf();

        if file_type.is_dir() {
            let dir_metadata = walk_entry.metadata().map_err(classify_walkdir_error)?;
            directories.push(DirEntry {
                path: walk_entry.path().to_path_buf(),
                relative_path,
                mode: mode_of(&dir_metadata),
            });
            continue;
        }

        if !file_type.is_file() {
            continue;
        }

        let file_metadata = walk_entry.metadata().map_err(classify_walkdir_error)?;

        entries.push(Entry {
            path: walk_entry.path().to_path_buf(),
            relative_path,
            size: file_metadata.len(),
            modified: file_metadata.modified().ok(),
        });
    }

    let mut workload = Workload::partition(entries, threshold);
    workload.directories = directories;
    Ok(workload)
}

fn classify_io_error(err: io::Error, path: PathBuf) -> Error {
    match err.kind() {
        io::ErrorKind::NotFound => Error::SourceNotFound { path },
        io::ErrorKind::PermissionDenied => Error::PermissionDenied { path },
        io::ErrorKind::StorageFull => Error::NoSpace {
            needed: 0,
            available: 0,
        },
        _ => Error::Io { path, source: err },
    }
}

fn classify_walkdir_error(err: walkdir::Error) -> Error {
    let path = err.path().map(|p| p.to_path_buf());
    match err.into_io_error() {
        Some(io_err) => classify_io_error(io_err, path.unwrap_or_default()),
        None => Error::Io {
            path: path.unwrap_or_default(),
            source: io::Error::other("directory walk error"),
        },
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[tokio::test]
    async fn single_file_root_produces_one_entry_workload() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("only.txt");
        fs::write(&file_path, vec![0u8; 100]).unwrap();

        let workload = scan(&file_path, 256).await.unwrap();

        assert_eq!(workload.small.len() + workload.large.len(), 1);
        let entry = &workload.small[0];
        assert_eq!(entry.path, file_path);
        assert_eq!(entry.relative_path, PathBuf::from("only.txt"));
        assert_eq!(entry.size, 100);
    }

    #[tokio::test]
    async fn directory_with_mixed_sizes_classifies_correctly() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("small.txt"), vec![0u8; 10]).unwrap();
        fs::create_dir(dir.path().join("nested")).unwrap();
        fs::write(dir.path().join("nested").join("large.txt"), vec![0u8; 1000]).unwrap();

        let workload = scan(dir.path(), 256).await.unwrap();

        assert_eq!(workload.small.len(), 1);
        assert_eq!(workload.small[0].relative_path, PathBuf::from("small.txt"));
        assert_eq!(workload.small[0].size, 10);

        assert_eq!(workload.large.len(), 1);
        assert_eq!(
            workload.large[0].relative_path,
            PathBuf::from("nested").join("large.txt")
        );
        assert_eq!(workload.large[0].size, 1000);
    }

    #[tokio::test]
    async fn empty_directory_produces_empty_workload() {
        let dir = tempdir().unwrap();
        let workload = scan(dir.path(), 256).await.unwrap();
        assert_eq!(workload.small.len() + workload.large.len(), 0);
    }

    #[tokio::test]
    async fn directory_of_only_subdirectories_produces_empty_workload() {
        let dir = tempdir().unwrap();
        fs::create_dir_all(dir.path().join("a").join("b").join("c")).unwrap();

        let workload = scan(dir.path(), 256).await.unwrap();
        assert_eq!(workload.small.len() + workload.large.len(), 0);
    }

    #[tokio::test]
    async fn nonexistent_path_returns_error_instead_of_panicking_or_empty_workload() {
        let dir = tempdir().unwrap();
        let missing = dir.path().join("does-not-exist");

        let result = scan(&missing, 256).await;
        assert!(matches!(result, Err(Error::SourceNotFound { .. })));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn symlinks_are_skipped_not_followed() {
        let dir = tempdir().unwrap();
        let target = dir.path().join("real.txt");
        fs::write(&target, vec![0u8; 10]).unwrap();

        let link = dir.path().join("link.txt");
        std::os::unix::fs::symlink(&target, &link).unwrap();

        let workload = scan(dir.path(), 256).await.unwrap();

        assert_eq!(workload.small.len() + workload.large.len(), 1);
        assert_eq!(workload.small[0].relative_path, PathBuf::from("real.txt"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn directory_mode_is_captured() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempdir().unwrap();
        let subdir = dir.path().join("nested");
        fs::create_dir(&subdir).unwrap();
        fs::set_permissions(&subdir, fs::Permissions::from_mode(0o700)).unwrap();

        let workload = scan(dir.path(), 256).await.unwrap();

        let nested = workload
            .directories
            .iter()
            .find(|d| d.relative_path == Path::new("nested"))
            .expect("nested directory should have been captured");
        // Masked to the permission bits: `mode()` can also report
        // file-type bits mixed in on some platforms.
        assert_eq!(nested.mode.unwrap() & 0o7777, 0o700);
    }

    #[tokio::test]
    async fn scanned_root_appears_in_directories_with_empty_relative_path() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("a.txt"), b"a").unwrap();

        let workload = scan(dir.path(), 256).await.unwrap();

        assert!(workload
            .directories
            .iter()
            .any(|d| d.relative_path == PathBuf::new()));
    }

    #[tokio::test]
    async fn single_file_root_produces_no_directories() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("only.txt");
        fs::write(&file_path, b"hello").unwrap();

        let workload = scan(&file_path, 256).await.unwrap();

        assert!(workload.directories.is_empty());
    }

    #[tokio::test]
    async fn empty_directory_produces_one_directory_entry_for_the_root() {
        let dir = tempdir().unwrap();
        let workload = scan(dir.path(), 256).await.unwrap();
        assert_eq!(workload.directories.len(), 1);
        assert_eq!(workload.directories[0].relative_path, PathBuf::new());
    }
}
