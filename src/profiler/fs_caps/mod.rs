use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::error::{Error, Result};

#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;

/// What a specific mounted filesystem can and can't represent, probed
/// once per operation against the destination's containing volume —
/// not per-entry, since these are properties of the mount, not of any
/// individual file. See dev-docs/design/filesystem-detection.md.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FilesystemCapabilities {
    /// Raw OS-reported name (e.g. "apfs", "exfat", "NTFS"), kept only
    /// for error messages/debugging. Every actual decision is made on
    /// the fields below, never by matching this string a second time
    /// outside this module.
    pub name: String,
    pub case_sensitive: bool,
    /// `None` when the filesystem has no meaningful practical ceiling
    /// (everything in scope here except FAT32).
    pub max_file_size: Option<u64>,
    /// Windows/NTFS/exFAT/FAT32's reserved-name and reserved-character
    /// rules apply, regardless of which OS is doing the writing —
    /// empirically confirmed those rules are enforced by Windows's own
    /// API layer, not the on-disk format, so a non-Windows writer can
    /// silently create names that only break later, on Windows. See
    /// "Windows as a first-class destination target" in
    /// dev-docs/design/filesystem-detection.md.
    pub windows_naming_rules: bool,
    /// Smallest reliably-representable gap between two distinct
    /// mtimes. `Duration::ZERO` for filesystems with sub-second
    /// resolution.
    pub timestamp_granularity: Duration,
    /// True only for the specific combination this crate has a
    /// precedented integrity concern about: exFAT, host OS macOS.
    /// See the Bitcoin Core `F_FULLFSYNC` finding in
    /// dev-docs/research/filesystem-limitations.md, section 9.
    pub write_integrity_risk: bool,
}

/// Probes the filesystem containing `path`. If `path` doesn't exist yet
/// (the normal case for a copy/move destination that hasn't been
/// created), walks up to the nearest existing ancestor first — the
/// filesystem is a property of the mount, not the not-yet-created leaf
/// path, so this is exact, not an approximation.
pub(crate) async fn probe(path: &Path) -> Result<FilesystemCapabilities> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || probe_blocking(&path))
        .await
        .expect("filesystem probe blocking task panicked")
}

fn probe_blocking(path: &Path) -> Result<FilesystemCapabilities> {
    let existing = nearest_existing_ancestor(path)?;
    raw_probe(&existing)
}

/// Walks `path` upward until it finds a component that actually
/// exists, so the probe below always has a real path to call into the
/// OS with. A destination that doesn't exist yet always has *some*
/// existing ancestor (at minimum the filesystem root), so the only way
/// this returns an error is a genuinely broken path.
fn nearest_existing_ancestor(path: &Path) -> Result<PathBuf> {
    let mut current = path;
    loop {
        if current.exists() {
            return Ok(current.to_path_buf());
        }
        match current.parent() {
            Some(parent) => current = parent,
            None => return Err(Error::SourceNotFound { path: path.to_path_buf() }),
        }
    }
}

#[cfg(unix)]
fn raw_probe(path: &Path) -> Result<FilesystemCapabilities> {
    unix::probe(path)
}

#[cfg(windows)]
fn raw_probe(path: &Path) -> Result<FilesystemCapabilities> {
    windows::probe(path)
}

#[cfg(not(any(unix, windows)))]
fn raw_probe(_path: &Path) -> Result<FilesystemCapabilities> {
    Ok(FilesystemCapabilities {
        name: "unknown".to_string(),
        case_sensitive: true,
        max_file_size: None,
        windows_naming_rules: false,
        timestamp_granularity: Duration::ZERO,
        write_integrity_risk: false,
    })
}

/// Shared by `unix.rs`/`windows.rs` for the "the probing syscall itself
/// failed" case — matches the classification `scan.rs` already does
/// for its own I/O errors, kept separate rather than shared since both
/// are small and module-local (same pattern as `pipeline.rs`'s own
/// `classify_error`).
#[cfg(any(unix, windows))]
fn classify_io_error(err: io::Error, path: PathBuf) -> Error {
    match err.kind() {
        io::ErrorKind::NotFound => Error::SourceNotFound { path },
        io::ErrorKind::PermissionDenied => Error::PermissionDenied { path },
        _ => Error::Io { path, source: err },
    }
}

/// Filesystem-type classification shared by the unix and windows
/// probes: given the OS-reported filesystem name (already
/// lowercased), what does everything except case-sensitivity look
/// like. Case-sensitivity is deliberately not decided here — it comes
/// from a platform-specific call (`pathconf`/`GetVolumeInformationW`),
/// not from name matching, since it can vary per-volume even for the
/// same filesystem type (e.g. an explicitly case-sensitive APFS
/// volume).
fn classify_by_name(name: &str, host_is_macos: bool) -> (Option<u64>, bool, Duration, bool) {
    match name {
        "exfat" => {
            let write_integrity_risk = host_is_macos;
            (None, true, Duration::from_secs(2), write_integrity_risk)
        }
        // FAT32/FAT16 family: macOS reports "msdos", Linux reports
        // "vfat"/"msdos" depending on driver, Windows reports "FAT32".
        "msdos" | "vfat" | "fat32" | "fat16" | "fat" => {
            (Some(4_294_967_295), true, Duration::from_secs(2), false)
        }
        "ntfs" => (None, true, Duration::ZERO, false),
        // APFS/HFS+ and every native Unix filesystem this crate has a
        // name for: no practical size ceiling, no Windows naming
        // rules, sub-second timestamps, no known integrity risk.
        "apfs" | "hfs" | "ext2" | "ext3" | "ext4" | "btrfs" | "xfs" => {
            (None, false, Duration::ZERO, false)
        }
        // Unrecognized (network shares, exotic/FUSE filesystems,
        // anything not in the research this module is based on):
        // conservative in the direction of "assume no extra
        // protection is needed" rather than guessing — false
        // positives here would block operations against filesystems
        // this crate has no actual evidence about. See "Open items"
        // in dev-docs/design/filesystem-detection.md.
        _ => (None, false, Duration::ZERO, false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exfat_on_macos_is_flagged_as_integrity_risk() {
        let (_, windows_naming, granularity, risk) = classify_by_name("exfat", true);
        assert!(windows_naming);
        assert_eq!(granularity, Duration::from_secs(2));
        assert!(risk);
    }

    #[test]
    fn exfat_on_non_macos_is_not_flagged_as_integrity_risk() {
        let (_, _, _, risk) = classify_by_name("exfat", false);
        assert!(!risk);
    }

    #[test]
    fn fat32_has_the_four_gigabyte_ceiling() {
        let (max_size, windows_naming, granularity, risk) = classify_by_name("msdos", false);
        assert_eq!(max_size, Some(4_294_967_295));
        assert!(windows_naming);
        assert_eq!(granularity, Duration::from_secs(2));
        assert!(!risk);
    }

    #[test]
    fn apfs_has_no_special_restrictions() {
        let (max_size, windows_naming, granularity, risk) = classify_by_name("apfs", true);
        assert_eq!(max_size, None);
        assert!(!windows_naming);
        assert_eq!(granularity, Duration::ZERO);
        assert!(!risk);
    }

    #[test]
    fn unrecognized_filesystem_gets_conservative_defaults() {
        let (max_size, windows_naming, granularity, risk) = classify_by_name("zfs", false);
        assert_eq!(max_size, None);
        assert!(!windows_naming);
        assert_eq!(granularity, Duration::ZERO);
        assert!(!risk);
    }

    #[tokio::test]
    async fn probing_a_path_that_does_not_exist_yet_walks_up_to_an_existing_ancestor() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("does").join("not").join("exist");

        let caps = probe(&missing).await.unwrap();
        // Whatever filesystem the tempdir itself lives on — just
        // confirming this didn't error out on the missing leaf path.
        assert!(!caps.name.is_empty());
    }

    #[tokio::test]
    async fn probing_an_existing_path_directly_succeeds() {
        let dir = tempfile::tempdir().unwrap();
        let caps = probe(dir.path()).await.unwrap();
        assert!(!caps.name.is_empty());
    }
}
