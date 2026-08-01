use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use crate::error::Result;

use super::{classify_by_name, classify_io_error, FilesystemCapabilities};

pub(super) fn probe(path: &Path) -> Result<FilesystemCapabilities> {
    let c_path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| crate::error::Error::SourceNotFound { path: path.to_path_buf() })?;

    let name = fstype_name(&c_path, path)?;
    let case_sensitive = case_sensitive(&c_path);
    let host_is_macos = cfg!(target_os = "macos");
    let (max_file_size, windows_naming_rules, timestamp_granularity, write_integrity_risk) =
        classify_by_name(&name, host_is_macos);

    Ok(FilesystemCapabilities {
        name,
        case_sensitive,
        max_file_size,
        windows_naming_rules,
        timestamp_granularity,
        write_integrity_risk,
    })
}

/// macOS's `statfs` reports the filesystem type as a direct string
/// (`f_fstypename`) — verified empirically (`f_fstypename = "apfs"`
/// on an APFS volume, `"exfat"` on an exFAT external drive), no
/// mapping table needed.
#[cfg(target_os = "macos")]
fn fstype_name(c_path: &std::ffi::CStr, path: &Path) -> Result<String> {
    use std::ffi::CStr;

    unsafe {
        let mut buf: libc::statfs = std::mem::zeroed();
        if libc::statfs(c_path.as_ptr(), &mut buf) != 0 {
            return Err(classify_io_error(std::io::Error::last_os_error(), path.to_path_buf()));
        }
        Ok(CStr::from_ptr(buf.f_fstypename.as_ptr()).to_string_lossy().into_owned())
    }
}

/// Linux's `statfs` only reports a numeric magic number (`f_type`),
/// not a name — mapped against the handful of filesystems this crate
/// actually has research on (`dev-docs/research/filesystem-limitations.md`).
/// FUSE-mounted filesystems (`ntfs-3g`, `exfat-fuse`) all report the
/// same generic FUSE magic and can't be told apart this way; they fall
/// through to "unrecognized" (conservative defaults) rather than a
/// guess. Not empirically verified on Linux — no Linux machine
/// available in the environment this was built in; see "Open items"
/// in dev-docs/design/filesystem-detection.md.
#[cfg(target_os = "linux")]
fn fstype_name(c_path: &std::ffi::CStr, path: &Path) -> Result<String> {
    unsafe {
        let mut buf: libc::statfs = std::mem::zeroed();
        if libc::statfs(c_path.as_ptr(), &mut buf) != 0 {
            return Err(classify_io_error(std::io::Error::last_os_error(), path.to_path_buf()));
        }
        Ok(magic_to_name(buf.f_type as i64).to_string())
    }
}

#[cfg(target_os = "linux")]
fn magic_to_name(magic: i64) -> &'static str {
    const BTRFS_SUPER_MAGIC: i64 = 0x9123683E_u32 as i64;
    const EXFAT_SUPER_MAGIC: i64 = 0x2011BAB0_u32 as i64;

    match magic {
        0xEF53 => "ext4",
        BTRFS_SUPER_MAGIC => "btrfs",
        0x58465342 => "xfs",
        0x4d44 => "msdos",
        EXFAT_SUPER_MAGIC => "exfat",
        0x5346544E => "ntfs",
        _ => "unknown",
    }
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn fstype_name(_c_path: &std::ffi::CStr, _path: &Path) -> Result<String> {
    Ok("unknown".to_string())
}

/// macOS exposes case-sensitivity per-volume via `pathconf` — verified
/// empirically (`_PC_CASE_SENSITIVE` returns 0 on both this machine's
/// default-case-insensitive APFS boot volume and an exFAT external
/// drive). Falls back to `true` (the safer default — case-sensitive
/// means the crate's case-collision check runs, which is the
/// conservative direction) if the call itself fails, since a failed
/// probe shouldn't silently disable a data-loss protection.
#[cfg(target_os = "macos")]
fn case_sensitive(c_path: &std::ffi::CString) -> bool {
    let rc = unsafe { libc::pathconf(c_path.as_ptr(), libc::_PC_CASE_SENSITIVE) };
    if rc < 0 {
        true
    } else {
        rc != 0
    }
}

/// Linux has no per-volume case-sensitivity query analogous to macOS's
/// `pathconf` (ext4's optional per-directory casefold feature is out
/// of scope — see dev-docs/design/filesystem-detection.md's future-work
/// notes) — derived from the filesystem type instead: the FAT/exFAT/
/// NTFS family is case-insensitive, everything else in the match
/// table is case-sensitive. Not empirically verified on Linux.
#[cfg(target_os = "linux")]
fn case_sensitive(c_path: &std::ffi::CString) -> bool {
    let mut buf: libc::statfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statfs(c_path.as_ptr(), &mut buf) } != 0 {
        return true;
    }
    !matches!(magic_to_name(buf.f_type as i64), "msdos" | "exfat" | "ntfs")
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn case_sensitive(_c_path: &std::ffi::CString) -> bool {
    true
}
