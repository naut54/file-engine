use std::iter;
use std::os::windows::ffi::OsStrExt;
use std::path::Path;

use windows_sys::Win32::Storage::FileSystem::{GetVolumeInformationW, GetVolumePathNameW};

use crate::error::Result;

use super::{case_sensitive_by_name, classify_by_name, classify_io_error, FilesystemCapabilities};

/// Implemented from documented Win32 behavior (`GetVolumePathNameW` +
/// `GetVolumeInformationW`). The name lookup and the capabilities
/// derived from it are exercised on Windows by CI; the error paths
/// (both `last_os_error()` branches) are not.
pub(super) fn probe(path: &Path) -> Result<FilesystemCapabilities> {
    let wide_path = to_wide(path);

    // `GetVolumeInformationW` needs a volume *root* path, not an
    // arbitrary directory on the volume — resolve it first, per the
    // pattern Microsoft's own docs recommend for this exact call.
    let mut root_buf = [0u16; 261];
    let resolved = unsafe {
        GetVolumePathNameW(
            wide_path.as_ptr(),
            root_buf.as_mut_ptr(),
            root_buf.len() as u32,
        )
    };
    if resolved == 0 {
        return Err(classify_io_error(
            std::io::Error::last_os_error(),
            path.to_path_buf(),
        ));
    }

    let mut fs_name_buf = [0u16; 261];
    let mut max_component_len: u32 = 0;
    let mut flags: u32 = 0;
    let ok = unsafe {
        GetVolumeInformationW(
            root_buf.as_ptr(),
            std::ptr::null_mut(),
            0,
            std::ptr::null_mut(),
            &mut max_component_len,
            &mut flags,
            fs_name_buf.as_mut_ptr(),
            fs_name_buf.len() as u32,
        )
    };
    if ok == 0 {
        return Err(classify_io_error(
            std::io::Error::last_os_error(),
            path.to_path_buf(),
        ));
    }

    let name = from_wide(&fs_name_buf).to_lowercase();
    // Derived from the filesystem name, *not* from this call's
    // `FILE_CASE_SENSITIVE_SEARCH` flag — see `case_sensitive_by_name`
    // for why that flag reports the opposite of what it appears to.
    // `flags` is still requested because `GetVolumeInformationW` wants
    // somewhere to put it, and a future capability may want to read it.
    let case_sensitive = case_sensitive_by_name(&name);

    // Host OS is Windows itself here, so the exFAT-write-integrity
    // risk (specific to macOS's `F_FULLFSYNC` implementation) never
    // applies — `classify_by_name`'s `host_is_macos` is always false.
    let (max_file_size, windows_naming_rules, timestamp_granularity, write_integrity_risk) =
        classify_by_name(&name, false);

    Ok(FilesystemCapabilities {
        name,
        case_sensitive,
        max_file_size,
        windows_naming_rules,
        timestamp_granularity,
        write_integrity_risk,
    })
}

fn to_wide(path: &Path) -> Vec<u16> {
    path.as_os_str()
        .encode_wide()
        .chain(iter::once(0))
        .collect()
}

fn from_wide(buf: &[u16]) -> String {
    let len = buf.iter().position(|&c| c == 0).unwrap_or(buf.len());
    String::from_utf16_lossy(&buf[..len])
}
