use std::path::Path;

/// Empirically determines whether `dir`'s filesystem is case-insensitive,
/// by writing a file and checking whether a different-case path resolves
/// to it. `file_engine`'s own answer to this question
/// (`profiler::fs_caps`, backed by `statfs`/`GetVolumeInformationW`)
/// isn't reachable from outside the crate (`pub(crate)`) — this harness
/// needs its own, arrived at empirically rather than by calling in.
///
/// Empirical testing is the right approach *here*: case-folded lookup is
/// enforced by the filesystem driver itself at the VFS layer, so probing
/// actual behavior gives the real answer. Contrast `has_windows_naming_rules`
/// below, where the equivalent empirical approach turned out to be wrong.
pub fn is_case_insensitive(dir: &Path) -> std::io::Result<bool> {
    let probe = dir.join(".fault_harness_case_probe");
    std::fs::write(&probe, b"x")?;
    let upper = dir.join(".FAULT_HARNESS_CASE_PROBE");
    let insensitive = upper.exists();
    std::fs::remove_file(&probe)?;
    Ok(insensitive)
}

/// Determines whether `dir`'s filesystem enforces Windows reserved-name
/// rules, by filesystem *type*, not by testing whether a write succeeds.
///
/// An earlier version of this function created a file literally named
/// `CON` and checked whether the write failed — verified wrong,
/// empirically: mounting a real exFAT disk image on macOS and writing to
/// `CON` succeeds silently (file present on disk afterward, no error).
/// `file_engine`'s own `fs_caps` probe already made the correct call
/// here (see `dev-docs/design/filesystem-detection.md`'s "Open items"
/// section) — the restriction is enforced by Windows's own Win32 API
/// layer at write time, not by the exFAT/FAT32 format itself, so a Mac
/// or Linux writer can place a Windows-illegal name onto a FAT-family
/// destination with no error at all. `windows_naming_rules` has to be
/// keyed on the destination filesystem *type*, never on whether the
/// current host happens to enforce it — this function now matches that.
#[cfg(target_os = "macos")]
pub fn has_windows_naming_rules(dir: &Path) -> std::io::Result<bool> {
    // `diskutil info` only accepts a disk identifier or a volume's mount
    // point, not an arbitrary subdirectory (verified: `diskutil info
    // /tmp/some/subdir` fails with "Could not find disk") — so the
    // containing device has to be resolved first, via `df`, exactly the
    // way `file_engine`'s own `fs_caps::probe()` walks up to the nearest
    // existing ancestor to find a mount point rather than assuming `dir`
    // itself is one.
    let df_output = std::process::Command::new("df").arg(dir).output()?;
    if !df_output.status.success() {
        return Err(std::io::Error::other(format!(
            "df {} failed: {}",
            dir.display(),
            String::from_utf8_lossy(&df_output.stderr)
        )));
    }
    let device = String::from_utf8_lossy(&df_output.stdout)
        .lines()
        .nth(1) // header line, then the one data row
        .and_then(|line| line.split_whitespace().next())
        .map(str::to_string)
        .ok_or_else(|| {
            std::io::Error::other(format!(
                "could not parse a device out of `df {}`",
                dir.display()
            ))
        })?;

    let output = std::process::Command::new("diskutil")
        .arg("info")
        .arg(&device)
        .output()?;
    if !output.status.success() {
        return Err(std::io::Error::other(format!(
            "diskutil info {device} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let personality = stdout
        .lines()
        .find_map(|line| line.trim().strip_prefix("File System Personality:"))
        .map(|s| s.trim().to_ascii_lowercase())
        .unwrap_or_default();

    // Matches `fs_caps`'s own family: FAT/exFAT/NTFS. `personality` is
    // one of "ExFAT", "MS-DOS FAT32", "Windows NT File System"-ish
    // strings on macOS — matched by substring rather than an exact list,
    // since `diskutil`'s exact wording per FAT variant isn't guaranteed.
    Ok(personality.contains("fat") || personality.contains("ntfs"))
}

/// Not yet implemented for non-macOS: no `diskutil`-equivalent call is
/// wired up here yet, and the empirical write-based test this replaced
/// on macOS is known to be unreliable in general (only Windows's own
/// Win32 API layer enforces the restriction, not the FAT/exFAT format
/// itself) — so rather than give a plausible-looking wrong answer on
/// other platforms, this conservatively reports "not enforced," which
/// only matters today for `ReservedName`, itself not yet built for
/// non-macOS destinations either (see the design doc's "Open items").
#[cfg(not(target_os = "macos"))]
pub fn has_windows_naming_rules(_dir: &Path) -> std::io::Result<bool> {
    Ok(false)
}
