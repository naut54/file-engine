use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::paths::normalize_for_comparison;

use super::fs_caps::FilesystemCapabilities;
use super::workload::{DirEntry, Entry, Workload};

/// Entries `validate()` removed from the `Workload` before it ever
/// reaches the Planner, paired with why. Unlike a typical pre-flight
/// check, these aren't necessarily whole-operation failures — the
/// pipeline layer applies `ErrorStrategy` to decide what happens next,
/// same as any other per-entry failure. See
/// dev-docs/design/filesystem-detection.md, "Decisions".
#[derive(Debug, Default)]
pub(crate) struct ValidationOutcome {
    pub rejected_entries: Vec<(Entry, Error)>,
    /// Full `DirEntry`, not just a path — mirrors `rejected_entries`.
    /// This module has no notion of a destination path (it only knows
    /// `relative_path`s and `dest_caps`); the pipeline layer computes
    /// the actual destination path the same way it already does for
    /// `ensure_directories_exist`/`apply_directory_permissions`.
    pub rejected_directories: Vec<(DirEntry, Error)>,
}

/// Checks `workload` against what `dest_caps` can actually represent,
/// removing anything that can't be copied as-is. Only the exFAT-on-macOS
/// write-integrity risk is a true whole-operation failure (it isn't a
/// property of any specific entry — every write to that destination
/// carries it) — everything else is per-entry, reflected in the
/// returned `ValidationOutcome` rather than an `Err`. `allow_filesystem_integrity_risk`
/// is the one override this feature has (see
/// dev-docs/design/filesystem-detection.md, "Decisions") — every other
/// rejection is per-entry and already governed by the caller's
/// `ErrorStrategy`, which needs no override here.
pub(crate) fn validate(
    workload: &mut Workload,
    dest_caps: &FilesystemCapabilities,
    allow_filesystem_integrity_risk: bool,
) -> Result<ValidationOutcome> {
    if dest_caps.write_integrity_risk && !allow_filesystem_integrity_risk {
        return Err(Error::FilesystemIntegrityRisk {
            filesystem: dest_caps.name.clone(),
        });
    }

    let mut outcome = ValidationOutcome::default();

    if !dest_caps.case_sensitive {
        reject_case_collisions(workload, &mut outcome);
    }

    retain_entries(
        &mut workload.small,
        dest_caps,
        &mut outcome.rejected_entries,
    );
    retain_entries(
        &mut workload.large,
        dest_caps,
        &mut outcome.rejected_entries,
    );
    retain_directories(
        &mut workload.directories,
        dest_caps,
        &mut outcome.rejected_directories,
    );

    Ok(outcome)
}

/// Case-folded, Unicode-normalized key two entries collide under on a
/// case-insensitive destination — folds in item 6 (normalization) so a
/// destination-side NFC rewrite of an NFD source name doesn't dodge
/// detection here the same way it wouldn't dodge exFAT's real
/// case-insensitive lookup.
fn collision_key(relative_path: &Path) -> String {
    normalize_for_comparison(relative_path)
        .to_string_lossy()
        .to_lowercase()
}

/// Groups every file *and* directory by `collision_key` — a file and a
/// directory differing only by case collide just as badly as two files
/// would — and removes every member of any group with more than one
/// entry. Both entries in a colliding pair are rejected, not just one:
/// there's no principled way to pick a "winner", and silently keeping
/// one without saying so is the exact silent-loss problem this check
/// exists to prevent.
fn reject_case_collisions(workload: &mut Workload, outcome: &mut ValidationOutcome) {
    let mut groups: HashMap<String, Vec<PathBuf>> = HashMap::new();
    for entry in workload.small.iter().chain(&workload.large) {
        groups
            .entry(collision_key(&entry.relative_path))
            .or_default()
            .push(entry.relative_path.clone());
    }
    for dir in &workload.directories {
        groups
            .entry(collision_key(&dir.relative_path))
            .or_default()
            .push(dir.relative_path.clone());
    }

    let colliding_keys: HashSet<String> = groups
        .into_iter()
        .filter(|(_, paths)| paths.len() > 1)
        .map(|(key, _)| key)
        .collect();
    if colliding_keys.is_empty() {
        return;
    }

    // A second pass to know, for each rejected entry, which specific
    // sibling it collided with (for the error message) rather than just
    // "something, somewhere, collided".
    let mut siblings: HashMap<String, Vec<PathBuf>> = HashMap::new();
    for entry in workload.small.iter().chain(&workload.large) {
        let key = collision_key(&entry.relative_path);
        if colliding_keys.contains(&key) {
            siblings
                .entry(key)
                .or_default()
                .push(entry.relative_path.clone());
        }
    }
    for dir in &workload.directories {
        let key = collision_key(&dir.relative_path);
        if colliding_keys.contains(&key) {
            siblings
                .entry(key)
                .or_default()
                .push(dir.relative_path.clone());
        }
    }
    let other_for = |path: &Path, key: &str| -> PathBuf {
        siblings[key]
            .iter()
            .find(|candidate| candidate.as_path() != path)
            .cloned()
            .unwrap_or_else(|| path.to_path_buf())
    };

    for group in [&mut workload.small, &mut workload.large] {
        let mut i = 0;
        while i < group.len() {
            let key = collision_key(&group[i].relative_path);
            if colliding_keys.contains(&key) {
                let entry = group.remove(i);
                let other = other_for(&entry.relative_path, &key);
                let error = Error::CaseCollision {
                    path: entry.relative_path.clone(),
                    other,
                };
                outcome.rejected_entries.push((entry, error));
            } else {
                i += 1;
            }
        }
    }

    let mut i = 0;
    while i < workload.directories.len() {
        let key = collision_key(&workload.directories[i].relative_path);
        if colliding_keys.contains(&key) {
            let dir = workload.directories.remove(i);
            let other = other_for(&dir.relative_path, &key);
            let error = Error::CaseCollision {
                path: dir.relative_path.clone(),
                other,
            };
            outcome.rejected_directories.push((dir, error));
        } else {
            i += 1;
        }
    }
}

fn retain_entries(
    entries: &mut Vec<Entry>,
    dest_caps: &FilesystemCapabilities,
    rejected: &mut Vec<(Entry, Error)>,
) {
    let mut i = 0;
    while i < entries.len() {
        match entry_violation(&entries[i], dest_caps) {
            Some(err) => rejected.push((entries.remove(i), err)),
            None => i += 1,
        }
    }
}

fn entry_violation(entry: &Entry, dest_caps: &FilesystemCapabilities) -> Option<Error> {
    if let Some(max) = dest_caps.max_file_size {
        if entry.size > max {
            return Some(Error::FileTooLargeForDest {
                path: entry.relative_path.clone(),
                size: entry.size,
                max,
            });
        }
    }
    if dest_caps.windows_naming_rules {
        if let Some(err) = check_windows_naming(&entry.relative_path) {
            return Some(err);
        }
    }
    None
}

/// Directories go through the naming check too (a directory named `CON`
/// is exactly as broken as a file named `CON`) but not the size check —
/// directories have no size. The scanned root itself (`relative_path`
/// empty) never has anything to check: `Path::components()` on an empty
/// path yields no components, so the loop below naturally passes it
/// through without needing to special-case it.
fn retain_directories(
    directories: &mut Vec<DirEntry>,
    dest_caps: &FilesystemCapabilities,
    rejected: &mut Vec<(DirEntry, Error)>,
) {
    if !dest_caps.windows_naming_rules {
        return;
    }
    let mut i = 0;
    while i < directories.len() {
        match check_windows_naming(&directories[i].relative_path) {
            Some(err) => rejected.push((directories.remove(i), err)),
            None => i += 1,
        }
    }
}

/// Windows/NTFS/exFAT/FAT32's reserved-character set, reserved device
/// names, and trailing dot/space rule — empirically confirmed these are
/// enforced by Windows's own API layer, not the on-disk format itself
/// (every one of them was silently accepted when written directly to a
/// mounted exFAT drive from macOS), so this check applies based on
/// `dest_caps.windows_naming_rules` (destination filesystem type)
/// regardless of which OS is doing the writing. See "Windows as a
/// first-class destination target" in
/// dev-docs/design/filesystem-detection.md.
const RESERVED_CHARS: &[char] = &['<', '>', ':', '"', '/', '\\', '|', '?', '*'];
const RESERVED_BASE_NAMES: &[&str] = &[
    "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
    "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
];

fn check_windows_naming(relative_path: &Path) -> Option<Error> {
    for component in relative_path.components() {
        let std::path::Component::Normal(os_str) = component else {
            continue;
        };
        let name = os_str.to_string_lossy();

        let has_reserved_char = name
            .chars()
            .any(|c| RESERVED_CHARS.contains(&c) || (c as u32) < 0x20);
        let has_trailing_dot_or_space = name.ends_with('.') || name.ends_with(' ');
        let base = name.split('.').next().unwrap_or(&name);
        let is_reserved_name = RESERVED_BASE_NAMES
            .iter()
            .any(|reserved| reserved.eq_ignore_ascii_case(base));

        if has_reserved_char || has_trailing_dot_or_space || is_reserved_name {
            return Some(Error::ReservedName {
                path: relative_path.to_path_buf(),
            });
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn caps(overrides: impl FnOnce(&mut FilesystemCapabilities)) -> FilesystemCapabilities {
        let mut caps = FilesystemCapabilities {
            name: "test".to_string(),
            case_sensitive: true,
            max_file_size: None,
            windows_naming_rules: false,
            timestamp_granularity: Duration::ZERO,
            write_integrity_risk: false,
        };
        overrides(&mut caps);
        caps
    }

    fn entry(relative_path: &str, size: u64) -> Entry {
        Entry {
            path: PathBuf::from(relative_path),
            relative_path: PathBuf::from(relative_path),
            size,
            modified: None,
        }
    }

    fn dir(relative_path: &str) -> DirEntry {
        DirEntry {
            path: PathBuf::from(relative_path),
            relative_path: PathBuf::from(relative_path),
            mode: None,
        }
    }

    #[test]
    fn a_valid_workload_is_untouched() {
        let mut workload = Workload {
            small: vec![entry("a.txt", 10)],
            large: vec![entry("nested/big.bin", 1000)],
            directories: vec![dir(""), dir("nested")],
        };
        let dest_caps = caps(|_| {});

        let outcome = validate(&mut workload, &dest_caps, false).unwrap();

        assert!(outcome.rejected_entries.is_empty());
        assert!(outcome.rejected_directories.is_empty());
        assert_eq!(workload.small.len(), 1);
        assert_eq!(workload.large.len(), 1);
        assert_eq!(workload.directories.len(), 2);
    }

    #[test]
    fn write_integrity_risk_returns_err_before_touching_the_workload() {
        let mut workload = Workload {
            small: vec![entry("a.txt", 10)],
            large: vec![],
            directories: vec![],
        };
        let dest_caps = caps(|c| {
            c.name = "exfat".to_string();
            c.write_integrity_risk = true;
        });

        let result = validate(&mut workload, &dest_caps, false);

        assert!(
            matches!(result, Err(Error::FilesystemIntegrityRisk { filesystem }) if filesystem == "exfat")
        );
        assert_eq!(
            workload.small.len(),
            1,
            "workload must be untouched when this short-circuits"
        );
    }

    #[test]
    fn allow_filesystem_integrity_risk_bypasses_the_check() {
        let mut workload = Workload {
            small: vec![entry("a.txt", 10)],
            large: vec![],
            directories: vec![],
        };
        let dest_caps = caps(|c| {
            c.name = "exfat".to_string();
            c.write_integrity_risk = true;
        });

        let outcome = validate(&mut workload, &dest_caps, true).unwrap();

        assert!(outcome.rejected_entries.is_empty());
        assert_eq!(
            workload.small.len(),
            1,
            "the entry should proceed normally once the risk is acknowledged"
        );
    }

    #[test]
    fn case_sensitive_destination_never_flags_collisions() {
        let mut workload = Workload {
            small: vec![entry("Report.txt", 1), entry("report.txt", 1)],
            large: vec![],
            directories: vec![],
        };
        let dest_caps = caps(|c| c.case_sensitive = true);

        let outcome = validate(&mut workload, &dest_caps, false).unwrap();

        assert!(outcome.rejected_entries.is_empty());
        assert_eq!(workload.small.len(), 2);
    }

    #[test]
    fn case_insensitive_destination_rejects_both_sides_of_a_collision() {
        let mut workload = Workload {
            small: vec![entry("Report.txt", 1), entry("report.txt", 1)],
            large: vec![],
            directories: vec![],
        };
        let dest_caps = caps(|c| c.case_sensitive = false);

        let outcome = validate(&mut workload, &dest_caps, false).unwrap();

        assert!(
            workload.small.is_empty(),
            "both colliding entries should be removed, not just one"
        );
        assert_eq!(outcome.rejected_entries.len(), 2);
        for (_, err) in &outcome.rejected_entries {
            assert!(matches!(err, Error::CaseCollision { .. }));
        }
    }

    #[test]
    fn a_file_and_a_directory_differing_only_by_case_collide() {
        let mut workload = Workload {
            small: vec![entry("Assets/readme.txt", 1)],
            large: vec![],
            directories: vec![dir("Assets"), dir("assets")],
        };
        let dest_caps = caps(|c| c.case_sensitive = false);

        let outcome = validate(&mut workload, &dest_caps, false).unwrap();

        assert_eq!(outcome.rejected_directories.len(), 2);
        assert!(workload.directories.is_empty());
        // The file inside `Assets/` isn't itself part of the collision
        // (its own relative path is unique) — only the two directory
        // entries collide.
        assert_eq!(workload.small.len(), 1);
    }

    #[test]
    fn normalization_differences_are_treated_as_the_same_name() {
        use unicode_normalization::UnicodeNormalization;

        let nfc: String = "café.txt".nfc().collect();
        let nfd: String = "café.txt".nfd().collect();
        assert_ne!(
            nfc.as_bytes(),
            nfd.as_bytes(),
            "test setup: these must start out byte-different"
        );

        let mut workload = Workload {
            small: vec![entry(&nfc, 1), entry(&nfd, 1)],
            large: vec![],
            directories: vec![],
        };
        let dest_caps = caps(|c| c.case_sensitive = false);

        let outcome = validate(&mut workload, &dest_caps, false).unwrap();

        assert!(workload.small.is_empty());
        assert_eq!(outcome.rejected_entries.len(), 2);
    }

    #[test]
    fn non_colliding_entries_survive_alongside_a_collision() {
        let mut workload = Workload {
            small: vec![
                entry("Report.txt", 1),
                entry("report.txt", 1),
                entry("unrelated.txt", 1),
            ],
            large: vec![],
            directories: vec![],
        };
        let dest_caps = caps(|c| c.case_sensitive = false);

        let outcome = validate(&mut workload, &dest_caps, false).unwrap();

        assert_eq!(outcome.rejected_entries.len(), 2);
        assert_eq!(workload.small.len(), 1);
        assert_eq!(
            workload.small[0].relative_path,
            PathBuf::from("unrelated.txt")
        );
    }

    #[test]
    fn file_over_the_size_limit_is_rejected() {
        let mut workload = Workload {
            small: vec![],
            large: vec![entry("big.bin", 101)],
            directories: vec![],
        };
        let dest_caps = caps(|c| c.max_file_size = Some(100));

        let outcome = validate(&mut workload, &dest_caps, false).unwrap();

        assert!(workload.large.is_empty());
        assert_eq!(outcome.rejected_entries.len(), 1);
        assert!(matches!(
            outcome.rejected_entries[0].1,
            Error::FileTooLargeForDest {
                size: 101,
                max: 100,
                ..
            }
        ));
    }

    #[test]
    fn file_exactly_at_the_size_limit_is_kept() {
        let mut workload = Workload {
            small: vec![],
            large: vec![entry("big.bin", 100)],
            directories: vec![],
        };
        let dest_caps = caps(|c| c.max_file_size = Some(100));

        let outcome = validate(&mut workload, &dest_caps, false).unwrap();

        assert!(outcome.rejected_entries.is_empty());
        assert_eq!(workload.large.len(), 1);
    }

    #[test]
    fn no_size_limit_never_rejects_on_size() {
        let mut workload = Workload {
            small: vec![],
            large: vec![entry("huge.bin", u64::MAX)],
            directories: vec![],
        };
        let dest_caps = caps(|c| c.max_file_size = None);

        let outcome = validate(&mut workload, &dest_caps, false).unwrap();

        assert!(outcome.rejected_entries.is_empty());
    }

    #[test]
    fn reserved_character_in_a_filename_is_rejected_when_windows_naming_rules_apply() {
        let mut workload = Workload {
            small: vec![entry("a:b.txt", 1)],
            large: vec![],
            directories: vec![],
        };
        let dest_caps = caps(|c| c.windows_naming_rules = true);

        let outcome = validate(&mut workload, &dest_caps, false).unwrap();

        assert!(workload.small.is_empty());
        assert!(matches!(
            outcome.rejected_entries[0].1,
            Error::ReservedName { .. }
        ));
    }

    #[test]
    fn reserved_characters_are_allowed_when_windows_naming_rules_do_not_apply() {
        let mut workload = Workload {
            small: vec![entry("a:b.txt", 1)],
            large: vec![],
            directories: vec![],
        };
        let dest_caps = caps(|c| c.windows_naming_rules = false);

        let outcome = validate(&mut workload, &dest_caps, false).unwrap();

        assert!(outcome.rejected_entries.is_empty());
        assert_eq!(workload.small.len(), 1);
    }

    #[test]
    fn reserved_device_name_is_rejected_case_insensitively_regardless_of_extension() {
        let dest_caps = caps(|c| c.windows_naming_rules = true);
        for name in [
            "CON", "con.txt", "NUL", "nul.log", "COM1", "com1.dat", "AUX",
        ] {
            let mut workload = Workload {
                small: vec![entry(name, 1)],
                large: vec![],
                directories: vec![],
            };
            let outcome = validate(&mut workload, &dest_caps, false).unwrap();
            assert!(
                workload.small.is_empty(),
                "{name} should have been rejected"
            );
            assert!(matches!(
                outcome.rejected_entries[0].1,
                Error::ReservedName { .. }
            ));
        }
    }

    #[test]
    fn a_name_that_merely_contains_a_reserved_word_is_not_rejected() {
        let mut workload = Workload {
            small: vec![entry("console.txt", 1)],
            large: vec![],
            directories: vec![],
        };
        let dest_caps = caps(|c| c.windows_naming_rules = true);

        let outcome = validate(&mut workload, &dest_caps, false).unwrap();

        assert!(outcome.rejected_entries.is_empty());
    }

    #[test]
    fn trailing_dot_or_space_is_rejected() {
        let dest_caps = caps(|c| c.windows_naming_rules = true);
        for name in ["trailing.", "trailing "] {
            let mut workload = Workload {
                small: vec![entry(name, 1)],
                large: vec![],
                directories: vec![],
            };
            let outcome = validate(&mut workload, &dest_caps, false).unwrap();
            assert!(
                workload.small.is_empty(),
                "{name:?} should have been rejected"
            );
            assert!(matches!(
                outcome.rejected_entries[0].1,
                Error::ReservedName { .. }
            ));
        }
    }

    #[test]
    fn reserved_directory_name_is_rejected() {
        let mut workload = Workload {
            small: vec![],
            large: vec![],
            directories: vec![dir("CON")],
        };
        let dest_caps = caps(|c| c.windows_naming_rules = true);

        let outcome = validate(&mut workload, &dest_caps, false).unwrap();

        assert!(workload.directories.is_empty());
        assert!(matches!(
            outcome.rejected_directories[0].1,
            Error::ReservedName { .. }
        ));
    }

    #[test]
    fn the_scanned_root_with_empty_relative_path_is_never_rejected() {
        let mut workload = Workload {
            small: vec![],
            large: vec![],
            directories: vec![dir("")],
        };
        let dest_caps = caps(|c| c.windows_naming_rules = true);

        let outcome = validate(&mut workload, &dest_caps, false).unwrap();

        assert!(outcome.rejected_directories.is_empty());
        assert_eq!(workload.directories.len(), 1);
    }
}
