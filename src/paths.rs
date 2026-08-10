use std::ffi::OsString;
use std::path::{Path, PathBuf};

use unicode_normalization::UnicodeNormalization;

/// NFC-normalizes every component of `path` for *comparison purposes
/// only* — never used to construct a path actually passed to a
/// filesystem call, since that must preserve whatever bytes the
/// filesystem actually gave us. Non-UTF8 components (rare, Unix-only)
/// pass through unchanged — there's no normalization to apply to bytes
/// that aren't valid Unicode in the first place.
///
/// Exists because HFS+ forces NFD on disk and APFS preserves whatever
/// form it's given (verified during real-world testing that copying
/// accented filenames from APFS to exFAT produces byte-different but
/// visually identical names) — comparing raw path bytes across two
/// filesystems with different normalization behavior produces false
/// mismatches.
pub(crate) fn normalize_for_comparison(path: &Path) -> PathBuf {
    path.components()
        .map(|component| {
            let os_str = component.as_os_str();
            match os_str.to_str() {
                Some(s) => OsString::from(s.nfc().collect::<String>()),
                None => os_str.to_os_string(),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nfc_and_nfd_forms_of_the_same_name_normalize_identically() {
        let nfc: String = "café".nfc().collect();
        let nfd: String = "café".nfd().collect();
        assert_ne!(
            nfc.as_bytes(),
            nfd.as_bytes(),
            "test setup: these must start out byte-different"
        );

        assert_eq!(
            normalize_for_comparison(Path::new(&nfc)),
            normalize_for_comparison(Path::new(&nfd)),
        );
    }

    #[test]
    fn normalizes_every_component_not_just_the_last() {
        let nfd: String = "café".nfd().collect();
        let path = PathBuf::from(&nfd).join("nested").join(&nfd);

        let nfc: String = "café".nfc().collect();
        let expected = PathBuf::from(&nfc).join("nested").join(&nfc);

        assert_eq!(normalize_for_comparison(&path), expected);
    }

    #[test]
    fn already_normalized_ascii_path_is_unchanged() {
        let path = PathBuf::from("a").join("b.txt");
        assert_eq!(normalize_for_comparison(&path), path);
    }
}
