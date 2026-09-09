use std::path::Path;
use std::time::SystemTime;

use globset::{Glob, GlobSet, GlobSetBuilder};

use crate::error::{Error, Result};

/// Same shape and semantics as `analysis::filter::AnalysisFilter`, kept
/// as its own type rather than shared: `analysis` is gated behind the
/// `analyze` feature and `remove` doesn't otherwise depend on it, so
/// reusing `AnalysisFilter` here would force every `remove`-only build
/// to pull in `analyze` (jwalk, infer) for a filter that doesn't need
/// any of it. Duplicated deliberately, not extracted to a shared crate
/// module, since the two filters are free to diverge (e.g. `remove`
/// never grew `analyze`'s depth/symlink options — see `RemoveBuilder`'s
/// doc comment) without one edit rippling into the other.
///
/// Filters AND together: a matched entry must pass every filter that's
/// set.
#[derive(Debug, Clone, Default)]
pub(crate) struct RemoveFilter {
    /// `None` matches any extension. Compared case-insensitively,
    /// without the leading dot.
    pub(crate) extensions: Option<Vec<String>>,
    pub(crate) exclude_patterns: Vec<String>,
    pub(crate) min_size: Option<u64>,
    pub(crate) max_size: Option<u64>,
    pub(crate) modified_after: Option<SystemTime>,
    pub(crate) modified_before: Option<SystemTime>,
}

impl RemoveFilter {
    /// Whether no criterion has been set at all — an unfiltered
    /// `RemoveFilter` matches every entry, which `RemoveBuilder::start`
    /// refuses to run without an explicit `.allow_unfiltered_delete(true)`.
    pub(crate) fn is_empty(&self) -> bool {
        self.extensions.is_none()
            && self.exclude_patterns.is_empty()
            && self.min_size.is_none()
            && self.max_size.is_none()
            && self.modified_after.is_none()
            && self.modified_before.is_none()
    }

    /// Compiles `exclude_patterns` once per run rather than per entry.
    /// `Ok(None)` when there are no patterns to compile, so callers can
    /// skip the exclusion check entirely instead of matching against an
    /// empty set.
    pub(crate) fn compiled_excludes(&self) -> Result<Option<GlobSet>> {
        if self.exclude_patterns.is_empty() {
            return Ok(None);
        }

        let mut builder = GlobSetBuilder::new();
        for pattern in &self.exclude_patterns {
            let glob = Glob::new(pattern).map_err(|source| Error::InvalidGlobPattern {
                pattern: pattern.clone(),
                source,
            })?;
            builder.add(glob);
        }

        let set = builder
            .build()
            .map_err(|source| Error::InvalidGlobPattern {
                pattern: self.exclude_patterns.join(", "),
                source,
            })?;
        Ok(Some(set))
    }

    /// Whether an already-scanned entry passes every filter: exclude
    /// pattern, extension, size range, modified-time range.
    pub(crate) fn matches(
        &self,
        relative_path: &Path,
        size: u64,
        modified: Option<SystemTime>,
        excludes: Option<&GlobSet>,
    ) -> bool {
        if let Some(excludes) = excludes {
            if excludes.is_match(relative_path) {
                return false;
            }
        }

        if let Some(extensions) = &self.extensions {
            let ext = relative_path.extension().and_then(|e| e.to_str());
            let matched = match ext {
                Some(ext) => extensions.iter().any(|e| e.eq_ignore_ascii_case(ext)),
                None => false,
            };
            if !matched {
                return false;
            }
        }

        if let Some(min) = self.min_size {
            if size < min {
                return false;
            }
        }
        if let Some(max) = self.max_size {
            if size > max {
                return false;
            }
        }

        if let Some(after) = self.modified_after {
            match modified {
                Some(m) if m >= after => {}
                _ => return false,
            }
        }
        if let Some(before) = self.modified_before {
            match modified {
                Some(m) if m <= before => {}
                _ => return false,
            }
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_filters_matches_everything_and_is_empty() {
        let filter = RemoveFilter::default();
        assert!(filter.is_empty());
        assert!(filter.matches(Path::new("a.txt"), 0, None, None));
    }

    #[test]
    fn any_single_criterion_makes_the_filter_non_empty() {
        let filter = RemoveFilter {
            min_size: Some(1),
            ..Default::default()
        };
        assert!(!filter.is_empty());
    }

    #[test]
    fn extension_filter_is_case_insensitive_and_excludes_others() {
        let filter = RemoveFilter {
            extensions: Some(vec!["tmp".to_string()]),
            ..Default::default()
        };
        assert!(filter.matches(Path::new("cache.TMP"), 0, None, None));
        assert!(!filter.matches(Path::new("cache.log"), 0, None, None));
        assert!(!filter.matches(Path::new("no_extension"), 0, None, None));
    }

    #[test]
    fn size_range_is_inclusive_on_both_ends() {
        let filter = RemoveFilter {
            min_size: Some(10),
            max_size: Some(20),
            ..Default::default()
        };
        assert!(filter.matches(Path::new("a"), 10, None, None));
        assert!(filter.matches(Path::new("a"), 20, None, None));
        assert!(!filter.matches(Path::new("a"), 9, None, None));
        assert!(!filter.matches(Path::new("a"), 21, None, None));
    }

    #[test]
    fn modified_range_rejects_entries_with_no_known_mtime() {
        let filter = RemoveFilter {
            modified_after: Some(SystemTime::UNIX_EPOCH),
            ..Default::default()
        };
        assert!(!filter.matches(Path::new("a"), 0, None, None));
    }

    #[test]
    fn compiled_excludes_prunes_matching_paths() {
        let filter = RemoveFilter {
            exclude_patterns: vec!["**/*.keep".to_string()],
            ..Default::default()
        };
        let set = filter.compiled_excludes().unwrap().unwrap();
        assert!(!filter.matches(Path::new("a.keep"), 0, None, Some(&set)));
        assert!(filter.matches(Path::new("a.tmp"), 0, None, Some(&set)));
    }

    #[test]
    fn invalid_glob_pattern_is_reported_as_an_error() {
        let filter = RemoveFilter {
            exclude_patterns: vec!["[".to_string()],
            ..Default::default()
        };
        assert!(matches!(
            filter.compiled_excludes(),
            Err(Error::InvalidGlobPattern { .. })
        ));
    }

    #[test]
    fn compiled_excludes_is_none_when_no_patterns_set() {
        assert!(RemoveFilter::default()
            .compiled_excludes()
            .unwrap()
            .is_none());
    }

    // Kept for parity with `AnalysisFilter`'s own test, which exercises
    // exactly this path relative to a root — irrelevant here since
    // `RemoveFilter` always matches against a `relative_path` the
    // caller already computed, but worth a smoke test to be sure a
    // multi-directory glob still isn't mistaken for an extension.
    #[test]
    fn exclude_pattern_can_target_nested_paths() {
        let filter = RemoveFilter {
            exclude_patterns: vec!["**/node_modules/**".to_string()],
            ..Default::default()
        };
        let set = filter.compiled_excludes().unwrap().unwrap();
        assert!(!filter.matches(
            Path::new("project/node_modules/pkg/index.js"),
            0,
            None,
            Some(&set)
        ));
        assert!(filter.matches(Path::new("project/src/main.rs"), 0, None, Some(&set)));
    }
}
