use std::path::Path;
use std::time::SystemTime;

use globset::{Glob, GlobSet, GlobSetBuilder};

use crate::error::{Error, Result};

/// Filters AND together: a matched entry must pass every filter that's
/// set. Extension/size/modified-time filters narrow which files count
/// toward the report at all (silently, not as errors); exclude patterns
/// additionally prune directory traversal itself (see
/// `walk.rs`'s `filter_entry` use) rather than being applied post-walk.
#[derive(Debug, Clone, Default)]
pub(crate) struct AnalysisFilter {
    /// `None` matches any extension. Compared case-insensitively,
    /// without the leading dot.
    pub(crate) extensions: Option<Vec<String>>,
    pub(crate) exclude_patterns: Vec<String>,
    pub(crate) min_size: Option<u64>,
    pub(crate) max_size: Option<u64>,
    pub(crate) modified_after: Option<SystemTime>,
    pub(crate) modified_before: Option<SystemTime>,
}

impl AnalysisFilter {
    /// Compiles `exclude_patterns` once per walk rather than per entry.
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

    /// Whether a matched (non-excluded, already-walked) entry passes the
    /// non-traversal filters: extension, size range, modified-time range.
    pub(crate) fn matches(
        &self,
        relative_path: &Path,
        size: u64,
        modified: Option<SystemTime>,
    ) -> bool {
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
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn no_filters_matches_everything() {
        let filter = AnalysisFilter::default();
        assert!(filter.matches(Path::new("a.txt"), 0, None));
    }

    #[test]
    fn extension_filter_is_case_insensitive_and_excludes_others() {
        let filter = AnalysisFilter {
            extensions: Some(vec!["rs".to_string()]),
            ..Default::default()
        };
        assert!(filter.matches(Path::new("main.RS"), 0, None));
        assert!(!filter.matches(Path::new("main.toml"), 0, None));
        assert!(!filter.matches(Path::new("no_extension"), 0, None));
    }

    #[test]
    fn size_range_is_inclusive_on_both_ends() {
        let filter = AnalysisFilter {
            min_size: Some(10),
            max_size: Some(20),
            ..Default::default()
        };
        assert!(filter.matches(Path::new("a"), 10, None));
        assert!(filter.matches(Path::new("a"), 20, None));
        assert!(!filter.matches(Path::new("a"), 9, None));
        assert!(!filter.matches(Path::new("a"), 21, None));
    }

    #[test]
    fn modified_range_rejects_entries_with_no_known_mtime() {
        let filter = AnalysisFilter {
            modified_after: Some(SystemTime::UNIX_EPOCH),
            ..Default::default()
        };
        assert!(!filter.matches(Path::new("a"), 0, None));
    }

    #[test]
    fn compiled_excludes_prunes_matching_paths() {
        let filter = AnalysisFilter {
            exclude_patterns: vec!["**/node_modules/**".to_string()],
            ..Default::default()
        };
        let set = filter.compiled_excludes().unwrap().unwrap();
        assert!(set.is_match(PathBuf::from("project/node_modules/pkg/index.js")));
        assert!(!set.is_match(PathBuf::from("project/src/main.rs")));
    }

    #[test]
    fn compiled_excludes_is_none_when_no_patterns_set() {
        assert!(AnalysisFilter::default()
            .compiled_excludes()
            .unwrap()
            .is_none());
    }

    #[test]
    fn invalid_glob_pattern_is_reported_as_an_error() {
        let filter = AnalysisFilter {
            exclude_patterns: vec!["[".to_string()],
            ..Default::default()
        };
        assert!(matches!(
            filter.compiled_excludes(),
            Err(Error::InvalidGlobPattern { .. })
        ));
    }
}
