use std::path::PathBuf;

use file_engine::{Error, OperationOutcome};

use crate::fixtures::SabotagedEntry;

/// Whether one planted fault produced the `Error` variant it was
/// designed to. This is the harness's first deliverable per the design
/// doc — a correctness check on the crate's existing error handling,
/// independent of any later corpus use.
pub struct CheckResult {
    pub relative_path: PathBuf,
    pub expected: &'static str,
    pub observed: Option<&'static str>,
    pub passed: bool,
}

pub fn check(outcome: &OperationOutcome, sabotaged: &[SabotagedEntry]) -> Vec<CheckResult> {
    sabotaged
        .iter()
        .map(|entry| {
            let observed = outcome
                .failed
                .iter()
                .find(|(e, _)| e.relative_path == entry.relative_path)
                .map(|(_, err)| error_variant_name(err));

            let passed = observed == Some(entry.fault.expected_error_name());

            CheckResult {
                relative_path: entry.relative_path.clone(),
                expected: entry.fault.expected_error_name(),
                observed,
                passed,
            }
        })
        .collect()
}

/// Mirrors `error.rs`'s variant list, restricted to the variants
/// reachable under the features `fault-harness/Cargo.toml` actually
/// enables (`operations`, `sync` — the latter pulls in `analyze` per
/// `file-engine`'s own `Cargo.toml`, hence `InvalidGlobPattern` below).
/// `compress`-gated variants aren't enabled here and so don't exist to
/// match on. Note `#[cfg(feature = ...)]` can't be used here to track
/// *file_engine's* features — it only ever sees this crate's own — so
/// this list must be kept in sync by hand with whatever
/// `fault-harness/Cargo.toml` requests.
fn error_variant_name(err: &Error) -> &'static str {
    match err {
        Error::SourceNotFound { .. } => "SourceNotFound",
        Error::DestExists { .. } => "DestExists",
        Error::Cancelled => "Cancelled",
        Error::NoSpace { .. } => "NoSpace",
        Error::PermissionDenied { .. } => "PermissionDenied",
        Error::Io { .. } => "Io",
        Error::InvalidGlobPattern { .. } => "InvalidGlobPattern",
        Error::CaseCollision { .. } => "CaseCollision",
        Error::FileTooLargeForDest { .. } => "FileTooLargeForDest",
        Error::ReservedName { .. } => "ReservedName",
        Error::FilesystemIntegrityRisk { .. } => "FilesystemIntegrityRisk",
    }
}
