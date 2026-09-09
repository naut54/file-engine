use std::io::Write;
use std::path::Path;

use serde::Serialize;

use crate::checker::CheckResult;
use crate::runner::WorkloadShape;

/// The environment columns for every row produced by one harness run —
/// gathered once via `dest_probe` and duplicated across rows, since each
/// row needs to stand alone as a training example.
pub struct Environment {
    pub source_case_insensitive: bool,
    pub dest_case_insensitive: bool,
    pub dest_windows_naming_rules: bool,
}

/// One row of the fault/outcome training corpus (see the design doc's
/// "What type of data" discussion). One row per planted fault per run.
#[derive(Debug, Serialize)]
pub struct CorpusRow {
    // Environment.
    os: &'static str,
    source_case_insensitive: bool,
    dest_case_insensitive: bool,
    dest_windows_naming_rules: bool,

    // Workload shape, off `Progress::Planned`.
    directories: Option<usize>,
    small_files: Option<usize>,
    small_bytes: Option<u64>,
    large_files: Option<usize>,
    large_bytes: Option<u64>,
    small_file_threshold: Option<u64>,

    // Label + observed outcome.
    relative_path: String,
    injected_fault: &'static str,
    observed_error: Option<&'static str>,
    passed: bool,

    // Timing — `OperationOutcome.duration`, the whole run, not just this
    // one entry (the pipeline doesn't time per-entry).
    run_duration_ms: u128,
}

/// Builds one `CorpusRow` per checked entry from the same run, sharing
/// the run's environment/shape/timing across all of them.
pub fn rows(
    env: &Environment,
    shape: Option<WorkloadShape>,
    run_duration_ms: u128,
    results: &[CheckResult],
) -> Vec<CorpusRow> {
    results
        .iter()
        .map(|r| CorpusRow {
            os: std::env::consts::OS,
            source_case_insensitive: env.source_case_insensitive,
            dest_case_insensitive: env.dest_case_insensitive,
            dest_windows_naming_rules: env.dest_windows_naming_rules,
            directories: shape.map(|s| s.directories),
            small_files: shape.map(|s| s.small_files),
            small_bytes: shape.map(|s| s.small_bytes),
            large_files: shape.map(|s| s.large_files),
            large_bytes: shape.map(|s| s.large_bytes),
            small_file_threshold: shape.map(|s| s.small_file_threshold),
            relative_path: r.relative_path.display().to_string(),
            injected_fault: r.expected,
            observed_error: r.observed,
            passed: r.passed,
            run_duration_ms,
        })
        .collect()
}

/// Appends one JSON-lines row per `CorpusRow` to `path`.
pub fn record(path: &Path, rows: &[CorpusRow]) -> std::io::Result<()> {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;

    for row in rows {
        let line =
            serde_json::to_string(row).expect("CorpusRow only contains JSON-representable types");
        writeln!(file, "{line}")?;
    }

    Ok(())
}
