use std::path::Path;

use file_engine::{FileEngine, OperationOutcome, Progress, Result};
use tokio_stream::StreamExt;

/// The workload-shape fields off `Progress::Planned` — carried into the
/// corpus row alongside each run's outcome, so a training example
/// records what kind of workload the fault was planted into, not just
/// the fault itself.
#[derive(Debug, Clone, Copy, Default)]
pub struct WorkloadShape {
    pub directories: usize,
    pub small_files: usize,
    pub small_bytes: u64,
    pub large_files: usize,
    pub large_bytes: u64,
    pub small_file_threshold: u64,
}

pub struct RunRecord {
    /// `None` if the pipeline never emitted `Planned` — shouldn't happen
    /// for a `copy`, but the type stays honest about it rather than
    /// unwrapping.
    pub shape: Option<WorkloadShape>,
    pub outcome: OperationOutcome,
}

/// Drives one real copy through the public API only — never reaches
/// into `file_engine`'s internals. `overwrite` defaults to `false`,
/// which is what `Fault::DestExists` depends on to actually fire.
///
/// `.allow_filesystem_integrity_risk(true)`: `file_engine` correctly
/// refuses to write to exFAT-on-macOS by default (a real, precedented
/// data-corruption risk — see
/// `dev-docs/design/filesystem-detection.md`), and the harness's
/// `ReservedName` fault deliberately mounts exactly that kind of
/// destination. The override is safe here specifically because this
/// destination is a throwaway `hdiutil` image the harness tears down
/// immediately after, never real user data.
pub async fn run_copy(src: &Path, dest: &Path) -> Result<RunRecord> {
    let engine = FileEngine::new();
    let mut handle = engine
        .copy(src, dest)
        .allow_filesystem_integrity_risk(true)
        .start()?;

    let mut shape = None;
    while let Some(event) = handle.progress().next().await {
        if let Progress::Planned {
            directories,
            small_files,
            small_bytes,
            large_files,
            large_bytes,
            small_file_threshold,
        } = event
        {
            shape = Some(WorkloadShape {
                directories,
                small_files,
                small_bytes,
                large_files,
                large_bytes,
                small_file_threshold,
            });
        }
    }

    let outcome = handle.await?;
    Ok(RunRecord { shape, outcome })
}
