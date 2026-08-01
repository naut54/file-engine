use std::path::PathBuf;

use tokio_util::sync::CancellationToken;

use crate::error::Result;
use crate::handle::Handle;
use crate::planner::{BatchConfig, ErrorStrategy, OperationOutcome, SortOrder};
use crate::profiler::DEFAULT_SMALL_FILE_THRESHOLD;
use crate::progress::ProgressReporter;

use super::default_concurrency;
use super::pipeline::run_copy_pipeline;

pub struct CopyBuilder {
    source: PathBuf,
    dest: PathBuf,
    overwrite: bool,
    /// Unconditional field even though only `.preserve_permissions()`
    /// (Unix-only) can ever set it — see dev-docs/design/permissions.md.
    preserve_permissions: bool,
    allow_filesystem_integrity_risk: bool,
    small_file_threshold: Option<u64>,
    batch_config: BatchConfig,
    concurrency: Option<usize>,
}

impl CopyBuilder {
    pub(crate) fn new(source: impl Into<PathBuf>, dest: impl Into<PathBuf>) -> Self {
        Self {
            source: source.into(),
            dest: dest.into(),
            overwrite: false,
            preserve_permissions: false,
            allow_filesystem_integrity_risk: false,
            small_file_threshold: None,
            batch_config: BatchConfig::default(),
            concurrency: None,
        }
    }

    pub fn overwrite(mut self, overwrite: bool) -> Self {
        self.overwrite = overwrite;
        self
    }

    #[cfg(all(unix, feature = "permissions"))]
    pub fn preserve_permissions(mut self, preserve: bool) -> Self {
        self.preserve_permissions = preserve;
        self
    }

    /// Proceed even when the destination filesystem has a known
    /// write-integrity risk on this platform (currently: exFAT on
    /// macOS, see dev-docs/research/filesystem-limitations.md, section 9) —
    /// without this, `.start()`'s `Handle` resolves to
    /// `Err(Error::FilesystemIntegrityRisk)` before any data is written.
    /// See dev-docs/design/filesystem-detection.md.
    pub fn allow_filesystem_integrity_risk(mut self, allow: bool) -> Self {
        self.allow_filesystem_integrity_risk = allow;
        self
    }

    pub fn small_file_threshold(mut self, bytes: u64) -> Self {
        self.small_file_threshold = Some(bytes);
        self
    }

    pub fn max_bytes_per_batch(mut self, bytes: u64) -> Self {
        self.batch_config.max_bytes_per_batch = bytes;
        self
    }

    pub fn max_files_per_batch(mut self, n: usize) -> Self {
        self.batch_config.max_files_per_batch = Some(n);
        self
    }

    pub fn batch_sort_order(mut self, order: SortOrder) -> Self {
        self.batch_config.sort_order = order;
        self
    }

    pub fn on_error(mut self, strategy: ErrorStrategy) -> Self {
        self.batch_config.error_strategy = strategy;
        self
    }

    pub fn batch_concurrency(mut self, n: usize) -> Self {
        self.concurrency = Some(n);
        self
    }

    pub fn start(self) -> Result<Handle<OperationOutcome>> {
        let cancel = CancellationToken::new();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let reporter = ProgressReporter::new(tx);

        let concurrency = self.concurrency.unwrap_or_else(default_concurrency);
        let threshold = self.small_file_threshold.unwrap_or(DEFAULT_SMALL_FILE_THRESHOLD);
        let cancel_for_task = cancel.clone();

        let join_handle = tokio::spawn(async move {
            run_copy_pipeline(
                &self.source,
                &self.dest,
                self.overwrite,
                self.preserve_permissions,
                self.allow_filesystem_integrity_risk,
                threshold,
                &self.batch_config,
                concurrency,
                cancel_for_task,
                reporter,
            )
            .await
        });

        Ok(Handle::new(join_handle, rx, cancel))
    }
}
