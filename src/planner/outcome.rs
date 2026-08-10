use std::path::PathBuf;

use crate::error::Error;
use crate::profiler::Entry;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    Fatal,
    AbortOnError,
    Cancelled,
    Undo,
}

/// Aggregate result of running an `ExecutionPlan`. Replaces a bare
/// `Result<(), Error>` because `ErrorStrategy::ContinueAndCollect` can
/// finish with a mix of successes and failures that a single `Result`
/// can't represent.
#[derive(Debug, Default)]
pub struct OperationOutcome {
    pub succeeded: Vec<Entry>,
    pub failed: Vec<(Entry, Error)>,
    /// Populated only by move's deferred deletion sweep: entries that
    /// copied successfully but whose original source could not be
    /// removed afterward (data duplicated, not lost). Copy never
    /// populates this field.
    pub cleanup_failed: Vec<(Entry, Error)>,
    pub stopped_early: Option<StopReason>,
    /// Populated only by the directory-permissions pass
    /// (`.preserve_permissions()`), which always runs to completion
    /// regardless of individual failures and never affects
    /// `stopped_early` — a directory `chmod` failure is a best-effort
    /// finishing touch, not an interruption of the actual data transfer.
    /// Unconditional (not `#[cfg(unix)]`-gated), same reasoning as
    /// `Entry.mode`: avoids a platform-conditional shape for a type with
    /// many existing construction sites. Always empty on non-Unix or
    /// when permission preservation wasn't requested.
    pub directories_failed: Vec<(PathBuf, Error)>,
}
