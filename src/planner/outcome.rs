use std::path::PathBuf;
use std::time::Duration;

use crate::error::Error;
use crate::profiler::Entry;

/// `#[non_exhaustive]`: a downstream `match` needs a `_` arm, so a new
/// way for an operation to stop early isn't a breaking change. Same
/// reasoning as `Progress`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
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
/// `#[non_exhaustive]`: an output type, built by this crate and read by
/// the caller, so blocking downstream construction costs nothing —
/// `Default` and `..`-destructuring both still work. Adding `duration`
/// was a breaking change for exhaustive struct literals; marked now so
/// the next field isn't, same reasoning as `Progress`.
#[derive(Debug, Default)]
#[non_exhaustive]
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
    /// Wall time the operation took, stamped where the outcome is
    /// produced for the caller. The counterpart to `Handle::elapsed()`
    /// for after the handle has been consumed by `.await`.
    ///
    /// `SyncOutcome`'s two outcomes are timed per phase, so they don't
    /// sum to the whole run — the diff that precedes them belongs to
    /// neither. `Handle::elapsed()` remains the figure for the run as a
    /// whole.
    ///
    /// `Duration::ZERO` on a phase that never ran (sync's delete sweep
    /// when the copy phase stopped early) and on outcomes built by hand
    /// in tests, which take the `Default`.
    pub duration: Duration,
}
