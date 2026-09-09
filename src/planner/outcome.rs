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
    /// Entries left untouched because the destination already held
    /// byte-identical content — populated only when `.overwrite(false)`
    /// (the default) is paired with `.skip_if_identical(true)` (the
    /// `checksum` feature; see `CopyBuilder`/`MoveBuilder`). Disjoint
    /// from `succeeded`: nothing was written, so counting it as a normal
    /// success would overstate the bytes this run actually transferred.
    /// Always empty otherwise.
    pub skipped: Vec<Entry>,
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
    /// Populated only by `MoveManyBuilder`: whole-source failures that
    /// happen before any per-file `Entry` exists for that source (the
    /// atomic-rename fast path failed for a reason other than
    /// cross-device, or the source vanished before it could be
    /// scanned) — one entry per failed source, keyed by that source's
    /// original path rather than an `Entry`, since none was ever built.
    /// Always empty for `copy`/`move`.
    pub sources_failed: Vec<(PathBuf, Error)>,
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
