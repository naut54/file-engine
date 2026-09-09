mod action;
mod batch;
mod config;
mod dispatcher;
mod outcome;
mod plan;

pub(crate) use action::{CopyAction, EntryAction};
// Only `remove.rs`'s own `EntryAction` impl constructs this variant
// directly (`CopyAction`'s callers go through `EntryOutcome` internally
// within `action.rs`/`dispatcher.rs` without needing it re-exported) —
// gated to match, so a `remove`-less build doesn't carry a dead
// re-export.
#[cfg(feature = "remove")]
pub(crate) use action::EntryOutcome;
pub use config::{BatchConfig, ErrorStrategy, SortOrder};
pub(crate) use dispatcher::dispatch;
pub use outcome::{OperationOutcome, StopReason};
pub(crate) use plan::plan;
