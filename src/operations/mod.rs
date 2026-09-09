#[cfg(feature = "compress")]
mod compress;
#[cfg(feature = "operations")]
mod copy;
#[cfg(feature = "sync")]
pub(crate) mod diff;
#[cfg(feature = "operations")]
mod move_many;
#[cfg(feature = "operations")]
mod move_path;
#[cfg(feature = "operations")]
pub(crate) mod pipeline;
#[cfg(feature = "remove")]
mod remove;
#[cfg(feature = "remove")]
mod remove_filter;
#[cfg(feature = "sync")]
mod sync;
#[cfg(feature = "watch")]
mod watch;

/// Default worker pool size: `available_parallelism()` — falls back to 1
/// if the platform can't report it.
#[cfg(feature = "operations")]
pub(crate) fn default_concurrency() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

#[cfg(feature = "compress")]
pub use compress::{CompressBuilder, CompressFormat};
#[cfg(feature = "operations")]
pub use copy::CopyBuilder;
#[cfg(feature = "operations")]
pub use move_many::MoveManyBuilder;
#[cfg(feature = "operations")]
pub use move_path::MoveBuilder;
#[cfg(feature = "remove")]
pub use remove::{RemoveBuilder, RemoveOutcome};
#[cfg(feature = "sync")]
pub use sync::{SyncBuilder, SyncOutcome};
#[cfg(feature = "watch")]
pub use watch::WatchBuilder;
