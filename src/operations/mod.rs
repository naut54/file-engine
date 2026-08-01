#[cfg(feature = "operations")]
mod copy;
#[cfg(feature = "sync")]
pub(crate) mod diff;
#[cfg(feature = "operations")]
mod move_path;
#[cfg(feature = "operations")]
pub(crate) mod pipeline;
#[cfg(feature = "sync")]
mod sync;
#[cfg(feature = "compress")]
mod compress;
#[cfg(feature = "watch")]
mod watch;

/// Default worker pool size: `available_parallelism()`, per
/// dev-docs/design/batching-engine.md's "Worker pool size" decision — falls
/// back to 1 if the platform can't report it.
#[cfg(feature = "operations")]
pub(crate) fn default_concurrency() -> usize {
    std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1)
}

#[cfg(feature = "operations")]
pub use copy::CopyBuilder;
#[cfg(feature = "operations")]
pub use move_path::MoveBuilder;
#[cfg(feature = "sync")]
pub use sync::{SyncBuilder, SyncOutcome};
#[cfg(feature = "compress")]
pub use compress::{CompressBuilder, CompressFormat};
#[cfg(feature = "watch")]
pub use watch::WatchBuilder;
