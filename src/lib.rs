mod engine;
mod error;
mod handle;
mod progress;

#[cfg(feature = "analyze")]
mod analyze;
#[cfg(feature = "compress")]
mod compress;
#[cfg(feature = "operations")]
mod operations;
#[cfg(feature = "sync")]
mod sync;
#[cfg(feature = "watch")]
mod watch;
// `permissions` is Unix-only — see §9.6. `nix` (its only dependency) isn't
// even present in the manifest for non-Unix targets, so this must be
// gated on `unix` too, not just the feature.
#[cfg(all(feature = "permissions", unix))]
mod permissions;

pub use engine::{EngineOptions, FileEngine};
pub use error::{FileEngineError, Result};
pub use handle::Handle;
pub use progress::Progress;

#[cfg(feature = "analyze")]
pub use analyze::{AnalyzeBuilder, FileInfo, FileKind};
#[cfg(feature = "compress")]
pub use compress::{CompressBuilder, CompressFormat, DecompressBuilder};
#[cfg(feature = "operations")]
pub use operations::{CopyBuilder, MoveBuilder};
#[cfg(feature = "sync")]
pub use sync::{SyncBuilder, SyncSummary};
#[cfg(feature = "watch")]
pub use watch::{WatchBuilder, WatchEvent, WatchEventKind, WatchHandle};

#[cfg(feature = "diagnostics")]
pub use error::catalog;
