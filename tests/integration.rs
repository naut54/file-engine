#[cfg(feature = "diagnostics")]
#[path = "integration/diagnostics.rs"]
mod diagnostics;

#[cfg(feature = "operations")]
#[path = "integration/copy.rs"]
mod copy;

#[cfg(feature = "operations")]
#[path = "integration/move_op.rs"]
mod move_op;

#[cfg(feature = "analyze")]
#[path = "integration/analyze.rs"]
mod analyze;

#[cfg(feature = "compress")]
#[path = "integration/compress.rs"]
mod compress;

#[cfg(feature = "sync")]
#[path = "integration/sync.rs"]
mod sync;

#[cfg(feature = "watch")]
#[path = "integration/watch.rs"]
mod watch;

#[cfg(all(feature = "operations", feature = "permissions", unix))]
#[path = "integration/permissions.rs"]
mod permissions;
