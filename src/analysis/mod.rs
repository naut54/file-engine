mod builder;
mod error_strategy;
mod filter;
mod handle;
#[cfg(feature = "checksum")]
mod hash;
mod progress;
mod report;
mod util;
mod walk;

pub use builder::{AnalyzeBuilder, DEFAULT_TOP_N_LARGEST};
pub use error_strategy::AnalysisErrorStrategy;
pub use handle::AnalysisHandle;
pub use progress::AnalysisProgress;
pub use report::{
    AgeBuckets, AnalysisReport, Entry, ExtensionStats, MimeStats, DEFAULT_MAX_REPORTED_ERRORS,
};
#[cfg(feature = "checksum")]
pub use report::{DuplicateGroup, DEFAULT_MAX_REPORTED_DUPLICATE_GROUPS};
