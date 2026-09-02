/// How `AnalyzeBuilder::start()` handles a per-entry error mid-walk
/// (e.g. a permission-denied subdirectory, or a symlink loop under
/// `.follow_symlinks(true)`).
///
/// Deliberately a smaller enum than `planner::ErrorStrategy` rather than
/// reusing it: analysis never writes anything, so `Undo` has no meaning
/// here, and `ErrorStrategy` lives behind the `operations` feature,
/// which `analyze` doesn't require.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AnalysisErrorStrategy {
    /// Skip the offending entry, record it in `AnalysisReport::errors`
    /// (subject to the `max_reported_errors` cap), and keep walking.
    #[default]
    ContinueAndCollect,
    /// Stop the walk and return the error from `.start()`'s `Handle`
    /// the moment one is encountered.
    AbortOnError,
}
