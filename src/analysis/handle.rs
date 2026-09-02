use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use tokio::task::JoinHandle;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::Stream;
use tokio_util::sync::CancellationToken;

use crate::error::Result;

use super::progress::AnalysisProgress;
use super::report::AnalysisReport;

/// Structurally similar to `Handle<T>` (event stream + `.cancel()` +
/// awaitable) but a distinct type, following the same precedent
/// `WatchHandle` already sets: `Handle<T>`'s progress stream is hard-coded
/// to `crate::progress::Progress`, which lives behind the `operations`
/// feature — `analyze` doesn't require `operations`, and `AnalysisProgress`
/// isn't `Progress`, so genericizing the shared `Handle<T>` would mean
/// either coupling `analyze` to `operations` or threading a second type
/// parameter through every existing call site for no benefit to them.
pub struct AnalysisHandle {
    join_handle: JoinHandle<Result<AnalysisReport>>,
    progress: UnboundedReceiverStream<AnalysisProgress>,
    cancel: CancellationToken,
    started: Instant,
    finished: Option<Instant>,
}

impl AnalysisHandle {
    pub(crate) fn new(
        join_handle: JoinHandle<Result<AnalysisReport>>,
        progress: tokio::sync::mpsc::UnboundedReceiver<AnalysisProgress>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            join_handle,
            progress: UnboundedReceiverStream::new(progress),
            cancel,
            started: Instant::now(),
            finished: None,
        }
    }

    pub fn progress(&mut self) -> &mut (impl Stream<Item = AnalysisProgress> + Unpin) {
        &mut self.progress
    }

    /// Wall time since the analysis was spawned. Frozen once the handle
    /// has been polled to completion — see `Handle::elapsed`.
    pub fn elapsed(&self) -> Duration {
        match self.finished {
            Some(finished) => finished.saturating_duration_since(self.started),
            None => self.started.elapsed(),
        }
    }

    /// Cooperative. Dropping the `AnalysisHandle` without calling this
    /// keeps the walk running to completion.
    pub fn cancel(&self) {
        self.cancel.cancel();
    }
}

impl Future for AnalysisHandle {
    type Output = Result<AnalysisReport>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match Pin::new(&mut this.join_handle).poll(cx) {
            Poll::Ready(Ok(result)) => {
                this.finished.get_or_insert_with(Instant::now);
                Poll::Ready(result)
            }
            Poll::Ready(Err(join_err)) => std::panic::resume_unwind(join_err.into_panic()),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;

    use super::*;

    #[tokio::test]
    async fn awaiting_yields_the_wrapped_ok_value() {
        let (_tx, rx) = mpsc::unbounded_channel();
        let join_handle = tokio::spawn(async {
            Ok(AnalysisReport {
                file_count: 1,
                dir_count: 0,
                total_size: 0,
                largest_files: Vec::new(),
                by_extension: Default::default(),
                by_mime: Default::default(),
                age_buckets: Default::default(),
                errors: Vec::new(),
                errors_total: 0,
                #[cfg(feature = "checksum")]
                duplicates: Vec::new(),
                #[cfg(feature = "checksum")]
                duplicate_groups_total: 0,
                #[cfg(feature = "checksum")]
                duplicate_bytes_wasted: 0,
                duration: Duration::ZERO,
            })
        });
        let handle = AnalysisHandle::new(join_handle, rx, CancellationToken::new());

        assert_eq!(handle.await.unwrap().file_count, 1);
    }

    #[tokio::test]
    async fn cancel_triggers_the_wrapped_cancellation_token() {
        let (_tx, rx) = mpsc::unbounded_channel();
        let cancel = CancellationToken::new();
        let join_handle = tokio::spawn(async { Err(crate::error::Error::Cancelled) });
        let handle = AnalysisHandle::new(join_handle, rx, cancel.clone());

        handle.cancel();
        assert!(cancel.is_cancelled());
        assert!(matches!(handle.await, Err(crate::error::Error::Cancelled)));
    }
}
