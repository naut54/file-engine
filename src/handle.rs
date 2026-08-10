use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::task::JoinHandle;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::Stream;
use tokio_util::sync::CancellationToken;

use crate::error::Result;
use crate::progress::Progress;

/// One generic type reused across every operation (`T` =
/// `OperationOutcome` for copy/move/compress, `SyncOutcome` for sync)
/// rather than a handle type per operation.
pub struct Handle<T> {
    join_handle: JoinHandle<Result<T>>,
    progress: UnboundedReceiverStream<Progress>,
    cancel: CancellationToken,
}

impl<T> Handle<T> {
    pub(crate) fn new(
        join_handle: JoinHandle<Result<T>>,
        progress: tokio::sync::mpsc::UnboundedReceiver<Progress>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            join_handle,
            progress: UnboundedReceiverStream::new(progress),
            cancel,
        }
    }

    pub fn progress(&mut self) -> &mut (impl Stream<Item = Progress> + Unpin) {
        &mut self.progress
    }

    /// Cooperative. Detached-on-drop: dropping the `Handle` without
    /// calling this keeps the operation running to completion. See
    /// `docs/guide/progress-and-cancellation.md`.
    pub fn cancel(&self) {
        self.cancel.cancel();
    }
}

impl<T> Future for Handle<T> {
    type Output = Result<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match Pin::new(&mut this.join_handle).poll(cx) {
            Poll::Ready(Ok(result)) => Poll::Ready(result),
            // The task panicked (dispatcher.rs's own internal
            // `.expect()`s already treat that as unrecoverable) or was
            // aborted (never happens — nothing calls `.abort()` on this
            // JoinHandle). Propagate rather than silently swallowing it.
            Poll::Ready(Err(join_err)) => std::panic::resume_unwind(join_err.into_panic()),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use tokio::sync::mpsc;

    use super::*;

    #[tokio::test]
    async fn awaiting_yields_the_wrapped_ok_value() {
        let (_tx, rx) = mpsc::unbounded_channel();
        let join_handle = tokio::spawn(async { Ok::<_, crate::error::Error>(42) });
        let handle = Handle::new(join_handle, rx, CancellationToken::new());

        assert_eq!(handle.await.unwrap(), 42);
    }

    #[tokio::test]
    async fn awaiting_yields_the_wrapped_err_value() {
        let (_tx, rx) = mpsc::unbounded_channel();
        let join_handle = tokio::spawn(async { Err::<i32, _>(crate::error::Error::Cancelled) });
        let handle = Handle::new(join_handle, rx, CancellationToken::new());

        assert!(matches!(handle.await, Err(crate::error::Error::Cancelled)));
    }

    #[tokio::test]
    async fn progress_stream_yields_events_in_send_order_then_ends() {
        let (tx, rx) = mpsc::unbounded_channel();
        let reporter = crate::progress::ProgressReporter::new(tx);
        let join_handle = tokio::spawn(async move {
            reporter.send(Progress::Started {
                bytes_total: None,
                entries_total: 1,
            });
            reporter.send(Progress::EntryStarted {
                entry: test_entry(),
            });
            // Reporter (and its clones) drop here, closing the channel.
            Ok::<_, crate::error::Error>(())
        });
        let mut handle = Handle::new(join_handle, rx, CancellationToken::new());

        use tokio_stream::StreamExt;
        assert!(matches!(
            handle.progress().next().await,
            Some(Progress::Started { .. })
        ));
        assert!(matches!(
            handle.progress().next().await,
            Some(Progress::EntryStarted { .. })
        ));
        assert!(handle.progress().next().await.is_none());

        handle.await.unwrap();
    }

    #[tokio::test]
    async fn cancel_triggers_the_wrapped_cancellation_token() {
        let (_tx, rx) = mpsc::unbounded_channel();
        let cancel = CancellationToken::new();
        let join_handle = tokio::spawn(async { Ok::<_, crate::error::Error>(()) });
        let handle = Handle::new(join_handle, rx, cancel.clone());

        handle.cancel();
        assert!(cancel.is_cancelled());
    }

    #[tokio::test]
    async fn dropping_the_handle_does_not_stop_the_wrapped_task() {
        let (_tx, rx) = mpsc::unbounded_channel();
        let sentinel = Arc::new(Mutex::new(0));
        let sentinel_for_task = Arc::clone(&sentinel);

        let join_handle = tokio::spawn(async move {
            *sentinel_for_task.lock().unwrap() = 1;
            tokio::time::sleep(Duration::from_millis(30)).await;
            *sentinel_for_task.lock().unwrap() = 2;
            Ok::<_, crate::error::Error>(())
        });
        let handle = Handle::new(join_handle, rx, CancellationToken::new());

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(*sentinel.lock().unwrap(), 1);
        drop(handle);

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            *sentinel.lock().unwrap(),
            2,
            "task should have run to completion despite the handle being dropped"
        );
    }

    fn test_entry() -> crate::profiler::Entry {
        crate::profiler::Entry {
            path: std::path::PathBuf::from("a"),
            relative_path: std::path::PathBuf::from("a"),
            size: 1,
            modified: None,
        }
    }
}
