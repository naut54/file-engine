use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::task::JoinHandle;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::Stream;
use tokio_util::sync::CancellationToken;

use crate::error::Result;
use crate::watch_event::WatchEvent;

/// See dev-docs/design/watch.md. Structurally similar to `Handle<T>` (event
/// stream + `.cancel()` + awaitable) but a distinct type, not `Handle<()>`
/// — there's no bounded `T` to genericize over, and the event type
/// (`WatchEvent`) isn't `Progress`.
pub struct WatchHandle {
    join_handle: JoinHandle<Result<()>>,
    events: UnboundedReceiverStream<WatchEvent>,
    cancel: CancellationToken,
}

impl WatchHandle {
    pub(crate) fn new(
        join_handle: JoinHandle<Result<()>>,
        events: tokio::sync::mpsc::UnboundedReceiver<WatchEvent>,
        cancel: CancellationToken,
    ) -> Self {
        Self { join_handle, events: UnboundedReceiverStream::new(events), cancel }
    }

    pub fn events(&mut self) -> &mut (impl Stream<Item = WatchEvent> + Unpin) {
        &mut self.events
    }

    /// Cooperative. Resolves the `WatchHandle`'s `Future` output to
    /// `Ok(())` once observed — see the "Stopping" section of
    /// dev-docs/design/watch.md.
    pub fn cancel(&self) {
        self.cancel.cancel();
    }
}

impl Future for WatchHandle {
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match Pin::new(&mut this.join_handle).poll(cx) {
            Poll::Ready(Ok(result)) => Poll::Ready(result),
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
    use crate::watch_event::WatchEventKind;

    fn event() -> WatchEvent {
        WatchEvent { kind: WatchEventKind::Created, paths: vec![std::path::PathBuf::from("a")] }
    }

    #[tokio::test]
    async fn awaiting_yields_the_wrapped_ok_value() {
        let (_tx, rx) = mpsc::unbounded_channel();
        let join_handle = tokio::spawn(async { Ok::<_, crate::error::Error>(()) });
        let handle = WatchHandle::new(join_handle, rx, CancellationToken::new());

        handle.await.unwrap();
    }

    #[tokio::test]
    async fn awaiting_yields_the_wrapped_err_value() {
        let (_tx, rx) = mpsc::unbounded_channel();
        let join_handle = tokio::spawn(async { Err(crate::error::Error::Cancelled) });
        let handle = WatchHandle::new(join_handle, rx, CancellationToken::new());

        assert!(matches!(handle.await, Err(crate::error::Error::Cancelled)));
    }

    #[tokio::test]
    async fn events_stream_yields_events_in_send_order_then_ends() {
        let (tx, rx) = mpsc::unbounded_channel();
        let join_handle = tokio::spawn(async move {
            let _ = tx.send(event());
            let _ = tx.send(event());
            // tx drops here, closing the channel.
            Ok::<_, crate::error::Error>(())
        });
        let mut handle = WatchHandle::new(join_handle, rx, CancellationToken::new());

        use tokio_stream::StreamExt;
        assert!(handle.events().next().await.is_some());
        assert!(handle.events().next().await.is_some());
        assert!(handle.events().next().await.is_none());

        handle.await.unwrap();
    }

    #[tokio::test]
    async fn cancel_triggers_the_wrapped_cancellation_token() {
        let (_tx, rx) = mpsc::unbounded_channel();
        let cancel = CancellationToken::new();
        let join_handle = tokio::spawn(async { Ok::<_, crate::error::Error>(()) });
        let handle = WatchHandle::new(join_handle, rx, cancel.clone());

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
        let handle = WatchHandle::new(join_handle, rx, CancellationToken::new());

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(*sentinel.lock().unwrap(), 1);
        drop(handle);

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(*sentinel.lock().unwrap(), 2, "task should have run to completion despite the handle being dropped");
    }
}
