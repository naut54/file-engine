use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;

use crate::error::{FileEngineError, Result};
use crate::progress::Progress;

/// Handle to a spawned operation (`copy`, `move_path`, `analyze`, `compress`,
/// `sync`, ...). Implements `Future` so callers can simply `.await` it for
/// the final result, and exposes a `Progress` stream + cooperative
/// cancellation in the meantime.
///
/// Not used by `watch` — see design doc §9.1.
pub struct Handle<T> {
    pub(crate) join: tokio::task::JoinHandle<Result<T>>,
    pub(crate) progress_rx: UnboundedReceiverStream<Progress>,
    pub(crate) cancel_token: CancellationToken,
}

impl<T> Handle<T> {
    pub fn progress(&mut self) -> &mut UnboundedReceiverStream<Progress> {
        &mut self.progress_rx
    }

    pub fn cancel(&self) {
        self.cancel_token.cancel();
    }
}

impl<T> Future for Handle<T> {
    type Output = Result<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match Pin::new(&mut this.join).poll(cx) {
            Poll::Ready(Ok(result)) => Poll::Ready(result),
            Poll::Ready(Err(join_err)) => Poll::Ready(Err(FileEngineError::Io {
                path: Default::default(),
                source: std::io::Error::other(join_err),
            })),
            Poll::Pending => Poll::Pending,
        }
    }
}
