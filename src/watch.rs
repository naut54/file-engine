use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;

use crate::engine::FileEngine;
use crate::error::{FileEngineError, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchEventKind {
    Created,
    Modified,
    Removed,
    Other,
}

#[derive(Debug, Clone)]
pub struct WatchEvent {
    pub kind: WatchEventKind,
    pub paths: Vec<PathBuf>,
}

/// `watch` is a continuous stream of filesystem events with no single final
/// result, so — unlike every other builder — it does not produce a
/// `Handle<T>` (§7.1). See design doc §7.3.
pub struct WatchBuilder {
    path: PathBuf,
    recursive: bool,
    cancel_token: Option<CancellationToken>,
}

impl WatchBuilder {
    pub(crate) fn new(path: PathBuf) -> Self {
        Self {
            path,
            recursive: true,
            cancel_token: None,
        }
    }

    pub fn recursive(mut self, enabled: bool) -> Self {
        self.recursive = enabled;
        self
    }

    pub fn cancellation_token(mut self, token: CancellationToken) -> Self {
        self.cancel_token = Some(token);
        self
    }

    pub fn start(self) -> Result<WatchHandle> {
        let cancel_token = self.cancel_token.unwrap_or_default();
        let (_events_tx, events_rx) = tokio::sync::mpsc::unbounded_channel();

        let task_cancel_token = cancel_token.clone();
        let join = tokio::spawn(async move {
            let _ = &task_cancel_token;
            // TODO(implementation): own a `notify::RecommendedWatcher` on
            // `self.path` (honoring `self.recursive`), map its events to
            // `WatchEvent`, and forward them via `_events_tx` until
            // `task_cancel_token` is cancelled or the watcher errors.
            let _ = self.path;
            let _ = self.recursive;
            Ok(())
        });

        Ok(WatchHandle {
            join,
            events_rx: UnboundedReceiverStream::new(events_rx),
            cancel_token,
        })
    }
}

/// Sibling to `Handle<T>` (§7.1), not a generalization of it — mirrors its
/// `.cancel()` and `Future` shape but swaps `.progress()` for `.events()`,
/// since `watch` has no meaningful final result.
pub struct WatchHandle {
    join: tokio::task::JoinHandle<Result<()>>,
    events_rx: UnboundedReceiverStream<WatchEvent>,
    cancel_token: CancellationToken,
}

impl WatchHandle {
    pub fn events(&mut self) -> &mut UnboundedReceiverStream<WatchEvent> {
        &mut self.events_rx
    }

    pub fn cancel(&self) {
        self.cancel_token.cancel();
    }
}

impl Future for WatchHandle {
    type Output = Result<()>;

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

#[cfg(feature = "watch")]
impl FileEngine {
    pub fn watch(&self, path: impl Into<PathBuf>) -> WatchBuilder {
        WatchBuilder::new(path.into())
    }
}
