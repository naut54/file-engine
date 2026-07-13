use std::path::PathBuf;

use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;

use crate::error::Result;
use crate::handle::Handle;

pub struct MoveBuilder {
    src: PathBuf,
    dst: PathBuf,
    overwrite: bool,
    cancel_token: Option<CancellationToken>,
}

impl MoveBuilder {
    pub(crate) fn new(src: PathBuf, dst: PathBuf) -> Self {
        Self {
            src,
            dst,
            overwrite: false,
            cancel_token: None,
        }
    }

    pub fn overwrite(mut self, enabled: bool) -> Self {
        self.overwrite = enabled;
        self
    }

    pub fn cancellation_token(mut self, token: CancellationToken) -> Self {
        self.cancel_token = Some(token);
        self
    }

    pub fn start(self) -> Result<Handle<()>> {
        let cancel_token = self.cancel_token.unwrap_or_default();
        let (_progress_tx, progress_rx) = tokio::sync::mpsc::unbounded_channel();

        let task_cancel_token = cancel_token.clone();
        let join = tokio::spawn(async move {
            let _ = &task_cancel_token;
            // TODO(implementation): perform the actual move (src -> dst,
            // honoring `overwrite`), reporting progress via `_progress_tx`
            // and observing `task_cancel_token`.
            let _ = (self.src, self.dst, self.overwrite);
            Ok(())
        });

        Ok(Handle {
            join,
            progress_rx: UnboundedReceiverStream::new(progress_rx),
            cancel_token,
        })
    }
}
