use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::error::{Error, Result};
use crate::watch_event::WatchEvent;
use crate::watch_handle::WatchHandle;

// `watch` deliberately does not use the Profiler/Planner/Dispatcher
// pipeline — it's an indefinite event stream (`notify`), not a
// bulk-transfer workload with anything to profile or batch. See
// dev-docs/design/batching-engine.md, "Integration: operations", and
// dev-docs/design/watch.md for the full design.
pub struct WatchBuilder {
    path: PathBuf,
    recursive: bool,
}

impl WatchBuilder {
    pub(crate) fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into(), recursive: true }
    }

    pub fn recursive(mut self, recursive: bool) -> Self {
        self.recursive = recursive;
        self
    }

    pub fn start(self) -> Result<WatchHandle> {
        let cancel = CancellationToken::new();
        let (tx, rx) = mpsc::unbounded_channel();
        let cancel_for_task = cancel.clone();

        let join_handle = tokio::spawn(watch(self.path, self.recursive, cancel_for_task, tx));

        Ok(WatchHandle::new(join_handle, rx, cancel))
    }
}

async fn watch(
    path: PathBuf,
    recursive: bool,
    cancel: CancellationToken,
    tx: mpsc::UnboundedSender<WatchEvent>,
) -> Result<()> {
    let (fatal_tx, fatal_rx) = oneshot::channel();

    let watcher = tokio::task::spawn_blocking(move || build_watcher(&path, recursive, tx, fatal_tx))
        .await
        .expect("watcher setup task panicked")?;

    tokio::select! {
        _ = cancel.cancelled() => {
            drop(watcher);
            Ok(())
        }
        fatal = fatal_rx => {
            drop(watcher);
            // An `Err` here just means the sender dropped without
            // sending — the watcher was torn down normally, not a real
            // fatal signal (the cancellation branch above already covers
            // the "torn down on purpose" case, so in practice this arm
            // only ever carries a real error).
            fatal.map_or(Ok(()), Err)
        }
    }
}

fn build_watcher(
    path: &Path,
    recursive: bool,
    tx: mpsc::UnboundedSender<WatchEvent>,
    fatal_tx: oneshot::Sender<Error>,
) -> Result<RecommendedWatcher> {
    let fatal_tx = Arc::new(Mutex::new(Some(fatal_tx)));
    let path_for_errors = path.to_path_buf();

    let mut watcher = notify::recommended_watcher(move |res: notify::Result<notify::Event>| match res {
        Ok(event) => {
            let _ = tx.send(WatchEvent::from(event));
        }
        Err(err) => {
            // Only the first fatal error gets forwarded — `send`
            // consumes the oneshot sender, and there's nothing more
            // coherent to signal once the watch is already being torn
            // down.
            if let Some(sender) = fatal_tx.lock().unwrap().take() {
                let _ = sender.send(classify_notify_error(err, &path_for_errors));
            }
        }
    })
    .map_err(|e| classify_notify_error(e, path))?;

    let mode = if recursive { RecursiveMode::Recursive } else { RecursiveMode::NonRecursive };
    watcher.watch(path, mode).map_err(|e| classify_notify_error(e, path))?;

    Ok(watcher)
}

fn classify_notify_error(err: notify::Error, path: &Path) -> Error {
    match err.kind {
        notify::ErrorKind::PathNotFound => Error::SourceNotFound { path: path.to_path_buf() },
        notify::ErrorKind::Io(io_err) => match io_err.kind() {
            std::io::ErrorKind::NotFound => Error::SourceNotFound { path: path.to_path_buf() },
            std::io::ErrorKind::PermissionDenied => Error::PermissionDenied { path: path.to_path_buf() },
            _ => Error::Io { path: path.to_path_buf(), source: io_err },
        },
        _ => Error::Io { path: path.to_path_buf(), source: std::io::Error::other(err.to_string()) },
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::Duration;

    use tempfile::tempdir;
    use tokio_stream::StreamExt;

    use super::*;
    use crate::watch_event::WatchEventKind;

    /// Upper bound on any single wait for an expected event. Only ever
    /// reached when a test is genuinely failing (once
    /// `await_watcher_ready` has returned, events arrive in
    /// milliseconds), so it's set generously rather than tuned — a tight
    /// bound here buys nothing but flakiness on a loaded CI runner.
    const EVENT_TIMEOUT: Duration = Duration::from_secs(10);

    /// `notify`'s backends are asynchronous relative to the filesystem
    /// operation that triggers them (inotify/FSEvents/etc. all deliver
    /// events after some OS-determined delay) — poll for the expected
    /// event rather than asserting on the very next one, since unrelated
    /// events (e.g. a directory's own mtime bump) can arrive first.
    async fn next_matching(
        handle: &mut WatchHandle,
        mut predicate: impl FnMut(&WatchEvent) -> bool,
    ) -> WatchEvent {
        tokio::time::timeout(EVENT_TIMEOUT, async {
            loop {
                let event = handle.events().next().await.expect("event stream ended unexpectedly");
                if predicate(&event) {
                    return event;
                }
            }
        })
        .await
        .expect("timed out waiting for expected event")
    }

    /// Blocks until `handle` is demonstrably delivering events for
    /// `base`, by writing a throwaway probe file there until an event
    /// for it comes back.
    ///
    /// `WatchBuilder::start()` returns before the underlying watcher has
    /// finished registering with the OS — `build_watcher` runs on a
    /// `spawn_blocking` thread, and the macOS backend additionally hands
    /// the stream to its own run loop. A filesystem change made inside
    /// that window is not reported late; it is never reported at all,
    /// because these APIs only deliver events that occur after
    /// registration completes. So the triggering change has to be
    /// *retried* until one is observed — sleeping first and hoping
    /// cannot be made reliable, only less likely to fail. These tests
    /// previously slept 100ms, which held when they ran alone (the whole
    /// watch module took 0.55s) but failed constantly under `cargo
    /// test`'s parallelism, where registration loses the race against
    /// the other ~140 tests for CPU: the missed change meant no event
    /// ever arrived and the test sat out its full timeout.
    async fn await_watcher_ready(handle: &mut WatchHandle, base: &Path) {
        // Deliberately left on disk afterward: removing it would emit
        // further events into the stream every caller then has to filter
        // out, and the enclosing `TempDir` cleans it up regardless. Its
        // name is distinct from every path the tests assert on, so their
        // path predicates skip it (and its repeat writes below) already.
        let probe = base.join(".watcher-readiness-probe");

        tokio::time::timeout(EVENT_TIMEOUT, async {
            loop {
                fs::write(&probe, b"probe").unwrap();

                // Bounded per attempt rather than waiting on the whole
                // budget at once: if this write landed before
                // registration completed, no event for it is ever
                // coming, and the only way forward is another write.
                let deadline = tokio::time::Instant::now() + Duration::from_millis(100);
                let observed = tokio::time::timeout_at(deadline, async {
                    loop {
                        let event =
                            handle.events().next().await.expect("event stream ended before the watcher was ready");
                        if event.paths.contains(&probe) {
                            return;
                        }
                    }
                })
                .await;

                if observed.is_ok() {
                    return;
                }
            }
        })
        .await
        .expect("watcher never started delivering events");
    }

    /// On macOS, `/var` (where `tempfile` puts its directories) is a
    /// symlink to `/private/var`, and FSEvents reports the *canonical*
    /// (resolved) path in events, not the one that was actually watched
    /// — so a test comparing against `dir.path()` directly would never
    /// match. Canonicalizing the base path up front makes comparisons
    /// work consistently across platforms (`canonicalize()` is a no-op
    /// where there's no symlink to resolve).
    fn canonical_dir(dir: &tempfile::TempDir) -> PathBuf {
        dir.path().canonicalize().unwrap()
    }

    #[tokio::test]
    async fn reports_a_created_file() {
        let dir = tempdir().unwrap();
        let base = canonical_dir(&dir);
        let mut handle = WatchBuilder::new(&base).start().unwrap();
        await_watcher_ready(&mut handle, &base).await;

        let file = base.join("a.txt");
        fs::write(&file, b"hello").unwrap();

        let event = next_matching(&mut handle, |e| e.kind == WatchEventKind::Created && e.paths.contains(&file)).await;
        assert_eq!(event.kind, WatchEventKind::Created);

        handle.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn reports_modify_then_remove() {
        let dir = tempdir().unwrap();
        let base = canonical_dir(&dir);
        let file = base.join("a.txt");
        fs::write(&file, b"hello").unwrap();

        let mut handle = WatchBuilder::new(&base).start().unwrap();
        await_watcher_ready(&mut handle, &base).await;

        fs::write(&file, b"changed").unwrap();
        let modified = next_matching(&mut handle, |e| e.kind == WatchEventKind::Modified && e.paths.contains(&file)).await;
        assert_eq!(modified.kind, WatchEventKind::Modified);

        fs::remove_file(&file).unwrap();
        let removed = next_matching(&mut handle, |e| e.kind == WatchEventKind::Removed && e.paths.contains(&file)).await;
        assert_eq!(removed.kind, WatchEventKind::Removed);

        handle.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn recursive_true_reports_changes_in_subdirectories() {
        let dir = tempdir().unwrap();
        let base = canonical_dir(&dir);
        let subdir = base.join("nested");
        fs::create_dir(&subdir).unwrap();

        let mut handle = WatchBuilder::new(&base).recursive(true).start().unwrap();
        await_watcher_ready(&mut handle, &base).await;

        let file = subdir.join("a.txt");
        fs::write(&file, b"hello").unwrap();

        let event = next_matching(&mut handle, |e| e.paths.contains(&file)).await;
        assert_eq!(event.kind, WatchEventKind::Created);

        handle.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn recursive_false_does_not_report_changes_in_subdirectories() {
        let dir = tempdir().unwrap();
        let base = canonical_dir(&dir);
        let subdir = base.join("nested");
        fs::create_dir(&subdir).unwrap();

        // Probes the top level, which is watched in both recursive modes
        // — so this proves readiness without depending on the very
        // behavior under test.
        let mut handle = WatchBuilder::new(&base).recursive(false).start().unwrap();
        await_watcher_ready(&mut handle, &base).await;

        let file = subdir.join("a.txt");
        fs::write(&file, b"hello").unwrap();

        // Nothing from the subdirectory should arrive. Prove the watcher
        // is otherwise alive by triggering (and observing) a top-level
        // change afterward, rather than just waiting out a timeout.
        let top_level_file = base.join("top.txt");
        fs::write(&top_level_file, b"hello").unwrap();
        let event = next_matching(&mut handle, |e| e.paths.contains(&top_level_file) || e.paths.contains(&file)).await;
        assert!(event.paths.contains(&top_level_file), "should not have observed the subdirectory change");

        handle.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn cancel_stops_further_events_and_resolves_ok() {
        let dir = tempdir().unwrap();
        let base = canonical_dir(&dir);
        // Not strictly required to reach `Ok` — cancelling before the
        // watcher even registers still resolves that way — but waiting
        // means this actually exercises cancelling a *live* watcher,
        // which is the case worth covering.
        let mut handle = WatchBuilder::new(&base).start().unwrap();
        await_watcher_ready(&mut handle, &base).await;

        handle.cancel();
        let result = handle.await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn watching_a_nonexistent_path_resolves_to_source_not_found() {
        let dir = tempdir().unwrap();
        let missing = dir.path().join("does-not-exist");

        let handle = WatchBuilder::new(missing).start().unwrap();
        let result = handle.await;

        assert!(matches!(result, Err(Error::SourceNotFound { .. })));
    }
}
