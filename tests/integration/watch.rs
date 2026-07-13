use std::fs;
use std::time::Duration;

use file_engine::{FileEngine, WatchEventKind};
use tokio_stream::StreamExt;

#[tokio::test]
async fn observes_a_file_creation() {
    let dir = tempfile::tempdir().unwrap();

    let engine = FileEngine::new();
    let mut handle = engine.watch(dir.path()).start().unwrap();

    // Give the OS-level watch (inotify/FSEvents/ReadDirectoryChangesW)
    // time to actually register before triggering the event we're
    // watching for — a real race inherent to filesystem watching, not
    // specific to this crate's implementation.
    tokio::time::sleep(Duration::from_millis(300)).await;

    fs::write(dir.path().join("new.txt"), b"hi").unwrap();

    // Backends differ in exactly how a fresh file's creation is reported
    // (a bare `Created`, or a `Created` immediately followed by `Modified`
    // once content is flushed) — collect events for a short window and
    // assert at least one references the new file, rather than pinning to
    // a single exact event, to avoid backend-specific flakiness.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut saw_new_file = false;
    while tokio::time::Instant::now() < deadline {
        let Ok(Some(event)) = tokio::time::timeout_at(deadline, handle.events().next()).await
        else {
            break;
        };
        if event.paths.iter().any(|p| p.ends_with("new.txt"))
            && matches!(
                event.kind,
                WatchEventKind::Created | WatchEventKind::Modified
            )
        {
            saw_new_file = true;
            break;
        }
    }

    assert!(
        saw_new_file,
        "expected a Created/Modified event for new.txt"
    );
    handle.cancel();
}
