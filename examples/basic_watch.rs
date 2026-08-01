//! Watches a path for filesystem changes and prints each event as it
//! arrives, until interrupted with Ctrl+C.
//!
//!     cargo run --example basic_watch --features watch -- <path> [--recursive]
//!
//! Unlike copy/move/sync/compress, watching has no natural end — there's
//! no fixed set of entries to finish processing, just an open-ended
//! stream of events. `WatchHandle` reflects that: it's awaitable like
//! `Handle<T>`, but only ever resolves once you call `.cancel()` (or the
//! watcher hits an unrecoverable error) — there's no "outcome" to
//! summarize at the end the way the other examples have.

use std::env;

use file_engine::FileEngine;
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> file_engine::Result<()> {
    let mut recursive = false;
    let mut positional = Vec::new();
    for arg in env::args().skip(1) {
        if arg == "--recursive" {
            recursive = true;
        } else {
            positional.push(arg);
        }
    }

    let Some(path) = positional.into_iter().next() else {
        eprintln!("usage: basic_watch [--recursive] <path>");
        std::process::exit(2);
    };

    println!("watching {path} (recursive: {recursive}), Ctrl+C to stop");

    let engine = FileEngine::new();
    let mut handle = engine.watch(&path).recursive(recursive).start()?;

    loop {
        tokio::select! {
            event = handle.events().next() => {
                match event {
                    Some(event) => println!("{:?}: {:?}", event.kind, event.paths),
                    // The watcher stopped producing events on its own
                    // (e.g. the watched path was removed) — nothing left
                    // to wait on.
                    None => break,
                }
            }
            _ = tokio::signal::ctrl_c() => {
                println!("stopping...");
                handle.cancel();
                break;
            }
        }
    }

    handle.await?;
    println!("stopped");
    Ok(())
}
