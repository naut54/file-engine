//! Moves several independent sources into one destination directory as a
//! single batched operation, printing live progress and a final summary.
//!
//!     cargo run --example basic_move_many --features operations -- <dest_dir> <source>...
//!
//! Each source keeps its own basename under `dest_dir`. Sources on the
//! same filesystem as `dest_dir` move via an atomic rename each (no
//! progress events); the batching pipeline (and its progress events)
//! only kicks in for sources that need the cross-filesystem fallback.

use std::env;

use file_engine::{FileEngine, Progress};

#[tokio::main]
async fn main() -> file_engine::Result<()> {
    let mut args = env::args().skip(1);
    let dest = match args.next() {
        Some(dest) => dest,
        None => {
            eprintln!("usage: basic_move_many <dest_dir> <source>...");
            std::process::exit(2);
        }
    };
    let sources: Vec<String> = args.collect();
    if sources.is_empty() {
        eprintln!("usage: basic_move_many <dest_dir> <source>...");
        std::process::exit(2);
    }

    println!("moving {} source(s) into {dest}", sources.len());

    let engine = FileEngine::new();
    let mut handle = engine
        .move_many(sources, &dest)
        // Same meaning as `MoveBuilder::overwrite` — only matters for
        // sources that need the cross-filesystem fallback; the
        // atomic-rename fast path replaces an existing destination file
        // outright regardless of this flag, matching `basic_move.rs`.
        .overwrite(true)
        .start()?;

    let mut completed: usize = 0;

    while let Some(progress) = tokio_stream::StreamExt::next(handle.progress()).await {
        match progress {
            Progress::Started {
                bytes_total,
                entries_total,
            } => match bytes_total {
                Some(bytes) => println!("started: {entries_total} entries, {bytes} bytes total"),
                None => println!("started: {entries_total} entries"),
            },
            Progress::EntryCompleted { .. } => {
                completed += 1;
                if completed.is_multiple_of(200) {
                    println!(
                        "...{completed} entries done ({:?} elapsed)",
                        handle.elapsed()
                    );
                }
            }
            Progress::EntryFailed { entry } => {
                eprintln!("FAILED (during move): {}", entry.relative_path.display());
            }
            // `Progress` is `#[non_exhaustive]`; see `basic_copy.rs` for
            // the full set of variants.
            _ => {}
        }
    }

    let outcome = handle.await?;

    println!();
    println!("done in {:?}", outcome.duration);
    println!("succeeded: {}", outcome.succeeded.len());
    println!("failed: {}", outcome.failed.len());
    for (entry, err) in &outcome.failed {
        println!("  - {}: {err}", entry.relative_path.display());
    }

    // Whole-source failures: a source's own atomic-rename attempt failed
    // for a reason other than crossing filesystems (permission denied, a
    // source that vanished before it could be moved, ...) — reported
    // here rather than under `failed` since no per-file `Entry` was ever
    // built for it. `MoveManyBuilder`-only; always empty for `.move_path()`.
    if !outcome.sources_failed.is_empty() {
        println!("sources_failed: {}", outcome.sources_failed.len());
        for (path, err) in &outcome.sources_failed {
            println!("  - {}: {err}", path.display());
        }
    }

    if !outcome.cleanup_failed.is_empty() {
        println!("cleanup_failed: {}", outcome.cleanup_failed.len());
        for (entry, err) in &outcome.cleanup_failed {
            println!("  - {}: {err}", entry.relative_path.display());
        }
    }

    if let Some(reason) = outcome.stopped_early {
        println!("stopped early: {reason:?}");
    }

    Ok(())
}
