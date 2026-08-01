//! Moves a file or directory tree, printing live progress and a final
//! summary.
//!
//!     cargo run --example basic_move --features operations -- <source> <dest> [--allow-integrity-risk]
//!
//! If `source` and `dest` are on the same filesystem, the move completes
//! as a single atomic rename and you'll see no progress output at all —
//! that's expected, not a hang; there's nothing to report progress on.
//! The batching pipeline (and its progress events) only kicks in for the
//! cross-filesystem fallback.

use std::env;
use std::time::Instant;

use file_engine::{FileEngine, Progress};

#[tokio::main]
async fn main() -> file_engine::Result<()> {
    let mut allow_integrity_risk = false;
    let mut positional = Vec::new();
    for arg in env::args().skip(1) {
        if arg == "--allow-integrity-risk" {
            allow_integrity_risk = true;
        } else {
            positional.push(arg);
        }
    }
    let mut positional = positional.into_iter();
    let (source, dest) = match (positional.next(), positional.next()) {
        (Some(source), Some(dest)) => (source, dest),
        _ => {
            eprintln!("usage: basic_move [--allow-integrity-risk] <source> <dest>");
            eprintln!();
            eprintln!("  --allow-integrity-risk  proceed even if the destination filesystem");
            eprintln!("                          has a known write-integrity risk on this");
            eprintln!("                          platform (currently: exFAT on macOS) —");
            eprintln!("                          only relevant if the cross-filesystem");
            eprintln!("                          fallback path is taken");
            std::process::exit(2);
        }
    };

    println!("moving {source} -> {dest}");

    let engine = FileEngine::new();
    let mut handle = engine
        .move_path(&source, &dest)
        // Overwrite files at the destination that already exist under
        // the same relative path — only matters for the cross-filesystem
        // fallback; the atomic-rename fast path replaces `dest` outright
        // regardless of this flag, matching `std::fs::rename`'s own
        // behavior.
        .overwrite(true)
        .allow_filesystem_integrity_risk(allow_integrity_risk)
        .start()?;

    let start = Instant::now();
    let mut completed: usize = 0;
    let mut dirs_completed: usize = 0;

    while let Some(progress) = tokio_stream::StreamExt::next(handle.progress()).await {
        match progress {
            Progress::DirectoriesStarted { total } => {
                println!("creating {total} directories...");
            }
            Progress::DirectoryCompleted { .. } => {
                dirs_completed += 1;
                if dirs_completed % 500 == 0 {
                    println!("...{dirs_completed} directories done ({:?} elapsed)", start.elapsed());
                }
            }
            Progress::DirectoryFailed { path } => {
                eprintln!("FAILED (creating directory): {}", path.display());
            }
            Progress::Started { bytes_total, entries_total } => match bytes_total {
                Some(bytes) => println!("started: {entries_total} entries, {bytes} bytes total"),
                None => println!("started: {entries_total} entries"),
            },
            Progress::EntryStarted { .. } => {}
            Progress::EntryCompleted { .. } => {
                completed += 1;
                if completed % 200 == 0 {
                    println!("...{completed} entries done ({:?} elapsed)", start.elapsed());
                }
            }
            Progress::EntryFailed { entry } => {
                eprintln!("FAILED (during move): {}", entry.relative_path.display());
            }
        }
    }

    let outcome = handle.await?;
    let elapsed = start.elapsed();

    println!();
    println!("done in {elapsed:?}");
    println!("succeeded: {}", outcome.succeeded.len());
    println!("failed: {}", outcome.failed.len());
    for (entry, err) in &outcome.failed {
        println!("  - {}: {err}", entry.relative_path.display());
    }

    // Populated only if an entry copied successfully but its original
    // source couldn't then be deleted — the move partially duplicated
    // data instead of relocating it. Copy never populates this field;
    // it's specific to move's deferred deletion sweep.
    if !outcome.cleanup_failed.is_empty() {
        println!("cleanup_failed: {}", outcome.cleanup_failed.len());
        for (entry, err) in &outcome.cleanup_failed {
            println!("  - {}: {err}", entry.relative_path.display());
        }
    }

    if !outcome.directories_failed.is_empty() {
        println!("directories_failed: {}", outcome.directories_failed.len());
        for (path, err) in &outcome.directories_failed {
            println!("  - {}: {err}", path.display());
        }
    }

    if let Some(reason) = outcome.stopped_early {
        println!("stopped early: {reason:?}");
    }

    Ok(())
}
