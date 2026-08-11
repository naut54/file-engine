//! One-directional mirror sync: copies new/changed entries from `source`
//! into `dest`, then deletes anything in `dest` that no longer exists in
//! `source` (orphans).
//!
//!     cargo run --example basic_sync --features sync -- <source> <dest> [--allow-integrity-risk]
//!
//! Unlike copy/move, `sync` reports two separate outcomes — a copy phase
//! and a delete phase — since they act on different entry sets. If the
//! copy phase doesn't finish cleanly, the delete phase is skipped
//! entirely: `sync` would rather leave a stale file for the next run to
//! catch than delete real data while the copy side is in a known-
//! incomplete state.

use std::env;

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
            eprintln!("usage: basic_sync [--allow-integrity-risk] <source> <dest>");
            eprintln!();
            eprintln!("  --allow-integrity-risk  proceed even if the destination filesystem");
            eprintln!("                          has a known write-integrity risk on this");
            eprintln!("                          platform (currently: exFAT on macOS)");
            std::process::exit(2);
        }
    };

    println!("syncing {source} -> {dest}");

    let engine = FileEngine::new();
    let mut handle = engine
        .sync(&source, &dest)
        .allow_filesystem_integrity_risk(allow_integrity_risk)
        .start()?;

    let mut completed: usize = 0;

    // Both the copy and delete phases send events through the same
    // stream — a second `Started` event marks the boundary between them.
    // For a simple demo we just log everything as it arrives rather than
    // tracking which phase we're in.
    while let Some(progress) = tokio_stream::StreamExt::next(handle.progress()).await {
        match progress {
            Progress::Started {
                bytes_total,
                entries_total,
            } => match bytes_total {
                Some(bytes) => println!("started: {entries_total} entries, {bytes} bytes total"),
                None => println!("started: {entries_total} entries"),
            },
            Progress::DirectoriesStarted { total } => println!("creating {total} directories..."),
            Progress::EntryStarted { .. } | Progress::DirectoryCompleted { .. } => {}
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
                eprintln!("FAILED: {}", entry.relative_path.display());
            }
            Progress::DirectoryFailed { path } => {
                eprintln!("FAILED (creating directory): {}", path.display());
            }
            // `Progress` is `#[non_exhaustive]`; see `basic_copy.rs` for
            // what `Progress::Planned` carries and how to use it.
            _ => {}
        }
    }

    let sync_outcome = handle.await?;

    println!();
    // Timed per phase, so these two don't add up to the whole run — the
    // diff that precedes them is counted in neither, and a delete phase
    // skipped because the copy phase stopped early reports zero.
    println!(
        "done in {:?} (copy) + {:?} (delete)",
        sync_outcome.copy.duration, sync_outcome.delete.duration
    );

    println!(
        "copy phase: succeeded {}, failed {}",
        sync_outcome.copy.succeeded.len(),
        sync_outcome.copy.failed.len()
    );
    for (entry, err) in &sync_outcome.copy.failed {
        println!("  - {}: {err}", entry.relative_path.display());
    }
    if let Some(reason) = sync_outcome.copy.stopped_early {
        println!("  copy phase stopped early: {reason:?}");
    }

    println!(
        "delete phase: succeeded {}, failed {}",
        sync_outcome.delete.succeeded.len(),
        sync_outcome.delete.failed.len()
    );
    for (entry, err) in &sync_outcome.delete.failed {
        println!("  - {}: {err}", entry.relative_path.display());
    }

    Ok(())
}
