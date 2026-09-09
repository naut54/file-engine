//! Removes files under a path matching an extension filter, printing
//! live progress and a final summary.
//!
//!     cargo run --example basic_remove --features remove -- <path> <ext> [--delete] [--hard-delete]
//!
//! Without `--delete`, this only previews what would be removed —
//! `RemoveBuilder` defaults to a dry run. Without `--hard-delete`,
//! matched files go to the platform trash rather than being unlinked
//! permanently.

use std::env;

use file_engine::{FileEngine, Progress};

#[tokio::main]
async fn main() -> file_engine::Result<()> {
    let mut delete = false;
    let mut hard_delete = false;
    let mut positional = Vec::new();
    for arg in env::args().skip(1) {
        match arg.as_str() {
            "--delete" => delete = true,
            "--hard-delete" => hard_delete = true,
            _ => positional.push(arg),
        }
    }
    let mut positional = positional.into_iter();
    let (path, ext) = match (positional.next(), positional.next()) {
        (Some(path), Some(ext)) => (path, ext),
        _ => {
            eprintln!("usage: basic_remove [--delete] [--hard-delete] <path> <extension>");
            eprintln!();
            eprintln!("  --delete       actually remove matches (default: preview only)");
            eprintln!("  --hard-delete  unlink permanently instead of using the platform trash");
            std::process::exit(2);
        }
    };

    println!(
        "scanning {path} for *.{ext} files{}",
        if delete { "" } else { " (dry run)" }
    );

    let engine = FileEngine::new();
    let mut handle = engine
        .remove(&path)
        .extensions([ext])
        .dry_run(!delete)
        .hard_delete(hard_delete)
        .start()?;

    while let Some(progress) = tokio_stream::StreamExt::next(handle.progress()).await {
        match progress {
            Progress::Started { entries_total, .. } => {
                println!("started: {entries_total} entries");
            }
            Progress::EntryCompleted { entry } => {
                println!("removed: {}", entry.relative_path.display());
            }
            Progress::EntryFailed { entry } => {
                eprintln!("FAILED (removing): {}", entry.relative_path.display());
            }
            // `Progress` is `#[non_exhaustive]`; see `basic_copy.rs` for
            // the other variants this example doesn't need.
            _ => {}
        }
    }

    let outcome = handle.await?;

    println!();
    println!("done in {:?}", outcome.duration);
    if !outcome.previewed.is_empty() {
        println!("would remove {} entries:", outcome.previewed.len());
        for entry in &outcome.previewed {
            println!("  - {}", entry.relative_path.display());
        }
    }
    println!("succeeded: {}", outcome.succeeded.len());
    println!("failed: {}", outcome.failed.len());
    for (entry, err) in &outcome.failed {
        println!("  - {}: {err}", entry.relative_path.display());
    }

    if let Some(reason) = outcome.stopped_early {
        println!("stopped early: {reason:?}");
    }

    Ok(())
}
