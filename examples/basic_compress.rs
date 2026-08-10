//! Compresses a file or directory into a zip or gzip archive, printing
//! live progress and a final summary.
//!
//!     cargo run --example basic_compress --features compress -- <source> <dest> [--format zip|gz]
//!
//! Format is inferred from `dest`'s extension (`.zip` / `.gz`) unless
//! `--format` overrides it. `.gz` only accepts a single file, not a
//! directory — use a `.zip` destination (or `--format zip`) for a
//! directory.

use std::env;
use std::time::Instant;

use file_engine::{CompressFormat, FileEngine, Progress};

#[tokio::main]
async fn main() -> file_engine::Result<()> {
    let mut format: Option<CompressFormat> = None;
    let mut positional = Vec::new();
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--format" {
            format = match args.next().as_deref() {
                Some("zip") => Some(CompressFormat::Zip),
                Some("gz") => Some(CompressFormat::Gzip),
                other => {
                    eprintln!("--format expects \"zip\" or \"gz\", got {other:?}");
                    std::process::exit(2);
                }
            };
        } else {
            positional.push(arg);
        }
    }

    let mut positional = positional.into_iter();
    let (source, dest) = match (positional.next(), positional.next()) {
        (Some(source), Some(dest)) => (source, dest),
        _ => {
            eprintln!("usage: basic_compress [--format zip|gz] <source> <dest>");
            std::process::exit(2);
        }
    };

    println!("compressing {source} -> {dest}");

    let engine = FileEngine::new();
    let mut builder = engine.compress(&source, &dest);
    if let Some(format) = format {
        builder = builder.format(format);
    }
    let mut handle = builder.start()?;

    let start = Instant::now();
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
            Progress::EntryStarted { .. } => {}
            Progress::EntryCompleted { .. } => {
                completed += 1;
                if completed.is_multiple_of(200) {
                    println!(
                        "...{completed} entries done ({:?} elapsed)",
                        start.elapsed()
                    );
                }
            }
            Progress::EntryFailed { entry } => {
                eprintln!("FAILED: {}", entry.relative_path.display());
            }
            // compress.rs has no directory-creation pre-pass (there's no
            // destination directory tree to mirror — everything lands
            // inside one archive file), so these never fire here.
            Progress::DirectoriesStarted { .. }
            | Progress::DirectoryCompleted { .. }
            | Progress::DirectoryFailed { .. } => {}
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

    if let Some(reason) = outcome.stopped_early {
        println!("stopped early: {reason:?}");
    }

    Ok(())
}
