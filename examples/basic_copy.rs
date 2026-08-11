//! Copies a file or directory tree, printing live progress and a final
//! summary.
//!
//!     cargo run --example basic_copy --features operations -- <source> <dest> [--allow-integrity-risk]

use std::env;

use file_engine::{EtaEstimator, FileEngine, Progress};

/// `1h 02m 03s` / `2m 03s` / `3s` — an ETA is read at a glance, and
/// `Duration`'s own `{:?}` renders sub-second precision nobody needs here.
fn format_eta(eta: std::time::Duration) -> String {
    let total = eta.as_secs();
    let (h, m, s) = (total / 3600, (total % 3600) / 60, total % 60);
    if h > 0 {
        format!("{h}h {m:02}m {s:02}s")
    } else if m > 0 {
        format!("{m}m {s:02}s")
    } else if total < 10 {
        // Whole seconds would round the entire endgame of a fast copy to
        // "0s", which reads as finished while work is still running.
        format!("{:.1}s", eta.as_secs_f64())
    } else {
        format!("{s}s")
    }
}

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
            eprintln!("usage: basic_copy [--allow-integrity-risk] <source> <dest>");
            eprintln!();
            eprintln!("  --allow-integrity-risk  proceed even if the destination filesystem");
            eprintln!("                          has a known write-integrity risk on this");
            eprintln!("                          platform (currently: exFAT on macOS)");
            std::process::exit(2);
        }
    };

    println!("copying {source} -> {dest}");
    if allow_integrity_risk {
        println!("(proceeding despite any destination filesystem integrity risk)");
    }

    let engine = FileEngine::new();
    let mut handle = engine
        .copy(&source, &dest)
        .overwrite(true)
        .allow_filesystem_integrity_risk(allow_integrity_risk)
        .start()?;

    let mut completed: usize = 0;
    let mut dirs_completed: usize = 0;
    let mut eta = EtaEstimator::new();

    while let Some(progress) = tokio_stream::StreamExt::next(handle.progress()).await {
        // Fed every event, including the ones this example doesn't print:
        // each either reports work done or closes out a span of wall time.
        eta.observe(&progress);

        let remaining = match eta.estimate() {
            Some(eta) => format!(", ~{} remaining", format_eta(eta)),
            // Nothing has completed yet, so there is no measured rate to
            // extrapolate from. Printing nothing beats printing a number
            // that will be wrong by an order of magnitude a second later.
            None => String::new(),
        };

        match progress {
            Progress::Planned {
                directories,
                small_files,
                small_bytes,
                large_files,
                large_bytes,
                ..
            } => {
                println!(
                    "planned: {directories} directories, {small_files} small files \
                     ({small_bytes} bytes), {large_files} large files ({large_bytes} bytes)"
                );
            }
            Progress::DirectoriesStarted { total } => {
                println!("creating {total} directories...");
            }
            Progress::DirectoryCompleted { .. } => {
                dirs_completed += 1;
                if dirs_completed.is_multiple_of(500) {
                    println!(
                        "...{dirs_completed} directories done ({:?} elapsed{remaining})",
                        handle.elapsed()
                    );
                }
            }
            Progress::DirectoryFailed { path } => {
                eprintln!("FAILED (creating directory): {}", path.display());
            }
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
                        "...{completed} entries done ({:?} elapsed{remaining})",
                        handle.elapsed()
                    );
                }
            }
            // Sampled while a large file is still being written. This is
            // the only progress a single-large-file copy produces before
            // it finishes.
            Progress::EntryProgress {
                entry,
                bytes_copied,
            } => {
                let percent = if entry.size > 0 {
                    bytes_copied as f64 / entry.size as f64 * 100.0
                } else {
                    100.0
                };
                println!(
                    "...{} {percent:.0}% ({:?} elapsed{remaining})",
                    entry.relative_path.display(),
                    handle.elapsed()
                );
            }
            Progress::EntryFailed { entry } => {
                eprintln!("FAILED (during copy): {}", entry.relative_path.display());
            }
            // `Progress` is `#[non_exhaustive]`: a future variant must not
            // stop this example compiling.
            _ => {}
        }
    }

    let outcome = handle.await?;

    println!();
    // From the outcome rather than a clock kept here: `handle.await`
    // consumed the handle, so `handle.elapsed()` is no longer reachable.
    println!("done in {:?}", outcome.duration);
    println!("succeeded: {}", outcome.succeeded.len());
    println!("failed: {}", outcome.failed.len());
    for (entry, err) in &outcome.failed {
        println!("  - {}: {err}", entry.relative_path.display());
    }

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
