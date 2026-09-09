mod checker;
mod dest_probe;
mod disk_image;
mod faults;
mod fixtures;
mod recorder;
mod runner;

use std::path::{Path, PathBuf};

use disk_image::MountedImage;
use faults::Fault;

/// v1 entry/batch-level fault-injection harness. Generates a sabotaged
/// fixture tree, runs one real copy through `file_engine`'s public API,
/// and checks that every planted fault produced the `Error` variant it
/// was designed to — see `dev-docs/design/fault-injection-harness.md`.
#[tokio::main]
async fn main() -> std::io::Result<()> {
    let root = std::env::temp_dir().join(format!("fault-harness-{}", std::process::id()));
    let corpus_path = root.join("corpus.jsonl");

    // Both guards, when present, must outlive every use of the
    // directories they back — `Drop` unmounts and deletes the backing
    // image, so binding either to `_` (dropping it immediately) would
    // pull the volume out from under the harness mid-run.
    let (src_dir, _src_volume) = case_sensitive_source(&root)?;
    let (dest_dir, _dest_volume) = windows_naming_destination(&root)?;

    // Probed once, reused both for deciding what to plant and as the
    // corpus row's environment columns.
    let source_case_insensitive = dest_probe::is_case_insensitive(&src_dir)?;
    let dest_case_insensitive = dest_probe::is_case_insensitive(&dest_dir)?;
    let dest_windows_naming_rules = dest_probe::has_windows_naming_rules(&dest_dir)?;

    let mut planted: Vec<Fault> = Fault::UNIVERSAL.to_vec();

    #[cfg(not(unix))]
    {
        planted.retain(|f| *f != Fault::PermissionDenied);
        eprintln!("skipping PermissionDenied: not implemented on non-Unix hosts (see fixtures.rs)");
    }

    for &fault in Fault::DESTINATION_CONDITIONAL {
        let reachable = match fault {
            // Needs BOTH: the source filesystem must be case-sensitive
            // (otherwise the two colliding names alias to one dirent —
            // confirmed empirically, see the design doc's correction —
            // there's nothing to scan as two entries in the first
            // place) AND the destination must be case-insensitive
            // (otherwise `validate()` has no reason to reject them).
            Fault::CaseCollision => !source_case_insensitive && dest_case_insensitive,
            Fault::ReservedName => dest_windows_naming_rules,
            _ => unreachable!("DESTINATION_CONDITIONAL only lists these two faults"),
        };
        if reachable {
            planted.push(fault);
        } else {
            eprintln!("skipping {fault:?}: not reachable given this source/destination filesystem combination");
        }
    }

    let sabotaged = fixtures::build(&src_dir, &dest_dir, &planted)?;

    let record = runner::run_copy(&src_dir, &dest_dir)
        .await
        .expect("copy should complete (per-entry failures land in outcome.failed, not Err)");

    let results = checker::check(&record.outcome, &sabotaged);

    let env = recorder::Environment {
        source_case_insensitive,
        dest_case_insensitive,
        dest_windows_naming_rules,
    };
    let rows = recorder::rows(
        &env,
        record.shape,
        record.outcome.duration.as_millis(),
        &results,
    );
    recorder::record(&corpus_path, &rows)?;

    let mut any_mismatch = false;
    for r in &results {
        let status = if r.passed {
            "OK"
        } else {
            any_mismatch = true;
            "MISMATCH"
        };
        println!(
            "{status}  {}  expected={}  observed={:?}",
            r.relative_path.display(),
            r.expected,
            r.observed
        );
    }

    if any_mismatch {
        std::process::exit(1);
    }

    Ok(())
}

/// The source tree's location: a mounted case-sensitive volume when one
/// can be created (macOS only, for now — see `disk_image.rs`), otherwise
/// an ordinary subdirectory of `root` on the OS default volume. Falling
/// back rather than failing means a machine without `hdiutil`, or one
/// where it errors for any reason, still runs every fault except
/// `CaseCollision` (which the reachability check above correctly skips
/// when the source turns out to be case-insensitive).
fn case_sensitive_source(root: &Path) -> std::io::Result<(PathBuf, Option<MountedImage>)> {
    #[cfg(target_os = "macos")]
    {
        // Kept to 11 characters or fewer — see `MountedImage::create`'s
        // doc comment for why (`hdiutil` rejects longer exFAT volume
        // labels outright; APFS has no such limit, but the same short
        // convention is used for both here rather than having two
        // different naming rules to remember).
        let name = format!("FHS{:05}", std::process::id() % 100_000);
        match MountedImage::create(root, &name, "Case-sensitive APFS") {
            Ok(volume) => {
                let path = volume.path().to_path_buf();
                return Ok((path, Some(volume)));
            }
            Err(e) => {
                eprintln!(
                    "warning: could not create case-sensitive source volume ({e}); CaseCollision will be skipped as unreachable"
                );
            }
        }
    }

    let path = root.join("src");
    std::fs::create_dir_all(&path)?;
    Ok((path, None))
}

/// The destination tree's location: a mounted exFAT volume when one can
/// be created (macOS only, for now). exFAT is both case-insensitive and
/// Windows-naming-rules-enforcing per `file_engine`'s own `fs_caps`
/// classification, so this single image satisfies `CaseCollision`'s and
/// `ReservedName`'s destination requirement at once — no need for two
/// separate destination images. Falls back to an ordinary subdirectory
/// of `root` (still case-insensitive by default on macOS, just without
/// Windows naming rules) if the image can't be created; `ReservedName`
/// then correctly gets skipped as unreachable, same fallback pattern as
/// the source side.
fn windows_naming_destination(root: &Path) -> std::io::Result<(PathBuf, Option<MountedImage>)> {
    #[cfg(target_os = "macos")]
    {
        let name = format!("FHD{:05}", std::process::id() % 100_000);
        match MountedImage::create(root, &name, "ExFAT") {
            Ok(volume) => {
                let path = volume.path().to_path_buf();
                return Ok((path, Some(volume)));
            }
            Err(e) => {
                eprintln!(
                    "warning: could not create exFAT destination volume ({e}); ReservedName will be skipped as unreachable"
                );
            }
        }
    }

    let path = root.join("dest");
    std::fs::create_dir_all(&path)?;
    Ok((path, None))
}
