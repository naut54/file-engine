use std::fs;
use std::path::{Path, PathBuf};

use crate::faults::Fault;

/// One entry the generator deliberately sabotaged, and which fault it
/// planted — the checker verifies the observed outcome against this.
pub struct SabotagedEntry {
    pub relative_path: PathBuf,
    pub fault: Fault,
}

/// Builds a small source tree under `src_dir`, one file per requested
/// fault, and applies exactly that fault's sabotage. `dest_dir` is only
/// needed for `DestExists`, which sabotages the destination side rather
/// than the source.
///
/// No use of anything `pub(crate)` in `file_engine` — every mutation
/// here is done directly on disk, matching the "black-box, through the
/// public API only" decision in the design doc.
pub fn build(src_dir: &Path, dest_dir: &Path, faults: &[Fault]) -> std::io::Result<Vec<SabotagedEntry>> {
    let mut sabotaged = Vec::new();

    for (i, &fault) in faults.iter().enumerate() {
        let relative_path = PathBuf::from(format!("entry_{i}.txt"));
        let src_path = src_dir.join(&relative_path);
        fs::write(&src_path, b"fault-harness payload")?;

        match fault {
            Fault::PermissionDenied => {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    fs::set_permissions(&src_path, fs::Permissions::from_mode(0o000))?;
                }
                #[cfg(not(unix))]
                {
                    // Not implemented on non-Unix hosts: there's no
                    // equivalent one-call way to make a file unreadable
                    // to its own owner via `std::fs` alone. See
                    // faults.rs's UNIVERSAL doc comment.
                    eprintln!(
                        "warning: PermissionDenied sabotage is a no-op on this platform; {} will copy successfully",
                        relative_path.display()
                    );
                }
            }
            Fault::DestExists => {
                let dest_path = dest_dir.join(&relative_path);
                if let Some(parent) = dest_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::write(&dest_path, b"pre-existing destination content")?;
            }
            Fault::CaseCollision => {
                // Only produces two distinct dirents if `src_dir` is on
                // a case-sensitive filesystem — on a case-insensitive
                // one this write aliases to `src_path` above instead of
                // creating a sibling (confirmed empirically; see the
                // design doc's "Status" section). `main.rs` is
                // responsible for not calling this arm unless that's
                // true.
                let sibling = src_dir.join(format!("ENTRY_{i}.txt"));
                fs::write(&sibling, b"colliding sibling")?;
            }
            Fault::ReservedName => {
                // The fault *is* the name, not a sibling file — rename
                // in place rather than appending a second entry.
                let reserved_path = src_dir.join("CON");
                fs::rename(&src_path, &reserved_path)?;
                sabotaged.push(SabotagedEntry {
                    relative_path: PathBuf::from("CON"),
                    fault,
                });
                continue;
            }
        }

        sabotaged.push(SabotagedEntry { relative_path, fault });
    }

    Ok(sabotaged)
}
