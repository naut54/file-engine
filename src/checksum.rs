use std::path::Path;
#[cfg(feature = "operations")]
use std::path::PathBuf;

use crate::error::{classify_io_error, Result};

/// Blocking (whole-file read + hash), meant to run inside
/// `spawn_blocking` — shared by `analysis::hash`'s duplicate detection
/// and `planner::action`/`operations::move_path`'s "is the destination
/// already identical to the source" safeguard, rather than duplicating
/// the same blake3-over-`std::io::copy` logic in both places.
pub(crate) fn hash_file(path: &Path) -> Result<[u8; 32]> {
    let mut hasher = blake3::Hasher::new();
    let mut file =
        std::fs::File::open(path).map_err(|e| classify_io_error(e, path.to_path_buf(), 0))?;
    std::io::copy(&mut file, &mut hasher)
        .map_err(|e| classify_io_error(e, path.to_path_buf(), 0))?;
    Ok(*hasher.finalize().as_bytes())
}

/// Only ever called from `operations`-gated code (`CopyAction`,
/// `move_path`'s fast path) — gated to match, since `checksum` alone
/// (without `operations`) only needs `hash_file` itself, for
/// `analysis::hash`'s duplicate detection.
#[cfg(feature = "operations")]
async fn hash_file_async(path: PathBuf) -> Result<[u8; 32]> {
    tokio::task::spawn_blocking(move || hash_file(&path))
        .await
        .expect("hash task panicked")
}

/// Whether `a` and `b` have identical content, cheaply ruling out most
/// non-matches by size alone (a byte-for-byte difference always implies
/// a size difference, or the reverse: same size is a prerequisite for
/// being identical, so there's no reason to read either file when the
/// sizes already disagree) before hashing either one.
///
/// `a_size`/`b_size` are taken as parameters rather than stat'd here
/// because both callers already have them on hand (the Profiler's
/// `Entry.size` for the source, a `metadata()` call the caller already
/// made for the destination) — an extra stat here would just repeat one
/// the caller already paid for.
#[cfg(feature = "operations")]
pub(crate) async fn files_identical(a: &Path, b: &Path, a_size: u64, b_size: u64) -> Result<bool> {
    if a_size != b_size {
        return Ok(false);
    }
    let (hash_a, hash_b) = tokio::try_join!(
        hash_file_async(a.to_path_buf()),
        hash_file_async(b.to_path_buf())
    )?;
    Ok(hash_a == hash_b)
}
