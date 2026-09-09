// `main.rs` only ever constructs a `MountedImage` inside
// `#[cfg(target_os = "macos")]` blocks — on every other platform this
// whole module (and every function in it) is legitimately unused, not a
// bug, so it's built and allowed instead of `#[cfg]`-gating the module
// declaration itself: keeping one `Option<MountedImage>` return type on
// `case_sensitive_source`/`windows_naming_destination` across all
// platforms is simpler than giving those functions a different
// signature per platform.
#![allow(dead_code)]

use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

/// A temporary disk image, created and mounted via `hdiutil`, for
/// filesystem properties this machine's default volume can't provide.
/// Used both for a case-sensitive source (`CaseCollision`) and a
/// Windows-naming-rules destination (`ReservedName`) — same mechanism,
/// different `-fs` argument.
///
/// macOS only, via `hdiutil`. Linux/Windows equivalents are a different
/// mechanism entirely (loop-mounted images with `mkfs`; NTFS's
/// per-directory case-sensitivity flag), not built here — see the
/// design doc's "Open items."
///
/// `Drop` detaches the volume and deletes the backing image, so a
/// mid-run panic or early `?` return doesn't leave a mounted volume or a
/// stray `.dmg` behind. Best-effort only: a `kill -9` of the harness
/// process itself would still leak it, same as any other `Drop`-based
/// cleanup.
pub struct MountedImage {
    mount_point: PathBuf,
    device: String,
    image_path: PathBuf,
}

impl MountedImage {
    /// Creates a small disk image under `scratch_dir` with the given
    /// `hdiutil -fs` filesystem type and mounts it (`-nobrowse`, so an
    /// ephemeral tool volume doesn't show up in Finder's sidebar).
    ///
    /// `name` is used both as the `.dmg` file's stem and as the volume
    /// label (`-volname`) — keep it to **11 characters or fewer**.
    /// Confirmed empirically, by binary search: `hdiutil create -fs
    /// "ExFAT"` fails every single time (deterministic, not flaky) for
    /// a volume label of 12+ characters, with the unhelpful error
    /// "Operación no permitida" rather than anything mentioning length.
    /// 11 characters matches the classic FAT/exFAT short volume-label
    /// limit; APFS itself has no such limit, but callers use the same
    /// short-name convention for both filesystem types here rather than
    /// having two different naming rules to remember.
    pub fn create(scratch_dir: &Path, name: &str, fs_type: &str) -> io::Result<Self> {
        assert!(
            name.len() <= 11,
            "volume name {name:?} is {} characters; hdiutil rejects exFAT volume labels over 11",
            name.len()
        );

        std::fs::create_dir_all(scratch_dir)?;
        let image_path = scratch_dir.join(format!("{name}.dmg"));

        run(Command::new("hdiutil").args([
            "create",
            "-size",
            "16m",
            "-fs",
            fs_type,
            "-volname",
            name,
        ]).arg(&image_path))?;

        let output = run(Command::new("hdiutil").args(["attach", "-nobrowse"]).arg(&image_path))?;
        let (device, mount_point) = parse_attach_output(&output)?;

        Ok(Self {
            mount_point,
            device,
            image_path,
        })
    }

    pub fn path(&self) -> &Path {
        &self.mount_point
    }
}

impl Drop for MountedImage {
    fn drop(&mut self) {
        // Best-effort: a `Drop` impl can't propagate an error, and this
        // already only runs during cleanup — a failure here means a
        // leaked mount/file, reported so it's at least visible.
        if let Err(e) = Command::new("hdiutil")
            .args(["detach", "-force"])
            .arg(&self.device)
            .status()
        {
            eprintln!("warning: failed to detach {}: {e}", self.device);
        }
        if let Err(e) = std::fs::remove_file(&self.image_path) {
            eprintln!(
                "warning: failed to remove {}: {e}",
                self.image_path.display()
            );
        }
    }
}

fn run(cmd: &mut Command) -> io::Result<Output> {
    let output = cmd.output()?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "{cmd:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    Ok(output)
}

/// `hdiutil attach`'s stdout has one line per partition/container it
/// created, each `<device>\t<content type>\t[mount point]` — only the
/// actual filesystem's line has a mount point at all. Matched by
/// structure (first field a `/dev/...` node, last field a path under
/// `/Volumes/`) rather than by column count, since the exact column
/// layout isn't documented and shouldn't be relied on.
fn parse_attach_output(output: &Output) -> io::Result<(String, PathBuf)> {
    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if let (Some(device), Some(mount)) = (fields.first(), fields.last()) {
            if device.starts_with("/dev/") && mount.contains("/Volumes/") {
                return Ok(((*device).to_string(), PathBuf::from(mount)));
            }
        }
    }
    Err(io::Error::other(format!(
        "could not find a mounted volume in hdiutil attach output:\n{stdout}"
    )))
}
