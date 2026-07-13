// `pub(crate)` so `sync.rs` (a sibling module of `operations`, not a
// descendant) can reach `copy::copy_file`/`copy_dir`/etc. — `sync` requires
// `operations` at the manifest level (§4), so this is always available
// whenever `sync.rs` is compiled.
pub(crate) mod copy;
mod move_op;

pub use copy::CopyBuilder;
pub use move_op::MoveBuilder;

use std::path::PathBuf;

use crate::engine::FileEngine;

#[cfg(feature = "operations")]
impl FileEngine {
    pub fn copy(&self, src: impl Into<PathBuf>, dst: impl Into<PathBuf>) -> CopyBuilder {
        let options = self.options();
        CopyBuilder::new(
            src.into(),
            dst.into(),
            options.buffer_size,
            options.follow_symlinks,
        )
    }

    pub fn move_path(&self, src: impl Into<PathBuf>, dst: impl Into<PathBuf>) -> MoveBuilder {
        let options = self.options();
        MoveBuilder::new(
            src.into(),
            dst.into(),
            options.buffer_size,
            options.follow_symlinks,
        )
    }
}
