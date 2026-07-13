mod copy;
mod move_op;

pub use copy::CopyBuilder;
pub use move_op::MoveBuilder;

use std::path::PathBuf;

use crate::engine::FileEngine;

#[cfg(feature = "operations")]
impl FileEngine {
    pub fn copy(&self, src: impl Into<PathBuf>, dst: impl Into<PathBuf>) -> CopyBuilder {
        CopyBuilder::new(src.into(), dst.into())
    }

    pub fn move_path(&self, src: impl Into<PathBuf>, dst: impl Into<PathBuf>) -> MoveBuilder {
        MoveBuilder::new(src.into(), dst.into())
    }
}
