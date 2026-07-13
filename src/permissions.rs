use crate::operations::{CopyBuilder, MoveBuilder};

// `permissions` is an enhancer, not its own surface (§8.3) — it only adds
// methods to the existing `operations` builders. Because `permissions`
// requires `operations` at the manifest level (§4), these impls can rely on
// `CopyBuilder`/`MoveBuilder` always being compiled alongside them.

#[cfg(feature = "permissions")]
impl CopyBuilder {
    pub fn preserve_permissions(mut self, enabled: bool) -> Self {
        self.preserve_permissions = enabled;
        self
    }
}

#[cfg(feature = "permissions")]
impl MoveBuilder {
    pub fn preserve_permissions(mut self, enabled: bool) -> Self {
        self.preserve_permissions = enabled;
        self
    }
}
