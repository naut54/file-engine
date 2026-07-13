/// Base struct (always present). Holds only shared config — no
/// feature-specific state (e.g. no `notify` watcher handles). Those live
/// inside each feature's own builder/handle types.
pub struct FileEngine {
    default_options: EngineOptions,
}

#[derive(Debug, Clone)]
pub struct EngineOptions {
    pub buffer_size: usize,
    pub follow_symlinks: bool,
}

impl Default for EngineOptions {
    fn default() -> Self {
        Self {
            buffer_size: 1024 * 1024,
            follow_symlinks: false,
        }
    }
}

impl FileEngine {
    pub fn new() -> Self {
        Self {
            default_options: EngineOptions::default(),
        }
    }

    pub fn with_options(options: EngineOptions) -> Self {
        Self {
            default_options: options,
        }
    }

    pub fn options(&self) -> &EngineOptions {
        &self.default_options
    }
}

impl Default for FileEngine {
    fn default() -> Self {
        Self::new()
    }
}

// Feature-gated `impl FileEngine` blocks (§8.2) live in their own feature
// modules (operations.rs, analyze.rs, watch.rs, compress.rs, sync.rs), not
// here — this module only owns the base struct.
