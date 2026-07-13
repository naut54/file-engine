# FileEngine — Design Document

**Status:** Design settled. Section 9's original open questions have all
been resolved (each entry is struck through with a pointer to where the
resolution lives); it's kept as a record of what was decided and why,
not a live TODO list.

**Audience:** this document is written for an implementer (human or Claude
Code) picking up the project from scratch. It captures *decisions*, not just
options — where multiple approaches were discussed, only the chosen one is
recorded here.

---

## 1. Overview

`file-engine` is a standalone, open-source Rust crate providing async,
cross-platform file operations for desktop applications and developer
tooling: copy, move, analyze, watch, compress, and sync files, with progress
reporting and cooperative cancellation built in from the start.

It is **not** tied to any specific application — it ships as an independent
crate on crates.io, consumable by any Rust project (desktop apps, CLIs,
Tauri backends, etc.).

## 2. Goals & Non-Goals

**Goals**
- Safe, async-first file I/O primitives (built on `tokio`).
- A single public entry point (`FileEngine`) whose surface grows/shrinks via
  Cargo feature flags, so consumers only pay for what they use.
- Progress reporting and cancellation as first-class, not bolted on.
- Cross-platform (Linux, macOS, Windows) from day one.

**Non-Goals (v0.1)**
- Not a GUI file manager or a sync service — it is a library of primitives.
- No network/cloud storage backends (local filesystem only).
- No built-in retry/backoff policies — callers compose that themselves.

## 3. Project Structure

Follows standard Rust library conventions:

```
file-engine/
├── Cargo.toml
├── Cargo.lock              # .gitignore'd (this is a library)
├── .cargo/
│   └── config.toml         # rustflags = ["-D", "warnings"]
├── errors.toml              # embedded catalog for the optional "diagnostics" feature
├── LICENSE                  # MIT (§9.3)
├── README.md
├── src/
│   ├── lib.rs               # public API surface, re-exports, feature-gated modules
│   ├── engine.rs            # `FileEngine` struct + impl blocks split by feature
│   ├── error.rs             # `FileEngineError` (thiserror) + optional EngineDiagnostic impl
│   ├── progress.rs          # `Progress` struct
│   ├── handle.rs             # generic operation `Handle<T>` (Future + progress stream)
│   ├── operations/           # feature = "operations"
│   │   ├── mod.rs
│   │   ├── copy.rs           # CopyBuilder
│   │   └── move_op.rs         # MoveBuilder
│   ├── analyze.rs            # feature = "analyze" — AnalyzeBuilder, FileInfo, FileKind
│   ├── watch.rs               # feature = "watch" — WatchBuilder (see §9, open design)
│   ├── compress.rs            # feature = "compress" — CompressBuilder / DecompressBuilder
│   ├── sync.rs                 # feature = "sync" — SyncBuilder
│   └── permissions.rs          # feature = "permissions" — extends operations builders
├── tests/
│   └── integration/
└── examples/
    ├── basic_copy.rs
    └── with_diagnostics.rs     # requires --features diagnostics
```

`checksum` and `permissions` do **not** get their own top-level module with a
builder; they extend existing builders (`AnalyzeBuilder`, `CopyBuilder`,
`MoveBuilder`) via feature-gated `impl` blocks. See §8.3.

## 4. Cargo.toml — Feature Flags

```toml
[package]
name = "file-engine"
version = "0.1.0"
edition = "2021"
rust-version = "1.88"
license = "MIT"
description = "Async, cross-platform file operations engine for desktop apps and developer tools: copy, move, analyze, watch, compress, and sync files with progress reporting and cancellation."
repository = "https://github.com/<author>/file-engine"
readme = "README.md"
keywords = ["filesystem", "io", "async", "tokio", "copy"]
categories = ["filesystem", "asynchronous"]

[features]
default     = ["operations", "analyze"]

operations  = []
analyze     = ["dep:walkdir", "dep:infer"]
checksum    = ["analyze", "dep:blake3"]
watch       = ["dep:notify"]
compress    = ["dep:zip", "dep:flate2", "dep:walkdir"]
permissions = ["operations", "dep:nix"]
sync        = ["operations", "analyze"]
diagnostics = ["dep:error-engine"]

[dependencies]
tokio = { version = "1", features = ["fs", "rt", "rt-multi-thread", "macros", "sync", "io-util"] }
tokio-stream = "0.1"
tokio-util = { version = "0.7", features = ["rt"] }
thiserror = "1"

walkdir      = { version = "2",   optional = true }
infer        = { version = "0.16", optional = true }
blake3       = { version = "1",   optional = true }
notify       = { version = "6",   optional = true }
zip          = { version = "2",   optional = true }
flate2       = { version = "1",   optional = true }
error-engine = { version = "0.2", optional = true }

# `nix` is Unix-only (`#![cfg(unix)]` internally) — see §9.6. Declared as a
# target-specific dependency, not in `[dependencies]`, so enabling
# `permissions` on a non-Unix target doesn't try to pull in a crate that
# can't build there.
[target.'cfg(unix)'.dependencies]
nix = { version = "0.29", optional = true }

[dev-dependencies]
tokio = { version = "1", features = ["full"] }
tempfile = "3"
```

**Feature dependency tree (Cargo-enforced, not just convention):**

```
operations ◄── permissions   (permissions is also Unix-only, §9.6)
operations ◄── sync
analyze    ◄── checksum
analyze    ◄── sync
watch        (independent)
compress      (independent — but shares the `walkdir` *crate* with `analyze`
               for directory-tree archiving; that's a shared dependency, not
               a feature edge, so `compress` still doesn't cascade-enable
               `analyze`)
diagnostics   (independent)
```

`checksum` and `permissions` pull in their parent feature automatically —
`features = ["checksum"]` in a consumer's `Cargo.toml` cascades to enable
`analyze` as well. This means code inside `#[cfg(feature = "permissions")]`
blocks can assume `operations` types are available without an additional
runtime or compile-time check.

`thiserror`, `tokio`/`tokio-stream`, and `tokio-util` are **not** optional —
every feature combination needs `FileEngineError` and the async runtime, so
gating them would add complexity for no benefit. `tokio-util` specifically
backs `Handle<T>`'s `CancellationToken` (§7.1), and `Handle<T>` is
unconditionally compiled (§3) — an earlier draft gated `tokio-util` behind
the `operations` feature, which broke any build enabling `compress`,
`sync`, `watch`, or `diagnostics` without `operations` (`E0433`, no
`tokio_util` crate in scope). Keeping it a plain dependency avoids that
class of feature-combination bug entirely.

## 5. Error Handling

### 5.1 Base error type (always present)

`FileEngineError` is a plain `thiserror`-derived enum. It exists and is
fully usable regardless of which features are enabled — no feature gate on
the type itself.

```rust
use thiserror::Error;
use std::path::PathBuf;

#[derive(Debug, Error)]
pub enum FileEngineError {
    #[error("source not found: {0:?}")]
    SourceNotFound(PathBuf),

    #[error("destination already exists: {0:?}")]
    DestinationExists(PathBuf),

    #[error("operation cancelled")]
    Cancelled,

    #[error("insufficient disk space: needed {needed} bytes, available {available} bytes")]
    InsufficientSpace { needed: u64, available: u64 },

    #[error("permission denied: {0:?}")]
    PermissionDenied(PathBuf),

    #[error("io error on {path:?}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("could not infer compression format from destination: {0:?}")]
    UnknownCompressFormat(PathBuf),

    #[error("gzip compression requires a single file, got a directory: {0:?}")]
    GzipRequiresFile(PathBuf),
}

pub type Result<T> = std::result::Result<T, FileEngineError>;
```

`UnknownCompressFormat` and `GzipRequiresFile` back the `compress`/`sync`
design in §8.4 — added here unconditionally rather than behind
`#[cfg(feature = "compress")]`, following the same "single enum, no
per-feature error enums" rule the `watch`-specific variant example already
implied. A build without `compress` enabled simply never constructs them.

Add variants as needed per feature (e.g. a `watch`-specific variant for a
failed OS-level watch registration), but keep them in this single enum —
do not create per-feature error enums.

### 5.2 Optional `diagnostics` feature — `error-engine` integration

`error-engine` (crates.io, currently v0.2.0) is a message-catalog +
presentation layer that sits on top of `thiserror`/`tracing`. It is **not**
a replacement for `thiserror` — `FileEngineError` above is unchanged whether
or not this feature is active.

When `diagnostics` is enabled:

```rust
#[cfg(feature = "diagnostics")]
use error_engine::{EngineDiagnostic, Severity};

#[cfg(feature = "diagnostics")]
impl EngineDiagnostic for FileEngineError {
    fn code(&self) -> &'static str {
        match self {
            Self::SourceNotFound(_) => "FE_SOURCE_NOT_FOUND",
            Self::DestinationExists(_) => "FE_DEST_EXISTS",
            Self::Cancelled => "FE_CANCELLED",
            Self::InsufficientSpace { .. } => "FE_NO_SPACE",
            Self::PermissionDenied(_) => "FE_PERMISSION_DENIED",
            Self::Io { .. } => "FE_IO",
            Self::UnknownCompressFormat(_) => "FE_UNKNOWN_COMPRESS_FORMAT",
            Self::GzipRequiresFile(_) => "FE_GZIP_REQUIRES_FILE",
        }
    }

    fn severity(&self) -> Severity {
        Severity::Error
    }

    fn context(&self) -> Vec<(&'static str, String)> {
        match self {
            Self::SourceNotFound(p) => vec![("path", p.display().to_string())],
            Self::DestinationExists(p) => vec![("path", p.display().to_string())],
            Self::InsufficientSpace { needed, available } => vec![
                ("needed", needed.to_string()),
                ("available", available.to_string()),
            ],
            Self::PermissionDenied(p) => vec![("path", p.display().to_string())],
            Self::Io { path, .. } => vec![("path", path.display().to_string())],
            Self::Cancelled => vec![],
            Self::UnknownCompressFormat(p) => vec![("path", p.display().to_string())],
            Self::GzipRequiresFile(p) => vec![("path", p.display().to_string())],
        }
    }
}

/// file-engine's own diagnostic catalog, embedded at compile time.
/// Consuming apps merge it into their own catalog:
///
/// ```ignore
/// let catalog = error_engine::Catalog::load_or_fallback("errors.toml")
///     .merged_with(file_engine::catalog());
/// ```
#[cfg(feature = "diagnostics")]
pub fn catalog() -> error_engine::Catalog {
    error_engine::Catalog::from_str(include_str!("../errors.toml"))
        .expect("file-engine's own catalog is valid TOML — covered by tests")
}
```

`errors.toml` at the crate root ships one entry per code above (`template`
+ optional `hint`), and is covered by a test that loads it via
`Catalog::from_str` and asserts it parses and that every `FE_*` code used in
`error.rs` has a matching entry (a simple string-matching test is enough —
no need for a proc macro).

**Why this shape:** `error-engine`'s own non-goals state there is no
automatic discovery of a dependency's catalog — merging is explicit via
`.merged_with(...)`. That's why `file-engine` exposes a plain `catalog()`
function rather than trying to auto-register anything.

## 6. Progress

```rust
#[derive(Debug, Clone)]
pub struct Progress {
    pub bytes_done: u64,
    pub bytes_total: u64,
    pub files_done: u64,
    pub files_total: u64,
    pub current_file: Option<PathBuf>,
}
```

Progress is delivered as a `Stream<Item = Progress>` (via
`tokio_stream::wrappers::UnboundedReceiverStream`), not a callback. This is
the idiomatic async-Rust choice and composes with `StreamExt` combinators
(`.throttle()`, etc.) for consumers who don't want to be flooded with
updates.

## 7. Builder + Handle Pattern

Every one-shot operation (`copy`, `move_path`, `analyze`, `compress`,
`sync`) follows the same shape: a chainable builder that configures the
operation, and a `.start()` that spawns it as a background `tokio` task and
returns a `Handle`. `watch` is a continuous stream of events with no single
final result, so it does *not* use `Handle<T>` — see §7.3.

```rust
let handle = engine
    .copy(src, dst)
    .overwrite(true)
    .cancellation_token(token)   // optional — engine generates one if omitted
    .start()?;

let mut progress = handle.progress(); // Stream<Item = Progress>
while let Some(p) = progress.next().await {
    // ...
}

let result: Result<()> = handle.await; // Handle implements Future
```

### 7.1 `Handle<T>`

A generic type in `handle.rs`, reused by every builder (`T` is the
operation's final output — `()` for copy/move, `Vec<FileInfo>` for analyze,
etc.):

```rust
pub struct Handle<T> {
    join: tokio::task::JoinHandle<Result<T>>,
    progress_rx: tokio_stream::wrappers::UnboundedReceiverStream<Progress>,
    cancel_token: tokio_util::sync::CancellationToken,
}

impl<T> Handle<T> {
    pub fn progress(&mut self) -> &mut UnboundedReceiverStream<Progress> { ... }
    pub fn cancel(&self) { self.cancel_token.cancel(); }
}

impl<T> Future for Handle<T> {
    type Output = Result<T>;
    // delegates to `self.join`
}
```

### 7.2 Builders

Each `*Builder` holds the parameters for its operation plus an optional
`CancellationToken`, and exposes `.start(self) -> Result<Handle<T>>`
(synchronous — spawning the task itself doesn't fail; errors surface
through the `Handle`). Builder-specific chainable methods (`.overwrite`,
`.recursive`, `.with_hash`, `.preserve_permissions`, etc.) are described per
feature in §8.

### 7.3 `watch` — `WatchBuilder` / `WatchHandle` (resolves §9.1)

`watch` follows the same *builder* half of the pattern but does not return a
`Handle<T>` — there is no single final `T` to resolve to, only an ongoing
stream of filesystem events. Instead it has its own sibling handle type,
`WatchHandle`, deliberately not a generalization of `Handle<T>` (forcing a
continuous stream into a type built around "one background task, one final
result" would leak awkwardness into every other builder).

```rust
let mut watch_handle = engine
    .watch(path)
    .recursive(true)              // default: true, matches AnalyzeBuilder
    .cancellation_token(token)    // optional, same as every other builder
    .start()?;

let mut events = watch_handle.events(); // Stream<Item = WatchEvent>
while let Some(event) = events.next().await {
    // ...
}

// Optional: detect the watch dying (cancelled, or an unrecoverable
// OS-level watch error). Not required for the common case — `events()`
// is the primary interface, unlike `Handle<T>` where `.await` is the point.
let result: Result<()> = watch_handle.await;
```

`WatchEvent` abstracts over `notify`'s event types so `notify` does not leak
through the public API:

```rust
pub enum WatchEventKind {
    Created,
    Modified,
    Removed,
    Other,
}

pub struct WatchEvent {
    pub kind: WatchEventKind,
    pub paths: Vec<PathBuf>,
}
```

`WatchHandle` mirrors `Handle<T>`'s shape and naming where the concepts
line up (`.cancel()`, a `Future` impl for optional awaiting) but swaps
`.progress()` for `.events()`:

```rust
pub struct WatchHandle {
    join: tokio::task::JoinHandle<Result<()>>,
    events_rx: tokio_stream::wrappers::UnboundedReceiverStream<WatchEvent>,
    cancel_token: tokio_util::sync::CancellationToken,
}

impl WatchHandle {
    pub fn events(&mut self) -> &mut UnboundedReceiverStream<WatchEvent> { ... }
    pub fn cancel(&self) { self.cancel_token.cancel(); }
}

impl Future for WatchHandle {
    type Output = Result<()>;
    // Resolves only when the watch task ends: via cancel() or an
    // unrecoverable OS-level watch error. Delegates to `self.join` the
    // same way `Handle<T>` does.
}
```

Internally, `WatchBuilder::start()` spawns a task that owns a
`notify::RecommendedWatcher`, maps raw `notify` events to `WatchEvent`, and
forwards them into the channel backing `events_rx` until cancelled or the
watcher errors — the same "spawn + channel + `CancellationToken`" plumbing
`Handle<T>`-based builders use, just without a meaningful final `T`.

## 8. Public API Surface — `FileEngine`

### 8.1 Base struct (always present)

```rust
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
        Self { buffer_size: 1024 * 1024, follow_symlinks: false }
    }
}

impl FileEngine {
    pub fn new() -> Self {
        Self { default_options: EngineOptions::default() }
    }

    pub fn with_options(options: EngineOptions) -> Self {
        Self { default_options: options }
    }

    pub fn options(&self) -> &EngineOptions {
        &self.default_options
    }
}
```

Keep this struct holding only shared config. Do not add feature-specific
state here (e.g. no `notify` watcher handles) — those live inside the
feature's own builder/handle types.

### 8.2 Feature-gated `impl` blocks

One `impl FileEngine` block per feature, each in its own module and gated
with `#[cfg(feature = "...")]` at the `impl` level:

```rust
#[cfg(feature = "operations")]
impl FileEngine {
    pub fn copy(&self, src: impl Into<PathBuf>, dst: impl Into<PathBuf>) -> CopyBuilder { ... }
    pub fn move_path(&self, src: impl Into<PathBuf>, dst: impl Into<PathBuf>) -> MoveBuilder { ... }
}

#[cfg(feature = "analyze")]
impl FileEngine {
    pub fn analyze(&self, path: impl Into<PathBuf>) -> AnalyzeBuilder { ... }
}

#[cfg(feature = "watch")]
impl FileEngine {
    pub fn watch(&self, path: impl Into<PathBuf>) -> WatchBuilder { ... }
}

#[cfg(feature = "compress")]
impl FileEngine {
    pub fn compress(&self, src: impl Into<PathBuf>, dst: impl Into<PathBuf>) -> CompressBuilder { ... }
    pub fn decompress(&self, src: impl Into<PathBuf>, dst: impl Into<PathBuf>) -> DecompressBuilder { ... }
}

#[cfg(feature = "sync")]
impl FileEngine {
    pub fn sync(&self, src: impl Into<PathBuf>, dst: impl Into<PathBuf>) -> SyncBuilder { ... }
}
```

The corresponding module (`mod operations;`, `mod analyze;`, etc.) in
`lib.rs` must also be feature-gated, so the code isn't even parsed when the
feature is off:

```rust
#[cfg(feature = "operations")] mod operations;
#[cfg(feature = "analyze")]    mod analyze;
#[cfg(feature = "watch")]      mod watch;
#[cfg(feature = "compress")]   mod compress;
#[cfg(feature = "sync")]       mod sync;
// `permissions` is also gated on `unix`, not just the feature — see §9.6.
#[cfg(all(feature = "permissions", unix))] mod permissions;
```

### 8.3 `checksum` and `permissions` — enhancers, not their own surface

These two features do not add a method to `FileEngine`. Instead they add
methods to *existing* builders, conditionally:

```rust
#[cfg(feature = "checksum")]
impl AnalyzeBuilder {
    pub fn with_hash(mut self, enabled: bool) -> Self { ... }
}

#[cfg(feature = "permissions")]
impl CopyBuilder {
    pub fn preserve_permissions(mut self, enabled: bool) -> Self { ... }
}

#[cfg(feature = "permissions")]
impl MoveBuilder {
    pub fn preserve_permissions(mut self, enabled: bool) -> Self { ... }
}
```

Because `checksum` requires `analyze` and `permissions` requires
`operations` at the manifest level (§4), these `impl` blocks can rely on
`AnalyzeBuilder`/`CopyBuilder`/`MoveBuilder` always being compiled when they
themselves are compiled.

### 8.4 `CompressBuilder` / `DecompressBuilder` internals (resolves §9.2, compress half)

Two distinct formats, not one generic "compress" — driven by the
dependencies already locked into §4 (`zip`, `flate2`; deliberately no
`tar`):

```rust
pub enum CompressFormat {
    Zip,
    Gzip,
}
```

- **`Zip`** (via the `zip` crate): multi-entry archive. Handles a single
  file or a whole directory tree, preserving structure. The general case.
- **`Gzip`** (via `flate2`): a single compressed stream. Because there is no
  `tar` dependency, there is no way to fold a directory into one `.gz` the
  way `tar.gz` would — `Gzip` only accepts a single file as source.

```rust
#[cfg(feature = "compress")]
impl FileEngine {
    pub fn compress(&self, src: impl Into<PathBuf>, dst: impl Into<PathBuf>) -> CompressBuilder { ... }
    pub fn decompress(&self, src: impl Into<PathBuf>, dst: impl Into<PathBuf>) -> DecompressBuilder { ... }
}

impl CompressBuilder {
    /// Optional — inferred from `dst`'s extension when omitted
    /// (`.zip` → `Zip`, `.gz` → `Gzip`).
    pub fn format(mut self, format: CompressFormat) -> Self { ... }
    pub fn cancellation_token(mut self, token: CancellationToken) -> Self { ... }
    pub fn start(self) -> Result<Handle<()>> { ... }
}

impl DecompressBuilder {
    /// Optional — inferred from `src`'s extension when omitted.
    pub fn format(mut self, format: CompressFormat) -> Self { ... }
    pub fn cancellation_token(mut self, token: CancellationToken) -> Self { ... }
    pub fn start(self) -> Result<Handle<()>> { ... }
}
```

Both builders follow §7.2 exactly (`.start()` is synchronous and cannot
fail; format-inference failure and the `Gzip`-on-a-directory case surface
through the `Handle` as `FileEngineError::UnknownCompressFormat` /
`FileEngineError::GzipRequiresFile`, §5.1) — not as a panic or a
synchronous `Result` from `.format()`/`.start()` itself, so error handling
stays uniform with every other builder in the crate.

### 8.5 `SyncBuilder` internals (resolves §9.2, sync half)

`sync` is a **one-directional mirror**: it makes `dst` look like `src`, not
a bidirectional reconciliation. That's the simplest interpretation that
still needs a real design, and matches what `sync` conventionally means in
tools like `rsync`.

```rust
pub struct SyncSummary {
    pub copied: u64,
    pub updated: u64,
    pub deleted: u64,
    pub skipped: u64,
}

#[cfg(feature = "sync")]
impl FileEngine {
    pub fn sync(&self, src: impl Into<PathBuf>, dst: impl Into<PathBuf>) -> SyncBuilder { ... }
}

impl SyncBuilder {
    /// Default: false. When true, files present in `dst` but not in `src`
    /// are removed — full mirror rather than additive-only sync.
    pub fn delete_extraneous(mut self, enabled: bool) -> Self { ... }
    pub fn cancellation_token(mut self, token: CancellationToken) -> Self { ... }
    pub fn start(self) -> Result<Handle<SyncSummary>> { ... }
}
```

Unlike `copy`/`move` (`Handle<()>`), `sync` resolves to `SyncSummary` — a
meaningful final `T`, the same way `analyze` resolves to `Vec<FileInfo>`
rather than `()`.

Change detection defaults to size + mtime comparison (cheap, adds no
dependency — `sync` already requires `analyze` at the manifest level, §4,
so it can reuse that walk). When `checksum` is *also* enabled — a legal
combination, since both `sync` and `checksum` independently depend on
`analyze` rather than on each other — an enhancer impl adds hash-based
comparison, following the same pattern as `AnalyzeBuilder::with_hash` in
§8.3:

```rust
#[cfg(all(feature = "sync", feature = "checksum"))]
impl SyncBuilder {
    /// Default: false (size + mtime). When true, compares file content
    /// hashes instead — more expensive, immune to mtime-only changes.
    pub fn compare_by_hash(mut self, enabled: bool) -> Self { ... }
}
```

## 9. Open Design Questions (not yet settled — resolve before/while implementing)

These were flagged during design but not closed. Do not silently invent an
answer that contradicts the patterns above — if unsure, follow the closest
existing pattern and leave a `// TODO(design):` comment rather than
guessing silently.

1. ~~**`WatchBuilder` shape.**~~ **Resolved** — see §7.3. `.start()` returns
   `WatchHandle` (not `Handle<T>`), whose `.events()` returns
   `Stream<Item = WatchEvent>`; its `Future` impl resolves only when the
   watch task ends (cancellation or an unrecoverable watch error).

2. ~~**`CompressBuilder` / `DecompressBuilder` and `SyncBuilder` internals.**~~
   **Resolved** — see §8.4 (compress: `Zip` via the `zip` crate for
   files/directories, `Gzip` via `flate2` for single files only, format
   inferred from the path extension unless set explicitly) and §8.5 (sync:
   one-directional mirror to `Handle<SyncSummary>`, size+mtime comparison by
   default with a `checksum`-gated `.compare_by_hash()` enhancer).

3. ~~**License.**~~ **Resolved** — MIT only (not the dual `MIT OR
   Apache-2.0` this document originally assumed as the ecosystem default).
   `license = "MIT"` in `Cargo.toml` (§4), single `LICENSE` file at the
   crate root (§3).

4. ~~**MSRV.**~~ **Resolved** — `rust-version = "1.88"` (§4). Not an
   editorial "last N releases" pick — it's the actual floor of the locked
   dependency graph as of this writing (`cargo tree --all-features`
   resolved against `Cargo.lock`).

   The binding constraint is transitive, not any direct dependency: `time`
   v0.3.53 declares `rust-version = "1.88.0"` and is pulled in because
   `zip`'s `default` feature set includes `time` (archive timestamp
   support). Direct dependencies are all lower (`tokio`/`tokio-stream`/
   `tokio-util` 1.71, `zip` itself 1.73.0, `flate2` 1.67.0, `nix` 1.69,
   `notify` 1.60, `thiserror` 1.61; `walkdir`/`infer`/`blake3`/
   `error-engine` declare none). The next-highest tier sits at 1.85
   (`hashbrown`/`indexmap`/`uuid`/`zeroize`/`getrandom`, via
   `infer → cfb → uuid` and `error-engine → toml → indexmap`) — dropping
   `zip`'s `time` feature to shed the 1.88 requirement would only buy back
   1.88 → 1.85, not lower, and costs zip archive timestamp support, so it
   isn't worth it.

   Caveat: every dependency in §4 is version-ranged (`"1"`, `"2"`, etc.),
   so a future `cargo update` can silently raise a transitive dep's own
   `rust-version` past 1.88 without any change to this crate's
   `Cargo.toml`. An MSRV check in CI (item 5, below) is what catches that
   drift — `rust-version` here is a floor as of *today's* lockfile, not a
   guarantee that stays true after every future `cargo update`.

5. ~~**CI.**~~ **Resolved** — see §11 for the full workflow
   (`.github/workflows/ci.yml`): `fmt`, `clippy` + `test` across a 3-OS
   matrix, `cargo hack --feature-powerset --depth 2` for feature-cfg
   coverage, and a pinned MSRV job. Surfaced item 6 (`permissions` /
   Windows) as a prerequisite while designing this.

6. ~~**`permissions` feature and Windows.**~~ **Resolved** — `permissions`
   is Unix-only. §2 states cross-platform Linux/macOS/Windows as a goal,
   but `permissions`'s only dependency, `nix`, declares `#![cfg(unix)]` at
   its own crate root — it cannot build for Windows at all. Rather than let
   that surface as a confusing failure the moment `permissions.rs` grows a
   real implementation, it's now an explicit, structural exclusion:
   - `nix` moved from `[dependencies]` to `[target.'cfg(unix)'.dependencies]`
     (§4) — enabling `permissions` on a non-Unix target no longer tries to
     pull in a crate that can't build there.
   - `mod permissions;` in `lib.rs` is gated on `#[cfg(all(feature =
     "permissions", unix))]`, not just the feature (§8.2) — so on Windows,
     `.preserve_permissions()` simply doesn't exist on `CopyBuilder`/
     `MoveBuilder` at all. A consumer who calls it on Windows gets a clear
     "method not found" compile error, not a silent no-op that quietly
     fails to preserve permissions.
   - Verified by cross-checking against the `x86_64-pc-windows-gnu` target:
     `--features permissions` compiles (the feature request is inert off
     of Unix — Cargo doesn't hard-error, it just never activates `nix`),
     while the `permissions` module and its API are absent, confirmed by
     inspecting the compiled output. `--all-features` is therefore
     Unix-only in practice; Windows CI (§11) uses an explicit feature list
     that excludes `permissions` instead.

## 10. Implementation Conventions

Follow the author's general Rust conventions:

- **Errors:** `thiserror` for this library's typed errors (§5); never
  `anyhow` inside the library itself (`anyhow` is for application code, not
  crates like this one).
- **Async:** `tokio` with the features actually needed (not blanket
  `"full"` in the published crate's own `[dependencies]` — reserve `"full"`
  for `[dev-dependencies]`/examples).
- **Logging:** `tracing`, structured (`tracing::instrument`, key-value
  fields), never `println!` for observability. Note: this is orthogonal to
  `error-engine`'s `Engine::log()` — internal `tracing` calls for debugging
  the crate's own operations are independent of whatever diagnostic
  presentation a consumer layers on top via `diagnostics`.
- **Formatting/linting:** default `rustfmt`; `clippy` clean, no blanket
  `#[allow(...)]`; `.cargo/config.toml` with `rustflags = ["-D", "warnings"]`.
- **Tests:** unit tests colocated (`#[cfg(test)]`) per module; integration
  tests in `tests/integration/` using `tempfile` for filesystem fixtures;
  `#[tokio::test]` for async tests.
- **`main.rs`:** none — this is a library crate (`src/lib.rs` only, plus
  `examples/`).

## 11. CI (resolves §9.5)

`.github/workflows/ci.yml`, five jobs, all verified to actually pass
against the state of the crate as of this writing (not just written and
assumed correct):

- **`fmt`** — `cargo fmt --all -- --check`. Ubuntu only; formatting isn't
  platform-dependent.
- **`clippy`** — 3-OS matrix (Ubuntu/macOS/Windows). `--all-features
  --all-targets -- -D warnings` on Unix; on Windows, an explicit feature
  list (`operations,analyze,checksum,watch,compress,sync,diagnostics`)
  that excludes `permissions` (§9.6) — `--all-features` isn't meaningful
  on Windows since one of the features can't exist there.
- **`test`** — same 3-OS matrix, same Windows feature-list exception. Runs
  `cargo test` twice per OS: once with default features, once with
  "all features valid for this OS". This is the job that actually catches
  platform-specific runtime bugs, not just compile-time `#[cfg]` mistakes.
- **`feature-matrix`** — `cargo hack check --feature-powerset --depth 2
  --no-dev-deps`, Ubuntu only. Every single feature and every *pair* of
  features (36 combinations for 8 features, verified locally: all 40
  invocations `cargo-hack` actually runs, including the `default`
  pseudo-feature, passed). This directly satisfies §9's original ask for
  "a curated subset of combinations covering each dependency edge" —
  full pairwise coverage is a stronger, more systematic version of that
  rather than a hand-picked list, for a crate this size still fast enough
  to run on every push. Deliberately Ubuntu-only (Unix) — this job is a
  feature-`#[cfg]` correctness sweep, not a cross-platform check (`test`
  already does that), so it can safely include `permissions`.
- **`msrv`** — pinned to `rust-version` from `Cargo.toml` (currently 1.88,
  §9.4) via `dtolnay/rust-toolchain@1.88`, `cargo check --all-features`.
  Exists specifically to catch the drift scenario described in §9.4's
  resolution: a `cargo update` silently raising a transitive dependency's
  own MSRV past what this crate declares.

Not covered by this workflow, deliberately deferred: publishing/release
automation (`cargo publish`, changelog generation) and doc-coverage checks
— neither was asked for, and adding them now would be scope creep beyond
what §9.5 asked to resolve.
