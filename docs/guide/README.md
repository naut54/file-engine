# Guide

`file-engine` is an async, cross-platform file operations engine: copy,
move, sync, watch, and compress files, with progress reporting and
cancellation built in. It automatically batches small-file operations to
avoid overloading the OS with per-file syscalls, without you having to
configure anything.

- **[Feature flags](#feature-flags)** — what's enabled by default, what
  you need to opt into
- **[Quickstart](quickstart.md)** — minimal working examples for each
  operation
- **[Operations](operations.md)** — the full builder API for
  copy/move/sync/watch/compress
- **[Progress and cancellation](progress-and-cancellation.md)** — the
  `Handle<T>` type and the `Progress` event stream
- **[Error handling](error-handling.md)** — the `Error` enum and
  `ErrorStrategy`
- **[Filesystem safety](filesystem-safety.md)** — what the crate checks
  for automatically when copying across different filesystems

## Feature flags

```toml
[dependencies]
file-engine = { version = "0.1", features = ["sync", "watch", "compress"] }
```

| Flag | Enables | Notes |
|---|---|---|
| `operations` *(default)* | `copy`, `move_path` | Pulls in the batching engine (Profiler/Planner/Dispatcher) and filesystem-capability detection. |
| `permissions` | `.preserve_permissions()` on copy/move/sync builders | Unix only. Mode bits, not ownership. |
| `sync` | `FileEngine::sync()` | Diff-then-copy-then-delete against a destination. |
| `checksum` | `DiffStrategy::Checksum` for `sync` | Content-hash comparison instead of size+mtime. |
| `watch` | `FileEngine::watch()` | Does **not** require `operations` — watching doesn't go through the batching pipeline. |
| `compress` | `FileEngine::compress()` | Zip or gzip, inferred from the destination extension unless set explicitly. |
| `analyze` *(default)* | — | Currently unused by any public API. |
| `diagnostics` | — | Not yet integrated. |

`sync` implies `operations`; `watch` does not.

## Minimal example

```rust
use file_engine::FileEngine;

#[tokio::main]
async fn main() -> file_engine::Result<()> {
    let engine = FileEngine::new();
    let handle = engine.copy("src/", "dst/").overwrite(true).start()?;
    let outcome = handle.await?;

    println!("copied {} entries, {} failed", outcome.succeeded.len(), outcome.failed.len());
    Ok(())
}
```

See [quickstart.md](quickstart.md) for one of these per operation,
including reading the progress stream.
