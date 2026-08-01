# file-engine

Async, cross-platform file operations engine for desktop apps and developer
tools: copy, move, sync, watch, and compress files, with progress
reporting and cooperative cancellation built in from the start. Small-file
operations are automatically batched to avoid overloading the OS with
per-file syscalls, without any configuration required.

Not tied to any specific application — a standalone crate consumable by any
Rust project (desktop apps, CLIs, Tauri backends, etc.), built on `tokio`.

## Quickstart

```rust
use file_engine::FileEngine;

#[tokio::main]
async fn main() -> file_engine::Result<()> {
    let engine = FileEngine::new();

    let mut handle = engine.copy("src.txt", "dst.txt").overwrite(true).start()?;

    while let Some(progress) = tokio_stream::StreamExt::next(handle.progress()).await {
        println!("{:?}", progress);
    }

    let outcome = handle.await?;
    println!("succeeded: {}, failed: {}", outcome.succeeded.len(), outcome.failed.len());
    Ok(())
}
```

Every operation follows the same builder pattern: a chainable builder
configures the operation, `.start()` spawns it as a background task and
returns a handle immediately, and the handle exposes a `Progress` stream
plus cooperative cancellation via `.cancel()`.

Copying across filesystems (e.g. onto a FAT32/exFAT drive) is checked for
several failure modes up front — case-insensitive-destination collisions,
Windows-reserved filenames, destination file-size limits, and a known
exFAT-on-macOS write-integrity risk — rather than failing unpredictably
partway through or silently losing data. See
[`docs/guide/filesystem-safety.md`](docs/guide/filesystem-safety.md).

## Features

Only pay for what you use — the public surface grows and shrinks via Cargo
feature flags.

| Feature | Enables | Notes |
| --- | --- | --- |
| `operations` *(default)* | `copy`, `move_path` | Also pulls in filesystem-capability detection (used by `copy`/`move`/`sync`). |
| `sync` | `FileEngine::sync()` | Implies `operations`. |
| `checksum` | `DiffStrategy::Checksum` for `sync` | Content-hash comparison instead of size+mtime. |
| `watch` | `FileEngine::watch()` | Does **not** require `operations` — watching doesn't use the batching pipeline. |
| `compress` | `FileEngine::compress()` | Zip or gzip, inferred from the destination extension or set explicitly via `CompressFormat`. No decompress support yet. |
| `permissions` | `.preserve_permissions()` on copy/move/sync | Unix only. Mode bits, not ownership. |
| `analyze` *(default)* | — | Reserved for a future standalone inspection API; not yet implemented — enabling it currently does nothing observable. |
| `diagnostics` | — | Reserved for `error-engine` message-catalog integration; not yet implemented. |

## Documentation

- [`docs/guide/`](docs/guide/) — using the crate: quickstart per
  operation, the full builder option reference, progress/cancellation,
  error handling, and the filesystem-safety behavior above in detail.
- [`docs/contributing/`](docs/contributing/) — working on the crate:
  architecture, conventions for adding a feature, and this project's
  testing discipline.

## License

Licensed under the [MIT license](LICENSE).
