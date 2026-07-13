# file-engine

Async, cross-platform file operations engine for desktop apps and developer
tools: copy, move, analyze, watch, compress, and sync files, with progress
reporting and cooperative cancellation built in from the start.

Not tied to any specific application — a standalone crate consumable by any
Rust project (desktop apps, CLIs, Tauri backends, etc.), built on `tokio`.

## Status

Early scaffolding — API surface is not yet stable or fully implemented.

## Usage

```rust
use file_engine::FileEngine;

#[tokio::main]
async fn main() -> file_engine::Result<()> {
    let engine = FileEngine::new();

    let mut handle = engine.copy("src.txt", "dst.txt").overwrite(true).start()?;

    while let Some(progress) = tokio_stream::StreamExt::next(handle.progress()).await {
        println!("{:?}", progress);
    }

    handle.await?;
    Ok(())
}
```

Every operation follows the same builder pattern: a chainable builder
configures the operation, `.start()` spawns it as a background task and
returns a handle, and the handle exposes a `Progress` stream plus
cooperative cancellation via `.cancel()`.

## Features

Only pay for what you use — the public surface grows and shrinks via Cargo
feature flags.

| Feature       | Enables                                        |
| ------------- | ----------------------------------------------- |
| `operations`  | `copy`, `move_path` (default)                    |
| `analyze`     | `analyze` (default)                              |
| `checksum`    | hashing on top of `analyze`                      |
| `watch`       | filesystem event watching                        |
| `compress`    | `compress` / `decompress`                        |
| `permissions` | permission preservation on top of `operations` — Unix only |
| `sync`        | one-directional mirror sync                       |
| `diagnostics` | `error-engine` integration for message catalogs   |

## License

Licensed under the [MIT license](LICENSE).
