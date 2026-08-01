# Quickstart

Every operation returns a `Handle<T>` (or `WatchHandle` for `watch`) from
`.start()` — a future you can `.await` for the final outcome, with a
`.progress()` stream you can read from concurrently. See
[progress-and-cancellation.md](progress-and-cancellation.md) for the full
contract.

The snippets below are trimmed for readability. Runnable, CLI-driven
versions of each — real progress output, full outcome summaries, proper
argument handling — live in [`examples/`](../../examples/):
`basic_copy.rs`, `basic_move.rs`, `basic_sync.rs`, `basic_watch.rs`,
`basic_compress.rs`. Try one directly:

```sh
cargo run --example basic_copy --features operations -- <source> <dest>
```

## Copy

```rust
use file_engine::FileEngine;

let engine = FileEngine::new();
let outcome = engine.copy("src/", "dst/").overwrite(true).start()?.await?;
```

## Move

```rust
let outcome = engine.move_path("src/", "dst/").start()?.await?;
```

Tries an atomic rename first; falls back to copy-then-delete
automatically if `src`/`dst` are on different filesystems.

## Sync

Requires the `sync` feature.

```rust
let sync_outcome = engine.sync("src/", "dst/").start()?.await?;
// sync_outcome.copy   — entries copied because they were new/changed
// sync_outcome.delete — dest-only entries removed (orphans)
```

## Watch

Requires the `watch` feature. Doesn't require `operations`.

```rust
use tokio_stream::StreamExt;

let mut handle = engine.watch("src/").recursive(true).start()?;
while let Some(event) = handle.events().next().await {
    println!("{:?}: {:?}", event.kind, event.paths);
}
```

## Compress

Requires the `compress` feature.

```rust
// Format inferred from the destination extension (.zip / .gz):
let outcome = engine.compress("src/", "out.zip").start()?.await?;

// Or set it explicitly:
use file_engine::CompressFormat;
let outcome = engine.compress("file.txt", "out.gz")
    .format(CompressFormat::Gzip)
    .start()?
    .await?;
```

`Gzip` requires a single file, not a directory — use `Zip` to compress a
directory.

## Reading progress while an operation runs

```rust
use tokio_stream::StreamExt;
use file_engine::Progress;

let mut handle = engine.copy("src/", "dst/").start()?;
while let Some(event) = handle.progress().next().await {
    match event {
        Progress::Started { entries_total, .. } => println!("copying {entries_total} entries"),
        Progress::EntryCompleted { entry } => println!("done: {}", entry.relative_path.display()),
        Progress::EntryFailed { entry } => eprintln!("failed: {}", entry.relative_path.display()),
        _ => {}
    }
}
let outcome = handle.await?;
```
