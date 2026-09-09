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

## Operations at a glance

| Method | Feature | Purpose | Key options | Returns |
| --- | --- | --- | --- | --- |
| `.copy(source, dest)` | `operations` | Copy a file or directory tree | `.overwrite()`, `.skip_if_identical()`¹, `.preserve_permissions()`², `.allow_filesystem_integrity_risk()`, `.small_file_threshold()`, `.max_bytes_per_batch()`, `.max_files_per_batch()`, `.batch_sort_order()`, `.on_error()`, `.batch_concurrency()` | `Handle<OperationOutcome>` |
| `.move_path(source, dest)` | `operations` | Move a file or directory tree — atomic rename, falls back to copy-then-delete across filesystems | same as `.copy()` | `Handle<OperationOutcome>` |
| `.move_many(sources, dest_dir)` | `operations` | Move several independent sources into one destination directory as a single batched operation | same as `.copy()` | `Handle<OperationOutcome>` (adds `sources_failed`) |
| `.sync(source, dest)` | `sync` | One-way mirror: copy new/changed entries, delete destination-only orphans | `.overwrite()` (defaults `true`), `.diff_strategy()`¹, plus the shared batching options above | `Handle<SyncOutcome>` |
| `.analyze(path)` | `analyze` | Read-only tree inspection — counts, sizes, largest files, extension/age breakdowns | `.extensions()`, `.exclude_globs()`, `.min_size()` / `.max_size()`, `.modified_after()` / `.modified_before()`, `.max_depth()`, `.follow_symlinks()`, `.detect_mime_types()`, `.detect_duplicates()`¹, `.top_n_largest()`, `.on_error()` | `AnalysisHandle` → `AnalysisReport` |
| `.compress(source, dest)` | `compress` | Zip or gzip a file or directory | `.format()`, `.small_file_threshold()`, `.on_error()`, `.batch_concurrency()` | `Handle<OperationOutcome>` |
| `.remove(path)` | `remove` | Criteria-based delete (extension, size, modified-time, glob) — trashes and dry-runs by default | `.extensions()`, `.exclude()`, `.min_size()` / `.max_size()`, `.modified_after()` / `.modified_before()`, `.max_depth()`, `.follow_symlinks()`, `.dry_run()`, `.hard_delete()`, `.allow_unfiltered_delete()`, `.on_error()`, `.batch_concurrency()` | `Handle<RemoveOutcome>` |
| `.watch(path)` | `watch` | Stream filesystem change events | `.recursive()` | `WatchHandle` |

¹ requires the `checksum` feature. ² Unix only, requires the `permissions`
feature. Full option reference, including which defaults differ and why:
[`docs/guide/operations.md`](docs/guide/operations.md).

Feed that stream to an `EtaEstimator` for a predicted time remaining. It
models batched small files (cost per file), streamed large files (cost
per byte), and the directory pre-pass (cost per directory) separately,
because a single bytes-per-second figure describes none of them well.
See [`docs/guide/progress-and-cancellation.md`](docs/guide/progress-and-cancellation.md).

`FileEngine::analyze()` inspects a path without touching it — file/
directory counts, total size, largest files, and extension/age
breakdowns, narrowed with filters (extension, glob excludes, size range,
modified-time range, depth). With the `checksum` feature, it can also
group files by content hash to surface duplicates:

```rust
let report = engine.analyze("some/dir").detect_duplicates(true).start()?.await?;
println!("{} files, {} bytes wasted on duplicates", report.file_count, report.duplicate_bytes_wasted);
```

Copying across filesystems (e.g. onto a FAT32/exFAT drive) is checked for
several failure modes up front — case-insensitive-destination collisions,
Windows-reserved filenames, destination file-size limits, and a known
exFAT-on-macOS write-integrity risk — rather than failing unpredictably
partway through or silently losing data. See
[`docs/guide/filesystem-safety.md`](docs/guide/filesystem-safety.md).

`FileEngine::move_many(sources, dest_dir)` moves several independent
sources into one destination directory as a single batched operation —
one shared `ErrorStrategy`, concurrency pool, and progress stream across
all of them, rather than one `.move_path()` call per source. Each source
keeps its own basename under `dest_dir`.

With the `checksum` feature, `.skip_if_identical(true)` on `.copy()`,
`.move_path()`, and `.move_many()` compares content instead of failing
outright when the destination already exists: an identical destination
is left in place (for a move, the now-redundant source is still
removed), while a genuinely different one still fails with
`Error::DestExists` for the caller to decide what to do.

```rust
engine.move_path("src.txt", "dst.txt").skip_if_identical(true).start()?.await?;
```

`FileEngine::remove()` deletes files matching criteria (extension, size,
modified-time, exclude globs) instead of a whole path outright. It
defaults to a dry run (`.dry_run(false)` to actually remove) and to
trashing matches rather than unlinking them (`.hard_delete(true)` to
unlink permanently), and refuses to run with no criteria set at all
unless `.allow_unfiltered_delete(true)`:

```rust
let outcome = engine.remove("some/dir").extensions(["tmp", "log"]).max_size(1024 * 1024).dry_run(false).start()?.await?;
println!("removed {} files", outcome.succeeded.len());
```

## Features

Only pay for what you use — the public surface grows and shrinks via Cargo
feature flags.

| Feature | Enables | Notes |
| --- | --- | --- |
| `operations` *(default)* | `copy`, `move_path`, `move_many` | Also pulls in filesystem-capability detection (used by `copy`/`move`/`sync`). |
| `sync` | `FileEngine::sync()` | Implies `operations`. |
| `checksum` | `DiffStrategy::Checksum` for `sync`; `.detect_duplicates()` for `analyze`; `.skip_if_identical()` for `copy`/`move_path`/`move_many` | Content-hash (blake3) comparison/grouping instead of size+mtime. |
| `watch` | `FileEngine::watch()` | Does **not** require `operations` — watching doesn't use the batching pipeline. |
| `compress` | `FileEngine::compress()` | Zip or gzip, inferred from the destination extension or set explicitly via `CompressFormat`. No decompress support yet. |
| `permissions` | `.preserve_permissions()` on copy/move/sync | Unix only. Mode bits, not ownership. |
| `analyze` *(default)* | `FileEngine::analyze()` | Read-only tree inspection — counts, total size, largest files, extension/age breakdowns, with filters. `checksum` additionally enables duplicate detection. |
| `remove` | `FileEngine::remove()` | Criteria-based delete (extension, size, modified-time, exclude globs). Implies `operations`. Trashes by default; dry-runs by default. |
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
