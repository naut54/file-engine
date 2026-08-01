# Operations

All builders share a common shape: `FileEngine::<operation>(...)` returns
a builder, options are chained, `.start()` kicks it off and returns a
`Handle<T>` (or `WatchHandle`) immediately — the work runs in the
background. See
[progress-and-cancellation.md](progress-and-cancellation.md) for what to
do with the handle.

## Shared options

These appear (with the same name and meaning) on every batching-pipeline
builder — `CopyBuilder`, `MoveBuilder`, `SyncBuilder` — unless noted:

| Method | Default | Meaning |
|---|---|---|
| `.overwrite(bool)` | `false` for copy/move, `true` for sync | Whether an existing file at the destination path may be replaced. Sync defaults to `true` because the diff step already decided which entries need copying — refusing to overwrite them would silently defeat the sync. |
| `.small_file_threshold(bytes)` | `262144` (256 KiB) | Files at or under this size are batched together; larger files stream individually. |
| `.batch_concurrency(n)` | number of CPU cores | How many batches/streams run concurrently. |
| `.on_error(ErrorStrategy)` | `ContinueAndCollect` | See [error-handling.md](error-handling.md). |
| `.preserve_permissions(bool)` *(`permissions` feature, Unix only)* | `false` | Preserves Unix mode bits on directories. File mode bits are preserved automatically by the underlying copy regardless of this flag. |
| `.allow_filesystem_integrity_risk(bool)` | `false` | See [filesystem-safety.md](filesystem-safety.md) — without this, copying to a destination filesystem with a known write-integrity risk (currently: exFAT on macOS) fails immediately rather than proceeding. |

`CopyBuilder` additionally has:

| Method | Default | Meaning |
|---|---|---|
| `.max_bytes_per_batch(bytes)` | 8 MiB | Hard cap on a batch's total size. |
| `.max_files_per_batch(n)` | derived from `max_bytes_per_batch / median file size` | Hard cap on a batch's file count. |
| `.batch_sort_order(SortOrder)` | `Descending` | Sort order small files are packed in before batching (`Ascending` or `Descending`). |

`SyncBuilder` additionally has:

| Method | Default | Meaning |
|---|---|---|
| `.diff_strategy(DiffStrategy)` | `SizeAndModifiedTime` | How `sync` decides a file changed. `Checksum` (requires the `checksum` feature) compares content hashes instead — more expensive, catches same-size-and-mtime content changes the default would miss. |

`WatchBuilder` has:

| Method | Default | Meaning |
|---|---|---|
| `.recursive(bool)` | `false` | Whether subdirectories are watched too. |

`CompressBuilder` has `.small_file_threshold()`, `.batch_concurrency()`,
`.on_error()` (same meaning as above) plus `.format()` — see the note in
[quickstart.md](quickstart.md#compress) about its current limitation.

## Move's fallback behavior

`move_path` attempts a single atomic rename first. If source and
destination are on different filesystems (`EXDEV`), it falls back to
copy-then-delete-source automatically, using the same batching pipeline
as `copy` — every option above applies to that fallback path too. On the
fast (same-filesystem) path, no `Progress` events are emitted at all —
there's nothing to report, the whole move is one atomic syscall.

## Sync's outcome shape

`sync()` returns `SyncOutcome { copy: OperationOutcome, delete:
OperationOutcome }` — the copy phase (new/changed entries) and delete
phase (dest-only orphans) are reported separately, since they're
different entry sets. If the copy phase stops early (aborted, cancelled,
or hits a fatal error), the delete phase doesn't run at all — sync would
rather leave a stale orphan for the next run than delete real data while
the copy side is in a known-incomplete state.
