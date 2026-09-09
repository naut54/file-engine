# Operations

All builders share a common shape: `FileEngine::<operation>(...)` returns
a builder, options are chained, `.start()` kicks it off and returns a
`Handle<T>` (or `WatchHandle`) immediately — the work runs in the
background. See
[progress-and-cancellation.md](progress-and-cancellation.md) for what to
do with the handle.

## Shared options

These appear (with the same name and meaning) on every batching-pipeline
builder — `CopyBuilder`, `MoveBuilder`, `MoveManyBuilder`, `SyncBuilder`
— unless noted. `RemoveBuilder` is the exception: it has no destination,
so it shares only `.on_error()` and `.batch_concurrency()` from this
list — see [Remove](#remove) below for its own options.

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

## Remove

`RemoveBuilder` deletes files under a root that match a filter, rather
than a path outright — there's no destination, so none of the
copy/move/sync options above apply beyond `.on_error()` and
`.batch_concurrency()`.

| Method | Default | Meaning |
|---|---|---|
| `.extensions(...)` | any | Only files with one of these extensions (case-insensitive, no leading dot) match. |
| `.exclude(...)` | none | Glob patterns (relative to the root) that exclude an otherwise-matching entry. |
| `.min_size(bytes)` / `.max_size(bytes)` | unbounded | Size range an entry must fall in, inclusive on both ends. |
| `.modified_after(t)` / `.modified_before(t)` | unbounded | Modified-time range, inclusive. A file with no readable mtime never matches once either is set. |
| `.max_depth(n)` | unbounded | Bounds how far the walk descends; the root is depth 0, its immediate children depth 1. |
| `.follow_symlinks(bool)` | `false` | Off by default (a symlink is skipped, not followed). When `true`, a symlinked directory's contents become eligible too, and a symlink cycle surfaces as a per-entry error rather than hanging. |
| `.dry_run(bool)` | **`true`** | Preview only: matches land in `RemoveOutcome::previewed` and nothing is touched. Pass `false` to actually remove. |
| `.hard_delete(bool)` | **`false`** | Matched entries go to the platform trash/recycle bin by default. Pass `true` to unlink them permanently instead. |
| `.allow_unfiltered_delete(bool)` | `false` | Required to run with every filter above left unset — otherwise `.start()`'s `Handle` resolves to `Err(Error::RemoveCriteriaRequired)` before anything is touched. |

The two defaults in bold are the reverse of every other builder in this
crate — biased toward safety since this is the one irreversible
operation the crate offers. A platform/environment with no trash service
available fails per-entry with `Error::TrashFailed` rather than silently
falling back to a permanent delete.

`ErrorStrategy::Undo` stops the batch on the first failure the same as
it does for copy/move, but can't roll back entries already removed — a
hard-deleted file is gone, and a trashed one has no reliable
cross-platform restore API. Preview with the default `.dry_run(true)`
before committing to `.dry_run(false)` rather than relying on `Undo` to
walk back a mistake.

## Sync's outcome shape

`sync()` returns `SyncOutcome { copy: OperationOutcome, delete:
OperationOutcome }` — the copy phase (new/changed entries) and delete
phase (dest-only orphans) are reported separately, since they're
different entry sets. If the copy phase stops early (aborted, cancelled,
or hits a fatal error), the delete phase doesn't run at all — sync would
rather leave a stale orphan for the next run than delete real data while
the copy side is in a known-incomplete state.

Each phase carries its own `duration`, so the two don't sum to the whole
run — the diff that precedes them belongs to neither, and a skipped
delete phase reports `Duration::ZERO`. Use `Handle::elapsed()` for the
run as a whole. See
[progress-and-cancellation.md](progress-and-cancellation.md#time-elapsed).
