# Changelog

All notable changes to this project are documented here.

This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.3.0]

### Added

- **`FileEngine::move_many(sources, dest)` / `MoveManyBuilder`** — moves
  several independent sources into one destination directory as a
  single batched operation, rather than requiring one `.move_path()`
  call per source (and losing a shared `ErrorStrategy`/concurrency
  pool/progress stream across them). `dest` is always a directory
  sources land *inside*; each source keeps its own basename. Two
  sources resolving to the same basename, or a source with no file name
  to move under, is rejected up front as `Error::DuplicateSourceName`/
  `Error::InvalidSourceName` before any source is touched.

  Each source first attempts its own atomic rename (same fast path
  `.move_path()` uses); sources that need the cross-filesystem fallback
  are scanned, re-rooted under their own basename, and merged into a
  single `run_workload_pipeline` call, so `AbortOnError`/`Undo` stop the
  whole batch together rather than source-by-source. New
  `OperationOutcome::sources_failed` reports whole-source failures (a
  rename error other than crossing filesystems, a source that vanished)
  that happen before any per-file `Entry` exists — always empty for
  `.copy()`/`.move_path()`.

- **`.skip_if_identical(bool)`** on `CopyBuilder`, `MoveBuilder`, and
  `MoveManyBuilder` (feature `checksum`) — when the destination already
  exists and `.overwrite(false)` (the default), compares content (size
  first, then a blake3 hash of both files) instead of immediately
  failing with `Error::DestExists`. An identical destination is left
  untouched rather than re-copied; a destination that exists but differs
  still fails exactly as without this — a library has no way to
  interactively ask whether to replace it, so that decision is left to
  whatever's built on top of this crate, which can catch `DestExists`
  and retry with `.overwrite(true)`. New `OperationOutcome::skipped` and
  `Progress::EntrySkipped` report entries left alone this way, distinct
  from `succeeded` since no bytes were transferred.

  For `.move_path()`/`.move_many()`, this applies to the atomic-rename
  fast path too, not just the cross-filesystem fallback: `source` is
  still removed on an identical-destination skip (that's still what
  "moved" means), it just skips redundantly rewriting a destination that
  already matches.

### Fixed

- **`.move_path()`'s atomic-rename fast path now creates a missing
  destination parent directory** instead of failing with a misleading
  `Error::SourceNotFound` that actually blamed the wrong path —
  `rename(2)` returns the same `NotFound` whether it's `source` or a
  component of `dest`'s parent chain that's missing, and the fast path
  used to attribute every such failure to `source` unconditionally.
  `.copy()` already handled this correctly via `create_dir_all`; only
  the move fast path had the gap.

- **`.move_path()`'s atomic-rename fast path now respects
  `.overwrite(false)`.** `rename(2)` (and its Windows equivalent)
  natively replaces an existing destination file with no error, so
  `overwrite(false)` was previously enforced only on the cross-device
  fallback path, not on same-filesystem moves — the overwhelmingly
  common case. A same-filesystem move into an existing file now fails
  with `Error::DestExists`, matching `.copy()`'s and the fallback path's
  existing behavior, unless `.skip_if_identical(true)` resolves it.

## [2.2.0]

### Changed

- **`analyze()`'s tree walk is now multithreaded.** Replaced `walkdir`
  with `jwalk` under the `analyze` feature: directory reads and `stat()`
  calls now run across a worker pool instead of one at a time on a
  single thread. Aggregation (the largest-files heap, extension/MIME
  stats, age buckets) still happens serially on the consuming task, so
  no locking was introduced there — only the underlying directory
  traversal is parallelized. On a 330k-file/138k-dir local tree this cut
  wall time from 12.8s (single-threaded, 45% CPU) to 9.0s (194% CPU)
  with a warm disk cache; the gap widens further on cold caches or
  network filesystems, where per-`stat()` latency (not local CPU) is the
  bottleneck. `profiler::scan` (feature `operations`) is unaffected — it
  still uses `walkdir`.

### Added

- **`AnalyzeBuilder::walk_concurrency(n)`** — worker-thread count for the
  parallel walk, defaulting to `available_parallelism()`, matching
  `.hash_concurrency()`.

## [2.1.0]

### Added

- **`FileEngine::analyze()`** — read-only tree inspection: file/directory
  counts, total size, the largest N files, size and count grouped by
  extension, an age histogram (last-modified bucketed into
  under-a-day/week/month/year/older), plus filters (extension,
  glob-based excludes, size range, modified-time range, max depth,
  symlink-following) so a caller narrows what gets counted rather than
  filtering a full report after the fact. Feature `analyze`, on by
  default — it was already reserved in `Cargo.toml` but unimplemented
  until now.

  Follows the same builder shape as every other operation
  (`FileEngine::analyze(path)...start()`), but returns a dedicated
  `AnalysisHandle`/`AnalysisProgress` pair rather than the existing
  `Handle<T>`/`Progress`: those are hard-coded to each other, and both
  live behind the `operations` feature, which `analyze` deliberately
  doesn't require — the same reasoning `WatchHandle` already established
  for `watch`.

  Excluded directories (`.exclude_globs()`, backed by `globset`) prune
  traversal itself rather than being filtered out after a full walk, and
  `max_depth` passes straight through to `walkdir`'s own depth-limiting
  for the same reason. A per-entry error (a permission-denied
  subdirectory, a symlink loop under `.follow_symlinks(true)`) is
  handled per `.on_error(AnalysisErrorStrategy)`, distinct from
  `planner::ErrorStrategy` since analysis never writes anything and
  `Undo` has no meaning here.

- **`.detect_mime_types(bool)`** (feature `analyze`) and
  **`.detect_duplicates(bool)`** (feature `checksum`) on
  `AnalyzeBuilder` — both off by default, since either turns the walk
  from metadata-only into a full extra read per matched file. Duplicate
  detection groups candidates by size first (two files can only be
  byte-identical if they're the same size) and only blake3-hashes files
  that actually collide, with hashing bounded by `.hash_concurrency(n)`
  (same `Arc<Semaphore>` pattern the batching pipeline already uses for
  its own worker pool).

- **`AnalysisReport::errors`/`errors_total`** and (feature `checksum`)
  **`AnalysisReport::duplicates`/`duplicate_groups_total`/
  `duplicate_bytes_wasted`** — the detailed lists are capped
  (`.max_reported_errors()`, `.max_reported_duplicates()`) so a badly
  permissioned or heavily duplicated tree can't grow the report
  unboundedly, but the counts and `duplicate_bytes_wasted` stay uncapped
  and accurate even once the sample is truncated — the wasted-space
  figure in particular is summed over every duplicate group found, not
  just the ones that made it into the capped list.

- **`FE_INVALID_GLOB_PATTERN`** in `errors.toml`, backing
  `Error::InvalidGlobPattern` — an invalid pattern passed to
  `.exclude_globs()`.

### Changed

- The README's feature table no longer describes `analyze` as
  unimplemented.

### Fixed

- Nothing user-visible; 2.1.0 is additive.

## [2.0.0]

### Upgrading from 1.x

Every public output type is now `#[non_exhaustive]` — `Progress`,
`StopReason`, `OperationOutcome`, and `SyncOutcome`. This release absorbs
the churn so that later additions to any of them are not breaking
changes. What that means for your code:

`Progress` gained a variant. If you `match` on it, add a wildcard arm:

```rust
match progress {
    Progress::Started { .. } => { /* ... */ }
    Progress::EntryCompleted { .. } => { /* ... */ }
    // ...
    _ => {}
}
```

`Progress` is now `#[non_exhaustive]`, so this arm is required — and this
is the last time adding a variant will break you.

`OperationOutcome` gained a `duration` field and is now
`#[non_exhaustive]`. Reading it is unaffected, as is
`OperationOutcome::default()`; exhaustive destructuring needs a trailing
`..`. Building one with a struct literal is no longer possible outside
the crate at all — including with `..Default::default()`, which
`#[non_exhaustive]` also blocks. It is an output type, so this should
only affect test fixtures; construct them from `Default::default()` and
assign the fields you need.

`SyncOutcome` is `#[non_exhaustive]` on the same terms — read `copy` and
`delete` as before, but build one via `SyncOutcome::default()` rather
than a struct literal.

`StopReason` is `#[non_exhaustive]`, so a `match` on it needs a `_` arm,
exactly like `Progress`.

Nothing else changed: every builder, `Handle<T>`, `Error`, and every
feature flag keep their 1.x behaviour and signatures.

### Added

- **`EtaEstimator`** — predicts time remaining from the `Progress`
  stream. Feed it every event with `.observe()` and read `.estimate()`,
  which returns `Option<Duration>` (`None` until there is something
  measured to extrapolate from). Purely observational: no I/O, no tasks,
  no reference to the running operation, and no cost if unused.

  It models three cost regimes separately, because a single
  bytes-per-second figure describes none of them: batched small files
  cost per *file*, streamed large files cost per *byte*, and the
  directory pre-pass costs per *directory* and isn't counted in
  `Started`'s `bytes_total` at all.

- **`Handle::elapsed()`** — wall time since the operation was spawned,
  the counterpart to `EtaEstimator::estimate()` for a UI showing elapsed
  next to remaining. Covers the whole run including the directory
  pre-pass, so it won't match a timer started on the first `Progress`
  event. Freezes once the handle has been polled to completion.

- **`OperationOutcome.duration`** — the same figure after the handle is
  gone, since `handle.await` consumes it. `SyncOutcome`'s two outcomes
  are timed per phase and so don't sum to the whole run (the diff
  preceding them is in neither); a delete phase skipped because the copy
  phase stopped early reports `Duration::ZERO`.

- **`Progress::Planned`** — the workload split (directories, small
  files/bytes, large files/bytes, and the threshold that separated them),
  emitted once per phase before the directory pre-pass. Earlier than
  `Started`, which is emitted after that pre-pass and counts only file
  entries. Not emitted by the delete sweeps, which are metadata-only.

- **`Progress::EntryProgress`** — cumulative bytes written for a large
  entry still in flight, sampled from the destination file every 250ms.
  `tokio::fs::copy` is opaque while it runs, so without this a lone large
  file emitted nothing between `EntryStarted` and `EntryCompleted` — its
  transfer rate was unmeasurable for exactly as long as the copy took. A
  copy the filesystem satisfies by copy-on-write finishes before the
  first sample and emits none, which is correct: there was nothing to
  wait for.

### Changed

- **`Progress` is `#[non_exhaustive]`.** See "Upgrading" above.

- **`OperationOutcome`, `SyncOutcome`, and `StopReason` are
  `#[non_exhaustive]`**, so future fields and variants land without a
  major version. See "Upgrading" above.

- **Dispatch order: the smallest large file now runs first.** Previously
  every batch was queued ahead of every stream, so on a workload of many
  small files plus a few large ones, no large file completed until the
  operation was nearly over — and with it, no per-byte transfer rate was
  observable. On a 2.7GB test tree the first stream completed at 95%
  elapsed. Everything behind that first stream is unchanged. This affects
  any consumer of `Progress`, not just time estimation. Applies to
  `copy`/`move`/`sync` and to `compress`'s own pipeline (where archive
  entry order changes as a result — immaterial, since zip entries are
  addressed by name, and order already varied with concurrency).

- **`tokio`'s `time` feature is now enabled.** Required by the
  destination sampling above. If you build a runtime without the time
  driver, sampling is skipped and the copy still succeeds — you simply
  get no `EntryProgress` events.

- `repository` in `Cargo.toml` now points at the real URL.

### Fixed

- Nothing user-visible; 2.0.0 is additive plus the `Progress` break.
