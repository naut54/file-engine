# Progress and cancellation

## `Handle<T>`

Every operation's `.start()` returns a `Handle<T>` (`T` is
`OperationOutcome` for copy/move/compress, `SyncOutcome` for sync) —
`watch` returns a distinct `WatchHandle` instead, since it streams
indefinitely rather than resolving to a final outcome. The work runs in
a background task the moment `.start()` returns; you don't need to poll
or await anything for it to make progress.

```rust
let mut handle = engine.copy("src/", "dst/").start()?;

// Read progress while it runs...
while let Some(event) = handle.progress().next().await { /* ... */ }

// ...then get the final result.
let outcome = handle.await?;
```

`Handle<T>` implements `Future<Output = Result<T>>` directly — `.await`
it (or hand it to `tokio::join!`/`select!` like any other future).

**Dropping a `Handle` does not cancel the operation.** It detaches —
the background task keeps running to completion on its own; you just
lose the ability to observe or cancel it. Call `.cancel()` explicitly if
you want to stop it.

## Cancellation

```rust
let handle = engine.copy("src/", "dst/").start()?;
handle.cancel();
let outcome = handle.await?; // outcome.stopped_early == Some(StopReason::Cancelled)
```

Cancellation is cooperative, checked between batches/streamed files —
not mid-file and not mid-batch. Whatever's already in flight when
`.cancel()` is called finishes normally; nothing queued after that point
starts.

## The `Progress` stream

```rust
#[non_exhaustive]
pub enum Progress {
    Planned {
        directories: usize,
        small_files: usize,
        small_bytes: u64,
        large_files: usize,
        large_bytes: u64,
        small_file_threshold: u64,
    },
    Started { bytes_total: Option<u64>, entries_total: usize },
    EntryStarted { entry: Entry },
    EntryProgress { entry: Entry, bytes_copied: u64 },
    EntryCompleted { entry: Entry },
    EntryFailed { entry: Entry },
    DirectoriesStarted { total: usize },
    DirectoryCompleted { path: PathBuf },
    DirectoryFailed { path: PathBuf },
}
```

`Progress` is `#[non_exhaustive]`, so a `match` on it needs a `_` arm.

- `Planned` comes first in each phase, before the directory pre-pass —
  so unlike `Started`, it accounts for directory creation too. It
  describes the work in the two forms that cost differently: batched
  small files (per-file cost) and streamed large files (per-byte cost).
  The delete sweeps don't emit it; they're metadata-only phases and just
  emit a bare `Started`.
- `Started`/`Directories*` can each be emitted more than once per
  operation — `sync` emits a `Started` for its copy phase and again for
  its delete phase; directory creation (a pre-pass that ensures
  destination directories exist, including ones with no files at all)
  reports separately from file entries since it operates on directories,
  not `Entry` values.
- `EntryProgress` is emitted only for large (streamed) entries, by
  sampling the destination file's size roughly every 250ms while the
  copy runs. `bytes_copied` is cumulative and clamped to the entry size.
  A copy the filesystem satisfies by copy-on-write (APFS `clonefile`,
  reflinks) finishes before the first sample and emits none — correctly,
  since there was nothing to wait for.
- `EntryFailed` carries the `Entry` only, not the `Error` — look up the
  actual error from the final outcome's `failed`/`cleanup_failed` list
  once the handle resolves.
- If nothing is reading `.progress()`, sends are silently dropped —
  it's not an error to ignore the stream entirely and just `.await` the
  handle.

## Estimating time remaining

`EtaEstimator` turns the `Progress` stream into a predicted time
remaining. Feed it every event and query it whenever you want to
redraw:

```rust
let mut handle = engine.copy("src/", "dst/").start()?;
let mut eta = EtaEstimator::new();

while let Some(event) = handle.progress().next().await {
    eta.observe(&event);
    match eta.estimate() {
        Some(remaining) => println!("{}s left", remaining.as_secs()),
        None => println!("estimating..."),  // nothing has completed yet
    }
}
```

It is purely observational — no I/O, no tasks, no reference to the
running operation — so it costs nothing if you don't use it.

Why not just divide bytes copied by elapsed time: this crate's pipeline
has two different cost regimes and a phase that isn't measured in bytes
at all. Batched small files cost roughly a fixed amount *per file*
regardless of size; streamed large files cost per *byte*; and the
directory pre-pass runs before `Started` and costs per *directory* (on
slow removable media it can dominate the whole run). `EtaEstimator`
measures the three separately and recombines them as
`directories + max(small, large)`, since the pre-pass finishes before
dispatch begins while small and large files overlap.

`estimate()` returns `None` rather than guessing when nothing has
completed yet.

## Time elapsed

`Handle::elapsed()` is the counterpart to `estimate()` — wall time since
the operation was spawned, so a UI can show elapsed alongside remaining
without keeping its own `Instant`:

```rust
while let Some(event) = handle.progress().next().await {
    eta.observe(&event);
    println!("{:?} elapsed, {:?} left", handle.elapsed(), eta.estimate());
}
```

It covers the whole run, including the directory pre-pass that precedes
the first `Started` event — so it won't match a timer you start on the
first event off the stream.

It freezes once the handle has been polled to completion, so it reads as
the operation's total duration rather than continuing to climb. Since
`handle.await` consumes the handle, the final duration is easier to read
off the outcome (below) than off the handle.

The outcome carries its own `duration`, for after the handle is gone:

```rust
let outcome = handle.await?;
println!("copied {} files in {:?}", outcome.succeeded.len(), outcome.duration);
```

`SyncOutcome`'s two `OperationOutcome`s are timed per phase, so
`copy.duration` and `delete.duration` don't sum to the whole run — the
diff that precedes them is counted in neither. A delete phase skipped
because the copy phase stopped early reports `Duration::ZERO`.
`Handle::elapsed()` remains the figure for the run as a whole.

Byte rates come from three sources, in decreasing order of authority:
`EntryProgress` samples from a large file still in flight; then
completed large files; then, before either exists, overall observed
throughput. That last one is dominated by small files, which pay
per-file overhead streaming doesn't, so the estimate starts pessimistic
and tightens as real measurements arrive. Bytes counted from sampling
aren't counted again when the entry completes.

## The final outcome

```rust
#[non_exhaustive]
pub struct OperationOutcome {
    pub succeeded: Vec<Entry>,
    pub failed: Vec<(Entry, Error)>,
    pub cleanup_failed: Vec<(Entry, Error)>,   // move only — see below
    pub stopped_early: Option<StopReason>,
    pub directories_failed: Vec<(PathBuf, Error)>,
    pub duration: Duration,                    // see "Time elapsed" above
}

#[non_exhaustive]
pub enum StopReason {
    Fatal,        // e.g. disk full — stops regardless of ErrorStrategy
    AbortOnError, // ErrorStrategy::AbortOnError triggered
    Cancelled,    // .cancel() was called
    Undo,         // ErrorStrategy::Undo triggered a rollback
}
```

`cleanup_failed` is populated only by `move`'s deferred deletion sweep:
entries that copied successfully but whose original source couldn't be
removed afterward — data duplicated, not lost. `directories_failed` is
populated by the directory-creation pre-pass and, if
`.preserve_permissions()` was used, the directory-permissions pass.

`stopped_early` being `Some(_)` means the operation didn't run to
completion — check it before assuming `succeeded`/`failed` cover every
entry you expected.

`OperationOutcome` and `SyncOutcome` are `#[non_exhaustive]`: read them
freely, but you can't build one outside the crate (`Default::default()`
plus field assignment works for test fixtures), and destructuring needs
a trailing `..`. `StopReason` is `#[non_exhaustive]` too, so a `match`
on it needs a `_` arm — same as `Progress`.
