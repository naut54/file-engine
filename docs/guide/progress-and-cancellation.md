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
pub enum Progress {
    Started { bytes_total: Option<u64>, entries_total: usize },
    EntryStarted { entry: Entry },
    EntryCompleted { entry: Entry },
    EntryFailed { entry: Entry },
    DirectoriesStarted { total: usize },
    DirectoryCompleted { path: PathBuf },
    DirectoryFailed { path: PathBuf },
}
```

- `Started`/`Directories*` can each be emitted more than once per
  operation — `sync` emits a `Started` for its copy phase and again for
  its delete phase; directory creation (a pre-pass that ensures
  destination directories exist, including ones with no files at all)
  reports separately from file entries since it operates on directories,
  not `Entry` values.
- `EntryFailed` carries the `Entry` only, not the `Error` — look up the
  actual error from the final outcome's `failed`/`cleanup_failed` list
  once the handle resolves.
- If nothing is reading `.progress()`, sends are silently dropped —
  it's not an error to ignore the stream entirely and just `.await` the
  handle.

## The final outcome

```rust
pub struct OperationOutcome {
    pub succeeded: Vec<Entry>,
    pub failed: Vec<(Entry, Error)>,
    pub cleanup_failed: Vec<(Entry, Error)>,   // move only — see below
    pub stopped_early: Option<StopReason>,
    pub directories_failed: Vec<(PathBuf, Error)>,
}

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
