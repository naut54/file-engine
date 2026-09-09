# Error handling

## `Error`

```rust
pub enum Error {
    SourceNotFound { path: PathBuf },
    DestExists { path: PathBuf },
    Cancelled,
    NoSpace { needed: u64, available: u64 },
    PermissionDenied { path: PathBuf },
    Io { path: PathBuf, source: io::Error },

    // `compress` feature only
    UnknownCompressFormat { path: PathBuf },
    GzipRequiresFile { path: PathBuf },

    // `operations` feature only — see filesystem-safety.md
    CaseCollision { path: PathBuf, other: PathBuf },
    FileTooLargeForDest { path: PathBuf, size: u64, max: u64 },
    ReservedName { path: PathBuf },
    FilesystemIntegrityRisk { filesystem: String },

    // `operations` feature only — `move_many` pre-flight validation
    DuplicateSourceName { path: PathBuf, other: PathBuf },
    InvalidSourceName { path: PathBuf },

    // `analyze` or `remove` feature (either enables it independently)
    InvalidGlobPattern { pattern: String, source: globset::Error },

    // `remove` feature only
    RemoveCriteriaRequired,
    TrashFailed { path: PathBuf, source: trash::Error },
}
```

`Error` shows up in two places with different meanings:

- **As a top-level `Err`** from `.start()` or from `.await`ing a
  `Handle` — something prevented the operation from running at all (bad
  source path, a destination filesystem's write-integrity risk you
  haven't opted into).
- **Inside `OperationOutcome.failed`/`cleanup_failed`** — a *specific
  entry* couldn't be processed, but the operation as a whole continued
  (or stopped, depending on `ErrorStrategy` — see below).

## `ErrorStrategy`

Set with `.on_error(strategy)` on any batching-pipeline builder.
Governs what happens when an individual entry fails during copy/move/sync
— case collisions, oversized files for the destination filesystem, and
reserved names (see [filesystem-safety.md](filesystem-safety.md)) are all
per-entry failures governed by this the same way an ordinary I/O error
would be:

```rust
pub enum ErrorStrategy {
    ContinueAndCollect, // default — keep going, collect every failure
    AbortOnError,       // stop at the first failure; queued work never starts
    Undo,                // like AbortOnError, but also rolls back what
                          // already succeeded (deletes destination copies
                          // already written)
}
```

Some errors bypass `ErrorStrategy` entirely and always stop the whole
operation — `Error::Cancelled`, `Error::NoSpace`,
`Error::FilesystemIntegrityRisk`, `Error::DuplicateSourceName`, and
`Error::InvalidSourceName`. These describe conditions where continuing
can't produce a trustworthy result, or (for `FilesystemIntegrityRisk`,
and `move_many`'s two duplicate/invalid-name checks) aren't a property
of any specific entry to begin with — they're caught before any source
is touched, as a top-level `Err` rather than a per-entry failure inside
`OperationOutcome.failed`.

`Error::RemoveCriteriaRequired` is the same shape for `remove()`: an
unconfigured filter would otherwise match everything under the root, so
it's returned as a top-level `Err` before any scanning happens, unless
`.allow_unfiltered_delete(true)` opts in.
`Error::TrashFailed`, by contrast, *is* a per-entry failure governed by
`ErrorStrategy` like any other — it just can't be rolled back by `Undo`
the way a copy/move can: a hard-deleted file is gone, and even a
trashed one has no reliable cross-platform restore API. Prefer
previewing with `remove()`'s default `.dry_run(true)` over relying on
`Undo` to walk back a mistake.

## Checking what happened

```rust
let outcome = engine.copy("src/", "dst/").on_error(ErrorStrategy::ContinueAndCollect).start()?.await?;

if !outcome.failed.is_empty() {
    for (entry, err) in &outcome.failed {
        eprintln!("{}: {err}", entry.relative_path.display());
    }
}

if let Some(reason) = outcome.stopped_early {
    eprintln!("operation did not complete: {reason:?}");
}
```
