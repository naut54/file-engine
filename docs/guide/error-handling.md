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
operation — `Error::Cancelled`, `Error::NoSpace`, and
`Error::FilesystemIntegrityRisk`. These describe conditions where
continuing can't produce a trustworthy result, or (for
`FilesystemIntegrityRisk`) aren't a property of any specific entry to
begin with — every write to that destination carries the risk.

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
