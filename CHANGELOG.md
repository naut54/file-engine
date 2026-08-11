# Changelog

All notable changes to this project are documented here.

This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
