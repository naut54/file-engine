# Contributing

`file-engine` is built as three layers — Profiler, Planner/Dispatcher,
and a thin Public API on top — plus a few features (`watch`, `compress`)
that sit beside the pipeline rather than through it. See
[architecture.md](architecture.md) for how they fit together today, and
[adding-a-feature.md](adding-a-feature.md) for the conventions a new
operation is expected to follow.

## Source layout

```
src/
  lib.rs              — FileEngine, public re-exports
  error.rs            — Error, Result
  handle.rs           — Handle<T> (Future + Progress stream)
  progress.rs          — Progress, ProgressReporter
  paths.rs             — cross-filesystem path-comparison normalization
  profiler/            — walks the source, classifies entries, probes
                          destination filesystem capabilities
    scan.rs
    workload.rs
    validate.rs
    fs_caps/            — per-platform filesystem-capability probing
  planner/              — turns a Workload into an ExecutionPlan, runs it
    batch.rs
    plan.rs
    action.rs
    dispatcher.rs
    outcome.rs
    config.rs
  operations/           — public builders + orchestration per operation
    pipeline.rs          — shared Profiler -> Planner -> Dispatcher wiring
    copy.rs
    move_path.rs
    sync.rs
    diff.rs
    compress.rs
    watch.rs
  watch_event.rs, watch_handle.rs — watch's types (outside operations/,
                                     since watch doesn't use the pipeline)
```

Every module above `operations/` is feature-gated to match what actually
needs it (`operations`, `watch`, `compress`, `sync`, `permissions`) —
see `Cargo.toml`'s `[features]` table and `lib.rs`'s `#[cfg(...)]`
attributes on each `mod` declaration for the exact gating.

## Before you start

Run the same verification this codebase's history has consistently used
before considering any change done — see
[testing.md](testing.md). The short version: this crate is built with
`-D warnings` (see `.cargo/config.toml`), and a change that compiles
under one feature combination can still fail under another, so check
more than the default build.
