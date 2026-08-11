# Architecture

## The three layers

**Profiler** (`src/profiler/`) walks the source tree once and produces a
`Workload`: files classified as `small` or `large` relative to a
threshold, plus every directory encountered (`DirEntry`). It also probes
the destination's filesystem capabilities (`fs_caps/`) — type, whether
it's case-sensitive, max file size, whether Windows naming rules apply,
timestamp granularity, and any known write-integrity risk — and
validates the workload against them (`validate.rs`), removing entries
that can't be represented on that destination (case collisions,
oversized files, reserved names) before anything is dispatched. See
[../guide/filesystem-safety.md](../guide/filesystem-safety.md) for the
user-facing behavior this produces.

**Planner** (`src/planner/`) turns a `Workload` into an `ExecutionPlan`:
small files are sorted and greedily packed into size/count-bounded
`Batch`es (`batch.rs`), large files each become an individual stream job
(`plan.rs`). The `Dispatcher` (`dispatcher.rs`) then runs that plan
against a concurrency-limited worker pool, applying the configured
`ErrorStrategy` to per-entry failures and stopping everything immediately
on a fatal one (`Error::is_fatal()`) regardless of strategy. `action.rs`
defines `EntryAction`, the trait a concrete operation (currently just
`CopyAction`) implements to say what actually happens to one entry.

**Operations** (`src/operations/`) are the public builders — `copy`,
`move_path`, `sync`, `compress`, `watch` — each a thin layer that turns
builder options into a call through `pipeline.rs`'s shared
`run_copy_pipeline`/`run_workload_pipeline`, which wires
Profiler → Planner → Dispatcher together. `move_path` attempts an atomic
rename first and only falls back to the pipeline on a cross-device
error; `sync` diffs source against destination first (`diff.rs`) and
runs only the changed entries through the same pipeline, then sweeps
dest-only orphans separately. `watch` and `compress` don't go through
this pipeline at all — `watch` is a thin wrapper over the `notify`
crate's event stream, `compress` has its own concurrent
read-and-compress-into-one-writer pipeline.

## Data flow for a copy

```
FileEngine::copy(src, dst)
  -> CopyBuilder (options)
  -> .start() spawns a task, returns Handle<OperationOutcome> immediately
       -> pipeline::run_copy_pipeline
            -> profiler::scan(src)              -> Workload
            -> profiler::probe_fs_caps(dst)      -> FilesystemCapabilities
            -> profiler::validate(workload, caps) -> entries that can't
                                                      go to this destination,
                                                      removed up front
            -> ensure_directories_exist            (only directories with
                                                      no file anywhere
                                                      beneath them — see
                                                      pipeline.rs's
                                                      directories_covered_by_files)
            -> planner::plan(workload)           -> ExecutionPlan
            -> planner::dispatch(plan, CopyAction) -> OperationOutcome
```

Progress events (`Progress::Planned`, `Started`,
`EntryStarted`/`Completed`/`Failed`,
`DirectoriesStarted`/`DirectoryCompleted`/`DirectoryFailed`) flow out
through an unbounded channel the whole way, independent of whether
anyone's listening.

The `Dispatcher` pulls the smallest stream unit to the front of its queue
before any batch, and samples each streamed entry's destination file every
250ms while it runs (`EntryProgress`). Both exist so that a per-byte
transfer rate is observable early: without the reordering, streams ran
after every batch and the first one completed at ~95% elapsed; without the
sampling, a lone large file reported nothing at all until it finished.
Sampling is used rather than a chunked copy loop so that `fs::copy` keeps
the kernel's accelerated paths (`clonefile`, `copy_file_range`).

`Planned` is emitted by `run_workload_pipeline` (and by `compress`'s own
pipeline) before the directory pre-pass, describing the workload split
that `Started` can't: small files cost per-file, large files cost per-byte,
and directories cost per-directory. `src/eta.rs`'s `EtaEstimator` consumes
it to price the three regimes separately — see its type-level docs for why
a single bytes/sec rate is wrong here, and for the one case (a streamed
file in flight reporting nothing until it completes) the model handles
only approximately.

## Cancellation

A `CancellationToken` is threaded through every layer. It's checked
between units of work (batches, individual streamed files, directories)
— never in the middle of one. Dropping a `Handle` does **not** cancel
anything; the background task detaches and keeps running. Call
`.cancel()` explicitly.

## Error handling

`Error` (`src/error.rs`) is one enum for the whole crate, feature-gated
per variant where a variant only makes sense under a specific feature
(e.g. `UnknownCompressFormat` under `compress`). `is_fatal()` decides
which errors bypass `ErrorStrategy` and stop everything regardless
(`Cancelled`, `NoSpace`, `FilesystemIntegrityRisk`) versus which are
per-entry and governed by the configured strategy.
