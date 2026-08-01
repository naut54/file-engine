# Adding a feature

Conventions this codebase has followed consistently — not hard rules,
but deviating from them should be a deliberate choice, not an oversight.

## A new Cargo feature

- Add it to `Cargo.toml`'s `[features]` table with a short comment on
  what it pulls in and why (see the existing entries for the style).
  Optional dependencies are `dep:`-referenced from inside the feature
  that needs them, not exposed as their own toggleable feature, unless
  there's a genuine reason a caller would want the dependency without
  the feature.
- Gate the module(s) it needs in `lib.rs` with `#[cfg(feature = "...")]`
  on the `mod` declaration and on the corresponding `pub use`. If the
  feature doesn't imply `operations` (like `watch` doesn't), any module
  it needs that `operations`-gated code also needs (e.g. `operations/`
  itself) needs an `any(...)` condition, not just `operations` alone —
  see the comment on `mod operations;` in `lib.rs` for the exact
  reasoning.
- Run the full feature-combination sweep before considering it done —
  see [testing.md](testing.md). A change can compile clean under
  `--all-features` and still fail under some other combination (usually
  dead-code from a `#[cfg]` that doesn't match reality once one feature
  is off).

## A new operation (following the `copy`/`move`/`sync` pattern)

1. **Builder struct** in `src/operations/<name>.rs` — fields for every
   option, a `pub(crate) fn new(...)` constructor with sensible
   defaults, chainable `pub fn <option>(mut self, ...) -> Self` methods,
   and `.start()` that spawns a task and returns `Handle<T>` (or a
   dedicated handle type if the operation doesn't fit `Handle<T>`'s
   shape — see `watch`'s `WatchHandle`).
2. **Orchestration function** — if it goes through the batching pipeline,
   call `pipeline::run_copy_pipeline` or `run_workload_pipeline`
   directly rather than reimplementing Profiler → Planner → Dispatcher
   wiring. If it needs its own `EntryAction` (copy is currently the only
   consumer of that trait), implement it in `planner/action.rs`.
3. **Wire it into `FileEngine`** in `lib.rs`, feature-gated to match the
   builder's own gate, and re-export the builder type **and any other
   public type it introduces**, at every module level between the type's
   definition and the crate root. `CompressFormat` was a real bug here:
   it was declared `pub enum` inside `operations/compress.rs`, but
   neither `operations/mod.rs` nor `lib.rs` re-exported it (only
   `CompressBuilder` was), and `mod operations` itself isn't `pub` — so
   `CompressBuilder::format(CompressFormat)` was a public method no
   external crate could actually call, since nothing could name or
   construct its argument type. `cargo build`/`test`/`check` all stayed
   clean throughout, because the enum's own `pub` satisfies rustc's
   local visibility check even though the *path* to reach it was
   private — this class of bug is only caught by actually trying to
   reference the type from outside the crate (an example under
   `examples/`, or a doctest, compiles as an external consumer and
   would have caught it; nothing in this crate's own `src/` tests
   would, since they compile with `pub(crate)`-level access
   regardless).
4. **Tests**: at minimum, mirror `copies_a_single_file`/
   `copies_a_directory_tree_end_to_end`-style tests in `pipeline.rs` or
   the operation's own file — a single file, and a nested directory tree
   with a mix of small/large-relative-to-threshold files, exercising
   Profiler classification, Planner batching, and Dispatcher fan-out
   together.

## Shared options across builders

If a new option applies to more than one builder (`.overwrite()`,
`.small_file_threshold()`, `.batch_concurrency()`, `.on_error()`,
`.allow_filesystem_integrity_risk()` all do), add it to every builder it
applies to with the same name and the same default, even if one
builder's internal plumbing differs — consistency across the builder
API matters more than each builder being minimal.

## Adding a new `Error` variant

- Add it to the `Error` enum in `src/error.rs` with a `#[error("...")]`
  message, feature-gated if it can only ever be constructed under one
  feature (see the `compress`-gated and `operations`-gated variants for
  the pattern).
- Add a matching entry to `errors.toml` (`FE_<NAME>` key, `template`,
  optional `hint`) — this is the message catalog `diagnostics` will
  eventually load from; keep the two in sync even though nothing
  auto-generates one from the other yet.
- Decide whether it's fatal (`Error::is_fatal()`) — fatal means it stops
  everything regardless of `ErrorStrategy`; per-entry means the caller's
  `ErrorStrategy` decides. Default to per-entry unless the error
  genuinely means "nothing after this point can be trusted" (disk full,
  cancelled, a whole-destination risk that isn't a property of any
  specific entry).
