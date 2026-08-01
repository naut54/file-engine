# file-engine documentation

- **[guide/](guide/)** — for anyone using `file-engine` as a dependency:
  getting started, feature flags, the copy/move/sync/watch/compress
  operations, progress and cancellation, error handling, and what the
  crate does to protect you from filesystem-specific pitfalls (case
  collisions, exFAT write-integrity risk, Windows-reserved names).
- **[contributing/](contributing/)** — for anyone working on
  `file-engine` itself: how the Profiler/Planner/Dispatcher pipeline
  fits together today, and the conventions to follow when adding to it.

These describe *current* behavior. They don't carry design rationale,
alternatives considered, or implementation history — that lives in this
repo's local, git-ignored `dev-docs/` (not published, not available if
you're reading this from a clone or from crates.io).
