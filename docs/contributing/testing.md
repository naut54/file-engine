# Testing

## `-D warnings`

`.cargo/config.toml` sets `-D warnings` project-wide. Dead code, unused
imports, and similar lints are build errors, not warnings — and they're
often feature-combination-specific: code that's genuinely used under
`--all-features` can be dead under `operations` alone, or vice versa,
because a `#[cfg(...)]`-gated call site disappears while the code it
calls doesn't have a matching gate.

## The feature-combination sweep

Before considering a change done, run tests under more than one feature
set. At minimum:

```sh
cargo test --features operations
cargo test --features operations,permissions
cargo test --features operations,sync
cargo test --all-features
cargo check --features watch --tests   # watch doesn't imply operations
cargo check --no-default-features --tests
```

## Cross-compilation as a correctness check, not just a build check

Platform-specific code (`src/profiler/fs_caps/{unix,windows}.rs` in
particular) can't all be exercised on one development machine. Add the
target once —

```sh
rustup target add x86_64-unknown-linux-gnu x86_64-pc-windows-gnu
```

— then check (not just the default target):

```sh
cargo check --features operations --tests --target x86_64-unknown-linux-gnu
cargo check --features operations --tests --target x86_64-pc-windows-gnu
```

This has caught real bugs before landing (a `windows-sys` module path
that doesn't exist where the code assumed it did; a `#[cfg(unix)]`-only
test helper that was genuinely dead on Windows) — it's cheap and worth
running even for changes that don't look platform-specific, since the
dead-code lint alone can surface unrelated pre-existing gaps.

Cross-compiling only proves the code *compiles* for that target, not
that its runtime behavior is correct — there's no substitute for running
on the real platform when one is available.

## Prefer empirical verification over assumption

For anything platform- or filesystem-specific, verify the actual
behavior with a small real test rather than trusting documentation or
memory of how it "should" work — this codebase's history has repeatedly
found real APIs behaving differently than expected (`std::fs::copy`
already preserving permission bits, `create_dir_all` tolerating
concurrent races, exFAT accepting Windows-illegal filenames when written
from macOS). A standalone `cargo run` probe in a scratch directory is
usually enough; delete it once you've confirmed the behavior.

## Public API reachability isn't checked by `src/`'s own tests

`src/`'s unit/integration tests compile with `pub(crate)`-level access
to the crate — they can see and use anything, so they can't catch a
type that's `pub` in name but structurally unreachable from outside
(declared `pub` inside a module that itself isn't `pub`, or a `pub`
type never re-exported at any level up to the crate root).
`CompressFormat` was exactly this: a real public method
(`CompressBuilder::format()`) took it by value, but no external crate
could ever construct one — `cargo build`/`test`/`check` all stayed
clean the whole time. The only way to catch this class of bug is to
compile something as an actual external consumer: an example under
`examples/` (which does), or a doctest. When adding a new public type,
either use it from an example/doctest, or at minimum mentally trace its
full path from the crate root and confirm every module in between is
also `pub` (or re-exported).

## Real-world testing

`scripts/make-fixtures.sh` builds the trees for this, so the runs are
reproducible rather than improvised each time:

```sh
./scripts/make-fixtures.sh generate   # ./.fixtures (gitignored, ~5.4GB)
./scripts/make-fixtures.sh volume     # macOS: mount /Volumes/fetest
./scripts/make-fixtures.sh clean      # unmount and delete both
```

Sizes are overridable for a quick pass, as flags or the matching
environment variables (the flag wins if both are set):

```sh
./scripts/make-fixtures.sh generate --small-files 500 --single-mb 64
./scripts/make-fixtures.sh volume --volume-fs ExFAT --volume-gb 2
```

`--dir DIR` relocates the fixture directory (and the disk image inside
it) for every command. `--help` lists every flag with its default.

| Fixture | What it exercises |
| --- | --- |
| `many-small` | 15,000 × 30KB — batching, the per-file cost regime |
| `single-large` | one 3GB file — in-flight `EntryProgress` sampling; the only signal a lone streamed entry produces |
| `mixed` | both regimes at once — the dispatcher's calibration reordering, and `max(small, large)` |
| `empty-dirs` | 411 directories with no files beneath them — the pre-pass that creates them, and the per-directory cost term |
| `edge-cases` | zero-byte, the 256KB small/large boundary in both directions, Windows-reserved names, and (where the source volume can represent them) case-collision and NFC/NFD pairs |

**Copy onto a different filesystem, not just a different directory.** A
same-volume copy on APFS is a `clonefile`: 3GB completes in under a
millisecond, moves no bytes, and emits no byte progress at all. It cannot
exercise throughput, sampling, or estimation. The `volume` subcommand
mounts a disk image for this; `VOLUME_FS=ExFAT` is the one to use for
`edge-cases`, since exFAT is where Windows naming rules and the known
write-integrity risk actually apply (that copy needs
`--allow-integrity-risk` to get past the fatal pre-flight check).

Unit and integration tests catch most things, but this crate's most
consequential bugs so far (silently-missing empty directory subtrees,
Unicode normalization mismatches, a directory-creation pass that was
slow enough over USB to look like a hang) were only found by running the
tool against large, real, messy directory trees — not by any test
someone thought to write in advance. If you're changing something in the
copy/sync pipeline, a real run against a large tree (ideally onto a
different filesystem than your source) is worth doing before considering
the change verified, not just a nice-to-have.
