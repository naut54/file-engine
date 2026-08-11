#!/usr/bin/env bash
#
# Builds the directory trees used for the real-world runs that
# docs/contributing/testing.md asks for, plus (on macOS) a separate
# volume to copy *onto* — same-volume copies on APFS are satisfied by
# copy-on-write and move no bytes at all, so they can't exercise
# throughput, progress sampling, or time estimation.
#
# Run with --help for usage.

set -euo pipefail

# Every setting below is an environment variable with a matching long
# flag; the flag wins when both are given. Defaults are printed by
# --help, interpolated from here so the two can't drift apart.
FIXTURE_DIR="${FIXTURE_DIR:-.fixtures}"

# 15,000 x 30KB matches the shape that first exposed the batching
# pipeline's behaviour; 400MB clears any plausible small-file threshold.
SMALL_FILES="${SMALL_FILES:-15000}"
SMALL_DIRS="${SMALL_DIRS:-60}"
SMALL_KB="${SMALL_KB:-30}"
LARGE_MB="${LARGE_MB:-400}"
LARGE_COUNT="${LARGE_COUNT:-4}"
SINGLE_LARGE_MB="${SINGLE_LARGE_MB:-3000}"

VOLUME_NAME="${VOLUME_NAME:-fetest}"
VOLUME_GB="${VOLUME_GB:-8}"
# HFS+ gives a clean cross-device target for throughput and ETA runs.
# ExFAT is the one to use for the edge-cases tree: it's where Windows
# naming rules, case-insensitivity, and (on macOS) the known
# write-integrity risk actually get applied.
#   VOLUME_FS=ExFAT ./scripts/make-fixtures.sh volume
VOLUME_FS="${VOLUME_FS:-HFS+}"

say() { printf '\033[1m%s\033[0m\n' "$*"; }

# One template copied N times, rather than N reads from /dev/urandom:
# generating 15,000 files individually from the random device dominates
# the runtime of this script for no benefit, since these fixtures are
# about file *count* and *size*, not content.
make_small_tree() {
    local root="$1" count="$2" dirs="$3"
    local template="$root/.template"
    mkdir -p "$root"
    head -c "$((SMALL_KB * 1024))" /dev/urandom > "$template"

    local per_dir=$(( (count + dirs - 1) / dirs ))
    local made=0
    for d in $(seq 1 "$dirs"); do
        mkdir -p "$root/dir$d"
        for f in $(seq 1 "$per_dir"); do
            [ "$made" -ge "$count" ] && break
            cp "$template" "$root/dir$d/f$f.bin"
            made=$((made + 1))
        done
    done
    rm -f "$template"
}

# /dev/urandom rather than /dev/zero: zero-filled files can be stored
# sparsely or compressed, which would make a copy of them unrepresentative
# of moving the same nominal number of bytes of real data.
make_large_file() {
    local path="$1" mb="$2"
    dd if=/dev/urandom of="$path" bs=1m count="$mb" status=none
}

generate() {
    say "==> fixtures in $FIXTURE_DIR"
    mkdir -p "$FIXTURE_DIR"

    # Exercises the batching path and the per-file cost regime.
    say "  many-small: $SMALL_FILES x ${SMALL_KB}KB across $SMALL_DIRS dirs"
    make_small_tree "$FIXTURE_DIR/many-small" "$SMALL_FILES" "$SMALL_DIRS"

    # The case that reported no ETA at all before in-flight sampling:
    # one streamed entry, no completion to learn a rate from until the end.
    say "  single-large: 1 x ${SINGLE_LARGE_MB}MB"
    mkdir -p "$FIXTURE_DIR/single-large"
    make_large_file "$FIXTURE_DIR/single-large/huge.bin" "$SINGLE_LARGE_MB"

    # Both regimes at once — the shape the calibration reordering exists
    # for, and the one where `max(small, large)` actually matters.
    say "  mixed: $SMALL_FILES small + $LARGE_COUNT x ${LARGE_MB}MB"
    make_small_tree "$FIXTURE_DIR/mixed" "$SMALL_FILES" "$SMALL_DIRS"
    for i in $(seq 1 "$LARGE_COUNT"); do
        make_large_file "$FIXTURE_DIR/mixed/big$i.bin" "$LARGE_MB"
    done

    # Directories with no files anywhere beneath them are created by an
    # explicit pre-pass, not as a side effect of copying files into them —
    # this is the tree that catches them going missing, and the one where
    # the per-directory cost term dominates on slow media.
    mkdir -p "$FIXTURE_DIR/empty-dirs"
    for a in $(seq 1 10); do
        for b in $(seq 1 10); do
            mkdir -p "$FIXTURE_DIR/empty-dirs/a$a/b$b/c1/d1"
            mkdir -p "$FIXTURE_DIR/empty-dirs/a$a/b$b/c2"
        done
    done
    say "  empty-dirs: $(find "$FIXTURE_DIR/empty-dirs" -type d | wc -l | tr -d ' ') empty directories, nested 4 deep"

    # Pre-flight validation inputs. Every one of these is a real
    # historical bug class for this crate, not a hypothetical.
    say "  edge-cases:"
    local edge="$FIXTURE_DIR/edge-cases"
    mkdir -p "$edge"
    : > "$edge/zero-bytes.bin"
    # Straddles DEFAULT_SMALL_FILE_THRESHOLD (256KB, in
    # src/profiler/scan.rs — not the 8MB DEFAULT_MAX_BYTES_PER_BATCH,
    # which is the batch byte budget and a different number). Entries
    # exactly at the threshold classify as small, so these two must land
    # on opposite sides: `Planned` should report one more small file and
    # one more large file than the rest of this tree contributes.
    head -c 262144 /dev/urandom > "$edge/exactly-256kb.bin"
    head -c 262145 /dev/urandom > "$edge/just-over-256kb.bin"
    say "    zero-byte, 256KB small/large boundary pair"

    # The next two pairs can only exist if the *source* volume can
    # represent them. macOS on APFS is case-insensitive and normalizes
    # Unicode, so both collapse to a single file here — which is why they
    # are verified rather than assumed. Reporting them as skipped is the
    # point: a fixture that silently contains half of what it claims is
    # worse than one that says so.
    printf 'lower\n' > "$edge/README.md"
    printf 'upper\n' > "$edge/readme.md" 2>/dev/null || true
    if [ -e "$edge/README.md" ] && [ -e "$edge/readme.md" ] &&
        [ "$(cat "$edge/README.md")" != "$(cat "$edge/readme.md")" ]; then
        say "    case-collision pair (README.md / readme.md)"
    else
        rm -f "$edge/readme.md"
        say "    case-collision pair SKIPPED — source volume is case-insensitive"
    fi

    # NFC vs NFD spelling of the same name.
    local nfc nfd
    nfc="$edge/$(printf 'caf\xc3\xa9').txt"
    nfd="$edge/$(printf 'cafe\xcc\x81').txt"
    printf 'nfc\n' > "$nfc"
    printf 'nfd\n' > "$nfd" 2>/dev/null || true
    if [ "$(find "$edge" -name '*.txt' | wc -l | tr -d ' ')" -ge 2 ]; then
        say "    unicode NFC/NFD pair"
    else
        say "    unicode NFC/NFD pair SKIPPED — source volume normalizes names"
    fi

    # Legal on macOS/Linux, rejected on Windows — so these exercise the
    # destination-side naming rules, not the source's.
    printf 'reserved device name\n' > "$edge/CON" 2>/dev/null || true
    printf 'trailing dot\n' > "$edge/trailing." 2>/dev/null || true
    printf 'trailing space\n' > "$edge/trailing " 2>/dev/null || true
    say "    windows-reserved names (CON, 'trailing.', 'trailing ')"

    say "==> done"
    du -sh "$FIXTURE_DIR"/* 2>/dev/null || true
}

# A separate filesystem is not a nicety here: on APFS a same-volume copy
# is a clonefile, which completes in microseconds regardless of size and
# emits no byte progress at all — correctly, but uselessly for testing.
volume() {
    if [ "$(uname)" != "Darwin" ]; then
        say "volume: macOS only."
        say "On Linux, use a loopback filesystem (needs root):"
        say "  truncate -s ${VOLUME_GB}G /tmp/$VOLUME_NAME.img"
        say "  mkfs.ext4 -q /tmp/$VOLUME_NAME.img"
        say "  sudo mount -o loop /tmp/$VOLUME_NAME.img /mnt/$VOLUME_NAME"
        say "Or point the destination at a real removable drive."
        exit 1
    fi

    if [ -d "/Volumes/$VOLUME_NAME" ]; then
        say "==> /Volumes/$VOLUME_NAME already mounted"
        df -h "/Volumes/$VOLUME_NAME" | tail -1
        return
    fi

    local image="$FIXTURE_DIR/$VOLUME_NAME.dmg"
    mkdir -p "$FIXTURE_DIR"

    # An existing image is re-attached, not rebuilt. `hdiutil create`
    # overwrites without asking, so recreating unconditionally would
    # silently discard a volume that already holds copied test data — and
    # would quietly ignore a --volume-fs/--volume-gb that no longer matches
    # what is actually on disk.
    if [ -e "$image" ]; then
        say "==> attaching existing image $image"
        say "    (delete it, or run 'clean', to rebuild at a different size or filesystem)"
    else
        say "==> creating ${VOLUME_GB}GB $VOLUME_FS volume $VOLUME_NAME"
        hdiutil create -size "${VOLUME_GB}g" -fs "$VOLUME_FS" -volname "$VOLUME_NAME" \
            -type UDIF -quiet "$image"
    fi

    hdiutil attach "$image" -quiet
    df -h "/Volumes/$VOLUME_NAME" | tail -1
}

clean() {
    if [ "$(uname)" = "Darwin" ] && [ -d "/Volumes/$VOLUME_NAME" ]; then
        say "==> detaching /Volumes/$VOLUME_NAME"
        hdiutil detach "/Volumes/$VOLUME_NAME" -quiet || true
    fi
    say "==> removing $FIXTURE_DIR"
    rm -rf "$FIXTURE_DIR"
}

usage() {
    cat <<EOF
Test fixtures for file-engine's real-world runs (docs/contributing/testing.md).

USAGE
    $(basename "$0") <command> [options]

COMMANDS
    generate    Build the fixture trees under the fixture directory.
    volume      Create and mount a scratch volume to copy *onto*. macOS only.
                A same-volume copy on APFS is a clonefile: it moves no bytes
                and emits no byte progress, so it cannot exercise throughput
                or time estimation. The disk image is stored in the fixture
                directory; the mount point is /Volumes/<volume-name>.
    clean       Unmount the volume (if mounted) and delete the fixture
                directory, disk image included.

GENERAL OPTIONS
    -d, --dir DIR         Fixture directory, also where the disk image lives.
                          Used by all three commands.  [\$FIXTURE_DIR: $FIXTURE_DIR]
    -h, --help            Show this help.

SIZE OPTIONS (generate)
        --small-files N   Small files in many-small/ and mixed/.
                                                [\$SMALL_FILES: $SMALL_FILES]
        --small-dirs N    Directories to spread them across.
                                                [\$SMALL_DIRS: $SMALL_DIRS]
        --small-kb N      Size of each small file, in KB.
                                                [\$SMALL_KB: $SMALL_KB]
        --large-mb N      Size of each large file in mixed/, in MB.
                                                [\$LARGE_MB: $LARGE_MB]
        --large-count N   Number of large files in mixed/.
                                                [\$LARGE_COUNT: $LARGE_COUNT]
        --single-mb N     Size of single-large/huge.bin, in MB.
                                                [\$SINGLE_LARGE_MB: $SINGLE_LARGE_MB]

VOLUME OPTIONS (volume)
        --volume-name N   Mount name, so /Volumes/<name>.
                                                [\$VOLUME_NAME: $VOLUME_NAME]
        --volume-gb N     Volume size, in GB.   [\$VOLUME_GB: $VOLUME_GB]
        --volume-fs FS    HFS+ or ExFAT.        [\$VOLUME_FS: $VOLUME_FS]
                          HFS+ is the clean cross-device target for throughput
                          and ETA runs. Use ExFAT for the edge-cases tree: it
                          is where Windows naming rules, case-insensitivity
                          and the macOS write-integrity risk actually apply
                          (that copy then needs --allow-integrity-risk).

EXAMPLES
    # Full set, roughly 5.4GB.
    $(basename "$0") generate

    # Small set, for a quick check.
    $(basename "$0") generate --small-files 500 --single-mb 64

    # Cross-device target, then copy onto it.
    $(basename "$0") volume
    cargo run --release --example basic_copy --features operations -- \\
        .fixtures/single-large /Volumes/fetest/dst

    # exFAT target for the validation paths.
    $(basename "$0") volume --volume-fs ExFAT --volume-gb 2

    $(basename "$0") clean
EOF
}

die() {
    printf 'error: %s\n\n' "$*" >&2
    usage >&2
    exit 2
}

COMMAND="${1:-}"
if [ $# -gt 0 ]; then
    shift
fi

case "$COMMAND" in
    -h | --help | help) usage; exit 0 ;;
    generate | volume | clean) ;;
    "") die "no command given" ;;
    -*) die "options must follow a command, e.g. '$(basename "$0") generate $COMMAND ...'" ;;
    *) die "unknown command '$COMMAND'" ;;
esac

# `--flag value` throughout, no `=` form and no short aliases beyond -d/-h:
# this is a handful of settings for a dev script, and a second accepted
# spelling per flag is more surface to get subtly wrong than it is
# convenience.
while [ $# -gt 0 ]; do
    case "$1" in
        -h | --help) usage; exit 0 ;;
        -d | --dir)       [ $# -ge 2 ] || die "$1 needs a value"; FIXTURE_DIR="$2";     shift 2 ;;
        --small-files)    [ $# -ge 2 ] || die "$1 needs a value"; SMALL_FILES="$2";     shift 2 ;;
        --small-dirs)     [ $# -ge 2 ] || die "$1 needs a value"; SMALL_DIRS="$2";      shift 2 ;;
        --small-kb)       [ $# -ge 2 ] || die "$1 needs a value"; SMALL_KB="$2";        shift 2 ;;
        --large-mb)       [ $# -ge 2 ] || die "$1 needs a value"; LARGE_MB="$2";        shift 2 ;;
        --large-count)    [ $# -ge 2 ] || die "$1 needs a value"; LARGE_COUNT="$2";     shift 2 ;;
        --single-mb)      [ $# -ge 2 ] || die "$1 needs a value"; SINGLE_LARGE_MB="$2"; shift 2 ;;
        --volume-name)    [ $# -ge 2 ] || die "$1 needs a value"; VOLUME_NAME="$2";     shift 2 ;;
        --volume-gb)      [ $# -ge 2 ] || die "$1 needs a value"; VOLUME_GB="$2";       shift 2 ;;
        --volume-fs)      [ $# -ge 2 ] || die "$1 needs a value"; VOLUME_FS="$2";       shift 2 ;;
        # The old interface took the directory as a bare second argument.
        # Rejected rather than silently accepted, so a stale invocation
        # fails loudly instead of writing several GB somewhere unintended.
        -*) die "unknown option '$1'" ;;
        *) die "unexpected argument '$1' — pass the fixture directory as --dir $1" ;;
    esac
done

case "$VOLUME_FS" in
    "HFS+" | ExFAT) ;;
    *) die "--volume-fs must be HFS+ or ExFAT, got '$VOLUME_FS'" ;;
esac

"$COMMAND"
