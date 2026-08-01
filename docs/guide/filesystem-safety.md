# Filesystem safety

Copying between different filesystems (e.g. your internal drive to a
FAT32/exFAT USB drive, or to a network share) has several failure modes
that don't show up as normal I/O errors — a copy can report full success
while silently losing a file, or fail unpredictably partway through for
reasons that were knowable in advance. `file-engine` probes the
destination filesystem once per operation and checks for these before —
or, for case-insensitive collisions, as part of — the actual copy.

## Case-insensitive destination collisions

If the destination filesystem is case-insensitive (the default on
Windows and on most macOS setups; always the case for FAT32/exFAT) and
your source contains two entries whose names differ only by case (e.g.
`Report.txt` and `report.txt`), copying both would silently let one
overwrite the other with no error — a real, documented failure mode in
mainstream tools, not a theoretical concern. `file-engine` detects this
and rejects **both** colliding entries as a per-entry
`Error::CaseCollision` (governed by `ErrorStrategy`, same as any other
per-entry failure — see [error-handling.md](error-handling.md)) rather
than silently keeping one.

## Destination file-size limits

FAT32 destinations cap individual files at 4 GiB. A file over that limit
is rejected as `Error::FileTooLargeForDest` before any bytes are written,
instead of failing partway through with a truncated file on disk.

## Windows-reserved names

If the destination filesystem is NTFS, exFAT, or FAT32 — regardless of
which OS is doing the writing — Windows's reserved-character set
(`< > : " / \ | ? *`), reserved device names (`CON`, `NUL`, `COM1`, etc.,
matched case-insensitively, extension or not), and trailing dot/space in
a filename are all rejected as `Error::ReservedName`. This is
enforced even on macOS/Linux writing to one of these filesystems:
none of those restrictions are enforced by the on-disk format itself,
only by Windows's own driver — a Mac or Linux machine can otherwise write
a name that will fail (or behave unpredictably) the moment that same
drive is read on an actual Windows machine.

## exFAT write-integrity risk on macOS

Writing to exFAT from macOS has a known, precedented data-integrity
issue under heavy sequential writes (unrelated tooling — Bitcoin
Core — found and worked around actual data corruption, not just
metadata loss, tracing it to an incomplete `F_FULLFSYNC` implementation
in macOS's exFAT driver). Since `file-engine`'s batching engine does
exactly this kind of sustained sequential-write workload, copying to an
exFAT destination from macOS fails immediately, before anything is
written, with `Error::FilesystemIntegrityRisk`.

To proceed anyway:

```rust
engine.copy("src/", "dst/").allow_filesystem_integrity_risk(true).start()?;
```

This is the one filesystem-safety check with an explicit opt-out — it
isn't a property of any specific entry (every write to that destination
carries the same risk), so `ErrorStrategy` doesn't apply to it the way
it does to the checks above.

## `sync`'s timestamp comparison

FAT/exFAT destinations only store modification times to a 2-second
granularity. `sync`'s default diff strategy
(`DiffStrategy::SizeAndModifiedTime`) accounts for this automatically —
comparisons against such a destination use a tolerance matching what
that filesystem can actually represent, so a normal sync run doesn't
treat every file as changed just because the destination can't store the
precise mtime. No configuration needed; this is automatic based on the
detected destination filesystem.
