# file-engine

Async, cross-platform file operations engine for desktop apps and developer
tools: copy, move, analyze, watch, compress, and sync files, with progress
reporting and cooperative cancellation built in.

See [`docs/file-engine-design.md`](docs/file-engine-design.md) for the full
design document, including open design questions still being resolved.

## Status

Early scaffolding — API surface is not yet stable or fully implemented.

## Features

| Feature       | Enables                                   |
| ------------- | ------------------------------------------ |
| `operations`  | `copy`, `move_path` (default)              |
| `analyze`     | `analyze` (default)                        |
| `checksum`    | hashing on top of `analyze`                |
| `watch`       | filesystem event watching                  |
| `compress`    | `compress` / `decompress`                  |
| `permissions` | permission preservation on top of `operations` |
| `sync`        | `sync`                                     |
| `diagnostics` | `error-engine` integration                 |

## License

Licensed under the [MIT license](LICENSE).
