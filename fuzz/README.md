# pipe-io fuzz targets

Fuzzing harnesses for `pipe-io`. Requires `cargo-fuzz` and a nightly
Rust toolchain (`libfuzzer-sys` does not build on stable).

## Install

```
cargo install cargo-fuzz
```

## Targets

| Target              | Invariant under test                                            |
|---------------------|-----------------------------------------------------------------|
| `batching_count`    | Count-triggered batching is lossless for arbitrary input bytes. |
| `batching_bytes`    | Byte-triggered batching is lossless for arbitrary string items. |
| `try_map_continue`  | `ErrorPolicy::Continue` never surfaces a run-level error.       |
| `window_tumbling`   | Tumbling windows preserve every input item under a fake clock.  |

## Run

```
cargo +nightly fuzz run batching_count
cargo +nightly fuzz run batching_bytes
cargo +nightly fuzz run try_map_continue
cargo +nightly fuzz run window_tumbling
```

Crash inputs land under `fuzz/artifacts/<target>/`. Seed corpora
can be added under `fuzz/corpus/<target>/` for guided fuzzing.

This crate is excluded from the main workspace (`workspace.exclude
= ["fuzz"]` in the parent `Cargo.toml`) and is not published.
