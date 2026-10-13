# pipe-io fuzz targets

Fuzzing harnesses for `pipe-io`. They need `cargo-fuzz` and a nightly
toolchain (`libfuzzer-sys` does not build on stable). The crate is
excluded from the main workspace (`workspace.exclude = ["fuzz"]`) and is
never published. `Cargo.lock` is committed so runs are reproducible.

## Install

```
cargo install cargo-fuzz
```

## Targets

| Target              | Invariant under test                                                                                       |
|---------------------|------------------------------------------------------------------------------------------------------------|
| `batching_count`    | Count-triggered batching is lossless for arbitrary input bytes.                                            |
| `batching_bytes`    | Byte-triggered batching is lossless for arbitrary string items.                                            |
| `try_map_continue`  | `ErrorPolicy::Continue` never surfaces a run-level error.                                                  |
| `window_policies`   | Every window policy with arbitrary (zero, tiny, huge) durations, caps, and arrival gaps either reports a config error or terminates with only non-empty windows that cover every item; tumbling and session windows partition the stream. |
| `policy_mix`        | Random error-policy mixes never lose or duplicate an item: each lands in the sink, the dead-letter sink (with its input), or the `Continue` drop count. |
| `reader_lines`      | The line readers never panic on arbitrary bytes and never return a line longer than the limit.            |

## Run

```
cargo +nightly fuzz run window_policies -- -max_total_time=60
```

CI runs every target for 60 seconds on Linux (the `fuzz` job).

On Windows (MSVC) libFuzzer needs AddressSanitizer, whose runtime DLL
ships with Visual Studio. Put its directory on `PATH` first, for example:

```
set PATH=%VCToolsInstallDir%bin\Hostx64\x64;%PATH%
```

Crash inputs land under `fuzz/artifacts/<target>/`. Seed corpora can be
added under `fuzz/corpus/<target>/`.
