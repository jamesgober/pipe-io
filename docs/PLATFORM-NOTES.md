# pipe-io - Platform Notes

`pipe-io` has no platform-specific code: it uses `core`, `alloc`, and
(with the default `std` feature) `std` threads, channels, `Instant`,
and `io`. Behaviour is the same everywhere, but a few platform traits
show through. CI runs the full test suite on Linux, macOS, and Windows,
and builds the `no_std` core for two embedded targets.

## Timing granularity

- **`Instant` resolution** differs by OS (about 100 ns on Windows, finer
  on Linux and macOS). Windows and age triggers compare instants, so a
  window boundary that falls between two clock ticks is observed at the
  next tick. Use `ManualClock` when a test needs exact boundaries.
- **Sleep granularity** affects `try_map_retry` backoff and the idle
  tick. Short sleeps and timeouts can overshoot by up to the scheduler
  quantum, which on Windows is commonly 1-16 ms depending on the system
  timer resolution. Treat backoff delays and `idle_tick` intervals as
  lower bounds.
- **`SystemClock` cost.** Reading the clock per item is the dominant
  cost of a window stage on small items (see [`BENCH.md`](BENCH.md)).

## Threads

- `run_threaded` spawns one thread per segment with
  `std::thread::Builder`, named `pipe-io:<stage id>`. Thread names show
  up in debuggers and panic messages.
- If the OS refuses to create a thread (resource limits, sandboxing),
  the run returns `Error::Driver` after tearing down whatever started.
- Channel hand-off cost across a `buffer` edge depends on the OS
  scheduler and core topology; it is measured on Windows in
  [`BENCH.md`](BENCH.md). On machines under heavy load, threaded
  pipelines see more variance than single-threaded ones.

## Panics

- With `std`, teardown after a panic runs every user `flush`/`close`
  under `catch_unwind`, so a second panic cannot abort the process.
- With `panic = "abort"` in your profile there is no unwinding: the
  process ends at the first panic and no teardown runs. That is a
  property of the panic strategy, not of `pipe-io`.

## `no_std` targets

- The crate needs `alloc`. CI builds `thumbv7em-none-eabihf` (Cortex-M4F)
  and `thumbv6m-none-eabi` (Cortex-M0, no compare-and-swap atomics).
- On targets without pointer-sized atomics (`target_has_atomic = "ptr"`
  is false), `alloc::sync::Arc` does not exist, so `CancelHandle` and
  `Pipeline::cancel_handle` are not available. Everything else works;
  stop a stepped pipeline by not calling `step` again and calling
  `finish`.
- Without `std` there is no `catch_unwind`. A panic inside a user
  `flush`/`close` during teardown after another panic aborts, which is
  also the default panic behaviour of most embedded targets.

## Fuzzing on Windows

`cargo fuzz` on MSVC requires AddressSanitizer, whose runtime
(`clang_rt.asan_dynamic-x86_64.dll`) ships with Visual Studio but is not
on `PATH` by default. Add the MSVC tools `bin\Hostx64\x64` directory to
`PATH` before running a target; see [`../fuzz/README.md`](../fuzz/README.md).
CI fuzzes on Linux.
