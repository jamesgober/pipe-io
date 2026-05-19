# Changelog

All notable changes to this project are documented here. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
the project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `REPS.md` section 4 locks the public API surface for the `0.2.0`
  design milestone. Modules: crate root, `source`, `stage`, `sink`,
  `batch`, `window` (std), `error`, `driver`. Builder methods,
  feature flags, MSRV, and runtime dependency posture are now
  binding for the design lock.
- `REPS.md` sections 3 (Scope), 10 (Testing requirements), and 12
  (Out of scope) filled in.
- `.dev/DESIGN.md` documents the major design trade-offs:
  synchronous core, hybrid pull/push via `Emit`, bounded buffers
  for backpressure, per-stage error policy, `Clock` trait for
  windowing, dual-driver model (`SyncDriver` and `ThreadedDriver`),
  `&'static str` stage identity, and the typed builder. Includes
  boundaries with `Iterator`, `futures::Stream`, `crossbeam`,
  `rayon`, distributed processors, and the sibling `log-io` crate,
  plus four consumer use cases (HiveDB ingest, log shipper, ETL
  load, metrics rollup).

### Changed

- `REPS.md` section 8 narrows the stability statement to point at
  the `1.0.0` policy and the `cargo-semver-checks` CI gate that
  arrives at `0.9.0`.

## [0.1.0] - 2026-05-12

### Added

- Initial repository scaffold.
- Apache-2.0 license, README, REPS specification stub, CI workflow,
  `.dev/` planning structure (DIRECTIVES, ROADMAP, PROMPTS).
- Crate name reserved on crates.io.

[Unreleased]: https://github.com/jamesgober/pipe-io/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/jamesgober/pipe-io/releases/tag/v0.1.0
