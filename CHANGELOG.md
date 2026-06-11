# Changelog

All notable changes to `gentletask` are recorded here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The 1.0.0 release marks the public API as stable; breaking changes from here on
bump the major version. The 0.x releases below predate that promise.

## [1.0.0] - 2026-06-11

### Changed

- **BREAKING:** Renamed `Promise` to `ManualTask`. The old name did not convey
  that it is a `Task`; the new name reflects that it is a `Task` completed
  *manually* via `resolve()` / `fail()`. The class is otherwise unchanged.
- **BREAKING:** Removed the `WorkTask` class. A worker job was just a `Task`
  completed by something external — the worker thread running its body — which
  is exactly what `ManualTask` already models. `WorkerThread.submit()` now
  returns a `ManualTask`; the worker holds the submitted callable and context
  snapshots internally and `resolve()`/`fail()`s the task when the body runs.
- **BREAKING (behavioral):** Stopping a *running* worker job now completes its
  task with `Stopped` immediately, so waiters wake at once; the body unwinds
  cooperatively in the background and its eventual completion is a no-op.
  Previously a stopped worker job's waiters blocked until the body had finished
  unwinding. (This matches how stopping any other `ManualTask` already behaved.)

### Removed

- Deleted `spec.md`; `README.md` is the single reference for the public API.

### Migration

- Replace `Promise` with `ManualTask` (constructor and API are identical).
- Replace `WorkTask` references with `ManualTask`. Code that called
  `WorkerThread.submit(...)` keeps working unchanged — only the returned type's
  name changed.

## [0.2.0] - 2026-06-10

### Added

- `MultiTask` and `MultiException` for aggregating several tasks into one
  waitable unit.
- `Promise`, a bodyless, externally-completed `Task`.
- Deferred `ThreadTask` start (`start=False`) and a pure-data `SemanticSnapshot`.
- Optional `reason` argument for `stop()`, cascaded to children and carried on
  the raised `Stopped`.

### Fixed

- Task completion is now atomic, fixing a race between concurrent completers,
  and stop-reason propagation was corrected.
- `ThroughlineNameFilter` no longer overwrites an existing `throughline` on a
  re-handled log record.
- `synch()` re-binds `self` for bound `asynch` methods.
- `sleep(inf)` waits until stopped instead of overflowing.

## [0.1.0] - 2026-06-09

### Added

- First PyPI release: `SemanticStack` / `throughline`, the stoppable `Task`
  hierarchy (`ThreadTask`, `WorkTask`, `WorkerThread`), `asynch`/`synch`, and the
  poll-free stop-aware primitives (`sleep`, `check_stop`, `poll`, `Queue`,
  `Event`).
