# Changelog

All notable changes to `gentletask` are recorded here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While the project is in `0.x`, the public API is not yet frozen: breaking
changes bump the minor version and are called out under **Migration** below.

## [0.4.0] - 2026-06-22

### Changed

- **BREAKING (behavioral):** `Task.wait(timeout=...)` now raises the new
  `Timeout` exception when the deadline elapses before the task is done,
  instead of returning `None`. This frees `None` to mean what it says — the
  task finished and its result was `None` — so a returned value is never
  confusable with a timeout. A parent-stop still raises `Stopped`, so a bounded
  `wait()` now has three unambiguous outcomes: a returned value (finished),
  `Stopped` (a stop cascaded in), or `Timeout` (the deadline elapsed). The
  change applies uniformly to every task type, since they all share
  `_TaskCore.wait` (`ThreadTask`, `WorkTask`, `Promise`, `MultiTask`).

### Added

- `Timeout` exception, raised by `Task.wait(timeout=...)` on a deadline. It
  subclasses the builtin `TimeoutError`, so `except TimeoutError` catches it
  idiomatically while `except gentletask.Timeout` catches only the wait
  deadline. Exported in `__all__`.
- Per-task `Timeout` subclasses, reachable as `task.Timeout` (each carrying
  `.task`). `wait(timeout=...)` raises `self.Timeout`, so `except
  some_task.Timeout` catches only *that* task's deadline — not a `Timeout` that
  escaped an inner `wait` and propagated up as the task's result. This avoids a
  retry loop guarding on `except Timeout` mistaking an inner task's propagated
  timeout for its own deadline and spinning forever.

### Migration

- Callers that treated a `None` return from `wait(timeout=...)` as "timed out"
  must now catch `Timeout` (or the builtin `TimeoutError`):

  ```python
  # Before
  result = task.wait(timeout=10)
  if result is None:        # ambiguous: timeout OR a real None result
      handle_timeout()

  # After
  try:
      result = task.wait(timeout=10)
  except Timeout:
      handle_timeout()
  ```

- Callers that checked `task.is_done` after `wait()` to disambiguate can drop
  that check — reaching the line after `wait()` now means the task is done, and
  `result` is its real value (possibly `None`).
- `timeout=None` is unchanged: it waits forever and never times out. The
  `result` property waits without a timeout, so it never raises `Timeout`.
- `timeout=0` now raises `Timeout` immediately unless the task is already done
  (previously it returned `None`). Use `task.is_done` for a non-blocking check.
- In a retry loop that should continue only while a task is still running,
  guard on the *per-task* class so a propagated inner timeout is not mistaken
  for the loop's own deadline:

  ```python
  while True:
      try:
          task.wait(timeout=20)
          break
      except task.Timeout:   # not bare `except Timeout`
          continue           # only OUR 20s wait elapsed; keep waiting
  ```

  With a bare `except Timeout`, a `Timeout` raised inside the task's body (e.g.
  an inner `wait(timeout=...)` the body forgot to catch) is re-raised by
  `task.wait()` and would be swallowed by the loop, spinning forever instead of
  surfacing the failure.

## [0.3.0] - 2026-06-16

### Fixed

- An exception unwinding out of a `throughline` block now carries the
  name-chain active at its raise site, so an error logged far above where it
  was raised — e.g. re-raised by `wait()` at the top level — still reports the
  throughline from the raise site rather than the shorter chain active at the
  logging call.

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
