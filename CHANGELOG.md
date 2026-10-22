# Changelog

All notable changes to `gentletask` are recorded here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While the project is in `0.x`, the public API is not yet frozen: breaking
changes bump the minor version and are called out under **Migration** below.

## Unreleased

### Added

- If the gentletask logger is set to DEBUG or lower, add a bunch of extra logging
  to help trace task relationships.

### Fixed

- **A child whose `stop()` fails no longer derails the cascade or its parent.**
  Stopping a child is a request made of that child, not part of the requester's
  own work, so a failure there is now logged at ERROR (with traceback, naming
  the child) and the cascade continues. Previously the exception propagated: the
  siblings after the failing child went un-stopped, and where the cascade runs
  from a parent's `finally` block — `ThreadTask._run`, `MultiTask.stop`, the
  `WorkerThread` runner — it also skipped the parent's own completion, so the
  parent never finished and every waiter on it blocked forever. A device proxy
  whose `stop()` raised on an unreachable device was enough to strand a whole
  task tree.
- **A hostile child no longer strands the cascade.** The guard covers the whole
  per-child body, not just the `stop()` call: the cascade reads `child.is_done`
  and formats a log line first, and a hand-rolled `Task` can make either raise.
  Previously that aborted the loop before the guarded call, stranding every
  sibling behind it — and in the `WorkerThread` runner it also killed the pump,
  which left the worker accepting jobs it would never run while `submit()` went
  on handing out tasks whose `wait()` blocked forever.
- Those completion sites now complete their task from a `finally`, so "a started
  task always completes" no longer depends on the stop cascade being
  exception-free. `MultiTask.stop` and `ManualTask.stop` extend that guard over
  `super().stop()` as well, since it runs the stop callbacks and the `_children`
  cascade: a callback raising `BaseException` escapes its own guard, and that
  must not cost the task its completion.
- `stop()` no longer requires the task that called it to have a `_name`. With
  DEBUG logging on, the line naming the requester read the attribute bare, so a
  hand-rolled `Task` as `current_task()` raised `AttributeError` out of `stop()`
  — taking the self-completion with it.

## [0.7.0] - 2026-07-03

### Added

- **`wait=False` parameter on `Task.stop()`** — with `wait=True`, `stop()` blocks
  until the task has actually exited. A task that exits badly (any exception
  other than the `Stopped` it raises in response to this stop) re-raises that
  failure from `stop()`; the `Stopped` we asked for is swallowed. Honored on a
  redundant stop too, so a second `stop(wait=True)` still blocks until done.

## [0.6.1] - 2026-06-24

### Added

- **`Empty`** and **`Full`** — re-exports of `queue.Empty` and `queue.Full` so
  callers can catch the exceptions `Queue.get()` and `Queue.put()` raise without
  importing from `queue` directly.

## [0.6.0] - 2026-06-23

### Added

- **`raise_errors(task, message=...)`** — a lightweight helper that installs a
  daemon thread watching *task* and re-raises any failure (excluding `Stopped`)
  through the process's unhandled-exception hook, so fire-and-forget tasks
  never silently discard errors. *message* is a format string supporting
  `{name}` (task name), `{error}` (exception string), and `{stack}` (caller
  traceback at the time `raise_errors` was called).
- **`raise_errors=False` parameter on `ThreadTask.__init__`, `asynch()`, and
  `detach()`** — accepts `False` (off, default) or a message string. Passing a
  string enables error surfacing with that message; `False` disables it.
  `child.detach(raise_errors="sensor {name!r} died: {error}")` detaches and
  installs the monitor in one call.

### Fixed

- `ThroughlineNameFilter` now walks the exception cause chain (`__cause__` →
  `__context__`, respecting `__suppress_context__`) when resolving the
  raise-site throughline. Previously, a wrapper exception raised with `raise W
  from original` carried no tag of its own, so the original's raise-site chain
  was silently dropped and the log record fell back to the (typically empty)
  chain active at the logging call. The fix mirrors how Python's own traceback
  printer traverses the exception chain.

## [0.5.0] - 2026-06-22

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

## [0.4.0] - 2026-06-22

### Changed

- **BREAKING (behavioral):** `Task.wait(timeout=...)` now raises a per-task
  `Timeout` exception (`task.Timeout`) when the deadline elapses before the
  task is done, instead of returning `None`. This frees `None` to mean what it
  says — the
  task finished and its result was `None` — so a returned value is never
  confusable with a timeout. A parent-stop still raises `Stopped`, so a bounded
  `wait()` now has three unambiguous outcomes: a returned value (finished),
  `Stopped` (a stop cascaded in), or `task.Timeout` (the deadline elapsed). The
  change applies uniformly to every task type, since they all share
  `_TaskCore.wait` (`ThreadTask`, `WorkTask`, `Promise`, `MultiTask`).

### Added

- Per-task `Timeout` exceptions, reachable as `task.Timeout` (each carrying
  `.task`). `wait(timeout=...)` raises `self.Timeout`, so `except
  some_task.Timeout` catches only *that* task's deadline — not a timeout that
  escaped an inner `wait` and propagated up as the task's result. This avoids a
  retry loop mistaking an inner task's propagated timeout for its own deadline
  and spinning forever. There is no public module-level timeout type: catch the
  specific `task.Timeout`, or the builtin `TimeoutError` (which every
  `task.Timeout` subclasses) for a broader catch.

### Migration

- Callers that treated a `None` return from `wait(timeout=...)` as "timed out"
  must now catch `task.Timeout` (or the builtin `TimeoutError`):

  ```python
  # Before
  result = task.wait(timeout=10)
  if result is None:        # ambiguous: timeout OR a real None result
      handle_timeout()

  # After
  try:
      result = task.wait(timeout=10)
  except task.Timeout:
      handle_timeout()
  ```

- Callers that checked `task.is_done` after `wait()` to disambiguate can drop
  that check — reaching the line after `wait()` now means the task is done, and
  `result` is its real value (possibly `None`).
- `timeout=None` is unchanged: it waits forever and never times out. The
  `result` property waits without a timeout, so it never times out.
- `timeout=0` now raises `task.Timeout` immediately unless the task is already
  done (previously it returned `None`). Use `task.is_done` for a non-blocking
  check.
- In a retry loop that should continue only while a task is still running,
  guard on the *per-task* class so a propagated inner timeout is not mistaken
  for the loop's own deadline:

  ```python
  while True:
      try:
          task.wait(timeout=20)
          break
      except task.Timeout:   # not the broader `except TimeoutError`
          continue           # only OUR 20s wait elapsed; keep waiting
  ```

  With a broader `except TimeoutError`, a timeout raised inside the task's body
  (e.g. an inner `wait(timeout=...)` the body forgot to catch) is re-raised by
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
