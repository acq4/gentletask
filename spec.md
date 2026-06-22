# gentletask

## Intent

Python's concurrency primitives are capable but cumbersome. `concurrent.futures`
gives you threads and results; it does not give you a way to stop a hierarchy
of running tasks, propagate that stop signal inward, or know — when something
goes wrong three threads deep — what your program was actually trying to do
when it failed.

`gentletask` is an attempt to fill those gaps without adding complexity that
isn't earned. A gentle task doesn't spin up a thread it doesn't need. It
communicates simply and honestly. When you ask it to stop, it stops — and it
asks its children to stop too, then waits for them. It carries a narrative of
meaning across every boundary it crosses so that your logs tell a story instead
of a list of disconnected events.

Two problems, two primitives:

- **Stopability.** A `Task` can be stopped cooperatively. Stop signals propagate
  down through hierarchies. We provide replacement blocking primitives (`sleep`,
  `Queue`, `Event`) to respect the stop signal, so you don't have to instrument
  every wait site manually.

- **Semantic context.** A `SemanticStack` carries labeled context — what your
  program is doing, not just where it is — across thread and process boundaries.
  The module singleton is called `throughline`: a continuous thread of meaning
  running through all your concurrent execution.

---

## `SemanticStack`

A context-local stack of *frames*, where each frame is an ordered collection of
key/value pairs. The stack lives in a single `ContextVar`. Internally, each
frame is stored as a `tuple[tuple[str, Any], ...]` — structurally immutable,
no defensive copying needed. `frames()` returns freshly constructed dicts, so
mutating them has no effect on the stack.

```python
class SemanticStack:
    def __init__(self, name: str = "semantic_stack", *, required: Iterable[str] = ()): ...
    def __call__(self, **kwargs) -> ContextManager[None]: ...
    def get(self, key: str, default: Any = None) -> Any: ...
    def collect(self, key: str) -> tuple[Any, ...]: ...
    def walk(self, fn: Callable[[dict[str, Any]], Any]) -> tuple[Any, ...]: ...
    def frames(self) -> tuple[dict[str, Any], ...]: ...
    def snapshot(self) -> "SemanticSnapshot": ...
    def restore(self, snapshot_or_frames) -> ContextManager[None]: ...

class SemanticSnapshot:
    def frames(self) -> tuple[dict[str, Any], ...]: ...
```

### `__call__(**kwargs)`

Returns a context manager. On enter, appends a new frame. On exit, removes it.
The `as` clause yields `None`. Nesting is supported and expected.

```python
with throughline(name="calibrate"):
    do_work()
```

### Required keys

A stack may be constructed with a list of `required` keys. Every frame entered
via `__call__` must supply them, or a `ValueError` is raised. `required` is a
floor, not a schema — extra keys are always allowed.

```python
labeled = SemanticStack(required=("name",))
with labeled(name="calibrate", detail="x-axis"):  # ok — extra keys welcome
    ...
with labeled(detail="x-axis"):                     # ValueError: missing "name"
    ...
```

### `get(key, default=None)`

Returns the value of `key` from the **innermost** frame that contains it, or
`default`.

### `collect(key)`

Returns a tuple of all values for `key`, outermost-first, skipping frames that
don't contain it.

```python
throughline.collect("name")  # ("acquisition", "calibrate")
```

### `walk(fn)`

Applies `fn` to each frame dict, outermost-first, returns a tuple of results.
`fn` receives a freshly constructed dict. Results are kept as-is — filter
separately if needed.

```python
throughline.walk(lambda f: f.get("name", "?"))
```

### `frames()`

Returns the full stack as a tuple of freshly constructed dicts, outermost
first.

### `snapshot()` / `restore()`

`snapshot()` captures the current stack state as a pure-data `SemanticSnapshot`
— just the frames, with no reference back to the stack. A snapshot is therefore
picklable when its frame values are picklable, and `SemanticSnapshot.frames()`
exposes the captured frames as fresh dicts (outermost-first, mirroring
`SemanticStack.frames()`) for callers that want to serialize them.

`stack.restore(snapshot_or_frames)` is a context manager that installs the given
frames onto *that stack* for the duration of its block, then resets. It accepts
either a `SemanticSnapshot` or an iterable of frame dicts (so the output of
`frames()` — a *list* of dicts after a serialization round-trip, or a tuple —
can be restored directly). Restoration is a **replay** of already-validated
frames, so it **bypasses** the `required`-key validation `__call__` enforces:
the frames may have come from a stack with different required keys, or from
another process entirely, and are not re-validated. The semantics of restoring
into a thread that already has frames (they are hidden, not merged) are defined
by tests, not by fiat.

---

## Two stacks: `throughline` and the task stack

The library keeps two `SemanticStack` singletons, each with a single, declared
purpose:

```python
from gentletask import throughline
# throughline = SemanticStack("throughline", required=("name",))
# _task_stack = SemanticStack("_task_stack", required=("task",))  # private
```

- **`throughline`** is the public, human-readable narrative. It requires a
  `name` on every frame and carries nothing else by convention — this is what
  `task_chain()` and `ThroughlineNameFilter` read, and what shows up in logs.
- **`_task_stack`** is private to the module. It requires a `task` on every
  frame and exists only to track which `Task` is running; `current_task()`
  reads from it. Keeping the task object off the throughline means the
  narrative stays clean and name-only.

Task code enters both at once via the `task_context` helper:

```python
from gentletask import task_context

@contextmanager
def task_context(task: Task, name: str) -> ContextManager[None]:
    with throughline(name=name), _task_stack(task=task):
        yield
```

Every `Task` implementation — built-in or custom — wraps its work in
`task_context(self, name)` so `current_task()` and the log chain both line up.

Independent `SemanticStack` instances can be constructed for isolated concerns,
but task-related helpers (`current_task()`, `task_context()`) are tied to these
two singletons.

---

## Thread transfer

`ThreadTask` uses `contextvars.copy_context()` at construction time — the new
thread inherits both stacks (the throughline narrative and the task chain)
automatically.

`WorkTask` snapshots *both* stacks (`throughline.snapshot()` and the private
task stack's snapshot) at `submit()` time, and restores both when the worker
picks up the job. The snapshots capture state *as it exists when `submit()` is
called* — this is intentional. A job inherits the context of the code that
caused it to be submitted, not the context of the worker that happens to execute
it. The worker thread is an implementation detail; it has no meaningful context
of its own.

```python
# job inherits "request_handler" — submit happens inside the frame
with throughline(name="request_handler"):
    worker.submit(process_image, args=(img,))

# job does not — submit happens after the frame exits
with throughline(name="request_handler"):
    job = build_job(img)
worker.submit(job.run)
```

---

## Process transfer

All values set by the gentletask library will be pickleable, and so long as your
code adheres to that restriction, the throughline can be copied across process
boundaries. `teleprox` makes use of this, as an example.

Because a `SemanticSnapshot` is pure data (no stack reference), it can be
pickled, sent to another process, and restored into that process's same-named
singleton — `throughline.restore(snapshot)` (or, equivalently, restoring a
serialized `frames()` payload, a list of dicts, via `throughline.restore(frames)`).
Restore bypasses required-key validation, so a frames payload built without the
throughline's required `name` key still replays cleanly.

---

## `SemanticStack` outside of tasks

A `SemanticStack` with no required keys accepts any keys and interprets none of
them — it is a general-purpose context-local stack. (The `throughline`
singleton, by contrast, requires `name`; build your own stack for arbitrary
keys.)

```python
from gentletask import SemanticStack

ctx = SemanticStack()
with ctx(operation="abc123", user="alice"):
    with ctx(operation="resize"):
        ctx.collect("operation")  # ("abc123", "resize")
        ctx.get("user")           # "alice"
        ctx.frames()
        # ({"operation": "abc123", "user": "alice"},
        #  {"operation": "resize"})
```

---

## `Task` Protocol

A `Task` is a structural protocol — implement it in any class. No base class
required.

```python
@runtime_checkable
class Task(Protocol):
    is_done: bool
    is_stopped: bool
    result: Any          # property: calls wait()

    def wait(self, timeout: float | None = None) -> Any: ...
    def stop(self) -> None: ...
    def add_finish_callback(self, fn: Callable[[Any, BaseException | None], None]) -> None: ...
    def add_stop_callback(self, fn: Callable[[], None]) -> None: ...
    def remove_stop_callback(self, fn: Callable[[], None]) -> None: ...
    def detach(self) -> None: ...
```

`add_stop_callback(fn)` registers a zero-argument callback fired exactly once
when the task is first stopped; if the task is already stopped, `fn` runs
immediately. `remove_stop_callback(fn)` unregisters it. These hooks are how stop
propagation reaches blocking primitives without polling — a waiter registers a
callback that wakes it, then unregisters on exit (see *Stop propagation*). Every
`Task` must implement them so any task — built-in or custom — can be waited on
poll-free.

---

## `ThreadTask`

Runs a callable in a new daemon thread.

```python
task = ThreadTask(fn, args=(), kwargs=None, name=None, detach=False, on_finish=None, start=True)
# Alternately, as a decorator:
task = asynch(fn, name=None, detach=False, on_finish=None)(*args, **kwargs)
```

**Deferred start.** With `start=True` (default) the thread is launched at the
end of `__init__`. With `start=False` the thread is created but not launched;
the caller calls `.start()` to begin work. This lets a caller attach
finish/stop callbacks or connect signals BEFORE any work runs, race-free:
construct, register callbacks, then `start()`. `.start()` is idempotent — a
second call (or calling it on a task already auto-started by `start=True`) is a
safe no-op. Context capture (`copy_context()`) and parent registration both
still happen at construction time, NOT at `start()`: the task inherits the
context of the code that *created* it, and registers in the parent's child set
immediately so a parent stop reaches even a not-yet-started child. `asynch()` is
a launcher and always starts immediately.

**Context inheritance.** Uses `contextvars.copy_context()` so the thread starts
with the caller's stack state (both stacks). The task then enters its own frame
on each via `task_context`:

```python
with task_context(self, self.name):  # name -> throughline, self -> task stack
    fn(*args, **kwargs)
```

**Child tracking.** Children are registered as weak refs. `stop()` cascades to
all registered children. When a task finishes — normally or by exception — it
stops any still-running children unless they were explicitly detached.

**`stop(reason=None)`.** Requests a cooperative stop. The optional `reason` is a
diagnostic string recorded by the *first* stop only (a later `stop()` is a no-op
and does not overwrite it), exposed via the `stop_reason` property. The reason
cascades to children (`child.stop(reason)`) and is carried into the `Stopped`
that the stop-aware primitives raise, so logs can record why work was stopped. A
reason-less `stop()` yields a message-less `Stopped()`, exactly as before.

**`detach()`.** Removes this task from its parent's stop propagation. A
subsequent `parent.stop()` will not cascade here.

**`wait(timeout=None)`.** Blocks until the task is done, parked on a per-task
`Condition` rather than a poll loop — `_finish()` notifies it directly. If
called from inside another task, `wait` registers a stop callback on that parent
so a `parent.stop()` wakes the wait immediately; it then calls
`self.stop(parent.stop_reason)` and raises `Stopped` carrying the parent's
reason. With a `timeout`, a deadline that elapses before the task is done raises
`Timeout` — `wait` never *returns* to signal a timeout, so a returned value
(including `None`) always means the task finished. `timeout=None` waits forever
and never times out; `timeout=0` raises `Timeout` immediately unless already
done. The `result` property waits without a timeout, so it never raises
`Timeout`.

**`__del__` cleanup.** A `ThreadTask` garbage-collected before `wait()` is
called automatically calls `stop()` on itself and its children.

---

## `asynch` and `synch`

`asynch(fn, ...)` returns a launcher: calling it starts `fn` in a `ThreadTask`
and returns the task. It is the asynchronous wrapper.

`synch(fn)` returns a *synchronous* version of `fn` that always yields a
concrete value. It flattens two layers of asynchrony:

- **The `asynch` wrapper.** If `fn` was produced by `asynch()`, it is de-wrapped
  to the original callable, so the work runs inline in the current thread with
  no extra `ThreadTask`.
- **A returned task.** When the (de-wrapped) callable is invoked and returns a
  value that implements the `Task` protocol, `synch` waits for that task and
  returns its result instead of the task itself.

A plain callable returning a plain value is simply called and its value
returned. `synch` is therefore safe to apply regardless of whether a function
was asynch-wrapped or returns a task — useful at call sites that accept either
form and just need to run the work and get a value.

```python
job = asynch(process)          # job(...) -> ThreadTask
result = synch(job)(img)       # de-wraps, runs process(img) inline, returns value
result = synch(process)(img)   # plain function: just called, value returned

def schedule(img):             # a function that hands back a task
    return worker.submit(process, (img,))
result = synch(schedule)(img)  # waits for the WorkTask, returns its result
```

---

## `WorkTask`

Returned by `WorkerThread.submit()`. Represents one job queued to a long-lived
worker thread.

**Context snapshot.** At submit time, snapshots both stacks. When the worker
picks up the job it restores both snapshots, then enters its own frame on each
via `task_context`:

```python
with throughline.restore(throughline_snapshot), _task_stack.restore(task_snapshot):
    with task_context(self, self.name):
        fn(*args, **kwargs)
```

**`stop()`.** Sets `is_stopped`. If the job hasn't started, it is skipped. If
it is running, stop propagates to child tasks via the normal mechanism.  The
worker thread itself is not interrupted — it is long-lived and not owned by any
single task.

**`wait(timeout=None)`.** Same poll-free wake-on-done / wake-on-parent-stop
semantics as `ThreadTask`.

---

## `Promise`

A `Task` with **no thread and no body**, completed *externally*. `ThreadTask`
and `WorkTask` are body-driven — a callable runs and its return value (or
exception) finishes the task. A `Promise` instead represents a result that some
already-existing producer will finish: a hardware monitor thread, a socket-reply
reader, a GUI callback, a lock loop. Wrapping those in a `ThreadTask` would mean
a useless parking thread per result; a `Promise` is just the shared completion
state. It otherwise participates fully in the Task protocol and the stop
hierarchy.

```python
class Promise(_TaskCore):
    def __init__(self, name: str | None = None, *, on_finish=None): ...
    def resolve(self, value: Any = None) -> None: ...   # complete successfully
    def fail(self, exc: BaseException) -> None: ...      # complete with exception
    def stop(self, reason: str | None = None) -> None: ...  # complete with Stopped
```

**Construction.** Registers with the creating task (`_register_with_parent`) so
a parent `stop()` cascades here. Spawns **no thread**. `fn` is unused, so the
name falls back to `"Promise"`.

**`resolve(value)` / `fail(exc)`.** Complete the promise via `_finish`, waking
waiters poll-free and firing finish callbacks with `(value, None)` or
`(None, exc)`. Both are **idempotent**: the first completion (resolve, fail, or
stop) wins; later calls are no-ops.

**`stop(reason=None)`.** A stopped promise has no body to raise `Stopped`, so it
must complete itself or its waiters hang. `stop()` first calls
`super().stop(reason)` — recording the reason, setting `is_stopped`, firing stop
callbacks (so the external producer can abort its side-effects), and cascading to
children — then, if the promise is still incomplete (a stop callback may have
already resolved/failed it), completes it with `Stopped` carrying that reason.
Idempotent.

**`wait(timeout=None)`.** Inherited: returns the resolved value, re-raises a
failed exception, or raises `Stopped` for a stopped promise, with the same
poll-free wake-on-done / wake-on-parent-stop semantics as `ThreadTask`.

---

## `MultiTask`

A bodyless, threadless `Task` that aggregates several already-running tasks into
one waitable unit and completes when **all** of its children complete. Like
`Promise` it spawns no thread and has no body; it is driven entirely by its
children's finish callbacks. Use it to wait for a group of tasks together,
collect their results in order, surface their combined errors, and stop them as
a unit. It participates fully in the Task protocol and the stop hierarchy.

```python
class MultiException(Exception):
    def __init__(self, message: str, exceptions): ...   # .exceptions = list(exceptions)

class MultiTask(_TaskCore):
    def __init__(self, tasks, name: str | None = None, *, on_finish=None): ...
    @property
    def tasks(self) -> tuple[Task, ...]: ...            # the children, in order
    def stop(self, reason: str | None = None) -> None: ...  # stop all children, then complete
```

**Construction.** Stores the children, registers with the creating task
(`_register_with_parent`) so a parent `stop()` cascades through here to the
children, and spawns **no thread**. `fn` is unused, so the name falls back to
`"MultiTask"`. Pre-sizes per-child result/exception slots and a `remaining`
counter initialized to the child count, then registers a per-index finish
callback on each child via `functools.partial(self._child_finished, i)`.

**Construction-time already-done race.** `add_finish_callback` fires
**immediately** when the child is already finished, so a child may call
`_child_finished` *during* the registration loop — before the other callbacks
are registered. Initializing `remaining` to the full child count up front and
decrementing once per callback makes the "all done" check (`remaining == 0`)
correct regardless of when each callback fires, including the all-already-done
case where every callback fires synchronously inside the loop. A zero-child
`MultiTask` completes immediately with an empty result list.

**`_child_finished(index, result, exc)`.** Under the lock, records the child's
`result`/`exc`, decrements `remaining`, and — only when it reaches zero and the
task is not already done — snapshots the results/exceptions. The completion
decision and `_finish` happen **outside the lock** (a finish callback may take
other locks; matching `_TaskCore` conventions), guarded by `is_done` so a
concurrent `stop()` that already completed the task wins. Completion rule:

- no child exceptions → `_finish(result=list(child_results))`;
- exactly one exception → `_finish(exc=that_exception)` (re-raised directly,
  unwrapped);
- two or more → `_finish(exc=MultiException("Multiple tasks failed", exceptions))`.

If a stop is propagating when the last child finishes — either this task was
stopped directly, or a grandparent stop reached the children first (a parent's
child set is unordered) — it completes with a single `Stopped` rather than
aggregating the children's `Stopped`s into a `MultiException`.

**`stop(reason=None)`.** Sets this task's own stop flag first via
`super().stop(reason)` (recording the reason, firing stop callbacks), then stops
every child. Setting the flag before cascading lets `_child_finished` see
`is_stopped` and report a single `Stopped` rather than a `MultiException` of the
children's `Stopped`s. Finally, if still incomplete (a child that does not
complete on stop), it self-completes with `Stopped` carrying the reason so
waiters never hang. Idempotent via `super().stop()`'s guard.

**`wait(timeout=None)`.** Inherited: returns the list of child results in task
order, re-raises the lone child exception or the aggregated `MultiException`, or
raises `Stopped` for a stopped `MultiTask`.

**`MultiException`.** Carries `.exceptions` (the failing children's exceptions,
in task order) and builds a combined message from *message* plus each child's
string form. Raised only when two or more children fail — a single failure
re-raises that child's own exception unwrapped.

---

## `WorkerThread`

A long-lived thread that serialises jobs.

```python
class WorkerThread:
    def __init__(self, name: str | None = None): ...
    def submit(self, fn: Callable, args=(), kwargs=None, *, name: str | None = None) -> WorkTask: ...
    def stop(self) -> None: ...  # drain queue and shut down
```

The worker loop:
1. Pull the next `WorkTask` from the queue.
2. If `task.is_stopped`, skip it (call callbacks with `Stopped` as the exception).
3. Otherwise restore the task's captured context (both stacks) and execute `fn`.
4. Finish the task: set result or exception, fire callbacks, set `is_done`.

---

## Stop propagation

`current_task()` returns the innermost `Task` on the private task stack, or
`None` if called outside any task.

```python
def current_task() -> Task | None:
    return _task_stack.get("task")
```

Stop propagation is **poll-free**: it is pushed, not polled. `stop()` sets the
stop flag and then fires every callback registered via `add_stop_callback`
(exactly once, on the first stop) before cascading to children. Each blocking
primitive, before it parks, registers a callback that wakes it — for `sleep` the
callback sets a private `threading.Event` it is waiting on; for `Queue`, `Event`,
and `wait` it notifies the `Condition` the primitive is parked on. So a stopped
task is woken the instant `stop()` runs, with latency independent of any polling
interval, and a fully idle wait consumes no CPU.

A small shared helper, `_stop_waker(wake)`, encapsulates the register-and-wake
pattern: it registers `wake` on the current task (firing it immediately if the
task is already stopped) and unregisters on exit. Callbacks fire *outside* the
task's lock so a waker may freely take the lock of the object it is waking
without risking deadlock.

Stop remains fully cooperative: tasks are never interrupted mid-line, only at
wait sites. Calling these primitives outside any task is safe — with no current
task there is nothing to stop, so they behave like their stdlib equivalents.

---

## Supporting primitives

**`Stopped`.** Exception raised when a task's stop has been requested. Carries
the optional `reason` passed to `stop()` as its message, so `str(Stopped("foo"))
== "foo"` and a reason-less `Stopped()` is empty. Every site that raises it for a
stopped current task — `check_stop`, `sleep`, `Queue.get`, `Event.wait`, `poll`
(via `check_stop`), and `Task.wait`'s parent-cascade — supplies that task's
`stop_reason`. Unwind normally; finally blocks run.

**`Timeout`.** Exception raised by `Task.wait(timeout=...)` when the deadline
elapses before the task is done. Dedicated to the wait deadline alone, so a
`None` return from `wait()` unambiguously means the task finished with a `None`
result. Subclasses the builtin `TimeoutError`, so `except TimeoutError` catches
it idiomatically while `except Timeout` catches only the wait deadline. A
parent-stop wakes `wait()` with `Stopped` instead, so a bounded `wait()`'s two
failure modes — deadline vs. stop — are never confused.

**`sleep(seconds)`.** Drop-in for `time.sleep`. Blocks on the stop signal
itself, so a stop unblocks it immediately (raising `Stopped`); otherwise it
returns once `seconds` have elapsed. No polling interval — the wait is exact.

**`check_stop()`.** Equivalent to `sleep(0)`. Use in tight loops to surrender at
a known-safe point.

**`Queue`.** Drop-in for `queue.Queue`, backed by its own `Condition` rather
than wrapping `queue.Queue`. `get(timeout=None)` raises `Stopped` while waiting
if the current task is stopped, woken at once by the stop signal.

**`Event`.** Drop-in for `threading.Event`, backed by its own `Condition`.
`wait(timeout=None)` raises `Stopped` while waiting if the current task is
stopped, woken at once by the stop signal.

**`poll(fn, *, interval, timeout)`.** Samples `fn` on `interval` (an arbitrary
predicate cannot be event-driven), but the inter-sample wait is poll-free with
respect to stop: a stop unblocks it immediately and raises `Stopped`.

---

## Logging integration

```python
def task_chain() -> tuple[str, ...]:
    return throughline.collect("name")
```

`ThroughlineNameFilter` injects `throughline.collect("name")` into every log
record as `throughline`. Wire it to any `logging.Handler` to get structured
ancestry in every log line.

---

## Custom tasks

Implement the `Task` Protocol in any class. Wrap your work in `task_context` at
execution time:

```python
with task_context(self, self.name):
    ...
```

This enters `self` on the task stack (so `current_task()` works) and
`self.name` on the throughline (so `task_chain()` and `ThroughlineNameFilter`
work). Because both stacks declare required keys, `task_context` always supplies
both — there is no "include only what you need" anymore; a task is named and
tracked, or it isn't a task.

A custom task must also implement `add_stop_callback` / `remove_stop_callback`
so the blocking primitives can wake on its stop without polling. The simplest
route is to reuse the built-in machinery by deriving from the same shared core
(or by keeping a list of stop callbacks and firing them, once, from `stop()`).
