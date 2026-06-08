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

class SemanticSnapshot:
    def restore(self) -> ContextManager[None]: ...
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

### `snapshot()` / `SemanticSnapshot.restore()`

`snapshot()` captures the current stack state. `restore()` is a context manager
that installs the snapshot for the duration of its block, then resets. The
semantics of restoring into a thread that already has frames are defined by
tests, not by fiat.

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
    def detach(self) -> None: ...
```

---

## `ThreadTask`

Runs a callable in a new daemon thread.

```python
task = ThreadTask(fn, args=(), kwargs=None, name=None, detach=False, on_finish=None)
# Alternately, as a decorator:
task = asynch(fn, name=None, detach=False, on_finish=None)(*args, **kwargs)
```

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

**`detach()`.** Removes this task from its parent's stop propagation. A
subsequent `parent.stop()` will not cascade here.

**`wait(timeout=None, interval=0.05)`.** Polls in 50 ms intervals. If called
from inside another task, checks whether that parent is stopped on each
interval; if so, calls `self.stop()` and raises `Stopped`.

**`__del__` cleanup.** A `ThreadTask` garbage-collected before `wait()` is
called automatically calls `stop()` on itself and its children.

---

## `WorkTask`

Returned by `WorkerThread.submit()`. Represents one job queued to a long-lived
worker thread.

**Context snapshot.** At submit time, snapshots both stacks. When the worker
picks up the job it restores both snapshots, then enters its own frame on each
via `task_context`:

```python
with throughline_snapshot.restore(), task_snapshot.restore():
    with task_context(self, self.name):
        fn(*args, **kwargs)
```

**`stop()`.** Sets `is_stopped`. If the job hasn't started, it is skipped. If
it is running, stop propagates to child tasks via the normal mechanism.  The
worker thread itself is not interrupted — it is long-lived and not owned by any
single task.

**`wait(timeout=None)`.** Same poll-and-check-stop semantics as `ThreadTask`.

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
`None` if called outside any task. The blocking primitives poll `current_task()`
on each interval and raise `Stopped` if that task has been stopped. This means
stop propagation is fully cooperative: tasks are never interrupted mid-line,
only at wait sites.

```python
def current_task() -> Task | None:
    return _task_stack.get("task")
```

Calling these primitives outside any task is safe — they behave like their
stdlib equivalents.

---

## Supporting primitives

**`Stopped`.** Exception raised when a task's stop has been requested. Unwind
normally; finally blocks run.

**`sleep(seconds, *, interval=0.05)`.** Drop-in for `time.sleep`. Polls in
`interval`-second chunks, raising `Stopped` if the current task is stopped.

**`check_stop()`.** Equivalent to `sleep(0)`. Use in tight loops where a sleep
interval would be inappropriate.

**`Queue`.** Drop-in for `queue.Queue`. `get(timeout=None)` raises `Stopped`
while waiting if the current task is stopped.

**`Event`.** Drop-in for `threading.Event`. `wait(timeout=None)` raises
`Stopped` while waiting if the current task is stopped.

**`poll(fn, *, interval, timeout)`.** Polls `fn` in a loop with periodic
`check_stop()` calls.

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
