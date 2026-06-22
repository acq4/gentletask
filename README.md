# gentletask

[![PyPI version](https://img.shields.io/pypi/v/gentletask.svg)](https://pypi.org/project/gentletask/)
[![Python versions](https://img.shields.io/pypi/pyversions/gentletask.svg)](https://pypi.org/project/gentletask/)

**Stoppable task hierarchies and a semantic-context "throughline" for Python concurrency.**

Python's concurrency primitives are capable but cumbersome. `concurrent.futures`
gives you threads and results; it does not give you a way to stop a hierarchy of
running tasks, propagate that stop signal inward, or know — when something goes
wrong three threads deep — what your program was actually trying to do when it
failed. `gentletask` fills those gaps without adding complexity that isn't
earned. A gentle task doesn't spin up a thread it doesn't need; when you ask it
to stop, it stops, asks its children to stop too, and waits for them. And it
carries a narrative of meaning across every boundary it crosses, so your logs
tell a story instead of a list of disconnected events.

Two problems, two primitives:

- **Stopability.** A `Task` can be stopped cooperatively. Stop signals propagate
  down through hierarchies. Replacement blocking primitives (`sleep`, `Queue`,
  `Event`, `poll`) respect the stop signal, so you don't have to instrument
  every wait site by hand.
- **Semantic context.** A `SemanticStack` carries labeled context — *what* your
  program is doing, not just *where* it is — across thread (and, when values are
  pickleable, process) boundaries. The module singleton is called `throughline`:
  a continuous thread of meaning running through all your concurrent execution.

---

## Installation

```bash
pip install gentletask
```

`gentletask` is pure Python, requires **Python >= 3.10**, and has **no runtime
dependencies**.

---

## Core concepts

### Task hierarchies and cooperative stopping

A **Task** is anything that satisfies the `Task` protocol — a unit of work that
can report whether it is `is_done` / `is_stopped`, be `wait()`-ed on, and be
asked to `stop()`. The built-in implementations are `ThreadTask` (runs a
callable in its own daemon thread), `WorkTask` (one job queued to a long-lived
`WorkerThread`), and `Promise` (no thread or body — completed externally by an
already-existing producer).

Tasks form a hierarchy automatically: when a task is created or waited on from
*inside* another running task, it registers as a **child** of that task. Calling
`stop()` on a parent cascades the stop to every child, and when a task finishes
(normally or by exception) it stops any still-running children — unless they
were explicitly `detach()`-ed.

Stopping is **cooperative**: a task is never interrupted mid-statement. Instead,
the stop-aware blocking primitives (`sleep`, `check_stop`, `Queue.get`,
`Event.wait`, `poll`, and `Task.wait`) notice the stop at their wait site and
raise `Stopped`, which unwinds normally so your `finally` blocks run.

`stop()` takes an optional `reason` string for diagnostics. The first stop on a
task records its reason (a later stop is a no-op and does not overwrite it),
which then travels into the `Stopped` exception raised at the wait site — so
`str(exc)` is the reason — and cascades along with the stop into children. A
plain `stop()` with no reason behaves exactly as before, yielding a reason-less
`Stopped()`. Read the recorded reason back via the `stop_reason` property.

Stop propagation is **poll-free**: `stop()` *pushes* a notification rather than
relying on a polling loop. Before a primitive parks, it registers a zero-arg
callback via `add_stop_callback`; `stop()` fires those callbacks (exactly once,
on the first stop) to wake the waiter the instant it is requested. An idle wait
consumes no CPU, and stop latency is independent of any polling interval.

### The throughline / semantic stack

A `SemanticStack` is a context-local stack of *frames*, where each frame is an
ordered collection of key/value pairs. It is backed by a single `ContextVar`,
and each frame is stored as an immutable `tuple[tuple[str, Any], ...]`, so no
defensive copying is needed and returned dicts are always fresh.

The library keeps two `SemanticStack` singletons, each with one declared job:

- **`throughline`** — the public, human-readable narrative. It requires a `name`
  on every frame and, by convention, carries nothing else. `task_chain()` and
  `ThroughlineNameFilter` read this, and it is what shows up in your logs.
- **`_task_stack`** — private to the module. It requires a `task` on every frame
  and exists only to track which `Task` is running; `current_task()` reads it.
  Keeping the task object off the throughline keeps the narrative clean and
  name-only.

Task code enters both stacks at once through the `task_context(task, name)`
helper. Context travels across thread boundaries automatically: `ThreadTask`
copies the calling context with `contextvars.copy_context()`, while `WorkTask`
*snapshots* both stacks at `submit()` time and restores them when the worker
picks up the job — so a job inherits the context of the code that caused it to
be submitted, not the worker's.

### `asynch` and `synch`

These are inverses that let a single call site choose how a function runs.

- **`asynch(fn, ...)`** returns a *launcher*: calling the launcher starts `fn`
  in a new `ThreadTask` and returns the task immediately.
- **`synch(fn)`** returns a *synchronous* version that always yields a concrete
  value. It flattens two layers of asynchrony: if `fn` was produced by
  `asynch()`, it is de-wrapped to the original callable so the work runs inline
  (no extra thread); and if the call returns something implementing the `Task`
  protocol, `synch` waits for that task and returns its result instead of the
  task. A plain function returning a plain value is simply called — so `synch`
  is safe to apply either way.

---

## Quick start

```python
import time
from gentletask import ThreadTask, sleep, Stopped

def counter(progress):
    n = 0
    while True:
        n += 1
        progress.append(n)
        sleep(0.02)  # stop-aware: raises Stopped when the task is stopped

progress = []
task = ThreadTask(counter, args=(progress,), name="counter")

time.sleep(0.1)   # let it tick a few times
task.stop()       # request a cooperative stop

try:
    task.wait()
except Stopped:
    print("counter stopped after", len(progress), "ticks")

print("is_stopped:", task.is_stopped)
```

---

## Guide

### `SemanticStack` basics

Each `with` block pushes a frame; leaving the block pops it. `get(key)` returns
the **innermost** value, `collect(key)` returns **all** values outermost-first
(skipping frames without the key), and `frames()` hands back fresh dicts.

```python
from gentletask import SemanticStack

ctx = SemanticStack()
with ctx(operation="abc123", user="alice"):
    with ctx(operation="resize"):
        print(ctx.get("operation"))      # "resize" (innermost)
        print(ctx.get("user"))           # "alice" (from the outer frame)
        print(ctx.collect("operation"))  # ('abc123', 'resize')
        for f in ctx.frames():
            print(f)
        # {'operation': 'abc123', 'user': 'alice'}
        # {'operation': 'resize'}
```

A stack can require certain keys on every frame. `required` is a floor, not a
schema — extra keys are always welcome.

```python
labeled = SemanticStack(required=("name",))

with labeled(name="ok", extra="also fine"):   # extra keys welcome
    ...

labeled(extra="but no name")                  # raises ValueError: missing "name"
```

### Snapshots

`snapshot()` captures the current frames as a pure-data `SemanticSnapshot` (just
the frames, no stack reference); `stack.restore(snapshot)` is a context manager
that reinstalls them onto that stack for a block. This is the machinery that
carries context onto worker threads.

```python
from gentletask import throughline

with throughline(name="request_handler", request_id="r-42"):
    snap = throughline.snapshot()

print(throughline.collect("name"))   # () — back outside, the stack is empty

with throughline.restore(snap):
    print(throughline.collect("name"), throughline.get("request_id"))
    # ('request_handler',) r-42
```

Because a snapshot is plain data, it is picklable (when its values are) and can
cross both thread and process boundaries. `restore()` also accepts a raw
iterable of frame dicts — e.g. `snap.frames()` after a serialization round-trip
returns a *list* of dicts — and replays already-validated frames, so it bypasses
the `required`-key check that `__call__` enforces:

```python
import pickle

payload = pickle.dumps(snap)          # send to another process...
revived = pickle.loads(payload)       # ...and restore into the same singleton
with throughline.restore(revived):
    print(throughline.get("request_id"))   # r-42

# Or restore a serialized frames() payload (a list of dicts) directly:
with throughline.restore([{"name": "request_handler", "request_id": "r-42"}]):
    ...
```

### `ThreadTask`

`ThreadTask` runs a callable in a new daemon thread. `wait()` blocks and returns
the result (or re-raises the worker's exception); `result` is shorthand for
`wait()`.

```python
from gentletask import ThreadTask

t = ThreadTask(lambda: 6 * 7)
print(t.wait())     # 42
print(t.is_done)    # True

# Exceptions surface through wait()
def boom():
    raise ValueError("kaboom")

failed = ThreadTask(boom)
try:
    failed.wait()
except ValueError as e:
    print("caught:", e)
```

It accepts positional/keyword args, an explicit `name`, and an `on_finish`
callback invoked with `(result, exception)` when the task finishes.

```python
log_lines = []
t = ThreadTask(
    lambda x, y: x + y,
    args=(3,),
    kwargs={"y": 4},
    name="adder",
    on_finish=lambda result, exc: log_lines.append(f"adder finished -> {result}"),
)
print(t.wait())     # 7
print(log_lines)    # ['adder finished -> 7']
```

Pass `start=False` to create the thread without launching it, then call
`.start()` once you have wired up callbacks. This lets you attach finish/stop
callbacks (or connect signals) before any work runs, race-free. `.start()` is
idempotent — a second call, or calling it on a task already started by the
default `start=True`, is a safe no-op. Context and parent registration are
still captured at construction time, so a not-yet-started child is stopped if
its parent stops.

```python
t = ThreadTask(work, start=False)
t.add_finish_callback(on_done)   # registered before any work runs
t.start()
```

### `asynch` and `synch`

```python
from gentletask import asynch, synch, ThreadTask

add = asynch(lambda x, y: x + y, name="async-add")
task = add(10, 20)            # starts a ThreadTask
print(task.wait())           # 30

def process(x):
    return x * 10

job = asynch(process)                 # job(...) -> ThreadTask
print(synch(job)(5))                  # 50 — de-wrapped, runs inline, no thread
print(synch(process)(5))              # 50 — plain function passes through

def schedule(x):                      # a function that hands back a task
    return ThreadTask(lambda: process(x), name="scheduled")

print(synch(schedule)(5))             # 50 — synch waits for the returned task
```

### Cooperative stop and stop-aware primitives

`sleep`, `check_stop`, `poll`, `Queue.get`, and `Event.wait` all check
`current_task()` at their wait site and raise `Stopped` when a stop has been
requested. Called outside any task, they behave like their stdlib equivalents.

```python
from gentletask import sleep, check_stop, Queue, Event, poll

def worker(q: Queue, done: Event):
    while True:
        check_stop()          # surrender at a known-safe point (like sleep(0))
        item = q.get()        # raises Stopped if stopped while waiting
        process(item)
        if done.wait(0.01):   # raises Stopped if stopped while waiting
            break

# poll() samples a predicate on `interval`, but the inter-sample wait is
# poll-free with respect to stop:
poll(lambda: connection.ready, interval=0.05, timeout=5.0)
```

### Stop cascades to children

Stopping a parent propagates to every child it started; when a task finishes it
also stops any still-running children.

```python
import time
from gentletask import ThreadTask, sleep, Stopped

child_ticks = []

def child():
    while True:
        child_ticks.append(1)
        sleep(0.02)

def parent():
    c = ThreadTask(child, name="child")
    c.wait()                  # parent blocks on the child

p = ThreadTask(parent, name="parent")
time.sleep(0.1)
p.stop()                      # cascades into the child
try:
    p.wait()
except Stopped:
    pass
print("child ticks frozen at", len(child_ticks))
```

`detach()` opts a child out of the cascade, so it keeps running after the parent
stops:

```python
def parent_detaching():
    c = ThreadTask(long_child, name="detached-child")
    c.detach()                # parent.stop() will NOT reach this child
    sleep(10)
```

### `WorkerThread` and `WorkTask` — serialized jobs with inherited context

A `WorkerThread` is a long-lived thread that runs submitted jobs one at a time.
`submit()` returns a `WorkTask` immediately and snapshots **both stacks at submit
time**, so the job inherits the context of the code that caused it — not the
worker's.

```python
from gentletask import WorkerThread, throughline, task_chain

worker = WorkerThread(name="io-worker")
captured = {}

def job(label):
    captured[label] = task_chain()
    return label.upper()

# Submitted inside a frame -> the job inherits it
with throughline(name="request_handler"):
    a = worker.submit(job, ("a",), name="job-a")
    a.wait()

# Submitted outside any frame -> the job sees only its own name
b = worker.submit(job, ("b",), name="job-b")
b.wait()

print(captured["a"])   # ('request_handler', 'job-a')
print(captured["b"])   # ('job-b',)
print(a.result)        # 'A'
worker.stop()          # already-queued jobs drain, then the thread shuts down
```

After `stop()`, the worker drains any jobs already queued and then exits;
further `submit()` calls raise `RuntimeError`.

### `Promise` — an externally-completed task

`ThreadTask` and `WorkTask` are *body-driven*: a callable runs and its return
value (or exception) finishes the task. But many real results are *externally
completed* — finished by a producer that already exists (a hardware monitor
thread, a socket-reply reader, a GUI callback, a lock loop) rather than by a
body of their own. Wrapping those in a `ThreadTask` would burn a useless parking
thread per result. A `Promise` is the missing primitive: a `Task` with **no
thread and no body**, completed externally via `resolve()` / `fail()`, that
otherwise participates fully in the Task protocol and the stop hierarchy.

```python
from gentletask import Promise, ThreadTask

# A producer hands out a Promise and finishes it later from wherever it runs.
target_reached = Promise(name="target-reached")

def monitor():  # some already-running producer
    ...                            # watch the hardware
    target_reached.resolve(123)    # complete it from outside — no new thread

ThreadTask(monitor, name="hw-monitor")
print(target_reached.wait())       # blocks poll-free until resolve() -> 123
```

`resolve(value)` completes it successfully; `fail(exc)` completes it with an
exception that `wait()` re-raises. Both are idempotent — the first completion
wins and later calls are no-ops. A `Promise` created inside a running task
registers as that task's child, so a parent `stop()` cascades to it; because a
stopped promise has no body to raise `Stopped`, `stop()` fires its stop
callbacks (letting the external producer abort its side-effects) and then
completes the promise with `Stopped` so its waiters never hang. `stop(reason)`
carries the reason into that injected `Stopped`.

### `MultiTask` — aggregate several running tasks into one

Sometimes you have several tasks already running and want to treat them as a
single waitable unit: block until they have **all** finished, collect their
results, surface their errors together, and stop them as a group. `MultiTask` is
that aggregator. Like `Promise` it is **bodyless and threadless** — it spawns no
thread, and is driven entirely by its children's finish callbacks.

```python
from gentletask import MultiTask, ThreadTask

a = ThreadTask(lambda: 1, name="a")
b = ThreadTask(lambda: 2, name="b")
c = ThreadTask(lambda: 3, name="c")

both = MultiTask([a, b, c], name="gather")
print(both.wait())   # blocks until all three finish -> [1, 2, 3] (task order)
```

`wait()` returns the list of child results **in task order**. Errors aggregate
by count:

- all children succeed → `wait()` returns the list of results;
- exactly one child fails → `wait()` re-raises **that** child's exception
  directly (no wrapping);
- two or more fail → `wait()` raises `MultiException`, whose `.exceptions` holds
  the failing children's exceptions in task order and whose message combines
  them.

`stop(reason)` stops every child and then this task, completing with `Stopped`
(carrying the reason) so waiters never hang even if a child does not complete on
stop. A `MultiTask` created inside a running task registers as that task's child,
so a parent `stop()` cascades through the `MultiTask` to all of its children.
Because `add_finish_callback` fires immediately for an already-finished child, a
`MultiTask` constructed over a mix of already-done and still-pending children
counts them all correctly and only completes once the last pending child
finishes.

### Logging integration

`ThroughlineNameFilter` injects `throughline.collect("name")` onto every log
record as `record.throughline`, giving each line its full task ancestry — across
thread boundaries, with no manual plumbing.

```python
import logging, sys
from gentletask import ThreadTask, ThroughlineNameFilter

logger = logging.getLogger("gentletask.demo")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(throughline)s | %(message)s"))
handler.addFilter(ThroughlineNameFilter())
logger.addHandler(handler)
logger.propagate = False

def calibrate():
    logger.info("measuring offset")

def acquire():
    logger.info("starting acquisition")
    ThreadTask(calibrate, name="calibrate").wait()
    logger.info("acquisition complete")

ThreadTask(acquire, name="acquisition").wait()
# ('acquisition',)               | starting acquisition
# ('acquisition', 'calibrate')   | measuring offset
# ('acquisition',)               | acquisition complete
```

### A custom `Task`

`Task` is a `runtime_checkable` structural `Protocol` — any object with the right
shape qualifies, no base class required. Wrap your work in `task_context(self,
name)` so `current_task()` and the log chain line up without touching either
stack directly. For *poll-free* stop, implement `add_stop_callback` /
`remove_stop_callback` and fire the registered callbacks exactly once from
`stop()`: those are the hooks the blocking primitives use to wake without
polling.

```python
import threading
from gentletask import task_context, sleep, Task

class CountdownTask:
    """A minimal hand-rolled Task that counts down on a background thread."""

    def __init__(self, n):
        self._n = n
        self.is_stopped = False
        self.is_done = False
        self._result = None
        # Internal plumbing uses plain threading primitives, NOT gentletask's.
        # These coordinate the task's own lifecycle and are read from outside
        # the task (e.g. wait() is usually called by a parent), so they must
        # not raise Stopped based on the caller's current_task().
        self._done = threading.Event()  # completion signal
        self._lock = threading.Lock()  # guards the stop-callback list
        self._stop_callbacks = []
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self):
        with task_context(self, "countdown"):
            try:
                while self._n > 0 and not self.is_stopped:
                    self._n -= 1
                    # The actual work waits with gentletask.sleep so a stop
                    # aborts it cooperatively (poll-free, raises Stopped).
                    sleep(0.01)
                self._result = "liftoff" if not self.is_stopped else "aborted"
            finally:
                self.is_done = True
                self._done.set()

    def wait(self, timeout=None):
        self._done.wait(timeout)
        return self._result

    @property
    def result(self):
        return self.wait()

    def stop(self):
        with self._lock:
            if self.is_stopped:
                return
            self.is_stopped = True
            callbacks, self._stop_callbacks = list(self._stop_callbacks), []
        for cb in callbacks:
            cb()

    def add_finish_callback(self, fn):
        self.wait()
        fn(self._result, None)

    def add_stop_callback(self, fn):
        with self._lock:
            if not self.is_stopped:
                self._stop_callbacks.append(fn)
                return
        fn()

    def remove_stop_callback(self, fn):
        with self._lock:
            if fn in self._stop_callbacks:
                self._stop_callbacks.remove(fn)

    def detach(self):
        pass

cd = CountdownTask(5)
print(isinstance(cd, Task))   # True
print(cd.wait())              # 'liftoff'
```

> **Note.** `runtime_checkable` only verifies attribute *presence*, not full
> signatures — so an object can pass `isinstance(obj, Task)` while still missing
> the behavior the primitives rely on. Implement all of the protocol members,
> especially the stop-callback hooks, for a task that participates fully in
> poll-free stopping.

### `threading` or `gentletask`? When to use which

The example above deliberately mixes the two, and the choice is not arbitrary.
The rule of thumb:

- Use the **`gentletask`** primitives (`sleep`, `Event`, `Queue`, `poll`,
  `check_stop`) for the **work a task performs** — the wait points that should
  abort with `Stopped` when *that* task is stopped. These are stop-aware: they
  consult `current_task()` and raise `Stopped` so a cooperative stop unwinds the
  work cleanly. Reach for these whenever you are blocking *inside* a task and
  want the block to be interruptible.
- Use the plain **`threading`** primitives (`threading.Event`, `Lock`,
  `Thread`, `queue.Queue`) for a task's **own lifecycle machinery** — completion
  signals, internal locks, the worker thread itself. This plumbing is read and
  driven from *outside* the running task (a parent calls `wait()`, `stop()` may
  fire from any thread), so it must **not** be stop-aware: a `gentletask.Event`
  used for `self._done` would raise `Stopped` if `wait()` happened to be called
  from some unrelated stopped task, corrupting the task's own bookkeeping.

In short: **`gentletask` primitives for interruptible work; `threading`
primitives for the scaffolding that makes a task a task.** The built-in
`ThreadTask` and `WorkTask` follow exactly this split internally — their
done/stop signaling is `threading`-based, while the work you hand them is free
to use the stop-aware primitives.

---

## API reference

### Exceptions

| Name | Description |
| --- | --- |
| `Stopped` | Raised inside a task when `stop()` has been requested. Carries the optional `reason` passed to `stop()` as its message (`str(Stopped("foo")) == "foo"`; a reason-less `Stopped()` is empty). Unwinds normally; `finally` blocks run. |
| `Timeout` | Raised by `Task.wait(timeout=...)` when the deadline elapses before the task is done — `wait()` never *returns* to signal a timeout, so a returned `None` unambiguously means the task finished with a `None` result. Subclasses the builtin `TimeoutError` (so `except TimeoutError` also catches it). A parent-stop raises `Stopped` instead, so a bounded `wait()`'s two failure modes are never confused. Each task raises its own subclass `task.Timeout` (carrying `.task`), so `except some_task.Timeout` catches only *that* task's deadline — not a `Timeout` that propagated up from an inner `wait` as the task's result. |
| `MultiException(message, exceptions)` | Aggregate raised by `MultiTask.wait()` when more than one child failed. `.exceptions` holds the child exceptions in task order; the message combines `message` with each child's string form. |

### Tasks

| Name | Description |
| --- | --- |
| `Task` | `runtime_checkable` structural `Protocol` for a stoppable, waitable unit of work. |
| `ThreadTask(fn, args=(), kwargs=None, *, name=None, detach=False, on_finish=None, start=True)` | Runs `fn` in a new daemon thread; implements `Task`. With `start=False`, call `.start()` to launch (idempotent). |
| `WorkTask(fn, args, kwargs, name=None)` | One job queued to a `WorkerThread`; implements `Task`. Usually created via `WorkerThread.submit`. |
| `Promise(name=None, *, on_finish=None)` | A `Task` with no thread or body, completed externally via `resolve(value)` / `fail(exc)`; implements `Task`. Idempotent completion; `stop(reason=None)` completes it with `Stopped` carrying the reason. |
| `MultiTask(tasks, name=None, *, on_finish=None)` | A bodyless, threadless `Task` that completes when ALL its child `tasks` complete; implements `Task`. `wait()` returns the list of child results in task order, re-raises a lone child failure, or raises `MultiException` for two or more. `stop(reason=None)` stops every child then completes with `Stopped`. `tasks` exposes the children. |
| `WorkerThread(name=None)` | Long-lived worker thread that serializes submitted jobs. `submit(...)` returns a `WorkTask`; `stop()` drains queued jobs and shuts down. |
| `asynch(fn, name=None, detach=False, on_finish=None)` | Returns a launcher that starts `fn` in a new `ThreadTask` when called. |
| `synch(fn)` | Returns a synchronous version of `fn` that de-wraps `asynch` and awaits returned tasks, yielding a concrete value. |

### `Task` protocol members

| Member | Description |
| --- | --- |
| `is_done: bool` | Whether the task has finished. |
| `is_stopped: bool` | Whether a stop has been requested. |
| `stop_reason` | Property; the `reason` passed to the first `stop()`, or `None` for a reason-less stop. |
| `result` | Property; shorthand for `wait()`. |
| `wait(timeout=None)` | Block until done, re-raising any worker exception. A stop on the calling parent propagates here and raises `Stopped` carrying the parent's reason. With a `timeout`, raises `Timeout` if the task is not done by the deadline (never returns to signal it, so a returned `None` means the task finished). `timeout=None` waits forever; `result` uses no timeout, so it never raises `Timeout`. |
| `stop(reason=None)` | Request cooperative stop; record `reason` (first stop only); fire stop callbacks once; cascade to children (passing the reason along). |
| `add_finish_callback(fn)` | Call `fn(result, exception)` when the task finishes (immediately if already finished). |
| `add_stop_callback(fn)` | Call zero-arg `fn()` once when the task is stopped (immediately if already stopped). |
| `remove_stop_callback(fn)` | Unregister a stop callback; no-op if absent. |
| `detach()` | Remove this task from its parent's stop propagation. Parent-only: the caller must be the task whose children include this one (a task cannot detach itself); otherwise raises `RuntimeError`. |

### Context / throughline

| Name | Description |
| --- | --- |
| `SemanticStack(name="semantic_stack", *, required=())` | Context-local stack of labeled frames backed by a `ContextVar`. |
| `SemanticStack.__call__(**kwargs)` | Context manager that pushes a frame for the block (raises `ValueError` if a required key is missing). |
| `SemanticStack.get(key, default=None)` | Value of `key` from the innermost frame that has it. |
| `SemanticStack.collect(key)` | Tuple of all values for `key`, outermost-first. |
| `SemanticStack.walk(fn)` | Apply `fn` to each frame dict, outermost-first; return a tuple of results. |
| `SemanticStack.frames()` | Full stack as fresh dicts, outermost-first. |
| `SemanticStack.snapshot()` | Capture current state as a pure-data `SemanticSnapshot` (picklable). |
| `SemanticStack.restore(snapshot_or_frames)` | Context manager that installs a snapshot (or an iterable of frame dicts) onto this stack for the block; replays without `required`-key validation. |
| `SemanticSnapshot.frames()` | Captured frames as fresh dicts, outermost-first (for serialization). |
| `throughline` | Module singleton `SemanticStack("throughline", required=("name",))` — the human-readable narrative. |
| `task_context(task, name)` | Context manager that enters `name` on the throughline and `task` on the private task stack. |
| `current_task()` | The innermost running `Task`, or `None` outside any task. |
| `task_chain()` | Tuple of task names, outermost-first (`throughline.collect("name")`). |
| `ThroughlineNameFilter` | `logging.Filter` that injects `task_chain()` onto each record as `record.throughline`. |

### Stop-aware blocking primitives

| Name | Description |
| --- | --- |
| `sleep(seconds)` | Drop-in for `time.sleep`; raises `Stopped` if the current task is stopped (poll-free). |
| `check_stop()` | Raise `Stopped` if the current task is stopped; like `sleep(0)`. |
| `poll(fn, *, interval=0.05, timeout=None)` | Sample `fn` until truthy; stop-aware inter-sample wait. Returns `fn`'s truthy value, or the last falsy value on timeout. |
| `Queue(maxsize=0)` | Drop-in for `queue.Queue`; `get()` raises `Stopped` if the current task is stopped. |
| `Event()` | Drop-in for `threading.Event`; `wait()` raises `Stopped` if the current task is stopped. |

Every `Stopped` these raise carries the stopped task's `stop_reason` as its
message, so callers can log *why* the task unwound.

---

## Testing

The test suite runs under `pytest`:

```bash
pip install -e ".[dev]"
pytest
```

---

## Contributing

Contributions are welcome. Please open an issue to discuss substantial changes
first. Keep changes small and focused, match the surrounding code style, and add
tests covering new behavior. The library is intentionally dependency-free and
pure Python — please keep it that way.

---

## License

MIT License. See the [LICENSE](LICENSE) file for details.

---

## Authors

- **Martin Chase**
- **Luke Campagnola**

Copyright © Martin Chase and Luke Campagnola.
