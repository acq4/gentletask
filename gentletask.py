# Reference implementation of the gentletask v7 spec.
# Provides the SemanticStack (`throughline`), stoppable Task hierarchy, and
# stop-aware blocking primitives described in v7/spec.md.

from __future__ import annotations

import collections
import contextlib
import contextvars
import logging
import queue as _queue
import threading
import time
import weakref
from typing import Any, Callable, Iterable, Iterator, Protocol, runtime_checkable


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class Stopped(Exception):
    """Raised inside a task when stop() has been requested.

    Unwinds normally; finally blocks run.
    """


# ---------------------------------------------------------------------------
# SemanticStack
# ---------------------------------------------------------------------------

# A frame is an ordered, structurally-immutable collection of key/value pairs.
Frame = tuple[tuple[str, Any], ...]


class SemanticStack:
    """A context-local stack of labeled frames, backed by a single ContextVar.

    Each frame is stored as a ``tuple[tuple[str, Any], ...]`` so the stack is
    structurally immutable — no defensive copying is needed. ``frames()`` (and
    friends) construct fresh dicts on the way out, so mutating returned dicts
    has no effect on the stack.

    The stack has no opinion about tasks, threads, or logging; it just holds
    whatever keys callers provide.

    A stack may declare *required* keys: every frame entered via ``__call__``
    must supply them, or a ``ValueError`` is raised. Extra keys are always
    allowed — ``required`` is a floor, not a schema.
    """

    def __init__(
        self, name: str = "semantic_stack", *, required: Iterable[str] = ()
    ) -> None:
        self._label = name
        self._required: tuple[str, ...] = tuple(required)
        # A per-instance ContextVar so independent stacks stay isolated.
        self._var: contextvars.ContextVar[tuple[Frame, ...]] = contextvars.ContextVar(
            name, default=()
        )

    @contextlib.contextmanager
    def __call__(self, **kwargs: Any) -> Iterator[None]:
        """Append a frame for the duration of the with block, then remove it.

        Raises ValueError if any of this stack's required keys are missing.
        """
        missing = [key for key in self._required if key not in kwargs]
        if missing:
            raise ValueError(
                f"{self._label} frame missing required key(s): " f"{', '.join(missing)}"
            )
        frame: Frame = tuple(kwargs.items())
        token = self._var.set(self._var.get() + (frame,))
        try:
            yield
        finally:
            self._var.reset(token)

    def get(self, key: str, default: Any = None) -> Any:
        """Return the value of *key* from the innermost frame that has it."""
        for frame in reversed(self._var.get()):
            for k, v in frame:
                if k == key:
                    return v
        return default

    def collect(self, key: str) -> tuple[Any, ...]:
        """Return all values for *key*, outermost-first, skipping frames without it."""
        result: list[Any] = []
        for frame in self._var.get():
            for k, v in frame:
                if k == key:
                    result.append(v)
        return tuple(result)

    def walk(self, fn: Callable[[dict[str, Any]], Any]) -> tuple[Any, ...]:
        """Apply *fn* to each frame dict, outermost-first; return a tuple of results.

        *fn* receives a freshly constructed dict for each frame. Results are
        kept as-is — filter separately if needed.
        """
        return tuple(fn(dict(frame)) for frame in self._var.get())

    def frames(self) -> tuple[dict[str, Any], ...]:
        """Return the full stack as fresh dicts, outermost-first."""
        return tuple(dict(frame) for frame in self._var.get())

    def snapshot(self) -> "SemanticSnapshot":
        """Capture the current stack state for later restoration."""
        return SemanticSnapshot(self, self._var.get())


class SemanticSnapshot:
    """An immutable capture of a SemanticStack's frames at a point in time."""

    def __init__(self, stack: SemanticStack, frames: tuple[Frame, ...]) -> None:
        self._stack = stack
        self._frames = frames

    @contextlib.contextmanager
    def restore(self) -> Iterator[None]:
        """Install the captured frames as the current stack for this block.

        Restoring replaces whatever frames are currently on the stack for the
        duration of the block; on exit the previous state is reset. A thread
        that already had frames sees them hidden — not merged — while the
        snapshot is active.
        """
        token = self._stack._var.set(self._frames)
        try:
            yield
        finally:
            self._stack._var.reset(token)


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

# A continuous thread of meaning running through all concurrent execution.
# Names only: this is the fun, human-readable narrative. Every frame must
# carry a `name`. Logging filters and user code share this one instance.
throughline = SemanticStack("throughline", required=("name",))

# A private stack with a single purpose: tracking which Task is running. Every
# frame carries exactly the running `task`. current_task() reads from here, so
# the task machinery never has to mingle bookkeeping into the throughline.
_task_stack = SemanticStack("_task_stack", required=("task",))


# ---------------------------------------------------------------------------
# Task Protocol and derived helpers
# ---------------------------------------------------------------------------


@runtime_checkable
class Task(Protocol):
    is_done: bool
    is_stopped: bool

    @property
    def result(self) -> Any: ...

    def wait(self, timeout: float | None = None) -> Any: ...
    def stop(self) -> None: ...
    def add_finish_callback(
        self, fn: Callable[[Any, BaseException | None], Any]
    ) -> None: ...
    def add_stop_callback(self, fn: Callable[[], Any]) -> None: ...
    def remove_stop_callback(self, fn: Callable[[], Any]) -> None: ...
    def detach(self) -> None: ...


@contextlib.contextmanager
def task_context(task: Task, name: str) -> Iterator[None]:
    """Mark *task* as running for the duration of the block.

    Enters one frame on each stack: *name* on the throughline (narrative) and
    *task* on the private task stack (bookkeeping). Every Task implementation —
    built-in or custom — should wrap its work in this so current_task() and the
    log chain both line up.
    """
    with throughline(name=name), _task_stack(task=task):
        yield


def current_task() -> Task | None:
    """Return the innermost running Task, or None outside any task."""
    return _task_stack.get("task")


def task_chain() -> tuple[str, ...]:
    """Return the chain of task names, outermost-first."""
    return throughline.collect("name")


class ThroughlineNameFilter(logging.Filter):
    """Inject ``throughline.collect("name")`` into each log record as ``throughline``."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.throughline = task_chain()
        return True


# ---------------------------------------------------------------------------
# Stop-aware blocking primitives
# ---------------------------------------------------------------------------

_POLL_INTERVAL = 0.05


@contextlib.contextmanager
def _stop_waker(wake: Callable[[], None]) -> Iterator[None]:
    """Call *wake* whenever the current task is stopped, for the block's duration.

    This is the poll-free heart of stop propagation: instead of a waiter waking
    periodically to ask "have I been stopped?", it parks indefinitely and lets
    ``stop()`` push a *wake* notification through. *wake* fires immediately if
    the task is already stopped, and once on each later ``stop()``.

    Outside any task there is nothing to stop, so this is a no-op. *wake* must
    not acquire a lock the caller already holds when entering this block — a
    task that is already stopped fires *wake* synchronously from here.
    """
    task = current_task()
    if task is None:
        yield
        return
    task.add_stop_callback(wake)
    try:
        yield
    finally:
        task.remove_stop_callback(wake)


def check_stop() -> None:
    """Raise Stopped if the current task has been stopped. Equivalent to sleep(0)."""
    task = current_task()
    if task is not None and task.is_stopped:
        raise Stopped()


def sleep(seconds: float) -> None:
    """Drop-in for time.sleep. Raises Stopped if the current task is stopped.

    Poll-free: blocks on the stop signal itself, so a stop unblocks the sleep
    immediately rather than at the next polling tick. Safe to call outside any
    task — behaves like time.sleep.
    """
    task = current_task()
    if task is None:
        time.sleep(seconds)
        return
    woken = threading.Event()
    with _stop_waker(woken.set):
        if task.is_stopped:
            raise Stopped()
        # woken.wait() returns True only if stop() set the event; a timeout
        # (the full sleep elapsed without a stop) returns False.
        if woken.wait(seconds):
            raise Stopped()


def poll(
    fn: Callable[[], Any],
    *,
    interval: float = _POLL_INTERVAL,
    timeout: float | None = None,
) -> Any:
    """Poll *fn* until it returns truthy, checking for stop between intervals.

    *fn* is an arbitrary predicate, so it is necessarily sampled on *interval*;
    stop, however, is poll-free — a stop unblocks the inter-poll wait at once.
    Returns the truthy value from *fn*, or the last falsy value on timeout.
    Raises Stopped if the current task is stopped while polling.
    """
    deadline = None if timeout is None else time.monotonic() + timeout
    woken = threading.Event()
    with _stop_waker(woken.set):
        while True:
            check_stop()
            result = fn()
            if result:
                return result
            if deadline is not None and time.monotonic() >= deadline:
                return result
            wait_for = interval
            if deadline is not None:
                wait_for = min(interval, max(0.0, deadline - time.monotonic()))
            if woken.wait(wait_for):
                check_stop()  # stop arrived during the wait — raises Stopped


class Queue:
    """Drop-in for queue.Queue. get() raises Stopped if the current task is stopped.

    Backed by its own Condition rather than wrapping queue.Queue, so a stopped
    task waiting in get() is woken immediately by the stop signal instead of
    polling the underlying queue.
    """

    def __init__(self, maxsize: int = 0) -> None:
        self._maxsize = maxsize
        self._items: collections.deque = collections.deque()
        self._cond = threading.Condition()
        self._unfinished = 0

    def get(self, block: bool = True, timeout: float | None = None) -> Any:
        task = current_task()
        if task is None or not block:
            # No task to stop, or a non-blocking get: plain queue semantics.
            with self._cond:
                deadline = None if timeout is None else time.monotonic() + timeout
                while not self._items:
                    if not block:
                        raise _queue.Empty()
                    remaining = (
                        None if deadline is None else deadline - time.monotonic()
                    )
                    if remaining is not None and remaining <= 0:
                        raise _queue.Empty()
                    self._cond.wait(remaining)
                return self._items.popleft()

        # Stop-aware path. Register the waker *before* taking self._cond so an
        # already-stopped task fires wake() without re-entering our held lock.
        def wake() -> None:
            with self._cond:
                self._cond.notify_all()

        deadline = None if timeout is None else time.monotonic() + timeout
        with _stop_waker(wake):
            with self._cond:
                while True:
                    if task.is_stopped:
                        raise Stopped()
                    if self._items:
                        return self._items.popleft()
                    remaining = (
                        None if deadline is None else deadline - time.monotonic()
                    )
                    if remaining is not None and remaining <= 0:
                        raise _queue.Empty()
                    self._cond.wait(remaining)

    def put(self, item: Any, block: bool = True, timeout: float | None = None) -> None:
        with self._cond:
            if self._maxsize > 0:
                deadline = None if timeout is None else time.monotonic() + timeout
                while len(self._items) >= self._maxsize:
                    if not block:
                        raise _queue.Full()
                    remaining = (
                        None if deadline is None else deadline - time.monotonic()
                    )
                    if remaining is not None and remaining <= 0:
                        raise _queue.Full()
                    self._cond.wait(remaining)
            self._items.append(item)
            self._unfinished += 1
            self._cond.notify_all()

    def put_nowait(self, item: Any) -> None:
        self.put(item, block=False)

    def get_nowait(self) -> Any:
        return self.get(block=False)

    def task_done(self) -> None:
        with self._cond:
            if self._unfinished <= 0:
                raise ValueError("task_done() called too many times")
            self._unfinished -= 1
            if self._unfinished == 0:
                self._cond.notify_all()

    def join(self) -> None:
        with self._cond:
            while self._unfinished > 0:
                self._cond.wait()

    def qsize(self) -> int:
        with self._cond:
            return len(self._items)

    def empty(self) -> bool:
        with self._cond:
            return not self._items

    def full(self) -> bool:
        with self._cond:
            return self._maxsize > 0 and len(self._items) >= self._maxsize


class Event:
    """Drop-in for threading.Event. wait() raises Stopped if the current task is stopped.

    Backed by its own Condition so a stopped task waiting in wait() is woken
    immediately by the stop signal rather than polling the flag.
    """

    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._flag = False

    def wait(self, timeout: float | None = None) -> bool:
        task = current_task()
        if task is None:
            with self._cond:
                deadline = None if timeout is None else time.monotonic() + timeout
                while not self._flag:
                    remaining = (
                        None if deadline is None else deadline - time.monotonic()
                    )
                    if remaining is not None and remaining <= 0:
                        return self._flag
                    self._cond.wait(remaining)
                return True

        # Stop-aware path. Register the waker before taking self._cond (see Queue).
        def wake() -> None:
            with self._cond:
                self._cond.notify_all()

        deadline = None if timeout is None else time.monotonic() + timeout
        with _stop_waker(wake):
            with self._cond:
                while not self._flag:
                    if task.is_stopped:
                        raise Stopped()
                    remaining = (
                        None if deadline is None else deadline - time.monotonic()
                    )
                    if remaining is not None and remaining <= 0:
                        return self._flag
                    self._cond.wait(remaining)
                return True

    def set(self) -> None:
        with self._cond:
            self._flag = True
            self._cond.notify_all()

    def clear(self) -> None:
        with self._cond:
            self._flag = False

    def is_set(self) -> bool:
        with self._cond:
            return self._flag


# ---------------------------------------------------------------------------
# Shared stop/child/callback bookkeeping
# ---------------------------------------------------------------------------


class _TaskCore:
    """State and behavior shared by ThreadTask and WorkTask.

    Holds the done/stop events, child registry, result/exception, and the
    finish-callback machinery. Subclasses provide their own scheduling and
    context handling.
    """

    def __init__(
        self,
        fn: Callable[..., Any],
        args: tuple,
        kwargs: dict,
        name: str | None,
        on_finish: Callable[[Any, BaseException | None], Any] | None = None,
    ) -> None:
        self._fn = fn
        self._args = tuple(args)
        self._kwargs = dict(kwargs or {})
        self._name = name or getattr(fn, "__qualname__", repr(fn))

        self._done = threading.Event()
        self._stop_requested = threading.Event()
        self._lock = threading.RLock()
        # A Condition over the same lock lets wait() park indefinitely and be
        # woken poll-free, either by _finish() (this task is done) or by a stop
        # nudge registered on the parent (the parent was stopped).
        self._cond = threading.Condition(self._lock)
        self._children: weakref.WeakSet[Task] = weakref.WeakSet()
        self._callbacks: list[Callable[[Any, BaseException | None], Any]] = []
        # Zero-arg callbacks fired exactly once when stop() is first requested.
        self._stop_callbacks: list[Callable[[], Any]] = []
        self._result: Any = None
        self._exception: BaseException | None = None
        self._detach = False

        if on_finish is not None:
            self._callbacks.append(on_finish)

    # -- public interface ----------------------------------------------------

    @property
    def is_done(self) -> bool:
        return self._done.is_set()

    @property
    def is_stopped(self) -> bool:
        return self._stop_requested.is_set()

    @property
    def result(self) -> Any:
        return self.wait()

    def wait(self, timeout: float | None = None) -> Any:
        """Block until done, re-raising any worker exception.

        If called from inside another task, a stop request on that parent
        propagates to this task: the parent wakes this wait the moment it is
        stopped (poll-free), and we then stop ourselves and raise Stopped.
        """
        parent = current_task()
        if parent is not None and parent is not self and not self._detach:
            if hasattr(parent, "_children"):
                parent._children.add(self)

        # Wake this wait when the parent stops, so the cascade is immediate.
        def _nudge() -> None:
            with self._cond:
                self._cond.notify_all()

        watch_parent = (
            parent is not None
            and parent is not self
            and hasattr(parent, "add_stop_callback")
        )
        stopped_by_parent = False
        if watch_parent:
            parent.add_stop_callback(_nudge)
        try:
            deadline = None if timeout is None else time.monotonic() + timeout
            with self._cond:
                while True:
                    if self._done.is_set():
                        break
                    if parent is not None and parent is not self and parent.is_stopped:
                        stopped_by_parent = True
                        break
                    remaining = (
                        None if deadline is None else deadline - time.monotonic()
                    )
                    if remaining is not None and remaining <= 0:
                        break  # timed out
                    self._cond.wait(remaining)
        finally:
            if watch_parent:
                parent.remove_stop_callback(_nudge)

        if stopped_by_parent:
            self.stop()
            raise Stopped()
        if not self.is_done:
            return None
        if self._exception is not None:
            raise self._exception
        return self._result

    def stop(self) -> None:
        """Request cooperative stop; fire stop callbacks; cascade to children.

        Idempotent: the stop callbacks fire exactly once, on the first stop.
        """
        with self._lock:
            already = self._stop_requested.is_set()
            self._stop_requested.set()
            children = list(self._children)
            callbacks, self._stop_callbacks = list(self._stop_callbacks), []
        if already:
            return
        # Fire callbacks and cascade outside the lock: a stop callback typically
        # acquires another object's lock to wake a waiter, and a child's stop()
        # takes the child's lock — holding ours here would invite deadlock.
        for cb in callbacks:
            try:
                cb()
            except Exception:
                pass
        for child in children:
            child.stop()

    def add_finish_callback(
        self, fn: Callable[[Any, BaseException | None], Any]
    ) -> None:
        """Call fn(result, exception) when this task finishes.

        Calls fn immediately in the current thread if already finished.
        """
        with self._lock:
            if self._done.is_set():
                result, exc = self._result, self._exception
            else:
                self._callbacks.append(fn)
                return
        fn(result, exc)

    def add_stop_callback(self, fn: Callable[[], Any]) -> None:
        """Call fn() when this task is stopped.

        Fires fn immediately in the current thread if already stopped. This is
        the hook the blocking primitives use to wake without polling.
        """
        with self._lock:
            if not self._stop_requested.is_set():
                self._stop_callbacks.append(fn)
                return
        fn()

    def remove_stop_callback(self, fn: Callable[[], Any]) -> None:
        """Remove a previously registered stop callback; a no-op if absent."""
        with self._lock:
            try:
                self._stop_callbacks.remove(fn)
            except ValueError:
                pass

    def detach(self) -> None:
        """Remove this task from its parent's stop propagation."""
        self._detach = True
        parent = current_task()
        if parent is not None and hasattr(parent, "_children"):
            parent._children.discard(self)

    # -- internals -----------------------------------------------------------

    def _register_with_parent(self) -> None:
        parent = current_task()
        if parent is not None and not self._detach and hasattr(parent, "_children"):
            parent._children.add(self)

    def _finish(self, result: Any = None, exc: BaseException | None = None) -> None:
        self._result = result
        self._exception = exc
        self._mark_done()

    def _mark_done(self) -> None:
        """Mark the task done, wake any waiters poll-free, then run callbacks.

        ``self._result`` / ``self._exception`` must already be set.
        """
        with self._cond:
            self._done.set()
            self._cond.notify_all()
        self._run_callbacks()

    def _run_callbacks(self) -> None:
        with self._lock:
            callbacks, self._callbacks = list(self._callbacks), []
        for cb in callbacks:
            try:
                cb(self._result, self._exception)
            except Exception:
                pass

    def _stop_running_children(self) -> None:
        with self._lock:
            children = list(self._children)
        for child in children:
            if not child.is_done:
                child.stop()


# ---------------------------------------------------------------------------
# ThreadTask
# ---------------------------------------------------------------------------


class ThreadTask(_TaskCore):
    """Runs fn(*args, **kwargs) in a new daemon thread. Implements the Task protocol."""

    Stopped = Stopped

    def __init__(
        self,
        fn: Callable[..., Any],
        args: Iterable[Any] = (),
        kwargs: dict[str, Any] | None = None,
        *,
        name: str | None = None,
        detach: bool = False,
        on_finish: Callable[[Any, BaseException | None], Any] | None = None,
    ) -> None:
        super().__init__(fn, tuple(args), kwargs or {}, name, on_finish)
        self._detach = detach
        self._waited = False

        # Register with the parent task (if any) so stop cascades.
        self._register_with_parent()

        # Copy the calling context so the thread inherits both stacks (the
        # throughline narrative and the task chain) automatically.
        ctx = contextvars.copy_context()
        self._thread = threading.Thread(
            target=ctx.run,
            args=(self._run,),
            daemon=True,
            name=self._name,
        )
        self._thread.start()

    def wait(self, timeout: float | None = None) -> Any:
        self._waited = True
        return super().wait(timeout)

    def detach(self) -> None:
        self._detach = True
        super().detach()

    # -- internals -----------------------------------------------------------

    def _run(self) -> None:
        with task_context(self, self._name):
            try:
                if self.is_stopped:
                    raise Stopped()
                self._result = self._fn(*self._args, **self._kwargs)
            except BaseException as exc:
                self._exception = exc
            finally:
                self._stop_running_children()
                self._mark_done()

    def __del__(self) -> None:
        if not self._waited and not self.is_done:
            self.stop()

    def __repr__(self) -> str:
        status = (
            "done" if self.is_done else ("stopped" if self.is_stopped else "running")
        )
        return f"<ThreadTask {self._name!r} {status}>"


def asynch(
    fn: Callable,
    name: str | None = None,
    detach: bool = False,
    on_finish: Callable[[Any, BaseException | None], Any] | None = None,
) -> Callable[..., ThreadTask]:
    """Return a callable that starts *fn* in a new ThreadTask when called.

    Usage:
        task = asynch(fn)(*args, **kwargs)
        task = asynch(fn, name="my task")(*args, **kwargs)
    """

    def wrapper(*args: Any, **kwargs: Any) -> ThreadTask:
        return ThreadTask(
            fn, args, kwargs, name=name, detach=detach, on_finish=on_finish
        )

    # Record the original callable so synch() can de-wrap back to it.
    wrapper._asynch_wraps = fn
    return wrapper


def synch(fn: Callable) -> Callable:
    """Return a synchronous version of *fn* that yields a concrete value.

    Flattens two layers of asynchrony:

    - If *fn* was produced by asynch(), it is de-wrapped to the original
      callable, so the work runs inline in the current thread with no extra
      ThreadTask.
    - When the (de-wrapped) callable is invoked and returns a value that
      implements the Task protocol, synch waits for that task and returns its
      result instead of the task itself.

    A plain callable returning a plain value is simply called and its value
    returned. synch is safe to apply whether or not a function was
    asynch-wrapped or returns a task.
    """
    target = getattr(fn, "_asynch_wraps", fn)

    def wrapper(*args: Any, **kwargs: Any) -> Any:
        result = target(*args, **kwargs)
        if isinstance(result, Task):
            return result.wait()
        return result

    return wrapper


# ---------------------------------------------------------------------------
# WorkTask
# ---------------------------------------------------------------------------


class WorkTask(_TaskCore):
    """One job queued to a WorkerThread. Implements the Task protocol."""

    def __init__(
        self,
        fn: Callable[..., Any],
        args: tuple,
        kwargs: dict,
        name: str | None = None,
    ) -> None:
        super().__init__(fn, args, kwargs, name)
        # Snapshot the submitter's context at submit time. The job inherits the
        # context of the code that caused it to be submitted, not the worker's.
        # Both stacks travel: the narrative (throughline) and the task chain.
        self._throughline_snapshot = throughline.snapshot()
        self._task_snapshot = _task_stack.snapshot()

    def _execute(self) -> None:
        """Run the job body. Called by the worker thread."""
        with self._throughline_snapshot.restore(), self._task_snapshot.restore():
            with task_context(self, self._name):
                result = None
                exc: BaseException | None = None
                try:
                    result = self._fn(*self._args, **self._kwargs)
                except BaseException as e:
                    exc = e
                finally:
                    self._stop_running_children()
                    self._finish(result=result, exc=exc)

    def __repr__(self) -> str:
        status = (
            "done"
            if self.is_done
            else ("stopped" if self.is_stopped else "queued/running")
        )
        return f"<WorkTask {self._name!r} {status}>"


# ---------------------------------------------------------------------------
# WorkerThread
# ---------------------------------------------------------------------------

_WORKER_STOP = object()


class WorkerThread:
    """Long-lived worker thread that serialises submitted jobs."""

    def __init__(self, name: str | None = None) -> None:
        self._name = name or "WorkerThread"
        self._queue: _queue.Queue = _queue.Queue()
        self._thread = threading.Thread(target=self._run, daemon=True, name=self._name)
        self._thread.start()

    def submit(
        self,
        fn: Callable,
        args: tuple = (),
        kwargs: dict | None = None,
        *,
        name: str | None = None,
    ) -> WorkTask:
        """Enqueue *fn* and return a WorkTask immediately."""
        task = WorkTask(fn, args, kwargs or {}, name=name)
        self._queue.put(task)
        return task

    def stop(self) -> None:
        """Drain the queue and shut down the thread."""
        self._queue.put(_WORKER_STOP)

    # -- internals -----------------------------------------------------------

    def _run(self) -> None:
        while True:
            item = self._queue.get()
            if item is _WORKER_STOP:
                break

            task: WorkTask = item
            if task.is_stopped:
                # Skip a job that was stopped before it ever ran.
                task._finish(exc=Stopped())
                continue

            task._execute()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

__all__ = [
    "Stopped",
    "SemanticStack",
    "SemanticSnapshot",
    "throughline",
    "Task",
    "ThreadTask",
    "WorkTask",
    "WorkerThread",
    "asynch",
    "synch",
    "task_context",
    "current_task",
    "task_chain",
    "ThroughlineNameFilter",
    "sleep",
    "check_stop",
    "poll",
    "Queue",
    "Event",
]
