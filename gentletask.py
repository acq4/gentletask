# Reference implementation of the gentletask v7 spec.
# Provides the SemanticStack (`throughline`), stoppable Task hierarchy, and
# stop-aware blocking primitives described in v7/spec.md.

from __future__ import annotations

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
    """

    def __init__(self, name: str = "semantic_stack") -> None:
        # A per-instance ContextVar so independent stacks stay isolated.
        self._var: contextvars.ContextVar[tuple[Frame, ...]] = contextvars.ContextVar(
            name, default=()
        )

    @contextlib.contextmanager
    def __call__(self, **kwargs: Any) -> Iterator[None]:
        """Append a frame for the duration of the with block, then remove it."""
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
# Task machinery, logging filters, and user code all share this one instance.
throughline = SemanticStack("throughline")


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
    def detach(self) -> None: ...


def current_task() -> Task | None:
    """Return the innermost Task on the throughline, or None outside any task."""
    return throughline.get("task")


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


def check_stop() -> None:
    """Raise Stopped if the current task has been stopped. Equivalent to sleep(0)."""
    task = current_task()
    if task is not None and task.is_stopped:
        raise Stopped()


def sleep(seconds: float, *, interval: float = _POLL_INTERVAL) -> None:
    """Drop-in for time.sleep. Raises Stopped if the current task is stopped.

    Safe to call outside any task — behaves like time.sleep.
    """
    task = current_task()
    if task is None:
        time.sleep(seconds)
        return
    deadline = time.monotonic() + seconds
    while True:
        if task.is_stopped:
            raise Stopped()
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        time.sleep(min(interval, remaining))


def poll(
    fn: Callable[[], Any],
    *,
    interval: float = _POLL_INTERVAL,
    timeout: float | None = None,
) -> Any:
    """Poll *fn* until it returns truthy, checking for stop each interval.

    Returns the truthy value from *fn*, or the last falsy value on timeout.
    Raises Stopped if the current task is stopped while polling.
    """
    deadline = None if timeout is None else time.monotonic() + timeout
    while True:
        check_stop()
        result = fn()
        if result:
            return result
        if deadline is not None and time.monotonic() >= deadline:
            return result
        time.sleep(interval)


class Queue:
    """Drop-in for queue.Queue. get() raises Stopped if the current task is stopped."""

    def __init__(self, maxsize: int = 0) -> None:
        self._q: _queue.Queue = _queue.Queue(maxsize)

    def get(self, block: bool = True, timeout: float | None = None) -> Any:
        task = current_task()
        if task is None or not block:
            return self._q.get(block=block, timeout=timeout)
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            if task.is_stopped:
                raise Stopped()
            remaining = (
                None if deadline is None else max(0.0, deadline - time.monotonic())
            )
            if remaining == 0.0:
                raise _queue.Empty()
            wait_for = (
                _POLL_INTERVAL if remaining is None else min(_POLL_INTERVAL, remaining)
            )
            try:
                return self._q.get(timeout=wait_for)
            except _queue.Empty:
                if remaining is not None and time.monotonic() >= deadline:
                    raise

    def put(self, item: Any, block: bool = True, timeout: float | None = None) -> None:
        self._q.put(item, block=block, timeout=timeout)

    def put_nowait(self, item: Any) -> None:
        self._q.put_nowait(item)

    def get_nowait(self) -> Any:
        return self._q.get_nowait()

    def task_done(self) -> None:
        self._q.task_done()

    def join(self) -> None:
        self._q.join()

    def qsize(self) -> int:
        return self._q.qsize()

    def empty(self) -> bool:
        return self._q.empty()

    def full(self) -> bool:
        return self._q.full()


class Event:
    """Drop-in for threading.Event. wait() raises Stopped if the current task is stopped."""

    def __init__(self) -> None:
        self._event = threading.Event()

    def wait(self, timeout: float | None = None) -> bool:
        task = current_task()
        if task is None:
            return self._event.wait(timeout=timeout)
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            if task.is_stopped:
                raise Stopped()
            remaining = (
                None if deadline is None else max(0.0, deadline - time.monotonic())
            )
            if remaining == 0.0:
                return self._event.is_set()
            wait_for = (
                _POLL_INTERVAL if remaining is None else min(_POLL_INTERVAL, remaining)
            )
            if self._event.wait(wait_for):
                return True

    def set(self) -> None:
        self._event.set()

    def clear(self) -> None:
        self._event.clear()

    def is_set(self) -> bool:
        return self._event.is_set()


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
        self._children: weakref.WeakSet[Task] = weakref.WeakSet()
        self._callbacks: list[Callable[[Any, BaseException | None], Any]] = []
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
        propagates to this task on each poll interval.
        """
        parent = current_task()
        if parent is not None and parent is not self and not self._detach:
            if hasattr(parent, "_children"):
                parent._children.add(self)

        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            remaining = (
                None if deadline is None else max(0.0, deadline - time.monotonic())
            )
            wait_for = (
                _POLL_INTERVAL if remaining is None else min(_POLL_INTERVAL, remaining)
            )
            if self._done.wait(wait_for):
                break
            if parent is not None and parent is not self and parent.is_stopped:
                self.stop()
                raise Stopped()
            if remaining == 0.0:
                break  # timed out

        if not self.is_done:
            return None
        if self._exception is not None:
            raise self._exception
        return self._result

    def stop(self) -> None:
        """Request cooperative stop; cascades to all registered children."""
        self._stop_requested.set()
        with self._lock:
            children = list(self._children)
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
        self._done.set()
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

        # Copy the calling context so the thread inherits the throughline stack.
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
        with throughline(name=self._name, task=self):
            try:
                if self.is_stopped:
                    raise Stopped()
                self._result = self._fn(*self._args, **self._kwargs)
            except BaseException as exc:
                self._exception = exc
            finally:
                self._stop_running_children()
                self._done.set()
                self._run_callbacks()

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
        self._snapshot = throughline.snapshot()

    def _execute(self) -> None:
        """Run the job body. Called by the worker thread."""
        with self._snapshot.restore():
            with throughline(name=self._name, task=self):
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
    "current_task",
    "task_chain",
    "ThroughlineNameFilter",
    "sleep",
    "check_stop",
    "poll",
    "Queue",
    "Event",
]
