# Reference implementation of the gentletask concurrency model.
# Provides the SemanticStack (`throughline`), a stoppable Task hierarchy, and
# stop-aware blocking primitives.

from __future__ import annotations

import collections
import contextlib
import contextvars
import functools
import logging
import queue as _queue
import threading
import time
import traceback
import weakref
from typing import Any, Callable, Iterable, Iterator, Protocol, runtime_checkable

__version__ = "0.6.0"

_logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class Stopped(Exception):
    """Raised inside a task when stop() has been requested.

    Carries the optional *reason* passed to ``stop()`` as its message, so
    ``str(Stopped("foo")) == "foo"`` and a reason-less ``Stopped()`` is empty.
    Unwinds normally; finally blocks run.
    """


class _Timeout(TimeoutError):
    """Internal base for the per-task wait-timeout exceptions.

    Not part of the public API — callers never catch this directly. Each task
    raises its *own* subclass, reachable as ``task.Timeout``, so ``except
    task.Timeout`` catches only that task's deadline (and not a ``Timeout`` that
    merely propagated up from an inner ``wait`` as the task's result). For a
    broader catch, callers fall back to the builtin ``TimeoutError``, which this
    subclasses. The raised instance carries ``.task``, the task whose wait
    elapsed.

    A timeout is raised — never returned — so a ``None`` return from ``wait()``
    unambiguously means "the task finished and its result was None". A
    parent-stop wakes ``wait()`` with ``Stopped`` instead, so the reasons a
    bounded ``wait()`` can fail are never confused.
    """


def _stopped(reason: str | None) -> Stopped:
    """Build a ``Stopped`` carrying *reason* as its message, or empty if None.

    A reason-less stop must produce a message-less ``Stopped`` (``str(...) ==
    ""``) exactly as before, so a None reason is dropped rather than stringified
    to the literal ``"None"``.
    """
    return Stopped(reason) if reason is not None else Stopped()


def _task_stop_reason(task: "Task | None") -> str | None:
    """Read *task*'s stop reason, tolerating Task implementations without one.

    The built-in tasks record ``_stop_reason``; a hand-rolled Task that merely
    satisfies the protocol need not, so a missing attribute reads as None.
    """
    return getattr(task, "_stop_reason", None)


def _stop_child(task: "Task", reason: str | None) -> None:
    """Stop *task*, forwarding *reason*, tolerating a reason-less ``stop()``.

    The Task protocol declares ``stop(reason=None)``, but a hand-rolled Task may
    predate the reason parameter and define ``stop()`` with no argument. Such a
    task still satisfies the protocol structurally, so a cascade must not break
    on it: a binding ``TypeError`` (raised before the body runs, so there is no
    double-stop) falls back to a reason-less stop.
    """
    try:
        task.stop(reason)
    except TypeError:
        task.stop()


class MultiException(Exception):
    """Aggregate of several child exceptions raised by a MultiTask.

    Raised by ``MultiTask.wait()`` when more than one child task failed: a
    single failure re-raises that child's own exception directly, so a
    ``MultiException`` always carries at least two. ``exceptions`` holds the
    child exceptions in task order; the message combines *message* with each
    child's string form so a log line shows what actually went wrong.
    """

    def __init__(self, message: str, exceptions: Iterable[BaseException]) -> None:
        self.exceptions = list(exceptions)
        detail = "; ".join(f"{type(e).__name__}: {e}" for e in self.exceptions)
        super().__init__(f"{message}: {detail}" if detail else message)


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
        self,
        name: str = "semantic_stack",
        *,
        required: Iterable[str] = (),
        tag_exceptions: bool = False,
    ) -> None:
        self._label = name
        self._required: tuple[str, ...] = tuple(required)
        # When set, an exception unwinding out of a __call__ block is tagged with
        # the name-chain that was active at the raise site (see __call__). This is
        # what lets a log emitted far above the failure still report where it
        # actually happened, after the block has unwound.
        self._tag_exceptions = tag_exceptions
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
        except BaseException as exc:
            # The block failed. Capture the chain *before* the finally pops this
            # frame, so the recorded narrative includes the raise site. Outer
            # blocks unwind afterward and find the tag already set, so the
            # innermost (richest) chain is the one that sticks.
            if self._tag_exceptions:
                _tag_exception(exc, self.collect("name"))
            raise
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
        """Capture the current stack state as pure data for later restoration.

        The returned snapshot holds only the captured frames — no reference to
        this stack — so it is picklable (when its values are) and can be
        restored into any same-shaped stack via ``stack.restore()``.
        """
        return SemanticSnapshot(self._var.get())

    @contextlib.contextmanager
    def restore(
        self, snapshot_or_frames: "SemanticSnapshot | Iterable[dict[str, Any]]"
    ) -> Iterator[None]:
        """Install the given frames onto THIS stack for the block, then reset.

        Accepts either a ``SemanticSnapshot`` or an iterable of frame dicts
        (e.g. the output of ``frames()``, which after a serialization round-trip
        is a *list* of dicts) — both list and tuple forms are handled.

        Restoring replaces whatever frames are currently on the stack for the
        duration of the block; on exit the previous state is reset. A thread
        that already had frames sees them hidden — not merged — while the
        restored frames are active.

        Restoration is a REPLAY of already-validated frames, so it BYPASSES the
        ``required``-key check that ``__call__`` enforces: the frames may have
        been produced by a stack with different required keys (or by another
        process entirely), and must not be re-validated here.
        """
        if isinstance(snapshot_or_frames, SemanticSnapshot):
            frames = snapshot_or_frames._frames
        else:
            # An iterable of dicts (list or tuple), e.g. from a serialized
            # frames() payload. Convert each to a structurally-immutable Frame.
            frames = tuple(tuple(d.items()) for d in snapshot_or_frames)
        token = self._var.set(frames)
        try:
            yield
        finally:
            self._var.reset(token)


class SemanticSnapshot:
    """An immutable, pure-data capture of a stack's frames at a point in time.

    Holds only the captured frames — no reference to the originating stack — so
    it is picklable when its frame values are picklable, and can therefore cross
    both thread and process boundaries. Restore it onto any stack via
    ``stack.restore(snapshot)``.
    """

    def __init__(self, frames: tuple[Frame, ...]) -> None:
        self._frames = frames

    def frames(self) -> tuple[dict[str, Any], ...]:
        """Return the captured frames as fresh dicts, outermost-first.

        Mirrors ``SemanticStack.frames()`` output shape, for callers that want
        to serialize the snapshot's contents.
        """
        return tuple(dict(frame) for frame in self._frames)


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

# The attribute under which an exception carries the throughline name-chain that
# was active where it was raised. Read back by ThroughlineNameFilter so a log
# emitted above the failure still reports where it actually happened.
_THROUGHLINE_EXC_ATTR = "_gentletask_throughline"


def _tag_exception(exc: BaseException, chain: tuple[str, ...]) -> None:
    """Record *chain* on *exc* as its raise-site throughline, first writer wins.

    The innermost ``throughline`` block to unwind tags the exception; outer
    blocks find the tag already present and leave it, so the most specific chain
    survives. Best-effort: a few C-level exceptions reject attribute assignment,
    in which case the tag is simply skipped.
    """
    if getattr(exc, _THROUGHLINE_EXC_ATTR, None) is not None:
        return
    try:
        setattr(exc, _THROUGHLINE_EXC_ATTR, chain)
    except (AttributeError, TypeError):
        pass


# A continuous thread of meaning running through all concurrent execution.
# Names only: this is the fun, human-readable narrative. Every frame must
# carry a `name`. Logging filters and user code share this one instance.
# tag_exceptions=True so an error logged above where it was raised still carries
# the narrative from the raise site.
throughline = SemanticStack("throughline", required=("name",), tag_exceptions=True)

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
    def stop(self, reason: str | None = None) -> None: ...
    def add_finish_callback(
        self, fn: Callable[[Any, BaseException | None], Any]
    ) -> None: ...
    def add_stop_callback(self, fn: Callable[[], Any]) -> None: ...
    def remove_stop_callback(self, fn: Callable[[], Any]) -> None: ...
    def detach(self, raise_errors: bool = False) -> None: ...


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


def _record_throughline(record: logging.LogRecord) -> tuple[str, ...]:
    """The throughline to tag *record* with: raise-site chain if any, else live.

    A record logging an exception (``exc_info`` set) prefers the chain captured
    where that exception was raised — otherwise an error logged above the
    failure would report only the unrelated chain active at the logging call.
    Records with no such exception fall back to the currently active chain.
    """
    exc_info = record.exc_info
    if isinstance(exc_info, tuple) and exc_info[1] is not None:
        captured = getattr(exc_info[1], _THROUGHLINE_EXC_ATTR, None)
        if captured is not None:
            return captured
    return task_chain()


class ThroughlineNameFilter(logging.Filter):
    """Inject the throughline name-chain into each log record as ``throughline``."""

    def filter(self, record: logging.LogRecord) -> bool:
        # Only set if absent: the originating process tags the record with its
        # own throughline, and a record may be re-handled elsewhere (e.g. a log
        # server re-emitting records received from other processes) where the
        # current throughline is unrelated. First writer — the emitter — wins.
        if not hasattr(record, "throughline"):
            record.throughline = _record_throughline(record)
        return True


# ---------------------------------------------------------------------------
# Stop-aware blocking primitives
# ---------------------------------------------------------------------------

_DEFAULT_POLL_INTERVAL = 0.05


def _remaining(deadline: float | None) -> float | None:
    """Seconds left until *deadline*, or None when there is no deadline.

    A small helper so the Condition-wait loops below don't each re-spell the
    ``None if deadline is None else deadline - now`` dance.
    """
    return None if deadline is None else deadline - time.monotonic()


def _condition_waker(cond: threading.Condition) -> Callable[[], None]:
    """Return a zero-arg callback that notifies all waiters on *cond*.

    Used as the *wake* passed to ``_stop_waker``: when the task is stopped, the
    waiter parked on *cond* is notified and re-checks its stop flag.
    """

    def wake() -> None:
        with cond:
            cond.notify_all()

    return wake


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
        raise _stopped(_task_stop_reason(task))


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
            raise _stopped(_task_stop_reason(task))
        # woken.wait() returns True only if stop() set the event; a timeout
        # (the full sleep elapsed without a stop) returns False. A non-finite
        # duration means "wait until stopped" — Event.wait cannot take an
        # infinite timeout, so pass None to block until the stop signal arrives.
        timeout = None if seconds == float("inf") else seconds
        if woken.wait(timeout):
            raise _stopped(_task_stop_reason(task))


def poll(
    fn: Callable[[], Any],
    *,
    interval: float = _DEFAULT_POLL_INTERVAL,
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
                    remaining = _remaining(deadline)
                    if remaining is not None and remaining <= 0:
                        raise _queue.Empty()
                    self._cond.wait(remaining)
                item = self._items.popleft()
                self._cond.notify_all()  # wake a putter blocked on a full queue
                return item

        # Stop-aware path. Register the waker *before* taking self._cond so an
        # already-stopped task fires wake() without re-entering our held lock.
        deadline = None if timeout is None else time.monotonic() + timeout
        with _stop_waker(_condition_waker(self._cond)):
            with self._cond:
                while True:
                    if task.is_stopped:
                        raise _stopped(_task_stop_reason(task))
                    if self._items:
                        item = self._items.popleft()
                        self._cond.notify_all()  # wake a putter waiting for space
                        return item
                    remaining = _remaining(deadline)
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
                    remaining = _remaining(deadline)
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
                    remaining = _remaining(deadline)
                    if remaining is not None and remaining <= 0:
                        return self._flag
                    self._cond.wait(remaining)
                return True

        # Stop-aware path. Register the waker before taking self._cond (see Queue).
        deadline = None if timeout is None else time.monotonic() + timeout
        with _stop_waker(_condition_waker(self._cond)):
            with self._cond:
                while not self._flag:
                    if task.is_stopped:
                        raise _stopped(_task_stop_reason(task))
                    remaining = _remaining(deadline)
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
    """State and behavior shared by every concrete Task.

    Holds the done/stop events, child registry, result/exception, and the
    finish-callback machinery. Subclasses provide their own completion path: a
    body run in a thread (ThreadTask), external completion (ManualTask, also
    used by WorkerThread to back queued jobs), or aggregation of children
    (MultiTask).
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
        # The reason recorded by the first stop(), or None for a reason-less
        # stop. Read via the stop_reason property; threaded into the Stopped
        # raised at each stop-aware site so logs can say WHY work was stopped.
        self._stop_reason: str | None = None
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
        # This task's own Timeout subclass, built lazily by the Timeout property
        # the first time it is needed (most tasks never time out).
        self._timeout_cls: type[_Timeout] | None = None

        if on_finish is not None:
            self._callbacks.append(on_finish)

    # -- public interface ----------------------------------------------------

    @property
    def is_done(self) -> bool:
        return self._done.is_set()

    @property
    def Timeout(self) -> type[_Timeout]:
        """This task's own ``Timeout`` subclass, for catching only its deadline.

        ``wait(timeout=...)`` raises ``self.Timeout``, so ``except
        some_task.Timeout`` catches only *that* task's deadline — never a
        ``Timeout`` that propagated up from an inner ``wait`` as this task's
        result. Built once per task and cached. For a broader catch (any wait
        deadline, or an OS-level timeout), fall back to the builtin
        ``TimeoutError``, which every ``task.Timeout`` subclasses.
        """
        cls = self._timeout_cls
        if cls is None:
            with self._lock:
                cls = self._timeout_cls
                if cls is None:
                    cls = type("Timeout", (_Timeout,), {})
                    self._timeout_cls = cls
        return cls

    @property
    def is_stopped(self) -> bool:
        return self._stop_requested.is_set()

    @property
    def stop_reason(self) -> str | None:
        """The reason passed to the first stop(), or None for a reason-less stop."""
        return self._stop_reason

    @property
    def result(self) -> Any:
        return self.wait()

    def wait(self, timeout: float | None = None) -> Any:
        """Block until done, re-raising any worker exception.

        If called from inside another task, a stop request on that parent
        propagates to this task: the parent wakes this wait the moment it is
        stopped (poll-free), and we then stop ourselves and raise Stopped.

        With a *timeout*, this raises ``self.Timeout`` if the task is not done
        by the deadline — it never returns to signal that. So a returned value
        (incl. ``None``) always means the task finished, a ``Stopped`` means a
        stop cascaded in, and a timeout means the deadline elapsed; the three
        are never confused. Because the raised class is per-task, ``except
        self.Timeout`` catches only this task's deadline, not a timeout
        re-raised from a failed task body; fall back to the builtin
        ``TimeoutError`` for a broader catch. ``timeout=None`` waits forever and
        so never times out; ``timeout=0`` raises at once unless already done.
        The ``result`` property waits without a timeout, so it never times out.
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
                    remaining = _remaining(deadline)
                    if remaining is not None and remaining <= 0:
                        break  # timed out
                    self._cond.wait(remaining)
        finally:
            if watch_parent:
                parent.remove_stop_callback(_nudge)

        if stopped_by_parent:
            self.stop(_task_stop_reason(parent))
            raise _stopped(_task_stop_reason(parent))
        if not self.is_done:
            exc = self.Timeout(f"timed out after {timeout}s waiting for {self!r}")
            exc.task = self
            raise exc
        if self._exception is not None:
            raise self._exception
        return self._result

    def stop(self, reason: str | None = None) -> None:
        """Request cooperative stop; fire stop callbacks; cascade to children.

        Idempotent: the stop callbacks fire exactly once, on the first stop, and
        only that first stop records *reason*. A later stop() is a no-op and does
        not overwrite the reason. The reason cascades to children so a parent
        stop explains itself all the way down.
        """
        with self._lock:
            if self._stop_requested.is_set():
                return  # already stopped; callbacks fired on the first stop
            self._stop_reason = reason
            self._stop_requested.set()
            children = list(self._children)
            callbacks, self._stop_callbacks = list(self._stop_callbacks), []
        # Fire callbacks and cascade outside the lock: a stop callback typically
        # acquires another object's lock to wake a waiter, and a child's stop()
        # takes the child's lock — holding ours here would invite deadlock.
        for cb in callbacks:
            try:
                cb()
            except Exception:
                _logger.exception("stop callback raised")
        for child in children:
            _stop_child(child, reason)

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

    def detach(self, raise_errors: bool = False) -> None:
        """Remove this task from the calling parent's stop propagation.

        Only a parent may detach one of its own children: the caller's
        current_task() must be the task whose children include this one. A
        task therefore cannot detach itself (it is not its own child), and
        detaching a task that is not the caller's child is rejected. Parents
        own this decision; a task meant to outlive its creator should instead
        be created with ``detach=True``.

        Detaching does not touch the throughline. A detached task keeps its
        narrative ancestry, so logs can still say its work was started in
        service of the operation that spawned it.

        With ``raise_errors=True`` any exception from this task (other than
        ``Stopped``) is re-raised loudly on a background daemon thread so it
        surfaces through the process's unhandled-exception hook rather than
        being silently discarded.
        """
        parent = current_task()
        if (
            parent is None
            or not hasattr(parent, "_children")
            or self not in parent._children
        ):
            raise RuntimeError(
                "detach() may only be called by the parent task whose "
                "children include this task"
            )
        self._detach = True
        parent._children.discard(self)
        if raise_errors:
            _raise_errors_impl(self)

    # -- internals -----------------------------------------------------------

    def _register_with_parent(self) -> None:
        parent = current_task()
        if parent is not None and not self._detach and hasattr(parent, "_children"):
            parent._children.add(self)

    def _finish(self, result: Any = None, exc: BaseException | None = None) -> None:
        """Complete the task; the first completion wins, later calls are no-ops.

        Recording the result/exception and marking done happen together under
        the lock, so concurrent completers — e.g. ``resolve()`` racing
        ``fail()``, or a stop callback resolving a ManualTask while ``stop()``
        injects ``Stopped`` — cannot interleave and overwrite each other's
        outcome. Waiters are woken poll-free; callbacks run after the lock is
        released (a finish callback may take other locks).
        """
        with self._cond:
            if self._done.is_set():
                return
            self._result = result
            self._exception = exc
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
                _logger.exception("finish callback raised")

    def _stop_running_children(self) -> None:
        with self._lock:
            children = list(self._children)
            reason = self._stop_reason
        for child in children:
            if not child.is_done:
                _stop_child(child, reason)


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
        raise_errors: bool = False,
        on_finish: Callable[[Any, BaseException | None], Any] | None = None,
        start: bool = True,
    ) -> None:
        """Run fn in a new daemon thread.

        With ``start=True`` (default) the thread is created and launched here,
        as before. With ``start=False`` the thread is created but not launched;
        the caller must call ``.start()`` to begin work. Deferred start lets a
        caller attach finish/stop callbacks or connect signals BEFORE any work
        runs, race-free: construct, register callbacks, then ``start()``. (This
        is what acq4's Qt bridge needs: construct the QObject, connect Qt
        signals, then start.) ``asynch()`` always starts immediately.
        """
        super().__init__(fn, tuple(args), kwargs or {}, name, on_finish)
        self._detach = detach
        self._waited = False
        self._started = False

        # Context capture and parent registration both happen at construction
        # time, NOT at start() time, regardless of `start`. The task inherits
        # the context of the code that *created* it (copy_context here), and
        # registers in the parent's child set immediately so a parent stop
        # reaches even a not-yet-started child.
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
        if start:
            self.start()
        if raise_errors:
            _raise_errors_impl(self)

    def start(self) -> None:
        """Launch the daemon thread. Idempotent — extra calls are a no-op.

        Calling start() on a task created with ``start=True`` (already running)
        is also a no-op, so the method is always safe to call.
        """
        with self._lock:
            if self._started:
                return
            self._started = True
        self._thread.start()

    def wait(self, timeout: float | None = None) -> Any:
        self._waited = True
        return super().wait(timeout)

    # -- internals -----------------------------------------------------------

    def _run(self) -> None:
        with task_context(self, self._name):
            result = None
            exc: BaseException | None = None
            try:
                if self.is_stopped:
                    raise _stopped(self._stop_reason)
                result = self._fn(*self._args, **self._kwargs)
            except BaseException as e:
                exc = e
            finally:
                self._stop_running_children()
                self._finish(result=result, exc=exc)

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
    raise_errors: bool = False,
    on_finish: Callable[[Any, BaseException | None], Any] | None = None,
) -> Callable[..., ThreadTask]:
    """Return a callable that starts *fn* in a new ThreadTask when called.

    Usage:
        task = asynch(fn)(*args, **kwargs)
        task = asynch(fn, name="my task")(*args, **kwargs)
    """

    def wrapper(*args: Any, **kwargs: Any) -> ThreadTask:
        return ThreadTask(
            fn, args, kwargs, name=name, detach=detach, raise_errors=raise_errors, on_finish=on_finish
        )

    # Record the original callable so synch() can de-wrap back to it.
    wrapper._asynch_wraps = fn
    return wrapper


def raise_errors(
    task: Task,
    message: str = "background task {name!r} failed: {error}",
) -> None:
    """Surface a discarded task's failure loudly from a daemon thread.

    Spawns a lightweight daemon thread that waits on *task* and, if the task
    fails with anything other than ``Stopped``, re-raises the error on that
    thread so the process's unhandled-exception hook reports it.

    *message* is a format string supporting the keys ``{name}`` (the task's
    name), ``{error}`` (the exception's string form), and ``{stack}`` (the
    caller's traceback at the time ``raise_errors`` was called).

    The monitor thread sets ``_waited`` on the task, which prevents
    ``ThreadTask.__del__`` from stopping a detached task before it finishes.
    """
    _raise_errors_impl(task, message)


def _raise_errors_impl(
    task: Task,
    message: str = "background task {name!r} failed: {error}",
) -> None:
    """Internal implementation shared by raise_errors() and the detach/asynch flags."""
    caller_stack = "".join(traceback.format_stack()[:-1])
    task_name = getattr(task, "_name", repr(task))

    def _monitor() -> None:
        try:
            task.wait()
        except Stopped:
            return
        except BaseException as exc:
            msg = message.format(name=task_name, error=str(exc), stack=caller_stack)
            raise RuntimeError(msg) from exc

    threading.Thread(
        target=_monitor,
        daemon=True,
        name=f"error-monitor({task_name})",
    ).start()


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
    func = getattr(fn, "__func__", None)
    instance = getattr(fn, "__self__", None)
    if func is not None and instance is not None and hasattr(func, "_asynch_wraps"):
        # A bound method of an asynch-wrapped function: its attribute access
        # proxies to the underlying wrapper, so de-wrapping naively would yield
        # the *unbound* original and drop ``self``. Re-apply the binding.
        wraps = func._asynch_wraps

        def target(*args: Any, **kwargs: Any) -> Any:
            return wraps(instance, *args, **kwargs)

    else:
        target = getattr(fn, "_asynch_wraps", fn)

    def wrapper(*args: Any, **kwargs: Any) -> Any:
        result = target(*args, **kwargs)
        if isinstance(result, Task):
            return result.wait()
        return result

    return wrapper


# ---------------------------------------------------------------------------
# ManualTask
# ---------------------------------------------------------------------------


class ManualTask(_TaskCore):
    """A Task with no body of its own, completed manually. Implements the Task protocol.

    Where ThreadTask is *body-driven* — a callable runs and its return value (or
    exception) finishes the task — a ManualTask is *manually completed*: it has
    no body, and something outside it finishes it by calling ``resolve()`` or
    ``fail()``. Two kinds of producer drive one:

    - An already-existing producer that is going to fire anyway — a hardware
      monitor thread that notices a target reached, a socket-reply reader, a GUI
      callback, a lock loop. Wrapping those in a ThreadTask would mean a useless
      parking thread per result; a ManualTask is just the shared completion
      state, completed in-place when the producer fires.
    - A ``WorkerThread``: ``submit()`` hands back a ManualTask and the worker
      runs the submitted callable on its own thread, resolving the task with the
      return value or failing it with the exception. The body is the worker's
      private mechanism; to the submitter and to waiters it is an externally
      completed task like any other.

    A ManualTask otherwise participates fully in the Task protocol and the stop
    hierarchy: it registers with the task that created it, so a parent stop
    cascades to it; ``wait()`` blocks poll-free until completion and returns the
    resolved value, re-raises a failed exception, or raises ``Stopped`` for a
    stopped task; finish and stop callbacks behave exactly as on the other
    tasks.
    """

    def __init__(
        self,
        name: str | None = None,
        *,
        on_finish: Callable[[Any, BaseException | None], Any] | None = None,
    ) -> None:
        # A ManualTask has no body — fn is unused. Pass a sensible default name
        # so the _TaskCore name fallback never has to stringify a None callable.
        super().__init__(
            fn=None, args=(), kwargs={}, name=name or "ManualTask", on_finish=on_finish
        )
        # Register immediately so the creating task's stop cascades here.
        self._register_with_parent()

    def resolve(self, value: Any = None) -> None:
        """Complete the task successfully with *value*.

        Idempotent: a no-op if the task is already resolved, failed, or stopped.
        Otherwise wakes any waiters poll-free and fires finish callbacks with
        ``(value, None)``. ``_finish`` is the single completion gate, so the
        first of several racing completers wins.
        """
        self._finish(result=value)

    def fail(self, exc: BaseException) -> None:
        """Complete the task with an exception.

        Idempotent: a no-op if the task is already done. Otherwise wakes any
        waiters poll-free; ``wait()`` will re-raise *exc* and finish callbacks
        fire with ``(None, exc)``. ``_finish`` is the single completion gate, so
        the first of several racing completers wins.
        """
        self._finish(exc=exc)

    def stop(self, reason: str | None = None) -> None:
        """Request stop, then complete the task with ``Stopped``.

        A stopped ManualTask has no body to raise ``Stopped``, so it must
        complete itself or its waiters would hang forever. ``super().stop()``
        runs first: it sets the stop flag (recording *reason*), fires stop
        callbacks (so a producer can abort its side-effects), and cascades to
        children. A stop callback may legitimately resolve or fail the task;
        ``_finish`` is idempotent, so the injected ``Stopped`` is a no-op when
        the task is already complete and otherwise wins. The injected
        ``Stopped`` carries the recorded reason so a stopped task's waiters learn
        WHY. Idempotent via ``super().stop()``'s own guard.
        """
        super().stop(reason)
        self._finish(exc=_stopped(self._stop_reason))

    def __repr__(self) -> str:
        status = (
            "done" if self.is_done else ("stopped" if self.is_stopped else "pending")
        )
        return f"<ManualTask {self._name!r} {status}>"


# ---------------------------------------------------------------------------
# MultiTask
# ---------------------------------------------------------------------------


class MultiTask(_TaskCore):
    """A bodyless Task that completes when ALL its child tasks complete.

    Like ``ManualTask``, a MultiTask has no body and spawns no thread; it is driven
    entirely by its children's finish callbacks. It aggregates several
    already-running tasks into one waitable unit: ``wait()`` blocks until every
    child has finished, then returns the list of child results (in task order)
    or raises the combined error. It participates fully in the stop hierarchy —
    it registers with the task that created it, so a parent stop cascades to it
    and on to its children, and ``stop()`` stops every child before completing.

    Completion rule:

    - all children succeed → ``wait()`` returns the list of results, in order;
    - exactly one child fails → ``wait()`` re-raises THAT child's exception;
    - two or more fail → ``wait()`` raises ``MultiException`` whose
      ``exceptions`` holds them, in task order.
    """

    def __init__(
        self,
        tasks: Iterable[Task],
        name: str | None = None,
        *,
        on_finish: Callable[[Any, BaseException | None], Any] | None = None,
    ) -> None:
        super().__init__(
            fn=None, args=(), kwargs={}, name=name or "MultiTask", on_finish=on_finish
        )
        self._tasks: list[Task] = list(tasks)
        # Register immediately so the creating task's stop cascades here (and on
        # to the children via stop()).
        self._register_with_parent()

        n = len(self._tasks)
        self._child_results: list[Any] = [None] * n
        self._child_excs: list[BaseException | None] = [None] * n
        # Remaining count guards the construction-time race: add_finish_callback
        # fires synchronously for an already-finished child, so a child may call
        # _child_finished before the rest are registered. By initialising
        # remaining to n up front and decrementing per callback, the "all done"
        # check (remaining == 0) is correct no matter when each callback fires —
        # including entirely during this loop for all-already-done children.
        self._remaining = n
        if n == 0:
            # No children to wait on: complete immediately with an empty list.
            self._finish(result=[])
            return
        for i, task in enumerate(self._tasks):
            task.add_finish_callback(functools.partial(self._child_finished, i))

    @property
    def tasks(self) -> tuple[Task, ...]:
        """The child tasks this MultiTask aggregates, in order."""
        return tuple(self._tasks)

    def _child_finished(
        self, index: int, result: Any, exc: BaseException | None
    ) -> None:
        """Record one child's outcome; complete the MultiTask when all are in.

        Under the lock we only record the child's result/exception and decrement
        the remaining count, snapshotting what we need. The completion decision
        and ``_finish()`` happen OUTSIDE the lock (matching _TaskCore: a finish
        callback may take other locks), guarded by ``is_done`` so a stop() that
        already completed the MultiTask wins the race.
        """
        with self._lock:
            self._child_results[index] = result
            self._child_excs[index] = exc
            self._remaining -= 1
            if self._remaining != 0 or self._done.is_set():
                return
            results = list(self._child_results)
            excs = [e for e in self._child_excs if e is not None]
        if self.is_done:
            return
        if self.is_stopped or (excs and all(isinstance(e, Stopped) for e in excs)):
            # The children finished because a stop is propagating: either this
            # task was stopped directly (stop() cascades to the children), or a
            # grandparent stop reached the children before it reached us (the
            # parent's child set is unordered). Either way the outcome is a stop,
            # so report a single Stopped rather than a MultiException of the
            # children's Stoppeds. stop() also self-completes as a backstop.
            reason = self._stop_reason
            if reason is None:
                # A grandparent stop reached the children before us, so our own
                # reason is not set yet. Recover it from a child's Stopped so the
                # single Stopped we report still explains itself.
                reason = next(
                    (str(e) for e in excs if isinstance(e, Stopped) and str(e)),
                    None,
                )
            self._finish(exc=_stopped(reason))
        elif not excs:
            self._finish(result=results)
        elif len(excs) == 1:
            self._finish(exc=excs[0])
        else:
            self._finish(exc=MultiException("Multiple tasks failed", excs))

    def stop(self, reason: str | None = None) -> None:
        """Stop every child, then this task; complete with Stopped if still open.

        Each child's own stop drives ``_child_finished``, which would normally
        complete the MultiTask. But a child that does not complete on stop must
        not leave this task's waiters hanging, so — mirroring ``ManualTask.stop`` —
        we self-complete with ``Stopped`` if still incomplete afterwards. The
        injected ``Stopped`` carries the recorded reason. Idempotent via
        ``super().stop()``'s own guard.
        """
        # Set our own stop flag BEFORE cascading to the children: each child's
        # stop() drives _child_finished synchronously, and that handler checks
        # self.is_stopped to report a single Stopped rather than aggregating the
        # children's Stoppeds into a MultiException. super().stop() also records
        # the reason and fires our stop callbacks.
        super().stop(reason)
        for t in self._tasks:
            _stop_child(t, reason)
        self._finish(exc=_stopped(self._stop_reason))

    def __repr__(self) -> str:
        status = (
            "done" if self.is_done else ("stopped" if self.is_stopped else "pending")
        )
        return f"<MultiTask {self._name!r} {status} ({len(self._tasks)} tasks)>"


# ---------------------------------------------------------------------------
# WorkerThread
# ---------------------------------------------------------------------------

_WORKER_STOP = object()


class _WorkerJob:
    """A unit of work queued to a WorkerThread.

    Pairs the ManualTask handed back to the submitter with the captured callable
    and context snapshots the worker needs to run it. Keeping the body here —
    rather than on the task — lets ``submit()`` return a plain ManualTask while
    the worker retains everything required to complete it. The snapshots are
    taken at submit time so the body inherits the submitter's stacks (narrative
    throughline and task chain), not the worker's.
    """

    def __init__(
        self,
        task: "ManualTask",
        fn: Callable[..., Any],
        args: tuple,
        kwargs: dict,
        throughline_snapshot: SemanticSnapshot,
        task_snapshot: SemanticSnapshot,
    ) -> None:
        self.task = task
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.throughline_snapshot = throughline_snapshot
        self.task_snapshot = task_snapshot


class WorkerThread:
    """Long-lived worker thread that serialises submitted jobs."""

    def __init__(self, name: str | None = None) -> None:
        self._name = name or "WorkerThread"
        self._queue: _queue.Queue = _queue.Queue()
        self._stopping = False
        # Serialises submit() against stop() so the stop check and the enqueue
        # are atomic with respect to each other. Without it, submit() could
        # pass the stop check, stop() could then set the flag and enqueue the
        # sentinel, and submit() would enqueue its task *behind* the sentinel —
        # where it never runs and its wait() hangs forever.
        self._lock = threading.Lock()
        self._thread = threading.Thread(target=self._run, daemon=True, name=self._name)
        self._thread.start()

    def submit(
        self,
        fn: Callable,
        args: tuple = (),
        kwargs: dict | None = None,
        *,
        name: str | None = None,
    ) -> ManualTask:
        """Enqueue *fn* and return a ManualTask immediately.

        The worker completes the returned task when the job runs: it resolves
        with *fn*'s return value or fails with its exception. Both the
        throughline narrative and the task chain are snapshotted **at submit
        time** and restored around the body, so the job inherits the context of
        the code that submitted it, not the worker's.

        Raises RuntimeError if the worker has been stopped — a job submitted
        after stop() would sit behind the shutdown sentinel and never run,
        leaving its wait() to block forever.
        """
        with self._lock:
            if self._stopping:
                raise RuntimeError(f"{self._name} is stopped; cannot submit new jobs")
            # Default the task name to the callable's qualname so logs name the
            # work, not the bare "ManualTask" placeholder.
            job_name = name or getattr(fn, "__qualname__", repr(fn))
            task = ManualTask(name=job_name)
            job = _WorkerJob(
                task,
                fn,
                tuple(args),
                dict(kwargs or {}),
                throughline.snapshot(),
                _task_stack.snapshot(),
            )
            self._queue.put(job)
        return task

    def stop(self) -> None:
        """Drain already-queued jobs, then shut the thread down.

        Jobs enqueued before this call still run (the sentinel is FIFO behind
        them); further submit() calls are rejected. Idempotent.
        """
        with self._lock:
            if self._stopping:
                return
            self._stopping = True
            self._queue.put(_WORKER_STOP)

    # -- internals -----------------------------------------------------------

    def _run(self) -> None:
        while True:
            item = self._queue.get()
            if item is _WORKER_STOP:
                break

            job: _WorkerJob = item
            task = job.task
            if task.is_done:
                # Resolved, failed, or stopped before it ran — nothing to do.
                # A stop already self-completed the task with Stopped (see
                # ManualTask.stop), so there is no body to run.
                continue

            # Restore the submitter's stacks, then run the body as this task.
            # The captured callable's return value resolves the task; an
            # exception fails it. If the task was stopped mid-run, _finish has
            # already completed it with Stopped, so resolve()/fail() here is a
            # no-op (the first completion wins).
            with (
                throughline.restore(job.throughline_snapshot),
                _task_stack.restore(job.task_snapshot),
            ):
                with task_context(task, task._name):
                    result = None
                    exc: BaseException | None = None
                    try:
                        result = job.fn(*job.args, **job.kwargs)
                    except BaseException as e:
                        exc = e
                    finally:
                        task._stop_running_children()
                        if exc is not None:
                            task.fail(exc)
                        else:
                            task.resolve(result)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

__all__ = [
    "Stopped",
    "MultiException",
    "SemanticStack",
    "SemanticSnapshot",
    "throughline",
    "Task",
    "ThreadTask",
    "ManualTask",
    "MultiTask",
    "WorkerThread",
    "asynch",
    "synch",
    "raise_errors",
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
