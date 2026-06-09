# Tests for the gentletask v7 reference implementation.
# Covers SemanticStack/throughline, ThreadTask, WorkTask, WorkerThread,
# stop propagation, the stop-aware primitives, and the logging filter.

from __future__ import annotations

import logging
import threading
import time

import queue as _queue
import pytest

from gentletask import (
    Event,
    Queue,
    SemanticStack,
    Stopped,
    Task,
    ThreadTask,
    ThroughlineNameFilter,
    WorkTask,
    WorkerThread,
    asynch,
    check_stop,
    current_task,
    poll,
    sleep,
    synch,
    task_chain,
    task_context,
    throughline,
)
from gentletask import _task_stack


# ---------------------------------------------------------------------------
# SemanticStack
# ---------------------------------------------------------------------------


class TestSemanticStack:
    def test_get_innermost_wins(self):
        s = SemanticStack()
        with s(x=1):
            with s(x=2):
                assert s.get("x") == 2
            assert s.get("x") == 1

    def test_get_default_when_absent(self):
        s = SemanticStack()
        assert s.get("missing") is None
        assert s.get("missing", "fallback") == "fallback"

    def test_get_falls_through_frames(self):
        s = SemanticStack()
        with s(user="alice"):
            with s(operation="resize"):
                assert s.get("user") == "alice"
                assert s.get("operation") == "resize"

    def test_collect_outermost_first(self):
        s = SemanticStack()
        with s(name="outer"):
            with s(name="inner"):
                assert s.collect("name") == ("outer", "inner")

    def test_collect_skips_frames_without_key(self):
        s = SemanticStack()
        with s(name="a"):
            with s(other=1):
                with s(name="b"):
                    assert s.collect("name") == ("a", "b")

    def test_collect_empty_when_absent(self):
        s = SemanticStack()
        with s(x=1):
            assert s.collect("name") == ()

    def test_walk_outermost_first(self):
        s = SemanticStack()
        with s(name="outer"):
            with s(name="inner"):
                assert s.walk(lambda f: f.get("name", "?")) == ("outer", "inner")

    def test_walk_keeps_none_results(self):
        s = SemanticStack()
        with s(name="outer"):
            with s(other=1):
                assert s.walk(lambda f: f.get("name")) == ("outer", None)

    def test_frames_returns_fresh_dicts(self):
        s = SemanticStack()
        with s(operation="abc123", user="alice"):
            with s(operation="resize"):
                frames = s.frames()
                assert frames == (
                    {"operation": "abc123", "user": "alice"},
                    {"operation": "resize"},
                )
                # Mutating a returned dict must not corrupt the stack.
                frames[0]["operation"] = "tampered"
                assert s.get("user") == "alice"
                assert s.collect("operation") == ("abc123", "resize")

    def test_empty_stack(self):
        s = SemanticStack()
        assert s.frames() == ()
        assert s.collect("anything") == ()
        assert s.get("anything") is None

    def test_independent_instances_are_isolated(self):
        a = SemanticStack("a")
        b = SemanticStack("b")
        with a(name="in-a"):
            assert a.collect("name") == ("in-a",)
            assert b.collect("name") == ()

    def test_frame_removed_on_exit(self):
        s = SemanticStack()
        with s(name="temp"):
            assert s.collect("name") == ("temp",)
        assert s.collect("name") == ()


class TestRequiredKeys:
    def test_missing_required_key_raises(self):
        s = SemanticStack(required=("name",))
        with pytest.raises(ValueError, match="required key"):
            with s(other=1):
                pass

    def test_required_key_present_is_ok(self):
        s = SemanticStack(required=("name",))
        with s(name="ok"):
            assert s.get("name") == "ok"

    def test_extra_keys_allowed_alongside_required(self):
        s = SemanticStack(required=("name",))
        with s(name="ok", extra="also-fine"):
            assert s.get("extra") == "also-fine"

    def test_multiple_required_keys_all_listed_when_missing(self):
        s = SemanticStack(required=("a", "b"))
        with pytest.raises(ValueError, match="a, b"):
            with s(c=1):
                pass

    def test_no_required_keys_allows_empty_frame(self):
        s = SemanticStack()
        with s():
            assert s.frames() == ({},)


class TestTwoStackDesign:
    def test_throughline_requires_name(self):
        with pytest.raises(ValueError, match="throughline"):
            with throughline(note="no name here"):
                pass

    def test_task_stack_requires_task(self):
        with pytest.raises(ValueError, match="_task_stack"):
            with _task_stack(name="no task here"):
                pass

    def test_task_context_populates_both_stacks(self):
        sentinel = object()
        with task_context(sentinel, "the-name"):
            assert current_task() is sentinel
            assert throughline.collect("name") == ("the-name",)

    def test_throughline_carries_no_task_during_task(self):
        # The split means the task object lives only on _task_stack, never
        # leaking into the name-only throughline.
        seen = {}

        def fn():
            seen["throughline_frames"] = throughline.frames()
            seen["task"] = current_task()

        t = ThreadTask(fn, name="worker")
        t.wait()
        assert seen["task"] is t
        assert all("task" not in f for f in seen["throughline_frames"])
        assert seen["throughline_frames"][-1] == {"name": "worker"}


class TestSemanticSnapshot:
    def test_restore_installs_captured_frames(self):
        s = SemanticStack()
        with s(name="captured"):
            snap = s.snapshot()
        # Outside the original frame the stack is empty again...
        assert s.collect("name") == ()
        # ...but restore brings the captured frames back.
        with snap.restore():
            assert s.collect("name") == ("captured",)
        assert s.collect("name") == ()

    def test_restore_replaces_existing_frames(self):
        # Restoring into a thread that already has frames hides them for the
        # duration of the block rather than merging.
        s = SemanticStack()
        with s(name="captured"):
            snap = s.snapshot()
        with s(name="live"):
            assert s.collect("name") == ("live",)
            with snap.restore():
                assert s.collect("name") == ("captured",)
            # Live frame comes back after the snapshot block ends.
            assert s.collect("name") == ("live",)

    def test_can_push_on_top_of_restored_snapshot(self):
        s = SemanticStack()
        with s(name="base"):
            snap = s.snapshot()
        with snap.restore():
            with s(name="added"):
                assert s.collect("name") == ("base", "added")
            assert s.collect("name") == ("base",)


# ---------------------------------------------------------------------------
# Task Protocol
# ---------------------------------------------------------------------------


class TestTaskProtocol:
    def test_thread_task_is_task(self):
        t = ThreadTask(lambda: None)
        t.wait()
        assert isinstance(t, Task)

    def test_work_task_is_task(self):
        worker = WorkerThread()
        wt = worker.submit(lambda: None)
        wt.wait()
        worker.stop()
        assert isinstance(wt, Task)

    def test_protocol_requires_stop_callbacks(self):
        # A class missing the stop-callback hooks is not a Task: poll-free stop
        # propagation depends on every task being able to wake its waiters.
        class WithoutHooks:
            is_done = True
            is_stopped = False
            result = None

            def wait(self, timeout=None):
                return None

            def stop(self):
                pass

            def add_finish_callback(self, fn):
                pass

            def detach(self):
                pass

        assert not isinstance(WithoutHooks(), Task)


# ---------------------------------------------------------------------------
# Stop callback registry — the poll-free propagation mechanism
# ---------------------------------------------------------------------------


class TestStopCallbacks:
    def test_callback_fires_on_stop(self):
        fired = []
        t = ThreadTask(lambda: sleep(10))
        t.add_stop_callback(lambda: fired.append(True))
        time.sleep(0.05)
        t.stop()
        with pytest.raises(Stopped):
            t.wait(timeout=1.0)
        assert fired == [True]

    def test_callback_fires_immediately_if_already_stopped(self):
        fired = []
        t = ThreadTask(lambda: sleep(10))
        time.sleep(0.05)
        t.stop()
        t.add_stop_callback(lambda: fired.append(True))
        assert fired == [True]
        with pytest.raises(Stopped):
            t.wait(timeout=1.0)

    def test_callback_fires_once_for_repeated_stop(self):
        fired = []
        t = ThreadTask(lambda: sleep(10))
        t.add_stop_callback(lambda: fired.append(True))
        time.sleep(0.05)
        t.stop()
        t.stop()
        with pytest.raises(Stopped):
            t.wait(timeout=1.0)
        assert fired == [True]

    def test_remove_stop_callback(self):
        fired = []
        cb = lambda: fired.append(True)
        t = ThreadTask(lambda: sleep(10))
        t.add_stop_callback(cb)
        t.remove_stop_callback(cb)
        time.sleep(0.05)
        t.stop()
        with pytest.raises(Stopped):
            t.wait(timeout=1.0)
        assert fired == []

    def test_work_task_stop_callback_fires(self):
        worker = WorkerThread()
        fired = []
        wt = worker.submit(lambda: sleep(10))
        wt.add_stop_callback(lambda: fired.append(True))
        time.sleep(0.05)
        wt.stop()
        with pytest.raises(Stopped):
            wt.wait(timeout=1.0)
        worker.stop()
        assert fired == [True]


# ---------------------------------------------------------------------------
# Poll-free wake — long waits with no polling interval still unblock on stop
# ---------------------------------------------------------------------------


class TestPollFreeWake:
    def test_sleep_wakes_on_stop_without_interval(self):
        # A poll-free sleep blocks on the stop signal itself, so even a very
        # long sleep unblocks promptly on stop — latency is not tied to any
        # polling interval.
        def fn():
            sleep(30)

        t = ThreadTask(fn)
        time.sleep(0.05)
        start = time.monotonic()
        t.stop()
        with pytest.raises(Stopped):
            t.wait(timeout=1.0)
        assert time.monotonic() - start < 0.5

    def test_event_wait_wakes_on_stop_with_no_timeout(self):
        e = Event()

        def fn():
            e.wait()  # timeout=None — blocks indefinitely until set or stopped

        t = ThreadTask(fn)
        time.sleep(0.05)
        start = time.monotonic()
        t.stop()
        with pytest.raises(Stopped):
            t.wait(timeout=1.0)
        assert time.monotonic() - start < 0.5

    def test_queue_get_wakes_on_stop_with_no_timeout(self):
        q = Queue()

        def fn():
            q.get()  # timeout=None — blocks indefinitely until item or stopped

        t = ThreadTask(fn)
        time.sleep(0.05)
        start = time.monotonic()
        t.stop()
        with pytest.raises(Stopped):
            t.wait(timeout=1.0)
        assert time.monotonic() - start < 0.5


# ---------------------------------------------------------------------------
# ThreadTask — basic behavior
# ---------------------------------------------------------------------------


class TestThreadTask:
    def test_runs_fn_and_returns_result(self):
        t = ThreadTask(lambda: 42)
        assert t.wait() == 42

    def test_result_property_blocks(self):
        t = ThreadTask(lambda: "hello")
        assert t.result == "hello"

    def test_is_done_after_wait(self):
        t = ThreadTask(lambda: None)
        t.wait()
        assert t.is_done

    def test_exception_propagates_through_wait(self):
        def boom():
            raise ValueError("boom")

        t = ThreadTask(boom)
        with pytest.raises(ValueError, match="boom"):
            t.wait()

    def test_name_from_callable(self):
        def my_func():
            pass

        t = ThreadTask(my_func)
        t.wait()
        assert t._name == my_func.__qualname__

    def test_explicit_name(self):
        t = ThreadTask(lambda: None, name="custom")
        t.wait()
        assert t._name == "custom"

    def test_args_and_kwargs(self):
        t = ThreadTask(lambda x, y=0: x + y, args=(3,), kwargs={"y": 4})
        assert t.wait() == 7

    def test_on_finish_callback(self):
        results = []
        t = ThreadTask(lambda: 7, on_finish=lambda r, e: results.append((r, e)))
        t.wait()
        assert results == [(7, None)]

    def test_add_finish_callback(self):
        results = []
        t = ThreadTask(lambda: 5)
        t.add_finish_callback(lambda r, e: results.append(r))
        t.wait()
        assert results == [5]

    def test_add_finish_callback_after_done(self):
        t = ThreadTask(lambda: 3)
        t.wait()
        results = []
        t.add_finish_callback(lambda r, e: results.append(r))
        assert results == [3]

    def test_wait_timeout_returns_none(self):
        barrier = threading.Event()
        t = ThreadTask(barrier.wait)
        result = t.wait(timeout=0.05)
        barrier.set()
        assert result is None

    def test_stop_before_run_marks_stopped(self):
        barrier = threading.Barrier(2)

        def fn():
            barrier.wait()

        t = ThreadTask(fn)
        t.stop()
        barrier.wait()
        t.wait(timeout=1.0)
        assert t.is_stopped


# ---------------------------------------------------------------------------
# ThreadTask — stop propagation
# ---------------------------------------------------------------------------


class TestThreadTaskStop:
    def test_stop_cascades_to_child(self):
        def child_fn():
            while True:
                sleep(0.01)

        def parent_fn():
            child = ThreadTask(child_fn)
            child.wait()

        parent = ThreadTask(parent_fn)
        time.sleep(0.05)
        parent.stop()
        with pytest.raises(Stopped):
            parent.wait(timeout=1.0)
        assert parent.is_stopped

    def test_stop_inside_wait_propagates(self):
        def slow():
            sleep(10)

        def outer():
            inner = ThreadTask(slow)
            inner.wait()

        t = ThreadTask(outer)
        time.sleep(0.05)
        t.stop()
        with pytest.raises(Stopped):
            t.wait(timeout=1.0)
        assert t.is_stopped

    def test_detach_prevents_stop_cascade(self):
        child_ran = []

        def child_fn():
            time.sleep(0.1)
            child_ran.append(True)

        def parent_fn():
            child = ThreadTask(child_fn)
            child.detach()
            sleep(10)

        parent = ThreadTask(parent_fn)
        time.sleep(0.02)
        parent.stop()
        with pytest.raises(Stopped):
            parent.wait(timeout=1.0)
        # child was detached — it should finish naturally
        time.sleep(0.2)
        assert child_ran == [True]

    def test_task_cannot_detach_itself(self):
        # Detachment is the parent's decision; a task is not its own child.
        def fn():
            current_task().detach()

        t = ThreadTask(fn)
        with pytest.raises(RuntimeError, match="parent"):
            t.wait(timeout=1.0)

    def test_detach_outside_a_parent_is_rejected(self):
        # No current task means no parent to detach from.
        t = ThreadTask(lambda: sleep(10))
        try:
            with pytest.raises(RuntimeError, match="parent"):
                t.detach()
        finally:
            t.stop()

    def test_detach_of_non_child_is_rejected(self):
        # A task may only detach tasks that are its own children.
        outsider = ThreadTask(lambda: sleep(10))

        def parent_fn():
            outsider.detach()  # outsider is not a child of this task

        parent = ThreadTask(parent_fn)
        try:
            with pytest.raises(RuntimeError, match="parent"):
                parent.wait(timeout=1.0)
        finally:
            outsider.stop()

    def test_del_stops_unwaited_task(self):
        finished = threading.Event()

        def fn():
            finished.set()

        exc_ref = [None]
        t = ThreadTask(fn, on_finish=lambda r, e: exc_ref.__setitem__(0, e))
        finished.wait(timeout=1.0)
        del t
        time.sleep(0.05)
        assert exc_ref[0] is None  # finished normally; __del__ stop() was a no-op


# ---------------------------------------------------------------------------
# asynch factory
# ---------------------------------------------------------------------------


class TestAsynch:
    def test_basic_usage(self):
        task = asynch(lambda: 99)()
        assert task.wait() == 99

    def test_with_args(self):
        task = asynch(lambda x, y: x + y)(3, 4)
        assert task.wait() == 7

    def test_with_name(self):
        task = asynch(lambda: None, name="named")()
        task.wait()
        assert task._name == "named"

    def test_with_on_finish(self):
        done = []
        asynch(lambda: 1, on_finish=lambda r, e: done.append(r))()
        time.sleep(0.1)
        assert done == [1]


# ---------------------------------------------------------------------------
# synch — the inverse of asynch
# ---------------------------------------------------------------------------


class TestSynch:
    def test_plain_function_runs_and_returns_value(self):
        assert synch(lambda x: x + 1)(1) == 2

    def test_non_task_return_passed_through(self):
        assert synch(lambda: 5)() == 5

    def test_synch_of_asynch_returns_value_not_task(self):
        # asynch(f) hands back a Task when called; synch flattens both the
        # wrapper and the task to the concrete value.
        result = synch(asynch(lambda x, y: x + y))(3, 4)
        assert result == 7

    def test_synch_dewraps_asynch_and_runs_in_current_thread(self):
        # The asynch layer is de-wrapped, so the work runs inline: no new task
        # is spawned and current_task() is the caller's (None at top level).
        seen = []
        synch(asynch(lambda: seen.append(current_task())))()
        assert seen == [None]

    def test_waits_for_returned_thread_task(self):
        # A plain function that returns a ThreadTask: synch waits for it.
        def make():
            return ThreadTask(lambda: 42)

        assert synch(make)() == 42

    def test_waits_for_returned_work_task(self):
        worker = WorkerThread()

        def make():
            return worker.submit(lambda: "done")

        try:
            assert synch(make)() == "done"
        finally:
            worker.stop()

    def test_propagates_exception_from_returned_task(self):
        def boom():
            raise ValueError("nope")

        def make():
            return ThreadTask(boom)

        with pytest.raises(ValueError, match="nope"):
            synch(make)()

    def test_dewrapped_function_returning_task_is_waited(self):
        # Combine both layers: asynch-wrapped function whose body returns a task.
        def make():
            return ThreadTask(lambda: 7)

        assert synch(asynch(make))() == 7


# ---------------------------------------------------------------------------
# WorkerThread and WorkTask
# ---------------------------------------------------------------------------


class TestWorkerThread:
    def setup_method(self):
        self.worker = WorkerThread(name="test-worker")

    def teardown_method(self):
        self.worker.stop()

    def test_submit_returns_result(self):
        wt = self.worker.submit(lambda: 42)
        assert wt.wait() == 42

    def test_submit_with_args(self):
        wt = self.worker.submit(lambda x, y: x + y, (3, 4))
        assert wt.wait() == 7

    def test_submit_with_kwargs(self):
        def add(x, y=0):
            return x + y

        wt = self.worker.submit(add, (10,), {"y": 5})
        assert wt.wait() == 15

    def test_jobs_run_serially(self):
        order = []

        def job(n):
            order.append(n)
            time.sleep(0.02)

        tasks = [self.worker.submit(job, (i,)) for i in range(5)]
        for t in tasks:
            t.wait()
        assert order == list(range(5))

    def test_exception_propagates(self):
        def boom():
            raise RuntimeError("oops")

        wt = self.worker.submit(boom)
        with pytest.raises(RuntimeError, match="oops"):
            wt.wait()

    def test_is_done_after_completion(self):
        wt = self.worker.submit(lambda: None)
        wt.wait()
        assert wt.is_done

    def test_stopped_job_skipped(self):
        ran = []
        barrier = threading.Event()

        def blocker():
            barrier.wait(timeout=5)

        def job():
            ran.append(True)

        slow = self.worker.submit(blocker)
        wt = self.worker.submit(job)
        wt.stop()
        barrier.set()
        slow.wait(timeout=1.0)
        with pytest.raises(Stopped):
            wt.wait(timeout=1.0)

        assert ran == []
        assert wt.is_stopped

    def test_stop_propagates_to_child_task(self):
        def job():
            child = ThreadTask(lambda: sleep(10))
            time.sleep(0.05)
            return child

        wt = self.worker.submit(job)
        child = wt.wait(timeout=1.0)
        wt.stop()
        time.sleep(0.1)
        assert wt.is_stopped
        assert child.is_stopped


# ---------------------------------------------------------------------------
# Context propagation via throughline
# ---------------------------------------------------------------------------


class TestContextPropagation:
    def test_thread_task_inherits_chain(self):
        captured = []

        def fn():
            captured.append(task_chain())

        with throughline(name="parent"):
            t = ThreadTask(fn)
            t.wait()

        assert "parent" in captured[0]

    def test_thread_task_extends_chain(self):
        captured = []

        def fn():
            captured.append(task_chain())

        with throughline(name="outer"):
            t = ThreadTask(fn, name="inner")
            t.wait()

        assert captured[0] == ("outer", "inner")

    def test_worker_task_restores_chain(self):
        captured = []

        def job():
            captured.append(task_chain())

        worker = WorkerThread(name="ctx-worker")
        with throughline(name="caller"):
            wt = worker.submit(job, name="job")
            wt.wait()
        worker.stop()

        assert captured[0] == ("caller", "job")

    def test_worker_task_snapshots_at_submit_not_execute(self):
        # A job submitted after the frame exits does NOT inherit that frame.
        captured = []

        def job():
            captured.append(task_chain())

        worker = WorkerThread(name="snap-worker")
        with throughline(name="request_handler"):
            built = (job,)
        wt = worker.submit(built[0], name="job")
        wt.wait()
        worker.stop()

        assert captured[0] == ("job",)

    def test_current_task_inside_thread_task(self):
        captured = []

        def fn():
            captured.append(current_task())

        t = ThreadTask(fn)
        t.wait()
        assert captured[0] is t

    def test_current_task_inside_work_task(self):
        captured = []

        def job():
            captured.append(current_task())

        worker = WorkerThread()
        wt = worker.submit(job)
        wt.wait()
        worker.stop()
        assert captured[0] is wt

    def test_current_task_none_outside_task(self):
        assert current_task() is None


# ---------------------------------------------------------------------------
# ThroughlineNameFilter
# ---------------------------------------------------------------------------


class TestThroughlineNameFilter:
    def test_injects_name_chain_into_record(self):
        f = ThroughlineNameFilter()
        record = logging.LogRecord("test", logging.INFO, "", 0, "msg", (), None)
        with throughline(name="alpha"):
            with throughline(name="beta"):
                f.filter(record)
        assert record.throughline == ("alpha", "beta")

    def test_empty_chain_at_top_level(self):
        f = ThroughlineNameFilter()
        record = logging.LogRecord("test", logging.INFO, "", 0, "msg", (), None)
        f.filter(record)
        assert record.throughline == ()


# ---------------------------------------------------------------------------
# Stoppable primitives
# ---------------------------------------------------------------------------


class TestSleep:
    def test_sleep_outside_task(self):
        start = time.monotonic()
        sleep(0.05)
        assert time.monotonic() - start >= 0.04

    def test_sleep_raises_stopped(self):
        def fn():
            sleep(10)

        t = ThreadTask(fn)
        time.sleep(0.05)
        t.stop()
        with pytest.raises(Stopped):
            t.wait()

    def test_sleep_zero_seconds(self):
        def fn():
            sleep(0)

        t = ThreadTask(fn)
        t.wait()


class TestCheckStop:
    def test_check_stop_outside_task(self):
        check_stop()  # should not raise

    def test_check_stop_raises_when_stopped(self):
        barrier = threading.Event()

        def fn():
            barrier.set()
            while True:
                check_stop()
                time.sleep(0.005)

        t = ThreadTask(fn)
        barrier.wait(timeout=1.0)
        t.stop()
        with pytest.raises(Stopped):
            t.wait()


class TestQueue:
    def test_get_returns_item(self):
        q = Queue()
        q.put(1)
        assert q.get() == 1

    def test_get_raises_stopped_when_task_stopped(self):
        q = Queue()

        def fn():
            q.get()

        t = ThreadTask(fn)
        time.sleep(0.05)
        t.stop()
        with pytest.raises(Stopped):
            t.wait()

    def test_get_timeout_raises_empty(self):
        q = Queue()
        with pytest.raises(_queue.Empty):
            q.get(timeout=0.05)

    def test_get_outside_task_blocks_normally(self):
        q = Queue()
        q.put("x")
        assert q.get() == "x"


class TestEvent:
    def test_wait_returns_true_when_set(self):
        e = Event()
        e.set()
        assert e.wait() is True

    def test_wait_raises_stopped_when_task_stopped(self):
        e = Event()

        def fn():
            e.wait()

        t = ThreadTask(fn)
        time.sleep(0.05)
        t.stop()
        with pytest.raises(Stopped):
            t.wait()

    def test_wait_timeout_returns_false(self):
        e = Event()
        assert e.wait(timeout=0.05) is False

    def test_wait_outside_task(self):
        e = Event()
        e.set()
        assert e.wait(timeout=1.0) is True


class TestPoll:
    def test_returns_truthy_value(self):
        results = iter([None, None, 42])
        assert poll(lambda: next(results), interval=0.01) == 42

    def test_timeout_returns_last_value(self):
        result = poll(lambda: False, interval=0.01, timeout=0.05)
        assert result is False

    def test_raises_stopped_when_task_stopped(self):
        def fn():
            poll(lambda: False, interval=0.01)

        t = ThreadTask(fn)
        time.sleep(0.05)
        t.stop()
        with pytest.raises(Stopped):
            t.wait()


class TestPollEdges:
    def test_immediate_success_does_not_wait(self):
        calls = []

        def fn():
            calls.append(1)
            return "ready"

        assert poll(fn, interval=10.0) == "ready"
        assert calls == [1]  # truthy on the first sample; never parked

    def test_fn_exception_propagates(self):
        def fn():
            raise RuntimeError("poll boom")

        with pytest.raises(RuntimeError, match="poll boom"):
            poll(fn, interval=0.01, timeout=1.0)


# ---------------------------------------------------------------------------
# Queue — bounded mode, join, and non-blocking accessors
# ---------------------------------------------------------------------------


class TestQueueBounded:
    def test_put_nowait_raises_full(self):
        q = Queue(maxsize=1)
        q.put(1)
        with pytest.raises(_queue.Full):
            q.put_nowait(2)

    def test_put_blocks_until_space(self):
        q = Queue(maxsize=1)
        q.put(1)
        order = []

        def producer():
            order.append("before")
            q.put(2)  # blocks until the consumer makes room
            order.append("after")

        t = threading.Thread(target=producer, daemon=True)
        t.start()
        time.sleep(0.05)
        assert order == ["before"]  # still parked in put()
        assert q.get() == 1  # frees a slot
        t.join(timeout=1.0)
        assert order == ["before", "after"]
        assert q.get() == 2

    def test_put_timeout_raises_full(self):
        q = Queue(maxsize=1)
        q.put(1)
        with pytest.raises(_queue.Full):
            q.put(2, timeout=0.05)

    def test_full_reports_capacity(self):
        q = Queue(maxsize=2)
        assert not q.full()
        q.put(1)
        q.put(2)
        assert q.full()

    def test_unbounded_is_never_full(self):
        q = Queue()
        for i in range(100):
            q.put(i)
        assert not q.full()


class TestQueueJoin:
    def test_task_done_too_many_raises(self):
        q = Queue()
        with pytest.raises(ValueError, match="too many"):
            q.task_done()

    def test_join_unblocks_when_all_tasks_done(self):
        q = Queue()
        q.put("a")
        q.put("b")
        order = []

        def waiter():
            q.join()
            order.append("joined")

        t = threading.Thread(target=waiter, daemon=True)
        t.start()
        q.get()
        q.task_done()
        time.sleep(0.02)
        assert order == []  # one unfinished item remains
        q.get()
        q.task_done()
        t.join(timeout=1.0)
        assert order == ["joined"]

    def test_join_returns_immediately_when_idle(self):
        q = Queue()
        q.join()  # nothing outstanding


class TestQueueNonBlocking:
    def test_get_nowait_raises_empty(self):
        q = Queue()
        with pytest.raises(_queue.Empty):
            q.get_nowait()

    def test_get_nowait_returns_item(self):
        q = Queue()
        q.put(5)
        assert q.get_nowait() == 5

    def test_qsize_and_empty(self):
        q = Queue()
        assert q.empty()
        assert q.qsize() == 0
        q.put(1)
        assert not q.empty()
        assert q.qsize() == 1


# ---------------------------------------------------------------------------
# Event — flag state
# ---------------------------------------------------------------------------


class TestEventState:
    def test_is_set_reflects_flag(self):
        e = Event()
        assert e.is_set() is False
        e.set()
        assert e.is_set() is True

    def test_clear_then_wait_blocks_again(self):
        e = Event()
        e.set()
        assert e.wait(timeout=0.05) is True
        e.clear()
        assert e.is_set() is False
        assert e.wait(timeout=0.05) is False


# ---------------------------------------------------------------------------
# Already-stopped tasks raise at the wait site without parking
# ---------------------------------------------------------------------------


class TestAlreadyStopped:
    def test_sleep_zero_raises_when_stopped(self):
        # check_stop's "equivalent to sleep(0)" claim: sleep(0) must surrender.
        def fn():
            current_task().stop()
            sleep(0)

        t = ThreadTask(fn)
        with pytest.raises(Stopped):
            t.wait(timeout=1.0)

    def test_queue_get_raises_immediately_when_already_stopped(self):
        q = Queue()

        def fn():
            current_task().stop()
            q.get()  # must raise without deadlocking on the empty queue

        t = ThreadTask(fn)
        with pytest.raises(Stopped):
            t.wait(timeout=1.0)

    def test_event_wait_raises_immediately_when_already_stopped(self):
        e = Event()

        def fn():
            current_task().stop()
            e.wait()  # must raise without parking on the unset event

        t = ThreadTask(fn)
        with pytest.raises(Stopped):
            t.wait(timeout=1.0)


# ---------------------------------------------------------------------------
# wait() timeout, repeated result access, and post-exception callbacks
# ---------------------------------------------------------------------------


class TestWaitSemantics:
    def test_wait_timeout_then_completes(self):
        release = threading.Event()

        def fn():
            release.wait(5)
            return 99

        t = ThreadTask(fn)
        assert t.wait(timeout=0.05) is None  # not done yet
        release.set()
        assert t.wait(timeout=1.0) == 99  # re-waited to completion

    def test_result_reraises_on_each_access(self):
        def boom():
            raise ValueError("kaboom")

        t = ThreadTask(boom)
        with pytest.raises(ValueError, match="kaboom"):
            t.wait()
        with pytest.raises(ValueError, match="kaboom"):
            t.wait()  # the stored exception is re-raised, not cleared

    def test_add_finish_callback_after_done_delivers_exception(self):
        def boom():
            raise ValueError("late")

        t = ThreadTask(boom)
        with pytest.raises(ValueError):
            t.wait()
        seen = []
        t.add_finish_callback(lambda r, e: seen.append((r, e)))
        assert seen[0][0] is None
        assert isinstance(seen[0][1], ValueError)


# ---------------------------------------------------------------------------
# Multi-level stop cascade
# ---------------------------------------------------------------------------


class TestMultiLevelCascade:
    def test_stop_cascades_through_grandchild(self):
        captured = {}

        def grandchild_fn():
            while True:
                sleep(0.01)

        def child_fn():
            gc = ThreadTask(grandchild_fn, name="grandchild")
            captured["gc"] = gc
            gc.wait()

        def parent_fn():
            c = ThreadTask(child_fn, name="child")
            captured["c"] = c
            c.wait()

        parent = ThreadTask(parent_fn, name="parent")
        time.sleep(0.1)
        parent.stop()
        with pytest.raises(Stopped):
            parent.wait(timeout=1.0)
        time.sleep(0.1)
        assert captured["c"].is_stopped
        assert captured["gc"].is_stopped


# ---------------------------------------------------------------------------
# WorkerThread shutdown semantics
# ---------------------------------------------------------------------------


class TestWorkerThreadShutdown:
    def test_submit_after_stop_raises(self):
        worker = WorkerThread()
        worker.stop()
        with pytest.raises(RuntimeError, match="stopped"):
            worker.submit(lambda: None)

    def test_queued_jobs_drain_before_shutdown(self):
        worker = WorkerThread()
        results = []
        tasks = [worker.submit(lambda i=i: results.append(i)) for i in range(5)]
        worker.stop()  # sentinel queued behind the five jobs
        for t in tasks:
            t.wait(timeout=1.0)
        assert results == list(range(5))


# ---------------------------------------------------------------------------
# Custom Task implementations interoperate with the stop-aware primitives
# ---------------------------------------------------------------------------


class CustomTask:
    """A hand-rolled Task (not derived from the built-ins) that fires its own
    stop callbacks, so the stop-aware primitives can wake it poll-free."""

    def __init__(self, fn):
        self.is_done = False
        self.is_stopped = False
        self._result = None
        self._exc = None
        self._done = threading.Event()
        self._lock = threading.Lock()
        self._stop_callbacks = []
        self._thread = threading.Thread(target=self._run, args=(fn,), daemon=True)
        self._thread.start()

    def _run(self, fn):
        with task_context(self, "custom"):
            try:
                self._result = fn()
            except BaseException as e:
                self._exc = e
            finally:
                self.is_done = True
                self._done.set()

    def wait(self, timeout=None):
        self._done.wait(timeout)
        if self._exc is not None:
            raise self._exc
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
        self._done.wait()
        fn(self._result, self._exc)

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


class TestCustomTask:
    def test_satisfies_protocol(self):
        t = CustomTask(lambda: None)
        t.wait()
        assert isinstance(t, Task)

    def test_sleep_wakes_poll_free_on_custom_task_stop(self):
        # The headline contract: a custom Task that implements the stop-callback
        # hooks gets poll-free wake-up from the built-in primitives.
        t = CustomTask(lambda: sleep(30))
        time.sleep(0.05)
        start = time.monotonic()
        t.stop()
        with pytest.raises(Stopped):
            t.wait(timeout=1.0)
        assert time.monotonic() - start < 0.5


# ---------------------------------------------------------------------------
# Callback exceptions are logged rather than silently swallowed
# ---------------------------------------------------------------------------


class TestCallbackErrorsLogged:
    def test_finish_callback_exception_is_logged(self, caplog):
        def bad(result, exc):
            raise RuntimeError("bad finish cb")

        with caplog.at_level(logging.ERROR, logger="gentletask"):
            t = ThreadTask(lambda: 1, on_finish=bad)
            t.wait()
            time.sleep(0.05)  # callbacks run just after wait() is released
        assert "finish callback raised" in caplog.text

    def test_stop_callback_exception_is_logged(self, caplog):
        def bad():
            raise RuntimeError("bad stop cb")

        t = ThreadTask(lambda: sleep(10))
        t.add_stop_callback(bad)
        time.sleep(0.05)
        with caplog.at_level(logging.ERROR, logger="gentletask"):
            t.stop()
            with pytest.raises(Stopped):
                t.wait(timeout=1.0)
        assert "stop callback raised" in caplog.text
