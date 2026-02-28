"""
Tests for typed convenience API methods in duroxide Python SDK.

Uses SQLite in-memory provider for all tests — no external database required.
"""

import json
import time
import pytest

from duroxide import (
    SqliteProvider,
    Client,
    Runtime,
    PyRuntimeOptions,
)

RUN_ID = f"typed{int(time.time() * 1000):x}"


def uid(name):
    return f"{RUN_ID}-{name}"


def make_env(poll_ms=50):
    """Create a fresh SQLite provider, client, and runtime."""
    provider = SqliteProvider.in_memory()
    client = Client(provider)
    runtime = Runtime(provider, PyRuntimeOptions(dispatcher_poll_interval_ms=poll_ms))
    return provider, client, runtime


# ─── 1. Typed Activity on Session Round-Trip ─────────────────────


def test_typed_activity_on_session_round_trip():
    """schedule_activity_on_session_typed serializes input and deserializes output."""
    _, client, runtime = make_env()

    @runtime.register_activity_typed("Add")
    def add(ctx, input):
        return {"sum": input["a"] + input["b"]}

    @runtime.register_orchestration_typed("SessionTypedOrch")
    def orch(ctx, input):
        result = yield ctx.schedule_activity_on_session_typed(
            "Add", {"a": 2, "b": 3}, "sess-1"
        )
        return result

    runtime.start()
    try:
        iid = uid("sess-typed-1")
        client.start_orchestration(iid, "SessionTypedOrch", {"a": 2, "b": 3})
        result = client.wait_for_orchestration(iid, 5_000)
        assert result.status == "Completed"
        assert result.output == {"sum": 5}
    finally:
        runtime.shutdown(100)


# ─── 2. Typed Activity with Retry ────────────────────────────────


def test_typed_activity_with_retry():
    """schedule_activity_with_retry_typed retries and returns parsed result."""
    _, client, runtime = make_env()

    counter = [0]

    @runtime.register_activity_typed("Flaky")
    def flaky(ctx, input):
        counter[0] += 1
        if counter[0] < 2:
            raise Exception("transient")
        return {"result": 42}

    @runtime.register_orchestration_typed("RetryTypedOrch")
    def orch(ctx, input):
        result = yield ctx.schedule_activity_with_retry_typed(
            "Flaky", {"x": 1}, {"max_attempts": 3, "backoff": "fixed"}
        )
        return result

    runtime.start()
    try:
        iid = uid("retry-typed-1")
        client.start_orchestration(iid, "RetryTypedOrch", {"x": 1})
        result = client.wait_for_orchestration(iid, 5_000)
        assert result.status == "Completed"
        assert result.output == {"result": 42}
    finally:
        runtime.shutdown(100)


# ─── 3. Typed Activity with Retry on Session ─────────────────────


def test_typed_activity_with_retry_on_session():
    """schedule_activity_with_retry_on_session_typed combines retry and session affinity."""
    _, client, runtime = make_env()

    counter = [0]

    @runtime.register_activity_typed("FlakySession")
    def flaky_session(ctx, input):
        counter[0] += 1
        if counter[0] < 2:
            raise Exception("transient")
        return {"result": 99, "session": ctx.session_id}

    @runtime.register_orchestration_typed("RetrySessionTypedOrch")
    def orch(ctx, input):
        result = yield ctx.schedule_activity_with_retry_on_session_typed(
            "FlakySession", {"x": 1}, {"max_attempts": 3, "backoff": "fixed"}, "retry-sess"
        )
        return result

    runtime.start()
    try:
        iid = uid("retry-sess-typed-1")
        client.start_orchestration(iid, "RetrySessionTypedOrch", {"x": 1})
        result = client.wait_for_orchestration(iid, 5_000)
        assert result.status == "Completed"
        assert result.output["result"] == 99
        assert result.output["session"] == "retry-sess"
    finally:
        runtime.shutdown(100)


# ─── 4. Typed Wait for Event ─────────────────────────────────────


def test_typed_wait_for_event():
    """wait_for_event_typed auto-deserializes event data."""
    _, client, runtime = make_env()

    @runtime.register_orchestration_typed("WaitEventTypedOrch")
    def orch(ctx, input):
        data = yield ctx.wait_for_event_typed("Ready")
        return data

    runtime.start()
    try:
        iid = uid("wait-evt-typed-1")
        client.start_orchestration(iid, "WaitEventTypedOrch", None)
        time.sleep(0.5)
        client.raise_event(iid, "Ready", {"ok": True})
        result = client.wait_for_orchestration(iid, 5_000)
        assert result.status == "Completed"
        assert result.output == {"ok": True}
    finally:
        runtime.shutdown(100)


# ─── 5. Typed Dequeue Event ──────────────────────────────────────


def test_typed_dequeue_event():
    """dequeue_event_typed auto-deserializes queued event data."""
    _, client, runtime = make_env()

    @runtime.register_orchestration_typed("DequeueTypedOrch")
    def orch(ctx, input):
        msg = yield ctx.dequeue_event_typed("inbox")
        return msg

    runtime.start()
    try:
        iid = uid("dequeue-typed-1")
        client.start_orchestration(iid, "DequeueTypedOrch", None)
        time.sleep(0.5)
        client.enqueue_event(iid, "inbox", {"msg": "hello"})
        result = client.wait_for_orchestration(iid, 5_000)
        assert result.status == "Completed"
        assert result.output == {"msg": "hello"}
    finally:
        runtime.shutdown(100)


# ─── 6. Typed Sub-Orchestration with ID ──────────────────────────


def test_typed_sub_orchestration_with_id():
    """schedule_sub_orchestration_with_id_typed serializes/deserializes correctly."""
    _, client, runtime = make_env()

    @runtime.register_orchestration_typed("ChildTyped")
    def child(ctx, input):
        return {"computed": input["n"]}

    @runtime.register_orchestration_typed("ParentWithIdTyped")
    def parent(ctx, input):
        result = yield ctx.schedule_sub_orchestration_with_id_typed(
            "ChildTyped", "child-123", {"n": 99}
        )
        return result

    runtime.start()
    try:
        iid = uid("sub-id-typed-1")
        client.start_orchestration(iid, "ParentWithIdTyped", {"n": 99})
        result = client.wait_for_orchestration(iid, 5_000)
        assert result.status == "Completed"
        assert result.output == {"computed": 99}
    finally:
        runtime.shutdown(100)


# ─── 7. Typed Detached Orchestration ─────────────────────────────


def test_typed_detached_orchestration():
    """start_orchestration_typed fires a detached orchestration."""
    _, client, runtime = make_env()

    @runtime.register_activity_typed("Stamp")
    def stamp(ctx, input):
        return {"stamped": input["task"]}

    @runtime.register_orchestration_typed("Worker")
    def worker(ctx, input):
        result = yield ctx.schedule_activity_typed("Stamp", input)
        return result

    @runtime.register_orchestration_typed("Launcher")
    def launcher(ctx, input):
        yield ctx.start_orchestration_typed("Worker", "detached-1", {"task": "go"})
        return "launched"

    runtime.start()
    try:
        iid = uid("detached-typed-1")
        client.start_orchestration(iid, "Launcher", None)
        result = client.wait_for_orchestration(iid, 5_000)
        assert result.status == "Completed"
        assert result.output == "launched"
        # Verify detached orchestration completed
        detached = client.wait_for_orchestration("detached-1", 5_000)
        assert detached.status == "Completed"
        assert detached.output == {"stamped": "go"}
    finally:
        runtime.shutdown(100)


# ─── 8. Typed Continue as New ─────────────────────────────────────


def test_typed_continue_as_new():
    """continue_as_new_typed serializes input for the next execution."""
    _, client, runtime = make_env()

    @runtime.register_orchestration_typed("CanTypedOrch")
    def orch(ctx, input):
        counter = input["counter"] if isinstance(input, dict) else 0
        if counter < 3:
            yield ctx.continue_as_new_typed({"counter": counter + 1})
            return None
        return {"final": counter}

    runtime.start()
    try:
        iid = uid("can-typed-1")
        client.start_orchestration(iid, "CanTypedOrch", {"counter": 0})
        result = client.wait_for_orchestration(iid, 10_000)
        assert result.status == "Completed"
        assert result.output == {"final": 3}
    finally:
        runtime.shutdown(100)


# ─── 9. Typed Client Start and Wait ──────────────────────────────


def test_typed_client_start_and_wait():
    """client.start_orchestration_typed + wait_for_orchestration_typed returns parsed output."""
    _, client, runtime = make_env()

    @runtime.register_activity_typed("Echo")
    def echo(ctx, input):
        return input

    @runtime.register_orchestration_typed("EchoOrch")
    def orch(ctx, input):
        result = yield ctx.schedule_activity_typed("Echo", input)
        return result

    runtime.start()
    try:
        iid = uid("client-typed-1")
        client.start_orchestration_typed(iid, "EchoOrch", {"echo": "ping"})
        output = client.wait_for_orchestration_typed(iid)
        assert output == {"echo": "ping"}
    finally:
        runtime.shutdown(100)


# ─── 10. Typed Client Events ─────────────────────────────────────


def test_typed_client_events():
    """client.raise_event_typed and enqueue_event_typed deliver typed data."""
    _, client, runtime = make_env()

    @runtime.register_orchestration_typed("EventTypedOrch")
    def orch(ctx, input):
        evt = yield ctx.wait_for_event_typed("Evt")
        msg = yield ctx.dequeue_event_typed("Q")
        return {"evt": evt, "msg": msg}

    runtime.start()
    try:
        iid = uid("client-evt-typed-1")
        client.start_orchestration_typed(iid, "EventTypedOrch", None)
        time.sleep(0.5)
        client.raise_event_typed(iid, "Evt", {"typed": True})
        time.sleep(0.5)
        client.enqueue_event_typed(iid, "Q", {"typed": True})
        output = client.wait_for_orchestration_typed(iid)
        assert output["evt"] == {"typed": True}
        assert output["msg"] == {"typed": True}
    finally:
        runtime.shutdown(100)


# ─── 11. Typed Versioned Start Paths ──────────────────────────────


def test_typed_versioned_start_paths():
    """Versioned typed start paths work from both ctx and client."""
    _, client, runtime = make_env()

    @runtime.register_orchestration_versioned_typed("VersionedWorker", "1.0.0")
    def worker(ctx, input):
        return {"done": True, "task": input["task"], "version": "v1"}

    @runtime.register_orchestration_typed("VersionedLauncher")
    def launcher(ctx, input):
        yield ctx.start_orchestration_versioned_typed(
            "VersionedWorker", "1.0.0", uid("detached-v1"), {"task": "go-v1"}
        )
        return "launched-v1"

    runtime.start()
    try:
        parent_id = uid("versioned-launcher-typed-1")
        client.start_orchestration(parent_id, "VersionedLauncher", None)
        parent_result = client.wait_for_orchestration(parent_id, 10_000)
        assert parent_result.status == "Completed"
        assert parent_result.output == "launched-v1"

        detached_result = client.wait_for_orchestration(uid("detached-v1"), 10_000)
        assert detached_result.status == "Completed"
        assert detached_result.output == {"done": True, "task": "go-v1", "version": "v1"}

        direct_id = uid("client-versioned-typed-1")
        client.start_orchestration_versioned_typed(
            direct_id, "VersionedWorker", {"task": "direct-v1"}, "1.0.0"
        )
        direct_output = client.wait_for_orchestration_typed(direct_id)
        assert direct_output == {"done": True, "task": "direct-v1", "version": "v1"}
    finally:
        runtime.shutdown(100)
