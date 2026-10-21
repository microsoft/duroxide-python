# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import gc
import json
import time
import weakref

from duroxide import Client, Runtime, RuntimeOptions, SqliteProvider
from duroxide import _duroxide as native


class ActivityOwner:
    def __init__(self, runtime):
        self.runtime = runtime

    def activity(self, _context, value):
        return value


def attach_activity_owner(runtime, capture):
    owner = ActivityOwner(runtime)
    if capture == "bound":
        callback = owner.activity
    elif capture == "self-capturing":
        callback = lambda context, value: owner.activity(context, value)
    else:
        callback = lambda _context, value: value
    runtime.register_activity("CallbackCollection-" + capture, callback)
    return {
        "owner": weakref.ref(owner),
        "runtime": weakref.ref(runtime),
        "callback": weakref.ref(callback),
    }


def collection_state(references):
    return {name: reference() is None for name, reference in references.items()}


def settle_deferred_references(references, deadline=None):
    if deadline is None:
        deadline = time.perf_counter_ns() + 2_000_000_000
    while True:
        # A fresh PyO3 entry flushes queued off-GIL decrefs; task counts alone
        # would not show whether an opaque callback cycle was actually released.
        native.RuntimeOptions()
        gc.collect()
        collected = collection_state(references)
        if all(collected.values()) or time.perf_counter_ns() >= deadline:
            return collected
        time.sleep(0.001)


def stopped_owner(capture, scenario, database_directory):
    # Canceled SQLx acquisitions can discard the last in-memory connection.
    # A file store isolates provider ownership from that connection-lifetime rule.
    provider = SqliteProvider.open(str(database_directory / f"{capture}.db"))
    client = Client(provider)
    runtime = Runtime(provider, RuntimeOptions(
        dispatcher_poll_interval_ms=5,
        session_idle_timeout_ms=0 if scenario == "callbacks-preparation-failed" else None,
    ))
    references = attach_activity_owner(runtime, capture)
    hooks = None
    start_error = None
    stop_error = None
    if scenario == "callbacks-operational-failed":
        hooks = native._lifecycle_test_hooks(runtime._native)
        hooks.hold("worker")
        hooks.fail("worker", "callback-collection-fault")
    if scenario != "callbacks-prestart":
        try:
            runtime.start()
            assert scenario != "callbacks-preparation-failed"
        except RuntimeError as error:
            assert scenario == "callbacks-preparation-failed"
            assert "lifecycle_start_failed" in str(error)
            start_error = str(error)
    if hooks is not None:
        try:
            hooks.wait_entered("worker")
        finally:
            hooks.release("worker")
    for timeout in (100, 30_000):
        try:
            assert runtime.shutdown(timeout) is None
            assert scenario not in ("callbacks-preparation-failed", "callbacks-operational-failed")
        except RuntimeError as error:
            if scenario == "callbacks-operational-failed":
                assert "lifecycle_shutdown_failed" in str(error)
                assert "cleanup completed and is quiescent" in str(error)
                assert "terminate the process" not in str(error)
            else:
                assert scenario == "callbacks-preparation-failed"
                assert str(error) == start_error
            if stop_error is not None:
                assert str(error) == stop_error
            stop_error = str(error)
    if hooks is not None:
        state = json.loads(hooks.snapshot())
        assert state["active"] == state["cleanupActive"] == 0
        assert state["started"] == state["completed"]
    assert runtime.metrics_snapshot() is None
    for operation in (
        runtime.start,
        lambda: runtime.register_activity("CallbackCollection-late", lambda _context, value: value),
        lambda: runtime.register_orchestration("CallbackCollection-late", lambda _context, value: value),
    ):
        try:
            operation()
            raise AssertionError("quiescent runtime was reusable")
        except RuntimeError as error:
            assert "lifecycle_terminal" in str(error)
    return references, provider, client, stop_error


def collect_stopped_owners(scenario, database_directory):
    cases = {}
    for capture in ("noncapturing", "bound", "self-capturing"):
        # The creating frame must be gone before inspecting any weak reference.
        references, provider, client, stop_error = stopped_owner(capture, scenario, database_directory)
        collected = settle_deferred_references(references)
        # Independent clients/providers must still work after callback release.
        client.start_orchestration("after-callback-cleanup", "Unregistered", None)
        assert provider is not None
        cases[capture] = {"collected": collected, "stopError": stop_error}
    return cases
