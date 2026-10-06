# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import gc
import json
import sys
import threading
import time
import traceback

from duroxide import Runtime, RuntimeOptions, SqliteProvider
from duroxide import _duroxide as native


def emit(phase, **detail):
    print("LIFECYCLE_PROBE " + json.dumps({"phase": phase, **detail}), flush=True)


def main():
    scenario = sys.argv[1]
    runtime = Runtime(SqliteProvider.in_memory(), RuntimeOptions(
        dispatcher_poll_interval_ms=5, service_name="lifecycle-private-sentinel",
    ))
    if scenario in ("registry-invalid", "registry-descending"):
        versions = ["not-semver"] if scenario == "registry-invalid" else ["2.0.0", "1.0.0"]
        for version in versions:
            runtime.register_orchestration_versioned(
                "registry-private-sentinel", version, lambda _context, _input: "ok",
            )
        try:
            runtime.start()
            raise AssertionError("invalid registry unexpectedly started")
        except RuntimeError as error:
            assert type(error) is RuntimeError
            start_message = str(error)
            assert "lifecycle_start_failed" in start_message
            assert "sentinel" not in start_message
        for timeout in (0, 30_000):
            try:
                runtime.shutdown(timeout)
                raise AssertionError("failed startup was replaced by successful shutdown")
            except RuntimeError as error:
                assert str(error) == start_message
        assert runtime.metrics_snapshot() is None
        for operation in (
            runtime.start,
            lambda: runtime.register_activity("late", lambda _context, _input: "late"),
            lambda: runtime.register_orchestration("late", lambda _context, _input: "late"),
            lambda: runtime.register_orchestration_versioned("late", "3.0.0", lambda _context, _input: "late"),
        ):
            try:
                operation()
                raise AssertionError("failed startup was reusable")
            except RuntimeError as error:
                assert "lifecycle_terminal" in str(error)
        globals()["retained_runtime"] = runtime
        emit("REGISTRY_FAILURE_RETAINED", scenario=scenario)
        return
    if scenario == "exit":
        runtime.register_activity("ExitActivity", lambda _context, value: value)
        runtime.register_orchestration("ExitFlow", lambda _context, _input: "ok")
        runtime.start()
        runtime.shutdown(100)
        globals()["retained_runtime"] = runtime
        emit("EXIT_READY")
        return
    hooks = native._lifecycle_test_hooks(runtime._native)
    hooks.hold("provider")
    runtime.start()
    hooks.wait_entered("provider")
    emit("STOPPING")
    start = time.monotonic()
    try:
        runtime.shutdown(None if scenario == "late" else 0)
        raise AssertionError("held provider falsely completed")
    except TimeoutError as error:
        assert "lifecycle_shutdown_timed_out" in str(error)
        assert "cleanup remains owned and incomplete" in str(error)
        assert "terminate the process" in str(error)
        assert "sentinel" not in str(error)
    elapsed = time.monotonic() - start
    repeated = time.monotonic()
    try:
        runtime.shutdown(30_000)
        raise AssertionError("repeated stop falsely completed")
    except TimeoutError as error:
        assert "lifecycle_shutdown_timed_out" in str(error)
        assert "sentinel" not in str(error)
    repeated = time.monotonic() - repeated
    assert json.loads(hooks.snapshot())["active"] > 0
    assert runtime.metrics_snapshot() is None
    emit("TIMED_OUT", elapsedSeconds=elapsed, repeatedSeconds=repeated)
    if sys.stdin.readline().strip() == "retain":
        runtime = None
        gc.collect()
        state = json.loads(hooks.snapshot())
        assert state["active"] > 0
        assert state["coordinators"] == 1
        emit("RETAINED")
        threading.Event().wait()
    else:
        hooks.release("provider")
        deadline = time.monotonic() + 2
        while True:
            try:
                runtime.shutdown(30_000)
                break
            except TimeoutError:
                assert time.monotonic() < deadline
                time.sleep(0.001)
        state = json.loads(hooks.snapshot())
        assert state["active"] == 0
        assert state["cleanupActive"] == 0
        assert state["started"] == state["completed"]
        assert state["coordinators"] == 1
        assert state["forceRequests"] == 1
        try:
            runtime.start()
            raise AssertionError("timed-out runtime restarted")
        except RuntimeError as error:
            assert "lifecycle_terminal" in str(error)
        emit("RECLAIMED")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        emit("FAILED", error=traceback.format_exc())
        raise
