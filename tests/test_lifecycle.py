# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import hashlib
import json
import os
from pathlib import Path
import queue
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from duroxide import Client, Runtime, RuntimeOptions, SqliteProvider
from duroxide import _duroxide as native


INSTRUMENTED = os.environ.get("DUROXIDE_LIFECYCLE_TEST_HOOKS") == "1"
SECRET = "Password=lifecycle-secret-sentinel;Host=private-sentinel"
MAX_GRACE_MS = (2**64 - 1) // 1_000_000 - 5000
hook_test = pytest.mark.skipif(not INSTRUMENTED, reason="requires freshly built test-hooks extension")


def fixture(**options):
    provider = SqliteProvider.in_memory()
    runtime = Runtime(provider, RuntimeOptions(dispatcher_poll_interval_ms=5, **options))
    hooks = native._lifecycle_test_hooks(runtime._native) if INSTRUMENTED else None
    return runtime, provider, hooks


def diagnostic(error, category, incomplete=False):
    message = str(error.value)
    assert category in message
    assert SECRET not in message
    assert "sentinel" not in message
    if incomplete:
        assert "cleanup remains owned and incomplete" in message
        assert "terminate the process" in message
    if category == "lifecycle_shutdown_failed":
        assert "cleanup completed and is quiescent" in message
        assert "terminate the process" not in message


def retired(hooks, forced=None):
    state = json.loads(hooks.snapshot())
    assert state["active"] == 0
    assert state["cleanupActive"] == 0
    assert state["started"] == state["completed"]
    assert state["coordinators"] == 1
    if forced is not None:
        assert state["forceRequests"] == int(forced)


def test_native_provenance_and_production_exports():
    extension = Path(native.__file__).resolve()
    assert extension.parent == Path(__file__).resolve().parents[1] / "python" / "duroxide"
    assert hasattr(native, "_lifecycle_test_hooks") == INSTRUMENTED
    assert hasattr(native, "LifecycleTestHooks") == INSTRUMENTED
    print("LIFECYCLE_NATIVE " + json.dumps({
        "path": str(extension),
        "sha256": hashlib.sha256(extension.read_bytes()).hexdigest(),
        "instrumented": INSTRUMENTED,
    }))


def test_invalid_durations_preserve_runtime_ownership():
    runtime, _, hooks = fixture()
    for value in [-1, MAX_GRACE_MS + 1, 2**63 - 1]:
        with pytest.raises(ValueError) as error:
            runtime.shutdown(value)
        diagnostic(error, "lifecycle_invalid_timeout")
    for value in [0.5, float("nan"), float("inf"), "0"]:
        with pytest.raises(TypeError):
            runtime.shutdown(value)
    with pytest.raises(OverflowError):
        runtime.shutdown(2**63)
    runtime.register_orchestration("LifecycleInvalidInput", lambda _ctx, _input: "ok")
    assert runtime.start() is None
    assert runtime.metrics_snapshot() is not None
    with pytest.raises(ValueError):
        runtime.shutdown(-1)
    assert runtime.metrics_snapshot() is not None
    assert runtime.shutdown(0) is None
    assert runtime.metrics_snapshot() is None
    with pytest.raises(ValueError):
        runtime.shutdown(2**63 - 1)
    assert runtime.shutdown() is None
    if hooks:
        retired(hooks)


def test_maximum_default_total_boundary_on_no_work_and_repeated_stop():
    runtime, _, _ = fixture()
    assert runtime.shutdown(MAX_GRACE_MS) is None
    with pytest.raises(ValueError) as error:
        runtime.shutdown(MAX_GRACE_MS + 1)
    diagnostic(error, "lifecycle_invalid_timeout")
    assert runtime.shutdown() is None


def test_prestart_shutdown_is_terminal_no_work():
    runtime, _, _ = fixture()
    assert runtime.shutdown(0) is None
    assert runtime.shutdown() is None
    with pytest.raises(RuntimeError, match="lifecycle_terminal"):
        runtime.start()
    with pytest.raises(RuntimeError, match="lifecycle_terminal"):
        runtime.register_activity("late", lambda _ctx, _input: "late")
    with pytest.raises(RuntimeError, match="lifecycle_terminal"):
        runtime.register_orchestration("late", lambda _ctx, _input: "late")
    with pytest.raises(RuntimeError, match="lifecycle_terminal"):
        runtime.register_orchestration_versioned("late", "1.0.0", lambda _ctx, _input: "late")
    assert runtime.metrics_snapshot() is None


def test_fallible_preparation_rejects_options_without_panic():
    runtime, _, _ = fixture(session_idle_timeout_ms=0)
    with pytest.raises(RuntimeError) as error:
        runtime.start()
    diagnostic(error, "lifecycle_start_failed")
    with pytest.raises(RuntimeError, match="lifecycle_start_failed"):
        runtime.shutdown(0)
    with pytest.raises(RuntimeError, match="lifecycle_terminal"):
        runtime.start()
    assert runtime.metrics_snapshot() is None


@pytest.mark.parametrize("scenario", ["registry-invalid", "registry-descending"])
def test_pre_core_startup_failure_is_ordinary_and_retained(scenario):
    result = subprocess.run(
        [sys.executable, str(Path(__file__).parent / "fixtures" / "lifecycle_probe.py"), scenario],
        capture_output=True, text=True, timeout=5,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "REGISTRY_FAILURE_RETAINED" in result.stdout


def test_ascending_and_duplicate_versions_keep_existing_successful_startup_policy():
    runtime, _, _ = fixture()
    for version in ("1.0.0", "1.0.0", "2.0.0"):
        runtime.register_orchestration_versioned("LifecycleVersionPolicy", version, lambda _context, _input: "ok")
    assert runtime.start() is None
    assert runtime.shutdown(100) is None
    assert runtime.shutdown(0) is None


def test_idle_shutdown_short_circuits_and_preserves_success():
    runtime, _, hooks = fixture()
    runtime.start()
    start = time.monotonic()
    assert runtime.shutdown(30_000) is None
    assert time.monotonic() - start < 2
    assert runtime.shutdown(0) is None
    with pytest.raises(RuntimeError, match="lifecycle_terminal"):
        runtime.start()
    if hooks:
        retired(hooks, False)


def test_sqlite_public_activity_and_orchestration():
    runtime, provider, _ = fixture()
    client = Client(provider)
    runtime.register_activity("LifecycleEcho", lambda _ctx, value: value)

    @runtime.register_orchestration("LifecycleEchoFlow")
    def flow(context, value):
        return (yield context.schedule_activity("LifecycleEcho", value))

    runtime.start()
    try:
        client.start_orchestration("lifecycle-echo", "LifecycleEchoFlow", "hello")
        result = client.wait_for_orchestration("lifecycle-echo", 5000)
        assert result.status == "Completed"
        assert result.output == "hello"
    finally:
        runtime.shutdown(100)


@pytest.mark.parametrize("typed", [False, True])
def test_sqlite_races_preserve_failures_values_and_shutdown(typed):
    runtime, provider, hooks = fixture()
    client = Client(provider)

    @runtime.register_activity("LifecycleRaceFail")
    def fail(_context, _input):
        raise RuntimeError("race-failure")

    runtime.register_activity("LifecycleRaceValue", lambda _context, _input: {"err": "data", "ok": True})

    @runtime.register_orchestration("LifecycleRaceFlow")
    def flow(context, _input):
        schedule = context.schedule_activity_typed if typed else context.schedule_activity
        race = context.race_typed if typed else context.race
        direct = None
        try:
            yield schedule("LifecycleRaceFail", None)
        except Exception as error:
            direct = {"type": type(error).__name__, "message": str(error)}
        raced = None
        try:
            yield race(context.schedule_timer(60_000), schedule("LifecycleRaceFail", None))
        except Exception as error:
            raced = {"type": type(error).__name__, "message": str(error)}
        winner = yield race(schedule("LifecycleRaceValue", None), context.schedule_timer(60_000))
        return {"direct": direct, "raced": raced, "winner": winner}

    runtime.start()
    try:
        client.start_orchestration("lifecycle-race", "LifecycleRaceFlow", None)
        result = client.wait_for_orchestration("lifecycle-race", 5000)
        assert result.status == "Completed"
        assert result.output["direct"] is not None
        assert "race-failure" in result.output["direct"]["message"]
        assert result.output["raced"] == result.output["direct"]
        assert result.output["winner"] == {"index": 0, "value": {"err": "data", "ok": True}}
    finally:
        runtime.shutdown(100)
    assert runtime.shutdown(0) is None
    if hooks:
        retired(hooks)


@hook_test
def test_partial_startup_failure_retains_rollback():
    runtime, _, hooks = fixture()
    hooks.hold("partial-startup")
    hooks.hold("provider")
    hooks.fail("partial-startup", SECRET)
    with ThreadPoolExecutor(max_workers=1) as pool:
        startup = pool.submit(runtime.start)
        try:
            hooks.wait_entered("partial-startup")
            hooks.wait_entered("provider")
            hooks.release("partial-startup")
            with pytest.raises(RuntimeError) as error:
                startup.result(timeout=2)
            diagnostic(error, "lifecycle_start_failed")
            assert json.loads(hooks.snapshot())["active"] > 0
            with pytest.raises(RuntimeError, match="lifecycle_terminal"):
                runtime.start()
        finally:
            hooks.release("partial-startup")
            hooks.release("provider")
    with pytest.raises(RuntimeError) as error:
        runtime.shutdown(0)
    diagnostic(error, "lifecycle_shutdown_failed")
    with pytest.raises(RuntimeError, match="lifecycle_shutdown_failed"):
        runtime.shutdown(30_000)
    retired(hooks)


@hook_test
def test_operational_failure_is_retained_on_repeated_shutdown():
    runtime, _, hooks = fixture()
    hooks.hold("worker")
    hooks.fail("worker", SECRET)
    runtime.start()
    hooks.wait_entered("worker")
    hooks.release("worker")
    with pytest.raises(RuntimeError) as error:
        runtime.shutdown(100)
    diagnostic(error, "lifecycle_shutdown_failed")
    with pytest.raises(RuntimeError, match="lifecycle_shutdown_failed"):
        runtime.shutdown(30_000)
    retired(hooks)


@hook_test
@pytest.mark.parametrize("force", [False, True])
def test_ordered_force_and_retirement_100_times(force):
    with ThreadPoolExecutor(max_workers=1) as pool:
        for _ in range(100):
            runtime, _, hooks = fixture()
            hooks.hold("provider")
            hooks.hold("force" if force else "coordinator")
            runtime.start()
            hooks.wait_entered("provider")
            stop = pool.submit(runtime.shutdown, 0 if force else 30_000)
            try:
                hooks.wait_entered("force" if force else "coordinator")
                assert json.loads(hooks.snapshot())["active"] > 0
            finally:
                hooks.release("provider")
                hooks.release("force" if force else "coordinator")
            assert stop.result(timeout=2) is None
            assert runtime.shutdown() is None
            retired(hooks, force)


@hook_test
@pytest.mark.parametrize("scenario", ["late", "retained"])
def test_isolated_provider_timeout(scenario):
    child = subprocess.Popen(
        [sys.executable, str(Path(__file__).parent / "fixtures" / "lifecycle_probe.py"), scenario],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    messages = queue.Queue()
    errors = []

    def read_messages():
        for line in child.stdout:
            if line.startswith("LIFECYCLE_PROBE "):
                messages.put(json.loads(line[len("LIFECYCLE_PROBE "):]))
        messages.put({"phase": "EXIT"})

    def read_errors():
        errors.append(child.stderr.read())

    reader = threading.Thread(target=read_messages, daemon=True)
    stderr_reader = threading.Thread(target=read_errors, daemon=True)
    reader.start()
    stderr_reader.start()
    try:
        assert messages.get(timeout=15)["phase"] == "STOPPING"
        timed_out = messages.get(timeout=7)
        assert timed_out["phase"] == "TIMED_OUT", timed_out
        assert (6 if scenario == "late" else 5) <= timed_out["elapsedSeconds"] < (7 if scenario == "late" else 6)
        assert timed_out["repeatedSeconds"] < 1
        child.stdin.write("release\n" if scenario == "late" else "retain\n")
        child.stdin.flush()
        done = messages.get(timeout=3)
        assert done["phase"] == ("RECLAIMED" if scenario == "late" else "RETAINED"), done
    finally:
        if child.poll() is None:
            child.kill()
        child.wait(timeout=5)
        reader.join(timeout=2)
        stderr_reader.join(timeout=2)
        child.stdin.close()
        child.stdout.close()
        child.stderr.close()


def test_retained_completed_runtime_allows_natural_process_exit():
    result = subprocess.run(
        [sys.executable, str(Path(__file__).parent / "fixtures" / "lifecycle_probe.py"), "exit"],
        capture_output=True, text=True, timeout=3, check=True,
    )
    assert "EXIT_READY" in result.stdout
