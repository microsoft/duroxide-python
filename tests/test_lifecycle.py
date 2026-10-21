# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest

from duroxide import Client, Runtime, RuntimeOptions, SqliteProvider
from duroxide import _duroxide as native
from duroxide import _orchestration_functions
from duroxide.driver import create_generator, next_step, dispose_generator

from lifecycle_probe_support import (
    LifecycleProbe,
    assert_timeout_measurements,
    clock_info,
    timeout_delivery_budget,
)


INSTRUMENTED = os.environ.get("DUROXIDE_LIFECYCLE_TEST_HOOKS") == "1"
SECRET = "Password=lifecycle-secret-sentinel;Host=private-sentinel"
MAX_GRACE_MS = (2**64 - 1) // 1_000_000 - 5000
hook_test = pytest.mark.skipif(not INSTRUMENTED, reason="requires freshly built test-hooks extension")


def fixture(**options):
    provider = SqliteProvider.in_memory()
    runtime = Runtime(provider, RuntimeOptions(dispatcher_poll_interval_ms=5, **options))
    hooks = native._lifecycle_test_hooks(runtime._native) if INSTRUMENTED else None
    return runtime, provider, hooks


def diagnostic(error, category):
    expected_type = ValueError if category == "lifecycle_invalid_timeout" else RuntimeError
    assert type(error.value) is expected_type
    message = str(error.value)
    assert category in message
    assert SECRET not in message
    assert "sentinel" not in message


def assert_quiescent_shutdown_failure(error):
    diagnostic(error, "lifecycle_shutdown_failed")
    assert "cleanup completed and is quiescent" in str(error.value)
    assert "terminate the process" not in str(error.value)


def retired(hooks, forced=None, rollback=False):
    state = json.loads(hooks.snapshot())
    assert state["active"] == 0
    assert state["cleanupActive"] == 0
    assert state["started"] == state["completed"]
    if rollback:
        assert 1 <= state["coordinators"] <= 2
    else:
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


def test_missing_native_drivers_reject_before_accepting_startup():
    provider = SqliteProvider.in_memory()
    runtime = native.PyRuntime.from_sqlite(provider._native)
    with pytest.raises(RuntimeError) as error:
        runtime.start()
    diagnostic(error, "lifecycle_start_failed")
    assert "generator driver not set" in str(error.value)

    runtime.set_generator_driver(create_generator, next_step, dispose_generator)
    assert runtime.start() is None
    assert runtime.metrics_snapshot() is not None
    assert runtime.shutdown(0) is None
    assert runtime.shutdown() is None
    with pytest.raises(RuntimeError, match="lifecycle_terminal"):
        runtime.start()


def test_no_work_stop_after_missing_native_drivers_is_successful_and_terminal():
    provider = SqliteProvider.in_memory()
    runtime = native.PyRuntime.from_sqlite(provider._native)
    with pytest.raises(RuntimeError) as error:
        runtime.start()
    diagnostic(error, "lifecycle_start_failed")
    assert runtime.shutdown(0) is None
    assert runtime.shutdown() is None
    with pytest.raises(RuntimeError, match="lifecycle_terminal"):
        runtime.set_generator_driver(create_generator, next_step, dispose_generator)
    with pytest.raises(RuntimeError, match="lifecycle_terminal"):
        runtime.start()


@pytest.mark.parametrize("decorator", [False, True])
@pytest.mark.parametrize("versioned", [False, True])
@pytest.mark.parametrize("terminal_state", ["stopped", "started", "startup-failed"])
def test_rejected_registrations_preserve_handler_keys_identity_and_execution(
    decorator, versioned, terminal_state,
):
    active, provider, _ = fixture()
    terminal, _, _ = fixture(**({"session_idle_timeout_ms": 0} if terminal_state == "startup-failed" else {}))
    client = Client(provider)
    prefix = "LifecycleAtomic-" + uuid.uuid4().hex
    existing, fresh = prefix + "-existing", prefix + "-fresh"
    version = "1.0.0"
    existing_key = f"{existing}@{version}" if versioned else existing
    owned_keys = {existing, fresh, f"{existing}@{version}", f"{fresh}@{version}"}
    saved = {key: _orchestration_functions[key] for key in owned_keys if key in _orchestration_functions}

    def original(_context, value):
        return {"handler": "original", "input": value}

    def rejected(_context, value):
        return {"handler": "rejected", "input": value}

    try:
        if versioned:
            active.register_orchestration_versioned(existing, version, original)
        else:
            active.register_orchestration(existing, original)
        active.start()
        if terminal_state == "startup-failed":
            with pytest.raises(RuntimeError, match="lifecycle_start_failed"):
                terminal.start()
        elif terminal_state == "started":
            terminal.start()
        else:
            terminal.shutdown(0)
        keys_before = {key for key in owned_keys if key in _orchestration_functions}
        for index, name in enumerate((existing, fresh)):
            register = terminal.register_orchestration_versioned if versioned else terminal.register_orchestration
            args = (name, version) if versioned else (name,)
            with pytest.raises(RuntimeError, match="lifecycle_terminal"):
                if decorator:
                    register(*args)(rejected)
                else:
                    register(*args, rejected)
            assert {key for key in owned_keys if key in _orchestration_functions} == keys_before
            assert _orchestration_functions[existing_key] is original
            instance = f"{prefix}-{index}"
            if versioned:
                client.start_orchestration_versioned(instance, existing, index, version)
            else:
                client.start_orchestration(instance, existing, index)
            result = client.wait_for_orchestration(instance, 5000)
            assert result.status == "Completed"
            assert result.output == {"handler": "original", "input": index}
    finally:
        try:
            active.shutdown(100)
        finally:
            try:
                if terminal_state == "startup-failed":
                    with pytest.raises(RuntimeError, match="lifecycle_start_failed"):
                        terminal.shutdown(0)
                else:
                    terminal.shutdown(0)
            finally:
                for key in owned_keys:
                    if key in saved:
                        _orchestration_functions[key] = saved[key]
                    else:
                        _orchestration_functions.pop(key, None)


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
    start = time.perf_counter_ns()
    assert runtime.shutdown(30_000) is None
    elapsed = time.perf_counter_ns() - start
    print("LIFECYCLE_IDLE " + json.dumps({"elapsedNanoseconds": elapsed, "clock": clock_info()}))
    assert elapsed < 2_000_000_000, clock_info()
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
    hooks.hold("blocking-provider")
    hooks.fail("partial-startup", SECRET)
    with ThreadPoolExecutor(max_workers=1) as pool:
        startup = pool.submit(runtime.start)
        try:
            hooks.wait_entered("partial-startup")
            hooks.wait_entered("blocking-provider")
            hooks.release("partial-startup")
            with pytest.raises(RuntimeError) as error:
                startup.result(timeout=2)
            diagnostic(error, "lifecycle_start_failed")
            assert json.loads(hooks.snapshot())["active"] > 0
            assert runtime.metrics_snapshot() is None
            with pytest.raises(RuntimeError, match="lifecycle_terminal"):
                runtime.start()
        finally:
            hooks.release("partial-startup")
            hooks.release("blocking-provider")
    with pytest.raises(RuntimeError) as error:
        runtime.shutdown(0)
    assert_quiescent_shutdown_failure(error)
    with pytest.raises(RuntimeError, match="lifecycle_shutdown_failed"):
        runtime.shutdown(30_000)
    retired(hooks, rollback=True)


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
    assert_quiescent_shutdown_failure(error)
    with pytest.raises(RuntimeError, match="lifecycle_shutdown_failed"):
        runtime.shutdown(30_000)
    retired(hooks, rollback=True)


@hook_test
def test_cancellation_safe_fetch_retires_without_waiting_for_its_gate():
    runtime, _, hooks = fixture()
    hooks.hold("provider")
    runtime.start()
    hooks.wait_entered("provider")
    try:
        runtime.shutdown(1000)
        retired(hooks, False)
    finally:
        hooks.release("provider")


@hook_test
@pytest.mark.parametrize("force", [False, True])
def test_ordered_force_and_retirement_100_times(force):
    with ThreadPoolExecutor(max_workers=1) as pool:
        for _ in range(100):
            runtime, _, hooks = fixture()
            hooks.hold("blocking-provider")
            hooks.hold("force" if force else "coordinator")
            runtime.start()
            hooks.wait_entered("blocking-provider")
            stop = pool.submit(runtime.shutdown, 0 if force else 30_000)
            try:
                hooks.wait_entered("force" if force else "coordinator")
                assert json.loads(hooks.snapshot())["active"] > 0
            finally:
                hooks.release("blocking-provider")
                hooks.release("force" if force else "coordinator")
            assert stop.result(timeout=2) is None
            assert runtime.shutdown() is None
            retired(hooks, force)


@hook_test
@pytest.mark.parametrize("scenario", ["late", "retained", "callbacks-late"])
def test_isolated_non_yielding_provider_timeout(scenario):
    with LifecycleProbe(scenario) as probe:
        probe.expect_phase("STOPPING", 15)
        timed_out = probe.expect_phase("TIMED_OUT", timeout_delivery_budget(scenario))
        print("LIFECYCLE_TIMING " + json.dumps({"scenario": scenario, **timed_out}))
        assert_timeout_measurements(scenario, timed_out)
        probe.send("retain" if scenario == "retained" else "release")
        probe.expect_phase("RETAINED" if scenario == "retained" else "RECLAIMED", 3)


@pytest.mark.parametrize("scenario", [
    "callbacks-prestart", "callbacks-stopped", "callbacks-preparation-failed",
])
def test_activity_only_callbacks_collect_after_quiescence_in_live_process(scenario):
    database_directory = Path(__file__).resolve().parents[1] / "target" / "lifecycle-callbacks" / uuid.uuid4().hex
    database_directory.mkdir(parents=True)
    command = [
        sys.executable, str(Path(__file__).parent / "fixtures" / "lifecycle_probe.py"),
        scenario, str(database_directory),
    ]
    try:
        with LifecycleProbe(scenario, command) as probe:
            report = probe.expect_phase("COLLECTION", 10)
            print("LIFECYCLE_COLLECTION " + json.dumps({"scenario": scenario, **report}))
            assert probe.child.poll() is None
            assert set(report["cases"]) == {"noncapturing", "bound", "self-capturing"}
            for case in report["cases"].values():
                assert all(case["collected"].values()), report
            probe.send("finish")
            probe.expect_phase("COLLECTION_FINISHED", 3)
    finally:
        shutil.rmtree(database_directory)


@hook_test
def test_activity_only_callbacks_collect_after_quiescent_operational_failure():
    test_activity_only_callbacks_collect_after_quiescence_in_live_process("callbacks-operational-failed")


def test_retained_completed_runtime_allows_natural_process_exit():
    result = subprocess.run(
        [sys.executable, str(Path(__file__).parent / "fixtures" / "lifecycle_probe.py"), "exit"],
        capture_output=True, text=True, timeout=3, check=True,
    )
    assert "EXIT_READY" in result.stdout
