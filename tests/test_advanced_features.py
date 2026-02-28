"""
Advanced feature tests covering typed registration, retry, cancellation,
timers, custom status, event queues, CAN, versioning, errors, and nondeterminism.
"""

import json
import os
import time
import pytest

from dotenv import load_dotenv

from duroxide import (
    PostgresProvider,
    Client,
    Runtime,
    PyRuntimeOptions,
)

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

SCHEMA = "duroxide_python_advanced_features"
RUN_ID = f"af{int(time.time() * 1000):x}"


def uid(name):
    return f"{RUN_ID}-{name}"


@pytest.fixture(scope="module")
def provider():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        pytest.skip("DATABASE_URL not set")
    return PostgresProvider.connect_with_schema(db_url, SCHEMA)


def run_orchestration(provider, orch_name, input, setup_fn, timeout_ms=10_000, instance_name=None):
    client = Client(provider)
    runtime = Runtime(provider, PyRuntimeOptions(dispatcher_poll_interval_ms=50))
    setup_fn(runtime)
    runtime.start()
    try:
        instance_id = uid(instance_name or orch_name)
        client.start_orchestration(instance_id, orch_name, input)
        return client.wait_for_orchestration(instance_id, timeout_ms)
    finally:
        runtime.shutdown(100)


# === TYPED REGISTRATION (1-3) ===

def test_typed_registration_round_trip(provider):
    def setup(rt):
        rt.register_activity_typed("Double", lambda ctx, n: n * 2)

        @rt.register_orchestration_typed("TypedRegRT")
        def orch(ctx, n):
            result = yield ctx.schedule_activity_typed("Double", n)
            return result

    result = run_orchestration(provider, "TypedRegRT", json.dumps(21), setup, instance_name="typed-rt-1")
    assert result.status == "Completed"
    assert result.output == 42


def test_typed_registration_fan_out(provider):
    def setup(rt):
        rt.register_activity_typed("AddTen", lambda ctx, n: n + 10)
        rt.register_activity_typed("MulThree", lambda ctx, n: n * 3)

        @rt.register_orchestration("TypedFanOut")
        def orch(ctx, input):
            n = int(input)
            results = yield ctx.all_typed([
                ctx.schedule_activity_typed("AddTen", n),
                ctx.schedule_activity_typed("MulThree", n),
            ])
            # all_typed() unwraps [{"ok": val}, ...] → [val, ...]
            total = results[0] + results[1]
            return str(total)

    result = run_orchestration(provider, "TypedFanOut", "5", setup, instance_name="typed-fan-1")
    assert result.status == "Completed"
    assert result.output == "30"


def test_typed_registration_race_wins(provider):
    def setup(rt):
        rt.register_activity_typed("QuickCalc", lambda ctx, n: n * 2)

        @rt.register_orchestration("TypedRace")
        def orch(ctx, input):
            n = int(input)
            winner = yield ctx.race(
                ctx.schedule_activity_typed("QuickCalc", n),
                ctx.schedule_timer(2000),
            )
            # race() returns {"index": N, "value": val}
            return str(winner["value"])

    result = run_orchestration(provider, "TypedRace", "21", setup, instance_name="typed-race-1")
    assert result.status == "Completed"
    assert result.output == "42"


# === RETRY (4-5) ===

def test_retry_succeeds_after_failures(provider):
    counter = [0]

    def setup(rt):
        def flaky(ctx, input):
            counter[0] += 1
            if counter[0] < 3:
                raise Exception("transient")
            return f"success-on-{counter[0]}"

        rt.register_activity("Flaky", flaky)

        @rt.register_orchestration("RetrySuccess")
        def orch(ctx, input):
            result = yield ctx.schedule_activity_with_retry(
                "Flaky", "go", {"max_attempts": 5, "backoff": "fixed"}
            )
            return result

    result = run_orchestration(provider, "RetrySuccess", "go", setup, instance_name="retry-ok-1")
    assert result.status == "Completed"
    assert "success" in str(result.output)


def test_retry_exhaustion_compensation(provider):
    def setup(rt):
        def always_fails(ctx, inp):
            raise Exception("permanent")

        rt.register_activity("AlwaysFails", always_fails)
        rt.register_activity("Compensate", lambda ctx, inp: "compensated")

        @rt.register_orchestration("RetryComp")
        def orch(ctx, input):
            try:
                yield ctx.schedule_activity_with_retry(
                    "AlwaysFails", "go", {"max_attempts": 2, "backoff": "fixed"}
                )
            except Exception:
                result = yield ctx.schedule_activity("Compensate", "fix")
                return result

    result = run_orchestration(provider, "RetryComp", "go", setup, instance_name="retry-comp-1")
    assert result.status == "Completed"
    assert result.output == "compensated"


# === CANCELLATION (6-8) ===

def test_cancel_propagates_to_sub_orchestration(provider):
    client = Client(provider)
    runtime = Runtime(provider, PyRuntimeOptions(dispatcher_poll_interval_ms=50))

    @runtime.register_orchestration("LongChild")
    def long_child(ctx, input):
        yield ctx.wait_for_event("never_comes")
        return "should-not-reach"

    @runtime.register_orchestration("CancelParent")
    def cancel_parent(ctx, input):
        result = yield ctx.schedule_sub_orchestration("LongChild", "child")
        return result

    runtime.start()
    try:
        iid = uid("cancel-prop-1")
        client.start_orchestration(iid, "CancelParent", "go")
        time.sleep(2)
        client.cancel_instance(iid, "test-cancel")
        result = client.wait_for_orchestration(iid, 10_000)
        assert result.status == "Failed"
    finally:
        runtime.shutdown(100)


def test_cancel_completed_is_noop(provider):
    client = Client(provider)
    runtime = Runtime(provider, PyRuntimeOptions(dispatcher_poll_interval_ms=50))

    runtime.register_activity("Echo", lambda ctx, inp: inp)

    @runtime.register_orchestration("QuickOrch")
    def quick_orch(ctx, input):
        result = yield ctx.schedule_activity("Echo", "hello")
        return result

    runtime.start()
    try:
        iid = uid("cancel-noop-1")
        client.start_orchestration(iid, "QuickOrch", "go")
        result = client.wait_for_orchestration(iid, 10_000)
        assert result.status == "Completed"
        client.cancel_instance(iid, "too-late")
        status = client.get_status(iid)
        assert status.status == "Completed"
    finally:
        runtime.shutdown(100)


def test_race_sub_orchestration_wins(provider):
    def setup(rt):
        rt.register_activity("Work", lambda ctx, inp: "work-done")

        @rt.register_orchestration("FastChild")
        def fast_child(ctx, input):
            result = yield ctx.schedule_activity("Work", "go")
            return result

        @rt.register_orchestration("SlowChild")
        def slow_child(ctx, input):
            yield ctx.wait_for_event("never")
            return "slow"

        @rt.register_orchestration("RaceParent")
        def race_parent(ctx, input):
            winner = yield ctx.race(
                ctx.schedule_sub_orchestration("FastChild", "a"),
                ctx.schedule_sub_orchestration("SlowChild", "b"),
            )
            return winner

    result = run_orchestration(provider, "RaceParent", "go", setup, instance_name="race-sub-1")
    assert result.status == "Completed"
    assert "work-done" in str(result.output)


# === TIMERS (9-10) ===

def test_timer_activity_interleave(provider):
    def setup(rt):
        rt.register_activity("Step", lambda ctx, inp: f"step:{inp}")

        @rt.register_orchestration("TimerInterleave")
        def orch(ctx, input):
            yield ctx.schedule_timer(50)
            r1 = yield ctx.schedule_activity("Step", "1")
            yield ctx.schedule_timer(50)
            r2 = yield ctx.schedule_activity("Step", "2")
            return f"{r1},{r2}"

    result = run_orchestration(provider, "TimerInterleave", "go", setup, instance_name="timer-interl-1")
    assert result.status == "Completed"
    assert result.output == "step:1,step:2"


def test_zero_duration_timer(provider):
    def setup(rt):
        rt.register_activity("After", lambda ctx, inp: "after-timer")

        @rt.register_orchestration("ZeroTimer")
        def orch(ctx, input):
            yield ctx.schedule_timer(0)
            result = yield ctx.schedule_activity("After", "go")
            return result

    result = run_orchestration(provider, "ZeroTimer", "go", setup, instance_name="zero-timer-1")
    assert result.status == "Completed"
    assert result.output == "after-timer"


# === CUSTOM STATUS (11-12) ===

def test_custom_status_multi_turn_lifecycle(provider):
    def setup(rt):
        rt.register_activity("Noop", lambda ctx, inp: "ok")

        @rt.register_orchestration("StatusLifecycle")
        def orch(ctx, input):
            ctx.set_custom_status("phase1")
            yield ctx.schedule_activity("Noop", "a")
            s1 = ctx.get_custom_status()
            ctx.set_custom_status("phase2")
            yield ctx.schedule_activity("Noop", "b")
            s2 = ctx.get_custom_status()
            ctx.reset_custom_status()
            yield ctx.schedule_activity("Noop", "c")
            s3 = ctx.get_custom_status()
            return f"{s1},{s2},{s3 or 'none'}"

    result = run_orchestration(provider, "StatusLifecycle", "go", setup, instance_name="status-lc-1")
    assert result.status == "Completed"
    assert result.output == "phase1,phase2,none"


def test_custom_status_visible_from_client(provider):
    client = Client(provider)
    runtime = Runtime(provider, PyRuntimeOptions(dispatcher_poll_interval_ms=50))

    @runtime.register_orchestration("StatusVisible")
    def orch(ctx, input):
        ctx.set_custom_status("processing")
        yield ctx.wait_for_event("proceed")
        return "done"

    runtime.start()
    try:
        iid = uid("status-vis-1")
        client.start_orchestration(iid, "StatusVisible", "go")
        time.sleep(2)
        status = client.get_status(iid)
        assert status.custom_status == "processing"
        client.raise_event(iid, "proceed", "go")
        result = client.wait_for_orchestration(iid, 10_000)
        assert result.status == "Completed"
    finally:
        runtime.shutdown(100)


# === EVENT QUEUES (13-14) ===

def test_event_queue_fifo_ordering(provider):
    client = Client(provider)
    runtime = Runtime(provider, PyRuntimeOptions(dispatcher_poll_interval_ms=50))

    @runtime.register_orchestration("QueueFIFO")
    def orch(ctx, input):
        m1 = yield ctx.dequeue_event("work")
        m2 = yield ctx.dequeue_event("work")
        m3 = yield ctx.dequeue_event("work")
        return f"{m1},{m2},{m3}"

    runtime.start()
    try:
        iid = uid("queue-fifo-1")
        client.start_orchestration(iid, "QueueFIFO", "go")
        time.sleep(0.5)
        client.enqueue_event(iid, "work", "first")
        client.enqueue_event(iid, "work", "second")
        client.enqueue_event(iid, "work", "third")
        result = client.wait_for_orchestration(iid, 10_000)
        assert result.status == "Completed"
        assert result.output == "first,second,third"
    finally:
        runtime.shutdown(100)


def test_event_queue_multi_queue_isolation(provider):
    client = Client(provider)
    runtime = Runtime(provider, PyRuntimeOptions(dispatcher_poll_interval_ms=50))

    @runtime.register_orchestration("MultiQueue")
    def orch(ctx, input):
        a1 = yield ctx.dequeue_event("alpha")
        b1 = yield ctx.dequeue_event("beta")
        a2 = yield ctx.dequeue_event("alpha")
        return f"a:{a1}+{a2},b:{b1}"

    runtime.start()
    try:
        iid = uid("multi-queue-1")
        client.start_orchestration(iid, "MultiQueue", "go")
        time.sleep(0.5)
        client.enqueue_event(iid, "alpha", "a-first")
        client.enqueue_event(iid, "beta", "b-first")
        client.enqueue_event(iid, "alpha", "a-second")
        result = client.wait_for_orchestration(iid, 10_000)
        assert result.status == "Completed"
        assert result.output == "a:a-first+a-second,b:b-first"
    finally:
        runtime.shutdown(100)


# === CONTINUE-AS-NEW (15) ===

def test_continue_as_new_with_activity(provider):
    def setup(rt):
        rt.register_activity("Record", lambda ctx, inp: f"recorded:{inp}")

        @rt.register_orchestration("CanCounter")
        def orch(ctx, input):
            n = int(input)
            r = yield ctx.schedule_activity("Record", str(n))
            if n > 1:
                yield ctx.continue_as_new(str(n - 1))
                return None
            return r

    result = run_orchestration(provider, "CanCounter", "3", setup, instance_name="can-counter-1", timeout_ms=15_000)
    assert result.status == "Completed"
    assert result.output == "recorded:1"


# === VERSIONING (16) ===

def test_versioned_sub_orchestration_explicit_pin(provider):
    def setup(rt):
        @rt.register_orchestration_versioned("VChild", "1.0.0")
        def v_child_v1(ctx, input):
            yield ctx.schedule_timer(0)
            return "v1-result"

        @rt.register_orchestration_versioned("VChild", "1.0.1")
        def v_child_v2(ctx, input):
            yield ctx.schedule_timer(0)
            return "v2-result"

        @rt.register_orchestration("VersionParent")
        def orch(ctx, input):
            result = yield ctx.schedule_sub_orchestration_versioned("VChild", "1.0.0", "go")
            return result

    result = run_orchestration(provider, "VersionParent", "go", setup, instance_name="version-pin-1")
    assert result.status == "Completed"
    assert result.output == "v1-result"


# === ERROR HANDLING (17-18) ===

def test_error_sub_orchestration_propagates(provider):
    def setup(rt):
        def fail_act(ctx, inp):
            raise Exception("child-error")

        rt.register_activity("FailAct", fail_act)

        @rt.register_orchestration("FailChild")
        def fail_child(ctx, input):
            result = yield ctx.schedule_activity("FailAct", "go")
            return result

        @rt.register_orchestration("ErrorParent")
        def orch(ctx, input):
            try:
                yield ctx.schedule_sub_orchestration("FailChild", "go")
            except Exception as e:
                return f"caught:{e}"

    result = run_orchestration(provider, "ErrorParent", "go", setup, instance_name="err-prop-1")
    assert result.status == "Completed"
    assert "caught" in str(result.output)


def test_error_compensation_debit_ship_refund(provider):
    def setup(rt):
        rt.register_activity("Debit", lambda ctx, inp: "debited")

        def ship(ctx, inp):
            raise Exception("ship-failed")

        rt.register_activity("Ship", ship)
        rt.register_activity("Refund", lambda ctx, inp: "refunded")

        @rt.register_orchestration("Saga")
        def orch(ctx, input):
            debit = yield ctx.schedule_activity("Debit", "order")
            try:
                yield ctx.schedule_activity("Ship", "order")
            except Exception:
                refund = yield ctx.schedule_activity("Refund", "order")
                return f"compensated:{debit}:{refund}"

    result = run_orchestration(provider, "Saga", "order", setup, instance_name="saga-comp-1")
    assert result.status == "Completed"
    assert result.output == "compensated:debited:refunded"


# === NONDETERMINISM (19-20) ===

def test_nondeterminism_command_type_swap(provider):
    client = Client(provider)
    iid = uid("nondet-swap-1")

    # Phase 1: activity then wait_for_event
    runtime1 = Runtime(provider, PyRuntimeOptions(dispatcher_poll_interval_ms=50))
    runtime1.register_activity("Work", lambda ctx, inp: "done")

    @runtime1.register_orchestration("NondetSwap")
    def orch_v1(ctx, input):
        yield ctx.schedule_activity("Work", "go")
        yield ctx.wait_for_event("proceed")
        return "v1"

    runtime1.start()
    client.start_orchestration(iid, "NondetSwap", "go")
    time.sleep(2)
    runtime1.shutdown(100)

    # Phase 2: timer then wait_for_event (swapped command type)
    runtime2 = Runtime(provider, PyRuntimeOptions(dispatcher_poll_interval_ms=50))
    runtime2.register_activity("Work", lambda ctx, inp: "done")

    @runtime2.register_orchestration("NondetSwap")
    def orch_v2(ctx, input):
        yield ctx.schedule_timer(100)
        yield ctx.wait_for_event("proceed")
        return "v2"

    runtime2.start()
    try:
        client.raise_event(iid, "proceed", "go")
        result = client.wait_for_orchestration(iid, 10_000)
        assert result.status == "Failed"
    finally:
        runtime2.shutdown(100)


def test_nondeterminism_extra_activity(provider):
    client = Client(provider)
    iid = uid("nondet-extra-1")

    # Phase 1: one activity then wait
    runtime1 = Runtime(provider, PyRuntimeOptions(dispatcher_poll_interval_ms=50))
    runtime1.register_activity("Work", lambda ctx, inp: "done")
    runtime1.register_activity("Extra", lambda ctx, inp: "extra")

    @runtime1.register_orchestration("NondetExtra")
    def orch_v1(ctx, input):
        yield ctx.schedule_activity("Work", "go")
        yield ctx.wait_for_event("proceed")
        return "v1"

    runtime1.start()
    client.start_orchestration(iid, "NondetExtra", "go")
    time.sleep(2)
    runtime1.shutdown(100)

    # Phase 2: two activities then wait (extra activity inserted)
    runtime2 = Runtime(provider, PyRuntimeOptions(dispatcher_poll_interval_ms=50))
    runtime2.register_activity("Work", lambda ctx, inp: "done")
    runtime2.register_activity("Extra", lambda ctx, inp: "extra")

    @runtime2.register_orchestration("NondetExtra")
    def orch_v2(ctx, input):
        yield ctx.schedule_activity("Work", "go")
        yield ctx.schedule_activity("Extra", "go")
        yield ctx.wait_for_event("proceed")
        return "v2"

    runtime2.start()
    try:
        client.raise_event(iid, "proceed", "go")
        result = client.wait_for_orchestration(iid, 10_000)
        assert result.status == "Failed"
    finally:
        runtime2.shutdown(100)
