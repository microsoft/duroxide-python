"""
End-to-end tests for duroxide Python SDK — PostgreSQL backend.
Ported from duroxide-node/__tests__/e2e.test.js

Requires DATABASE_URL env var or .env file with a PostgreSQL connection string.
SQLite smoketest at the bottom verifies basic functionality without PG.
"""

import json
import os
import time
import threading
import pytest

from dotenv import load_dotenv

from duroxide import (
    SqliteProvider,
    PostgresProvider,
    Client,
    Runtime,
    PyRuntimeOptions,
    PyPruneOptions,
    PyInstanceFilter,
)

# Load .env from project root
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

SCHEMA = "duroxide_python_e2e"

# ─── Helpers ─────────────────────────────────────────────────────

RUN_ID = f"e2e{int(time.time() * 1000):x}"


def uid(name):
    return f"{RUN_ID}-{name}"


@pytest.fixture(scope="module")
def provider():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        pytest.skip("DATABASE_URL not set")
    return PostgresProvider.connect_with_schema(db_url, SCHEMA)


def run_orchestration(provider, name, input, setup_fn, timeout_ms=10_000):
    """Helper: register handlers, start runtime, run one orchestration, shutdown."""
    client = Client(provider)
    runtime = Runtime(provider, PyRuntimeOptions(dispatcher_poll_interval_ms=50))

    setup_fn(runtime)
    runtime.start()

    try:
        instance_id = uid(name)
        client.start_orchestration(instance_id, name, input)
        return client.wait_for_orchestration(instance_id, timeout_ms)
    finally:
        runtime.shutdown(100)


# ─── 1. Hello World ──────────────────────────────────────────────


def test_hello_world(provider):
    def setup(rt):
        @rt.register_activity("Hello")
        def hello(ctx, input):
            ctx.trace_info(f"greeting {input}")
            return f"Hello, {input}!"

        @rt.register_orchestration("HelloWorld")
        def hello_world(ctx, input):
            r1 = yield ctx.schedule_activity("Hello", "Rust")
            ctx.trace_info(f"first greeting: {r1}")
            r2 = yield ctx.schedule_activity("Hello", input)
            return r2

    result = run_orchestration(provider, "HelloWorld", "World", setup)
    assert result.status == "Completed"
    assert result.output == "Hello, World!"


# ─── 2. Control Flow ─────────────────────────────────────────────


def test_control_flow(provider):
    def setup(rt):
        rt.register_activity("GetFlag", lambda ctx, inp: "yes")
        rt.register_activity("SayYes", lambda ctx, inp: "picked_yes")
        rt.register_activity("SayNo", lambda ctx, inp: "picked_no")

        @rt.register_orchestration("ControlFlow")
        def control_flow(ctx, input):
            flag = yield ctx.schedule_activity("GetFlag", "")
            if flag == "yes":
                return (yield ctx.schedule_activity("SayYes", ""))
            else:
                return (yield ctx.schedule_activity("SayNo", ""))

    result = run_orchestration(provider, "ControlFlow", None, setup)
    assert result.status == "Completed"
    assert result.output == "picked_yes"


# ─── 3. Loop ─────────────────────────────────────────────────────


def test_loop(provider):
    def setup(rt):
        @rt.register_activity("Append")
        def append(ctx, input):
            ctx.trace_info(f'appending to "{input}"')
            return f"{input}x"

        @rt.register_orchestration("LoopOrchestration")
        def loop_orch(ctx, input):
            acc = "start"
            for i in range(3):
                acc = yield ctx.schedule_activity("Append", acc)
                ctx.trace_info(f"iteration {i}: {acc}")
            return acc

    result = run_orchestration(provider, "LoopOrchestration", None, setup)
    assert result.status == "Completed"
    assert result.output == "startxxx"


# ─── 4. Error Handling ───────────────────────────────────────────


def test_error_handling(provider):
    def setup(rt):
        @rt.register_activity("Fragile")
        def fragile(ctx, input):
            ctx.trace_warn(f'fragile called with "{input}"')
            if input == "bad":
                raise Exception("boom")
            return "ok"

        rt.register_activity("Recover", lambda ctx, inp: "recovered")

        @rt.register_orchestration("ErrorHandling")
        def error_handling(ctx, input):
            try:
                return (yield ctx.schedule_activity("Fragile", "bad"))
            except Exception as e:
                ctx.trace_warn(f"fragile failed: {e}")
                return (yield ctx.schedule_activity("Recover", ""))

    result = run_orchestration(provider, "ErrorHandling", None, setup)
    assert result.status == "Completed"
    assert result.output == "recovered"


# ─── 5. Timer ────────────────────────────────────────────────────


def test_timer(provider):
    def setup(rt):
        rt.register_activity("AfterTimer", lambda ctx, inp: "done")

        @rt.register_orchestration("TimerSample")
        def timer_sample(ctx, input):
            yield ctx.schedule_timer(100)
            return (yield ctx.schedule_activity("AfterTimer", ""))

    result = run_orchestration(provider, "TimerSample", None, setup)
    assert result.status == "Completed"
    assert result.output == "done"


# ─── 6. Fan-out Fan-in (join) ────────────────────────────────────


def test_fan_out_fan_in(provider):
    def setup(rt):
        @rt.register_activity("Greetings")
        def greetings(ctx, input):
            ctx.trace_info(f"building greeting for {input}")
            return f"Hello, {input}!"

        @rt.register_orchestration("FanOut")
        def fan_out(ctx, input):
            a = ctx.schedule_activity("Greetings", "Gabbar")
            b = ctx.schedule_activity("Greetings", "Samba")
            results = yield ctx.all([a, b])
            vals = [r.get("ok", r.get("err")) for r in results]
            vals.sort()
            return f"{vals[0]}, {vals[1]}"

    result = run_orchestration(provider, "FanOut", None, setup)
    assert result.status == "Completed"
    assert result.output == "Hello, Gabbar!, Hello, Samba!"


# ─── 7. System Activities (utcNow, newGuid) ──────────────────────


def test_system_activities(provider):
    def setup(rt):
        @rt.register_orchestration("SystemActivities")
        def sys_act(ctx, input):
            now = yield ctx.utc_now()
            guid = yield ctx.new_guid()
            ctx.trace_info(f"now={now}, guid={guid}")
            return {"now": now, "guid": guid}

    result = run_orchestration(provider, "SystemActivities", None, setup)
    assert result.status == "Completed"
    assert result.output["now"] > 0
    assert len(result.output["guid"]) == 36


# ─── 8. Status Polling ───────────────────────────────────────────


def test_status_polling(provider):
    client = Client(provider)
    runtime = Runtime(provider, PyRuntimeOptions(dispatcher_poll_interval_ms=50))

    @runtime.register_orchestration("StatusSample")
    def status_sample(ctx, input):
        yield ctx.schedule_timer(50)
        return "done"

    runtime.start()

    try:
        instance_id = uid("status")
        client.start_orchestration(instance_id, "StatusSample", None)
        early = client.get_status(instance_id)
        assert early.status in ("Running", "NotFound")
        final = client.wait_for_orchestration(instance_id, 5000)
        assert final.status == "Completed"
        assert final.output == "done"
    finally:
        runtime.shutdown(100)


# ─── 9. Sub-orchestration (basic) ────────────────────────────────


def test_sub_orchestration_basic(provider):
    def setup(rt):
        rt.register_activity("Upper", lambda ctx, inp: inp.upper())

        @rt.register_orchestration("ChildUpper")
        def child_upper(ctx, input):
            return (yield ctx.schedule_activity("Upper", input))

        @rt.register_orchestration("Parent")
        def parent(ctx, input):
            r = yield ctx.schedule_sub_orchestration("ChildUpper", input)
            return f"parent:{r}"

    result = run_orchestration(provider, "Parent", "hi", setup)
    assert result.status == "Completed"
    assert result.output == "parent:HI"


# ─── 10. Sub-orchestration Fan-out ───────────────────────────────


def test_sub_orchestration_fan_out(provider):
    def setup(rt):
        @rt.register_activity("Add")
        def add(ctx, input):
            a, b = input.split(",")
            return int(a) + int(b)

        @rt.register_orchestration("ChildSum")
        def child_sum(ctx, input):
            return (yield ctx.schedule_activity("Add", input))

        @rt.register_orchestration("ParentFan")
        def parent_fan(ctx, input):
            a = ctx.schedule_sub_orchestration("ChildSum", "1,2")
            b = ctx.schedule_sub_orchestration("ChildSum", "3,4")
            results = yield ctx.all([a, b])
            total = 0
            for r in results:
                v = r.get("ok", r.get("err"))
                if isinstance(v, str):
                    v = int(v)
                total += v
            return f"total={total}"

    result = run_orchestration(provider, "ParentFan", None, setup)
    assert result.status == "Completed"
    assert result.output == "total=10"


# ─── 11. Sub-orchestration Chained (3 levels) ────────────────────


def test_sub_orchestration_chained(provider):
    def setup(rt):
        rt.register_activity("AppendX", lambda ctx, inp: f"{inp}x")

        @rt.register_orchestration("Leaf")
        def leaf(ctx, input):
            return (yield ctx.schedule_activity("AppendX", input))

        @rt.register_orchestration("Mid")
        def mid(ctx, input):
            r = yield ctx.schedule_sub_orchestration("Leaf", input)
            return f"{r}-mid"

        @rt.register_orchestration("Root")
        def root(ctx, input):
            r = yield ctx.schedule_sub_orchestration("Mid", input)
            return f"root:{r}"

    result = run_orchestration(provider, "Root", "a", setup)
    assert result.status == "Completed"
    assert result.output == "root:ax-mid"


# ─── 12. External Event ──────────────────────────────────────────


def test_external_event(provider):
    client = Client(provider)
    runtime = Runtime(provider, PyRuntimeOptions(dispatcher_poll_interval_ms=50))

    @runtime.register_orchestration("WaitForApproval")
    def wait_for_approval(ctx, input):
        ctx.trace_info("Waiting for approval...")
        event = yield ctx.wait_for_event("approval")
        return {"approved": event}

    runtime.start()

    try:
        instance_id = uid("event")
        client.start_orchestration(instance_id, "WaitForApproval", None)
        time.sleep(0.5)
        client.raise_event(instance_id, "approval", {"status": "yes"})
        result = client.wait_for_orchestration(instance_id, 10_000)
        assert result.status == "Completed"
        assert result.output == {"approved": {"status": "yes"}}
    finally:
        runtime.shutdown(100)


# ─── 13. Continue-as-New ─────────────────────────────────────────


def test_continue_as_new(provider):
    def setup(rt):
        @rt.register_orchestration("CanSample")
        def can_sample(ctx, n):
            if n < 3:
                ctx.trace_info(f"CAN sample n={n} -> continue")
                yield ctx.continue_as_new(n + 1)
                return None
            else:
                return f"final:{n}"

    result = run_orchestration(provider, "CanSample", 0, setup)
    assert result.status == "Completed"
    assert result.output == "final:3"


# ─── 14. Cancellation ────────────────────────────────────────────


def test_cancellation(provider):
    client = Client(provider)
    runtime = Runtime(provider, PyRuntimeOptions(dispatcher_poll_interval_ms=50))

    @runtime.register_orchestration("LongRunning")
    def long_running(ctx, input):
        yield ctx.wait_for_event("never_arrives")
        return "should not reach"

    runtime.start()

    try:
        instance_id = uid("cancel")
        client.start_orchestration(instance_id, "LongRunning", None)
        time.sleep(0.5)
        client.cancel_instance(instance_id, "user_request")
        result = client.wait_for_orchestration(instance_id, 10_000)
        assert result.status == "Failed"
        assert result.error is not None
    finally:
        runtime.shutdown(100)


# ─── 15. Conditional Logic ───────────────────────────────────────


def test_conditional_logic(provider):
    def setup(rt):
        rt.register_activity("ProcessHigh", lambda ctx, inp: "high")
        rt.register_activity("ProcessLow", lambda ctx, inp: "low")

        @rt.register_orchestration("Conditional")
        def conditional(ctx, input):
            if input["value"] > input["threshold"]:
                return (yield ctx.schedule_activity("ProcessHigh", input["value"]))
            else:
                return (yield ctx.schedule_activity("ProcessLow", input["value"]))

    result = run_orchestration(
        provider, "Conditional", {"threshold": 50, "value": 75}, setup
    )
    assert result.status == "Completed"
    assert result.output == "high"


# ─── 16. Activity Chain ──────────────────────────────────────────


def test_activity_chain(provider):
    def setup(rt):
        rt.register_activity("AddOne", lambda ctx, inp: inp + 1)

        @rt.register_orchestration("Chain")
        def chain(ctx, input):
            value = input
            value = yield ctx.schedule_activity("AddOne", value)
            value = yield ctx.schedule_activity("AddOne", value)
            value = yield ctx.schedule_activity("AddOne", value)
            return value

    result = run_orchestration(provider, "Chain", 1, setup)
    assert result.status == "Completed"
    assert result.output == 4


# ─── 17. Retry Policy ────────────────────────────────────────────


def test_retry_policy(provider):
    attempts = {"count": 0}

    def setup(rt):
        @rt.register_activity("Flaky")
        def flaky(ctx, input):
            attempts["count"] += 1
            if attempts["count"] < 3:
                raise Exception(f"attempt {attempts['count']} failed")
            return "success"

        @rt.register_orchestration("RetryTest")
        def retry_test(ctx, input):
            result = yield ctx.schedule_activity_with_retry(
                "Flaky", None, {"max_attempts": 5, "backoff": "linear"}
            )
            return result

    result = run_orchestration(provider, "RetryTest", None, setup)
    assert result.status == "Completed"
    assert result.output == "success"
    assert attempts["count"] >= 3


# ─── 18. Race / Select ───────────────────────────────────────────


def test_race_activity_vs_timer(provider):
    def setup(rt):
        rt.register_activity("QuickWork", lambda ctx, inp: "quick_result")

        @rt.register_orchestration("RaceTest")
        def race_test(ctx, input):
            winner = yield ctx.race(
                ctx.schedule_activity("QuickWork", None),
                ctx.schedule_timer(60000),
            )
            return winner

    result = run_orchestration(provider, "RaceTest", None, setup)
    assert result.status == "Completed"
    output = result.output
    assert output["index"] == 0  # activity wins


# ─── 19. Versioned Orchestration ─────────────────────────────────


def test_versioned_orchestration(provider):
    def setup(rt):
        rt.register_activity("Work", lambda ctx, inp: "done")

        @rt.register_orchestration_versioned("VersionedOrch", "1.0.0")
        def versioned_orch_v1(ctx, input):
            return "v1"

        @rt.register_orchestration_versioned("VersionedOrch", "1.0.1")
        def versioned_orch_v2(ctx, input):
            result = yield ctx.schedule_activity("Work", None)
            return f"v2:{result}"

    client = Client(provider)
    runtime = Runtime(provider, PyRuntimeOptions(dispatcher_poll_interval_ms=50))
    setup(runtime)
    runtime.start()

    try:
        # Start with v1.0.0
        id_v1 = uid("ver-v1")
        client.start_orchestration_versioned(id_v1, "VersionedOrch", None, "1.0.0")
        r1 = client.wait_for_orchestration(id_v1, 10_000)
        assert r1.status == "Completed"
        assert r1.output == "v1"

        # Start with v1.0.1
        id_v2 = uid("ver-v2")
        client.start_orchestration_versioned(id_v2, "VersionedOrch", None, "1.0.1")
        r2 = client.wait_for_orchestration(id_v2, 10_000)
        assert r2.status == "Completed"
        assert r2.output == "v2:done"
    finally:
        runtime.shutdown(100)


# ─── 20. Join with mixed types (activity + timer) ────────────────


def test_join_with_timer(provider):
    def setup(rt):
        rt.register_activity("QuickTask", lambda ctx, inp: "task_done")

        @rt.register_orchestration("JoinTimer")
        def join_timer(ctx, input):
            results = yield ctx.all([
                ctx.schedule_activity("QuickTask", None),
                ctx.schedule_timer(100),
            ])
            # First result is activity (ok/err), second is timer (ok: null)
            return {
                "activity": results[0].get("ok"),
                "timer": results[1].get("ok"),
            }

    result = run_orchestration(provider, "JoinTimer", None, setup)
    assert result.status == "Completed"
    assert result.output["activity"] == "task_done"
    assert result.output["timer"] is None


# ─── 21. Stop & Resume ──────────────────────────────────────────


def test_stop_and_resume(provider):
    """Orchestration started on one runtime can be completed by another."""
    client = Client(provider)

    # Phase 1: start and immediately shutdown
    rt1 = Runtime(provider, PyRuntimeOptions(dispatcher_poll_interval_ms=50))

    @rt1.register_activity("Tick")
    def tick(ctx, inp):
        return "ticked"

    @rt1.register_orchestration("StopResume")
    def stop_resume(ctx, input):
        r = yield ctx.schedule_activity("Tick", None)
        return f"done:{r}"

    rt1.start()
    instance_id = uid("stop-resume")
    client.start_orchestration(instance_id, "StopResume", None)
    rt1.shutdown(50)

    # Phase 2: new runtime picks up the instance
    rt2 = Runtime(provider, PyRuntimeOptions(dispatcher_poll_interval_ms=50))

    @rt2.register_activity("Tick")
    def tick2(ctx, inp):
        return "ticked"

    @rt2.register_orchestration("StopResume")
    def stop_resume2(ctx, input):
        r = yield ctx.schedule_activity("Tick", None)
        return f"done:{r}"

    rt2.start()
    try:
        result = client.wait_for_orchestration(instance_id, 10_000)
        assert result.status == "Completed"
        assert result.output == "done:ticked"
    finally:
        rt2.shutdown(100)


# ─── SQLite Smoketest ────────────────────────────────────────────


def test_sqlite_smoketest():
    """Quick sanity check that SQLite provider still works."""
    sqlite_provider = SqliteProvider.in_memory()
    client = Client(sqlite_provider)
    runtime = Runtime(sqlite_provider, PyRuntimeOptions(dispatcher_poll_interval_ms=50))

    @runtime.register_activity("Echo")
    def echo(ctx, inp):
        return inp

    @runtime.register_orchestration("SqliteSmoketest")
    def sqlite_smoketest(ctx, input):
        r = yield ctx.schedule_activity("Echo", input)
        return r

    runtime.start()
    try:
        instance_id = uid("sqlite-smoke")
        client.start_orchestration(instance_id, "SqliteSmoketest", "hello-sqlite")
        result = client.wait_for_orchestration(instance_id, 5_000)
        assert result.status == "Completed"
        assert result.output == "hello-sqlite"
    finally:
        runtime.shutdown(100)


# ─── 22. Retry Exhaustion ────────────────────────────────────────


def test_retry_exhaustion(provider):
    """Activity exhausts all retries — error is caught by orchestration."""
    client = Client(provider)
    runtime = Runtime(provider, PyRuntimeOptions(dispatcher_poll_interval_ms=50))

    def always_fails(ctx, input):
        raise Exception("permanent failure")

    runtime.register_activity("AlwaysFails", always_fails)

    @runtime.register_orchestration("RetryExhaust")
    def retry_exhaust(ctx, input):
        try:
            yield ctx.schedule_activity_with_retry("AlwaysFails", None, {
                "max_attempts": 2,
                "backoff": "fixed",
            })
            return "should not reach"
        except Exception as e:
            return f"caught: {e}"

    runtime.start()
    try:
        instance_id = uid("retry-exhaust")
        client.start_orchestration(instance_id, "RetryExhaust")
        result = client.wait_for_orchestration(instance_id, 10_000)
        assert result.status == "Completed"
        assert result.output.startswith("caught:"), f"expected caught error, got: {result.output}"
    finally:
        runtime.shutdown(100)


# ─── 23. CAN Version Upgrade ────────────────────────────────────


def test_continue_as_new_version_upgrade(provider):
    """CAN from v1.0.0 picks up v1.0.1 on next execution."""
    client = Client(provider)
    runtime = Runtime(provider, PyRuntimeOptions(dispatcher_poll_interval_ms=50))
    runtime.register_activity("Echo", lambda ctx, inp: inp)

    @runtime.register_orchestration("UpgradingWorkflow")
    def upgrading_v1(ctx, input):
        ctx.trace_info("[v1.0.0] will upgrade")
        yield ctx.continue_as_new({"fromVersion": "1.0.0"})

    @runtime.register_orchestration_versioned("UpgradingWorkflow", "1.0.1")
    def upgrading_v2(ctx, input):
        ctx.trace_info("[v1.0.1] upgraded")
        return {"version": "1.0.1", "previousVersion": input.get("fromVersion")}

    runtime.start()
    try:
        instance_id = uid("ver-can")
        client.start_orchestration_versioned(instance_id, "UpgradingWorkflow", {}, "1.0.0")
        result = client.wait_for_orchestration(instance_id, 10_000)
        assert result.status == "Completed"
        assert result.output["version"] == "1.0.1"
        assert result.output["previousVersion"] == "1.0.0"
    finally:
        runtime.shutdown(100)


# ─── 24. Version Routing ────────────────────────────────────────


def test_version_routing(provider):
    """Different versions run different logic."""
    client = Client(provider)
    runtime = Runtime(provider, PyRuntimeOptions(dispatcher_poll_interval_ms=50))
    runtime.register_activity("Work", lambda ctx, inp: f"done-{inp}")

    @runtime.register_orchestration("MultiVerWorkflow")
    def multi_ver_v1(ctx, input):
        return "v1-simple"

    @runtime.register_orchestration_versioned("MultiVerWorkflow", "1.0.1")
    def multi_ver_v2(ctx, input):
        validated = yield ctx.schedule_activity("Work", "validated")
        return f"v1.0.1-{validated}"

    runtime.start()
    try:
        id_v1 = uid("multiver-v1")
        id_v101 = uid("multiver-v101")
        client.start_orchestration_versioned(id_v1, "MultiVerWorkflow", None, "1.0.0")
        client.start_orchestration(id_v101, "MultiVerWorkflow", None)
        r1 = client.wait_for_orchestration(id_v1, 10_000)
        r2 = client.wait_for_orchestration(id_v101, 10_000)
        assert r1.status == "Completed"
        assert r1.output == "v1-simple"
        assert r2.status == "Completed"
        assert r2.output == "v1.0.1-done-validated"
    finally:
        runtime.shutdown(100)


# ─── 25. Activity get_client ────────────────────────────────────


def test_activity_get_client(provider):
    """Activity can use get_client to start a new orchestration."""
    client = Client(provider)
    runtime = Runtime(provider, PyRuntimeOptions(dispatcher_poll_interval_ms=50))

    def spawn_child(ctx, input):
        child_client = ctx.get_client()
        child_id = f"child-{ctx.instance_id}"
        child_client.start_orchestration(child_id, "SimpleChild", input)
        child_result = child_client.wait_for_orchestration(child_id, 10_000)
        return child_result.output

    runtime.register_activity("SpawnChild", spawn_child)

    @runtime.register_orchestration("SimpleChild")
    def simple_child(ctx, input):
        return f"child-got-{input}"

    @runtime.register_orchestration("ClientFromActivity")
    def client_from_activity(ctx, input):
        return (yield ctx.schedule_activity("SpawnChild", input))

    runtime.start()
    try:
        instance_id = uid("cfa")
        client.start_orchestration(instance_id, "ClientFromActivity", "trigger")
        result = client.wait_for_orchestration(instance_id, 10_000)
        assert result.status == "Completed"
        assert result.output == "child-got-trigger"
    finally:
        runtime.shutdown(100)


# ─── 26. Custom Status Get ──────────────────────────────────────


def test_custom_status_get_reflects_set_across_turns(provider):
    """ctx.get_custom_status() returns correct value, including across turn boundaries."""
    client = Client(provider)
    runtime = Runtime(provider, PyRuntimeOptions(dispatcher_poll_interval_ms=50))

    @runtime.register_activity("Echo")
    def echo(ctx, input):
        return input

    @runtime.register_orchestration("GetterTest")
    def getter_test(ctx, input):
        # Before any set, should be None
        assert ctx.get_custom_status() is None, "initial should be None"

        ctx.set_custom_status("step-1")
        assert ctx.get_custom_status() == "step-1", "should reflect set immediately"

        # Cross a turn boundary
        yield ctx.schedule_activity("Echo", "ping")

        # After replay, should still return "step-1"
        assert ctx.get_custom_status() == "step-1", "should survive replay across turns"

        ctx.set_custom_status("step-2")
        assert ctx.get_custom_status() == "step-2", "should reflect second set"

        return "done"

    runtime.start()
    try:
        instance_id = uid("cs-getter")
        client.start_orchestration(instance_id, "GetterTest", "")
        result = client.wait_for_orchestration(instance_id, 10_000)
        assert result.status == "Completed"
        assert result.output == "done"
        assert result.custom_status == "step-2"
    finally:
        runtime.shutdown(100)


def test_custom_status_persists_across_continue_as_new(provider):
    """Custom status persists across CAN boundaries and can be reset."""
    client = Client(provider)
    runtime = Runtime(provider, PyRuntimeOptions(dispatcher_poll_interval_ms=50))

    @runtime.register_activity("Echo")
    def echo(ctx, input):
        return input

    @runtime.register_orchestration("StatusCanTest")
    def status_can_test(ctx, input):
        generation = input.get("generation", 1) if isinstance(input, dict) else 1

        if generation == 1:
            # First generation: set status, verify, then CAN
            assert ctx.get_custom_status() is None, "gen1: initial should be None"
            ctx.set_custom_status("gen1-active")
            assert ctx.get_custom_status() == "gen1-active", "gen1: should reflect set"

            # Cross a turn boundary to ensure persistence
            yield ctx.schedule_activity("Echo", "ping")
            assert ctx.get_custom_status() == "gen1-active", "gen1: should survive turn"

            # CAN — runtime carries forward custom status automatically
            yield ctx.continue_as_new({"generation": 2})
            return None
        elif generation == 2:
            # Second generation: status should be carried forward from gen1
            assert ctx.get_custom_status() == "gen1-active", "gen2: should inherit from gen1"

            # Update status in gen2
            ctx.set_custom_status("gen2-updated")
            assert ctx.get_custom_status() == "gen2-updated", "gen2: should reflect new set"

            # Now reset it
            ctx.reset_custom_status()
            assert ctx.get_custom_status() is None, "gen2: should be None after reset"

            # CAN again with no status
            yield ctx.continue_as_new({"generation": 3})
            return None
        else:
            # Third generation: status should be None (was reset before CAN)
            assert ctx.get_custom_status() is None, "gen3: should be None after reset+CAN"

            ctx.set_custom_status("gen3-final")
            return "done"

    runtime.start()
    try:
        instance_id = uid("cs-can")
        client.start_orchestration(instance_id, "StatusCanTest", {"generation": 1})
        result = client.wait_for_orchestration(instance_id, 15_000)
        assert result.status == "Completed"
        assert result.output == "done"
        assert result.custom_status == "gen3-final"
    finally:
        runtime.shutdown(100)


# ─── 27. Metrics Snapshot ───────────────────────────────────────


def test_metrics_snapshot(provider):
    """Runtime exposes metrics snapshot after processing."""
    client = Client(provider)
    runtime = Runtime(provider, PyRuntimeOptions(dispatcher_poll_interval_ms=50))
    runtime.register_activity("Echo", lambda ctx, inp: inp)

    @runtime.register_orchestration("MetricsTest")
    def metrics_test(ctx, input):
        return (yield ctx.schedule_activity("Echo", input))

    runtime.start()
    try:
        instance_id = uid("metrics")
        client.start_orchestration(instance_id, "MetricsTest", "hello")
        client.wait_for_orchestration(instance_id, 10_000)
        snapshot = runtime.metrics_snapshot()
        assert snapshot is not None, "metrics snapshot should be available"
        assert snapshot.orch_starts >= 1
        assert snapshot.orch_completions >= 1
        assert snapshot.activity_success >= 1
    finally:
        runtime.shutdown(100)


# ─── 28. Detached Orchestration Scheduling ───────────────────────


def test_detached_orchestration_scheduling(provider):
    """Coordinator fires-and-forgets two child orchestrations, then returns immediately."""
    client = Client(provider)
    runtime = Runtime(provider, PyRuntimeOptions(dispatcher_poll_interval_ms=50))

    runtime.register_activity("Echo", lambda ctx, inp: inp)

    @runtime.register_orchestration("Chained")
    def chained(ctx, input):
        yield ctx.schedule_timer(5)
        return (yield ctx.schedule_activity("Echo", input))

    @runtime.register_orchestration("Coordinator")
    def coordinator(ctx, input):
        yield ctx.start_orchestration("Chained", uid("W1"), "A")
        yield ctx.start_orchestration("Chained", uid("W2"), "B")
        return "scheduled"

    runtime.start()
    try:
        coord_id = uid("Coordinator")
        client.start_orchestration(coord_id, "Coordinator", None)
        result = client.wait_for_orchestration(coord_id, 10_000)
        assert result.status == "Completed"
        assert result.output == "scheduled"

        r1 = client.wait_for_orchestration(uid("W1"), 10_000)
        r2 = client.wait_for_orchestration(uid("W2"), 10_000)
        assert r1.status == "Completed"
        assert r1.output == "A"
        assert r2.status == "Completed"
        assert r2.output == "B"
    finally:
        runtime.shutdown(100)


# ─── 29. Detached Then Activity ──────────────────────────────────


def test_detached_then_activity(provider):
    """Parent fires-and-forgets child, then awaits activity. Tests replay correctness."""
    client = Client(provider)
    runtime = Runtime(provider, PyRuntimeOptions(dispatcher_poll_interval_ms=50))

    runtime.register_activity("EchoDT", lambda ctx, inp: inp)

    @runtime.register_orchestration("DetachedChild")
    def detached_child(ctx, input):
        yield ctx.schedule_timer(5)
        return f"child-{input}"

    @runtime.register_orchestration("DetachedParent")
    def detached_parent(ctx, input):
        yield ctx.start_orchestration("DetachedChild", uid("detached-child"), "payload")
        return (yield ctx.schedule_activity("EchoDT", "hello"))

    runtime.start()
    try:
        parent_id = uid("DetachedParent")
        client.start_orchestration(parent_id, "DetachedParent", None)
        result = client.wait_for_orchestration(parent_id, 10_000)
        assert result.status == "Completed"
        assert result.output == "hello"

        child_result = client.wait_for_orchestration(uid("detached-child"), 10_000)
        assert child_result.status == "Completed"
        assert child_result.output == "child-payload"
    finally:
        runtime.shutdown(100)


# ─── 30. Self-Pruning Eternal Orchestration ──────────────────────


def test_self_pruning_eternal_orchestration(provider):
    """Eternal orchestration processes 5 batches with CAN, pruning old executions each cycle."""
    client = Client(provider)
    runtime = Runtime(provider, PyRuntimeOptions(dispatcher_poll_interval_ms=50))

    runtime.register_activity("ProcessBatch", lambda ctx, inp: f"Processed batch {inp}")

    @runtime.register_activity("PruneSelf")
    def prune_self(ctx, input):
        c = ctx.get_client()
        c.prune_executions(ctx.instance_id, PyPruneOptions(keep_last=1))
        return "pruned"

    @runtime.register_orchestration("EternalPruner")
    def eternal_pruner(ctx, input):
        state = json.loads(input) if isinstance(input, str) else input
        batch_num = state["batchNum"]
        total = state["totalBatches"]

        result = yield ctx.schedule_activity("ProcessBatch", batch_num)
        yield ctx.schedule_activity("PruneSelf", None)

        if batch_num + 1 < total:
            yield ctx.continue_as_new(json.dumps({
                "batchNum": batch_num + 1,
                "totalBatches": total,
            }))
            return None
        else:
            return result

    runtime.start()
    try:
        instance_id = uid("EternalPruner")
        client.start_orchestration(
            instance_id, "EternalPruner",
            json.dumps({"batchNum": 0, "totalBatches": 5}),
        )
        result = client.wait_for_orchestration(instance_id, 30_000)
        assert result.status == "Completed"
        assert result.output == "Processed batch 4"
    finally:
        runtime.shutdown(100)


# ─── 31. Config Hot Reload with Persistent Events ────────────────


def test_config_hot_reload_persistent_events(provider):
    """Orchestration drains pending events between work cycles."""
    client = Client(provider)
    runtime = Runtime(provider, PyRuntimeOptions(dispatcher_poll_interval_ms=50))

    runtime.register_activity("ApplyConfig", lambda ctx, inp: f"applied:{inp}")

    @runtime.register_orchestration("ConfigHotReload")
    def config_hot_reload(ctx, input):
        log = []

        for cycle in range(3):
            # Drain loop: race dequeue vs short timer
            while True:
                winner = yield ctx.race(
                    ctx.dequeue_event("ConfigUpdate"),
                    ctx.schedule_timer(50),
                )
                if winner["index"] == 0:
                    log.append(winner["value"])
                else:
                    break

            log.append(f"cycle:{cycle}")
            yield ctx.schedule_timer(100)
            yield ctx.schedule_activity("ApplyConfig", f"c{cycle}")

        # Final drain
        while True:
            winner = yield ctx.race(
                ctx.dequeue_event("ConfigUpdate"),
                ctx.schedule_timer(50),
            )
            if winner["index"] == 0:
                log.append(winner["value"])
            else:
                break

        return ",".join(str(x) for x in log)

    runtime.start()
    try:
        instance_id = uid("ConfigHotReload")
        client.start_orchestration(instance_id, "ConfigHotReload", None)
        # Enqueue v1 and v2 immediately after start (before first drain completes)
        client.enqueue_event(instance_id, "ConfigUpdate", "v1")
        client.enqueue_event(instance_id, "ConfigUpdate", "v2")

        time.sleep(0.3)
        client.enqueue_event(instance_id, "ConfigUpdate", "v3")

        result = client.wait_for_orchestration(instance_id, 30_000)
        assert result.status == "Completed"
        output = result.output
        # v1 and v2 should appear before cycle:0
        v1_pos = output.index("v1")
        v2_pos = output.index("v2")
        cycle0_pos = output.index("cycle:0")
        assert v1_pos < cycle0_pos, f"v1 should be before cycle:0, got: {output}"
        assert v2_pos < cycle0_pos, f"v2 should be before cycle:0, got: {output}"
        # v3 should appear somewhere in the output
        assert "v3" in output, f"v3 should be in output, got: {output}"
    finally:
        runtime.shutdown(100)


# ─── 32. Typed API ───────────────────────────────────────────────


def test_typed_activity_round_trip(provider):
    """Activity receives/returns JSON objects via typed API."""

    def setup(rt):
        @rt.register_activity("ProcessData")
        def process_data(ctx, input):
            return {"processed": True, "name": input["name"], "count": input["count"] * 2}

        @rt.register_orchestration("TypedActivityRoundTrip")
        def typed_orch(ctx, input):
            result = yield ctx.schedule_activity_typed(
                "ProcessData", {"name": "test", "count": 5}
            )
            return result

    result = run_orchestration(provider, "TypedActivityRoundTrip", None, setup)
    assert result.status == "Completed"
    assert result.output == {"processed": True, "name": "test", "count": 10}


def test_typed_all_returns_parsed(provider):
    """all_typed auto-unwraps ok values from join results."""

    def setup(rt):
        @rt.register_activity("MakeItem")
        def make_item(ctx, input):
            return {"id": input, "status": "created"}

        @rt.register_orchestration("TypedAllTest")
        def typed_all_orch(ctx, input):
            tasks = [ctx.schedule_activity_typed("MakeItem", i) for i in range(3)]
            results = yield ctx.all_typed(tasks)
            return results

    result = run_orchestration(provider, "TypedAllTest", None, setup)
    assert result.status == "Completed"
    output = result.output
    assert len(output) == 3
    ids = sorted([item["id"] for item in output])
    assert ids == [0, 1, 2]
    for item in output:
        assert item["status"] == "created"


def test_typed_race_returns_parsed(provider):
    """race_typed, verify winner value is parsed."""

    def setup(rt):
        @rt.register_activity("FastWork")
        def fast_work(ctx, input):
            return {"winner": True, "data": input}

        @rt.register_orchestration("TypedRaceTest")
        def typed_race_orch(ctx, input):
            winner = yield ctx.race_typed(
                ctx.schedule_activity_typed("FastWork", "speedy"),
                ctx.schedule_timer(60000),
            )
            return winner

    result = run_orchestration(provider, "TypedRaceTest", None, setup)
    assert result.status == "Completed"
    output = result.output
    assert output["index"] == 0
    assert output["value"] == {"winner": True, "data": "speedy"}


def test_typed_sub_orchestration_round_trip(provider):
    """Typed sub-orchestration call with auto JSON serialization."""

    def setup(rt):
        @rt.register_activity("SubWork")
        def sub_work(ctx, input):
            return {"result": f"processed-{input['key']}"}

        @rt.register_orchestration("TypedSubChild")
        def typed_sub_child(ctx, input):
            result = yield ctx.schedule_activity_typed("SubWork", input)
            return result

        @rt.register_orchestration("TypedSubParent")
        def typed_sub_parent(ctx, input):
            result = yield ctx.schedule_sub_orchestration_typed(
                "TypedSubChild", {"key": "alpha"}
            )
            return result

    result = run_orchestration(provider, "TypedSubParent", None, setup)
    assert result.status == "Completed"
    assert result.output == {"result": "processed-alpha"}


# ─── All Composition Edge Cases ──────────────────────────────────


def test_all_with_dynamic_fan_out(provider):
    """all() with a dynamically-built task list from input."""

    def setup(rt):
        @rt.register_activity("Square")
        def square(ctx, input):
            return input * input

        @rt.register_orchestration("AllDynamic")
        def all_dynamic(ctx, input):
            tasks = [ctx.schedule_activity("Square", n) for n in input]
            results = yield ctx.all(tasks)
            return [r.get("ok") for r in results]

    result = run_orchestration(provider, "AllDynamic", [2, 3, 5], setup)
    assert result.status == "Completed"
    assert result.output == [4, 9, 25]


# ─── initTracing ──────────────────────────────────────────────────────

import tempfile


def test_init_tracing_is_callable():
    """init_tracing is importable and callable."""
    from duroxide import init_tracing

    assert callable(init_tracing)


def test_init_tracing_writes_to_file():
    """init_tracing creates a log file (may fail if subscriber already installed)."""
    from duroxide import init_tracing

    with tempfile.NamedTemporaryFile(suffix=".log", delete=False) as f:
        log_file = f.name

    try:
        init_tracing(log_file, log_level="info")
    except RuntimeError as e:
        assert "Failed to init tracing" in str(e)
    finally:
        try:
            os.unlink(log_file)
        except OSError:
            pass


def test_init_tracing_invalid_path():
    """init_tracing raises a clear error for invalid log file paths."""
    from duroxide import init_tracing

    with pytest.raises(RuntimeError, match="Failed to open log file"):
        init_tracing("/nonexistent-dir/sub/test.log")
