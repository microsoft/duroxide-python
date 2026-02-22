"""
Tests for ctx.all() (join) and ctx.race() (select) with mixed task types,
including activity cooperative cancellation via is_cancelled().

Ported from duroxide-node/__tests__/races.test.js
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

# Load .env from project root
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

SCHEMA = "duroxide_python_races"
RUN_ID = f"rc{int(time.time() * 1000):x}"


def uid(name):
    return f"{RUN_ID}-{name}"


@pytest.fixture(scope="module")
def provider():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        pytest.skip("DATABASE_URL not set")
    return PostgresProvider.connect_with_schema(db_url, SCHEMA)


def run_orchestration(provider, name, input, setup_fn, timeout_ms=10_000):
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


# ─── ctx.all() with mixed task types ─────────────────────────────


class TestAllMixedTypes:
    def test_joins_activity_and_timer(self, provider):
        def setup(rt):
            rt.register_activity("Slow", lambda ctx, inp: f"done-{inp}")

            @rt.register_orchestration("AllActivityTimer")
            def all_act_timer(ctx, input):
                results = yield ctx.all([
                    ctx.schedule_activity("Slow", "work"),
                    ctx.schedule_timer(50),
                ])
                return results

        result = run_orchestration(provider, "AllActivityTimer", None, setup)
        assert result.status == "Completed"
        assert len(result.output) == 2
        assert result.output[0]["ok"] == "done-work"
        assert result.output[1]["ok"] is None

    def test_joins_activity_and_wait_event(self, provider):
        instance_id = uid("all-wait")
        client = Client(provider)
        runtime = Runtime(provider, PyRuntimeOptions(dispatcher_poll_interval_ms=50))

        runtime.register_activity("Quick", lambda ctx, inp: f"quick-{inp}")

        @runtime.register_orchestration("AllActivityWait")
        def all_act_wait(ctx, input):
            results = yield ctx.all([
                ctx.schedule_activity("Quick", "go"),
                ctx.wait_for_event("signal"),
            ])
            return results

        runtime.start()
        try:
            client.start_orchestration(instance_id, "AllActivityWait", None)
            time.sleep(0.5)
            client.raise_event(instance_id, "signal", {"msg": "hi"})
            result = client.wait_for_orchestration(instance_id, 10_000)

            assert result.status == "Completed"
            assert len(result.output) == 2
            assert result.output[0]["ok"] == "quick-go"
            assert result.output[1]["ok"] == {"msg": "hi"}
        finally:
            runtime.shutdown(100)

    def test_joins_multiple_timers(self, provider):
        def setup(rt):
            @rt.register_orchestration("AllTimers")
            def all_timers(ctx, input):
                results = yield ctx.all([
                    ctx.schedule_timer(50),
                    ctx.schedule_timer(100),
                ])
                return results

        result = run_orchestration(provider, "AllTimers", None, setup)
        assert result.status == "Completed"
        assert len(result.output) == 2
        assert result.output[0]["ok"] is None
        assert result.output[1]["ok"] is None

    def test_joins_activity_and_dequeue_event(self, provider):
        instance_id = uid("all-deq")
        client = Client(provider)
        runtime = Runtime(provider, PyRuntimeOptions(dispatcher_poll_interval_ms=50))

        runtime.register_activity("Quick", lambda ctx, inp: f"quick-{inp}")

        @runtime.register_orchestration("AllActivityDequeue")
        def all_act_dequeue(ctx, input):
            results = yield ctx.all([
                ctx.schedule_activity("Quick", "go"),
                ctx.dequeue_event("inbox"),
            ])
            return results

        runtime.start()
        try:
            client.start_orchestration(instance_id, "AllActivityDequeue", None)
            time.sleep(0.5)
            client.enqueue_event(instance_id, "inbox", {"prompt": "HELLO"})
            result = client.wait_for_orchestration(instance_id, 10_000)

            assert result.status == "Completed"
            assert len(result.output) == 2
            assert result.output[0]["ok"] == "quick-go"
            # Verify dequeue value is properly structured, not double-serialized
            val = result.output[1]["ok"]
            assert isinstance(val, dict), (
                f"all() dequeue value should be a dict, not {type(val).__name__}"
            )
            assert val == {"prompt": "HELLO"}
        finally:
            runtime.shutdown(100)


# ─── ctx.race() with mixed task types ────────────────────────────


class TestRaceMixedTypes:
    def test_races_activity_vs_timer_activity_wins(self, provider):
        def setup(rt):
            rt.register_activity("Fast", lambda ctx, inp: f"fast-{inp}")

            @rt.register_orchestration("RaceActTimer")
            def race_act_timer(ctx, input):
                winner = yield ctx.race(
                    ctx.schedule_activity("Fast", "go"),
                    ctx.schedule_timer(60000),
                )
                return winner

        result = run_orchestration(provider, "RaceActTimer", None, setup)
        assert result.status == "Completed"
        assert result.output["index"] == 0
        assert result.output["value"] == "fast-go"

    def test_races_timer_vs_activity_timer_wins_cooperative_cancel(self, provider):
        instance_id = uid("race-timer-act")
        client = Client(provider)
        runtime = Runtime(
            provider,
            PyRuntimeOptions(
                dispatcher_poll_interval_ms=50,
                worker_lock_timeout_ms=2000,
            ),
        )
        activity_cancelled = {"value": False}

        @runtime.register_activity("Glacial")
        def glacial(ctx, inp):
            # Cooperative cancellation: poll is_cancelled() instead of sleeping forever
            for _ in range(200):
                if ctx.is_cancelled():
                    activity_cancelled["value"] = True
                    return "cancelled"
                time.sleep(0.05)
            return "timeout"

        @runtime.register_orchestration("RaceTimerAct")
        def race_timer_act(ctx, input):
            winner = yield ctx.race(
                ctx.schedule_timer(50),
                ctx.schedule_activity("Glacial", "x"),
            )
            return winner

        runtime.start()
        try:
            client.start_orchestration(instance_id, "RaceTimerAct", None)
            result = client.wait_for_orchestration(instance_id, 15_000)

            assert result.status == "Completed"
            assert result.output["index"] == 0
            assert result.output["value"] is None

            # Wait for the cancellation signal to propagate
            for _ in range(60):
                if activity_cancelled["value"]:
                    break
                time.sleep(0.1)
            assert activity_cancelled["value"], "activity should have seen is_cancelled()"
        finally:
            runtime.shutdown(2000)

    def test_races_wait_event_vs_timer_event_wins(self, provider):
        instance_id = uid("race-wait")
        client = Client(provider)
        runtime = Runtime(provider, PyRuntimeOptions(dispatcher_poll_interval_ms=50))

        @runtime.register_orchestration("RaceWaitTimer")
        def race_wait_timer(ctx, input):
            winner = yield ctx.race(
                ctx.wait_for_event("approval"),
                ctx.schedule_timer(60000),
            )
            return winner

        runtime.start()
        try:
            client.start_orchestration(instance_id, "RaceWaitTimer", None)
            time.sleep(0.3)
            client.raise_event(instance_id, "approval", {"ok": True})
            result = client.wait_for_orchestration(instance_id, 10_000)

            assert result.status == "Completed"
            assert result.output["index"] == 0
            assert result.output["value"] == {"ok": True}
        finally:
            runtime.shutdown(100)

    def test_races_two_timers_shorter_wins(self, provider):
        def setup(rt):
            @rt.register_orchestration("RaceTwoTimers")
            def race_two_timers(ctx, input):
                winner = yield ctx.race(
                    ctx.schedule_timer(50),
                    ctx.schedule_timer(60000),
                )
                return winner

        result = run_orchestration(provider, "RaceTwoTimers", None, setup)
        assert result.status == "Completed"
        assert result.output["index"] == 0

    def test_race_dequeue_event_not_double_serialized(self, provider):
        """Regression test for duroxide#59: race(timer, dequeue_event) double-serializes value."""
        instance_id = uid("race-deq")
        client = Client(provider)
        runtime = Runtime(provider, PyRuntimeOptions(dispatcher_poll_interval_ms=50))

        @runtime.register_orchestration("RaceDequeue")
        def race_dequeue(ctx, input):
            winner = yield ctx.race(
                ctx.schedule_timer(60000),
                ctx.dequeue_event("messages"),
            )
            return winner

        runtime.start()
        try:
            client.start_orchestration(instance_id, "RaceDequeue", None)
            time.sleep(0.5)
            # Pass object directly — enqueue_event handles json.dumps internally
            client.enqueue_event(instance_id, "messages", {"prompt": "HELLO"})
            result = client.wait_for_orchestration(instance_id, 10_000)

            assert result.status == "Completed"
            assert result.output["index"] == 1

            # Before the fix, value was double-serialized: a string needing two json.loads() calls.
            # After the fix, value is the properly parsed object after one parse.
            val = result.output["value"]
            assert isinstance(val, dict), (
                f"race dequeue value should be a dict, not {type(val).__name__}"
            )
            assert val == {"prompt": "HELLO"}
        finally:
            runtime.shutdown(100)


# ─── Type preservation through all() and race() ─────────────────


class TestTypePreservation:
    def test_all_preserves_all_value_types(self, provider):
        def setup(rt):
            rt.register_activity("ReturnString", lambda ctx, inp: "hello")
            rt.register_activity("ReturnNumber", lambda ctx, inp: 42)
            rt.register_activity("ReturnObject", lambda ctx, inp: {"key": "val"})
            rt.register_activity("ReturnArray", lambda ctx, inp: [1, 2, 3])
            rt.register_activity("ReturnNull", lambda ctx, inp: None)
            rt.register_activity("ReturnBool", lambda ctx, inp: True)

            @rt.register_orchestration("AllTypes")
            def all_types(ctx, input):
                return (yield ctx.all([
                    ctx.schedule_activity("ReturnString", None),
                    ctx.schedule_activity("ReturnNumber", None),
                    ctx.schedule_activity("ReturnObject", None),
                    ctx.schedule_activity("ReturnArray", None),
                    ctx.schedule_activity("ReturnNull", None),
                    ctx.schedule_activity("ReturnBool", None),
                ]))

        result = run_orchestration(provider, "AllTypes", None, setup)
        assert result.status == "Completed"
        vals = [r["ok"] for r in result.output]

        assert vals[0] == "hello"
        assert isinstance(vals[0], str)

        assert vals[1] == 42
        assert isinstance(vals[1], int)

        assert vals[2] == {"key": "val"}
        assert isinstance(vals[2], dict)

        assert vals[3] == [1, 2, 3]
        assert isinstance(vals[3], list)

        assert vals[4] is None

        assert vals[5] is True
        assert isinstance(vals[5], bool)

    def test_race_preserves_all_value_types(self, provider):
        # Test string
        def setup_str(rt):
            rt.register_activity("FastStr", lambda ctx, inp: "hello")

            @rt.register_orchestration("RaceString")
            def race_str(ctx, input):
                return (yield ctx.race(
                    ctx.schedule_activity("FastStr", None),
                    ctx.schedule_timer(60000),
                ))

        r1 = run_orchestration(provider, "RaceString", None, setup_str)
        assert r1.output["value"] == "hello"
        assert isinstance(r1.output["value"], str)

        # Test number
        def setup_num(rt):
            rt.register_activity("FastNum", lambda ctx, inp: 42)

            @rt.register_orchestration("RaceNumber")
            def race_num(ctx, input):
                return (yield ctx.race(
                    ctx.schedule_activity("FastNum", None),
                    ctx.schedule_timer(60000),
                ))

        r2 = run_orchestration(provider, "RaceNumber", None, setup_num)
        assert r2.output["value"] == 42
        assert isinstance(r2.output["value"], int)

        # Test object
        def setup_obj(rt):
            rt.register_activity("FastObj", lambda ctx, inp: {"key": "val"})

            @rt.register_orchestration("RaceObject")
            def race_obj(ctx, input):
                return (yield ctx.race(
                    ctx.schedule_activity("FastObj", None),
                    ctx.schedule_timer(60000),
                ))

        r3 = run_orchestration(provider, "RaceObject", None, setup_obj)
        assert r3.output["value"] == {"key": "val"}
        assert isinstance(r3.output["value"], dict)
