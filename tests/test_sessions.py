"""
End-to-end tests for activity session affinity in duroxide Python SDK.

Uses PostgreSQL backend with schema isolation and SQLite fallback tests.
Requires DATABASE_URL env var or .env file with a PostgreSQL connection string.
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
)

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

SCHEMA = "duroxide_python_sessions"

RUN_ID = f"sess{int(time.time() * 1000):x}"


def uid(name):
    return f"{RUN_ID}-{name}"


@pytest.fixture(scope="module")
def provider():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        pytest.skip("DATABASE_URL not set")
    return PostgresProvider.connect_with_schema(db_url, SCHEMA)


def run_orchestration(provider, name, input, setup_fn, timeout_ms=10_000, options=None):
    """Helper: register handlers, start runtime, run one orchestration, shutdown."""
    if options is None:
        options = PyRuntimeOptions(dispatcher_poll_interval_ms=50)
    client = Client(provider)
    runtime = Runtime(provider, options)

    setup_fn(runtime)
    runtime.start()

    try:
        instance_id = uid(name)
        client.start_orchestration(instance_id, name, input)
        return client.wait_for_orchestration(instance_id, timeout_ms)
    finally:
        runtime.shutdown(100)


# ─── 1. Basic Session Activity ──────────────────────────────────


def test_session_activity_basic(provider):
    """Schedule activities with session_id, verify they complete."""
    def setup(rt):
        @rt.register_activity("Echo")
        def echo(ctx, input):
            return f"echo:{input}"

        @rt.register_orchestration("SessionBasicOrch")
        def session_basic(ctx, input):
            r1 = yield ctx.schedule_activity_on_session("Echo", "hello", "my-session-1")
            r2 = yield ctx.schedule_activity_on_session("Echo", "world", "my-session-1")
            return f"{r1}|{r2}"

    result = run_orchestration(provider, "SessionBasicOrch", None, setup)
    assert result.status == "Completed"
    assert result.output == "echo:hello|echo:world"


# ─── 2. Session Fan-Out ─────────────────────────────────────────


def test_session_fan_out(provider):
    """Multiple activities with same session_id complete correctly."""
    def setup(rt):
        @rt.register_activity("Append")
        def append(ctx, input):
            return f"{input}!"

        @rt.register_orchestration("SessionFanOutOrch")
        def fan_out(ctx, input):
            results = []
            for i in range(3):
                r = yield ctx.schedule_activity_on_session("Append", f"item{i}", "fan-session")
                results.append(r)
            return results

    result = run_orchestration(provider, "SessionFanOutOrch", None, setup)
    assert result.status == "Completed"
    assert result.output == ["item0!", "item1!", "item2!"]


# ─── 3. Multiple Sessions ───────────────────────────────────────


def test_session_multiple(provider):
    """Different session_ids can run in parallel within the same orchestration."""
    def setup(rt):
        @rt.register_activity("Tag")
        def tag(ctx, input):
            return f"tagged:{input}"

        @rt.register_orchestration("MultiSessionOrch")
        def multi_session(ctx, input):
            r1 = yield ctx.schedule_activity_on_session("Tag", "a", "session-A")
            r2 = yield ctx.schedule_activity_on_session("Tag", "b", "session-B")
            r3 = yield ctx.schedule_activity_on_session("Tag", "c", "session-A")
            return f"{r1}|{r2}|{r3}"

    result = run_orchestration(provider, "MultiSessionOrch", None, setup)
    assert result.status == "Completed"
    assert result.output == "tagged:a|tagged:b|tagged:c"


# ─── 4. Session ID Visible in Activity Context ──────────────────


def test_session_id_visible_in_context(provider):
    """Activity can read session_id from its context."""
    def setup(rt):
        @rt.register_activity("CheckSession")
        def check_session(ctx, input):
            return {"session_id": ctx.session_id, "input": input}

        @rt.register_orchestration("CheckSessionOrch")
        def check_session_orch(ctx, input):
            r = yield ctx.schedule_activity_on_session("CheckSession", "test", "visible-session")
            return r

    result = run_orchestration(provider, "CheckSessionOrch", None, setup)
    assert result.status == "Completed"
    assert result.output["session_id"] == "visible-session"
    assert result.output["input"] == "test"


# ─── 5. Mixed Session and Regular Activities ────────────────────


def test_mixed_session_and_regular(provider):
    """Mix of regular and session activities in the same orchestration."""
    def setup(rt):
        @rt.register_activity("Work")
        def work(ctx, input):
            sid = ctx.session_id
            return f"{'S' if sid else 'R'}:{input}"

        @rt.register_orchestration("MixedOrch")
        def mixed(ctx, input):
            r1 = yield ctx.schedule_activity("Work", "a")
            r2 = yield ctx.schedule_activity_on_session("Work", "b", "mixed-sess")
            r3 = yield ctx.schedule_activity("Work", "c")
            return f"{r1}|{r2}|{r3}"

    result = run_orchestration(provider, "MixedOrch", None, setup)
    assert result.status == "Completed"
    assert result.output == "R:a|S:b|R:c"


# ─── 6. Session via schedule_activity keyword arg ────────────────


def test_session_via_keyword_arg(provider):
    """schedule_activity with session_id keyword works like schedule_activity_on_session."""
    def setup(rt):
        @rt.register_activity("Echo")
        def echo(ctx, input):
            return {"value": input, "session": ctx.session_id}

        @rt.register_orchestration("KeywordSessionOrch")
        def keyword_session(ctx, input):
            r = yield ctx.schedule_activity("Echo", "kw-test", session_id="kw-session")
            return r

    result = run_orchestration(provider, "KeywordSessionOrch", None, setup)
    assert result.status == "Completed"
    assert result.output["session"] == "kw-session"
    assert result.output["value"] == "kw-test"


# ─── 7. No Session ID is None in Context ────────────────────────


def test_no_session_id_is_none(provider):
    """Regular activity has session_id == None in context."""
    def setup(rt):
        @rt.register_activity("CheckNone")
        def check_none(ctx, input):
            return {"has_session": ctx.session_id is not None}

        @rt.register_orchestration("NoSessionOrch")
        def no_session(ctx, input):
            r = yield ctx.schedule_activity("CheckNone", "test")
            return r

    result = run_orchestration(provider, "NoSessionOrch", None, setup)
    assert result.status == "Completed"
    assert result.output["has_session"] is False


# ─── 8. Session with RuntimeOptions ─────────────────────────────


def test_session_with_runtime_options(provider):
    """Session config options are accepted by RuntimeOptions."""
    def setup(rt):
        @rt.register_activity("Echo")
        def echo(ctx, input):
            return input

        @rt.register_orchestration("SessionOptsOrch")
        def session_opts(ctx, input):
            r = yield ctx.schedule_activity_on_session("Echo", "opts-test", "opts-session")
            return r

    options = PyRuntimeOptions(
        dispatcher_poll_interval_ms=50,
        max_sessions_per_runtime=5,
        session_idle_timeout_ms=60000,
    )
    result = run_orchestration(
        provider, "SessionOptsOrch", None, setup, options=options
    )
    assert result.status == "Completed"
    assert result.output == "opts-test"


# ─── 9. Session with worker_node_id ─────────────────────────────


def test_session_with_worker_node_id(provider):
    """Session activities complete with worker_node_id option set."""
    def setup(rt):
        @rt.register_activity("Work")
        def work(ctx, input):
            return f"done:{input}"

        @rt.register_orchestration("SessionNodeIdOrch")
        def session_node_id(ctx, input):
            r1 = yield ctx.schedule_activity_on_session("Work", "a", "stable-sess")
            r2 = yield ctx.schedule_activity_on_session("Work", "b", "stable-sess")
            return f"{r1}|{r2}"

    options = PyRuntimeOptions(
        dispatcher_poll_interval_ms=50,
        worker_node_id="test-pod-1",
    )
    result = run_orchestration(
        provider, "SessionNodeIdOrch", None, setup, options=options
    )
    assert result.status == "Completed"
    assert result.output == "done:a|done:b"


# ─── SQLite Session Smoketest ────────────────────────────────────


def test_sqlite_session_smoketest():
    """Session activities work with SQLite provider."""
    sqlite_provider = SqliteProvider.in_memory()
    client = Client(sqlite_provider)
    runtime = Runtime(
        sqlite_provider, PyRuntimeOptions(dispatcher_poll_interval_ms=50)
    )

    @runtime.register_activity("Echo")
    def echo(ctx, inp):
        return {"value": inp, "session": ctx.session_id}

    @runtime.register_orchestration("SqliteSessionTest")
    def sqlite_session(ctx, input):
        r = yield ctx.schedule_activity_on_session("Echo", input, "sqlite-sess")
        return r

    runtime.start()
    try:
        instance_id = uid("sqlite-session")
        client.start_orchestration(instance_id, "SqliteSessionTest", "hello")
        result = client.wait_for_orchestration(instance_id, 5_000)
        assert result.status == "Completed"
        assert result.output["value"] == "hello"
        assert result.output["session"] == "sqlite-sess"
    finally:
        runtime.shutdown(100)


# ─── 10. Session Fan-Out Mixed with Regular ──────────────────────


def test_session_fan_out_mixed_with_regular(provider):
    """All() with 2 session + 2 regular activities in same join."""
    def setup(rt):
        @rt.register_activity("SessionWork")
        def session_work(ctx, input):
            return f"sess:{input}"

        @rt.register_activity("RegularWork")
        def regular_work(ctx, input):
            return f"reg:{input}"

        @rt.register_orchestration("MixedFanOutOrch")
        def mixed_fan_out(ctx, input):
            results = yield ctx.all([
                ctx.schedule_activity_on_session("SessionWork", "a", "s1"),
                ctx.schedule_activity("RegularWork", "b"),
                ctx.schedule_activity_on_session("SessionWork", "c", "s2"),
                ctx.schedule_activity("RegularWork", "d"),
            ])
            return [r.get("ok", r.get("err")) for r in results]

    result = run_orchestration(provider, "MixedFanOutOrch", None, setup)
    assert result.status == "Completed"
    assert result.output == ["sess:a", "reg:b", "sess:c", "reg:d"]


# ─── 11. Fan-Out Multiple Per Session Mixed ──────────────────────


def test_fan_out_multiple_per_session_mixed(provider):
    """All() with 2 on session-1, 2 on session-2, 2 non-session."""
    def setup(rt):
        @rt.register_activity("Tag")
        def tag(ctx, input):
            return f"tag:{input}"

        @rt.register_orchestration("MultiPerSessionOrch")
        def multi_per_session(ctx, input):
            results = yield ctx.all([
                ctx.schedule_activity_on_session("Tag", "a", "session-1"),
                ctx.schedule_activity_on_session("Tag", "b", "session-1"),
                ctx.schedule_activity_on_session("Tag", "c", "session-2"),
                ctx.schedule_activity_on_session("Tag", "d", "session-2"),
                ctx.schedule_activity("Tag", "e"),
                ctx.schedule_activity("Tag", "f"),
            ])
            return [r.get("ok", r.get("err")) for r in results]

    options = PyRuntimeOptions(
        dispatcher_poll_interval_ms=50,
        worker_node_id="test-multi-sess",
    )
    result = run_orchestration(provider, "MultiPerSessionOrch", None, setup, options=options)
    assert result.status == "Completed"
    assert result.output == ["tag:a", "tag:b", "tag:c", "tag:d", "tag:e", "tag:f"]


# ─── 12. Session Survives Continue-as-New ────────────────────────


def test_session_survives_continue_as_new(provider):
    """Session activity, CAN to iter 1, session activity again, complete."""
    def setup(rt):
        @rt.register_activity("Track")
        def track(ctx, input):
            return f"tracked:{input}"

        @rt.register_orchestration("SessionCANOrch")
        def session_can(ctx, input):
            iteration = int(input) if isinstance(input, str) else (input or 0)
            result = yield ctx.schedule_activity_on_session(
                "Track", f"iter-{iteration}", "can-session"
            )
            if iteration == 0:
                yield ctx.continue_as_new("1")
                return None
            else:
                return result

    result = run_orchestration(provider, "SessionCANOrch", "0", setup, timeout_ms=15_000)
    assert result.status == "Completed"
    assert result.output == "tracked:iter-1"


# ─── 13. Session CAN Versioned Upgrade ──────────────────────────


def test_session_can_versioned_upgrade(provider):
    """v1 does session work → CAN versioned to v2 → v2 does session work → complete."""
    client = Client(provider)
    runtime = Runtime(provider, PyRuntimeOptions(dispatcher_poll_interval_ms=50))

    @runtime.register_activity("Work")
    def work(ctx, input):
        return f"done:{input}"

    @runtime.register_orchestration_versioned("SessionVerOrch", "1.0.0")
    def session_ver_v1(ctx, input):
        result = yield ctx.schedule_activity_on_session("Work", "from-v1", "ver-session")
        yield ctx.continue_as_new_versioned(result, "2.0.0")
        return None

    @runtime.register_orchestration_versioned("SessionVerOrch", "2.0.0")
    def session_ver_v2(ctx, input):
        result = yield ctx.schedule_activity_on_session("Work", "from-v2", "ver-session")
        return f"{input}+{result}"

    runtime.start()
    try:
        instance_id = uid("SessionVerOrch")
        client.start_orchestration_versioned(
            instance_id, "SessionVerOrch", None, "1.0.0"
        )
        result = client.wait_for_orchestration(instance_id, 15_000)
        assert result.status == "Completed"
        assert result.output == "done:from-v1+done:from-v2"
    finally:
        runtime.shutdown(100)
