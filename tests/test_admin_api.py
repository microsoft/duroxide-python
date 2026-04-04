"""
Management / Admin API tests for duroxide Python SDK — PostgreSQL backend.

Tests all client management APIs:
  - list_all_instances / list_instances_by_status
  - get_instance_info / get_execution_info
  - list_executions / read_execution_history
  - get_instance_tree
  - delete_instance / delete_instance_bulk
  - prune_executions / prune_executions_bulk
  - Versioned orchestration context methods:
    - schedule_sub_orchestration_versioned
    - start_orchestration_versioned (detached, from ctx)
    - continue_as_new_versioned

Ported from duroxide-node/__tests__/admin_api.test.js
Requires: DATABASE_URL env var or .env file with a PostgreSQL connection string.
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
    RuntimeOptions,
    PruneOptions,
    InstanceFilter,
)

# Load .env from project root
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

SCHEMA = "duroxide_python_admin"
RUN_ID = f"adm{int(time.time() * 1000):x}"


def uid(name):
    return f"{RUN_ID}-{name}"


@pytest.fixture(scope="module")
def provider():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        pytest.skip("DATABASE_URL not set")
    return PostgresProvider.connect_with_schema(db_url, SCHEMA)


def run_to_completion(provider, orch_name, input, setup_fn, timeout_ms=15_000):
    """Run an orchestration to completion and return (client, result, instance_id)."""
    client = Client(provider)
    runtime = Runtime(provider, RuntimeOptions(dispatcher_poll_interval_ms=50))
    setup_fn(runtime)
    runtime.start()

    try:
        instance_id = uid(orch_name)
        client.start_orchestration(instance_id, orch_name, input)
        result = client.wait_for_orchestration(instance_id, timeout_ms)
        return client, result, instance_id
    finally:
        runtime.shutdown(100)


# ─── 1. list_all_instances ───────────────────────────────────────


def test_list_all_instances(provider):
    client = Client(provider)
    runtime = Runtime(provider, RuntimeOptions(dispatcher_poll_interval_ms=50))

    runtime.register_activity("echo", lambda ctx, inp: inp)

    @runtime.register_orchestration("ListTest")
    def list_test(ctx, input):
        return (yield ctx.schedule_activity("echo", input))

    runtime.start()
    try:
        id1 = uid("list-1")
        id2 = uid("list-2")
        client.start_orchestration(id1, "ListTest", {"a": 1})
        client.start_orchestration(id2, "ListTest", {"a": 2})
        client.wait_for_orchestration(id1, 10_000)
        client.wait_for_orchestration(id2, 10_000)

        instances = client.list_all_instances()
        assert id1 in instances, f"should contain {id1}"
        assert id2 in instances, f"should contain {id2}"
    finally:
        runtime.shutdown(100)


# ─── 2. list_instances_by_status ─────────────────────────────────


def test_list_instances_by_status(provider):
    def setup(rt):
        rt.register_activity("echo", lambda ctx, inp: inp)

        @rt.register_orchestration("StatusFilter")
        def status_filter(ctx, input):
            return (yield ctx.schedule_activity("echo", input))

    client, result, instance_id = run_to_completion(
        provider, "StatusFilter", "hello", setup
    )

    completed = client.list_instances_by_status("Completed")
    assert instance_id in completed, "should list the completed instance"


# ─── 3. get_instance_info ────────────────────────────────────────


def test_get_instance_info(provider):
    def setup(rt):
        rt.register_activity("double", lambda ctx, inp: inp["x"] * 2)

        @rt.register_orchestration("InfoTest")
        def info_test(ctx, input):
            return (yield ctx.schedule_activity("double", input))

    client, result, instance_id = run_to_completion(
        provider, "InfoTest", {"x": 42}, setup
    )

    info = client.get_instance_info(instance_id)
    assert info.instance_id == instance_id
    assert info.orchestration_name == "InfoTest"
    assert info.status == "Completed"
    assert info.current_execution_id >= 1
    assert info.created_at > 0
    assert info.updated_at > 0


# ─── 4. get_orchestration_stats ──────────────────────────────────


def test_get_orchestration_stats(provider):
    def setup(rt):
        rt.register_activity("echo", lambda ctx, inp: inp)

        @rt.register_orchestration("StatsTest")
        def stats_test(ctx, input):
            ctx.set_kv_value("status", "running")
            result = yield ctx.schedule_activity("echo", input)
            ctx.set_kv_value("result", json.dumps(result))
            return result

    client, result, instance_id = run_to_completion(
        provider, "StatsTest", {"value": 42}, setup
    )

    stats = client.get_orchestration_stats(instance_id)
    assert stats is not None
    assert stats.history_event_count > 0
    assert stats.history_size_bytes > 0
    assert stats.kv_user_key_count == 2
    assert stats.kv_total_value_bytes >= len("running") + len(json.dumps({"value": 42}))

    assert client.get_orchestration_stats(uid("missing-stats")) is None


# ─── 5. get_execution_info ───────────────────────────────────────


def test_get_execution_info(provider):
    def setup(rt):
        rt.register_activity("echo", lambda ctx, inp: inp)

        @rt.register_orchestration("ExecInfo")
        def exec_info(ctx, input):
            return (yield ctx.schedule_activity("echo", input))

    client, result, instance_id = run_to_completion(
        provider, "ExecInfo", "test", setup
    )

    info = client.get_execution_info(instance_id, 1)
    assert info.execution_id == 1
    assert info.status == "Completed"
    assert info.event_count > 0, "should have events"
    assert info.started_at > 0


# ─── 6. list_executions ─────────────────────────────────────────


def test_list_executions(provider):
    client = Client(provider)
    runtime = Runtime(provider, RuntimeOptions(dispatcher_poll_interval_ms=50))

    @runtime.register_orchestration("CANExec")
    def can_exec(ctx, input):
        n = input.get("n", 0) if isinstance(input, dict) else 0
        if n >= 2:
            return {"done": True, "iterations": n}
        yield ctx.continue_as_new({"n": n + 1})

    runtime.start()
    instance_id = uid("can-exec")
    try:
        client.start_orchestration(instance_id, "CANExec", {"n": 0})
        client.wait_for_orchestration(instance_id, 15_000)

        executions = client.list_executions(instance_id)
        assert len(executions) >= 3, f"should have 3+ executions, got {len(executions)}"
        assert executions[0] == 1
    finally:
        runtime.shutdown(100)


# ─── 7. read_execution_history ───────────────────────────────────


def test_read_execution_history(provider):
    def setup(rt):
        rt.register_activity("echo", lambda ctx, inp: inp)

        @rt.register_orchestration("HistoryTest")
        def history_test(ctx, input):
            return (yield ctx.schedule_activity("echo", input))

    client, result, instance_id = run_to_completion(
        provider, "HistoryTest", "data", setup
    )

    events = client.read_execution_history(instance_id, 1)
    assert len(events) > 0, "should have events"
    first_event = events[0]
    assert "OrchestrationStarted" in first_event.kind, (
        f"first event should be OrchestrationStarted, got {first_event.kind}"
    )
    assert first_event.timestamp_ms > 0


def test_read_execution_history_data(provider):
    """Verify that event data is exposed in history events."""
    def setup(rt):
        rt.register_activity("reverse", lambda ctx, inp: {"reversed": True, **inp})

        @rt.register_orchestration("HistoryDataTest")
        def history_data_test(ctx, input):
            return (yield ctx.schedule_activity("reverse", input))

    client, result, instance_id = run_to_completion(
        provider, "HistoryDataTest", {"greeting": "hello"}, setup
    )

    events = client.read_execution_history(instance_id, 1)

    # OrchestrationStarted should have name, version, input
    started = [e for e in events if e.kind == "OrchestrationStarted"][0]
    assert started.data is not None, "OrchestrationStarted should have data"
    import json
    started_data = json.loads(started.data)
    assert started_data["name"] == "HistoryDataTest"
    assert started_data["input"] == {"greeting": "hello"}

    # ActivityScheduled should have name and input
    scheduled = [e for e in events if e.kind == "ActivityScheduled"][0]
    assert scheduled.data is not None, "ActivityScheduled should have data"
    sched_data = json.loads(scheduled.data)
    assert sched_data["name"] == "reverse"

    # ActivityCompleted should have the result
    completed = [e for e in events if e.kind == "ActivityCompleted"][0]
    assert completed.data is not None, "ActivityCompleted should have data"
    completed_data = json.loads(completed.data)
    assert completed_data["reversed"] is True
    assert completed_data["greeting"] == "hello"

    # OrchestrationCompleted should have output
    orch_completed = [e for e in events if e.kind == "OrchestrationCompleted"][0]
    assert orch_completed.data is not None, "OrchestrationCompleted should have data"
    orch_data = json.loads(orch_completed.data)
    assert orch_data["reversed"] is True


# ─── 8. get_instance_tree ────────────────────────────────────────


def test_get_instance_tree(provider):
    client = Client(provider)
    runtime = Runtime(provider, RuntimeOptions(dispatcher_poll_interval_ms=50))

    runtime.register_activity("echo", lambda ctx, inp: inp)

    @runtime.register_orchestration("Child")
    def child(ctx, input):
        return (yield ctx.schedule_activity("echo", input))

    @runtime.register_orchestration("Parent")
    def parent(ctx, input):
        child_result = yield ctx.schedule_sub_orchestration("Child", input)
        return {"parent": True, "child": child_result}

    runtime.start()
    instance_id = uid("tree-parent")
    try:
        client.start_orchestration(instance_id, "Parent", {"val": 1})
        result = client.wait_for_orchestration(instance_id, 15_000)
        assert result.status == "Completed"

        tree = client.get_instance_tree(instance_id)
        assert tree.root_id == instance_id
        assert tree.size >= 2, f"tree should have ≥2 nodes, got {tree.size}"
        assert instance_id in tree.all_ids
    finally:
        runtime.shutdown(100)


# ─── 8. delete_instance ──────────────────────────────────────────


def test_delete_instance(provider):
    def setup(rt):
        rt.register_activity("echo", lambda ctx, inp: inp)

        @rt.register_orchestration("DeleteMe")
        def delete_me(ctx, input):
            return (yield ctx.schedule_activity("echo", input))

    client, result, instance_id = run_to_completion(
        provider, "DeleteMe", "bye", setup
    )

    del_result = client.delete_instance(instance_id, False)
    assert del_result.instances_deleted >= 1, "should delete at least 1 instance"
    assert del_result.events_deleted >= 1, "should delete events"

    # Verify it's gone
    status = client.get_status(instance_id)
    assert status.status == "NotFound"


# ─── 9. delete_instance_bulk ─────────────────────────────────────


def test_delete_instance_bulk(provider):
    client = Client(provider)
    runtime = Runtime(provider, RuntimeOptions(dispatcher_poll_interval_ms=50))

    runtime.register_activity("echo", lambda ctx, inp: inp)

    @runtime.register_orchestration("BulkDel")
    def bulk_del(ctx, input):
        return (yield ctx.schedule_activity("echo", input))

    runtime.start()
    ids = [uid("bulk-1"), uid("bulk-2")]
    try:
        for id_ in ids:
            client.start_orchestration(id_, "BulkDel", {"id": id_})
        for id_ in ids:
            client.wait_for_orchestration(id_, 10_000)
    finally:
        runtime.shutdown(100)

    result = client.delete_instance_bulk(InstanceFilter(instance_ids=ids))
    assert result.instances_deleted >= 2, (
        f"should delete 2+, got {result.instances_deleted}"
    )

    # Verify they're gone
    for id_ in ids:
        status = client.get_status(id_)
        assert status.status == "NotFound"


# ─── 10. prune_executions ────────────────────────────────────────


def test_prune_executions(provider):
    client = Client(provider)
    runtime = Runtime(provider, RuntimeOptions(dispatcher_poll_interval_ms=50))

    @runtime.register_orchestration("PruneTarget")
    def prune_target(ctx, input):
        n = input.get("n", 0) if isinstance(input, dict) else 0
        if n >= 3:
            return {"done": True, "iterations": n}
        yield ctx.continue_as_new({"n": n + 1})

    runtime.start()
    instance_id = uid("prune")
    try:
        client.start_orchestration(instance_id, "PruneTarget", {"n": 0})
        client.wait_for_orchestration(instance_id, 15_000)
    finally:
        runtime.shutdown(100)

    # Should have 4 executions (0,1,2,3)
    before = client.list_executions(instance_id)
    assert len(before) >= 4, f"expected ≥4 executions, got {len(before)}"

    # Prune: keep last 1
    result = client.prune_executions(instance_id, PruneOptions(keep_last=1))
    assert result.executions_deleted >= 3, (
        f"should prune ≥3, got {result.executions_deleted}"
    )

    after = client.list_executions(instance_id)
    assert len(after) == 1, "should have 1 execution left"


# ─── 11. prune_executions_bulk ───────────────────────────────────


def test_prune_executions_bulk(provider):
    client = Client(provider)
    runtime = Runtime(provider, RuntimeOptions(dispatcher_poll_interval_ms=50))

    @runtime.register_orchestration("BulkPrune")
    def bulk_prune(ctx, input):
        n = input.get("n", 0) if isinstance(input, dict) else 0
        if n >= 2:
            return {"done": True}
        yield ctx.continue_as_new({"n": n + 1})

    runtime.start()
    ids = [uid("bprune-1"), uid("bprune-2")]
    try:
        for id_ in ids:
            client.start_orchestration(id_, "BulkPrune", {"n": 0})
        for id_ in ids:
            client.wait_for_orchestration(id_, 15_000)
    finally:
        runtime.shutdown(100)

    result = client.prune_executions_bulk(
        InstanceFilter(instance_ids=ids),
        PruneOptions(keep_last=1),
    )
    assert result.instances_processed >= 2, (
        f"should process ≥2, got {result.instances_processed}"
    )
    assert result.executions_deleted >= 4, (
        f"should prune ≥4, got {result.executions_deleted}"
    )


# ─── 12. Versioned sub-orchestration from ctx ────────────────────


def test_versioned_sub_orchestration(provider):
    def setup(rt):
        rt.register_activity("double", lambda ctx, inp: inp * 2)

        @rt.register_orchestration_versioned("VersionedChild", "2.0.0")
        def versioned_child(ctx, input):
            doubled = yield ctx.schedule_activity("double", input["val"])
            return {"version": "2.0.0", "result": doubled}

        @rt.register_orchestration("VersionedSubParent")
        def versioned_sub_parent(ctx, input):
            child = yield ctx.schedule_sub_orchestration_versioned(
                "VersionedChild", "2.0.0", input
            )
            return {"parent": True, "child": child}

    client, result, instance_id = run_to_completion(
        provider, "VersionedSubParent", {"val": 10}, setup
    )

    assert result.status == "Completed"
    child = result.output["child"]
    if isinstance(child, str):
        child = json.loads(child)
    assert child["version"] == "2.0.0"
    assert child["result"] == 20


# ─── 13. Versioned detached orchestration from ctx ───────────────


def test_versioned_detached_orchestration(provider):
    client = Client(provider)
    runtime = Runtime(provider, RuntimeOptions(dispatcher_poll_interval_ms=50))

    runtime.register_activity("echo", lambda ctx, inp: inp)

    @runtime.register_orchestration_versioned("DetachedWorker", "3.0.0")
    def detached_worker(ctx, input):
        result = yield ctx.schedule_activity("echo", input)
        return {"version": "3.0.0", "echoed": result}

    @runtime.register_orchestration("DetachedStarter")
    def detached_starter(ctx, input):
        yield ctx.start_orchestration_versioned(
            "DetachedWorker", "3.0.0", input["child_id"], {"msg": "hello"}
        )
        return {"started": True}

    runtime.start()
    parent_id = uid("detach-starter")
    child_id = uid("detach-worker")
    try:
        client.start_orchestration(parent_id, "DetachedStarter", {"child_id": child_id})
        client.wait_for_orchestration(parent_id, 15_000)

        # Child should also complete
        child_result = client.wait_for_orchestration(child_id, 15_000)
        assert child_result.status == "Completed"
        assert child_result.output["version"] == "3.0.0"
        echoed = child_result.output["echoed"]
        if isinstance(echoed, str):
            echoed = json.loads(echoed)
        assert echoed["msg"] == "hello"
    finally:
        runtime.shutdown(100)


# ─── 14. continue_as_new_versioned ───────────────────────────────


def test_continue_as_new_versioned(provider):
    client = Client(provider)
    runtime = Runtime(provider, RuntimeOptions(dispatcher_poll_interval_ms=50))

    # v1: upgrades to v2 via continue_as_new_versioned
    @runtime.register_orchestration_versioned("VersionedCAN", "1.0.0")
    def versioned_can_v1(ctx, input):
        ctx.trace_info("[v1.0.0] upgrading to v2")
        yield ctx.continue_as_new_versioned({"upgraded": True, "from": "1.0.0"}, "2.0.0")

    # v2: the target version
    @runtime.register_orchestration_versioned("VersionedCAN", "2.0.0")
    def versioned_can_v2(ctx, input):
        ctx.trace_info("[v2.0.0] running upgraded logic")
        return {"version": "2.0.0", "input": input}

    runtime.start()
    instance_id = uid("can-versioned")
    try:
        client.start_orchestration_versioned(
            instance_id, "VersionedCAN", {"start": True}, "1.0.0"
        )
        result = client.wait_for_orchestration(instance_id, 15_000)
        assert result.status == "Completed"
        assert result.output["version"] == "2.0.0"
        assert result.output["input"]["upgraded"] is True
    finally:
        runtime.shutdown(100)
