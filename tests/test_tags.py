"""
Activity tag routing tests for duroxide Python SDK.

Tests the .with_tag() API, TagFilter, and ActivityContext.tag().
Uses SQLite in-memory provider (no DATABASE_URL needed).
"""

import json
import os
import time
import pytest

from dotenv import load_dotenv

from duroxide import (
    SqliteProvider,
    PostgresProvider,
    Client,
    Runtime,
    RuntimeOptions,
    TagFilter,
)

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

RUN_ID = f"tag{int(time.time() * 1000):x}"


def uid(name):
    return f"{RUN_ID}-{name}"


# ─── SQLite Tests (no DB needed) ──────────────────────────────────


def test_tagged_activity_runs_on_matching_worker():
    """Schedule an activity with .with_tag('gpu'), start runtime with
    TagFilter.tags(['gpu']), verify execution and ctx.tag() returns 'gpu'."""
    provider = SqliteProvider.in_memory()
    client = Client(provider)
    runtime = Runtime(
        provider,
        RuntimeOptions(
            dispatcher_poll_interval_ms=50,
            worker_tag_filter=TagFilter.tags(["gpu"]),
        ),
    )

    @runtime.register_activity("GpuWork")
    def gpu_work(ctx, input):
        tag = ctx.tag()
        return {"result": input, "tag": tag}

    @runtime.register_orchestration("TaggedOrch")
    def tagged_orch(ctx, input):
        result = yield ctx.schedule_activity("GpuWork", "compute").with_tag("gpu")
        return result

    runtime.start()
    try:
        instance_id = uid("tagged")
        client.start_orchestration(instance_id, "TaggedOrch", "")
        result = client.wait_for_orchestration(instance_id, 10_000)
        assert result.status == "Completed"
        assert result.output["result"] == "compute"
        assert result.output["tag"] == "gpu"
    finally:
        runtime.shutdown(100)


def test_default_only_ignores_tagged_activity():
    """With DefaultOnly filter, a tagged activity should stall.
    Use a timer race to avoid hanging the test."""
    provider = SqliteProvider.in_memory()
    client = Client(provider)
    # Default filter = DefaultOnly (no tag filter specified)
    runtime = Runtime(
        provider,
        RuntimeOptions(dispatcher_poll_interval_ms=50),
    )

    @runtime.register_activity("TaggedWork")
    def tagged_work(ctx, input):
        return "done"

    @runtime.register_orchestration("DefaultOnlyOrch")
    def default_only_orch(ctx, input):
        # Race tagged activity vs timer — timer should win
        activity = ctx.schedule_activity("TaggedWork", "data").with_tag("gpu")
        timer = ctx.schedule_timer(500)
        result = yield ctx.race(activity, timer)
        if result["index"] == 1:
            return "timeout:no_gpu_worker"
        return f"completed:{result['value']}"

    runtime.start()
    try:
        instance_id = uid("default-only")
        client.start_orchestration(instance_id, "DefaultOnlyOrch", "")
        result = client.wait_for_orchestration(instance_id, 10_000)
        assert result.status == "Completed"
        assert result.output == "timeout:no_gpu_worker"
    finally:
        runtime.shutdown(100)


def test_default_and_filter_processes_both():
    """With DefaultAnd(['gpu']), both tagged and untagged activities run."""
    provider = SqliteProvider.in_memory()
    client = Client(provider)
    runtime = Runtime(
        provider,
        RuntimeOptions(
            dispatcher_poll_interval_ms=50,
            worker_tag_filter=TagFilter.default_and(["gpu"]),
        ),
    )

    @runtime.register_activity("Work")
    def work(ctx, input):
        return f"done:{input}:tag={ctx.tag()}"

    @runtime.register_orchestration("DefaultAndOrch")
    def default_and_orch(ctx, input):
        # One tagged, one untagged
        tagged = ctx.schedule_activity("Work", "tagged").with_tag("gpu")
        untagged = ctx.schedule_activity("Work", "untagged")
        results = yield ctx.all([tagged, untagged])
        return results

    runtime.start()
    try:
        instance_id = uid("default-and")
        client.start_orchestration(instance_id, "DefaultAndOrch", "")
        result = client.wait_for_orchestration(instance_id, 10_000)
        assert result.status == "Completed"
        # Results come as list of {"ok": value}
        values = [r["ok"] for r in result.output]
        assert "done:tagged:tag=gpu" in values
        assert "done:untagged:tag=None" in values
    finally:
        runtime.shutdown(100)


def test_any_filter_processes_all():
    """With Any filter, all activities run regardless of tag."""
    provider = SqliteProvider.in_memory()
    client = Client(provider)
    runtime = Runtime(
        provider,
        RuntimeOptions(
            dispatcher_poll_interval_ms=50,
            worker_tag_filter=TagFilter.ANY,
        ),
    )

    @runtime.register_activity("AnyWork")
    def any_work(ctx, input):
        return f"tag={ctx.tag()}"

    @runtime.register_orchestration("AnyOrch")
    def any_orch(ctx, input):
        r1 = yield ctx.schedule_activity("AnyWork", "a").with_tag("special")
        r2 = yield ctx.schedule_activity("AnyWork", "b")
        return [r1, r2]

    runtime.start()
    try:
        instance_id = uid("any-filter")
        client.start_orchestration(instance_id, "AnyOrch", "")
        result = client.wait_for_orchestration(instance_id, 10_000)
        assert result.status == "Completed"
        assert result.output[0] == "tag=special"
        assert result.output[1] == "tag=None"
    finally:
        runtime.shutdown(100)


def test_tag_filter_class():
    """Verify TagFilter produces correct string values."""
    assert TagFilter.DEFAULT_ONLY == "default_only"
    assert TagFilter.ANY == "any"
    assert TagFilter.NONE == "none"

    tags_val = json.loads(TagFilter.tags(["gpu", "cpu"]))
    assert set(tags_val["tags"]) == {"gpu", "cpu"}

    da_val = json.loads(TagFilter.default_and(["gpu"]))
    assert da_val["default_and"] == ["gpu"]


def test_with_tag_returns_scheduled_task():
    """Verify .with_tag() returns a ScheduledTask (dict subclass) with tag set."""
    from duroxide.context import OrchestrationContext, ScheduledTask

    ctx = OrchestrationContext({
        "instanceId": "test",
        "executionId": 1,
        "orchestrationName": "Test",
        "orchestrationVersion": "",
    })
    task = ctx.schedule_activity("Work", "data")
    assert isinstance(task, ScheduledTask)
    assert "tag" not in task

    tagged = task.with_tag("gpu")
    assert tagged is task  # mutates in place
    assert tagged["tag"] == "gpu"


def test_untagged_activity_returns_none_tag():
    """Schedule an untagged activity (no .with_tag()), verify ctx.tag() is None."""
    provider = SqliteProvider.in_memory()
    client = Client(provider)
    runtime = Runtime(
        provider,
        RuntimeOptions(dispatcher_poll_interval_ms=50),
    )

    @runtime.register_activity("Plain")
    def plain(ctx, input):
        return {"tag": ctx.tag()}

    @runtime.register_orchestration("NoTagOrch")
    def no_tag_orch(ctx, input):
        result = yield ctx.schedule_activity("Plain", input)
        return result

    runtime.start()
    try:
        instance_id = uid("untagged-none")
        client.start_orchestration(instance_id, "NoTagOrch", "x")
        result = client.wait_for_orchestration(instance_id, 10_000)
        assert result.status == "Completed"
        assert result.output["tag"] is None
    finally:
        runtime.shutdown(100)


# ─── PostgreSQL Tests ──────────────────────────────────────────────


@pytest.fixture(scope="module")
def pg_provider():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        pytest.skip("DATABASE_URL not set")
    return PostgresProvider.connect_with_schema(db_url, "duroxide_python_tags")


def test_heterogeneous_workers_gpu_cpu_untagged():
    """Fan-out: GPU render → CPU encode → untagged upload via DefaultAnd filter."""
    provider = SqliteProvider.in_memory()
    client = Client(provider)
    runtime = Runtime(
        provider,
        RuntimeOptions(
            dispatcher_poll_interval_ms=50,
            worker_tag_filter=TagFilter.default_and(["gpu", "cpu"]),
        ),
    )

    @runtime.register_activity("Render")
    def render(ctx, input):
        return f"rendered:{input}"

    @runtime.register_activity("Encode")
    def encode(ctx, input):
        return f"encoded:{input}"

    @runtime.register_activity("Upload")
    def upload(ctx, input):
        return f"uploaded:{input}"

    @runtime.register_orchestration("VideoPipeline")
    def video_pipeline(ctx, input):
        rendered = yield ctx.schedule_activity("Render", "frame42").with_tag("gpu")
        encoded = yield ctx.schedule_activity("Encode", rendered).with_tag("cpu")
        uploaded = yield ctx.schedule_activity("Upload", encoded)
        return uploaded

    runtime.start()
    try:
        client.start_orchestration("video-1", "VideoPipeline", "")
        result = client.wait_for_orchestration("video-1", 10_000)
        assert result.status == "Completed"
        assert result.output == "uploaded:encoded:rendered:frame42"
    finally:
        runtime.shutdown(100)


def test_starvation_safe_tagged_activity_timeout_fallback():
    """Race tagged activity vs timer; timer wins when no GPU worker exists."""
    provider = SqliteProvider.in_memory()
    client = Client(provider)
    runtime = Runtime(
        provider,
        RuntimeOptions(
            dispatcher_poll_interval_ms=50,
            worker_tag_filter=TagFilter.DEFAULT_ONLY,
        ),
    )

    @runtime.register_activity("GpuInference")
    def gpu_inference(ctx, input):
        return f"inference:{input}"

    @runtime.register_activity("CpuFallback")
    def cpu_fallback(ctx, input):
        return f"cpu_fallback:{input}"

    @runtime.register_orchestration("InferenceWithFallback")
    def inference_with_fallback(ctx, input):
        gpu_task = ctx.schedule_activity("GpuInference", input).with_tag("gpu")
        timeout = ctx.schedule_timer(500)
        winner = yield ctx.race(gpu_task, timeout)
        if winner["index"] == 0:
            return winner["value"]
        else:
            result = yield ctx.schedule_activity("CpuFallback", input)
            return result

    runtime.start()
    try:
        client.start_orchestration("infer-1", "InferenceWithFallback", "model-v3")
        result = client.wait_for_orchestration("infer-1", 10_000)
        assert result.status == "Completed"
        assert result.output == "cpu_fallback:model-v3"
    finally:
        runtime.shutdown(100)


def test_dual_runtime_orchestrator_plus_gpu_worker():
    """Two runtimes on same store: RT-A dispatches + CPU, RT-B handles GPU tags."""
    provider = SqliteProvider.in_memory()
    client = Client(provider)

    # Runtime A: orchestrator + default (CPU) worker
    rt_a = Runtime(
        provider,
        RuntimeOptions(
            dispatcher_poll_interval_ms=50,
            worker_tag_filter=TagFilter.DEFAULT_ONLY,
        ),
    )

    @rt_a.register_activity("PreProcess")
    def preprocess_a(ctx, input):
        return f"preprocessed:{input}"

    @rt_a.register_activity("GpuTrain")
    def gpu_train_a(ctx, input):
        return f"trained:{input}"

    @rt_a.register_activity("SaveModel")
    def save_model_a(ctx, input):
        return f"saved:{input}"

    @rt_a.register_orchestration("MLPipeline")
    def ml_pipeline_a(ctx, input):
        preprocessed = yield ctx.schedule_activity("PreProcess", input)
        model = yield ctx.schedule_activity("GpuTrain", preprocessed).with_tag("gpu")
        saved = yield ctx.schedule_activity("SaveModel", model)
        return saved

    # Runtime B: GPU worker only (no orchestration dispatcher)
    rt_b = Runtime(
        provider,
        RuntimeOptions(
            dispatcher_poll_interval_ms=50,
            orchestration_concurrency=0,
            worker_tag_filter=TagFilter.tags(["gpu"]),
        ),
    )

    @rt_b.register_activity("PreProcess")
    def preprocess_b(ctx, input):
        return f"preprocessed:{input}"

    @rt_b.register_activity("GpuTrain")
    def gpu_train_b(ctx, input):
        return f"trained:{input}"

    @rt_b.register_activity("SaveModel")
    def save_model_b(ctx, input):
        return f"saved:{input}"

    @rt_b.register_orchestration("MLPipeline")
    def ml_pipeline_b(ctx, input):
        preprocessed = yield ctx.schedule_activity("PreProcess", input)
        model = yield ctx.schedule_activity("GpuTrain", preprocessed).with_tag("gpu")
        saved = yield ctx.schedule_activity("SaveModel", model)
        return saved

    rt_a.start()
    rt_b.start()
    try:
        client.start_orchestration("ml-1", "MLPipeline", "dataset-v5")
        result = client.wait_for_orchestration("ml-1", 10_000)
        assert result.status == "Completed"
        assert result.output == "saved:trained:preprocessed:dataset-v5"
    finally:
        rt_b.shutdown(100)
        rt_a.shutdown(100)


def test_nested_error_handling_propagation():
    """Activity error propagates through orchestration via yield."""
    provider = SqliteProvider.in_memory()
    client = Client(provider)
    runtime = Runtime(provider, RuntimeOptions(dispatcher_poll_interval_ms=50))

    @runtime.register_activity("ProcessData")
    def process_data(ctx, input):
        if "error" in input:
            raise Exception("Processing failed")
        return f"Processed: {input}"

    @runtime.register_activity("FormatOutput")
    def format_output(ctx, input):
        return f"Final: {input}"

    @runtime.register_orchestration("NestedErrorHandling")
    def nested_error_handling(ctx, input):
        processed = yield ctx.schedule_activity("ProcessData", input)
        formatted = yield ctx.schedule_activity("FormatOutput", processed)
        return formatted

    runtime.start()
    try:
        # Success case
        client.start_orchestration("nested-ok", "NestedErrorHandling", "test")
        ok = client.wait_for_orchestration("nested-ok", 5_000)
        assert ok.status == "Completed"
        assert ok.output == "Final: Processed: test"

        # Error case
        client.start_orchestration("nested-err", "NestedErrorHandling", "error")
        err = client.wait_for_orchestration("nested-err", 5_000)
        assert err.status == "Failed"
        assert "Processing failed" in err.error
    finally:
        runtime.shutdown(100)


def test_error_recovery_with_logging():
    """Activity error caught, logged via another activity, then re-raised."""
    provider = SqliteProvider.in_memory()
    client = Client(provider)
    runtime = Runtime(provider, RuntimeOptions(dispatcher_poll_interval_ms=50))

    @runtime.register_activity("ProcessData")
    def process_data(ctx, input):
        if "error" in input:
            raise Exception("Processing failed")
        return f"Processed: {input}"

    @runtime.register_activity("LogError")
    def log_error(ctx, error):
        return f"Logged: {error}"

    @runtime.register_orchestration("ErrorRecovery")
    def error_recovery(ctx, input):
        try:
            result = yield ctx.schedule_activity("ProcessData", input)
            return result
        except Exception as e:
            yield ctx.schedule_activity("LogError", str(e))
            raise Exception(f"Failed to process '{input}': {e}")

    runtime.start()
    try:
        # Success case
        client.start_orchestration("recovery-ok", "ErrorRecovery", "test")
        ok = client.wait_for_orchestration("recovery-ok", 5_000)
        assert ok.status == "Completed"
        assert ok.output == "Processed: test"

        # Error recovery case
        client.start_orchestration("recovery-err", "ErrorRecovery", "error")
        err = client.wait_for_orchestration("recovery-err", 5_000)
        assert err.status == "Failed"
        assert "Failed to process 'error'" in err.error
    finally:
        runtime.shutdown(100)


# ─── PostgreSQL Tests ──────────────────────────────────────────────


def test_pg_tagged_activity(pg_provider):
    """Full PostgreSQL test for tagged activity routing."""
    client = Client(pg_provider)
    runtime = Runtime(
        pg_provider,
        RuntimeOptions(
            dispatcher_poll_interval_ms=50,
            worker_tag_filter=TagFilter.tags(["gpu"]),
        ),
    )

    @runtime.register_activity("PgGpuWork")
    def pg_gpu_work(ctx, input):
        return {"input": input, "tag": ctx.tag()}

    @runtime.register_orchestration("PgTaggedOrch")
    def pg_tagged_orch(ctx, input):
        result = yield ctx.schedule_activity("PgGpuWork", input).with_tag("gpu")
        return result

    runtime.start()
    try:
        instance_id = uid("pg-tagged")
        client.start_orchestration(instance_id, "PgTaggedOrch", "pg-compute")
        result = client.wait_for_orchestration(instance_id, 10_000)
        assert result.status == "Completed"
        assert result.output["input"] == "pg-compute"
        assert result.output["tag"] == "gpu"
    finally:
        runtime.shutdown(100)
