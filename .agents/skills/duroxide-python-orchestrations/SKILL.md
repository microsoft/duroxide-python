---
name: duroxide-python-orchestrations
description: Writing durable workflows in Python using duroxide-python. Use when creating orchestrations, activities, writing tests, or when the user mentions generator workflows, yield patterns, or duroxide-python development.
---

# Duroxide-Python Orchestration Development

## Core Rule: Yield vs Regular Functions

| Context | Syntax | Why |
|---------|--------|-----|
| Orchestrations | `def` + `yield` (generator) | Rust replay engine needs step-by-step control |
| Activities | `def` (regular function) | Run once, result cached — no replay constraints |
| Orchestration tracing | Direct call (no yield) | Fire-and-forget, delegates to Rust |

```python
# ✅ Orchestration: generator, yield for durable operations
@runtime.register_orchestration("MyWorkflow")
def my_workflow(ctx, input):
    ctx.trace_info("starting")                                     # no yield
    result = yield ctx.schedule_activity("Work", input)            # yield
    return result

# ✅ Activity: regular function for I/O
@runtime.register_activity("Work")
def work(ctx, input):
    ctx.trace_info(f"processing {input}")                          # no yield
    data = requests.get(input["url"]).json()                       # sync I/O
    return data
```

**Never use `async def` with `yield` for orchestrations** — async generators break the replay model.

## Orchestration Context API

### Scheduling (MUST yield)

```python
def my_orch(ctx, input):
    # Activity
    result = yield ctx.schedule_activity("Name", input)

    # Activity with retry
    result = yield ctx.schedule_activity_with_retry("Name", input, {
        "max_attempts": 3,
        "backoff": "exponential",
        "timeout_ms": 5000,
        "total_timeout_ms": 30000,
    })

    # Timer (durable delay)
    yield ctx.schedule_timer(60000)  # 1 minute

    # External event
    event_data = yield ctx.wait_for_event("approval")

    # Sub-orchestration (waits for completion)
    child_result = yield ctx.schedule_sub_orchestration("Child", child_input)

    # Sub-orchestration with explicit ID
    child_result = yield ctx.schedule_sub_orchestration_with_id("Child", "child-1", child_input)

    # Fire-and-forget orchestration (returns immediately)
    yield ctx.start_orchestration("BackgroundWork", "bg-1", bg_input)

    # Deterministic values
    now = yield ctx.utc_now()      # timestamp in ms
    guid = yield ctx.new_guid()    # deterministic UUID

    # Continue as new (restart with fresh history)
    yield ctx.continue_as_new(new_input)
```

### Composition (MUST yield)

```python
# Fan-out / fan-in — wait for ALL tasks (supports all task types)
results = yield ctx.all([
    ctx.schedule_activity("TaskA", input_a),
    ctx.schedule_activity("TaskB", input_b),
    ctx.schedule_timer(5000),                    # timers work too
    ctx.wait_for_event("approval"),              # waits work too
])
# results = [result_a, result_b, {"ok": None}, {"ok": event_data}]

# Race — wait for FIRST of 2 tasks (supports all task types)
winner = yield ctx.race(
    ctx.schedule_activity("FastService", input),
    ctx.schedule_timer(5000),
)
# winner = {"index": 0|1, "value": ...}
```

**`ctx.race()` supports exactly 2 tasks** (maps to Rust `select2`). Nesting `all()`/`race()` inside `all()` or `race()` is **not supported** — the runtime rejects it.

### Cooperative Activity Cancellation

```python
@runtime.register_activity("LongTask")
def long_task(ctx, input):
    for i in range(1000):
        if ctx.is_cancelled():
            ctx.trace_info("cancelled, cleaning up")
            return {"status": "cancelled"}
        time.sleep(0.1)  # do work
    return {"status": "done"}
```

`ctx.is_cancelled()` checks whether the orchestration no longer needs the activity result (e.g., lost a race). Detection latency is `worker_lock_timeout_ms / 2` (default 30s → ~15s).

### Tracing (NO yield — fire-and-forget)

```python
ctx.trace_info("message")    # suppressed during replay automatically
ctx.trace_warn("message")
ctx.trace_error("message")
ctx.trace_debug("message")
```

Tracing delegates to the Rust `OrchestrationContext` which has the `is_replaying` guard. **Do not use `print()`** in orchestrations — it will duplicate on replay.

## Activity Context API

```python
@runtime.register_activity("MyActivity")
def my_activity(ctx, input):
    # Available fields
    ctx.instance_id
    ctx.execution_id
    ctx.orchestration_name
    ctx.orchestration_version
    ctx.activity_name
    ctx.worker_id

    # Cooperative cancellation
    if ctx.is_cancelled():
        ctx.trace_info("cancelled")
        return {"status": "cancelled"}

    # Tracing (delegates to Rust ActivityContext — full structured fields)
    ctx.trace_info(f"processing {input['id']}")
    ctx.trace_warn("slow response")
    ctx.trace_error("connection failed")
    ctx.trace_debug("raw payload: ...")

    # Activities can do anything — I/O, HTTP, DB, etc.
    data = requests.get(input["url"]).json()
    return data
```

## Determinism Rules

Orchestrations **must be deterministic** — the replay engine re-executes from the beginning on every dispatch:

| ✅ Safe | ❌ Breaks Replay |
|---------|-----------------|
| `yield ctx.utc_now()` | `time.time()` |
| `yield ctx.new_guid()` | `uuid.uuid4()` |
| `ctx.trace_info()` | `print()` (duplicates) |
| `yield ctx.schedule_timer(ms)` | `time.sleep()` |
| Pure computation, conditionals | `requests.get()`, file I/O, DB queries |
| `json.loads()`, `json.dumps()` | `os.environ["X"]` (may change) |

**All I/O belongs in activities**, not orchestrations.

## Common Patterns

### Error Handling

```python
def my_orch(ctx, input):
    try:
        result = yield ctx.schedule_activity("RiskyOp", input)
        return result
    except Exception as e:
        ctx.trace_error(f"failed: {e}")
        yield ctx.schedule_activity("Cleanup", {"error": str(e)})
        return {"status": "failed"}
```

### Eternal Orchestration (continue-as-new)

```python
def monitor(ctx, input):
    state = input.get("state", {"iteration": 0})
    health = yield ctx.schedule_activity("CheckHealth", input["target"])
    ctx.trace_info(f"check #{state['iteration']}: {health['status']}")
    yield ctx.schedule_timer(30000)
    yield ctx.continue_as_new({
        "target": input["target"],
        "state": {"iteration": state["iteration"] + 1},
    })
```

### Race with Timeout

```python
def my_orch(ctx, input):
    winner = yield ctx.race(
        ctx.schedule_activity("SlowOperation", input),
        ctx.schedule_timer(10000),
    )
    if winner["index"] == 1:
        ctx.trace_warn("operation timed out")
        return {"status": "timeout"}
    return {"status": "ok", "result": winner["value"]}
```

### Versioned Orchestrations

```python
@runtime.register_orchestration("MyWorkflow")
def my_workflow_v1(ctx, input):
    ctx.trace_info("[v1.0.0] starting")
    return (yield ctx.schedule_activity("Work", input))

@runtime.register_orchestration_versioned("MyWorkflow", "1.0.1")
def my_workflow_v2(ctx, input):
    ctx.trace_info("[v1.0.1] starting")
    yield ctx.schedule_activity("Validate", input)
    return (yield ctx.schedule_activity("Work", input))
```

## Writing Tests

Tests use pytest:

```python
import time, pytest
from duroxide import SqliteProvider, PostgresProvider, Client, Runtime, PyRuntimeOptions

@pytest.fixture(scope="module")
def provider():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        pytest.skip("DATABASE_URL not set")
    return PostgresProvider.connect_with_schema(db_url, "my_test_schema")

def test_my_feature(provider):
    client = Client(provider)
    runtime = Runtime(provider, PyRuntimeOptions(dispatcher_poll_interval_ms=50))

    runtime.register_activity("Echo", lambda ctx, inp: inp)

    @runtime.register_orchestration("MyWorkflow")
    def my_workflow(ctx, input):
        return (yield ctx.schedule_activity("Echo", input))

    runtime.start()
    try:
        client.start_orchestration("test-1", "MyWorkflow", "hello")
        result = client.wait_for_orchestration("test-1", 10_000)
        assert result.status == "Completed"
        assert result.output == "hello"
    finally:
        runtime.shutdown(100)
```

### Test Commands

```bash
source .venv/bin/activate
maturin develop                      # rebuild after Rust changes
pytest -v                            # all 54 tests
pytest tests/test_e2e.py -v          # e2e (27 tests)
pytest tests/test_races.py -v        # race/join (7 tests)
pytest tests/test_admin_api.py -v    # admin API (14 tests)
pytest tests/scenarios/ -v           # scenario tests (6 tests)
```

### Test Tips

- Use `SqliteProvider.in_memory()` for fast isolated tests (SQLite smoketest only)
- All PG tests need `DATABASE_URL` in `.env` (loaded by `python-dotenv`)
- Each test file uses a separate PG schema for isolation
- `runtime.shutdown(100)` permits 100 ms of grace plus five seconds of cleanup headroom. It can finish early; timeout raises while cleanup remains owned and requires application/supervisor termination.
- Set `RUST_LOG=info` and use `pytest -s` to see traces in test output
- Use `worker_lock_timeout_ms=2000` in tests needing fast activity cancellation detection

## Logging Control

```bash
RUST_LOG=info pytest -s                              # All INFO
RUST_LOG=duroxide::orchestration=debug pytest -s      # Orchestration DEBUG
RUST_LOG=duroxide::activity=info pytest -s            # Activity INFO only
```

Traces include structured fields automatically:
- **Orchestration**: `instance_id`, `execution_id`, `orchestration_name`, `orchestration_version`
- **Activity**: above + `activity_name`, `activity_id`, `worker_id`
