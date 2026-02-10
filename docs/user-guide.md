# User Guide

This guide covers how to build workflows with duroxide-python. For architecture details and internals, see [architecture.md](architecture.md).

## Installation

```bash
pip install duroxide
```

## Core Concepts

| Concept | What It Is |
|---------|-----------|
| **Orchestration** | A durable workflow (generator function). Survives restarts via replay. |
| **Activity** | A regular function for side effects (HTTP calls, DB writes). Runs once, result is cached. |
| **Provider** | Storage backend (SQLite or PostgreSQL). Stores orchestration state. |
| **Client** | Starts orchestrations, raises events, queries status. |
| **Runtime** | Runs the dispatchers that execute orchestrations and activities. |

## Minimal Example

```python
from duroxide import SqliteProvider, Client, Runtime

provider = SqliteProvider.in_memory()
client = Client(provider)
runtime = Runtime(provider)

@runtime.register_activity("SayHello")
def say_hello(ctx, name):
    return f"Hello, {name}!"

@runtime.register_orchestration("HelloWorkflow")
def hello_workflow(ctx, input):
    greeting = yield ctx.schedule_activity("SayHello", input["name"])
    return greeting

runtime.start()

client.start_orchestration("hello-1", "HelloWorkflow", {"name": "World"})
result = client.wait_for_orchestration("hello-1")
print(result.output)  # "Hello, World!"

runtime.shutdown()
```

## Orchestration Patterns

### Sequential Steps

```python
@runtime.register_orchestration("Pipeline")
def pipeline(ctx, input):
    step1 = yield ctx.schedule_activity("Extract", input)
    step2 = yield ctx.schedule_activity("Transform", step1)
    step3 = yield ctx.schedule_activity("Load", step2)
    return step3
```

### Fan-Out / Fan-In

Run multiple tasks in parallel and wait for all to complete. `ctx.all()` supports all task types — activities, timers, waits, and sub-orchestrations:

```python
@runtime.register_orchestration("FanOut")
def fan_out(ctx, input):
    tasks = [ctx.schedule_activity("ProcessItem", item) for item in input["items"]]
    results = yield ctx.all(tasks)
    return results
```

Mixed task types in `ctx.all()`:

```python
@runtime.register_orchestration("MixedAll")
def mixed_all(ctx, input):
    act_result, _, event_data = yield ctx.all([
        ctx.schedule_activity("Work", input),
        ctx.schedule_timer(1000),           # timers return {"ok": None}
        ctx.wait_for_event("approval"),     # waits return {"ok": event_data}
    ])
    return act_result
```

> **Note:** Nesting `ctx.all()` or `ctx.race()` inside each other is not supported — the runtime will reject it.

### Race / Select

Wait for the first of two tasks to complete. `ctx.race()` supports all task types:

```python
@runtime.register_orchestration("RaceExample")
def race_example(ctx, input):
    winner = yield ctx.race(
        ctx.schedule_activity("FastService", input),
        ctx.schedule_timer(5000),  # 5 second timeout
    )

    if winner["index"] == 0:
        return winner["value"]  # FastService completed first
    else:
        return "timed out"
```

`ctx.race()` supports exactly 2 tasks (maps to Rust `select2`). Nesting `ctx.all()` or `ctx.race()` inside each other is not supported.

### Durable Timers

```python
@runtime.register_orchestration("DelayedNotification")
def delayed_notification(ctx, input):
    yield ctx.schedule_timer(60 * 60 * 1000)  # wait 1 hour (survives restarts)
    yield ctx.schedule_activity("SendReminder", input)
    return "done"
```

### External Events

Wait for a signal from outside the orchestration:

```python
@runtime.register_orchestration("ApprovalWorkflow")
def approval_workflow(ctx, input):
    yield ctx.schedule_activity("RequestApproval", input)
    ctx.trace_info("waiting for approval...")

    approval = yield ctx.wait_for_event("approval")

    if approval["approved"]:
        yield ctx.schedule_activity("Execute", input)
        return "approved"
    else:
        return "rejected"

# Raise the event from outside:
client.raise_event("instance-1", "approval", {"approved": True})
```

### Sub-Orchestrations

Compose workflows from smaller workflows:

```python
@runtime.register_orchestration("Parent")
def parent(ctx, input):
    child_result = yield ctx.schedule_sub_orchestration("Child", input)
    return {"parent_result": child_result}

@runtime.register_orchestration("Child")
def child(ctx, input):
    r = yield ctx.schedule_activity("DoWork", input)
    return r
```

With an explicit instance ID:

```python
result = yield ctx.schedule_sub_orchestration_with_id(
    "Child", f"child-{input['id']}", input
)
```

### Fire-and-Forget Orchestrations

Start another orchestration without waiting for it to complete:

```python
@runtime.register_orchestration("CreateInstance")
def create_instance(ctx, input):
    yield ctx.schedule_activity("ProvisionVM", input)

    # Launch monitor — runs independently
    yield ctx.start_orchestration(
        "InstanceMonitor",
        f"monitor-{input['instance_id']}",
        {"instance_id": input["instance_id"]},
    )

    return {"status": "provisioned"}
```

### Continue-as-New (Eternal Orchestrations)

For long-running orchestrations that need periodic refresh (e.g., monitoring loops):

```python
@runtime.register_orchestration("Monitor")
def monitor(ctx, input):
    state = input.get("state", {"check_count": 0})

    # Do periodic work
    health = yield ctx.schedule_activity("CheckHealth", input["target"])
    ctx.trace_info(f"health check #{state['check_count']}: {health['status']}")

    # Wait before next check
    yield ctx.schedule_timer(30000)  # 30 seconds

    # Restart with updated state (keeps history from growing unbounded)
    yield ctx.continue_as_new({
        "target": input["target"],
        "state": {"check_count": state["check_count"] + 1},
    })
```

### Error Handling

Use try/except around yielded operations:

```python
@runtime.register_orchestration("SafeWorkflow")
def safe_workflow(ctx, input):
    try:
        result = yield ctx.schedule_activity("RiskyCall", input)
        return result
    except Exception as e:
        ctx.trace_error(f"activity failed: {e}")
        yield ctx.schedule_activity("Cleanup", {"error": str(e)})
        return {"status": "failed", "error": str(e)}
```

### Retry Policies

```python
result = yield ctx.schedule_activity_with_retry("FlakeyApi", input, {
    "max_attempts": 3,
    "backoff": "exponential",
    "timeout_ms": 5000,       # per-attempt timeout
    "total_timeout_ms": 30000, # total timeout across all attempts
})
```

## Activity Patterns

### Basic Activity

```python
@runtime.register_activity("SendEmail")
def send_email(ctx, input):
    ctx.trace_info(f"sending to {input['to']}")
    # ... actual email sending ...
    return {"sent": True}
```

### Activity with Client Access

Activities can start other orchestrations or raise events:

```python
@runtime.register_activity("TriggerCleanup")
def trigger_cleanup(ctx, input):
    client = ctx.get_client()
    client.start_orchestration(
        f"cleanup-{input['id']}",
        "CleanupWorkflow",
        {"resource_id": input["id"]},
    )
    return {"triggered": True}
```

### Returning Errors

Raise an exception to mark the activity as failed:

```python
@runtime.register_activity("ValidateInput")
def validate_input(ctx, input):
    if not input.get("email"):
        raise Exception("email is required")
    return {"valid": True}
```

### Cooperative Cancellation

Activities can check for cancellation (e.g., when they lose a race):

```python
@runtime.register_activity("LongRunning")
def long_running(ctx, input):
    for i in range(100):
        if ctx.is_cancelled():
            ctx.trace_info("cancelled, cleaning up")
            return "cancelled"
        time.sleep(0.1)  # do work
    return "done"
```

## Tracing

### Orchestration Tracing

Tracing calls are automatically suppressed during replay — no duplicates:

```python
@runtime.register_orchestration("Traced")
def traced(ctx, input):
    ctx.trace_info("[v1.0.0] starting workflow")
    ctx.trace_debug(f"input: {input}")

    result = yield ctx.schedule_activity("Work", input)

    ctx.trace_info(f"[v1.0.0] completed: {result}")
    return result
```

### Activity Tracing

Activity traces include full structured metadata (activity name, ID, worker ID):

```python
@runtime.register_activity("FetchData")
def fetch_data(ctx, input):
    ctx.trace_info(f"fetching data for {input['id']}")
    data = requests.get(input["url"]).json()
    ctx.trace_info(f"got {len(data)} records")
    return data
```

### Controlling Log Level

```bash
RUST_LOG=info pytest -s                              # INFO and above
RUST_LOG=duroxide::orchestration=debug pytest -s      # Orchestration debug
RUST_LOG=duroxide::activity=info pytest -s            # Activity info only
```

## Providers

### SQLite

Good for development, testing, and single-node deployments:

```python
# File-backed (persistent)
provider = SqliteProvider.open("myapp.db")

# In-memory (ephemeral, great for tests)
provider = SqliteProvider.in_memory()
```

### PostgreSQL

For production multi-node deployments:

```python
provider = PostgresProvider.connect("postgresql://user:pass@host:5432/mydb")

# With schema isolation
provider = PostgresProvider.connect_with_schema(
    "postgresql://user:pass@host:5432/mydb",
    "duroxide_app",
)
```

## Runtime Options

```python
from duroxide import Runtime, PyRuntimeOptions

runtime = Runtime(provider, PyRuntimeOptions(
    orchestration_concurrency=4,     # Max concurrent orchestration dispatches
    worker_concurrency=8,            # Max concurrent activity workers
    dispatcher_poll_interval_ms=100, # Polling interval in ms
    log_level="info",                # Tracing log level
    log_format="pretty",             # "pretty" or "json"
    service_name="my-service",       # Service name for tracing metadata
    service_version="1.0.0",         # Service version for tracing metadata
))
```

## Metrics

Get a snapshot of runtime metrics (requires observability to be configured):

```python
snapshot = runtime.metrics_snapshot()
if snapshot:
    print(f"Orchestrations started: {snapshot['orch_starts']}")
    print(f"Orchestrations completed: {snapshot['orch_completions']}")
    print(f"Activity successes: {snapshot['activity_success']}")
    print(f"Provider errors: {snapshot['provider_errors']}")
```

Returns `None` if observability is not enabled. The snapshot includes counters for orchestration starts/completions/failures, activity results, dispatcher stats, and provider errors.

## Client Operations

```python
client = Client(provider)

# Start an orchestration
client.start_orchestration("id", "WorkflowName", input_data)
client.start_orchestration_versioned("id", "WorkflowName", input_data, "1.0.2")

# Wait for completion (with timeout in ms)
result = client.wait_for_orchestration("id", 30000)
# result.status: "Completed" | "Failed" | "Running" | "Terminated" | ...

# Cancel a running orchestration
client.cancel_instance("id", "reason")

# Raise an event
client.raise_event("id", "event_name", event_data)

# Get status without waiting
status = client.get_status("id")

# Admin operations
client.delete_instance("id", force=False)
metrics = client.get_system_metrics()
depths = client.get_queue_depths()
```

## Versioning

Register multiple versions of an orchestration:

```python
@runtime.register_orchestration("MyWorkflow")
def my_workflow_v1(ctx, input):
    # v1.0.0 — original
    r = yield ctx.schedule_activity("Work", input)
    return r

@runtime.register_orchestration_versioned("MyWorkflow", "1.0.1")
def my_workflow_v2(ctx, input):
    # v1.0.1 — with validation
    yield ctx.schedule_activity("Validate", input)
    r = yield ctx.schedule_activity("Work", input)
    return r
```

New orchestrations use the latest version. Running orchestrations stay on their original version until they complete or call `continue_as_new`.

## Determinism Rules

Orchestration functions **must be deterministic**. The replay engine re-executes the generator from the beginning on every dispatch, feeding back cached results. If the code path changes, replay breaks.

**Do:**
- Use `yield ctx.utc_now()` for timestamps
- Use `yield ctx.new_guid()` for random IDs
- Use `ctx.trace_info()` for logging (auto-suppressed during replay)

**Don't:**
- Use `time.time()`, `random.random()`, `uuid.uuid4()`
- Make HTTP calls or read files in orchestrations
- Use `print()` (will duplicate on replay — use `ctx.trace_info()` instead)
- Read environment variables that might change between restarts

Activities have no such restrictions — they run once and can do anything.
