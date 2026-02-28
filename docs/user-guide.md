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

## Sessions (Activity Affinity)

Sessions provide **activity affinity** — all activities scheduled with the same `session_id` are routed to the same worker slot on the same runtime instance. This is useful when activities need to share in-memory state (e.g., a database connection, a loaded ML model, or a stateful client).

### Scheduling a Session Activity

Use `schedule_activity_on_session` or pass `session_id` as a keyword argument to `schedule_activity`:

```python
@runtime.register_orchestration("SessionWorkflow")
def session_workflow(ctx, input):
    # Explicit method
    r1 = yield ctx.schedule_activity_on_session("ProcessData", input, "my-session-1")

    # Keyword argument (equivalent)
    r2 = yield ctx.schedule_activity("ProcessData", input, session_id="my-session-1")

    return [r1, r2]
```

Both activities above run on the same worker slot because they share `session_id="my-session-1"`.

### Reading Session ID in an Activity

Activities can check their session ID via `ctx.session_id`. Regular (non-session) activities get `None`:

```python
@runtime.register_activity("MyActivity")
def my_activity(ctx, input):
    if ctx.session_id:
        print(f"Running in session: {ctx.session_id}")
    else:
        print("No session (regular activity)")
    return input
```

### Session Runtime Options

Configure session behavior via `PyRuntimeOptions`:

```python
from duroxide import Runtime, PyRuntimeOptions

runtime = Runtime(provider, PyRuntimeOptions(
    max_sessions_per_runtime=10,       # Max concurrent sessions (default: 10)
    session_idle_timeout_ms=300000,    # Idle timeout before releasing slot (default: 5 min)
    worker_node_id="pod-abc-123",      # Stable worker identity (e.g., K8s pod name)
))
```

| Option | Default | Description |
|--------|---------|-------------|
| `max_sessions_per_runtime` | 10 | Maximum number of concurrent session slots per runtime instance |
| `session_idle_timeout_ms` | 300000 (5 min) | How long an idle session slot is held before being released |
| `worker_node_id` | Auto-generated | Stable identity for session ownership. Set this to a K8s pod name or hostname so sessions survive runtime restarts on the same node. |

### Complete Example

```python
from duroxide import SqliteProvider, Client, Runtime, PyRuntimeOptions

provider = SqliteProvider.in_memory()
client = Client(provider)
runtime = Runtime(provider, PyRuntimeOptions(
    max_sessions_per_runtime=5,
    session_idle_timeout_ms=60000,
    worker_node_id="worker-1",
))

# Stateful activity — shares in-memory state within a session
session_state = {}

@runtime.register_activity("Accumulate")
def accumulate(ctx, input):
    sid = ctx.session_id
    if sid not in session_state:
        session_state[sid] = []
    session_state[sid].append(input)
    return session_state[sid]

@runtime.register_orchestration("BatchProcess")
def batch_process(ctx, input):
    for item in input["items"]:
        result = yield ctx.schedule_activity_on_session(
            "Accumulate", item, f"batch-{input['batch_id']}"
        )
    return result  # All items accumulated in order

runtime.start()

client.start_orchestration("batch-1", "BatchProcess", {
    "batch_id": "001",
    "items": ["a", "b", "c"],
})
result = client.wait_for_orchestration("batch-1")
print(result.output)  # ["a", "b", "c"]

runtime.shutdown()
```

## Custom Status — Orchestration Progress Reporting

Custom status lets orchestrations report progress visible to external clients. Status updates are fire-and-forget (no `yield` needed) and survive replays.

### Setting Custom Status

```python
@runtime.register_orchestration("ProvisionServer")
def provision_server(ctx, input):
    ctx.set_custom_status("validating configuration")
    yield ctx.schedule_activity("ValidateConfig", input)

    ctx.set_custom_status("creating VM")
    vm = yield ctx.schedule_activity("CreateVM", input)

    ctx.set_custom_status("installing software")
    yield ctx.schedule_activity("InstallSoftware", vm)

    ctx.reset_custom_status()  # clear status when done
    return {"vm_id": vm["id"]}
```

### Polling for Status Changes

Use `client.wait_for_status_change()` for efficient status polling — it blocks until the status version changes or the timeout expires:

```python
# Start orchestration
client.start_orchestration("prov-1", "ProvisionServer", config)

# Poll for status changes
last_version = 0
while True:
    result = client.wait_for_status_change("prov-1", last_version, 50, 10000)
    if result is None:
        break  # timeout — orchestration may have completed
    print(f"Status: {result.custom_status}")
    last_version = result.custom_status_version
```

**Parameters:**
- `instance_id` — the orchestration instance to watch
- `last_seen_version` — the version you last saw (0 to start); returns when version exceeds this
- `poll_interval_ms` — how often to poll the provider
- `timeout_ms` — max time to wait before returning `None`

**OrchestrationResult fields:**
- `result.custom_status` — the custom status string, or `None` if not set
- `result.custom_status_version` — monotonically increasing version counter

## Event Queues — Persistent FIFO Message Passing

Event queues provide durable, ordered message passing between external clients and orchestrations. Unlike `wait_for_event()` which waits for a single named event, event queues support FIFO ordering with multiple messages on named queues. Messages survive `continue_as_new`.

### Dequeuing Events in Orchestrations

```python
@runtime.register_orchestration("RequestProcessor")
def request_processor(ctx, input):
    # Block until a message arrives on the "requests" queue
    request_json = yield ctx.dequeue_event("requests")
    request = json.loads(request_json)

    result = yield ctx.schedule_activity("ProcessRequest", request)
    return result
```

### Enqueuing Events from Clients

```python
client.enqueue_event("proc-1", "requests", json.dumps({
    "action": "process",
    "data": {"id": 42},
}))
```

### Multiple Queues

Orchestrations can dequeue from different named queues:

```python
@runtime.register_orchestration("MultiQueue")
def multi_queue(ctx, input):
    # Each queue is independent — FIFO within each queue
    command = yield ctx.dequeue_event("commands")
    config = yield ctx.dequeue_event("config")
    return {"command": command, "config": config}
```

## Retry on Session — Retry with Session Affinity

`schedule_activity_with_retry_on_session` combines retry policies with session affinity — all retry attempts are pinned to the same worker session:

```python
@runtime.register_orchestration("RetrySessionWorkflow")
def retry_session_workflow(ctx, input):
    result = yield ctx.schedule_activity_with_retry_on_session(
        "FlakeyGpuTask",
        input,
        {"max_attempts": 3, "backoff_strategy": "none"},
        "gpu-session-1",
    )
    return result
```

This is useful when the activity relies on in-memory state (e.g., a loaded ML model or GPU context) that would be lost if retries landed on a different worker.

## Real-World Example: Copilot Chat Bot

This pattern combines event queues, custom status, and continue-as-new to build an interactive chat bot that processes messages one at a time:

```python
import json

@runtime.register_activity("Generate")
def generate(ctx, text):
    # Call an LLM or generate a response
    return f"Echo: {text}"

@runtime.register_orchestration("ChatBot")
def chat_bot(ctx, input):
    # Wait for next message on the "inbox" queue
    msg_json = yield ctx.dequeue_event("inbox")
    msg = json.loads(msg_json)

    # Process the message
    response = yield ctx.schedule_activity("Generate", msg["text"])

    # Report the response via custom status
    ctx.set_custom_status(json.dumps({
        "state": "replied",
        "response": response,
        "seq": msg["seq"],
    }))

    # Exit on "bye", otherwise loop
    if "bye" in msg["text"].lower():
        return f"Done after {msg['seq']} msgs"

    # Restart with fresh history (messages survive continue-as-new)
    return (yield ctx.continue_as_new(""))

# --- Client side ---

# Start the chat bot
client.start_orchestration("chat-1", "ChatBot", "")

# Send a message
client.enqueue_event("chat-1", "inbox", json.dumps({"seq": 1, "text": "Hello!"}))

# Wait for the response
status = client.wait_for_status_change("chat-1", 0, 50, 10000)
reply = json.loads(status.custom_status)
print(reply["response"])  # "Echo: Hello!"

# End the conversation
client.enqueue_event("chat-1", "inbox", json.dumps({"seq": 2, "text": "Bye!"}))
```

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
    max_sessions_per_runtime=10,     # Max concurrent session slots
    session_idle_timeout_ms=300000,  # Session idle timeout (5 min default)
    worker_node_id="pod-name",       # Stable worker identity for sessions
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
# result.custom_status: custom status string or None
# result.custom_status_version: monotonically increasing version counter

# Cancel a running orchestration
client.cancel_instance("id", "reason")

# Raise an event
client.raise_event("id", "event_name", event_data)

# Enqueue event to a named queue (FIFO, survives continue-as-new)
client.enqueue_event("id", "queue_name", data_string)

# Poll for custom status changes
status = client.wait_for_status_change("id", last_seen_version, poll_interval_ms, timeout_ms)

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

## Multi-Step Parallel Blocks (Sub-Orchestration Pattern)

In Rust, arbitrary async blocks can be composed with `join()`/`select()`. In the Python SDK, `all()`/`race()` only accept single task descriptors — multi-step blocks must be wrapped as sub-orchestrations.

```python
# Pattern: wrap multi-step logic as a sub-orchestration
@runtime.register_orchestration("BlockA")
def block_a(ctx, input):
    first = yield ctx.schedule_activity("Step", "A1")
    if "step" in first:
        second = yield ctx.schedule_activity("Step", "A2")
        return f"A:[{first},{second}]"
    return "A:fallback"

@runtime.register_orchestration("BlockB")
def block_b(ctx, input):
    yield ctx.schedule_timer(5)
    result = yield ctx.schedule_activity("Step", "B1")
    return f"B:[timer,{result}]"

# Parent: join/race sub-orchestration descriptors
@runtime.register_orchestration("Parent")
def parent(ctx, input):
    # Join multiple multi-step blocks
    a, b = yield ctx.all([
        ctx.schedule_sub_orchestration("BlockA", ""),
        ctx.schedule_sub_orchestration("BlockB", ""),
    ])
    return f"{a},{b}"
```

Use `all()` for joining (all must complete) and `race()` for racing (first wins, loser is cancelled). For 3+ way races, nest `race()` calls. See `test_async_blocks.py` for 12 examples covering join, race, nested chains, and timeout patterns.

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
