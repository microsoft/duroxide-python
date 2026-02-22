# duroxide-python

Python SDK for the [Duroxide](https://github.com/microsoft/duroxide) durable execution runtime.

Write durable workflows as Python generators. The Rust runtime handles replay, persistence, and fault tolerance.

## Features

- **Generator-based orchestrations** — `yield` task descriptors, Rust handles DurableFutures
- **Activities** — regular Python functions for side effects (I/O, network calls)
- **Timers** — durable delays that survive process restarts
- **Events** — wait for external signals
- **Sub-orchestrations** — compose workflows hierarchically
- **Fan-out/Fan-in** — `ctx.all()` for parallel execution, `ctx.race()` for first-to-complete
- **Continue-as-new** — long-running orchestrations with bounded history
- **Deterministic replay** — safe resume after crashes
- **SQLite & PostgreSQL** — pluggable storage providers
- **Custom Status** — `ctx.set_custom_status()` / `ctx.reset_custom_status()` for orchestration progress reporting, `client.wait_for_status_change()` for efficient polling
- **Event Queues** — `ctx.dequeue_event(queue_name)` for FIFO mailbox-style message passing, `client.enqueue_event()` to send messages
- **Retry on Session** — `ctx.schedule_activity_with_retry_on_session()` for retry with session affinity
- **Admin APIs** — instance management, metrics, pruning
- **Activity client access** — `ctx.get_client()` lets activities start new orchestrations
- **Runtime metrics** — `metrics_snapshot()` for orchestration/activity counters

## Installation

```bash
pip install duroxide
```

## Quick Start

```python
from duroxide import SqliteProvider, Client, Runtime

# Create provider and runtime
provider = SqliteProvider.in_memory()
runtime = Runtime(provider)

# Register an activity
@runtime.register_activity("greet")
def greet(ctx, input):
    return f"Hello, {input['name']}!"

# Register an orchestration (generator function)
@runtime.register_orchestration("GreetWorkflow")
def greet_workflow(ctx, input):
    result = yield ctx.schedule_activity("greet", input)
    return result

# Start runtime and run orchestration
import threading
runtime.start()

client = Client(provider)
client.start_orchestration("greet-1", "GreetWorkflow", {"name": "World"})
status = client.wait_for_orchestration("greet-1", 10000)
print(status.output)  # "Hello, World!"

runtime.shutdown()
```

## Orchestrations

Orchestrations are Python generator functions. They must be **deterministic** — no I/O,
no randomness, no `time.time()`. Use only `ctx.*` methods for side effects.

```python
@runtime.register_orchestration("MyWorkflow")
def my_workflow(ctx, input):
    # Schedule activities
    result = yield ctx.schedule_activity("DoWork", input)

    # Fan-out / Fan-in
    results = yield ctx.all([
        ctx.schedule_activity("TaskA", {"id": 1}),
        ctx.schedule_activity("TaskB", {"id": 2}),
    ])

    # Timer
    yield ctx.schedule_timer(5000)  # 5 seconds

    # Wait for external event
    approval = yield ctx.wait_for_event("approval")

    # Sub-orchestration
    sub_result = yield ctx.schedule_sub_orchestration("SubWorkflow", input)

    # Race (first to complete wins)
    winner = yield ctx.race(
        ctx.schedule_activity("Fast", None),
        ctx.schedule_timer(10000),
    )

    # Custom status (fire-and-forget, no yield)
    ctx.set_custom_status("processing complete")

    # Dequeue from event queue (FIFO, blocks until message available)
    msg = yield ctx.dequeue_event("inbox")

    return {"result": result, "winner": winner}
```

## Activities

Activities are regular Python functions that perform side effects. They run outside
the replay engine and are safe for I/O operations.

```python
@runtime.register_activity("SendEmail")
def send_email(ctx, input):
    ctx.trace_info(f"Sending email to {input['to']}")
    # ... actual email sending ...
    return {"sent": True}
```

## PostgreSQL Provider

```python
from duroxide import PostgresProvider, Client, Runtime

provider = PostgresProvider.connect("postgresql://user:pass@localhost:5432/mydb")
# or with custom schema:
provider = PostgresProvider.connect_with_schema("postgresql://...", "duroxide_python")

runtime = Runtime(provider)
client = Client(provider)
```

## Admin APIs

```python
client = Client(provider)

# Metrics
metrics = client.get_system_metrics()
depths = client.get_queue_depths()

# Instance management
instances = client.list_all_instances()
info = client.get_instance_info("instance-1")
tree = client.get_instance_tree("instance-1")

# Execution history with full event data
executions = client.list_executions("instance-1")
events = client.read_execution_history("instance-1", executions[0])
for event in events:
    print(event.kind, event.data)
    # event.kind: "OrchestrationStarted" | "ActivityCompleted" | ...
    # event.data: JSON string with event-specific content (result, input, error, etc.)

# Cleanup
client.delete_instance("instance-1", force=True)
client.prune_executions("instance-1", PyPruneOptions(keep_last=5))
```

## Custom Status

Report orchestration progress visible to external clients:

```python
@runtime.register_orchestration("ProgressWorkflow")
def progress_workflow(ctx, input):
    ctx.set_custom_status("step 1: validating")
    yield ctx.schedule_activity("Validate", input)

    ctx.set_custom_status("step 2: processing")
    result = yield ctx.schedule_activity("Process", input)

    ctx.reset_custom_status()  # clear status
    return result

# Poll for status changes from outside
status = client.wait_for_status_change("instance-1", 0, 50, 10000)
if status:
    print(status.custom_status)          # "step 1: validating"
    print(status.custom_status_version)  # monotonically increasing counter
```

## Event Queues

Persistent FIFO message passing between clients and orchestrations:

```python
@runtime.register_orchestration("ChatBot")
def chat_bot(ctx, input):
    msg_json = yield ctx.dequeue_event("inbox")
    msg = json.loads(msg_json)
    response = yield ctx.schedule_activity("Generate", msg["text"])
    ctx.set_custom_status(json.dumps({"state": "replied", "response": response, "seq": msg["seq"]}))
    if "bye" in msg["text"].lower():
        return f"Done after {msg['seq']} msgs"
    return (yield ctx.continue_as_new(""))

# Send messages from outside
client.enqueue_event(instance_id, "inbox", json.dumps({"seq": 1, "text": "Hello!"}))
status = client.wait_for_status_change(instance_id, 0, 50, 10000)
reply = json.loads(status.custom_status)
```

## Development

```bash
# Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install build tools and test dependencies
pip install maturin pytest

# Build the native extension and install in development mode
maturin develop

# Run all 54 tests
pytest

# Run tests with verbose output
pytest -v

# Run a single test file
pytest tests/test_e2e.py -v

# Run a single test
pytest tests/test_e2e.py::test_hello_world

# Stop on first failure
pytest -v -x

# Build release wheel
maturin build --release
```

After Rust source changes (`src/*.rs`), re-run `maturin develop` to rebuild.
Python-only changes (`python/duroxide/`, `tests/`) take effect immediately.

## Changelog

See [CHANGELOG.md](https://github.com/microsoft/duroxide-python/blob/main/CHANGELOG.md) for release notes.

## Documentation

- [User Guide](https://github.com/microsoft/duroxide-python/blob/main/docs/user-guide.md) — orchestration patterns, activities, providers, tracing, determinism rules
- [Architecture](https://github.com/microsoft/duroxide-python/blob/main/docs/architecture.md) — PyO3 interop, GIL deadlock fix, generator driver, tracing internals

## License

MIT
