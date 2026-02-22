# Architecture

duroxide-python is a Python wrapper around the Rust [duroxide](https://github.com/microsoft/duroxide) durable execution runtime, built with [PyO3](https://pyo3.rs) and [maturin](https://maturin.rs). This document explains how the interop works, key design decisions, and current limitations.

## Overview

```
┌─────────────────────────────────────────────────────────┐
│                    Python Process                        │
│                                                         │
│  ┌─────────────────────┐    ┌────────────────────────┐  │
│  │    User Code (Py)   │    │  Generator Driver (Py) │  │
│  │                     │    │                        │  │
│  │  def my_orch(ctx):  │    │  _generators dict      │  │
│  │    yield ctx.sched  │◄──►│  create_generator()    │  │
│  │    ...              │    │  next_step()           │  │
│  │                     │    │  dispose_generator()   │  │
│  └─────────────────────┘    └───────────┬────────────┘  │
│                                         │ PyO3          │
│  ┌──────────────────────────────────────┼────────────┐  │
│  │              Rust (PyO3)             │            │  │
│  │                                     ▼            │  │
│  │  ┌──────────────┐    ┌──────────────────────┐    │  │
│  │  │  PyRuntime   │    │ PyOrchestrationHandler│   │  │
│  │  │  PyClient    │    │  execute_task()       │   │  │
│  │  │  Providers   │    │  call_create_blocking()│  │  │
│  │  └──────┬───────┘    │  call_next_blocking() │   │  │
│  │         │            └──────────┬───────────┘    │  │
│  │         │                       │                │  │
│  │         ▼                       ▼                │  │
│  │  ┌──────────────────────────────────────────┐    │  │
│  │  │         duroxide (Rust crate)            │    │  │
│  │  │                                          │    │  │
│  │  │  Runtime, ReplayEngine, Client           │    │  │
│  │  │  OrchestrationContext, ActivityContext    │    │  │
│  │  │  DurableFuture, Provider trait           │    │  │
│  │  └──────────────────────────────────────────┘    │  │
│  └──────────────────────────────────────────────────┘  │
│                                                         │
│  ┌─────────────────────────────────────────────────┐    │
│  │              Storage (SQLite / PostgreSQL)       │    │
│  └─────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────┘
```

## Yield vs Async

This is the most important design decision in duroxide-python and the most common source of confusion.

### The Rule

- **Orchestrations** use generator functions and `yield`
- **Activities** use regular functions (synchronous `def`)

```python
# Orchestration: generator, yield for durable operations
@runtime.register_orchestration("MyWorkflow")
def my_workflow(ctx, input):
    a = yield ctx.schedule_activity("Step1", input)
    b = yield ctx.schedule_activity("Step2", a)
    return b

# Activity: regular function for I/O
@runtime.register_activity("Step1")
def step1(ctx, input):
    response = requests.get(f"https://api.example.com/{input}")
    return response.json()
```

### Why Not async/await for Orchestrations?

Durable orchestrations require **deterministic replay**. When a process restarts, the runtime replays the orchestration's history — re-executing the generator function from the beginning, feeding back previously recorded results. The runtime must control exactly when each step executes and what value it returns.

**Generators give Rust full control:**

```
Generator: yield descriptor ──► Rust: execute DurableFuture ──► Generator: receive result
                                      │
                                      ├─ First run: actually execute
                                      └─ Replay: return cached result
```

The generator yields a plain descriptor dict (e.g., `{"type": "activity", "name": "Greet", "input": "..."}`). Rust decides whether to execute it for real or return a cached result from history. The generator doesn't know the difference.

### Why Activities Are Synchronous

Unlike duroxide-node where activities are async JS functions called via ThreadsafeFunction, duroxide-python activities are regular synchronous functions called via `block_in_place` + `Python::with_gil()`. Activities run on tokio threads that acquire the GIL to call into Python.

Users can use `asyncio.run()` internally if they need async I/O, or use synchronous libraries like `requests`.

## The GIL Deadlock Problem

This is the most critical difference between duroxide-python and duroxide-node.

### The Problem

PyO3 holds the GIL when Python calls into Rust `#[pymethods]`. If that method calls `TOKIO_RT.block_on()`, it blocks the thread while holding the GIL. Meanwhile, orchestration handlers running on tokio threads need the GIL via `Python::with_gil()` — **deadlock**.

```
Thread A (Python → Rust):
  client.wait_for_orchestration()
    → PyO3 holds GIL
    → TOKIO_RT.block_on(async { ... })   ← BLOCKS, holding GIL

Thread B (Tokio → Python):
  orchestration handler invoked
    → block_in_place + Python::with_gil()  ← BLOCKS, waiting for GIL
```

### The Fix

EVERY method that calls `block_on` must use `py.allow_threads()` to release the GIL before blocking:

```rust
fn wait_for_orchestration(&self, py: Python<'_>, id: String, timeout: u64) -> PyResult<...> {
    py.allow_threads(|| {
        TOKIO_RT.block_on(async {
            self.client.wait_for_orchestration(&id, timeout).await
                .map_err(|e| format!("{e}"))
        })
    })
    .map_err(PyRuntimeError::new_err)
}
```

This pattern is applied to ALL 20+ methods in `client.rs` and `runtime.rs`.

### Error Handling Across the Boundary

`PyErr` is not `Send`, so you can't return `PyResult` from inside `allow_threads`. Pattern: map errors to `String` inside `allow_threads`, then `.map_err(PyRuntimeError::new_err)` outside.

## Orchestration Handler Loop

The core of the interop is in `src/handlers.rs`. Here's the sequence for a single orchestration execution:

```
                    Rust (tokio thread)                    Python (GIL)
                    ──────────────────                    ────────────────────
1. invoke(ctx, input)
   │
   ├─ Store ctx in ORCHESTRATION_CTXS map
   │
   ├─ call_create_blocking(payload) ──────────────────► create_generator(payload)
   │   (block_in_place + with_gil)                       │
   │                                                     ├─ Create OrchestrationContext
   │                                                     ├─ Create generator: fn(ctx, input)
   │                                                     ├─ gen.send(None) → first yield
   │                                                     └─ Return {"status": "yielded", "task": ...}
   │◄────────────────────────────────────────────────────┘
   │
   ├─ Loop:
   │   │
   │   ├─ execute_task(ctx, task)
   │   │   └─ ctx.schedule_activity / timer / wait / etc
   │   │      (DurableFuture — replayed or executed)
   │   │
   │   ├─ call_next_blocking(result) ─────────────────► next_step(result)
   │   │                                                 │
   │   │                                                 ├─ gen.send(value) or gen.throw(exc)
   │   │                                                 └─ Return next yielded task or completion
   │   │◄───────────────────────────────────────────────┘
   │   │
   │   └─ If completed/error: dispose generator, return
   │
   └─ Remove ctx from ORCHESTRATION_CTXS map
```

### The block_in_place Fix

The replay engine's `poll_once()` drops the handler future after a single poll. We use `tokio::task::block_in_place()` + `Python::with_gil()` to synchronously call Python generator functions from tokio threads. This works because:
- `block_in_place` tells tokio this thread is doing blocking work
- `with_gil()` acquires the GIL only when needed
- The GIL is released by `py.allow_threads()` in the client/runtime methods

### Activity Handler

Activities are simpler — they're regular functions, not generators:

```
Rust                                          Python
────                                          ──────
invoke(ctx, input)
  │
  ├─ Store ctx in ACTIVITY_CTXS map (token-keyed)
  ├─ Serialize ctx + input as payload
  │
  ├─ block_in_place + with_gil ─────────────────► wrapped_fn(payload)
  │                                                │
  │                                                ├─ Parse ctx, create ActivityContext
  │                                                ├─ Call user's function
  │                                                │   (can use ctx.trace_info() etc)
  │                                                └─ Return JSON result
  │◄───────────────────────────────────────────────┘
  │
  └─ Remove token from ACTIVITY_CTXS map
```

## Tracing Architecture

Both orchestration and activity tracing delegate to the Rust context objects, which use `tracing::info!` etc. with structured fields.

### Orchestration Tracing

```
Python: ctx.trace_info("message")
  │
  └─► orchestration_trace_log(instance_id, "info", "message")  [PyO3 function]
        │
        └─► ORCHESTRATION_CTXS.get(instance_id).trace("INFO", "message")
              │
              ├─ if is_replaying → suppressed (no output)
              └─ if live → tracing::info!(target: "duroxide::orchestration", ...)
```

### Activity Tracing

```
Python: ctx.trace_info("message")
  │
  └─► activity_trace_log(token, "info", "message")  [PyO3 function]
        │
        └─► ACTIVITY_CTXS.get(token).trace_info("message")
              │
              └─► tracing::info!(target: "duroxide::activity",
                      instance_id, activity_name, activity_id, worker_id, ...)
```

## Provider Polymorphism

PyO3 doesn't support trait objects in constructors, so we use factory methods:

```python
# SQLite
runtime = Runtime(sqlite_provider)

# PostgreSQL
runtime = Runtime(pg_provider)
```

The Python wrapper class detects the provider type via a `_type` field and calls the right factory:

```python
class Runtime:
    def __init__(self, provider, options=None):
        if getattr(provider, "_type", None) == "postgres":
            self._native = PyRuntime.from_postgres(provider._native, options)
        else:
            self._native = PyRuntime.from_sqlite(provider._native, options)
```

Internally, `PyRuntime` stores `Arc<dyn Provider>` so all provider operations are polymorphic.

## Global Tokio Runtime

A single `static TOKIO_RT: LazyLock<tokio::runtime::Runtime>` is used for all async operations. All `TOKIO_RT.block_on()` calls release the GIL first via `py.allow_threads()`. No pyo3-async-runtimes needed — this keeps the design simple.

## Crate Version Alignment

duroxide-python depends on both `duroxide` (local path) and `duroxide-pg` (which depends on duroxide from crates.io). To avoid "two versions of crate `duroxide`" errors:

```toml
[patch.crates-io]
duroxide = { path = "../duroxide" }
```

## Limitations

### No Parallel Orchestration Steps Without yield

Each orchestration step is sequential. To run tasks in parallel, use `ctx.all()` or `ctx.race()`:

```python
# ❌ This runs sequentially (each yield blocks)
a = yield ctx.schedule_activity("A", input)
b = yield ctx.schedule_activity("B", input)

# ✅ This runs in parallel
a, b = yield ctx.all([
    ctx.schedule_activity("A", input),
    ctx.schedule_activity("B", input),
])
```

### Generator Functions Must Not Be async

Orchestration functions must be regular generators (`def` with `yield`), not async generators (`async def` with `yield`). Async generators use `async for` and Promises internally, which conflicts with the replay engine's synchronous step-by-step execution model.

### Activities Are Synchronous

Activities are called via `block_in_place` + `with_gil()` on tokio threads. They are regular synchronous Python functions. For async I/O, use `asyncio.run()` inside the activity body.

### select/race Supports 2 Tasks

`ctx.race()` currently supports exactly 2 tasks (maps to `select2` in Rust). More tasks require nesting or a future `selectN` implementation.

### SQLite Lock Contention

File-based SQLite can hit "database is locked" errors under concurrent orchestration load. The runtime retries automatically, but high-throughput scenarios should use PostgreSQL.

### Runtime Shutdown

`Runtime.shutdown(timeout_ms)` waits for the full timeout duration unconditionally. Use a small timeout (e.g., 100ms) in tests.

### Platform-Specific Binary

PyO3/maturin compiles to a platform-specific `.so`/`.dylib`/`.pyd` binary. Cross-platform distribution requires building on each target platform or using maturin's CI toolchain with manylinux containers.

### Single GIL

All Python callbacks (generator steps, activity functions) run under the GIL. The Rust runtime is multi-threaded (tokio), but Python execution is single-threaded due to the GIL. Heavy computation in activities will block other Python callbacks. This is no different from standard Python threading.

## Custom Status Data Path

Custom status is a fire-and-forget side channel that orchestrations use to report progress. Unlike `yield`-based operations, `set_custom_status()` does not produce a history event — it writes directly to the instance metadata in the provider.

```
Python                              Rust (PyO3)                        Provider (DB)
──────                              ───────────                        ─────────────
ctx.set_custom_status("step 2")
  │
  └─► orchestration_set_custom_status(instance_id, "step 2")
        │
        └─► ORCHESTRATION_CTXS.get(instance_id)
              │
              └─► ctx.set_custom_status("step 2")
                    │
                    └─► provider.update_custom_status(id, status, version+1)

ctx.reset_custom_status()           Same path, sets status to None
```

**Client polling:**

```
Python                              Rust (PyO3)                        Provider (DB)
──────                              ───────────                        ─────────────
client.wait_for_status_change(id, last_version, poll_ms, timeout_ms)
  │
  └─► py.allow_threads(|| {
        TOKIO_RT.block_on(async {
          loop {
            status = provider.get_status(id)
            if status.custom_status_version > last_version:
              return Some(status)
            if elapsed > timeout_ms:
              return None
            sleep(poll_ms)
          }
        })
      })
```

The `custom_status_version` is a monotonically increasing counter incremented on every `set_custom_status()` call. Clients pass their last-seen version to avoid redundant reads.

## Event Queue Data Flow

Event queues provide persistent FIFO message passing between external clients and orchestrations. Unlike `wait_for_event()` which matches a single named event, event queues support multiple messages on named queues with guaranteed ordering. Messages survive `continue_as_new`.

### Enqueue (client → provider)

```
Python                              Rust (PyO3)                        Provider (DB)
──────                              ───────────                        ─────────────
client.enqueue_event(id, "inbox", data)
  │
  └─► py.allow_threads(|| {
        TOKIO_RT.block_on(async {
          client.enqueue_event(id, "inbox", data).await
        })
      })
            │
            └─► INSERT into event queue table (instance_id, queue_name, data, seq)
                Enqueue orchestrator work item (wake up the orchestration)
```

### Dequeue (orchestration ← provider)

```
Python (generator)                  Rust (handler loop)                Provider (DB)
──────────────────                  ───────────────────                ─────────────
yield ctx.dequeue_event("inbox")
  │
  └─► {"type": "dequeueEvent", "queue_name": "inbox"}
        │
        └─► execute_task(ctx, task)
              │
              └─► ctx.dequeue_event("inbox")  → DurableFuture
                    │
                    ├─ First run: subscribe to queue, block until message arrives
                    │    provider.dequeue_event(id, "inbox") → data
                    │
                    └─ Replay: return cached result from history
                         (QueueSubscribed + QueueEventDelivered events)
```

### Continue-as-New Semantics

When an orchestration calls `continue_as_new()`, pending queue messages are preserved. The new execution picks up where the old one left off — messages are not lost or redelivered. This enables the "eternal orchestration with mailbox" pattern used in chat bots and actor-style workflows.
