---
name: pyo3-interop
description: Rust↔Python interop architecture in duroxide-python. Use when modifying the PyO3 bridge, adding ScheduledTask types, fixing GIL deadlocks, changing tracing delegation, or debugging block_in_place / attach behavior.
---

# PyO3 Interop Architecture

## Overview

duroxide-python bridges Rust's duroxide runtime to Python via PyO3/maturin. The interop has two distinct paths — orchestrations (generator-based, synchronous blocking) and activities (synchronous GIL calls). Getting this wrong causes GIL deadlocks, silent replay corruption, or dropped futures.

## File Map

| File | Role |
|------|------|
| `src/handlers.rs` | Core interop — orchestration handler loop, activity invocation, global context maps, select/race/join, activity cancellation |
| `src/types.rs` | `ScheduledTask` enum — the protocol between Python and Rust |
| `src/lib.rs` | PyO3 module entry point, `#[pyfunction]` trace functions |
| `src/runtime.rs` | `PyRuntime` — wraps `duroxide::Runtime`, global tokio runtime |
| `src/client.rs` | `PyClient` — wraps `duroxide::Client`, all methods with `py.detach()` |
| `src/provider.rs` | `PySqliteProvider` |
| `src/pg_provider.rs` | `PyPostgresProvider` |
| `python/duroxide/__init__.py` | Python wrapper: SqliteProvider, PostgresProvider, Client, Runtime, decorators |
| `python/duroxide/context.py` | OrchestrationContext, ActivityContext |
| `python/duroxide/driver.py` | Generator driver: create_generator, next_step, dispose_generator |

## ⚠️ Critical: GIL Deadlock Problem

This is the most important difference between duroxide-python and duroxide-node. **Get this wrong and the process deadlocks.**

### The Problem

PyO3 holds the GIL when Python calls into Rust `#[pymethods]`. If that method calls `TOKIO_RT.block_on()`, it blocks the thread while holding the GIL. Meanwhile, orchestration handlers running on tokio threads need the GIL via `Python::attach()` — **deadlock**.

```
Thread A (Python → Rust):
  client.wait_for_orchestration()
    → PyO3 holds GIL
    → TOKIO_RT.block_on(async { ... })   ← BLOCKS, holding GIL

Thread B (Tokio → Python):
  orchestration handler invoked
    → block_in_place + Python::attach()  ← BLOCKS, waiting for GIL
```

### The Fix

EVERY method that calls `block_on` must use `py.detach()` to release the GIL before blocking:

```rust
fn wait_for_orchestration(&self, py: Python<'_>, id: String, timeout: u64) -> PyResult<...> {
    py.detach(|| {
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

`PyErr` is not `Send`, so you can't return `PyResult` from inside `detach`. Pattern:
1. Inside `detach`: map errors to `String` via `.map_err(|e| format!("{e}"))`
2. Outside `detach`: map `String` to `PyErr` via `.map_err(PyRuntimeError::new_err)`

### Rules for ANY New Method

1. **Add `py: Python<'_>` parameter** to the method signature
2. **Wrap `TOKIO_RT.block_on()` in `py.detach(|| { ... })`**
3. **Map errors to `String` inside, to `PyErr` outside**
4. **Never hold the GIL while blocking on tokio**

## Orchestration Interop (Blocking Generator Loop)

The replay engine calls `poll_once()` on the handler future. If the future isn't ready in one poll, it's **dropped**.

**Solution: `block_in_place` + `attach`**

```rust
fn call_create_blocking(&self, payload: String) -> Result<GeneratorStepResult, String> {
    tokio::task::block_in_place(|| {
        Python::attach(|py| {
            let result = self.create_fn.call1(py, (payload,))?;
            // parse result...
        })
    })
}
```

### Orchestration Handler Sequence

```
Rust (tokio thread)                         Python (GIL)
───────────────────                         ────────────────────
1. invoke(ctx, input)
   ├─ Store ctx in ORCHESTRATION_CTXS[instance_id]
   ├─ call_create_blocking(payload) ──────► create_generator(payload)
    │   (block_in_place + attach)              ├─ Create OrchestrationContext
   │                                          ├─ Create generator: fn(ctx, input)
   │                                          ├─ gen.send(None) → first yield
   │                                          └─ Return {"status": "yielded", "task": ...}
   │◄────────────────────────────────────────┘
   ├─ Loop:
   │   ├─ execute_task(ctx, task)           // Real DurableFuture or replay
   │   ├─ call_next_blocking(result) ──────► next_step(result)
    │   │   (block_in_place + attach)          ├─ gen.send(value) or gen.throw(exc)
   │   │                                      └─ Return next task or completion
   │   │◄────────────────────────────────────┘
   │   └─ If completed/error: break
   └─ Remove ctx from ORCHESTRATION_CTXS
```

## Activity Interop (Synchronous GIL Call)

Activities in duroxide-python are **synchronous** functions (unlike duroxide-node's async activities). They run on tokio threads via `block_in_place` + `attach`:

```
Rust                                        Python
────                                        ──────
invoke(ctx, input)
  ├─ Generate unique token (act-0, act-1, ...)
  ├─ Store ctx in ACTIVITY_CTXS[token]
    ├─ block_in_place + attach ───────────────► wrapped_fn(payload)
  │                                            ├─ Parse ctx, create ActivityContext
  │                                            ├─ Call user's function (synchronous)
  │                                            └─ Return JSON result
  │◄───────────────────────────────────────────┘
  └─ Remove token from ACTIVITY_CTXS
```

Users can use `asyncio.run()` inside activities if they need async I/O.

## Cross-Thread Tracing

Python callbacks acquire the GIL on tokio threads. Rust contexts live on tokio threads. Global `HashMap`s bridge the two:

```rust
static ACTIVITY_CTXS: LazyLock<Mutex<HashMap<String, ActivityContext>>>
static ORCHESTRATION_CTXS: LazyLock<Mutex<HashMap<String, OrchestrationContext>>>
```

**Python calls PyO3 functions that look up the Rust context:**

```python
# In OrchestrationContext (fire-and-forget, no yield)
def trace_info(self, message):
    orchestration_trace_log(self.instance_id, "info", str(message))

# In ActivityContext (fire-and-forget)
def trace_info(self, message):
    activity_trace_log(self._trace_token, "info", str(message))
```

### Rules for Tracing

1. **Never expose `is_replaying` to Python** — Rust `OrchestrationContext.trace()` handles suppression
2. **Always use global maps, not thread-locals** — Python runs on a different thread
3. **Clean up map entries on ALL exit paths** — leaked entries cause stale traces
4. **Use atomic tokens for activities** (not instance_id) — multiple activities for the same instance can run concurrently

## ScheduledTask Protocol

Python yields plain dicts. Rust deserializes them via `serde_json` into `ScheduledTask` enum variants:

```rust
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ScheduledTask {
    Activity { name: String, input: String },
    ActivityWithRetry { name: String, input: String, retry: RetryPolicyConfig },
    Timer { delay_ms: u64 },
    WaitEvent { name: String },
    SubOrchestration { name: String, input: String },
    SubOrchestrationWithId { name: String, instance_id: String, input: String },
    SubOrchestrationVersioned { name: String, version: String, input: String },
    Orchestration { name: String, instance_id: String, input: String },
    OrchestrationVersioned { name: String, version: String, instance_id: String, input: String },
    NewGuid,
    UtcNow,
    ContinueAsNew { input: String },
    ContinueAsNewVersioned { input: String, version: String },
    Join { tasks: Vec<ScheduledTask> },
    Select { tasks: Vec<ScheduledTask> },
}
```

### Adding a New ScheduledTask Type

1. Add variant to `ScheduledTask` in `src/types.rs` with correct `serde` attributes
2. Add execution branch in `execute_task()` in `src/handlers.rs`
3. If it should work in `select/race`, add branch in `make_select_future()`
4. If it should work in `join/all`, add branch in `make_join_future()`
5. Add Python method to `OrchestrationContext` in `python/duroxide/context.py`
6. Add test in `tests/test_e2e.py`
7. Rebuild: `maturin develop`

## Global Tokio Runtime

```rust
static TOKIO_RT: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime")
});
```

All async operations go through `TOKIO_RT.block_on()` (with GIL released via `py.detach()`). No pyo3-async-runtimes needed.

## Provider Polymorphism

PyO3 doesn't support trait objects in constructors. Python wrapper detects provider type:

```python
class Runtime:
    def __init__(self, provider, options=None):
        if getattr(provider, "_type", None) == "postgres":
            self._native = PyRuntime.from_postgres(provider._native, options)
        else:
            self._native = PyRuntime.from_sqlite(provider._native, options)
```

## PyO3 Object Mutability

`#[pyclass(get_all)]` makes fields readable but NOT writable from Python. Even with `set_all`, setting a Python `dict` to an `Option<String>` field fails with a type mismatch.

**Solution:** Create pure Python wrapper objects (e.g., `OrchestrationResult`) instead of mutating PyO3 objects:

```python
class OrchestrationResult:
    def __init__(self, status, output=None, error=None):
        self.status = status
        self.output = output  # parsed from JSON
        self.error = error

def _parse_status(raw):
    output = raw.output
    if output is not None:
        output = json.loads(output)
    return OrchestrationResult(status=raw.status, output=output, error=raw.error)
```

## Common Pitfalls

| Pitfall | What Happens | Fix |
|---------|-------------|-----|
| Missing `py.detach()` around `block_on` | GIL deadlock — process hangs forever | Wrap ALL `TOKIO_RT.block_on()` calls |
| Returning `PyErr` from inside `detach` | Compile error — `PyErr` is not `Send` | Map to `String` inside, `PyErr` outside |
| Thread-local for cross-thread context | Lookup returns `None` — traces silently fail | Use global `HashMap` |
| Mutating PyO3 `#[pyclass]` fields from Python | `TypeError` or silently ignored | Use Python wrapper objects |
| `cargo build` instead of `maturin develop` | Python imports stale `.so` — changes don't take effect | Always use `maturin develop` |
| Missing `serde(rename_all)` on new ScheduledTask variants | Deserialization fails — task type not recognized | Match Python naming convention (camelCase in JSON) |

## Build Requirements

```bash
# MUST use maturin (not cargo build) for the Python extension
source .venv/bin/activate
maturin develop           # Debug build + install
maturin develop --release # Release build + install
maturin build --release   # Build wheel for distribution

# cargo build alone produces a .dylib/.so that Python can't find
```
