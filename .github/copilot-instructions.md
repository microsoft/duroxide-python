# duroxide-python — Copilot Instructions

> **🚨 STOP: DO NOT `git commit` OR `git push` WITHOUT EXPLICIT USER PERMISSION 🚨**
>
> Always ask before committing or pushing. Never assume.

## ⚠️ SDK Symmetry — duroxide-python & duroxide-node MUST stay in sync

When making changes to this SDK, **always** check `../duroxide-node` for symmetry:
1. **Bug fixes**: If you fix a bug here, apply the same fix in duroxide-node (and vice versa)
2. **API parity**: Every public API in one SDK must have an equivalent in the other (adjusted for language idioms — snake_case in Python, camelCase in JS)
3. **Test parity**: Every test in one SDK must have a corresponding test in the other
4. **Feature parity**: New duroxide features exposed in one SDK must be exposed in both
5. **Rust handler code** (`src/handlers.rs`): Both SDKs share the same interop architecture — `execute_task`, `make_select_future`, `make_join_future` should be kept structurally identical

## Project Overview

**duroxide-python** is a Python SDK for the duroxide durable execution runtime. It wraps the Rust duroxide library via PyO3/maturin, providing a generator-based API for writing deterministic, replayable workflows.

This is a **thin binding layer** — the replay engine, state management, providers, and observability live in the `duroxide` Rust crate (peer folder `../duroxide`). This project only bridges Python ↔ Rust.

## Architecture

```
Python (generators)  ←→  PyO3 bridge  ←→  duroxide (Rust core)
                          ↕
                    tokio runtime (global)
```

- **Orchestrations**: Python generators that `yield` scheduling commands (dicts) to Rust
- **Activities**: Regular Python functions called by Rust via `block_in_place` + `Python::with_gil()`
- **Providers**: PostgreSQL (`duroxide-pg-opt`) or SQLite — configured at startup
- **Tracing**: Delegates to Rust `tracing` — controlled by `RUST_LOG` env var

## Key Files

### Rust Source (`src/`)

| File | Role |
|------|------|
| `lib.rs` | PyO3 module entry, `#[pyfunction]` exports |
| `types.rs` | `ScheduledTask` enum (Python→Rust protocol) |
| `handlers.rs` | Core interop: orchestration loop, activity handler, execute_task, select/join |
| `runtime.rs` | `PyRuntime` — global tokio runtime, start/shutdown |
| `client.rs` | `PyClient` — all client methods with `py.allow_threads()` |
| `provider.rs` | `PySqliteProvider` |
| `pg_provider.rs` | `PyPostgresProvider` |

### Python Source (`python/duroxide/`)

| File | Role |
|------|------|
| `__init__.py` | Public API: SqliteProvider, PostgresProvider, Client, Runtime, decorators |
| `context.py` | OrchestrationContext, ActivityContext (yield scheduling commands) |
| `driver.py` | Generator driver: create_generator, next_step, dispose_generator |

### Tests (`tests/`)

| File | Tests | Schema |
|------|-------|--------|
| `test_e2e.py` | 27 | `duroxide_python_e2e` |
| `test_races.py` | 7 | `duroxide_python_races` |
| `test_admin_api.py` | 14 | `duroxide_python_admin` |
| `scenarios/test_toygres.py` | 6 | `duroxide_python_toygres` |

## Build & Test

```bash
cd duroxide-python
source .venv/bin/activate

# Build (ALWAYS use maturin, never bare cargo build)
maturin develop                    # debug build + install into venv
maturin develop --release          # release build

# Lint
cargo clippy --all-targets         # must pass with zero warnings

# Test (requires DATABASE_URL in .env for PG tests)
pytest -v                          # all 54 tests
pytest tests/test_e2e.py -v        # e2e only
pytest -s                          # show Rust tracing output

# Logging
RUST_LOG=info pytest -s            # see orchestration/activity traces
```

## ⚠️ GIL Deadlock — The #1 Pitfall

**Every Rust method that calls `TOKIO_RT.block_on()` MUST release the GIL first:**

```rust
fn my_method(&self, py: Python<'_>, ...) -> PyResult<...> {
    py.allow_threads(|| {
        TOKIO_RT.block_on(async { ... })
            .map_err(|e| format!("{e}"))
    })
    .map_err(PyRuntimeError::new_err)
}
```

Without `py.allow_threads()`, Python holds the GIL while blocking on tokio. Orchestration handlers on tokio threads need the GIL → **deadlock**. See `pyo3-interop` skill for full details.

## Interop Model

### Orchestrations: Generator Protocol

Python generators yield scheduling commands as plain dicts. The Rust handler loop:

1. Calls `create_generator(payload)` → Python creates generator, calls `gen.send(None)` → first task
2. Loops: `execute_task()` in Rust → `next_step(result)` → Python calls `gen.send(value)` → next task
3. Generator returns (`StopIteration`) → orchestration complete

### Activities: Synchronous Call

Rust calls Python activity functions synchronously via `block_in_place` + `Python::with_gil()`. Activities are regular `def` functions (not generators, not async).

### Tracing: Global Context Maps

```rust
static ORCHESTRATION_CTXS: LazyLock<Mutex<HashMap<String, OrchestrationContext>>>
static ACTIVITY_CTXS: LazyLock<Mutex<HashMap<String, ActivityContext>>>
```

Python `ctx.trace_info()` → PyO3 function → looks up Rust context by key → delegates to `ctx.trace()`.

## ScheduledTask Protocol

Python `OrchestrationContext` methods return dicts with a `"type"` key. Rust deserializes to `ScheduledTask` enum:

```python
# Python side (context.py)
def schedule_activity(self, name, input):
    return {"type": "activity", "name": name, "input": json.dumps(input)}
```

```rust
// Rust side (types.rs)
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ScheduledTask {
    Activity { name: String, input: String },
    Timer { delay_ms: u64 },
    // ... etc
}
```

**To add a new task type:** Add to both `context.py` AND `types.rs`, add execution in `handlers.rs`.

## Key Patterns

### Error Handling Across py.allow_threads

`PyErr` is not `Send`. Map errors inside, convert outside:

```rust
py.allow_threads(|| {
    TOKIO_RT.block_on(async { ... }).map_err(|e| format!("{e}"))
})
.map_err(PyRuntimeError::new_err)
```

### Provider Polymorphism

PyO3 doesn't support trait objects directly. Detect provider type in Python:

```python
if getattr(provider, "_type", None) == "postgres":
    self._native = PyRuntime.from_postgres(provider._native, options)
else:
    self._native = PyRuntime.from_sqlite(provider._native, options)
```

### Activity Client Access

`ctx.get_client()` calls `activity_get_client(token)` PyO3 function → looks up `ActivityContext` in `ACTIVITY_CTXS` map → calls `ctx.get_client()` → wraps result as `PyClient`. Activities can use this to start orchestrations, raise events, etc.

### Metrics Snapshot

`runtime.metrics_snapshot()` returns a dict with 17 counters (orch starts/completions/failures, activity results, dispatcher stats, provider errors). Returns `None` if observability is not enabled.

### Observability Options

`PyRuntimeOptions` includes `log_format`, `log_level`, `service_name`, `service_version` which map to `duroxide::ObservabilityConfig`.

### Non-Generator Orchestrations

`driver.py` handles functions that return without yielding (checked via `isinstance(gen, types.GeneratorType)`). These are treated as immediate completion.

### PyO3 Object Mutability

`#[pyclass(get_all)]` fields are read-only from Python. Use Python wrapper objects (e.g., `OrchestrationResult`) instead of mutating PyO3 objects.

## Determinism Rules

Orchestration code must be deterministic — same input + history = same sequence of yields:

| ✅ Safe | ❌ Breaks Replay |
|---------|-----------------|
| `yield ctx.utc_now()` | `time.time()` |
| `yield ctx.new_guid()` | `uuid.uuid4()` |
| `ctx.trace_info()` | `print()` |
| `yield ctx.schedule_timer(ms)` | `time.sleep()` |
| Pure computation | I/O, HTTP, DB, randomness |

## Testing Conventions

- PostgreSQL tests use schema isolation (one schema per test file)
- `python-dotenv` loads `DATABASE_URL` from `.env`
- `PyRuntimeOptions(dispatcher_poll_interval_ms=50)` for fast test dispatch
- `runtime.shutdown(100)` — short timeout, it waits the full duration
- Use `SqliteProvider.in_memory()` only for SQLite smoketest
- `worker_lock_timeout_ms=2000` for tests needing fast cancellation detection

## Dependencies

- **Rust**: duroxide (core), duroxide-pg-opt (PG provider), sqlx, tokio, pyo3, tracing
- **Python**: maturin (build), pytest (test), python-dotenv (test config)
- **Build**: `maturin develop` (not `cargo build`) — produces the `.so`/`.dylib` Python can import
- **Cargo patch**: `[patch.crates-io] duroxide = { path = "../duroxide" }` forces local duroxide
