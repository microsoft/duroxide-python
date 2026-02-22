"""
duroxide - Python SDK for the Duroxide durable execution runtime.

Generator-based orchestrations: users write generator functions that yield
ScheduledTask descriptors. The Rust runtime handles DurableFutures.
"""

from duroxide._duroxide import (
    PySqliteProvider,
    PyPostgresProvider,
    PyClient,
    PyRuntime,
    PyRuntimeOptions,
    PyOrchestrationStatus,
    PySystemMetrics,
    PyQueueDepths,
    PyInstanceInfo,
    PyExecutionInfo,
    PyInstanceTree,
    PyDeleteInstanceResult,
    PyPruneOptions,
    PyPruneResult,
    PyInstanceFilter,
    PyEvent,
    activity_trace_log,
    orchestration_trace_log,
    orchestration_set_custom_status,
    orchestration_reset_custom_status,
    orchestration_get_custom_status,
    activity_is_cancelled,
    init_tracing,
)
from duroxide.context import OrchestrationContext, ActivityContext
from duroxide.driver import create_generator, next_step, dispose_generator

import json

# ─── Generator Driver Registry ───────────────────────────────────

_orchestration_functions: dict = {}


# ─── Result Wrapper ──────────────────────────────────────────────


class OrchestrationResult:
    """Wrapper for orchestration status with parsed output."""

    def __init__(self, status, output=None, error=None, custom_status=None, custom_status_version=0):
        self.status = status
        self.output = output
        self.error = error
        self.custom_status = custom_status
        self.custom_status_version = custom_status_version


def _parse_status(raw):
    """Convert a PyOrchestrationStatus to an OrchestrationResult with parsed output."""
    output = raw.output
    if output is not None:
        try:
            output = json.loads(output)
        except (json.JSONDecodeError, TypeError):
            pass
    return OrchestrationResult(
        status=raw.status,
        output=output,
        error=raw.error,
        custom_status=raw.custom_status,
        custom_status_version=raw.custom_status_version,
    )


# ─── Public API ───────────────────────────────────────────────────


class SqliteProvider:
    """SQLite provider for duroxide."""

    def __init__(self, native):
        self._native = native

    @staticmethod
    def open(path: str) -> "SqliteProvider":
        """Open a SQLite database file."""
        return SqliteProvider(PySqliteProvider.open(path))

    @staticmethod
    def in_memory() -> "SqliteProvider":
        """Create an in-memory SQLite database."""
        return SqliteProvider(PySqliteProvider.in_memory())


class PostgresProvider:
    """PostgreSQL provider for duroxide."""

    def __init__(self, native):
        self._native = native
        self._type = "postgres"

    @staticmethod
    def connect(database_url: str) -> "PostgresProvider":
        """Connect to a PostgreSQL database (uses 'public' schema)."""
        return PostgresProvider(PyPostgresProvider.connect(database_url))

    @staticmethod
    def connect_with_schema(database_url: str, schema: str) -> "PostgresProvider":
        """Connect to a PostgreSQL database with a custom schema."""
        return PostgresProvider(
            PyPostgresProvider.connect_with_schema(database_url, schema)
        )


class Client:
    """Client for starting and managing orchestration instances."""

    def __init__(self, provider):
        if getattr(provider, "_type", None) == "postgres":
            self._native = PyClient.from_postgres(provider._native)
        else:
            self._native = PyClient.from_sqlite(provider._native)

    def start_orchestration(self, instance_id: str, name: str, input=None):
        self._native.start_orchestration(
            instance_id, name, json.dumps(input)
        )

    def start_orchestration_versioned(
        self, instance_id: str, name: str, input, version: str
    ):
        self._native.start_orchestration_versioned(
            instance_id, name, json.dumps(input), version
        )

    def get_status(self, instance_id: str):
        result = self._native.get_status(instance_id)
        return _parse_status(result)

    def wait_for_orchestration(self, instance_id: str, timeout_ms: int = 30000):
        result = self._native.wait_for_orchestration(instance_id, timeout_ms)
        return _parse_status(result)

    def wait_for_status_change(
        self,
        instance_id: str,
        last_seen_version: int = 0,
        poll_interval_ms: int = 200,
        timeout_ms: int = 30000,
    ):
        """Wait for custom status changes on an orchestration instance.

        Polls until the custom_status_version changes from last_seen_version,
        or the orchestration reaches a terminal state.

        Returns an OrchestrationResult with custom_status and custom_status_version.
        """
        result = self._native.wait_for_status_change(
            instance_id, last_seen_version, poll_interval_ms, timeout_ms
        )
        return _parse_status(result)

    def cancel_instance(self, instance_id: str, reason: str = None):
        self._native.cancel_instance(instance_id, reason)

    def raise_event(self, instance_id: str, event_name: str, data=None):
        self._native.raise_event(
            instance_id, event_name, json.dumps(data)
        )

    def enqueue_event(self, instance_id: str, queue_name: str, data=None):
        """Enqueue an event into a named queue for an orchestration instance.

        Uses FIFO mailbox semantics. Matched by ctx.dequeue_event() in the orchestration.
        """
        self._native.enqueue_event(
            instance_id, queue_name, json.dumps(data)
        )

    def get_system_metrics(self):
        return self._native.get_system_metrics()

    def get_queue_depths(self):
        return self._native.get_queue_depths()

    # ─── Management / Admin API ─────────────────────────────

    def list_all_instances(self):
        return self._native.list_all_instances()

    def list_instances_by_status(self, status: str):
        return self._native.list_instances_by_status(status)

    def get_instance_info(self, instance_id: str):
        return self._native.get_instance_info(instance_id)

    def get_execution_info(self, instance_id: str, execution_id: int):
        return self._native.get_execution_info(instance_id, execution_id)

    def list_executions(self, instance_id: str):
        return self._native.list_executions(instance_id)

    def read_execution_history(self, instance_id: str, execution_id: int):
        return self._native.read_execution_history(instance_id, execution_id)

    def get_instance_tree(self, instance_id: str):
        return self._native.get_instance_tree(instance_id)

    def delete_instance(self, instance_id: str, force: bool = False):
        return self._native.delete_instance(instance_id, force)

    def delete_instance_bulk(self, filter=None):
        if filter is None:
            filter = PyInstanceFilter()
        return self._native.delete_instance_bulk(filter)

    def prune_executions(self, instance_id: str, options=None):
        if options is None:
            options = PyPruneOptions()
        return self._native.prune_executions(instance_id, options)

    def prune_executions_bulk(self, filter=None, options=None):
        if filter is None:
            filter = PyInstanceFilter()
        if options is None:
            options = PyPruneOptions()
        return self._native.prune_executions_bulk(filter, options)


class Runtime:
    """Durable execution runtime."""

    def __init__(self, provider, options=None):
        if getattr(provider, "_type", None) == "postgres":
            self._native = PyRuntime.from_postgres(provider._native, options)
        else:
            self._native = PyRuntime.from_sqlite(provider._native, options)
        # Wire up the generator driver functions
        self._native.set_generator_driver(
            create_generator, next_step, dispose_generator
        )

    def register_activity(self, name: str, fn=None):
        """Register an activity function. Can be used as a decorator.

        Usage:
            @runtime.register_activity("my_activity")
            def my_activity(ctx, input):
                return {"result": "done"}

            # or:
            runtime.register_activity("my_activity", my_fn)
        """
        if fn is not None:
            self._register_activity_impl(name, fn)
            return fn

        # Decorator usage
        def decorator(func):
            self._register_activity_impl(name, func)
            return func

        return decorator

    def _register_activity_impl(self, name: str, fn):
        def wrapped_fn(payload: str) -> str:
            newline_idx = payload.index("\n")
            ctx_info_str = payload[:newline_idx]
            input_str = payload[newline_idx + 1 :]
            ctx_info = json.loads(ctx_info_str)
            ctx = ActivityContext(ctx_info)

            try:
                input_val = json.loads(input_str)
            except (json.JSONDecodeError, TypeError):
                input_val = input_str

            result = fn(ctx, input_val)
            return json.dumps(result if result is not None else None)

        self._native.register_activity(name, wrapped_fn)

    def register_orchestration(self, name: str, fn=None):
        """Register an orchestration generator function. Can be used as a decorator.

        Usage:
            @runtime.register_orchestration("my_orch")
            def my_orch(ctx, input):
                result = yield ctx.schedule_activity("work", input)
                return result

            # or:
            runtime.register_orchestration("my_orch", my_fn)
        """
        if fn is not None:
            _orchestration_functions[name] = fn
            self._native.register_orchestration(name)
            return fn

        def decorator(func):
            _orchestration_functions[name] = func
            self._native.register_orchestration(name)
            return func

        return decorator

    def register_orchestration_versioned(self, name: str, version: str, fn=None):
        """Register a versioned orchestration generator function. Can be used as a decorator."""
        if fn is not None:
            key = f"{name}@{version}"
            _orchestration_functions[key] = fn
            self._native.register_orchestration_versioned(name, version)
            return fn

        def decorator(func):
            key = f"{name}@{version}"
            _orchestration_functions[key] = func
            self._native.register_orchestration_versioned(name, version)
            return func

        return decorator

    def start(self):
        """Start the runtime. Blocks until shutdown is called."""
        self._native.start()

    def shutdown(self, timeout_ms: int = None):
        """Shutdown the runtime gracefully."""
        self._native.shutdown(timeout_ms)

    def metrics_snapshot(self):
        """Get a snapshot of runtime metrics."""
        return self._native.metrics_snapshot()


__all__ = [
    "SqliteProvider",
    "PostgresProvider",
    "Client",
    "Runtime",
    "OrchestrationResult",
    "PyRuntimeOptions",
    "OrchestrationContext",
    "ActivityContext",
    "PyOrchestrationStatus",
    "PySystemMetrics",
    "PyQueueDepths",
    "PyInstanceInfo",
    "PyExecutionInfo",
    "PyInstanceTree",
    "PyDeleteInstanceResult",
    "PyPruneOptions",
    "PyPruneResult",
    "PyInstanceFilter",
    "PyEvent",
    "init_tracing",
]
