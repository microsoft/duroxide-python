"""
OrchestrationContext and ActivityContext for duroxide Python SDK.

OrchestrationContext methods return ScheduledTask descriptors (dicts) that the
user yields from their generator function. The Rust runtime receives these
descriptors and executes the corresponding DurableFutures.

ActivityContext provides tracing and cancellation for activity functions.
"""

import json

from duroxide._duroxide import (
    orchestration_trace_log,
    activity_trace_log,
    activity_is_cancelled,
)


class OrchestrationContext:
    """Context object passed to orchestration generator functions.

    Methods that schedule work return ScheduledTask descriptors to be yielded.
    Logging methods are fire-and-forget (no yield needed).
    """

    def __init__(self, ctx_info: dict):
        self.instance_id: str = ctx_info["instanceId"]
        self.execution_id: int = ctx_info["executionId"]
        self.orchestration_name: str = ctx_info["orchestrationName"]
        self.orchestration_version: str = ctx_info["orchestrationVersion"]

    # ─── Scheduling (yield these) ──────────────────────────

    def schedule_activity(self, name: str, input=None) -> dict:
        """Schedule an activity. Yield the return value."""
        return {
            "type": "activity",
            "name": name,
            "input": json.dumps(input),
        }

    def schedule_activity_with_retry(self, name: str, input, retry: dict) -> dict:
        """Schedule an activity with retry policy. Yield the return value."""
        return {
            "type": "activityWithRetry",
            "name": name,
            "input": json.dumps(input),
            "retry": {
                "maxAttempts": retry.get("max_attempts", retry.get("maxAttempts", 3)),
                "timeoutMs": retry.get("timeout_ms", retry.get("timeoutMs")),
                "totalTimeoutMs": retry.get(
                    "total_timeout_ms", retry.get("totalTimeoutMs")
                ),
                "backoff": retry.get("backoff"),
            },
        }

    def schedule_timer(self, delay_ms: int) -> dict:
        """Schedule a timer (delay in milliseconds). Yield the return value."""
        return {"type": "timer", "delayMs": delay_ms}

    def wait_for_event(self, name: str) -> dict:
        """Wait for an external event. Yield the return value."""
        return {"type": "waitEvent", "name": name}

    def schedule_sub_orchestration(self, name: str, input=None) -> dict:
        """Schedule a sub-orchestration. Yield the return value."""
        return {
            "type": "subOrchestration",
            "name": name,
            "input": json.dumps(input),
        }

    def schedule_sub_orchestration_with_id(
        self, name: str, instance_id: str, input=None
    ) -> dict:
        """Schedule a sub-orchestration with a specific instance ID."""
        return {
            "type": "subOrchestrationWithId",
            "name": name,
            "instanceId": instance_id,
            "input": json.dumps(input),
        }

    def schedule_sub_orchestration_versioned(
        self, name: str, version: str = None, input=None
    ) -> dict:
        """Schedule a versioned sub-orchestration. Yield the return value."""
        return {
            "type": "subOrchestrationVersioned",
            "name": name,
            "version": version,
            "input": json.dumps(input),
        }

    def schedule_sub_orchestration_versioned_with_id(
        self, name: str, version: str, instance_id: str, input=None
    ) -> dict:
        """Schedule a versioned sub-orchestration with a specific instance ID."""
        return {
            "type": "subOrchestrationVersionedWithId",
            "name": name,
            "version": version,
            "instanceId": instance_id,
            "input": json.dumps(input),
        }

    def start_orchestration(self, name: str, instance_id: str, input=None) -> dict:
        """Start a detached orchestration (fire-and-forget). Yield the return value."""
        return {
            "type": "orchestration",
            "name": name,
            "instanceId": instance_id,
            "input": json.dumps(input),
        }

    def start_orchestration_versioned(
        self, name: str, version: str, instance_id: str, input=None
    ) -> dict:
        """Start a versioned detached orchestration (fire-and-forget)."""
        return {
            "type": "orchestrationVersioned",
            "name": name,
            "version": version,
            "instanceId": instance_id,
            "input": json.dumps(input),
        }

    def new_guid(self) -> dict:
        """Get a deterministic GUID. Yield the return value."""
        return {"type": "newGuid"}

    def utc_now(self) -> dict:
        """Get the current deterministic UTC time (ms). Yield the return value."""
        return {"type": "utcNow"}

    def continue_as_new(self, input=None) -> dict:
        """Continue the orchestration as a new instance with new input."""
        return {
            "type": "continueAsNew",
            "input": json.dumps(input),
        }

    def continue_as_new_versioned(self, input, version: str = None) -> dict:
        """Continue as new with a specific version."""
        return {
            "type": "continueAsNewVersioned",
            "input": json.dumps(input),
            "version": version,
        }

    # ─── Composition helpers ───────────────────────────────

    def all(self, tasks: list) -> dict:
        """Join multiple tasks (wait for all). Yield the return value."""
        return {"type": "join", "tasks": tasks}

    def race(self, *tasks) -> dict:
        """Select/race multiple tasks (wait for first). Yield the return value."""
        return {"type": "select", "tasks": list(tasks)}

    # ─── Logging (fire-and-forget, delegates to Rust ctx.trace()) ───

    def trace_info(self, message: str):
        orchestration_trace_log(self.instance_id, "info", str(message))

    def trace_warn(self, message: str):
        orchestration_trace_log(self.instance_id, "warn", str(message))

    def trace_error(self, message: str):
        orchestration_trace_log(self.instance_id, "error", str(message))

    def trace_debug(self, message: str):
        orchestration_trace_log(self.instance_id, "debug", str(message))


class ActivityContext:
    """Context for activity execution."""

    def __init__(self, ctx_info: dict):
        self.instance_id: str = ctx_info["instanceId"]
        self.execution_id: int = ctx_info["executionId"]
        self.orchestration_name: str = ctx_info["orchestrationName"]
        self.orchestration_version: str = ctx_info["orchestrationVersion"]
        self.activity_name: str = ctx_info["activityName"]
        self.worker_id: str = ctx_info["workerId"]
        self._trace_token: str = ctx_info["_traceToken"]

    def trace_info(self, message: str):
        activity_trace_log(self._trace_token, "info", str(message))

    def trace_warn(self, message: str):
        activity_trace_log(self._trace_token, "warn", str(message))

    def trace_error(self, message: str):
        activity_trace_log(self._trace_token, "error", str(message))

    def trace_debug(self, message: str):
        activity_trace_log(self._trace_token, "debug", str(message))

    def is_cancelled(self) -> bool:
        """Check if this activity has been cancelled (e.g., lost a race/select)."""
        return activity_is_cancelled(self._trace_token)

    def get_client(self):
        """Get a Client from this activity's context (for starting orchestrations, etc.)."""
        from duroxide._duroxide import activity_get_client
        from duroxide import Client
        native = activity_get_client(self._trace_token)
        if native is None:
            raise RuntimeError("Activity context not found")
        client = Client.__new__(Client)
        client._native = native
        return client
