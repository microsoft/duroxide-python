"""
Generator driver for duroxide Python SDK.

This module manages Python generator instances and drives them step-by-step.
It is called from Rust via PyO3 to:
1. Create a generator from a registered orchestration function
2. Drive the generator one step with a result value
3. Dispose of a generator when done

The protocol is identical to the Node.js driver — JSON in, JSON out — so the
Rust handler code (handlers.rs) is shared across both languages.
"""

import json
import traceback

from duroxide.context import OrchestrationContext

# Generator storage
_generators: dict = {}
_next_generator_id = 1

# Track last yielded task per generator for typed result processing
_last_tasks: dict = {}


def create_generator(payload_json: str) -> str:
    """Create a generator and drive to first yield.

    Called from Rust. Takes a JSON payload with ctxInfo and input.
    Returns a JSON GeneratorStepResult.
    """
    global _next_generator_id
    try:
        payload = json.loads(payload_json)
        ctx_info = payload["ctxInfo"]
        input_str = payload["input"]

        # Find the orchestration function by name@version, falling back to name
        from duroxide import _orchestration_functions

        orch_name = ctx_info["orchestrationName"]
        orch_version = ctx_info.get("orchestrationVersion", "")
        versioned_key = f"{orch_name}@{orch_version}" if orch_version else None
        fn = None
        if versioned_key:
            fn = _orchestration_functions.get(versioned_key)
        if fn is None:
            fn = _orchestration_functions.get(orch_name)
        if fn is None:
            return json.dumps(
                {
                    "status": "error",
                    "message": f"Orchestration '{orch_name}' not registered on Python side",
                }
            )

        # Create the orchestration context
        ctx = OrchestrationContext(ctx_info)

        # Parse input
        try:
            input_val = json.loads(input_str) if input_str else None
        except (json.JSONDecodeError, TypeError):
            input_val = input_str

        # Create the generator
        gen = fn(ctx, input_val)

        # If fn returned a non-generator (plain function), treat as completed
        import types

        if not isinstance(gen, types.GeneratorType):
            return json.dumps(
                {
                    "status": "completed",
                    "output": json.dumps(gen if gen is not None else None),
                }
            )

        # Assign an ID and store
        gen_id = _next_generator_id
        _next_generator_id += 1
        _generators[gen_id] = gen

        # Drive to first yield
        return _drive_step(gen_id, gen, None)
    except Exception as e:
        return json.dumps({"status": "error", "message": traceback.format_exc()})


def next_step(payload_json: str) -> str:
    """Drive a generator one step with a result value.

    Called from Rust. Takes a JSON payload with generatorId, result, isError.
    Returns a JSON GeneratorStepResult.
    """
    try:
        payload = json.loads(payload_json)
        gen_id = payload["generatorId"]
        result_str = payload["result"]
        is_error = payload["isError"]

        gen = _generators.get(gen_id)
        if gen is None:
            return json.dumps(
                {"status": "error", "message": f"Generator {gen_id} not found"}
            )

        # Parse the result from Rust
        try:
            value = json.loads(result_str)
        except (json.JSONDecodeError, TypeError):
            value = result_str

        if is_error:
            return _drive_step_with_error(gen_id, gen, value)

        return _drive_step(gen_id, gen, value)
    except Exception as e:
        return json.dumps({"status": "error", "message": traceback.format_exc()})


def dispose_generator(id_str: str) -> str:
    """Dispose of a generator.

    Called from Rust when the orchestration handler finishes or is dropped.
    """
    gen_id = int(id_str)
    _generators.pop(gen_id, None)
    _last_tasks.pop(gen_id, None)
    return "ok"


def _auto_parse_typed_result(last_task, value):
    """Transform result based on typed task metadata."""
    if last_task is None or value is None:
        return value
    if last_task.get("_typed_all") and isinstance(value, list):
        # Join results: [{"ok": val}, ...] → [val, ...]
        return [
            item.get("ok") if isinstance(item, dict) and "ok" in item else item
            for item in value
        ]
    # _typed and _typed_race: no additional transformation needed
    # (single results and race results are already auto-deserialized)
    return value


def _drive_step(generator_id: int, gen, value) -> str:
    """Drive a generator one step forward with a value."""
    try:
        # Auto-parse typed results before sending to generator
        last_task = _last_tasks.get(generator_id)
        if last_task is not None and value is not None:
            value = _auto_parse_typed_result(last_task, value)

        task = gen.send(value)
        _last_tasks[generator_id] = task
        return json.dumps(
            {
                "status": "yielded",
                "generatorId": generator_id,
                "task": task,
            }
        )
    except StopIteration as e:
        _generators.pop(generator_id, None)
        _last_tasks.pop(generator_id, None)
        output = e.value
        return json.dumps(
            {
                "status": "completed",
                "output": json.dumps(output if output is not None else None),
            }
        )
    except Exception as e:
        _generators.pop(generator_id, None)
        _last_tasks.pop(generator_id, None)
        return json.dumps({"status": "error", "message": traceback.format_exc()})


def _drive_step_with_error(generator_id: int, gen, error) -> str:
    """Drive a generator by throwing an error into it."""
    try:
        error_msg = error if isinstance(error, str) else json.dumps(error)
        task = gen.throw(Exception(error_msg))
        _last_tasks[generator_id] = task
        return json.dumps(
            {
                "status": "yielded",
                "generatorId": generator_id,
                "task": task,
            }
        )
    except StopIteration as e:
        _generators.pop(generator_id, None)
        _last_tasks.pop(generator_id, None)
        output = e.value
        return json.dumps(
            {
                "status": "completed",
                "output": json.dumps(output if output is not None else None),
            }
        )
    except Exception as e:
        _generators.pop(generator_id, None)
        _last_tasks.pop(generator_id, None)
        return json.dumps({"status": "error", "message": traceback.format_exc()})
