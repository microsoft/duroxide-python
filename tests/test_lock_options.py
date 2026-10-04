# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
The lock options reach the Rust runtime.

When the runtime starts a lock renewal task it logs, at debug level, the lock
timeout and the renewal interval it uses. A child process starts a runtime with
every lock option set, runs one orchestration with one session activity, and
prints the runtime's JSON log lines. The test reads the intervals from them.

Renewal rule of the runtime: with a lock timeout of 15 s or more, the lock is
renewed `buffer` before it runs out (interval = timeout - buffer). With a shorter
timeout it is renewed at half the timeout and the buffer is ignored.

Each option gets its own value, so a value that reaches the wrong setting fails
the test. No database is needed: the child uses an in-memory SQLite provider.

Ported from duroxide-node/__tests__/lock_options.test.js
"""

import json
import os
import subprocess
import sys

CHILD = """
import json, sys
from duroxide import SqliteProvider, Client, Runtime, RuntimeOptions

options = json.loads(sys.argv[1])
provider = SqliteProvider.in_memory()
runtime = Runtime(provider, RuntimeOptions(dispatcher_poll_interval_ms=20, log_format="json", **options))
runtime.register_activity("Echo", lambda ctx, inp: inp)

@runtime.register_orchestration("LockOptions")
def lock_options(ctx, input):
    return (yield ctx.schedule_activity_on_session("Echo", "hi", "session-1"))

runtime.start()
client = Client(provider)
client.start_orchestration("lock-options-1", "LockOptions", None)
result = client.wait_for_orchestration("lock-options-1", 10_000)
runtime.shutdown(100)
print(json.dumps({"childResult": result.status}), flush=True)
"""


def run_child(options):
    env = dict(os.environ, RUST_LOG="warn,duroxide::runtime=debug")
    child = subprocess.run(
        [sys.executable, "-c", CHILD, json.dumps(options)],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert child.returncode == 0, f"child failed: {child.stderr}"
    lines = []
    for line in child.stdout.splitlines():
        try:
            lines.append(json.loads(line))
        except ValueError:
            pass
    status = next((line for line in lines if "childResult" in line), None)
    assert status is not None and status["childResult"] == "Completed", child.stdout
    return lines


def log_fields(lines, message):
    for line in lines:
        fields = line.get("fields") or {}
        if fields.get("message") == message:
            return fields
    raise AssertionError(f'no "{message}" line in the runtime log')


def test_every_lock_timeout_and_renewal_buffer_reaches_the_runtime():
    lines = run_child({
        "orchestrator_lock_timeout_ms": 20000,
        "orchestrator_lock_renewal_buffer_ms": 17000,  # renew every 3 s
        "worker_lock_timeout_ms": 21000,
        "worker_lock_renewal_buffer_ms": 17000,  # renew every 4 s
        "session_lock_timeout_ms": 22000,
        "session_lock_renewal_buffer_ms": 17000,  # renew every 5 s
    })

    orch = log_fields(lines, "Spawning orchestration lock renewal task")
    assert int(orch["lock_timeout_secs"]) == 20
    assert int(orch["buffer_secs"]) == 17
    assert int(orch["renewal_interval_secs"]) == 3

    activity = log_fields(lines, "Spawning activity manager")
    assert int(activity["lock_timeout_secs"]) == 21
    assert int(activity["renewal_interval_secs"]) == 4

    session = log_fields(lines, "Session manager started")
    assert int(session["renewal_interval_secs"]) == 5


def test_without_the_options_the_runtime_uses_its_defaults():
    lines = run_child({})

    # Orchestration lock: 5 s, below 15 s, so renewed at half the timeout.
    orch = log_fields(lines, "Spawning orchestration lock renewal task")
    assert int(orch["lock_timeout_secs"]) == 5
    assert int(orch["renewal_interval_secs"]) == 3

    # Worker lock: 30 s with a 5 s buffer.
    activity = log_fields(lines, "Spawning activity manager")
    assert int(activity["lock_timeout_secs"]) == 30
    assert int(activity["renewal_interval_secs"]) == 25

    # Session lock: 30 s with a 5 s buffer.
    session = log_fields(lines, "Session manager started")
    assert int(session["renewal_interval_secs"]) == 25
