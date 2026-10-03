# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Replay isolation regression test for the duroxide Python SDK.

Two replays of the same instance can be alive in one process at the same time:
a replay that lost its orchestration lock keeps running until it tries to commit,
and a free dispatcher slot of the same runtime can fetch the instance again.
The provider rejects the commit of the replay that lost the lock. Until then the
two replays must not see each other: ctx.get_kv_value / ctx.set_kv_value /
ctx.set_custom_status / ctx.trace_* of one replay must reach that replay's native
context and no other.

The scenario runs in a child process, because it freezes the whole process:
  1. An orchestration bumps a KV counter and yields, STEPS times.
  2. In the middle of one replay, the process stops itself (SIGSTOP) for longer
     than the orchestration lock timeout, then continues (SIGCONT).
  3. The frozen replay has lost its lock but still has steps to run. A second
     dispatcher slot fetches the same instance and replays it at the same time.
  4. The orchestration is deterministic, so it must complete with the right count.

A second test runs the same freeze with a longer `orchestrator_lock_timeout_ms`.
The lock then outlives the freeze, so the instance is not fetched a second time.

Uses SqliteProvider.in_memory(). Skipped on Windows (no SIGSTOP).
"""

import json
import os
import subprocess
import sys
import time

import pytest

STEPS = 6
FREEZE_PASS = 5  # the replay that gets frozen (replays are counted from 1)
FREEZE_STEP = 1  # ...while it runs this step
FREEZE_SECONDS = 7  # longer than the default 5s orchestration lock timeout
STALE_STEP_MS = 400  # the frozen replay stays busy after it wakes up
OTHER_STEP_MS = 100  # later replays are slow enough to still run when the frozen replay makes its calls


def _run_child(extra_env=None) -> dict:
    child = subprocess.run(
        [sys.executable, os.path.abspath(__file__)],
        env={**os.environ, **(extra_env or {}), "DUROXIDE_REPLAY_ISOLATION_CHILD": "1"},
        capture_output=True,
        text=True,
        timeout=120,
    )
    line = next((l for l in child.stdout.splitlines() if l.startswith("RESULT ")), None)
    assert line is not None, (
        f"scenario printed no result (exit code {child.returncode})\n"
        f"stdout:\n{child.stdout}\nstderr:\n{child.stderr}"
    )
    result = json.loads(line[len("RESULT "):])
    assert result.get("error") is None, result.get("error")
    return result


@pytest.mark.skipif(sys.platform == "win32", reason="needs SIGSTOP/SIGCONT")
def test_lost_lock_replay_does_not_disturb_another_replay_of_the_same_instance():
    result = _run_child()

    assert result["froze_for_ms"] >= 5000, f"process was frozen for {result['froze_for_ms']}ms only"

    assert result["status"] == "Completed", f"orchestration did not complete: {result}"
    assert result["output"] == f"done counter={STEPS}"

    # The replay that lost its lock still replays cleanly against its own context.
    assert result["frozen_replay_ran_all_steps"], (
        f"the frozen replay did not run all its steps; steps: {result['steps']}"
    )
    # The run only proves something if two replays really ran at the same time.
    assert result["overlapped"], (
        f"the frozen replay and a second replay did not overlap; steps: {result['steps']}"
    )


@pytest.mark.skipif(sys.platform == "win32", reason="needs SIGSTOP/SIGCONT")
def test_orchestrator_lock_timeout_keeps_the_lock_across_a_stall_shorter_than_the_timeout():
    # Same freeze (longer than the default 5s lock), but the lock now lasts 30s.
    result = _run_child({"DUROXIDE_REPLAY_ISOLATION_LOCK_MS": "30000"})

    assert result["froze_for_ms"] >= 5000, f"process was frozen for {result['froze_for_ms']}ms only"
    assert result["status"] == "Completed", f"orchestration did not complete: {result}"
    assert result["output"] == f"done counter={STEPS}"
    # One replay per turn: the frozen replay kept its lock, so nobody fetched the instance again.
    assert result["replays"] == STEPS + 1, (
        f"expected {STEPS + 1} replays, got {result['replays']}; steps: {result['steps']}"
    )


def _spin(ms: int) -> None:
    """Burn time on the calling thread."""
    end = time.monotonic() + ms / 1000.0
    while time.monotonic() < end:
        pass


def _freeze_this_process(seconds: int) -> int:
    """Stop this whole process for `seconds`, then continue.

    Returns how long the process was actually frozen, in ms. A helper shell sends
    the signals; this function spins until it sees the clock jump.
    """
    pid = os.getpid()
    # Read the clock before the helper starts. The helper can stop this process
    # before Popen returns, and the loop below must still see the gap.
    start = last = time.monotonic()
    subprocess.Popen(
        ["sh", "-c", f"kill -STOP {pid}; sleep {seconds}; kill -CONT {pid}"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    while True:
        now = time.monotonic()
        if now - last > 2.0:
            return int((now - last) * 1000)
        if now - start > 30.0:
            raise RuntimeError("the process was never stopped")
        last = now


def _run_scenario() -> dict:
    from duroxide import Client, Runtime, RuntimeOptions, SqliteProvider

    provider = SqliteProvider.in_memory()
    client = Client(provider)
    # Two dispatcher slots: one keeps running the frozen replay, the other fetches the instance again.
    lock_ms = int(os.environ.get("DUROXIDE_REPLAY_ISOLATION_LOCK_MS", "0"))
    runtime = Runtime(
        provider,
        RuntimeOptions(
            orchestration_concurrency=2,
            dispatcher_poll_interval_ms=10,
            log_level="error",
            orchestrator_lock_timeout_ms=lock_ms if lock_ms > 0 else None,
        ),
    )

    state = {"replays": 0, "froze_for_ms": 0}
    steps = []  # "replay:step", in the order the steps started

    @runtime.register_orchestration("Counter")
    def counter(ctx, _input):
        state["replays"] += 1
        replay = state["replays"]  # test bookkeeping only; never used for a decision
        for step in range(1, STEPS + 1):
            steps.append(f"{replay}:{step}")
            if replay == FREEZE_PASS and step == FREEZE_STEP and state["froze_for_ms"] == 0:
                state["froze_for_ms"] = _freeze_this_process(FREEZE_SECONDS)
            if state["froze_for_ms"] > 0:
                _spin(STALE_STEP_MS if replay == FREEZE_PASS else OTHER_STEP_MS)
            nxt = int(ctx.get_kv_value("counter") or 0) + 1
            ctx.set_kv_value("counter", str(nxt))
            yield ctx.utc_now()
        return f"done counter={ctx.get_kv_value('counter')}"

    runtime.start()
    try:
        client.start_orchestration("replay-isolation", "Counter", None)
        result = client.wait_for_orchestration("replay-isolation", 60_000)
        # The frozen replay runs steps 1..FREEZE_PASS and then tries to commit. Give it time to get there.
        last_frozen_step = f"{FREEZE_PASS}:{FREEZE_PASS}"
        deadline = time.monotonic() + 10.0
        while last_frozen_step not in steps and time.monotonic() < deadline:
            time.sleep(0.05)
        # Overlap: a later replay started before the frozen replay ran its last step.
        frozen = [i for i, s in enumerate(steps) if s.startswith(f"{FREEZE_PASS}:")]
        later = [i for i, s in enumerate(steps) if int(s.split(":")[0]) > FREEZE_PASS]
        return {
            "status": result.status,
            "output": result.output,
            "failure": result.error,
            "froze_for_ms": state["froze_for_ms"],
            "replays": state["replays"],
            "frozen_replay_ran_all_steps": last_frozen_step in steps,
            "overlapped": bool(frozen and later and later[0] < frozen[-1]),
            "steps": steps,
        }
    finally:
        runtime.shutdown(100)


if __name__ == "__main__" and os.environ.get("DUROXIDE_REPLAY_ISOLATION_CHILD") == "1":
    try:
        outcome = _run_scenario()
        code = 0
    except Exception as exc:  # report the failure to the parent test
        outcome = {"error": f"{type(exc).__name__}: {exc}"}
        code = 1
    sys.stdout.write("RESULT " + json.dumps(outcome) + "\n")
    sys.stdout.flush()
    sys.exit(code)
