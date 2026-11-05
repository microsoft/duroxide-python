# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import json
import queue
import sys

import pytest

from lifecycle_probe_support import (
    LifecycleProbe,
    TAIL_CHARS,
    assert_timeout_measurements,
    timeout_delivery_budget,
)


def child_command(program):
    return [sys.executable, "-u", "-c", program]


def assert_child_cleaned_up(probe):
    assert probe.child.poll() is not None
    assert all(not reader.is_alive() for reader in probe.readers)
    assert all(stream.closed for stream in (probe.child.stdin, probe.child.stdout, probe.child.stderr))


def test_deliberate_child_failure_preserves_bounded_evidence():
    program = """
import json, sys, traceback
print("old-stdout-" + "x" * 40000, flush=True)
print("stdout-tail-marker", flush=True)
print("old-stderr-" + "y" * 40000, file=sys.stderr, flush=True)
print("stderr-tail-marker", file=sys.stderr, flush=True)
print('LIFECYCLE_PROBE {"phase":"STOPPING"}', flush=True)
try:
    raise RuntimeError("deliberate-child-failure")
except RuntimeError:
    traceback.print_exc()
    print("LIFECYCLE_PROBE " + json.dumps({
        "phase": "FAILED", "error": traceback.format_exc(),
    }), flush=True)
sys.exit(17)
"""
    probe = LifecycleProbe("deliberate-failure", child_command(program))
    with pytest.raises(AssertionError) as failure:
        with probe:
            probe.expect_phase("STOPPING", 5)
            probe.expect_phase("TIMED_OUT", 5)
    evidence = str(failure.value)
    for expected in (
        "scenario='deliberate-failure'", "expected_phase='TIMED_OUT'",
        "last_phase='FAILED'", "child_exit=17", "stdout-tail-marker",
        "stderr-tail-marker", "Traceback", "deliberate-child-failure",
        "parent_perf_counter=", "implementation", "resolutionSeconds",
    ):
        assert expected in evidence
    assert "old-stdout-" not in evidence
    assert "old-stderr-" not in evidence
    assert len(probe.stdout.text()) <= TAIL_CHARS
    assert len(probe.stderr.text()) <= TAIL_CHARS
    assert_child_cleaned_up(probe)


def test_malformed_child_protocol_preserves_original_reader_error():
    probe = LifecycleProbe("malformed", child_command(
        'print(\'LIFECYCLE_PROBE {"phase":"STOPPING"}\', flush=True)\n'
        'print("LIFECYCLE_PROBE {broken-json", flush=True)\n'
    ))
    with pytest.raises(AssertionError) as failure:
        with probe:
            probe.expect_phase("STOPPING", 5)
            probe.expect_phase("TIMED_OUT", 5)
    assert "stdout protocol failed: JSONDecodeError" in str(failure.value)
    assert "last_phase='STOPPING'" in str(failure.value)
    assert isinstance(failure.value.__cause__.__cause__, json.JSONDecodeError)
    assert_child_cleaned_up(probe)


def test_delivery_watchdog_terminates_child_and_preserves_timeout_cause():
    probe = LifecycleProbe("silent-child", child_command(
        'import threading\n'
        'print(\'LIFECYCLE_PROBE {"phase":"STOPPING"}\', flush=True)\n'
        'threading.Event().wait()\n'
    ))
    with pytest.raises(AssertionError) as failure:
        with probe:
            probe.expect_phase("STOPPING", 5)
            probe.expect_phase("TIMED_OUT", 0.05)
    assert "phase delivery watchdog expired" in str(failure.value)
    assert "last_phase='STOPPING'" in str(failure.value)
    assert isinstance(failure.value.__cause__.__cause__, queue.Empty)
    assert_child_cleaned_up(probe)


def test_child_failure_after_expected_phase_is_not_hidden_by_cleanup():
    probe = LifecycleProbe("failed-exit", child_command(
        'import sys\n'
        'print(\'LIFECYCLE_PROBE {"phase":"RECLAIMED"}\', flush=True)\n'
        'sys.exit(19)\n'
    ))
    with pytest.raises(AssertionError, match="child exited with status 19") as failure:
        with probe:
            probe.expect_phase("RECLAIMED", 5)
    assert "child_exit=19" in str(failure.value)
    assert_child_cleaned_up(probe)


def test_oversized_protocol_is_reported_while_both_streams_are_drained():
    probe = LifecycleProbe("oversized", child_command(
        'import sys\n'
        'print("LIFECYCLE_PROBE " + "x" * 40000, flush=True)\n'
        'print("stderr-drained", file=sys.stderr, flush=True)\n'
    ))
    with pytest.raises(AssertionError, match="record exceeds the bounded limit") as failure:
        with probe:
            probe.expect_phase("STOPPING", 5)
    assert "stderr-drained" in str(failure.value)
    assert isinstance(failure.value.__cause__.__cause__, ValueError)
    assert_child_cleaned_up(probe)


@pytest.mark.parametrize("scenario,elapsed", [("late", 6.8), ("retained", 5.8)])
def test_delivery_watchdog_includes_both_observations_and_transport(scenario, elapsed):
    message = {"elapsedSeconds": elapsed, "repeatedSeconds": 0.5}
    assert_timeout_measurements(scenario, message)
    first_upper = 7 if scenario == "late" else 6
    assert timeout_delivery_budget(scenario) == first_upper + 1 + 2
    assert timeout_delivery_budget(scenario) > elapsed + message["repeatedSeconds"]


@pytest.mark.parametrize("scenario,elapsed,repeated", [
    ("late", 5.999999999, 0.1),
    ("late", 7, 0.1),
    ("retained", 4.999999999, 0.1),
    ("retained", 6, 0.1),
    ("late", 6, 1),
    ("retained", 5, 1),
])
def test_transport_headroom_does_not_relax_measurement_limits(scenario, elapsed, repeated):
    with pytest.raises(AssertionError):
        assert_timeout_measurements(
            scenario, {"elapsedSeconds": elapsed, "repeatedSeconds": repeated},
        )
