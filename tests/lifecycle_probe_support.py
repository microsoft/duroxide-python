# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import json
from pathlib import Path
import queue
import subprocess
import sys
import threading
import time


PREFIX = "LIFECYCLE_PROBE "
TAIL_CHARS = 8192
MAX_RECORD_CHARS = 16_384
FIRST_STOP_LIMITS = {"late": (6, 7), "retained": (5, 6), "callbacks-late": (5, 6)}
REPEATED_STOP_LIMIT = 1
TRANSPORT_MARGIN = 2


def timeout_delivery_budget(scenario):
    return FIRST_STOP_LIMITS[scenario][1] + REPEATED_STOP_LIMIT + TRANSPORT_MARGIN


def assert_timeout_measurements(scenario, message):
    lower, upper = FIRST_STOP_LIMITS[scenario]
    assert lower <= message["elapsedSeconds"] < upper, message
    assert message["repeatedSeconds"] < REPEATED_STOP_LIMIT, message


def clock_info():
    clock = time.get_clock_info("perf_counter")
    return {
        "implementation": clock.implementation,
        "resolutionSeconds": clock.resolution,
    }


class _Tail:
    def __init__(self):
        self._text = ""
        self._lock = threading.Lock()

    def append(self, text):
        with self._lock:
            self._text = (self._text + text)[-TAIL_CHARS:]

    def text(self):
        with self._lock:
            return self._text


class LifecycleProbe:
    def __init__(self, scenario, command=None):
        self.scenario = scenario
        self.command = command or [
            sys.executable,
            str(Path(__file__).parent / "fixtures" / "lifecycle_probe.py"),
            scenario,
        ]
        self.messages = queue.Queue()
        self.stdout = _Tail()
        self.stderr = _Tail()
        self.expected_phase = None
        self.last_phase = None
        self.last_message = None
        self.exit_before_cleanup = None

    def __enter__(self):
        self.child = subprocess.Popen(
            self.command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True,
        )
        self.readers = [
            threading.Thread(target=self._read_stdout, daemon=True),
            threading.Thread(target=self._read_stderr, daemon=True),
        ]
        for reader in self.readers:
            reader.start()
        return self

    def _read_stdout(self):
        continuation = False
        try:
            while True:
                line = self.child.stdout.readline(MAX_RECORD_CHARS + 1)
                if not line:
                    break
                self.stdout.append(line)
                was_continuation = continuation
                continuation = len(line) > MAX_RECORD_CHARS and not line.endswith("\n")
                if was_continuation or not line.startswith(PREFIX):
                    continue
                try:
                    if len(line) > MAX_RECORD_CHARS:
                        raise ValueError("lifecycle protocol record exceeds the bounded limit")
                    message = json.loads(line[len(PREFIX):])
                    if not isinstance(message, dict) or not isinstance(message.get("phase"), str):
                        raise ValueError("lifecycle protocol record must contain a string phase")
                    self.messages.put(("message", message))
                except (ValueError, TypeError) as error:
                    self.messages.put(("stdout protocol", error))
        except Exception as error:
            self.messages.put(("stdout reader", error))
        finally:
            self.messages.put(("exit", None))

    def _read_stderr(self):
        try:
            while True:
                text = self.child.stderr.readline(4096)
                if not text:
                    break
                self.stderr.append(text)
        except Exception as error:
            self.messages.put(("stderr reader", error))

    def expect_phase(self, phase, timeout):
        self.expected_phase = phase
        try:
            kind, value = self.messages.get(timeout=timeout)
        except queue.Empty as error:
            raise AssertionError("lifecycle phase delivery watchdog expired") from error
        if kind == "exit":
            raise AssertionError("child stdout closed before the expected phase")
        if kind != "message":
            raise AssertionError(f"{kind} failed: {type(value).__name__}: {value}") from value
        self.last_message = value
        self.last_phase = value["phase"]
        assert self.last_phase == phase, f"unexpected lifecycle phase: {self.last_phase}"
        return value

    def send(self, message):
        self.child.stdin.write(message + "\n")
        self.child.stdin.flush()

    def diagnostics(self):
        return (
            f"scenario={self.scenario!r} expected_phase={self.expected_phase!r} "
            f"last_phase={self.last_phase!r}\n"
            f"child_exit_before_cleanup={self.exit_before_cleanup!r} "
            f"child_exit={self.child.poll()!r}\n"
            f"parent_perf_counter={json.dumps(clock_info())}\n"
            f"last_message={json.dumps(self.last_message)[-TAIL_CHARS:]}\n"
            f"stdout tail:\n{self.stdout.text()}\n"
            f"stderr tail:\n{self.stderr.text()}"
        )

    def __exit__(self, error_type, error, traceback):
        self.exit_before_cleanup = self.child.poll()
        cleanup_error = None
        terminated_for_cleanup = False
        try:
            try:
                self.child.wait(timeout=0.2)
            except subprocess.TimeoutExpired:
                terminated_for_cleanup = True
                self.child.kill()
                self.child.wait(timeout=5)
            if not terminated_for_cleanup and self.child.returncode != 0:
                cleanup_error = AssertionError(f"child exited with status {self.child.returncode}")
        except Exception as failure:
            cleanup_error = failure
        finally:
            for reader in self.readers:
                reader.join(timeout=2)
            if any(reader.is_alive() for reader in self.readers):
                cleanup_error = cleanup_error or AssertionError("child stream reader did not stop")
            else:
                for stream in (self.child.stdin, self.child.stdout, self.child.stderr):
                    try:
                        stream.close()
                    except Exception as failure:
                        cleanup_error = cleanup_error or failure
        if error is not None or cleanup_error is not None:
            detail = f"{error}\n" if error is not None else ""
            if cleanup_error is not None:
                detail += f"child cleanup failed: {cleanup_error}\n"
            raise AssertionError(detail + self.diagnostics()) from (error or cleanup_error)
