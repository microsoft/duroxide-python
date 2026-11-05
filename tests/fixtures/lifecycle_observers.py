# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import _thread
import gc
import json
from pathlib import Path
import signal
import sys
import threading
import time
import traceback
import weakref

from duroxide import Client, Runtime, RuntimeOptions, RuntimeStopStatus, SqliteProvider
from duroxide import _duroxide as native

from lifecycle_callbacks import attach_activity_owner, settle_deferred_references


NOT_REQUESTED = RuntimeStopStatus.NOT_REQUESTED
REQUESTED = RuntimeStopStatus.REQUESTED
FAULTED = RuntimeStopStatus.FAULTED


def fixture(database=None):
    provider = SqliteProvider.open(str(database)) if database else SqliteProvider.in_memory()
    runtime = Runtime(provider, RuntimeOptions(dispatcher_poll_interval_ms=5))
    hooks = native._lifecycle_test_hooks(runtime._native) if hasattr(native, "_lifecycle_test_hooks") else None
    return runtime, provider, hooks


def expect_error(error_type, operation, category=None):
    try:
        operation()
    except error_type as error:
        assert type(error) is error_type, type(error)
        if category is not None:
            assert category in str(error), str(error)
        return str(error)
    raise AssertionError(f"expected {error_type.__name__}")


class Call:
    def __init__(self, function, *args):
        self.results = []
        self.thread = threading.Thread(target=self._run, args=(function, args), daemon=True)
        self.thread.start()

    def _run(self, function, args):
        try:
            self.results.append(("returned", function(*args)))
        except BaseException as error:
            # Do not keep exception tracebacks that can retain a creating worker frame.
            self.results.append(("raised", type(error).__name__, str(error)))

    def outcome(self, timeout=3):
        self.thread.join(timeout)
        assert not self.thread.is_alive(), "observer probe call did not finish"
        assert len(self.results) == 1, self.results
        return self.results[0]


def until_busy(observer, last_seen):
    deadline = time.perf_counter() + 2
    while True:
        try:
            observer.wait_for_change(last_seen, 0)
        except TimeoutError:
            pass
        except RuntimeError as error:
            assert "stop_observation_busy" in str(error), str(error)
            return
        else:
            raise AssertionError("wait was not pending on the expected state")
        assert time.perf_counter() < deadline, "native wait did not register"
        time.sleep(0.001)


def until_no_native_waits(hooks):
    deadline = time.perf_counter() + 2
    while json.loads(hooks.snapshot())["observerPending"]:
        assert time.perf_counter() < deadline, hooks.snapshot()
        time.sleep(0.001)


def assert_raised(call, error_type, category):
    outcome = call.outcome()
    assert outcome[:2] == ("raised", error_type.__name__), outcome
    assert category in outcome[2], outcome
    return outcome


def active_worker():
    runtime, provider, hooks = fixture()
    client = Client(provider)
    entered, release, completed = threading.Event(), threading.Event(), threading.Event()

    @runtime.register_activity("ObserverHeldCallback")
    def callback(_context, value):
        entered.set()
        release.wait()
        completed.set()
        return value

    @runtime.register_orchestration("ObserverHeldFlow")
    def flow(context, value):
        return (yield context.schedule_activity("ObserverHeldCallback", value))

    runtime.start()
    client.start_orchestration("observer-held", "ObserverHeldFlow", "observed")
    assert entered.wait(3), "foreign callback did not enter"
    return runtime, client, hooks, release, completed


def interruption(action):
    runtime, client, hooks, release, completed = active_worker()
    observer, peer = runtime.stop_observer(), runtime.stop_observer()
    progress, controller_errors, signal_calls = [], [], []
    previous_handler = signal.getsignal(signal.SIGINT)

    def handler(_number, _frame):
        assert observer.state is NOT_REQUESTED
        expect_error(RuntimeError, lambda: observer.wait_for_change(NOT_REQUESTED, 0), "stop_observation_busy")
        signal_calls.append("reentered")
        if action == "signal-close":
            observer.close()
        elif action == "signal-error":
            raise ValueError("observer signal handler failed")

    if action.startswith("signal-"):
        signal.signal(signal.SIGINT, handler)

    def control():
        try:
            until_busy(observer, NOT_REQUESTED)
            assert observer.state is NOT_REQUESTED
            expect_error(TimeoutError, lambda: peer.wait_for_change(NOT_REQUESTED, 0))
            progress.append("native-wait-active")
            if action == "close":
                observer.close()
            elif action != "timeout":
                _thread.interrupt_main()
        except BaseException:
            controller_errors.append(traceback.format_exc())
            observer.close()

    controller = threading.Thread(target=control, daemon=True)
    controller.start()
    before = time.perf_counter()
    try:
        if action in ("timeout", "signal-return"):
            expect_error(TimeoutError, lambda: observer.wait_for_change(NOT_REQUESTED, 175))
        elif action == "interrupt":
            expect_error(KeyboardInterrupt, lambda: observer.wait_for_change(NOT_REQUESTED))
        elif action == "signal-error":
            expect_error(ValueError, lambda: observer.wait_for_change(NOT_REQUESTED), "observer signal handler failed")
        else:
            expect_error(RuntimeError, lambda: observer.wait_for_change(NOT_REQUESTED), "stop_observation_closed")
        elapsed = time.perf_counter() - before
        controller.join(2)
        assert not controller.is_alive()
        assert not controller_errors, controller_errors
        assert progress == ["native-wait-active"], progress
        assert not release.is_set() and not completed.is_set()
        assert runtime.metrics_snapshot() is not None
        assert peer.state is NOT_REQUESTED
        if hooks:
            state = json.loads(hooks.snapshot())
            assert state["active"] > 0 and state["coordinators"] == 0, state
        if action in ("close", "signal-close"):
            observer.close()
            expect_error(RuntimeError, lambda: observer.state, "stop_observation_closed")
            expect_error(RuntimeError, lambda: observer.wait_for_change(NOT_REQUESTED), "stop_observation_closed")
        else:
            assert observer.state is NOT_REQUESTED
            expect_error(TimeoutError, lambda: observer.wait_for_change(NOT_REQUESTED, 0))
        if action in ("timeout", "signal-return"):
            assert 0.15 <= elapsed < 1, elapsed
        else:
            assert elapsed < 1, elapsed
        if action.startswith("signal-"):
            assert signal_calls == ["reentered"], signal_calls
        return {
            "action": action, "elapsedSeconds": elapsed,
            "otherThreadProgress": True, "nativeWaitActive": True,
            "callbackHeldAtSettlement": True, "stopStateAtSettlement": peer.state,
            "signalHandlerReentered": bool(signal_calls),
        }
    finally:
        signal.signal(signal.SIGINT, previous_handler)
        observer.close()
        peer.close()
        release.set()
        assert client.wait_for_orchestration("observer-held", 3000).status == "Completed"
        runtime.shutdown(100)


def ordered_races():
    outcomes = {"change": 0, "close": 0, "timeout": 0, "precore": 0}
    for iteration in range(100):
        runtime, _, hooks = fixture()
        observer, peer = runtime.stop_observer(), runtime.stop_observer()
        action = ("change", "close", "timeout", "precore")[iteration % 4]
        if hooks:
            hooks.hold("observer-wait")
        first = Call(observer.wait_for_change, NOT_REQUESTED, 175 if action == "timeout" else None)
        second = Call(peer.wait_for_change, NOT_REQUESTED)
        until_busy(observer, NOT_REQUESTED)
        until_busy(peer, NOT_REQUESTED)
        if hooks:
            hooks.wait_entered("observer-wait")
        assert observer.state is NOT_REQUESTED
        if action != "precore":
            runtime.start()
            assert observer.state is NOT_REQUESTED
            until_busy(peer, NOT_REQUESTED)
        if action == "close":
            observer.close()
            assert_raised(first, RuntimeError, "stop_observation_closed")
            until_busy(peer, NOT_REQUESTED)
        elif action == "timeout":
            assert_raised(first, TimeoutError, "stop_observation_timed_out")
            expect_error(TimeoutError, lambda: observer.wait_for_change(NOT_REQUESTED, 0))
            first = Call(observer.wait_for_change, NOT_REQUESTED)
            until_busy(observer, NOT_REQUESTED)
            until_busy(peer, NOT_REQUESTED)
        runtime.shutdown(100)
        if hooks:
            hooks.release("observer-wait")
        assert second.outcome() == ("returned", REQUESTED), second.results
        if action != "close":
            assert first.outcome() == ("returned", REQUESTED), first.results
            assert observer.state is REQUESTED
        assert peer.state is REQUESTED
        if action == "precore":
            assert peer.wait_for_change(REQUESTED, 0) is None
        else:
            expect_error(TimeoutError, lambda: peer.wait_for_change(REQUESTED, 0))
        observer.close()
        peer.close()
        if hooks:
            until_no_native_waits(hooks)
            counts = json.loads(hooks.snapshot())
            assert counts["active"] == counts["cleanupActive"] == 0, counts
            assert counts["coordinators"] == (0 if action == "precore" else 1), counts
        assert len(first.results) == len(second.results) == 1
        outcomes[action] += 1
    return {
        "iterations": 100, "outcomes": outcomes, "duplicateSettlements": 0,
        "nativeRegistrationHeld": hooks is not None,
    }


def attachment_faults():
    for _ in range(100):
        runtime, _, hooks = fixture()
        hooks.hold("startup")
        hooks.hold("worker")
        hooks.hold("blocking-provider")
        hooks.fail("worker", "observer-private-sentinel")
        observer = runtime.stop_observer()
        before_attach = Call(observer.wait_for_change, NOT_REQUESTED)
        until_busy(observer, NOT_REQUESTED)
        startup = Call(runtime.start)
        hooks.wait_entered("startup")
        during_start = runtime.stop_observer()
        assert during_start.state is NOT_REQUESTED
        expect_error(TimeoutError, lambda: during_start.wait_for_change(NOT_REQUESTED, 0))
        hooks.release("startup")
        assert startup.outcome() == ("returned", None), startup.results
        hooks.wait_entered("worker")
        hooks.wait_entered("blocking-provider")
        stopping = Call(runtime.shutdown, 30_000)
        assert before_attach.outcome() == ("returned", REQUESTED), before_attach.results
        during_stop = runtime.stop_observer()
        assert during_stop.state is REQUESTED
        fault_wait = Call(observer.wait_for_change, REQUESTED)
        until_busy(observer, REQUESTED)
        hooks.release("worker")
        assert fault_wait.outcome() == ("returned", FAULTED), fault_wait.results
        assert during_start.wait_for_change(NOT_REQUESTED, 0) is FAULTED
        assert during_stop.wait_for_change(REQUESTED, 0) is FAULTED
        late = runtime.stop_observer()
        assert late.state is FAULTED
        assert stopping.thread.is_alive()
        assert json.loads(hooks.snapshot())["active"] > 0
        observer.close()
        assert late.state is FAULTED
        hooks.release("blocking-provider")
        error = assert_raised(stopping, RuntimeError, "cleanup completed and is quiescent")
        assert "sentinel" not in error[2] and "terminate the process" not in error[2]
        assert late.state is FAULTED
        expect_error(TimeoutError, lambda: late.wait_for_change(FAULTED, 0))
        for item in (during_start, during_stop, late):
            item.close()
        until_no_native_waits(hooks)
        counts = json.loads(hooks.snapshot())
        assert counts["active"] == counts["cleanupActive"] == 0, counts
        # Core fault rollback can join the SDK's stop, as in the existing lifecycle controls.
        assert 1 <= counts["coordinators"] <= 2, counts
    return {"iterations": 100, "faultObservedBeforeCleanup": True, "duplicateSettlements": 0}


def fault_before_stop():
    runtime, client, hooks, release, completed = active_worker()
    observer, late = runtime.stop_observer(), None
    try:
        waiting = Call(observer.wait_for_change, NOT_REQUESTED)
        until_busy(observer, NOT_REQUESTED)
        hooks.fail("provider", "observer-foreign-private-sentinel")
        client.start_orchestration("observer-fault-trigger", "ObserverHeldFlow", "trigger")
        assert waiting.outcome() == ("returned", FAULTED), waiting.results
        assert observer.state is FAULTED
        assert not completed.is_set(), "foreign callback retired before fault observation"
        counts = json.loads(hooks.snapshot())
        assert counts["active"] + counts["cleanupActive"] > 0, counts
        late = runtime.stop_observer()
        assert late.state is FAULTED
        assert late.wait_for_change(NOT_REQUESTED, 0) is FAULTED
        expect_error(TimeoutError, lambda: late.wait_for_change(FAULTED, 0))
        assert not completed.is_set()
    finally:
        observer.close()
        if late is not None:
            late.close()
        release.set()
        error = expect_error(RuntimeError, lambda: runtime.shutdown(100), "cleanup completed and is quiescent")
        assert "sentinel" not in error
        assert completed.wait(1)
        until_no_native_waits(hooks)
    return {
        "faultWithoutSdkStop": True, "callbackHeldAtFault": True,
        "lateState": FAULTED, "callbackRetiredAfterRelease": True,
    }


def source_freeze():
    runtime, _, hooks = fixture()
    for point in ("worker", "blocking-provider", "coordinator"):
        hooks.hold(point)
    hooks.fail("worker", "observer-source-private-sentinel")
    runtime.start()
    hooks.wait_entered("worker")
    hooks.wait_entered("blocking-provider")
    observer = runtime.stop_observer()
    waiting = Call(observer.wait_for_change, NOT_REQUESTED)
    until_busy(observer, NOT_REQUESTED)
    hooks.retire_observation_publisher(runtime._native)
    assert waiting.outcome() == ("returned", None), waiting.results
    assert observer.state is NOT_REQUESTED
    hooks.release("worker")
    hooks.wait_entered("coordinator")
    assert json.loads(hooks.snapshot())["active"] > 0
    assert observer.state is NOT_REQUESTED
    assert observer.wait_for_change(NOT_REQUESTED, 0) is None
    hooks.release("blocking-provider")
    hooks.release("coordinator")
    expect_error(RuntimeError, lambda: runtime.shutdown(100), "cleanup completed and is quiescent")
    assert observer.state is NOT_REQUESTED
    assert observer.wait_for_change(NOT_REQUESTED, 0) is None
    observer.close()
    return {"sourceEndedBeforeFault": True, "finalState": NOT_REQUESTED}


def source_end():
    def unstarted():
        runtime, _, _ = fixture()
        return runtime.stop_observer(), weakref.ref(runtime)

    observer, runtime_reference = unstarted()
    gc.collect()
    assert runtime_reference() is None
    assert observer.state is NOT_REQUESTED
    assert observer.wait_for_change(NOT_REQUESTED, 1000) is None
    observer.close()

    def stopped():
        runtime, _, _ = fixture()
        references = attach_activity_owner(runtime, "self-capturing")
        observer, peer = runtime.stop_observer(), runtime.stop_observer()
        runtime.start()
        runtime.shutdown(100)
        return observer, peer, references

    observer, peer, references = stopped()
    collected = settle_deferred_references(references)
    assert all(collected.values()), collected
    assert observer.state is REQUESTED
    assert observer.wait_for_change(NOT_REQUESTED, 0) is REQUESTED
    assert observer.wait_for_change(REQUESTED, 0) is None
    observer.close()
    assert peer.state is REQUESTED
    assert peer.wait_for_change(NOT_REQUESTED, 0) is REQUESTED
    assert peer.wait_for_change(REQUESTED, 0) is None
    peer.close()
    return {"collected": collected, "unstartedOwnerCollected": True, "unseenFinalState": REQUESTED}


def collection(retain_delivery=False):
    runtime, provider, hooks = fixture(Path(sys.argv[2]) / "collection.db")
    references = attach_activity_owner(runtime, "bound")
    worker_references = {name: reference for name, reference in references.items() if name != "runtime"}
    finalized = []
    for name, reference in worker_references.items():
        weakref.finalize(reference(), finalized.append, name)
    observer = runtime.stop_observer()
    runtime.start()
    if retain_delivery:
        hooks.hold("observer-delivery")
        hooks.hold("observer-final-access")
    first = Call(observer.wait_for_change, NOT_REQUESTED, 175 if retain_delivery else None)
    until_busy(observer, NOT_REQUESTED)
    runtime.shutdown(100)
    if retain_delivery:
        hooks.wait_entered("observer-delivery")
        assert_raised(first, TimeoutError, "stop_observation_timed_out")
    else:
        assert first.outcome() == ("returned", REQUESTED), first.results
    retry = Call(observer.wait_for_change, REQUESTED)
    until_busy(observer, REQUESTED)
    if retain_delivery:
        hooks.release("observer-delivery")
        hooks.wait_entered("observer-final-access")
        assert json.loads(hooks.snapshot())["observerPending"] == 2
        until_busy(observer, REQUESTED)
    collected = settle_deferred_references(worker_references)
    assert all(collected.values()), collected
    assert set(finalized) == set(worker_references), finalized
    assert references["runtime"]() is runtime
    assert observer.state is REQUESTED
    assert retry.thread.is_alive()
    Client(provider).start_orchestration("independent-after-observation", "Unregistered", None)
    if hooks:
        state = json.loads(hooks.snapshot())
        assert state["active"] == state["cleanupActive"] == 0, state
        assert state["started"] == state["completed"], state
        assert state["observerPending"] >= 1, state
    observer.close()
    assert_raised(retry, RuntimeError, "stop_observation_closed")
    if retain_delivery:
        assert json.loads(hooks.snapshot())["observerPending"] == 2
        hooks.release("observer-final-access")
    if hooks:
        until_no_native_waits(hooks)
    return {
        "collected": collected, "finalized": sorted(finalized),
        "runtimeRetained": True, "observerRetained": True,
        "sourceOpenDuringCollection": True, "nativeTailHeld": retain_delivery,
        "oldDeliveryDidNotSettleRetry": retain_delivery,
    }


def transport_failure():
    runtime, _, hooks = fixture()
    observer, peer = runtime.stop_observer(), runtime.stop_observer()
    hooks.fail("observer-delivery", "observation-private-sentinel")
    waiting = Call(observer.wait_for_change, NOT_REQUESTED)
    until_busy(observer, NOT_REQUESTED)
    runtime.shutdown(0)
    error = assert_raised(waiting, RuntimeError, "stop_observation_failed")
    assert "sentinel" not in error[2]
    assert observer.state is peer.state is REQUESTED
    assert observer.wait_for_change(NOT_REQUESTED, 0) is REQUESTED
    assert observer.wait_for_change(REQUESTED, 0) is None
    observer.close()
    peer.close()
    until_no_native_waits(hooks)
    return {"failureSanitized": True, "stateAfterFailure": REQUESTED, "retrySucceeded": True}


def natural_exit():
    runtime, _, hooks = fixture()
    observer = runtime.stop_observer()
    runtime.start()
    runtime.shutdown(100)
    if hooks:
        hooks.hold("observer-final-access")
    expect_error(TimeoutError, lambda: observer.wait_for_change(REQUESTED, 20))
    if hooks:
        hooks.wait_entered("observer-final-access")
        counts = json.loads(hooks.snapshot())
        assert counts["active"] == counts["cleanupActive"] == 0
        assert counts["observerPending"] == 1
    globals()["retained_runtime"] = runtime
    globals()["retained_observer"] = observer
    globals()["retained_hooks"] = hooks
    return {"runtimeRetained": True, "observerRetained": True, "nativeTailHeld": hooks is not None}


def run_observer_scenario(scenario, emit):
    actions = ("timeout", "close", "interrupt", "signal-close", "signal-error", "signal-return")
    action = scenario.removeprefix("observer-")
    if action in actions:
        result = interruption(action)
    else:
        result = {
            "ordered-races": ordered_races,
            "attachment-faults": attachment_faults,
            "source-freeze": source_freeze,
            "fault-before-stop": fault_before_stop,
            "source-end": source_end,
            "collection": collection,
            "delivery-retention": lambda: collection(True),
            "transport-failure": transport_failure,
            "exit": natural_exit,
        }[action]()
    emit("OBSERVER_RESULT", scenario=scenario, **result)
    assert sys.stdin.readline().strip() == "finish"
    emit("OBSERVER_FINISHED")
