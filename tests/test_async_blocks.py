"""
Tests for async blocks using sub-orchestrations with all() and race().

Ported from duroxide core async_block_tests. In Rust, multi-step async blocks
can be joined/raced directly. In the Python SDK, multi-step blocks must be
wrapped as sub-orchestrations, then the parent uses all()/race() on sub-orch
descriptors.
"""

import os
import time
import pytest

from dotenv import load_dotenv

from duroxide import (
    PostgresProvider,
    Client,
    Runtime,
    PyRuntimeOptions,
)

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

SCHEMA = "duroxide_python_async_blocks"
RUN_ID = f"ab{int(time.time() * 1000):x}"


def uid(name):
    return f"{RUN_ID}-{name}"


@pytest.fixture(scope="module")
def provider():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        pytest.skip("DATABASE_URL not set")
    return PostgresProvider.connect_with_schema(db_url, SCHEMA)


def run_orchestration(provider, orch_name, input, setup_fn, timeout_ms=10_000, instance_name=None):
    client = Client(provider)
    runtime = Runtime(provider, PyRuntimeOptions(dispatcher_poll_interval_ms=50))
    setup_fn(runtime)
    runtime.start()
    try:
        instance_id = uid(instance_name or orch_name)
        client.start_orchestration(instance_id, orch_name, input)
        return client.wait_for_orchestration(instance_id, timeout_ms)
    finally:
        runtime.shutdown(100)


# ─── Test 1: Join with control flow ──────────────────────────────


def test_async_block_join_with_control_flow(provider):
    def setup(rt):
        rt.register_activity("Step", lambda ctx, inp: f"step:{inp}")
        rt.register_activity("Check", lambda ctx, inp: f"check:{inp}")

        @rt.register_orchestration("AB_JoinBlockA")
        def block_a(ctx, input):
            first = yield ctx.schedule_activity("Step", "A1")
            if "step" in first:
                second = yield ctx.schedule_activity("Step", "A2")
            else:
                second = "skipped"
            return f"A:[{first},{second}]"

        @rt.register_orchestration("AB_JoinBlockB")
        def block_b(ctx, input):
            check = yield ctx.schedule_activity("Check", "B1")
            results = [check]
            for i in range(2, 4):
                r = yield ctx.schedule_activity("Step", f"B{i}")
                results.append(r)
            return f"B:[{','.join(results)}]"

        @rt.register_orchestration("AB_JoinBlockC")
        def block_c(ctx, input):
            yield ctx.schedule_timer(5)
            result = yield ctx.schedule_activity("Step", "C1")
            return f"C:[timer,{result}]"

        @rt.register_orchestration("AB_JoinBlocks")
        def join_blocks(ctx, input):
            results = yield ctx.all([
                ctx.schedule_sub_orchestration("AB_JoinBlockA", None),
                ctx.schedule_sub_orchestration("AB_JoinBlockB", None),
                ctx.schedule_sub_orchestration("AB_JoinBlockC", None),
            ])
            vals = [r["ok"] for r in results]
            return ",".join(vals)

    result = run_orchestration(provider, "AB_JoinBlocks", None, setup, instance_name="join-blocks-1")
    assert result.status == "Completed"
    assert "A:[step:A1,step:A2]" in result.output
    assert "B:[check:B1,step:B2,step:B3]" in result.output
    assert "C:[timer,step:C1]" in result.output


# ─── Test 2: Join many ───────────────────────────────────────────


def test_async_block_join_many(provider):
    def setup(rt):
        rt.register_activity("Work", lambda ctx, inp: f"done:{inp}")

        @rt.register_orchestration("AB_JoinManyBlock")
        def join_many_block(ctx, input):
            parts = input.split(":")
            index = parts[0]
            delay = int(parts[1])
            result = yield ctx.schedule_activity("Work", str(delay))
            return f"block{index}:{result}"

        @rt.register_orchestration("AB_JoinMany")
        def join_many(ctx, input):
            descs = []
            for i in range(5):
                delay = (5 - i) * 5
                descs.append(ctx.schedule_sub_orchestration("AB_JoinManyBlock", f"{i}:{delay}"))
            results = yield ctx.all(descs)
            vals = [r["ok"] for r in results]
            return ",".join(vals)

    result = run_orchestration(provider, "AB_JoinMany", None, setup, instance_name="join-many-1")
    assert result.status == "Completed"
    for i in range(5):
        expected = f"block{i}:done:{(5 - i) * 5}"
        assert expected in result.output


# ─── Test 3: Sequential (no sub-orchs) ───────────────────────────


def test_async_block_sequential(provider):
    def setup(rt):
        rt.register_activity("Process", lambda ctx, inp: f"processed:{inp}")

        @rt.register_orchestration("AB_SequentialBlocks")
        def sequential_blocks(ctx, input):
            a = yield ctx.schedule_activity("Process", input)
            b = yield ctx.schedule_activity("Process", "extra")
            phase1 = f"{a}+{b}"
            phase2 = yield ctx.schedule_activity("Process", phase1)
            yield ctx.schedule_timer(5)
            final = yield ctx.schedule_activity("Process", phase2)
            return f"final:{final}"

    result = run_orchestration(provider, "AB_SequentialBlocks", "start", setup, instance_name="seq-blocks-1")
    assert result.status == "Completed"
    assert result.output.startswith("final:")
    assert "processed:" in result.output


# ─── Test 4: Select racing ───────────────────────────────────────


def test_async_block_select_racing(provider):
    def setup(rt):
        rt.register_activity("Fast", lambda ctx, inp: f"fast:{inp}")
        rt.register_activity("Slow", lambda ctx, inp: f"slow:{inp}")

        @rt.register_orchestration("AB_RaceFastBlock")
        def fast_block(ctx, input):
            a = yield ctx.schedule_activity("Fast", "1")
            b = yield ctx.schedule_activity("Fast", "2")
            return f"fast_block:[{a},{b}]"

        @rt.register_orchestration("AB_RaceSlowBlock")
        def slow_block(ctx, input):
            yield ctx.schedule_timer(60000)
            a = yield ctx.schedule_activity("Slow", "1")
            b = yield ctx.schedule_activity("Slow", "2")
            return f"slow_block:[{a},{b}]"

        @rt.register_orchestration("AB_RaceBlocks")
        def race_blocks(ctx, input):
            winner = yield ctx.race(
                ctx.schedule_sub_orchestration("AB_RaceFastBlock", None),
                ctx.schedule_sub_orchestration("AB_RaceSlowBlock", None),
            )
            return f"winner:{winner['index']},result:{winner['value']}"

    result = run_orchestration(provider, "AB_RaceBlocks", None, setup, instance_name="race-blocks-1")
    assert result.status == "Completed"
    assert result.output.startswith("winner:0,")
    assert "fast_block:[fast:1,fast:2]" in result.output


# ─── Test 5: Block vs durable future ─────────────────────────────


def test_async_block_vs_durable_future(provider):
    def setup(rt):
        rt.register_activity("Quick", lambda ctx, inp: f"quick:{inp}")
        rt.register_activity("Multi", lambda ctx, inp: f"multi:{inp}")

        @rt.register_orchestration("AB_MultiStepBlock")
        def multi_step_block(ctx, input):
            yield ctx.schedule_timer(60000)
            a = yield ctx.schedule_activity("Multi", "1")
            b = yield ctx.schedule_activity("Multi", "2")
            return f"block:[{a},{b}]"

        @rt.register_orchestration("AB_BlockVsFuture")
        def block_vs_future(ctx, input):
            winner = yield ctx.race(
                ctx.schedule_activity("Quick", "single"),
                ctx.schedule_sub_orchestration("AB_MultiStepBlock", None),
            )
            return f"winner:{winner['index']},result:{winner['value']}"

    result = run_orchestration(provider, "AB_BlockVsFuture", None, setup, instance_name="block-vs-future-1")
    assert result.status == "Completed"
    assert result.output.startswith("winner:0,")
    assert "quick:single" in result.output


# ─── Test 6: Select3 with timers ─────────────────────────────────


def test_async_block_select3_with_timers(provider):
    def setup(rt):
        rt.register_activity("Work", lambda ctx, inp: f"work:{inp}")

        @rt.register_orchestration("AB_TimerBlockA")
        def timer_block_a(ctx, input):
            yield ctx.schedule_timer(1)
            result = yield ctx.schedule_activity("Work", "A")
            return f"A:{result}"

        @rt.register_orchestration("AB_TimerBlockB")
        def timer_block_b(ctx, input):
            yield ctx.schedule_timer(60000)
            result = yield ctx.schedule_activity("Work", "B")
            return f"B:{result}"

        @rt.register_orchestration("AB_TimerBlockC")
        def timer_block_c(ctx, input):
            yield ctx.schedule_timer(30000)
            result = yield ctx.schedule_activity("Work", "C")
            return f"C:{result}"

        @rt.register_orchestration("AB_Select3Inner")
        def select3_inner(ctx, input):
            winner = yield ctx.race(
                ctx.schedule_sub_orchestration("AB_TimerBlockB", None),
                ctx.schedule_sub_orchestration("AB_TimerBlockC", None),
            )
            return winner["value"]

        @rt.register_orchestration("AB_Select3Timers")
        def select3_timers(ctx, input):
            winner = yield ctx.race(
                ctx.schedule_sub_orchestration("AB_TimerBlockA", None),
                ctx.schedule_sub_orchestration("AB_Select3Inner", None),
            )
            return f"winner:{winner['index']},result:{winner['value']}"

    result = run_orchestration(provider, "AB_Select3Timers", None, setup, instance_name="select3-timers-1")
    assert result.status == "Completed"
    assert result.output.startswith("winner:0,")
    assert "A:work:A" in result.output


# ─── Test 7: Nested join in select ───────────────────────────────


def test_async_block_nested_join_in_select(provider):
    def setup(rt):
        rt.register_activity("Step", lambda ctx, inp: f"step:{inp}")

        @rt.register_orchestration("AB_WorkBlock")
        def work_block(ctx, input):
            results = yield ctx.all([
                ctx.schedule_activity("Step", "1"),
                ctx.schedule_activity("Step", "2"),
                ctx.schedule_activity("Step", "3"),
            ])
            vals = [r["ok"] for r in results]
            return f"work:[{','.join(vals)}]"

        @rt.register_orchestration("AB_TimeoutBlock")
        def timeout_block(ctx, input):
            yield ctx.schedule_timer(60000)
            return "timeout"

        @rt.register_orchestration("AB_NestedJoinSelect")
        def nested_join_select(ctx, input):
            winner = yield ctx.race(
                ctx.schedule_sub_orchestration("AB_WorkBlock", None),
                ctx.schedule_sub_orchestration("AB_TimeoutBlock", None),
            )
            return f"winner:{winner['index']},result:{winner['value']}"

    result = run_orchestration(provider, "AB_NestedJoinSelect", None, setup, instance_name="nested-join-select-1")
    assert result.status == "Completed"
    assert result.output.startswith("winner:0,")
    assert "step:1" in result.output
    assert "step:2" in result.output
    assert "step:3" in result.output


# ─── Test 8: Sub-orchestration wins race ─────────────────────────


def test_async_block_suborchestration_wins_race(provider):
    def setup(rt):
        rt.register_activity("FastWork", lambda ctx, inp: f"fast:{inp}")
        rt.register_activity("SlowWork", lambda ctx, inp: f"slow:{inp}")

        @rt.register_orchestration("AB_FastChild")
        def fast_child(ctx, input):
            result = yield ctx.schedule_activity("FastWork", input)
            return f"child:{result}"

        @rt.register_orchestration("AB_SubOrchBlock")
        def sub_orch_block(ctx, input):
            sub = yield ctx.schedule_sub_orchestration("AB_FastChild", "sub-input")
            act = yield ctx.schedule_activity("FastWork", "after-sub")
            return f"blockA:[{sub},{act}]"

        @rt.register_orchestration("AB_SlowBlock8")
        def slow_block_8(ctx, input):
            yield ctx.schedule_timer(60000)
            a = yield ctx.schedule_activity("SlowWork", "1")
            b = yield ctx.schedule_activity("SlowWork", "2")
            return f"blockB:[{a},{b}]"

        @rt.register_orchestration("AB_RaceParent")
        def race_parent(ctx, input):
            winner = yield ctx.race(
                ctx.schedule_sub_orchestration("AB_SubOrchBlock", None),
                ctx.schedule_sub_orchestration("AB_SlowBlock8", None),
            )
            return f"winner:{winner['index']},result:{winner['value']}"

    result = run_orchestration(provider, "AB_RaceParent", None, setup, instance_name="suborchestration-wins-1")
    assert result.status == "Completed"
    assert result.output.startswith("winner:0,")
    assert "child:fast:sub-input" in result.output
    assert "fast:after-sub" in result.output


# ─── Test 9: Sub-orchestration loses race ────────────────────────


def test_async_block_suborchestration_loses_race(provider):
    def setup(rt):
        rt.register_activity("Fast", lambda ctx, inp: f"fast:{inp}")
        rt.register_activity("VerySlow", lambda ctx, inp: f"veryslow:{inp}")

        @rt.register_orchestration("AB_SlowChild")
        def slow_child(ctx, input):
            yield ctx.schedule_timer(60000)
            a = yield ctx.schedule_activity("VerySlow", input + "-1")
            b = yield ctx.schedule_activity("VerySlow", input + "-2")
            return f"child:[{a},{b}]"

        @rt.register_orchestration("AB_SlowSubOrchBlock")
        def slow_sub_orch_block(ctx, input):
            sub = yield ctx.schedule_sub_orchestration("AB_SlowChild", "sub-input")
            return f"blockA:{sub}"

        @rt.register_orchestration("AB_FastBlock9")
        def fast_block_9(ctx, input):
            a = yield ctx.schedule_activity("Fast", "1")
            b = yield ctx.schedule_activity("Fast", "2")
            return f"blockB:[{a},{b}]"

        @rt.register_orchestration("AB_RaceParentLoses")
        def race_parent_loses(ctx, input):
            winner = yield ctx.race(
                ctx.schedule_sub_orchestration("AB_SlowSubOrchBlock", None),
                ctx.schedule_sub_orchestration("AB_FastBlock9", None),
            )
            return f"winner:{winner['index']},result:{winner['value']}"

    result = run_orchestration(provider, "AB_RaceParentLoses", None, setup, instance_name="suborchestration-loses-1")
    assert result.status == "Completed"
    assert result.output.startswith("winner:1,")
    assert "blockB:[fast:1,fast:2]" in result.output


# ─── Test 10: Multiple sub-orchestrations joined ─────────────────


def test_async_block_multiple_suborchestrations_joined(provider):
    def setup(rt):
        rt.register_activity("Transform", lambda ctx, inp: f"transformed:{inp}")

        @rt.register_orchestration("AB_ChildA")
        def child_a(ctx, input):
            result = yield ctx.schedule_activity("Transform", "A-" + input)
            return f"childA:{result}"

        @rt.register_orchestration("AB_ChildB")
        def child_b(ctx, input):
            r1 = yield ctx.schedule_activity("Transform", "B1-" + input)
            r2 = yield ctx.schedule_activity("Transform", "B2-" + input)
            return f"childB:[{r1},{r2}]"

        @rt.register_orchestration("AB_ChildC")
        def child_c(ctx, input):
            yield ctx.schedule_timer(5)
            result = yield ctx.schedule_activity("Transform", "C-" + input)
            return f"childC:timer+{result}"

        @rt.register_orchestration("AB_JoinBlock1")
        def join_block_1(ctx, input):
            sub = yield ctx.schedule_sub_orchestration("AB_ChildA", input)
            act = yield ctx.schedule_activity("Transform", "block1-extra")
            return f"block1:[{sub},{act}]"

        @rt.register_orchestration("AB_JoinBlock2")
        def join_block_2(ctx, input):
            sub = yield ctx.schedule_sub_orchestration("AB_ChildB", input)
            return f"block2:{sub}"

        @rt.register_orchestration("AB_JoinBlock3")
        def join_block_3(ctx, input):
            act = yield ctx.schedule_activity("Transform", "block3-first")
            sub = yield ctx.schedule_sub_orchestration("AB_ChildC", input)
            return f"block3:[{act},{sub}]"

        @rt.register_orchestration("AB_JoinSubOrchParent")
        def join_sub_orch_parent(ctx, input):
            results = yield ctx.all([
                ctx.schedule_sub_orchestration("AB_JoinBlock1", input),
                ctx.schedule_sub_orchestration("AB_JoinBlock2", input),
                ctx.schedule_sub_orchestration("AB_JoinBlock3", input),
            ])
            vals = [r["ok"] for r in results]
            return ",".join(vals)

    result = run_orchestration(provider, "AB_JoinSubOrchParent", "data", setup, instance_name="join-suborchestration-1")
    assert result.status == "Completed"
    assert "block1:" in result.output
    assert "childA:" in result.output
    assert "block2:" in result.output
    assert "childB:" in result.output
    assert "block3:" in result.output
    assert "childC:" in result.output


# ─── Test 11: Sub-orchestration racing timeout ───────────────────


def test_async_block_suborchestration_racing_timeout(provider):
    def setup(rt):
        rt.register_activity("Work", lambda ctx, inp: f"work:{inp}")
        rt.register_activity("SlowWork", lambda ctx, inp: f"slowwork:{inp}")

        @rt.register_orchestration("AB_FastChild2")
        def fast_child_2(ctx, input):
            result = yield ctx.schedule_activity("Work", input)
            return f"fast-child:{result}"

        @rt.register_orchestration("AB_SlowChild2")
        def slow_child_2(ctx, input):
            yield ctx.schedule_timer(60000)
            result = yield ctx.schedule_activity("SlowWork", input)
            return f"slow-child:{result}"

        @rt.register_orchestration("AB_FastSubBlock")
        def fast_sub_block(ctx, input):
            sub = yield ctx.schedule_sub_orchestration("AB_FastChild2", "fast-input")
            return f"blockA:{sub}"

        @rt.register_orchestration("AB_SlowSubBlock")
        def slow_sub_block(ctx, input):
            sub = yield ctx.schedule_sub_orchestration("AB_SlowChild2", "slow-input")
            return f"blockB:{sub}"

        @rt.register_orchestration("AB_TimeoutOnlyBlock")
        def timeout_only_block(ctx, input):
            yield ctx.schedule_timer(120000)
            return "blockC:timeout"

        @rt.register_orchestration("AB_TimeoutRaceInner")
        def timeout_race_inner(ctx, input):
            winner = yield ctx.race(
                ctx.schedule_sub_orchestration("AB_SlowSubBlock", None),
                ctx.schedule_sub_orchestration("AB_TimeoutOnlyBlock", None),
            )
            return winner["value"]

        @rt.register_orchestration("AB_TimeoutRaceParent")
        def timeout_race_parent(ctx, input):
            winner = yield ctx.race(
                ctx.schedule_sub_orchestration("AB_FastSubBlock", None),
                ctx.schedule_sub_orchestration("AB_TimeoutRaceInner", None),
            )
            return f"winner:{winner['index']},result:{winner['value']}"

    result = run_orchestration(provider, "AB_TimeoutRaceParent", None, setup, instance_name="timeout-race-1")
    assert result.status == "Completed"
    assert result.output.startswith("winner:0,")
    assert "fast-child:work:fast-input" in result.output


# ─── Test 12: Nested sub-orchestration chain ─────────────────────


def test_async_block_nested_suborchestration_chain(provider):
    def setup(rt):
        rt.register_activity("Leaf", lambda ctx, inp: f"leaf:{inp}")

        @rt.register_orchestration("AB_Grandchild")
        def grandchild(ctx, input):
            result = yield ctx.schedule_activity("Leaf", input)
            return f"grandchild:{result}"

        @rt.register_orchestration("AB_Child")
        def child(ctx, input):
            results = yield ctx.all([
                ctx.schedule_sub_orchestration("AB_Grandchild", "gc-" + input),
                ctx.schedule_activity("Leaf", "child-" + input),
            ])
            gc = results[0]["ok"]
            own = results[1]["ok"]
            return f"child:[{gc},{own}]"

        @rt.register_orchestration("AB_ChainBlock1")
        def chain_block_1(ctx, input):
            sub = yield ctx.schedule_sub_orchestration("AB_Child", "c1-" + input)
            return f"block1:{sub}"

        @rt.register_orchestration("AB_ChainBlock2")
        def chain_block_2(ctx, input):
            yield ctx.schedule_timer(5)
            sub = yield ctx.schedule_sub_orchestration("AB_Child", "c2-" + input)
            return f"block2:timer+{sub}"

        @rt.register_orchestration("AB_NestedParent")
        def nested_parent(ctx, input):
            results = yield ctx.all([
                ctx.schedule_sub_orchestration("AB_ChainBlock1", input),
                ctx.schedule_sub_orchestration("AB_ChainBlock2", input),
            ])
            vals = [r["ok"] for r in results]
            return ",".join(vals)

    result = run_orchestration(provider, "AB_NestedParent", "root", setup, instance_name="nested-chain-1")
    assert result.status == "Completed"
    assert "block1:" in result.output
    assert "block2:timer+" in result.output
    assert "grandchild:" in result.output
    assert "child:" in result.output
