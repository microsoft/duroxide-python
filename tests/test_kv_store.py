"""End-to-end KV store tests for the duroxide Python SDK."""

import time

import pytest

from duroxide import Client, PyRuntimeOptions, Runtime, SqliteProvider

RUN_ID = f"kv{int(time.time() * 1000):x}"


def uid(name: str) -> str:
    return f"{RUN_ID}-{name}"


@pytest.fixture
def provider():
    return SqliteProvider.in_memory()


def test_kv_request_response(provider):
    client = Client(provider)
    runtime = Runtime(provider, PyRuntimeOptions(dispatcher_poll_interval_ms=50))

    @runtime.register_activity("ProcessCommand")
    def process_command(_ctx, input):
        return str(input)[::-1]

    @runtime.register_orchestration("RequestServer")
    def request_server(ctx, _input):
        ctx.set_value("scratch", "temp")
        assert ctx.get_value("scratch") == "temp"
        ctx.clear_value("scratch")
        assert ctx.get_value("scratch") is None
        ctx.set_value("scratch", "temp-again")
        ctx.clear_all_values()
        assert ctx.get_value("scratch") is None

        ctx.set_value("status", "ready")

        for _ in range(3):
            request = yield ctx.wait_for_event("request")
            op_id = request["op_id"]
            command = request["command"]

            ctx.set_value("status", "processing")
            result = yield ctx.schedule_activity("ProcessCommand", command)
            ctx.set_value(f"response:{op_id}", result)
            ctx.set_value("status", "ready")

        ctx.set_value("status", "shutdown")
        return "served 3 requests"

    runtime.start()

    try:
        instance_id = uid("req-resp-server")
        client.start_orchestration(instance_id, "RequestServer", "")

        assert client.wait_for_value(instance_id, "status", 5_000) == "ready"

        requests = [("op-1", "hello"), ("op-2", "world"), ("op-3", "rust")]
        for op_id, command in requests:
            client.raise_event(
                instance_id,
                "request",
                {"op_id": op_id, "command": command},
            )
            assert client.wait_for_value(instance_id, f"response:{op_id}", 5_000) == command[::-1]

        result = client.wait_for_orchestration(instance_id, 5_000)
        assert result.status == "Completed"
        assert result.output == "served 3 requests"

        assert client.get_value(instance_id, "status") == "shutdown"
        assert client.get_value(instance_id, "response:op-1") == "olleh"
        assert client.get_value(instance_id, "response:op-2") == "dlrow"
        assert client.get_value(instance_id, "response:op-3") == "tsur"
    finally:
        runtime.shutdown(100)


def test_kv_cross_orchestration_read(provider):
    client = Client(provider)
    runtime = Runtime(provider, PyRuntimeOptions(dispatcher_poll_interval_ms=50))

    @runtime.register_activity("ComputeResult")
    def compute_result(_ctx, input):
        return str(int(input) * int(input))

    @runtime.register_orchestration("Producer")
    def producer(ctx, input):
        n = int(input)
        ctx.set_value("status", "computing")

        squared = yield ctx.schedule_activity("ComputeResult", str(n))
        ctx.set_value("result", squared)
        ctx.set_value("status", "done")

        yield ctx.wait_for_event("ack")
        return f"produced:{squared}"

    @runtime.register_orchestration("Consumer")
    def consumer(ctx, producer_id):
        attempts = 0
        while True:
            status = yield ctx.get_value_from_instance(producer_id, "status")
            if status == "done":
                break
            attempts += 1
            if attempts > 20:
                raise RuntimeError("producer never finished")
            yield ctx.schedule_timer(100)

        result = yield ctx.get_value_from_instance(producer_id, "result")
        if result is None:
            raise RuntimeError("result key missing")
        return f"consumed:{result}"

    runtime.start()

    try:
        producer_id = uid("producer")
        consumer_id = uid("consumer")

        client.start_orchestration(producer_id, "Producer", 7)
        assert client.wait_for_value(producer_id, "result", 5_000) == "49"

        client.start_orchestration(consumer_id, "Consumer", producer_id)
        consumer_result = client.wait_for_orchestration(consumer_id, 10_000)
        assert consumer_result.status == "Completed"
        assert consumer_result.output == "consumed:49"

        client.raise_event(producer_id, "ack", "")
        producer_result = client.wait_for_orchestration(producer_id, 5_000)
        assert producer_result.status == "Completed"
        assert producer_result.output == "produced:49"

        assert client.get_value(producer_id, "result") == "49"
        assert client.get_value(producer_id, "status") == "done"
    finally:
        runtime.shutdown(100)
