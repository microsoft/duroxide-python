"""
Copilot Chat scenario — exercises event queues, custom status, continue-as-new,
and wait_for_status_change in a multi-turn conversational pattern.

Ported from duroxide/tests/scenarios/copilot_chat.rs

Pattern: UI → enqueue_event("inbox", msg) → Orchestration
         Orchestration → schedule_activity("Generate") → Activity (simulated LLM)
         Orchestration → set_custom_status(reply) → UI polls wait_for_status_change
         Orchestration → continue_as_new (keeps history bounded)
"""

import json
import os
import time
import pytest
from dotenv import load_dotenv

from duroxide import PostgresProvider, Client, Runtime, RuntimeOptions

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

SCHEMA = "duroxide_python_chat"
RUN_ID = f"chat{int(time.time() * 1000):x}"


def uid(name):
    return f"{RUN_ID}-{name}"


@pytest.fixture(scope="module")
def provider():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        pytest.skip("DATABASE_URL not set")
    return PostgresProvider.connect_with_schema(db_url, SCHEMA)


def wait_for_chat_state(client, instance_id, version_ref, expected_state, expected_seq, timeout=10):
    """Poll wait_for_status_change until custom_status matches expected state and seq."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        remaining_ms = max(100, int((deadline - time.time()) * 1000))
        status = client.wait_for_status_change(instance_id, version_ref[0], 50, remaining_ms)
        if status and status.custom_status:
            version_ref[0] = status.custom_status_version
            cs = json.loads(status.custom_status)
            if cs["state"] == expected_state and cs["msg_seq"] == expected_seq:
                return cs
    raise TimeoutError(f"Timeout waiting for state={expected_state} seq={expected_seq}")


def test_copilot_chat_scenario(provider):
    """Multi-turn chat with event queues + custom status + continue-as-new."""
    client = Client(provider)
    runtime = Runtime(provider, RuntimeOptions(dispatcher_poll_interval_ms=50))

    @runtime.register_activity("GenerateResponse")
    def generate(ctx, user_msg):
        return f"Echo: {user_msg}"

    @runtime.register_orchestration("ChatBot")
    def chat_bot(ctx, input):
        # Dequeue next message (blocks until available)
        msg_json = yield ctx.dequeue_event("inbox")
        msg = json.loads(msg_json)

        # Call the "LLM" activity
        response = yield ctx.schedule_activity("GenerateResponse", msg["text"])

        # Publish reply via custom status
        ctx.set_custom_status(json.dumps({
            "state": "replied",
            "last_response": response,
            "msg_seq": msg["seq"],
        }))

        if "bye" in msg["text"].lower():
            return f"Chat ended after {msg['seq']} messages"

        # Continue as new — custom status and queued events carry forward
        return (yield ctx.continue_as_new(""))

    runtime.start()

    try:
        instance_id = uid("chat")
        client.start_orchestration(instance_id, "ChatBot", "")
        version = [0]

        # --- Turn 1: "Hello!" ---
        time.sleep(0.3)  # let orchestration register subscription
        client.enqueue_event(instance_id, "inbox", json.dumps({"seq": 1, "text": "Hello!"}))
        cs1 = wait_for_chat_state(client, instance_id, version, "replied", 1)
        assert cs1["last_response"] == "Echo: Hello!"

        # --- Turn 2: "How are you?" ---
        client.enqueue_event(instance_id, "inbox", json.dumps({"seq": 2, "text": "How are you?"}))
        cs2 = wait_for_chat_state(client, instance_id, version, "replied", 2)
        assert cs2["last_response"] == "Echo: How are you?"

        # --- Turn 3: "Bye!" — orchestration completes ---
        client.enqueue_event(instance_id, "inbox", json.dumps({"seq": 3, "text": "Bye!"}))
        result = client.wait_for_orchestration(instance_id, 10_000)

        assert result.status == "Completed"
        assert result.output == "Chat ended after 3 messages"
        final_cs = json.loads(result.custom_status)
        assert final_cs["state"] == "replied"
        assert final_cs["last_response"] == "Echo: Bye!"
        assert final_cs["msg_seq"] == 3
    finally:
        runtime.shutdown(100)
