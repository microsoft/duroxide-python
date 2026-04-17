"""
Cross-platform packaging smoke test for the `duroxide` PyPI wheel.

IMPORTANT: This file MUST be run from a fresh directory OUTSIDE the repo,
against an INSTALLED `duroxide` wheel (local .whl or from PyPI). Running
inside the SDK source tree can shadow the installed extension module and
hide packaging bugs.

Semantics (MUST stay in sync with the Node smoke script):
  - Create SqliteProvider on a temp DB file (SQLite is linked statically).
  - Register a `Hello` activity returning `Hello, <input>!`.
  - Register a `HelloWorld` orchestration that calls the activity once.
  - Start runtime, start instance with input "World", wait, assert
    output == "Hello, World!".
"""

import os
import platform
import sys
import tempfile
import time

from duroxide import SqliteProvider, Client, Runtime, RuntimeOptions


def main() -> int:
    tmp = tempfile.mkdtemp(prefix="duroxide-smoke-")
    db_path = os.path.join(tmp, "smoke.db")
    print(
        f"[smoke] python={sys.version.split()[0]} "
        f"platform={platform.system().lower()} arch={platform.machine()}"
    )
    print(f"[smoke] db={db_path}")

    provider = SqliteProvider.open(db_path)
    client = Client(provider)
    runtime = Runtime(provider, RuntimeOptions(dispatcher_poll_interval_ms=50))

    @runtime.register_activity("Hello")
    def hello(_ctx, inp):
        return f"Hello, {inp}!"

    @runtime.register_orchestration("HelloWorld")
    def hello_world(ctx, inp):
        r = yield ctx.schedule_activity("Hello", inp)
        return r

    runtime.start()
    try:
        instance_id = f"smoke-{int(time.time() * 1000):x}"
        client.start_orchestration(instance_id, "HelloWorld", "World")
        result = client.wait_for_orchestration(instance_id, 15_000)

        if result.status != "Completed":
            raise SystemExit(f"[smoke] orchestration did not complete: {result!r}")
        if result.output != "Hello, World!":
            raise SystemExit(f"[smoke] unexpected output: {result.output!r}")
        print(f"[smoke] OK status={result.status} output={result.output}")
    finally:
        runtime.shutdown(200)

    return 0


if __name__ == "__main__":
    sys.exit(main())
