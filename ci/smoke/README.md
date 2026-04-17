# Packaging smoke tests

Cross-platform smoke tests that exercise the **distributable** `duroxide` Python
wheel — not the source tree. Installs into a fresh venv, resolves platform tags,
runs a minimal SQLite-backed orchestration.

Kept in lockstep with `sdks/duroxide-node/ci/smoke/`. The duroxide-update-manager
agent owns parity. See its instructions for the full checklist.

## Files

| File                       | Purpose                                      |
|----------------------------|----------------------------------------------|
| `smoke.py`                 | Hello-world orchestration via `SqliteProvider`. |
| `run-local.sh` / `.ps1`    | Install wheel from local `WHEEL_DIR` (pre-publish). |
| `run-registry.sh` / `.ps1` | Install from PyPI (post-publish).            |
