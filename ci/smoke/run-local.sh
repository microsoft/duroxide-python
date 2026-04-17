#!/usr/bin/env bash
# Pre-publish smoke: install the duroxide wheel for the current platform into
# a fresh venv OUTSIDE the repo, then run smoke.py.
#
# Required env:
#   WHEEL_DIR     - directory containing built wheels (one must match this platform)
#   SMOKE_SCRIPT  - absolute path to ci/smoke/smoke.py
set -euo pipefail

: "${WHEEL_DIR:?WHEEL_DIR must be set}"
: "${SMOKE_SCRIPT:?SMOKE_SCRIPT must be set}"

WORKDIR="$(mktemp -d -t duroxide-smoke-XXXXXX)"
echo "[smoke] workdir=$WORKDIR"
echo "[smoke] wheel-dir=$WHEEL_DIR"
ls -lh "$WHEEL_DIR"/*.whl || true

cd "$WORKDIR"
python -m venv .venv
# shellcheck disable=SC1091
source .venv/bin/activate
python -m pip install --upgrade pip >/dev/null

# pip picks the matching wheel for the current interpreter/platform tags.
# --no-index + --find-links ensures we install from LOCAL wheels only — no PyPI fallback.
pip install --no-index --find-links "$WHEEL_DIR" duroxide

cp "$SMOKE_SCRIPT" ./smoke.py
python ./smoke.py
echo "[smoke] local smoke OK"
