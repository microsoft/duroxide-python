#!/usr/bin/env bash
# Post-publish smoke: install `duroxide==<version>` from PyPI into a fresh
# venv and run smoke.py.
#
# Required env:
#   DUROXIDE_VERSION  - exact version to install
#   SMOKE_SCRIPT      - absolute path to ci/smoke/smoke.py
set -euo pipefail

: "${DUROXIDE_VERSION:?DUROXIDE_VERSION must be set}"
: "${SMOKE_SCRIPT:?SMOKE_SCRIPT must be set}"

WORKDIR="$(mktemp -d -t duroxide-smoke-XXXXXX)"
echo "[smoke] workdir=$WORKDIR"
echo "[smoke] version=$DUROXIDE_VERSION"

cd "$WORKDIR"
python -m venv .venv
# shellcheck disable=SC1091
source .venv/bin/activate
python -m pip install --upgrade pip >/dev/null

attempts=6
for i in $(seq 1 "$attempts"); do
  # --pre allows prereleases (0.1.20rc0); harmless for stable.
  if pip install --pre "duroxide==$DUROXIDE_VERSION"; then
    break
  fi
  if [ "$i" -eq "$attempts" ]; then
    echo "[smoke] pip install failed after $attempts attempts" >&2
    exit 1
  fi
  sleep_s=$(( i * 10 ))
  echo "[smoke] install attempt $i failed; sleeping ${sleep_s}s"
  sleep "$sleep_s"
done

cp "$SMOKE_SCRIPT" ./smoke.py
python ./smoke.py
echo "[smoke] registry smoke OK"
