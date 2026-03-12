#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="${1:-.}"
PROFILE="${2:-small}"
SEED="${3:-42}"

if [[ ! -x ".venv/bin/python" ]]; then
  echo "No venv interpreter found at .venv/bin/python. Run ./scripts/bootstrap_venv.sh first." >&2
  exit 1
fi

.venv/bin/python -m etl_identity_engine.cli run-all --base-dir "${BASE_DIR}" --profile "${PROFILE}" --seed "${SEED}"

