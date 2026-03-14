#!/usr/bin/env bash
set -euo pipefail

if [[ ! -x ".venv/bin/python" ]]; then
  echo "No venv interpreter found at .venv/bin/python. Run ./scripts/bootstrap_venv.sh first." >&2
  exit 1
fi

.venv/bin/python scripts/run_pipeline.py "$@"

