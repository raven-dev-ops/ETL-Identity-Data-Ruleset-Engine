#!/usr/bin/env bash
set -euo pipefail

VENV_PATH="${1:-.venv}"
PYTHON_CMD="${PYTHON_CMD:-}"
INSTALL_GH="${INSTALL_GH:-1}"

resolve_python() {
  if [[ -n "${PYTHON_CMD}" ]]; then
    if command -v "${PYTHON_CMD}" >/dev/null 2>&1; then
      command -v "${PYTHON_CMD}"
      return
    fi
    echo "Requested PYTHON_CMD not found: ${PYTHON_CMD}" >&2
    exit 1
  fi

  if command -v python3 >/dev/null 2>&1; then
    command -v python3
    return
  fi
  if command -v python >/dev/null 2>&1; then
    command -v python
    return
  fi

  echo "No usable Python interpreter found. Install Python 3.11+ and re-run." >&2
  exit 1
}

PYTHON_EXE="$(resolve_python)"
echo "Using Python: ${PYTHON_EXE}"

"${PYTHON_EXE}" -m venv "${VENV_PATH}"
"${VENV_PATH}/bin/python" -m pip install --upgrade pip setuptools wheel
"${VENV_PATH}/bin/python" -m pip install -e ".[dev]"

if [[ "${INSTALL_GH}" == "1" ]]; then
  if [[ -f "./scripts/install_gh_cli.sh" ]]; then
    bash ./scripts/install_gh_cli.sh "${VENV_PATH}"
  fi
fi

echo
echo "Bootstrap complete."
echo "Activate with:"
echo "  source ${VENV_PATH}/bin/activate"
echo "Then run:"
echo "  ruff check ."
echo "  pytest"
echo "  python -m etl_identity_engine.cli run-all"
echo "  python scripts/run_checks.py"
echo "  python scripts/run_pipeline.py"
echo "  gh --version"
echo "  ./scripts/run_checks.sh"
echo
echo "Optional deployed-state check after pushing:"
echo "  ./scripts/run_checks.sh --include-remote-github-checks"
