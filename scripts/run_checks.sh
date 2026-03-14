#!/usr/bin/env bash
set -euo pipefail

INCLUDE_REMOTE_GITHUB_CHECKS=0
REPO="raven-dev-ops/ETL-Identity-Data-Ruleset-Engine"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --include-remote-github-checks)
      INCLUDE_REMOTE_GITHUB_CHECKS=1
      shift
      ;;
    --repo)
      REPO="${2:-}"
      if [[ -z "${REPO}" ]]; then
        echo "--repo requires OWNER/REPO" >&2
        exit 1
      fi
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

if [[ ! -x ".venv/bin/python" ]]; then
  echo "No venv interpreter found at .venv/bin/python. Run ./scripts/bootstrap_venv.sh first." >&2
  exit 1
fi

ARGS=(
  "scripts/run_checks.py"
  "--repo" "${REPO}"
)

if [[ "${INCLUDE_REMOTE_GITHUB_CHECKS}" == "1" ]]; then
  ARGS+=("--include-remote-github-checks")
fi

.venv/bin/python "${ARGS[@]}"
