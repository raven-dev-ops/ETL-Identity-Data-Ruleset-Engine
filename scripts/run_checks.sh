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

.venv/bin/python -m ruff check .
.venv/bin/python -m pytest

if [[ "${INCLUDE_REMOTE_GITHUB_CHECKS}" != "1" ]]; then
  exit 0
fi

if [[ ! -x ".venv/bin/gh" ]]; then
  echo "No venv gh executable found at .venv/bin/gh. Run ./scripts/bootstrap_venv.sh first." >&2
  exit 1
fi

GH_TOKEN="$(".venv/bin/gh" auth token)"
if [[ -z "${GH_TOKEN}" ]]; then
  echo "Unable to read a GitHub token from the venv gh CLI. Run .venv/bin/gh auth login first." >&2
  exit 1
fi

export GH_TOKEN
.venv/bin/python scripts/verify_github_issue_metadata.py --repo "${REPO}"
