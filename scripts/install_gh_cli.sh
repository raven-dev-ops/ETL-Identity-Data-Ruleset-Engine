#!/usr/bin/env bash
set -euo pipefail

VENV_PATH="${1:-.venv}"
VERSION="${GH_VERSION:-2.88.0}"

if [[ ! -x "${VENV_PATH}/bin/python" ]]; then
  echo "Venv python not found at ${VENV_PATH}/bin/python. Run bootstrap_venv.sh first." >&2
  exit 1
fi

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH_RAW="$(uname -m)"

case "${OS}" in
  linux) GH_OS="linux" ;;
  darwin) GH_OS="macOS" ;;
  *)
    echo "Unsupported OS for automatic gh install: ${OS}" >&2
    exit 1
    ;;
esac

case "${ARCH_RAW}" in
  x86_64|amd64) GH_ARCH="amd64" ;;
  arm64|aarch64) GH_ARCH="arm64" ;;
  *)
    echo "Unsupported architecture for automatic gh install: ${ARCH_RAW}" >&2
    exit 1
    ;;
esac

TOOLS_ROOT="${VENV_PATH}/tools"
DOWNLOADS_DIR="${TOOLS_ROOT}/downloads"
GH_EXTRACT_DIR="${TOOLS_ROOT}/gh"
mkdir -p "${DOWNLOADS_DIR}" "${GH_EXTRACT_DIR}"

ARCHIVE_NAME="gh_${VERSION}_${GH_OS}_${GH_ARCH}.tar.gz"
ARCHIVE_PATH="${DOWNLOADS_DIR}/${ARCHIVE_NAME}"
URL="https://github.com/cli/cli/releases/download/v${VERSION}/${ARCHIVE_NAME}"

echo "Downloading GitHub CLI v${VERSION}..."
curl -L "${URL}" -o "${ARCHIVE_PATH}"

echo "Extracting GitHub CLI..."
tar -xzf "${ARCHIVE_PATH}" -C "${GH_EXTRACT_DIR}"

GH_BIN="$(find "${GH_EXTRACT_DIR}" -type f -name gh | head -n 1)"
if [[ -z "${GH_BIN}" ]]; then
  echo "gh binary not found after extraction." >&2
  exit 1
fi

cp "${GH_BIN}" "${VENV_PATH}/bin/gh"
chmod +x "${VENV_PATH}/bin/gh"

echo "Installed gh to: ${VENV_PATH}/bin/gh"
"${VENV_PATH}/bin/gh" --version

