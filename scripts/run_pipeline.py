"""Local runner for the scaffold pipeline."""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


if __name__ == "__main__":
    from etl_identity_engine.cli import main

    raise SystemExit(main(["run-all", *sys.argv[1:]]))
