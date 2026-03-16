from __future__ import annotations

import gc
import sys
from pathlib import Path

import pytest


SRC_DIR = Path(__file__).resolve().parents[1] / "src"
TESTS_DIR = Path(__file__).resolve().parent
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))


@pytest.fixture(autouse=True)
def _cleanup_sqlite_test_resources() -> None:
    yield
    gc.collect()
    try:
        from django.conf import settings
        from django.db import connections
    except Exception:
        return
    if not settings.configured:
        return
    connections.close_all()
