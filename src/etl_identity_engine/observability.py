"""Shared observability helpers for CLI and service surfaces."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import sys
import time
from typing import Any


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def seconds_since(started_at: float) -> float:
    return round(time.perf_counter() - started_at, 6)


def _json_safe(value: object) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_json_safe(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "as_posix"):
        return str(value)
    return value


def emit_structured_log(
    event: str,
    *,
    level: str = "INFO",
    **fields: object,
) -> None:
    payload = {
        "ts": utc_now(),
        "level": level.upper(),
        "event": event,
        **{key: _json_safe(value) for key, value in fields.items()},
    }
    print(json.dumps(payload, sort_keys=True), file=sys.stderr)
