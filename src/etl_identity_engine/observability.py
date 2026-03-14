"""Shared observability helpers for CLI and service surfaces."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import re
import sys
import time
from collections.abc import Mapping
from typing import Any


_FREE_TEXT_FIELD_NAMES = frozenset(
    {
        "notes",
        "operator_notes",
    }
)
_AUTH_FIELD_NAMES = frozenset(
    {
        "api_key",
        "authorization",
        "bearer_token",
        "jwt",
        "jwt_public_key_pem",
        "jwt_secret",
        "operator_api_key",
        "password",
        "reader_api_key",
        "secret",
        "token",
    }
)
_BEARER_TOKEN_PATTERN = re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]+")
_JWT_PATTERN = re.compile(r"\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b")
_PEM_PATTERN = re.compile(
    r"-----BEGIN [A-Z0-9 ]+-----.*?-----END [A-Z0-9 ]+-----",
    re.DOTALL,
)
_DSN_PASSWORD_PATTERN = re.compile(r"([a-z][a-z0-9+.\-]*://[^/\s:@]+:)([^@\s/]+)(@)", re.IGNORECASE)


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


def _normalized_field_name(field_name: str | None) -> str:
    if field_name is None:
        return ""
    return field_name.strip().lower()


def _redacted_free_text(value: str) -> str:
    return f"[REDACTED free_text len={len(value)}]"


def _sanitize_string_value(value: str) -> str:
    sanitized = _BEARER_TOKEN_PATTERN.sub("Bearer [REDACTED]", value)
    sanitized = _JWT_PATTERN.sub("[REDACTED jwt]", sanitized)
    sanitized = _PEM_PATTERN.sub("[REDACTED pem_material]", sanitized)
    sanitized = _DSN_PASSWORD_PATTERN.sub(r"\1[REDACTED]@", sanitized)
    return sanitized


def _sanitize_value(value: Any, *, field_name: str | None = None) -> Any:
    normalized_field_name = _normalized_field_name(field_name)
    if isinstance(value, Mapping):
        return {
            str(key): _sanitize_value(item, field_name=str(key))
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_sanitize_value(item, field_name=field_name) for item in value]
    if isinstance(value, str):
        if normalized_field_name in _FREE_TEXT_FIELD_NAMES:
            return _redacted_free_text(value)
        if normalized_field_name in _AUTH_FIELD_NAMES:
            return "[REDACTED auth_material]"
        return _sanitize_string_value(value)
    return value


def sanitize_observability_fields(fields: Mapping[str, object]) -> dict[str, Any]:
    return {
        str(key): _sanitize_value(_json_safe(value), field_name=str(key))
        for key, value in fields.items()
    }


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
        **sanitize_observability_fields(fields),
    }
    print(json.dumps(payload, sort_keys=True), file=sys.stderr)
