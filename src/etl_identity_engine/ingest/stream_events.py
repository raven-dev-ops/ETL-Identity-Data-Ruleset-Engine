"""Event-stream batch parsing and validation."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from etl_identity_engine.generate.synth_generator import PERSON_HEADERS


SUPPORTED_STREAM_EVENT_SUFFIXES = frozenset({".jsonl", ".ndjson"})
SUPPORTED_STREAM_EVENT_OPERATIONS = frozenset({"upsert", "delete"})


class StreamEventValidationError(ValueError):
    """Raised when an event-stream batch is incomplete or inconsistent."""


@dataclass(frozen=True)
class StreamEvent:
    event_id: str
    stream_id: str
    sequence: int
    operation: str
    occurred_at_utc: str
    source_record_id: str
    source_system: str
    record: dict[str, str] | None


@dataclass(frozen=True)
class ResolvedStreamEventBatch:
    event_path: Path
    stream_id: str
    batch_id: str
    content_sha256: str
    first_sequence: int
    last_sequence: int
    events: tuple[StreamEvent, ...]


def _stream_error(path: Path, message: str) -> StreamEventValidationError:
    return StreamEventValidationError(f"{path.name}: {message}")


def _require_non_empty_string(
    mapping: Mapping[str, object],
    key: str,
    *,
    path: Path,
    context: str,
) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value.strip():
        raise _stream_error(path, f"{context}.{key} must be a non-empty string")
    return value.strip()


def _require_positive_int(
    mapping: Mapping[str, object],
    key: str,
    *,
    path: Path,
    context: str,
) -> int:
    value = mapping.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise _stream_error(path, f"{context}.{key} must be a positive integer")
    return int(value)


def _normalize_upsert_record(
    value: object,
    *,
    path: Path,
    context: str,
    source_record_id: str,
    source_system: str,
) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise _stream_error(path, f"{context}.record must be a mapping for upsert events")

    unexpected_keys = sorted(set(value) - set(PERSON_HEADERS))
    if unexpected_keys:
        raise _stream_error(
            path,
            f"{context}.record contains unsupported keys: {', '.join(unexpected_keys)}",
        )

    resolved: dict[str, str] = {}
    for column in PERSON_HEADERS:
        if column == "source_record_id":
            record_value = value.get(column, source_record_id)
            if str(record_value).strip() != source_record_id:
                raise _stream_error(
                    path,
                    f"{context}.record.source_record_id must match {source_record_id!r}",
                )
            resolved[column] = source_record_id
            continue
        if column == "source_system":
            record_value = value.get(column, source_system)
            if str(record_value).strip() != source_system:
                raise _stream_error(
                    path,
                    f"{context}.record.source_system must match {source_system!r}",
                )
            resolved[column] = source_system
            continue

        item = value.get(column)
        if item is None:
            raise _stream_error(path, f"{context}.record.{column} must be present for upsert events")
        if not isinstance(item, str):
            raise _stream_error(path, f"{context}.record.{column} must be a string")
        resolved[column] = item
    return resolved


def resolve_stream_event_batch(
    event_path: Path,
    *,
    default_stream_id: str | None = None,
) -> ResolvedStreamEventBatch:
    resolved_path = event_path.resolve()
    if resolved_path.suffix.lower() not in SUPPORTED_STREAM_EVENT_SUFFIXES:
        raise _stream_error(
            resolved_path,
            "unsupported event-stream format; use .jsonl or .ndjson",
        )
    if not resolved_path.exists():
        raise FileNotFoundError(f"Event stream not found: {resolved_path}")

    payload = resolved_path.read_bytes()
    content_sha256 = hashlib.sha256(payload).hexdigest()
    lines = payload.decode("utf-8").splitlines()
    if not lines:
        raise _stream_error(resolved_path, "event stream must contain at least one JSON line")

    events: list[StreamEvent] = []
    seen_event_ids: set[str] = set()
    seen_sequences: set[int] = set()
    resolved_stream_id = default_stream_id.strip() if isinstance(default_stream_id, str) and default_stream_id.strip() else ""

    for index, line in enumerate(lines, start=1):
        context = f"events[{index - 1}]"
        stripped = line.strip()
        if not stripped:
            raise _stream_error(resolved_path, f"{context} must not be blank")
        try:
            raw_event = json.loads(stripped)
        except json.JSONDecodeError as exc:
            raise _stream_error(resolved_path, f"{context} is not valid JSON: {exc.msg}") from exc
        if not isinstance(raw_event, Mapping):
            raise _stream_error(resolved_path, f"{context} must be a JSON object")

        unexpected_keys = sorted(
            set(raw_event)
            - {"event_id", "stream_id", "sequence", "operation", "occurred_at_utc", "source_record_id", "source_system", "record"}
        )
        if unexpected_keys:
            raise _stream_error(
                resolved_path,
                f"{context} contains unsupported keys: {', '.join(unexpected_keys)}",
            )

        event_id = _require_non_empty_string(raw_event, "event_id", path=resolved_path, context=context)
        if event_id in seen_event_ids:
            raise _stream_error(resolved_path, f"{context}.event_id duplicates {event_id!r}")
        seen_event_ids.add(event_id)

        event_stream_id = _require_non_empty_string(
            raw_event,
            "stream_id",
            path=resolved_path,
            context=context,
        ) if "stream_id" in raw_event else ""
        if event_stream_id:
            if not resolved_stream_id:
                resolved_stream_id = event_stream_id
            elif event_stream_id != resolved_stream_id:
                raise _stream_error(
                    resolved_path,
                    f"{context}.stream_id must match {resolved_stream_id!r}",
                )

        sequence = _require_positive_int(raw_event, "sequence", path=resolved_path, context=context)
        if sequence in seen_sequences:
            raise _stream_error(resolved_path, f"{context}.sequence duplicates {sequence}")
        seen_sequences.add(sequence)

        operation = _require_non_empty_string(raw_event, "operation", path=resolved_path, context=context).lower()
        if operation not in SUPPORTED_STREAM_EVENT_OPERATIONS:
            raise _stream_error(
                resolved_path,
                f"{context}.operation must be one of: {', '.join(sorted(SUPPORTED_STREAM_EVENT_OPERATIONS))}",
            )

        occurred_at_utc = _require_non_empty_string(
            raw_event,
            "occurred_at_utc",
            path=resolved_path,
            context=context,
        )
        source_record_id = _require_non_empty_string(
            raw_event,
            "source_record_id",
            path=resolved_path,
            context=context,
        )
        source_system = _require_non_empty_string(
            raw_event,
            "source_system",
            path=resolved_path,
            context=context,
        )

        record: dict[str, str] | None = None
        if operation == "upsert":
            record = _normalize_upsert_record(
                raw_event.get("record"),
                path=resolved_path,
                context=context,
                source_record_id=source_record_id,
                source_system=source_system,
            )
        elif raw_event.get("record") not in (None, {}):
            raise _stream_error(
                resolved_path,
                f"{context}.record must be omitted for delete events",
            )

        events.append(
            StreamEvent(
                event_id=event_id,
                stream_id=event_stream_id,
                sequence=sequence,
                operation=operation,
                occurred_at_utc=occurred_at_utc,
                source_record_id=source_record_id,
                source_system=source_system,
                record=record,
            )
        )

    if not resolved_stream_id:
        resolved_stream_id = resolved_path.stem.replace(".", "_")

    ordered_events = tuple(sorted(events, key=lambda event: (event.sequence, event.event_id)))
    first_sequence = ordered_events[0].sequence
    last_sequence = ordered_events[-1].sequence
    batch_id = f"stream:{resolved_stream_id}:{first_sequence:06d}-{last_sequence:06d}:{content_sha256[:12]}"

    return ResolvedStreamEventBatch(
        event_path=resolved_path,
        stream_id=resolved_stream_id,
        batch_id=batch_id,
        content_sha256=content_sha256,
        first_sequence=first_sequence,
        last_sequence=last_sequence,
        events=ordered_events,
    )
