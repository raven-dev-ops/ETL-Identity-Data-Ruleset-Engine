"""Helpers for persisted event-stream refresh flows."""

from __future__ import annotations

import json
from pathlib import Path

from etl_identity_engine.generate.synth_generator import PERSON_HEADERS
from etl_identity_engine.ingest.stream_events import ResolvedStreamEventBatch
from etl_identity_engine.normalize.addresses import normalize_address
from etl_identity_engine.normalize.dates import normalize_date
from etl_identity_engine.normalize.names import normalize_name
from etl_identity_engine.normalize.phones import normalize_phone
from etl_identity_engine.observability import utc_now
from etl_identity_engine.runtime_config import PipelineConfig


def _normalize_source_row(row: dict[str, str], config: PipelineConfig) -> dict[str, str]:
    name_parts = [row.get("first_name", "").strip(), row.get("last_name", "").strip()]
    raw_name = " ".join(part for part in name_parts if part)
    return {
        **row,
        "canonical_name": normalize_name(
            raw_name,
            trim_whitespace=config.normalization.name.trim_whitespace,
            remove_punctuation=config.normalization.name.remove_punctuation,
            uppercase=config.normalization.name.uppercase,
        ),
        "canonical_dob": normalize_date(
            row.get("dob", ""),
            accepted_formats=config.normalization.date.accepted_formats,
            output_format=config.normalization.date.output_format,
        )
        or "",
        "canonical_address": normalize_address(row.get("address", "")),
        "canonical_phone": normalize_phone(
            row.get("phone", ""),
            digits_only=config.normalization.phone.digits_only,
            output_format=config.normalization.phone.output_format,
            default_country_code=config.normalization.phone.default_country_code,
        ),
    }


def apply_stream_event_batch(
    *,
    previous_rows: list[dict[str, str]],
    batch: ResolvedStreamEventBatch,
    config: PipelineConfig,
) -> tuple[list[dict[str, str]], dict[str, object]]:
    rows_by_id = {
        str(row.get("source_record_id", "")).strip(): dict(row)
        for row in previous_rows
        if str(row.get("source_record_id", "")).strip()
    }

    upsert_count = 0
    delete_count = 0
    inserted_count = 0
    updated_count = 0
    deleted_existing_count = 0
    noop_delete_count = 0
    noop_upsert_count = 0

    for event in batch.events:
        if event.operation == "delete":
            delete_count += 1
            removed = rows_by_id.pop(event.source_record_id, None)
            if removed is None:
                noop_delete_count += 1
            else:
                deleted_existing_count += 1
            continue

        upsert_count += 1
        assert event.record is not None
        normalized_row = _normalize_source_row(dict(event.record), config)
        existing = rows_by_id.get(event.source_record_id)
        if existing is None:
            inserted_count += 1
        elif existing == normalized_row:
            noop_upsert_count += 1
        else:
            updated_count += 1
        rows_by_id[event.source_record_id] = normalized_row

    current_rows = [rows_by_id[record_id] for record_id in sorted(rows_by_id)]
    summary = {
        "mode": "event_stream",
        "stream_id": batch.stream_id,
        "batch_id": batch.batch_id,
        "event_path": str(batch.event_path),
        "event_count": len(batch.events),
        "upsert_count": upsert_count,
        "delete_count": delete_count,
        "inserted_record_count": inserted_count,
        "updated_record_count": updated_count,
        "deleted_existing_record_count": deleted_existing_count,
        "noop_delete_count": noop_delete_count,
        "noop_upsert_count": noop_upsert_count,
        "first_sequence": batch.first_sequence,
        "last_sequence": batch.last_sequence,
        "event_sha256": batch.content_sha256,
        "processed_at_utc": utc_now(),
    }
    return current_rows, summary


def write_stream_events_jsonl(path: Path, events: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(json.dumps(event, sort_keys=True) for event in events) + "\n",
        encoding="utf-8",
    )


def synthesize_stream_events(
    previous_rows: list[dict[str, str]],
    *,
    stream_id: str,
    batch_index: int,
    events_per_batch: int,
) -> list[dict[str, object]]:
    if events_per_batch <= 0:
        raise ValueError("events_per_batch must be greater than 0")
    ordered_rows = [
        {
            column: str(row.get(column, "") or "")
            for column in PERSON_HEADERS
        }
        for row in sorted(
            previous_rows,
            key=lambda row: str(row.get("source_record_id", "")),
        )
        if str(row.get("source_record_id", "")).strip()
    ]
    if not ordered_rows:
        raise ValueError("previous_rows must contain at least one source record")

    delete_targets: set[str] = set()
    events: list[dict[str, object]] = []
    sequence_base = batch_index * 1000

    for index in range(events_per_batch):
        sequence = sequence_base + index + 1
        operation_selector = index % 3

        if operation_selector == 0:
            template = dict(ordered_rows[(batch_index + index) % len(ordered_rows)])
            phone_digits = "".join(character for character in template.get("phone", "") if character.isdigit())
            replacement_suffix = f"{(batch_index + index) % 10000:04d}"
            template["phone"] = f"555-{phone_digits[3:6] or '000'}-{replacement_suffix}"
            template["updated_at"] = f"2026-03-14T{(batch_index + index) % 24:02d}:00:00Z"
            events.append(
                {
                    "event_id": f"{stream_id}-evt-{sequence:06d}",
                    "stream_id": stream_id,
                    "sequence": sequence,
                    "operation": "upsert",
                    "occurred_at_utc": f"2026-03-14T{(batch_index + index) % 24:02d}:00:00Z",
                    "source_record_id": template["source_record_id"],
                    "source_system": template["source_system"],
                    "record": template,
                }
            )
            continue

        if operation_selector == 1:
            source_system = ordered_rows[(batch_index + index) % len(ordered_rows)].get("source_system", "source_a") or "source_a"
            record = {
                "source_record_id": f"EV-{batch_index:03d}-{index:04d}",
                "person_entity_id": f"STREAM-P-{batch_index:03d}-{index:04d}",
                "source_system": source_system,
                "first_name": f"STREAM{batch_index:03d}",
                "last_name": f"INSERT{index:04d}",
                "dob": "1990-01-01",
                "address": f"{1000 + batch_index + index} STREAM AVE",
                "city": "Columbus",
                "state": "OH",
                "postal_code": "43004",
                "phone": f"555-{(batch_index + 200) % 900 + 100:03d}-{index % 10000:04d}",
                "updated_at": f"2026-03-14T{(batch_index + index) % 24:02d}:15:00Z",
                "is_conflict_variant": "false",
                "conflict_types": "",
            }
            events.append(
                {
                    "event_id": f"{stream_id}-evt-{sequence:06d}",
                    "stream_id": stream_id,
                    "sequence": sequence,
                    "operation": "upsert",
                    "occurred_at_utc": f"2026-03-14T{(batch_index + index) % 24:02d}:15:00Z",
                    "source_record_id": record["source_record_id"],
                    "source_system": record["source_system"],
                    "record": record,
                }
            )
            continue

        candidate = ordered_rows[(batch_index + index) % len(ordered_rows)]
        source_record_id = candidate["source_record_id"]
        if source_record_id in delete_targets:
            candidate = ordered_rows[(batch_index + index + 1) % len(ordered_rows)]
            source_record_id = candidate["source_record_id"]
        delete_targets.add(source_record_id)
        events.append(
            {
                "event_id": f"{stream_id}-evt-{sequence:06d}",
                "stream_id": stream_id,
                "sequence": sequence,
                "operation": "delete",
                "occurred_at_utc": f"2026-03-14T{(batch_index + index) % 24:02d}:30:00Z",
                "source_record_id": source_record_id,
                "source_system": candidate["source_system"],
            }
        )

    return events
