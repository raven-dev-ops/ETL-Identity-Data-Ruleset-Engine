"""Deterministic synthetic source record generation with conflict recipes."""

from __future__ import annotations

import csv
import json
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

from etl_identity_engine.generate.conflict_recipes import apply_conflict_recipes


PROFILE_PERSON_COUNT = {
    "small": 24,
    "medium": 240,
    "large": 2400,
}

PROFILE_DEFAULT_DUPLICATE_RATE = {
    "small": 0.30,
    "medium": 0.35,
    "large": 0.40,
}

SOURCE_SYSTEMS = ("source_a", "source_b")
INCIDENT_SOURCE_SYSTEMS = ("cad", "rms")
INCIDENT_ROLES = ("VICTIM", "SUSPECT", "WITNESS", "REPORTING_PARTY")

PERSON_HEADERS = (
    "source_record_id",
    "person_entity_id",
    "source_system",
    "first_name",
    "last_name",
    "dob",
    "address",
    "city",
    "state",
    "postal_code",
    "phone",
    "updated_at",
    "is_conflict_variant",
    "conflict_types",
)

CONFLICT_HEADERS = (
    "source_record_id",
    "person_entity_id",
    "source_system",
    "conflict_types",
)

INCIDENT_HEADERS = (
    "incident_id",
    "source_system",
    "occurred_at",
    "location",
    "city",
    "state",
)

INCIDENT_LINK_HEADERS = (
    "incident_person_link_id",
    "incident_id",
    "person_entity_id",
    "source_record_id",
    "role",
)

ADDRESS_HISTORY_HEADERS = (
    "address_history_id",
    "person_entity_id",
    "address",
    "city",
    "state",
    "postal_code",
    "effective_start",
    "effective_end",
    "is_current",
)


@dataclass(frozen=True)
class SyntheticGenerationResult:
    generated_files: list[Path]
    summary_path: Path
    summary: dict[str, object]


def _validate_profile(profile: str) -> None:
    if profile not in PROFILE_PERSON_COUNT:
        raise ValueError(f"unknown profile: {profile}")


def _validate_formats(formats: tuple[str, ...]) -> tuple[str, ...]:
    normalized = tuple(fmt.strip().lower() for fmt in formats if fmt.strip())
    if not normalized:
        raise ValueError("at least one output format is required")

    allowed = {"csv", "parquet"}
    unknown = [fmt for fmt in normalized if fmt not in allowed]
    if unknown:
        raise ValueError(f"unsupported output formats: {unknown}")
    return normalized


def _write_csv(path: Path, rows: list[dict[str, str]], headers: tuple[str, ...]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(headers))
        writer.writeheader()
        writer.writerows(rows)


def _write_parquet(path: Path, rows: list[dict[str, str]], headers: tuple[str, ...]) -> None:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Parquet output requires `pyarrow`. Install project dependencies or omit `parquet`."
        ) from exc

    if rows:
        table = pa.Table.from_pylist(rows)
    else:
        arrays = [pa.array([], type=pa.string()) for _ in headers]
        table = pa.Table.from_arrays(arrays, names=list(headers))
    pq.write_table(table, path)


def _write_dataset(
    output_dir: Path,
    dataset_name: str,
    rows: list[dict[str, str]],
    headers: tuple[str, ...],
    formats: tuple[str, ...],
) -> list[Path]:
    generated: list[Path] = []
    for fmt in formats:
        path = output_dir / f"{dataset_name}.{fmt}"
        if fmt == "csv":
            _write_csv(path, rows, headers)
        elif fmt == "parquet":
            _write_parquet(path, rows, headers)
        else:
            raise ValueError(f"unsupported output format: {fmt}")
        generated.append(path)
    return generated


def _random_iso_timestamp(rng: random.Random, start: date, end: date) -> str:
    day_delta = (end - start).days
    chosen_day = start + timedelta(days=rng.randint(0, day_delta))
    chosen_hour = rng.randint(0, 23)
    chosen_minute = rng.randint(0, 59)
    chosen_second = rng.randint(0, 59)
    return datetime(
        chosen_day.year,
        chosen_day.month,
        chosen_day.day,
        chosen_hour,
        chosen_minute,
        chosen_second,
    ).strftime("%Y-%m-%dT%H:%M:%SZ")


def _profile_person_count(profile: str) -> int:
    _validate_profile(profile)
    return PROFILE_PERSON_COUNT[profile]


def _profile_duplicate_rate(profile: str, override: float | None) -> float:
    if override is not None:
        if override < 0 or override > 1:
            raise ValueError("duplicate_rate must be between 0 and 1")
        return override
    return PROFILE_DEFAULT_DUPLICATE_RATE[profile]


def generate_synthetic_sources(
    output_dir: Path,
    profile: str = "small",
    seed: int = 42,
    duplicate_rate: float | None = None,
    formats: tuple[str, ...] = ("csv", "parquet"),
) -> SyntheticGenerationResult:
    """Generate synthetic source datasets and return file paths + summary."""
    _validate_profile(profile)
    format_list = _validate_formats(formats)

    output_dir.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)

    first_names = ["JOHN", "JANE", "ALEX", "SAM", "RILEY", "MORGAN", "CASEY", "TAYLOR"]
    last_names = ["SMITH", "JONES", "DAVIS", "MILLER", "LEE", "BROWN", "WILSON", "CLARK"]
    street_names = ["MAIN", "OAK", "PINE", "CEDAR", "RIVER", "MAPLE", "ELM", "LAKE"]
    street_suffixes = ["STREET", "AVENUE", "ROAD", "LANE", "DRIVE"]
    cities = ["SUMMIT", "BROOKFIELD", "RIVERTON", "HAYES", "PARKSIDE"]
    states = ["OH", "PA", "MI", "IN"]

    person_count = _profile_person_count(profile)
    effective_duplicate_rate = _profile_duplicate_rate(profile, duplicate_rate)
    duplicate_count = int(round(person_count * effective_duplicate_rate))
    duplicate_indices = set(rng.sample(range(1, person_count + 1), duplicate_count))

    source_a_rows: list[dict[str, str]] = []
    source_b_rows: list[dict[str, str]] = []
    conflict_rows: list[dict[str, str]] = []
    conflicts_by_type: dict[str, int] = {}

    record_lookup: dict[str, dict[str, str]] = {}

    for idx in range(1, person_count + 1):
        person_id = f"P-{idx:06d}"
        first = rng.choice(first_names)
        last = rng.choice(last_names)
        year = rng.randint(1970, 2004)
        month = rng.randint(1, 12)
        day = rng.randint(1, 28)
        house = rng.randint(100, 9999)
        street = rng.choice(street_names)
        suffix = rng.choice(street_suffixes)
        city = rng.choice(cities)
        state = rng.choice(states)
        postal = f"{rng.randint(43000, 45999)}"
        phone = f"555-{rng.randint(100, 999)}-{rng.randint(1000, 9999)}"
        updated_at_a = _random_iso_timestamp(rng, date(2022, 1, 1), date(2024, 12, 31))
        updated_at_b = _random_iso_timestamp(rng, date(2023, 1, 1), date(2025, 12, 31))

        base = {
            "person_entity_id": person_id,
            "first_name": first,
            "last_name": last,
            "dob": f"{year:04d}-{month:02d}-{day:02d}",
            "address": f"{house} {street} {suffix}",
            "city": city,
            "state": state,
            "postal_code": postal,
            "phone": phone,
        }

        source_a = {
            "source_record_id": f"A-{idx:06d}",
            "source_system": SOURCE_SYSTEMS[0],
            "updated_at": updated_at_a,
            "is_conflict_variant": "false",
            "conflict_types": "",
            **base,
        }
        source_a_rows.append(source_a)
        record_lookup[source_a["source_record_id"]] = source_a

        source_b_base = {
            "source_record_id": f"B-{idx:06d}",
            "source_system": SOURCE_SYSTEMS[1],
            "updated_at": updated_at_b,
            "is_conflict_variant": "false",
            "conflict_types": "",
            **base,
        }

        if idx in duplicate_indices:
            applied = apply_conflict_recipes(source_b_base, rng)
            source_b_base["is_conflict_variant"] = "true"
            source_b_base["conflict_types"] = ";".join(sorted(applied))
            for conflict_type in applied:
                conflicts_by_type[conflict_type] = conflicts_by_type.get(conflict_type, 0) + 1
            conflict_rows.append(
                {
                    "source_record_id": source_b_base["source_record_id"],
                    "person_entity_id": person_id,
                    "source_system": source_b_base["source_system"],
                    "conflict_types": source_b_base["conflict_types"],
                }
            )

        source_b_rows.append(source_b_base)
        record_lookup[source_b_base["source_record_id"]] = source_b_base

    incident_count = max(1, person_count // 2)
    incident_rows: list[dict[str, str]] = []
    incident_link_rows: list[dict[str, str]] = []

    source_a_ids = [row["source_record_id"] for row in source_a_rows]
    source_b_ids = [row["source_record_id"] for row in source_b_rows]
    all_source_record_ids = source_a_ids + source_b_ids

    link_id = 1
    for idx in range(1, incident_count + 1):
        incident_id = f"INC-{idx:06d}"
        incident_rows.append(
            {
                "incident_id": incident_id,
                "source_system": rng.choice(INCIDENT_SOURCE_SYSTEMS),
                "occurred_at": _random_iso_timestamp(rng, date(2024, 1, 1), date(2025, 12, 31)),
                "location": (
                    f"{rng.randint(100, 9999)} "
                    f"{rng.choice(street_names)} "
                    f"{rng.choice(street_suffixes)}"
                ),
                "city": rng.choice(cities),
                "state": rng.choice(states),
            }
        )

        participant_count = rng.randint(1, 3)
        selected_records = rng.sample(all_source_record_ids, participant_count)
        for source_record_id in selected_records:
            incident_link_rows.append(
                {
                    "incident_person_link_id": f"LINK-{link_id:07d}",
                    "incident_id": incident_id,
                    "person_entity_id": record_lookup[source_record_id]["person_entity_id"],
                    "source_record_id": source_record_id,
                    "role": rng.choice(INCIDENT_ROLES),
                }
            )
            link_id += 1

    address_history_rows: list[dict[str, str]] = []
    history_id = 1
    for idx in range(1, person_count + 1):
        person_id = f"P-{idx:06d}"
        person_record = source_a_rows[idx - 1]

        base_year = rng.randint(2018, 2021)
        prior_count = rng.randint(0, 2)
        for offset in range(prior_count, -1, -1):
            house = int(person_record["address"].split(" ")[0]) + (offset * 50)
            address = " ".join([str(house)] + person_record["address"].split(" ")[1:])
            start_year = base_year + (prior_count - offset) * 2
            end_year = "" if offset == 0 else f"{start_year + 1}-12-31"
            address_history_rows.append(
                {
                    "address_history_id": f"ADDR-{history_id:07d}",
                    "person_entity_id": person_id,
                    "address": address,
                    "city": person_record["city"],
                    "state": person_record["state"],
                    "postal_code": person_record["postal_code"],
                    "effective_start": f"{start_year}-01-01",
                    "effective_end": end_year,
                    "is_current": "true" if offset == 0 else "false",
                }
            )
            history_id += 1

    generated_files: list[Path] = []
    generated_files.extend(
        _write_dataset(output_dir, "person_source_a", source_a_rows, PERSON_HEADERS, format_list)
    )
    generated_files.extend(
        _write_dataset(output_dir, "person_source_b", source_b_rows, PERSON_HEADERS, format_list)
    )
    generated_files.extend(
        _write_dataset(
            output_dir,
            "conflict_annotations",
            conflict_rows,
            CONFLICT_HEADERS,
            format_list,
        )
    )
    generated_files.extend(
        _write_dataset(output_dir, "incident_records", incident_rows, INCIDENT_HEADERS, format_list)
    )
    generated_files.extend(
        _write_dataset(
            output_dir,
            "incident_person_links",
            incident_link_rows,
            INCIDENT_LINK_HEADERS,
            format_list,
        )
    )
    generated_files.extend(
        _write_dataset(
            output_dir,
            "address_history",
            address_history_rows,
            ADDRESS_HISTORY_HEADERS,
            format_list,
        )
    )

    summary = {
        "profile": profile,
        "seed": seed,
        "duplicate_rate": effective_duplicate_rate,
        "person_entity_count": person_count,
        "source_a_record_count": len(source_a_rows),
        "source_b_record_count": len(source_b_rows),
        "duplicate_variant_count": len(conflict_rows),
        "incident_count": len(incident_rows),
        "incident_link_count": len(incident_link_rows),
        "address_history_count": len(address_history_rows),
        "conflicts_by_type": dict(sorted(conflicts_by_type.items())),
        "output_formats": list(format_list),
    }
    summary_path = output_dir / "generation_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    generated_files.append(summary_path)

    return SyntheticGenerationResult(
        generated_files=generated_files,
        summary_path=summary_path,
        summary=summary,
    )
