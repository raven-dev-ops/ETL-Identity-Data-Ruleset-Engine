"""Production batch manifest parsing and validation."""

from __future__ import annotations

import csv
from io import StringIO
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
import posixpath
import re
from urllib.parse import urlsplit, urlunsplit

import yaml

from etl_identity_engine.generate.synth_generator import PERSON_HEADERS
from etl_identity_engine.io.read import read_dict_fieldnames, read_dict_rows


SUPPORTED_MANIFEST_SUFFIXES = frozenset({".json", ".yaml", ".yml"})
SUPPORTED_MANIFEST_VERSION = "1.0"
SUPPORTED_ENTITY_TYPE = "person"
SUPPORTED_LANDING_ZONE_KINDS = frozenset({"local_filesystem", "object_storage"})
SUPPORTED_SOURCE_FORMATS = frozenset({"csv", "parquet"})
SUPPORTED_SOURCE_ID_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")
SUPPORTED_SCHEMA_VERSIONS = {"person-v1": PERSON_HEADERS}


class BatchManifestValidationError(ValueError):
    """Raised when a batch manifest is incomplete or inconsistent."""


@dataclass(frozen=True)
class LandingZoneSpec:
    kind: str
    base_location: str
    storage_options: dict[str, str | int | float | bool]


@dataclass(frozen=True)
class BatchSourceSpec:
    source_id: str
    path: str
    format: str
    schema_version: str
    required_columns: tuple[str, ...]


@dataclass(frozen=True)
class BatchManifest:
    manifest_version: str
    entity_type: str
    batch_id: str
    landing_zone: LandingZoneSpec
    sources: tuple[BatchSourceSpec, ...]


@dataclass(frozen=True)
class ResolvedBatchSource:
    spec: BatchSourceSpec
    source_reference: str
    fieldnames: tuple[str, ...]
    rows: tuple[dict[str, str], ...]


@dataclass(frozen=True)
class ResolvedBatchManifest:
    manifest_path: Path
    manifest: BatchManifest
    sources: tuple[ResolvedBatchSource, ...]

    @property
    def input_paths(self) -> tuple[str, ...]:
        return tuple(source.source_reference for source in self.sources)

    def all_rows(self) -> list[dict[str, str]]:
        rows: list[dict[str, str]] = []
        for source in self.sources:
            rows.extend(dict(row) for row in source.rows)
        return rows


def _manifest_error(path: Path, message: str) -> BatchManifestValidationError:
    return BatchManifestValidationError(f"{path.name}: {message}")


def _load_manifest_mapping(path: Path) -> Mapping[str, object]:
    if path.suffix.lower() not in SUPPORTED_MANIFEST_SUFFIXES:
        raise _manifest_error(
            path,
            "unsupported manifest format; use .json, .yaml, or .yml",
        )
    if not path.exists():
        raise FileNotFoundError(f"Batch manifest not found: {path}")

    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, Mapping):
        raise _manifest_error(path, "manifest must contain a mapping")
    return data


def _validate_allowed_keys(
    mapping: Mapping[str, object],
    *,
    allowed_keys: set[str],
    path: Path,
    context: str,
) -> None:
    unexpected_keys = sorted(set(mapping) - allowed_keys)
    if unexpected_keys:
        raise _manifest_error(
            path,
            f"{context} contains unsupported keys: {', '.join(unexpected_keys)}",
        )


def _require_mapping(
    mapping: Mapping[str, object],
    key: str,
    *,
    path: Path,
    context: str,
) -> Mapping[str, object]:
    value = mapping.get(key)
    if not isinstance(value, Mapping):
        raise _manifest_error(path, f"{context}.{key} must be a mapping")
    return value


def _require_non_empty_string(
    mapping: Mapping[str, object],
    key: str,
    *,
    path: Path,
    context: str,
) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value.strip():
        raise _manifest_error(path, f"{context}.{key} must be a non-empty string")
    return value.strip()


def _require_string_list(
    mapping: Mapping[str, object],
    key: str,
    *,
    path: Path,
    context: str,
) -> tuple[str, ...]:
    value = mapping.get(key)
    if not isinstance(value, list) or not value:
        raise _manifest_error(path, f"{context}.{key} must be a non-empty list of strings")

    items: list[str] = []
    for index, item in enumerate(value):
        if not isinstance(item, str) or not item.strip():
            raise _manifest_error(
                path,
                f"{context}.{key}[{index}] must be a non-empty string",
            )
        items.append(item.strip())
    return tuple(items)


def _require_scalar_mapping(
    mapping: Mapping[str, object],
    key: str,
    *,
    path: Path,
    context: str,
) -> dict[str, str | int | float | bool]:
    value = mapping.get(key, {})
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise _manifest_error(path, f"{context}.{key} must be a mapping")

    resolved: dict[str, str | int | float | bool] = {}
    for item_key, item_value in value.items():
        if not isinstance(item_key, str) or not item_key.strip():
            raise _manifest_error(path, f"{context}.{key} contains a non-string key")
        if isinstance(item_value, bool | int | float | str):  # type: ignore[arg-type]
            resolved[item_key.strip()] = item_value
            continue
        raise _manifest_error(
            path,
            f"{context}.{key}.{item_key} must be a scalar string, number, or boolean",
        )
    return resolved


def _is_uri(value: str) -> bool:
    parts = urlsplit(value)
    return bool(parts.scheme and "://" in value)


def _resolve_local_base_path(manifest_path: Path, landing_zone: LandingZoneSpec) -> Path:
    base_path = Path(landing_zone.base_location)
    if base_path.is_absolute():
        return base_path
    return (manifest_path.parent / base_path).resolve()


def _resolve_object_uri(base_uri: str, source_path: str) -> str:
    if _is_uri(source_path):
        return source_path

    base_parts = urlsplit(base_uri)
    joined_path = posixpath.join(base_parts.path.rstrip("/") or "/", source_path.lstrip("/"))
    return urlunsplit(
        (
            base_parts.scheme,
            base_parts.netloc,
            joined_path,
            base_parts.query,
            base_parts.fragment,
        )
    )


def _resolved_name_for_location(location: str) -> str:
    parts = urlsplit(location)
    if parts.scheme:
        return Path(parts.path).name
    return Path(location).name


def _load_object_storage_rows(
    location: str,
    *,
    source_format: str,
    storage_options: Mapping[str, str | int | float | bool],
) -> tuple[tuple[str, ...], list[dict[str, str]]]:
    try:
        import fsspec
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Object-storage manifest inputs require `fsspec`. Install project dependencies "
            "and any protocol-specific plugin such as `s3fs` for s3:// URIs."
        ) from exc

    try:
        with fsspec.open(location, "rb", **storage_options) as handle:
            payload = handle.read()
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"Input file not found: {location}") from exc

    if source_format == "csv":
        text = payload.decode("utf-8")
        reader = csv.DictReader(StringIO(text))
        return tuple(reader.fieldnames or ()), [dict(row) for row in reader]

    if source_format == "parquet":
        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pq.read_table(pa.BufferReader(payload))
        return tuple(table.column_names), [
            {
                key: "" if value is None else str(value)
                for key, value in row.items()
            }
            for row in table.to_pylist()
        ]

    raise ValueError(f"Unsupported source format: {source_format}")


def _load_batch_manifest(path: Path) -> BatchManifest:
    manifest = _load_manifest_mapping(path)
    _validate_allowed_keys(
        manifest,
        allowed_keys={"manifest_version", "entity_type", "batch_id", "landing_zone", "sources"},
        path=path,
        context="top-level manifest",
    )

    manifest_version = _require_non_empty_string(
        manifest,
        "manifest_version",
        path=path,
        context="manifest",
    )
    if manifest_version != SUPPORTED_MANIFEST_VERSION:
        raise _manifest_error(
            path,
            f"manifest.manifest_version must be {SUPPORTED_MANIFEST_VERSION!r}",
        )

    entity_type = _require_non_empty_string(
        manifest,
        "entity_type",
        path=path,
        context="manifest",
    )
    if entity_type != SUPPORTED_ENTITY_TYPE:
        raise _manifest_error(
            path,
            f"manifest.entity_type must be {SUPPORTED_ENTITY_TYPE!r}",
        )

    batch_id = _require_non_empty_string(
        manifest,
        "batch_id",
        path=path,
        context="manifest",
    )

    landing_zone = _require_mapping(
        manifest,
        "landing_zone",
        path=path,
        context="manifest",
    )
    landing_zone_kind = _require_non_empty_string(
        landing_zone,
        "kind",
        path=path,
        context="landing_zone",
    )
    if landing_zone_kind not in SUPPORTED_LANDING_ZONE_KINDS:
        raise _manifest_error(
            path,
            (
                "landing_zone.kind must be one of: "
                f"{', '.join(sorted(SUPPORTED_LANDING_ZONE_KINDS))}"
            ),
        )
    if landing_zone_kind == "local_filesystem":
        _validate_allowed_keys(
            landing_zone,
            allowed_keys={"kind", "base_path"},
            path=path,
            context="landing_zone",
        )
        base_location = _require_non_empty_string(
            landing_zone,
            "base_path",
            path=path,
            context="landing_zone",
        )
        storage_options: dict[str, str | int | float | bool] = {}
    else:
        _validate_allowed_keys(
            landing_zone,
            allowed_keys={"kind", "base_uri", "storage_options"},
            path=path,
            context="landing_zone",
        )
        base_location = _require_non_empty_string(
            landing_zone,
            "base_uri",
            path=path,
            context="landing_zone",
        )
        if not _is_uri(base_location):
            raise _manifest_error(path, "landing_zone.base_uri must be a URI such as s3://bucket/prefix")
        storage_options = _require_scalar_mapping(
            landing_zone,
            "storage_options",
            path=path,
            context="landing_zone",
        )

    sources_value = manifest.get("sources")
    if not isinstance(sources_value, list) or not sources_value:
        raise _manifest_error(path, "manifest.sources must be a non-empty list")

    sources: list[BatchSourceSpec] = []
    seen_source_ids: set[str] = set()
    for index, source_value in enumerate(sources_value):
        context = f"sources[{index}]"
        if not isinstance(source_value, Mapping):
            raise _manifest_error(path, f"{context} must be a mapping")
        _validate_allowed_keys(
            source_value,
            allowed_keys={"source_id", "path", "format", "schema_version", "required_columns"},
            path=path,
            context=context,
        )

        source_id = _require_non_empty_string(
            source_value,
            "source_id",
            path=path,
            context=context,
        )
        if not SUPPORTED_SOURCE_ID_PATTERN.match(source_id):
            raise _manifest_error(
                path,
                f"{context}.source_id must match {SUPPORTED_SOURCE_ID_PATTERN.pattern!r}",
            )
        if source_id in seen_source_ids:
            raise _manifest_error(path, f"{context}.source_id duplicates {source_id!r}")
        seen_source_ids.add(source_id)

        source_format = _require_non_empty_string(
            source_value,
            "format",
            path=path,
            context=context,
        ).lower()
        if source_format not in SUPPORTED_SOURCE_FORMATS:
            raise _manifest_error(
                path,
                f"{context}.format must be one of: {', '.join(sorted(SUPPORTED_SOURCE_FORMATS))}",
            )

        schema_version = _require_non_empty_string(
            source_value,
            "schema_version",
            path=path,
            context=context,
        )
        expected_columns = SUPPORTED_SCHEMA_VERSIONS.get(schema_version)
        if expected_columns is None:
            raise _manifest_error(
                path,
                f"{context}.schema_version must be one of: {', '.join(sorted(SUPPORTED_SCHEMA_VERSIONS))}",
            )

        required_columns = _require_string_list(
            source_value,
            "required_columns",
            path=path,
            context=context,
        )
        if set(required_columns) != set(expected_columns):
            raise _manifest_error(
                path,
                f"{context}.required_columns must match the {schema_version!r} contract",
            )

        sources.append(
            BatchSourceSpec(
                source_id=source_id,
                path=_require_non_empty_string(
                    source_value,
                    "path",
                    path=path,
                    context=context,
                ),
                format=source_format,
                schema_version=schema_version,
                required_columns=required_columns,
            )
        )

    return BatchManifest(
        manifest_version=manifest_version,
        entity_type=entity_type,
        batch_id=batch_id,
        landing_zone=LandingZoneSpec(
            kind=landing_zone_kind,
            base_location=base_location,
            storage_options=storage_options,
        ),
        sources=tuple(sources),
    )


def _resolve_local_source_path(
    manifest_path: Path,
    landing_zone: LandingZoneSpec,
    source: BatchSourceSpec,
) -> Path:
    source_path = Path(source.path)
    if source_path.is_absolute():
        resolved_path = source_path
    else:
        resolved_path = _resolve_local_base_path(manifest_path, landing_zone) / source_path

    suffix = resolved_path.suffix.lower().lstrip(".")
    if suffix != source.format:
        raise _manifest_error(
            manifest_path,
            (
                f"source {source.source_id!r} expects format {source.format!r} "
                f"but path resolves to {resolved_path.name!r}"
            ),
        )
    return resolved_path


def _resolve_object_source_location(
    manifest_path: Path,
    landing_zone: LandingZoneSpec,
    source: BatchSourceSpec,
) -> str:
    resolved_location = _resolve_object_uri(landing_zone.base_location, source.path)
    suffix = Path(urlsplit(resolved_location).path).suffix.lower().lstrip(".")
    if suffix != source.format:
        raise _manifest_error(
            manifest_path,
            (
                f"source {source.source_id!r} expects format {source.format!r} "
                f"but path resolves to {_resolved_name_for_location(resolved_location)!r}"
            ),
        )
    return resolved_location


def _validate_source_rows(
    manifest_path: Path,
    source: BatchSourceSpec,
    fieldnames: tuple[str, ...],
    rows: list[dict[str, str]],
) -> None:
    missing_columns = [column for column in source.required_columns if column not in fieldnames]
    if missing_columns:
        raise _manifest_error(
            manifest_path,
            (
                f"source {source.source_id!r} is missing required columns: "
                f"{', '.join(missing_columns)}"
            ),
        )

    observed_source_ids = {
        str(row.get("source_system", "")).strip()
        for row in rows
        if str(row.get("source_system", "")).strip()
    }
    if observed_source_ids and observed_source_ids != {source.source_id}:
        observed = ", ".join(sorted(observed_source_ids))
        raise _manifest_error(
            manifest_path,
            (
                f"source {source.source_id!r} contains source_system values "
                f"that do not match the manifest: {observed}"
            ),
        )


def resolve_batch_manifest(manifest_path: Path) -> ResolvedBatchManifest:
    resolved_manifest_path = manifest_path.resolve()
    manifest = _load_batch_manifest(resolved_manifest_path)

    resolved_sources: list[ResolvedBatchSource] = []
    for source in manifest.sources:
        if manifest.landing_zone.kind == "local_filesystem":
            resolved_path = _resolve_local_source_path(
                resolved_manifest_path,
                manifest.landing_zone,
                source,
            )
            source_reference = str(resolved_path)
            fieldnames = read_dict_fieldnames(resolved_path)
            rows = read_dict_rows(resolved_path)
        else:
            source_reference = _resolve_object_source_location(
                resolved_manifest_path,
                manifest.landing_zone,
                source,
            )
            fieldnames, rows = _load_object_storage_rows(
                source_reference,
                source_format=source.format,
                storage_options=manifest.landing_zone.storage_options,
            )
        _validate_source_rows(resolved_manifest_path, source, fieldnames, rows)
        resolved_sources.append(
            ResolvedBatchSource(
                spec=source,
                source_reference=source_reference,
                fieldnames=fieldnames,
                rows=tuple(rows),
            )
        )

    return ResolvedBatchManifest(
        manifest_path=resolved_manifest_path,
        manifest=manifest,
        sources=tuple(resolved_sources),
    )
