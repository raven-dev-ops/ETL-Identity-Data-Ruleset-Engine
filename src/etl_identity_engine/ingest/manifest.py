"""Production batch manifest parsing and validation."""

from __future__ import annotations

import csv
from io import StringIO
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
import posixpath
import re
from tempfile import TemporaryDirectory
from urllib.parse import urlsplit, urlunsplit

import yaml

from etl_identity_engine.generate.synth_generator import PERSON_HEADERS
from etl_identity_engine.ingest.public_safety_contracts import (
    PUBLIC_SAFETY_CONTRACT_MARKER,
    SUPPORTED_PUBLIC_SAFETY_CONTRACTS,
    PublicSafetyContractValidationError,
    ValidatedPublicSafetyContractBundle,
    validate_public_safety_contract_bundle,
)
from etl_identity_engine.io.read import read_dict_fieldnames, read_dict_rows


SUPPORTED_MANIFEST_SUFFIXES = frozenset({".json", ".yaml", ".yml"})
SUPPORTED_MANIFEST_VERSION = "1.0"
SUPPORTED_ENTITY_TYPE = "person"
SUPPORTED_LANDING_ZONE_KINDS = frozenset({"local_filesystem", "object_storage"})
SUPPORTED_SOURCE_FORMATS = frozenset({"csv", "parquet"})
SUPPORTED_SOURCE_ID_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")
SUPPORTED_SCHEMA_VERSIONS = {"person-v1": PERSON_HEADERS}
SUPPORTED_SOURCE_BUNDLE_CLASSES = frozenset(
    spec.source_system for spec in SUPPORTED_PUBLIC_SAFETY_CONTRACTS.values()
)


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
class BatchSourceBundleSpec:
    bundle_id: str
    source_class: str
    path: str
    contract_name: str
    contract_version: str


@dataclass(frozen=True)
class BatchManifest:
    manifest_version: str
    entity_type: str
    batch_id: str
    landing_zone: LandingZoneSpec
    sources: tuple[BatchSourceSpec, ...]
    source_bundles: tuple[BatchSourceBundleSpec, ...] = ()


@dataclass(frozen=True)
class ResolvedBatchSource:
    spec: BatchSourceSpec
    source_reference: str
    fieldnames: tuple[str, ...]
    rows: tuple[dict[str, str], ...]


@dataclass(frozen=True)
class ResolvedBatchSourceBundleFile:
    logical_name: str
    relative_path: str
    format: str
    fieldnames: tuple[str, ...]
    row_count: int


@dataclass(frozen=True)
class ResolvedBatchSourceBundle:
    spec: BatchSourceBundleSpec
    bundle_reference: str
    contract_name: str
    contract_version: str
    source_system: str
    files: tuple[ResolvedBatchSourceBundleFile, ...]


@dataclass(frozen=True)
class ResolvedBatchManifest:
    manifest_path: Path
    manifest: BatchManifest
    sources: tuple[ResolvedBatchSource, ...]
    source_bundles: tuple[ResolvedBatchSourceBundle, ...] = ()

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


def peek_manifest_batch_id(path: Path) -> str:
    manifest = _load_manifest_mapping(path)
    return _require_non_empty_string(
        manifest,
        "batch_id",
        path=path,
        context="top-level manifest",
    )


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
    payload = _load_object_storage_payload(location, storage_options=storage_options)

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


def _load_object_storage_payload(
    location: str,
    *,
    storage_options: Mapping[str, str | int | float | bool],
) -> bytes:
    try:
        import fsspec
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Object-storage manifest inputs require `fsspec`. Install project dependencies "
            "and any protocol-specific plugin such as `s3fs` for s3:// URIs."
        ) from exc

    try:
        with fsspec.open(location, "rb", **storage_options) as handle:
            return handle.read()
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"Input file not found: {location}") from exc


def _load_batch_manifest(path: Path) -> BatchManifest:
    manifest = _load_manifest_mapping(path)
    _validate_allowed_keys(
        manifest,
        allowed_keys={
            "manifest_version",
            "entity_type",
            "batch_id",
            "landing_zone",
            "sources",
            "source_bundles",
        },
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

    source_bundles_value = manifest.get("source_bundles", [])
    if source_bundles_value is None:
        source_bundles_value = []
    if not isinstance(source_bundles_value, list):
        raise _manifest_error(path, "manifest.source_bundles must be a list when present")

    source_bundles: list[BatchSourceBundleSpec] = []
    seen_bundle_ids: set[str] = set()
    for index, bundle_value in enumerate(source_bundles_value):
        context = f"source_bundles[{index}]"
        if not isinstance(bundle_value, Mapping):
            raise _manifest_error(path, f"{context} must be a mapping")
        _validate_allowed_keys(
            bundle_value,
            allowed_keys={"bundle_id", "source_class", "path", "contract_name", "contract_version"},
            path=path,
            context=context,
        )

        bundle_id = _require_non_empty_string(
            bundle_value,
            "bundle_id",
            path=path,
            context=context,
        )
        if not SUPPORTED_SOURCE_ID_PATTERN.match(bundle_id):
            raise _manifest_error(
                path,
                f"{context}.bundle_id must match {SUPPORTED_SOURCE_ID_PATTERN.pattern!r}",
            )
        if bundle_id in seen_bundle_ids:
            raise _manifest_error(path, f"{context}.bundle_id duplicates {bundle_id!r}")
        seen_bundle_ids.add(bundle_id)

        source_class = _require_non_empty_string(
            bundle_value,
            "source_class",
            path=path,
            context=context,
        ).lower()
        if source_class not in SUPPORTED_SOURCE_BUNDLE_CLASSES:
            raise _manifest_error(
                path,
                f"{context}.source_class must be one of: {', '.join(sorted(SUPPORTED_SOURCE_BUNDLE_CLASSES))}",
            )

        contract_name = _require_non_empty_string(
            bundle_value,
            "contract_name",
            path=path,
            context=context,
        )
        contract_spec = SUPPORTED_PUBLIC_SAFETY_CONTRACTS.get(contract_name)
        if contract_spec is None:
            raise _manifest_error(
                path,
                f"{context}.contract_name must be one of: {', '.join(sorted(SUPPORTED_PUBLIC_SAFETY_CONTRACTS))}",
            )

        contract_version = _require_non_empty_string(
            bundle_value,
            "contract_version",
            path=path,
            context=context,
        )
        if contract_version != contract_spec.contract_version:
            raise _manifest_error(
                path,
                f"{context}.contract_version must be {contract_spec.contract_version!r} for {contract_name!r}",
            )
        if source_class != contract_spec.source_system:
            raise _manifest_error(
                path,
                f"{context}.source_class must be {contract_spec.source_system!r} for {contract_name!r}",
            )

        source_bundles.append(
            BatchSourceBundleSpec(
                bundle_id=bundle_id,
                source_class=source_class,
                path=_require_non_empty_string(
                    bundle_value,
                    "path",
                    path=path,
                    context=context,
                ),
                contract_name=contract_name,
                contract_version=contract_version,
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
        source_bundles=tuple(source_bundles),
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


def _resolve_local_bundle_path(
    manifest_path: Path,
    landing_zone: LandingZoneSpec,
    bundle: BatchSourceBundleSpec,
) -> Path:
    bundle_path = Path(bundle.path)
    if bundle_path.is_absolute():
        return bundle_path
    return _resolve_local_base_path(manifest_path, landing_zone) / bundle_path


def _resolve_object_bundle_location(
    landing_zone: LandingZoneSpec,
    bundle: BatchSourceBundleSpec,
) -> str:
    return _resolve_object_uri(landing_zone.base_location, bundle.path)


def _build_resolved_source_bundle(
    bundle: BatchSourceBundleSpec,
    *,
    bundle_reference: str,
    validated_bundle: ValidatedPublicSafetyContractBundle,
) -> ResolvedBatchSourceBundle:
    return ResolvedBatchSourceBundle(
        spec=bundle,
        bundle_reference=bundle_reference,
        contract_name=validated_bundle.contract_name,
        contract_version=validated_bundle.contract_version,
        source_system=validated_bundle.source_system,
        files=tuple(
            ResolvedBatchSourceBundleFile(
                logical_name=file.logical_name,
                relative_path=str(file.path.relative_to(validated_bundle.bundle_dir)).replace("\\", "/"),
                format=file.format,
                fieldnames=file.fieldnames,
                row_count=file.row_count,
            )
            for file in validated_bundle.files
        ),
    )


def _validate_resolved_source_bundle(
    manifest_path: Path,
    bundle: BatchSourceBundleSpec,
    validated_bundle: ValidatedPublicSafetyContractBundle,
) -> None:
    if validated_bundle.contract_name != bundle.contract_name:
        raise _manifest_error(
            manifest_path,
            (
                f"source_bundle {bundle.bundle_id!r} declares contract_name "
                f"{bundle.contract_name!r} but the bundle marker resolved to "
                f"{validated_bundle.contract_name!r}"
            ),
        )
    if validated_bundle.contract_version != bundle.contract_version:
        raise _manifest_error(
            manifest_path,
            (
                f"source_bundle {bundle.bundle_id!r} declares contract_version "
                f"{bundle.contract_version!r} but the bundle marker resolved to "
                f"{validated_bundle.contract_version!r}"
            ),
        )
    if validated_bundle.source_system != bundle.source_class:
        raise _manifest_error(
            manifest_path,
            (
                f"source_bundle {bundle.bundle_id!r} declares source_class "
                f"{bundle.source_class!r} but the bundle contract resolved to "
                f"{validated_bundle.source_system!r}"
            ),
        )


def _read_object_storage_bundle_marker(
    manifest_path: Path,
    *,
    bundle: BatchSourceBundleSpec,
    bundle_location: str,
    storage_options: Mapping[str, str | int | float | bool],
) -> tuple[bytes, Mapping[str, object]]:
    marker_location = _resolve_object_uri(bundle_location, PUBLIC_SAFETY_CONTRACT_MARKER)
    try:
        marker_payload = _load_object_storage_payload(
            marker_location,
            storage_options=storage_options,
        )
    except FileNotFoundError as exc:
        raise _manifest_error(
            manifest_path,
            f"source_bundle {bundle.bundle_id!r} is missing {PUBLIC_SAFETY_CONTRACT_MARKER}",
        ) from exc

    try:
        marker = yaml.safe_load(marker_payload.decode("utf-8"))
    except UnicodeDecodeError as exc:
        raise _manifest_error(
            manifest_path,
            f"source_bundle {bundle.bundle_id!r} contains a non-UTF-8 contract marker",
        ) from exc

    if not isinstance(marker, Mapping):
        raise _manifest_error(
            manifest_path,
            f"source_bundle {bundle.bundle_id!r} contract marker must contain a mapping",
        )
    return marker_payload, marker


def _materialize_object_storage_bundle(
    manifest_path: Path,
    *,
    bundle: BatchSourceBundleSpec,
    bundle_location: str,
    storage_options: Mapping[str, str | int | float | bool],
) -> ValidatedPublicSafetyContractBundle:
    marker_payload, marker = _read_object_storage_bundle_marker(
        manifest_path,
        bundle=bundle,
        bundle_location=bundle_location,
        storage_options=storage_options,
    )
    file_mapping = marker.get("files")
    if not isinstance(file_mapping, Mapping) or not file_mapping:
        raise _manifest_error(
            manifest_path,
            f"source_bundle {bundle.bundle_id!r} contract marker must contain a non-empty files mapping",
        )

    with TemporaryDirectory(prefix="etl-identity-engine-bundle-") as temp_dir_name:
        staged_bundle_dir = Path(temp_dir_name)
        marker_path = staged_bundle_dir / PUBLIC_SAFETY_CONTRACT_MARKER
        marker_path.write_bytes(marker_payload)

        for logical_name, relative_path_value in file_mapping.items():
            if not isinstance(logical_name, str) or not logical_name.strip():
                raise _manifest_error(
                    manifest_path,
                    f"source_bundle {bundle.bundle_id!r} contract marker contains a non-string file key",
                )
            if not isinstance(relative_path_value, str) or not relative_path_value.strip():
                raise _manifest_error(
                    manifest_path,
                    (
                        f"source_bundle {bundle.bundle_id!r} contract marker file entry "
                        f"{logical_name!r} must be a non-empty string"
                    ),
                )
            relative_path = relative_path_value.strip()
            source_location = _resolve_object_uri(bundle_location, relative_path)
            try:
                payload = _load_object_storage_payload(
                    source_location,
                    storage_options=storage_options,
                )
            except FileNotFoundError as exc:
                raise _manifest_error(
                    manifest_path,
                    (
                        f"source_bundle {bundle.bundle_id!r} is missing declared bundle file "
                        f"{relative_path!r}"
                    ),
                ) from exc
            staged_path = staged_bundle_dir / Path(relative_path)
            staged_path.parent.mkdir(parents=True, exist_ok=True)
            staged_path.write_bytes(payload)

        try:
            return validate_public_safety_contract_bundle(staged_bundle_dir)
        except PublicSafetyContractValidationError as exc:
            raise _manifest_error(
                manifest_path,
                f"source_bundle {bundle.bundle_id!r} failed contract validation: {exc}",
            ) from exc


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

    resolved_source_bundles: list[ResolvedBatchSourceBundle] = []
    for bundle in manifest.source_bundles:
        if manifest.landing_zone.kind == "local_filesystem":
            bundle_path = _resolve_local_bundle_path(
                resolved_manifest_path,
                manifest.landing_zone,
                bundle,
            )
            try:
                validated_bundle = validate_public_safety_contract_bundle(bundle_path)
            except PublicSafetyContractValidationError as exc:
                raise _manifest_error(
                    resolved_manifest_path,
                    f"source_bundle {bundle.bundle_id!r} failed contract validation: {exc}",
                ) from exc
            bundle_reference = str(bundle_path)
        else:
            bundle_reference = _resolve_object_bundle_location(
                manifest.landing_zone,
                bundle,
            )
            validated_bundle = _materialize_object_storage_bundle(
                resolved_manifest_path,
                bundle=bundle,
                bundle_location=bundle_reference,
                storage_options=manifest.landing_zone.storage_options,
            )

        _validate_resolved_source_bundle(
            resolved_manifest_path,
            bundle,
            validated_bundle,
        )
        resolved_source_bundles.append(
            _build_resolved_source_bundle(
                bundle,
                bundle_reference=bundle_reference,
                validated_bundle=validated_bundle,
            )
        )

    return ResolvedBatchManifest(
        manifest_path=resolved_manifest_path,
        manifest=manifest,
        sources=tuple(resolved_sources),
        source_bundles=tuple(resolved_source_bundles),
    )
