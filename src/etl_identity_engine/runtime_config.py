"""Runtime loading for pipeline YAML configuration files."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
import os
from pathlib import Path
import re
from typing import Literal

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ec import EllipticCurvePublicKey
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey
import yaml
from etl_identity_engine.field_authorization import (
    FieldAuthorizationAction,
    FieldAuthorizationConfig,
    SUPPORTED_FIELD_AUTHORIZATION_ACTIONS,
    SUPPORTED_FIELD_AUTHORIZATION_SURFACES,
)
from etl_identity_engine.output_contracts import DELIVERY_CONTRACT_NAME, DELIVERY_CONTRACT_VERSION
from etl_identity_engine.storage.state_store_target import is_state_store_url, resolve_state_store_target


SUPPORTED_BLOCKING_FIELDS = frozenset({"last_initial", "last_name", "dob", "birth_year"})
SUPPORTED_WEIGHT_FIELDS = (
    "canonical_name",
    "canonical_dob",
    "canonical_phone",
    "canonical_address",
)
SUPPORTED_PHONE_OUTPUT_FORMATS = frozenset({"digits_only", "e164"})
SUPPORTED_SURVIVORSHIP_FIELDS = ("first_name", "last_name", "dob", "address", "phone")
SUPPORTED_SURVIVORSHIP_STRATEGIES = frozenset({"source_priority_then_non_null"})
SUPPORTED_EXPORT_JOB_CONSUMERS = frozenset({"warehouse", "data_product"})
SUPPORTED_EXPORT_JOB_FORMATS = frozenset({"csv_snapshot"})
SUPPORTED_SYNTHETIC_PROFILES = frozenset({"small", "medium", "large"})
SUPPORTED_BENCHMARK_FORMATS = frozenset({"csv", "parquet"})
SUPPORTED_BENCHMARK_MODES = frozenset({"batch", "event_stream"})
SUPPORTED_BENCHMARK_STATE_STORE_BACKENDS = frozenset({"sqlite", "postgresql"})
SUPPORTED_SERVICE_SCOPES = frozenset(
    {
        "service:health",
        "service:metrics",
        "runs:read",
        "runs:replay",
        "runs:publish",
        "golden:read",
        "crosswalk:read",
        "public_safety:read",
        "audit_events:read",
        "review_cases:read",
        "review_cases:write",
        "exports:run",
    }
)
DEFAULT_READER_SERVICE_SCOPES = (
    "service:health",
    "service:metrics",
    "runs:read",
    "golden:read",
    "crosswalk:read",
    "public_safety:read",
    "review_cases:read",
)
DEFAULT_OPERATOR_SERVICE_SCOPES = (
    *DEFAULT_READER_SERVICE_SCOPES,
    "audit_events:read",
    "runs:replay",
    "runs:publish",
    "review_cases:write",
    "exports:run",
)
ENV_VAR_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(:-([^}]*))?\}")
SECRET_FILE_ENV_SUFFIX = "_FILE"


class ConfigValidationError(ValueError):
    """Raised when repository runtime config is incomplete or inconsistent."""


@dataclass(frozen=True)
class NameNormalizationConfig:
    trim_whitespace: bool
    remove_punctuation: bool
    uppercase: bool


@dataclass(frozen=True)
class DateNormalizationConfig:
    accepted_formats: tuple[str, ...]
    output_format: str


@dataclass(frozen=True)
class PhoneNormalizationConfig:
    digits_only: bool
    output_format: str
    default_country_code: str


@dataclass(frozen=True)
class NormalizationConfig:
    name: NameNormalizationConfig
    date: DateNormalizationConfig
    phone: PhoneNormalizationConfig


@dataclass(frozen=True)
class BlockingPassConfig:
    name: str
    fields: tuple[str, ...]


@dataclass(frozen=True)
class ThresholdConfig:
    auto_merge: float
    manual_review_min: float
    no_match_max: float


@dataclass(frozen=True)
class MatchingConfig:
    blocking_passes: tuple[BlockingPassConfig, ...]
    weights: dict[str, float]
    thresholds: ThresholdConfig


@dataclass(frozen=True)
class SurvivorshipConfig:
    source_priority: tuple[str, ...]
    field_rules: dict[str, str]


@dataclass(frozen=True)
class PipelineConfig:
    normalization: NormalizationConfig
    matching: MatchingConfig
    survivorship: SurvivorshipConfig


@dataclass(frozen=True)
class RuntimeEnvironmentConfig:
    name: str
    config_dir: Path
    state_db: Path | str | None
    tenant_id: str | None
    secrets: dict[str, str]
    service_auth: ServiceAuthConfig | None
    field_authorization: FieldAuthorizationConfig | None


@dataclass(frozen=True)
class ServiceAuthConfig:
    header_name: str
    reader_api_key: str | None = None
    operator_api_key: str | None = None
    reader_tenant_id: str | None = None
    operator_tenant_id: str | None = None
    mode: Literal["api_key", "jwt"] = "api_key"
    issuer: str | None = None
    audience: str | None = None
    algorithms: tuple[str, ...] = ()
    jwt_secret: str | None = None
    jwt_public_key_pem: str | None = None
    role_claim: str = "roles"
    scope_claim: str = "scope"
    tenant_claim_path: str | None = None
    reader_roles: tuple[str, ...] = ()
    operator_roles: tuple[str, ...] = ()
    reader_scopes: tuple[str, ...] = DEFAULT_READER_SERVICE_SCOPES
    operator_scopes: tuple[str, ...] = DEFAULT_OPERATOR_SERVICE_SCOPES
    subject_claim: str = "sub"


@dataclass(frozen=True)
class EnvPlaceholderResolution:
    env_name: str
    source: Literal["env", "env_file", "default"]
    file_env_name: str | None = None
    file_path: Path | None = None


@dataclass(frozen=True)
class ExportJobConfig:
    name: str
    consumer: str
    description: str
    output_root: Path
    contract_name: str
    contract_version: str
    export_format: str


@dataclass(frozen=True)
class BenchmarkCapacityTargetConfig:
    deployment_name: str
    runtime_environment: str | None
    state_store_backend: str
    max_total_duration_seconds: float
    min_normalize_records_per_second: float
    min_match_candidate_pairs_per_second: float
    max_stream_batch_duration_seconds: float | None = None
    max_p95_stream_batch_duration_seconds: float | None = None
    min_stream_events_per_second: float | None = None


@dataclass(frozen=True)
class BenchmarkFixtureConfig:
    name: str
    description: str
    mode: str
    profile: str
    person_count: int
    duplicate_rate: float
    seed: int
    formats: tuple[str, ...]
    stream_batch_count: int
    stream_events_per_batch: int
    capacity_targets: dict[str, BenchmarkCapacityTargetConfig]


def default_config_dir() -> Path:
    return Path(__file__).resolve().parents[2] / "config"


def default_runtime_config_path() -> Path:
    return default_config_dir() / "runtime_environments.yml"


def _config_error(path: Path, message: str) -> ConfigValidationError:
    return ConfigValidationError(f"{path.name}: {message}")


def _resolve_env_reference(
    env_name: str,
    *,
    default_value: str | None,
    path: Path,
    context: str,
    environ: Mapping[str, str] | None = None,
) -> tuple[str, EnvPlaceholderResolution]:
    effective_environ = os.environ if environ is None else environ
    resolved = effective_environ.get(env_name)
    if resolved is not None:
        return resolved, EnvPlaceholderResolution(env_name=env_name, source="env")

    file_env_name = f"{env_name}{SECRET_FILE_ENV_SUFFIX}"
    file_reference = effective_environ.get(file_env_name)
    if file_reference:
        file_path = Path(file_reference).expanduser()
        if not file_path.exists():
            raise _config_error(
                path,
                f"{context} references secret-file environment variable {file_env_name} "
                f"but the configured path does not exist: {file_path}",
            )
        if not file_path.is_file():
            raise _config_error(
                path,
                f"{context} references secret-file environment variable {file_env_name} "
                f"but the configured path is not a file: {file_path}",
            )
        file_value = file_path.read_text(encoding="utf-8").strip()
        if not file_value:
            raise _config_error(
                path,
                f"{context} references secret-file environment variable {file_env_name} "
                f"but the file is empty: {file_path}",
            )
        return file_value, EnvPlaceholderResolution(
            env_name=env_name,
            source="env_file",
            file_env_name=file_env_name,
            file_path=file_path.resolve(),
        )

    if default_value is not None:
        return default_value, EnvPlaceholderResolution(env_name=env_name, source="default")

    raise _config_error(
        path,
        f"{context} references required environment variable {env_name}",
    )


def _resolve_env_placeholders(
    value: str,
    *,
    path: Path,
    context: str,
    environ: Mapping[str, str] | None = None,
    resolutions: list[EnvPlaceholderResolution] | None = None,
) -> str:
    def replacer(match: re.Match[str]) -> str:
        env_name = match.group(1)
        default_value = match.group(3)
        resolved, resolution = _resolve_env_reference(
            env_name,
            default_value=default_value,
            path=path,
            context=context,
            environ=environ,
        )
        if resolutions is not None:
            resolutions.append(resolution)
        return resolved

    return ENV_VAR_PATTERN.sub(replacer, value)


def _resolve_node_env_placeholders(
    value: object,
    *,
    path: Path,
    context: str,
    environ: Mapping[str, str] | None = None,
) -> object:
    if isinstance(value, str):
        return _resolve_env_placeholders(value, path=path, context=context, environ=environ)
    if isinstance(value, list):
        return [
            _resolve_node_env_placeholders(item, path=path, context=context, environ=environ)
            for item in value
        ]
    if isinstance(value, Mapping):
        return {
            key: _resolve_node_env_placeholders(item, path=path, context=context, environ=environ)
            for key, item in value.items()
        }
    return value


def _load_yaml(
    path: Path,
    *,
    allow_missing: bool = False,
    resolve_env: bool = True,
) -> dict[str, object]:
    if not path.exists():
        if allow_missing:
            return {}
        raise FileNotFoundError(f"Configuration file not found: {path}")

    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if data is None:
        if allow_missing:
            return {}
        raise _config_error(path, "configuration file must contain a mapping")
    if not isinstance(data, dict):
        raise _config_error(path, "configuration file must contain a mapping")
    if not resolve_env:
        return data

    resolved = _resolve_node_env_placeholders(data, path=path, context="configuration")
    if not isinstance(resolved, dict):
        raise _config_error(path, "configuration file must contain a mapping")
    return resolved


def _merge_mappings(
    base: Mapping[str, object],
    overlay: Mapping[str, object],
) -> dict[str, object]:
    merged = dict(base)
    for key, value in overlay.items():
        existing = merged.get(key)
        if isinstance(existing, Mapping) and isinstance(value, Mapping):
            merged[key] = _merge_mappings(existing, value)
        else:
            merged[key] = value
    return merged


def _load_pipeline_yaml(
    root: Path,
    file_name: str,
    *,
    environment: str | None,
) -> dict[str, object]:
    base_rules = _load_yaml(root / file_name)
    if not environment:
        return base_rules

    overlay_rules = _load_yaml(
        root / "environments" / environment / file_name,
        allow_missing=True,
    )
    return _merge_mappings(base_rules, overlay_rules)


def _validate_allowed_keys(
    mapping: Mapping[str, object],
    *,
    allowed_keys: set[str],
    path: Path,
    context: str,
) -> None:
    unexpected_keys = sorted(set(mapping) - allowed_keys)
    if unexpected_keys:
        raise _config_error(
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
        raise _config_error(path, f"{context}.{key} must be a mapping")
    return value


def _require_bool(
    mapping: Mapping[str, object],
    key: str,
    *,
    path: Path,
    context: str,
) -> bool:
    value = mapping.get(key)
    if not isinstance(value, bool):
        raise _config_error(path, f"{context}.{key} must be a boolean")
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
        raise _config_error(path, f"{context}.{key} must be a non-empty string")
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
        raise _config_error(path, f"{context}.{key} must be a non-empty list of strings")

    items: list[str] = []
    for index, item in enumerate(value):
        if not isinstance(item, str) or not item.strip():
            raise _config_error(
                path,
                f"{context}.{key}[{index}] must be a non-empty string",
            )
        items.append(item.strip())
    return tuple(items)


def _require_float(
    mapping: Mapping[str, object],
    key: str,
    *,
    path: Path,
    context: str,
) -> float:
    value = mapping.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _config_error(path, f"{context}.{key} must be a number")
    return float(value)


def _optional_non_empty_string(
    mapping: Mapping[str, object],
    key: str,
    *,
    path: Path,
    context: str,
    default: str,
) -> str:
    value = mapping.get(key, default)
    if not isinstance(value, str) or not value.strip():
        raise _config_error(path, f"{context}.{key} must be a non-empty string")
    return value.strip()


def _require_positive_int_if_present(
    mapping: Mapping[str, object],
    key: str,
    *,
    path: Path,
    context: str,
    default: int,
) -> int:
    if key not in mapping:
        return default
    value = mapping.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise _config_error(path, f"{context}.{key} must be an integer greater than 0")
    return value


def _optional_string_list(
    mapping: Mapping[str, object],
    key: str,
    *,
    path: Path,
    context: str,
    default: tuple[str, ...],
) -> tuple[str, ...]:
    if key not in mapping:
        return default
    return _require_string_list(mapping, key, path=path, context=context)


def _optional_float_if_present(
    mapping: Mapping[str, object],
    key: str,
    *,
    path: Path,
    context: str,
) -> float | None:
    if key not in mapping:
        return None
    return _require_float(mapping, key, path=path, context=context)


def _detect_service_auth_mode(
    raw_service_auth: Mapping[str, object],
    *,
    config_path: Path,
    context: str,
) -> Literal["api_key", "jwt"]:
    raw_mode = str(raw_service_auth.get("mode", "") or "").strip()
    if not raw_mode:
        return (
            "jwt"
            if {
                "issuer",
                "audience",
                "algorithms",
                "jwt_secret",
                "jwt_public_key_pem",
                "role_claim",
                "reader_roles",
                "operator_roles",
                "subject_claim",
            }
            & set(raw_service_auth)
            else "api_key"
        )
    if raw_mode in {"api_key", "jwt"}:
        return raw_mode
    raise _config_error(
        config_path,
        f"{context}.mode must be one of: api_key, jwt",
    )


def _file_age_hours(file_path: Path) -> float:
    modified = datetime.fromtimestamp(file_path.stat().st_mtime, tz=timezone.utc)
    return (datetime.now(timezone.utc) - modified).total_seconds() / 3600.0


def _file_last_modified_utc(file_path: Path) -> str:
    modified = datetime.fromtimestamp(file_path.stat().st_mtime, tz=timezone.utc)
    return modified.isoformat().replace("+00:00", "Z")


def _add_auth_material_check(
    checks: list[dict[str, object]],
    errors: list[str],
    *,
    check_name: str,
    raw_value: object,
    config_path: Path,
    context: str,
    environ: Mapping[str, str],
    max_secret_file_age_hours: float | None,
) -> str | None:
    if not isinstance(raw_value, str) or not raw_value.strip():
        checks.append({"check": check_name, "status": "error", "detail": "missing"})
        errors.append(f"{config_path.name}: {context} must be a non-empty string")
        return None

    placeholder_resolutions: list[EnvPlaceholderResolution] = []
    try:
        resolved = _resolve_env_placeholders(
            raw_value,
            path=config_path,
            context=context,
            environ=environ,
            resolutions=placeholder_resolutions,
        ).strip()
    except ConfigValidationError as exc:
        checks.append({"check": check_name, "status": "error", "detail": "unresolved"})
        errors.append(str(exc))
        return None

    if not resolved:
        checks.append({"check": check_name, "status": "error", "detail": "missing"})
        errors.append(f"{config_path.name}: {context} resolved to an empty value")
        return None

    check: dict[str, object] = {
        "check": check_name,
        "status": "ok",
        "detail": "configured",
    }
    if not placeholder_resolutions:
        check["source"] = "literal"
        checks.append(check)
        return resolved

    if len(placeholder_resolutions) == 1:
        resolution = placeholder_resolutions[0]
        check["source"] = resolution.source
        check["env_name"] = resolution.env_name
        if resolution.source == "env_file" and resolution.file_env_name and resolution.file_path is not None:
            check["file_env_name"] = resolution.file_env_name
            check["file_path"] = str(resolution.file_path)
            check["file_last_modified_utc"] = _file_last_modified_utc(resolution.file_path)
            file_age_hours = _file_age_hours(resolution.file_path)
            check["file_age_hours"] = file_age_hours
            if (
                max_secret_file_age_hours is not None
                and file_age_hours > max_secret_file_age_hours
            ):
                check["status"] = "error"
                check["detail"] = (
                    f"secret file age {file_age_hours:.3f}h exceeds "
                    f"max {max_secret_file_age_hours:.3f}h"
                )
                errors.append(
                    f"{config_path.name}: {context} secret file age {file_age_hours:.3f}h exceeds "
                    f"max {max_secret_file_age_hours:.3f}h"
                )
        checks.append(check)
        return resolved

    check["source"] = "mixed"
    check["env_names"] = [resolution.env_name for resolution in placeholder_resolutions]
    checks.append(check)
    return resolved


def evaluate_runtime_auth_material(
    environment_name: str,
    runtime_config_path: Path | None = None,
    *,
    environ: Mapping[str, str] | None = None,
    include_declared_secrets: bool = True,
    max_secret_file_age_hours: float | None = None,
) -> dict[str, object]:
    config_path = runtime_config_path or default_runtime_config_path()
    effective_environ = dict(os.environ if environ is None else environ)
    raw_config = _load_yaml(config_path, resolve_env=False)
    environments = _require_mapping(
        raw_config,
        "environments",
        path=config_path,
        context="runtime_config",
    )
    selected = environments.get(environment_name)
    if not isinstance(selected, Mapping):
        raise _config_error(
            config_path,
            f"environments must define a mapping for '{environment_name}'",
        )

    checks: list[dict[str, object]] = []
    errors: list[str] = []

    try:
        environment = load_runtime_environment(
            environment_name,
            config_path,
            environ=effective_environ,
        )
    except (ConfigValidationError, FileNotFoundError, ValueError) as exc:
        environment = None
        errors.append(str(exc))

    raw_service_auth = selected.get("service_auth")
    if raw_service_auth in (None, {}):
        checks.append({"check": "service_auth", "status": "error", "detail": "missing"})
        errors.append(f"{config_path.name}: environments.{environment_name}.service_auth is required")
    elif not isinstance(raw_service_auth, Mapping):
        checks.append({"check": "service_auth", "status": "error", "detail": "invalid"})
        errors.append(f"{config_path.name}: environments.{environment_name}.service_auth must be a mapping")
    else:
        context = f"environments.{environment_name}.service_auth"
        try:
            mode = _detect_service_auth_mode(
                raw_service_auth,
                config_path=config_path,
                context=context,
            )
            checks.append({"check": "service_auth.mode", "status": "ok", "detail": mode})
        except ConfigValidationError as exc:
            mode = None
            checks.append({"check": "service_auth.mode", "status": "error", "detail": "invalid"})
            errors.append(str(exc))

        if mode == "api_key":
            _add_auth_material_check(
                checks,
                errors,
                check_name="service_auth.reader_api_key",
                raw_value=raw_service_auth.get("reader_api_key"),
                config_path=config_path,
                context=f"{context}.reader_api_key",
                environ=effective_environ,
                max_secret_file_age_hours=max_secret_file_age_hours,
            )
            _add_auth_material_check(
                checks,
                errors,
                check_name="service_auth.operator_api_key",
                raw_value=raw_service_auth.get("operator_api_key"),
                config_path=config_path,
                context=f"{context}.operator_api_key",
                environ=effective_environ,
                max_secret_file_age_hours=max_secret_file_age_hours,
            )
        elif mode == "jwt":
            _add_auth_material_check(
                checks,
                errors,
                check_name="service_auth.issuer",
                raw_value=raw_service_auth.get("issuer"),
                config_path=config_path,
                context=f"{context}.issuer",
                environ=effective_environ,
                max_secret_file_age_hours=max_secret_file_age_hours,
            )
            _add_auth_material_check(
                checks,
                errors,
                check_name="service_auth.audience",
                raw_value=raw_service_auth.get("audience"),
                config_path=config_path,
                context=f"{context}.audience",
                environ=effective_environ,
                max_secret_file_age_hours=max_secret_file_age_hours,
            )

            jwt_secret = _add_auth_material_check(
                checks,
                errors,
                check_name="service_auth.jwt_secret",
                raw_value=raw_service_auth.get("jwt_secret"),
                config_path=config_path,
                context=f"{context}.jwt_secret",
                environ=effective_environ,
                max_secret_file_age_hours=max_secret_file_age_hours,
            ) if raw_service_auth.get("jwt_secret") not in (None, "") else None

            jwt_public_key_pem = _add_auth_material_check(
                checks,
                errors,
                check_name="service_auth.jwt_public_key_pem",
                raw_value=raw_service_auth.get("jwt_public_key_pem"),
                config_path=config_path,
                context=f"{context}.jwt_public_key_pem",
                environ=effective_environ,
                max_secret_file_age_hours=max_secret_file_age_hours,
            ) if raw_service_auth.get("jwt_public_key_pem") not in (None, "") else None

            if jwt_secret and jwt_public_key_pem:
                errors.append(
                    f"{config_path.name}: {context} must define exactly one of jwt_secret or jwt_public_key_pem"
                )
            if not jwt_secret and not jwt_public_key_pem:
                errors.append(
                    f"{config_path.name}: {context} must define exactly one of jwt_secret or jwt_public_key_pem"
                )

            if jwt_public_key_pem:
                try:
                    public_key = serialization.load_pem_public_key(jwt_public_key_pem.encode("utf-8"))
                except (TypeError, ValueError) as exc:
                    checks.append(
                        {
                            "check": "service_auth.jwt_public_key_format",
                            "status": "error",
                            "detail": "invalid_pem",
                        }
                    )
                    errors.append(
                        f"{config_path.name}: {context}.jwt_public_key_pem must resolve to a PEM-encoded public key ({exc})"
                    )
                else:
                    algorithms = ()
                    if environment is not None and environment.service_auth is not None:
                        algorithms = environment.service_auth.algorithms
                    elif isinstance(raw_service_auth.get("algorithms"), list):
                        algorithms = tuple(
                            str(item).strip()
                            for item in raw_service_auth.get("algorithms", [])
                            if str(item).strip()
                        )
                    if any(algorithm.startswith(("RS", "PS")) for algorithm in algorithms):
                        valid_key_type = isinstance(public_key, RSAPublicKey)
                        key_family = "rsa"
                    elif any(algorithm.startswith("ES") for algorithm in algorithms):
                        valid_key_type = isinstance(public_key, EllipticCurvePublicKey)
                        key_family = "ec"
                    elif any(algorithm == "EdDSA" for algorithm in algorithms):
                        valid_key_type = isinstance(public_key, Ed25519PublicKey)
                        key_family = "eddsa"
                    else:
                        valid_key_type = True
                        key_family = type(public_key).__name__
                    checks.append(
                        {
                            "check": "service_auth.jwt_public_key_format",
                            "status": "ok" if valid_key_type else "error",
                            "detail": key_family,
                        }
                    )
                    if not valid_key_type:
                        errors.append(
                            f"{config_path.name}: {context}.jwt_public_key_pem must resolve to a {key_family} public key "
                            f"for algorithms {', '.join(algorithms)}"
                        )

    if include_declared_secrets:
        raw_secrets = selected.get("secrets", {})
        if raw_secrets in (None, {}):
            pass
        elif not isinstance(raw_secrets, Mapping):
            checks.append({"check": "runtime_secrets", "status": "error", "detail": "invalid"})
            errors.append(f"{config_path.name}: environments.{environment_name}.secrets must be a mapping")
        else:
            for secret_name, secret_value in raw_secrets.items():
                _add_auth_material_check(
                    checks,
                    errors,
                    check_name=f"secret:{secret_name}",
                    raw_value=secret_value,
                    config_path=config_path,
                    context=f"environments.{environment_name}.secrets.{secret_name}",
                    environ=effective_environ,
                    max_secret_file_age_hours=max_secret_file_age_hours,
                )

    deduplicated_errors = list(dict.fromkeys(errors))
    return {
        "environment": environment_name,
        "runtime_config_path": str(config_path.resolve()),
        "max_secret_file_age_hours": max_secret_file_age_hours,
        "status": "ok" if not deduplicated_errors else "error",
        "checks": checks,
        "errors": deduplicated_errors,
    }


def _load_service_auth_config(
    raw_service_auth: object,
    *,
    config_path: Path,
    environment_name: str,
) -> ServiceAuthConfig | None:
    context = f"environments.{environment_name}.service_auth"
    if raw_service_auth in (None, {}):
        return None
    if not isinstance(raw_service_auth, Mapping):
        raise _config_error(config_path, f"{context} must be a mapping")

    mode = _detect_service_auth_mode(raw_service_auth, config_path=config_path, context=context)

    if mode == "api_key":
        _validate_allowed_keys(
            raw_service_auth,
            allowed_keys={
                "mode",
                "header_name",
                "reader_api_key",
                "operator_api_key",
                "reader_tenant_id",
                "operator_tenant_id",
                "reader_scopes",
                "operator_scopes",
            },
            path=config_path,
            context=context,
        )
        header_name = _optional_non_empty_string(
            raw_service_auth,
            "header_name",
            path=config_path,
            context=context,
            default="X-API-Key",
        )
        reader_scopes = _optional_string_list(
            raw_service_auth,
            "reader_scopes",
            path=config_path,
            context=context,
            default=DEFAULT_READER_SERVICE_SCOPES,
        )
        operator_scopes = _optional_string_list(
            raw_service_auth,
            "operator_scopes",
            path=config_path,
            context=context,
            default=DEFAULT_OPERATOR_SERVICE_SCOPES,
        )
        reader_api_key = str(raw_service_auth.get("reader_api_key", "") or "").strip()
        operator_api_key = str(raw_service_auth.get("operator_api_key", "") or "").strip()
        if not reader_api_key and not operator_api_key:
            return None
        if not reader_api_key or not operator_api_key:
            raise _config_error(
                config_path,
                f"{context} must define both reader_api_key and operator_api_key",
            )
        reader_tenant_id = _require_non_empty_string(
            raw_service_auth,
            "reader_tenant_id",
            path=config_path,
            context=context,
        )
        operator_tenant_id = _require_non_empty_string(
            raw_service_auth,
            "operator_tenant_id",
            path=config_path,
            context=context,
        )
        if reader_api_key == operator_api_key:
            raise _config_error(
                config_path,
                f"{context} must use distinct API keys for reader and operator access",
            )
        invalid_reader_scopes = sorted(set(reader_scopes) - SUPPORTED_SERVICE_SCOPES)
        invalid_operator_scopes = sorted(set(operator_scopes) - SUPPORTED_SERVICE_SCOPES)
        if invalid_reader_scopes:
            raise _config_error(
                config_path,
                f"{context}.reader_scopes contains unsupported values: "
                f"{', '.join(invalid_reader_scopes)}",
            )
        if invalid_operator_scopes:
            raise _config_error(
                config_path,
                f"{context}.operator_scopes contains unsupported values: "
                f"{', '.join(invalid_operator_scopes)}",
            )
        missing_reader_scope_coverage = sorted(set(reader_scopes) - set(operator_scopes))
        if missing_reader_scope_coverage:
            raise _config_error(
                config_path,
                f"{context}.operator_scopes must include all reader_scopes "
                f"(missing: {', '.join(missing_reader_scope_coverage)})",
            )
        return ServiceAuthConfig(
            header_name=header_name,
            reader_api_key=reader_api_key,
            operator_api_key=operator_api_key,
            reader_tenant_id=reader_tenant_id,
            operator_tenant_id=operator_tenant_id,
            mode="api_key",
            reader_scopes=reader_scopes,
            operator_scopes=operator_scopes,
        )

    _validate_allowed_keys(
        raw_service_auth,
        allowed_keys={
            "mode",
            "header_name",
            "issuer",
            "audience",
            "algorithms",
            "jwt_secret",
            "jwt_public_key_pem",
            "role_claim",
            "scope_claim",
            "tenant_claim_path",
            "reader_roles",
            "operator_roles",
            "reader_scopes",
            "operator_scopes",
            "subject_claim",
        },
        path=config_path,
        context=context,
    )
    header_name = _optional_non_empty_string(
        raw_service_auth,
        "header_name",
        path=config_path,
        context=context,
        default="Authorization",
    )
    issuer = _require_non_empty_string(
        raw_service_auth,
        "issuer",
        path=config_path,
        context=context,
    )
    audience = _require_non_empty_string(
        raw_service_auth,
        "audience",
        path=config_path,
        context=context,
    )
    algorithms = _require_string_list(
        raw_service_auth,
        "algorithms",
        path=config_path,
        context=context,
    )
    jwt_secret = str(raw_service_auth.get("jwt_secret", "") or "").strip()
    jwt_public_key_pem = str(raw_service_auth.get("jwt_public_key_pem", "") or "").strip()
    if bool(jwt_secret) == bool(jwt_public_key_pem):
        raise _config_error(
            config_path,
            f"{context} must define exactly one of jwt_secret or jwt_public_key_pem",
        )
    role_claim = _optional_non_empty_string(
        raw_service_auth,
        "role_claim",
        path=config_path,
        context=context,
        default="roles",
    )
    scope_claim = _optional_non_empty_string(
        raw_service_auth,
        "scope_claim",
        path=config_path,
        context=context,
        default="scope",
    )
    tenant_claim_path = _require_non_empty_string(
        raw_service_auth,
        "tenant_claim_path",
        path=config_path,
        context=context,
    )
    subject_claim = _optional_non_empty_string(
        raw_service_auth,
        "subject_claim",
        path=config_path,
        context=context,
        default="sub",
    )
    reader_roles = _require_string_list(
        raw_service_auth,
        "reader_roles",
        path=config_path,
        context=context,
    )
    operator_roles = _require_string_list(
        raw_service_auth,
        "operator_roles",
        path=config_path,
        context=context,
    )
    reader_scopes = _optional_string_list(
        raw_service_auth,
        "reader_scopes",
        path=config_path,
        context=context,
        default=DEFAULT_READER_SERVICE_SCOPES,
    )
    operator_scopes = _optional_string_list(
        raw_service_auth,
        "operator_scopes",
        path=config_path,
        context=context,
        default=DEFAULT_OPERATOR_SERVICE_SCOPES,
    )
    overlapping_roles = sorted(set(reader_roles) & set(operator_roles))
    if overlapping_roles:
        raise _config_error(
            config_path,
            f"{context} must use distinct reader_roles and operator_roles "
            f"(overlap: {', '.join(overlapping_roles)})",
        )
    invalid_reader_scopes = sorted(set(reader_scopes) - SUPPORTED_SERVICE_SCOPES)
    invalid_operator_scopes = sorted(set(operator_scopes) - SUPPORTED_SERVICE_SCOPES)
    if invalid_reader_scopes:
        raise _config_error(
            config_path,
            f"{context}.reader_scopes contains unsupported values: "
            f"{', '.join(invalid_reader_scopes)}",
        )
    if invalid_operator_scopes:
        raise _config_error(
            config_path,
            f"{context}.operator_scopes contains unsupported values: "
            f"{', '.join(invalid_operator_scopes)}",
        )
    missing_reader_scope_coverage = sorted(set(reader_scopes) - set(operator_scopes))
    if missing_reader_scope_coverage:
        raise _config_error(
            config_path,
            f"{context}.operator_scopes must include all reader_scopes "
            f"(missing: {', '.join(missing_reader_scope_coverage)})",
        )

    return ServiceAuthConfig(
        header_name=header_name,
        mode="jwt",
        issuer=issuer,
        audience=audience,
        algorithms=algorithms,
        jwt_secret=jwt_secret or None,
        jwt_public_key_pem=jwt_public_key_pem or None,
        role_claim=role_claim,
        scope_claim=scope_claim,
        tenant_claim_path=tenant_claim_path,
        reader_roles=reader_roles,
        operator_roles=operator_roles,
        reader_scopes=reader_scopes,
        operator_scopes=operator_scopes,
        subject_claim=subject_claim,
    )


def _load_field_authorization_config(
    raw_field_authorization: object,
    *,
    config_path: Path,
    environment_name: str,
) -> FieldAuthorizationConfig | None:
    context = f"environments.{environment_name}.field_authorization"
    if raw_field_authorization in (None, {}):
        return None
    if not isinstance(raw_field_authorization, Mapping):
        raise _config_error(config_path, f"{context} must be a mapping")

    surface_rules: dict[str, dict[str, FieldAuthorizationAction]] = {}
    for surface_name, raw_rules in raw_field_authorization.items():
        if not isinstance(surface_name, str) or not surface_name.strip():
            raise _config_error(config_path, f"{context} must use non-empty string surface names")
        normalized_surface_name = surface_name.strip()
        supported_fields = SUPPORTED_FIELD_AUTHORIZATION_SURFACES.get(normalized_surface_name)
        if supported_fields is None:
            raise _config_error(
                config_path,
                f"{context}.{normalized_surface_name} is unsupported; expected one of "
                f"{', '.join(sorted(SUPPORTED_FIELD_AUTHORIZATION_SURFACES))}",
            )
        if not isinstance(raw_rules, Mapping):
            raise _config_error(config_path, f"{context}.{normalized_surface_name} must be a mapping")
        _validate_allowed_keys(
            raw_rules,
            allowed_keys=set(supported_fields),
            path=config_path,
            context=f"{context}.{normalized_surface_name}",
        )
        resolved_rules: dict[str, FieldAuthorizationAction] = {}
        for field_name, raw_action in raw_rules.items():
            normalized_action = str(raw_action or "").strip().lower()
            if normalized_action not in SUPPORTED_FIELD_AUTHORIZATION_ACTIONS:
                raise _config_error(
                    config_path,
                    f"{context}.{normalized_surface_name}.{field_name} must be one of: "
                    f"{', '.join(sorted(SUPPORTED_FIELD_AUTHORIZATION_ACTIONS))}",
                )
            resolved_rules[str(field_name)] = normalized_action
        surface_rules[normalized_surface_name] = resolved_rules

    if not surface_rules:
        return None
    return FieldAuthorizationConfig(surface_rules=surface_rules)


def _resolve_runtime_environment_name(
    raw_config: Mapping[str, object],
    *,
    requested_environment: str | None,
    config_path: Path,
    environ: Mapping[str, str],
) -> str:
    selected_environment = requested_environment or environ.get("ETL_IDENTITY_ENV")
    if selected_environment is not None:
        return selected_environment

    raw_default_environment = raw_config.get("default_environment")
    if raw_default_environment in (None, ""):
        return "dev"
    if not isinstance(raw_default_environment, str):
        raise _config_error(config_path, "runtime_config.default_environment must be a non-empty string")

    resolved_default_environment = _resolve_node_env_placeholders(
        raw_default_environment,
        path=config_path,
        context="runtime_config.default_environment",
        environ=environ,
    )
    if not isinstance(resolved_default_environment, str) or not resolved_default_environment.strip():
        raise _config_error(config_path, "runtime_config.default_environment must be a non-empty string")
    return resolved_default_environment.strip()


def _load_resolved_runtime_environment_config(
    raw_config: Mapping[str, object],
    *,
    environment_name: str,
    config_path: Path,
    environ: Mapping[str, str],
) -> Mapping[str, object]:
    environments = _require_mapping(
        raw_config,
        "environments",
        path=config_path,
        context="runtime_config",
    )
    selected = environments.get(environment_name)
    if not isinstance(selected, Mapping):
        raise _config_error(
            config_path,
            f"environments must define a mapping for '{environment_name}'",
        )

    resolved_selected = _resolve_node_env_placeholders(
        dict(selected),
        path=config_path,
        context=f"environments.{environment_name}",
        environ=environ,
    )
    if not isinstance(resolved_selected, Mapping):
        raise _config_error(
            config_path,
            f"environments.{environment_name} must be a mapping",
        )
    _validate_allowed_keys(
        resolved_selected,
        allowed_keys={
            "description",
            "config_dir",
            "state_db",
            "tenant_id",
            "secrets",
            "service_auth",
            "field_authorization",
        },
        path=config_path,
        context=f"environments.{environment_name}",
    )
    return resolved_selected


def _resolve_runtime_state_db(
    raw_state_db: object,
    *,
    config_path: Path,
    environment_name: str,
) -> Path | str | None:
    if raw_state_db in (None, ""):
        return None
    if not isinstance(raw_state_db, str) or not raw_state_db.strip():
        raise _config_error(
            config_path,
            f"environments.{environment_name}.state_db must be a non-empty string when provided",
        )

    configured_state_db = raw_state_db.strip()
    if is_state_store_url(configured_state_db):
        return resolve_state_store_target(configured_state_db).raw_value

    resolved_state_db_path = Path(configured_state_db)
    if not resolved_state_db_path.is_absolute():
        resolved_state_db_path = (config_path.parent / resolved_state_db_path).resolve()
    return resolve_state_store_target(resolved_state_db_path).file_path


def _resolve_runtime_secrets(
    raw_secrets: object,
    *,
    config_path: Path,
    environment_name: str,
) -> dict[str, str]:
    if not isinstance(raw_secrets, Mapping):
        raise _config_error(
            config_path,
            f"environments.{environment_name}.secrets must be a mapping",
        )

    secrets: dict[str, str] = {}
    for key, value in raw_secrets.items():
        if not isinstance(key, str) or not key.strip():
            raise _config_error(
                config_path,
                f"environments.{environment_name}.secrets contains an invalid key",
            )
        if not isinstance(value, str) or not value.strip():
            raise _config_error(
                config_path,
                f"environments.{environment_name}.secrets.{key} must be a non-empty string",
            )
        secrets[key.strip()] = value.strip()
    return secrets


def _resolve_runtime_tenant_id(
    raw_tenant_id: object,
    *,
    config_path: Path,
    environment_name: str,
) -> str | None:
    if raw_tenant_id in (None, ""):
        return None
    if isinstance(raw_tenant_id, str) and raw_tenant_id.strip():
        return raw_tenant_id.strip()
    raise _config_error(
        config_path,
        f"environments.{environment_name}.tenant_id must be a non-empty string when provided",
    )


def load_runtime_environment(
    environment: str | None = None,
    runtime_config_path: Path | None = None,
    *,
    environ: Mapping[str, str] | None = None,
) -> RuntimeEnvironmentConfig:
    config_path = runtime_config_path or default_runtime_config_path()
    raw_config = _load_yaml(config_path, resolve_env=False)
    _validate_allowed_keys(
        raw_config,
        allowed_keys={"default_environment", "environments"},
        path=config_path,
        context="top-level runtime config",
    )

    effective_environ = os.environ if environ is None else environ
    default_environment = _resolve_runtime_environment_name(
        raw_config,
        requested_environment=environment,
        config_path=config_path,
        environ=effective_environ,
    )
    resolved_selected = _load_resolved_runtime_environment_config(
        raw_config,
        environment_name=default_environment,
        config_path=config_path,
        environ=effective_environ,
    )

    config_dir_value = _optional_non_empty_string(
        resolved_selected,
        "config_dir",
        path=config_path,
        context=f"environments.{default_environment}",
        default=".",
    )
    config_dir = Path(config_dir_value)
    if not config_dir.is_absolute():
        config_dir = (config_path.parent / config_dir).resolve()

    state_db = _resolve_runtime_state_db(
        resolved_selected.get("state_db"),
        config_path=config_path,
        environment_name=default_environment,
    )
    secrets = _resolve_runtime_secrets(
        resolved_selected.get("secrets", {}),
        config_path=config_path,
        environment_name=default_environment,
    )
    raw_service_auth = resolved_selected.get("service_auth")
    service_auth = _load_service_auth_config(
        raw_service_auth,
        config_path=config_path,
        environment_name=default_environment,
    )
    raw_field_authorization = resolved_selected.get("field_authorization")
    field_authorization = _load_field_authorization_config(
        raw_field_authorization,
        config_path=config_path,
        environment_name=default_environment,
    )
    tenant_id = _resolve_runtime_tenant_id(
        resolved_selected.get("tenant_id"),
        config_path=config_path,
        environment_name=default_environment,
    )

    return RuntimeEnvironmentConfig(
        name=default_environment,
        config_dir=config_dir,
        state_db=state_db,
        tenant_id=tenant_id,
        secrets=secrets,
        service_auth=service_auth,
        field_authorization=field_authorization,
    )


def load_pipeline_config(
    config_dir: Path | None = None,
    *,
    environment: str | None = None,
) -> PipelineConfig:
    root = config_dir or default_config_dir()

    normalization_path = root / "normalization_rules.yml"
    blocking_path = root / "blocking_rules.yml"
    matching_path = root / "matching_rules.yml"
    thresholds_path = root / "thresholds.yml"
    survivorship_path = root / "survivorship_rules.yml"

    normalization_rules = _load_pipeline_yaml(root, "normalization_rules.yml", environment=environment)
    blocking_rules = _load_pipeline_yaml(root, "blocking_rules.yml", environment=environment)
    matching_rules = _load_pipeline_yaml(root, "matching_rules.yml", environment=environment)
    thresholds_rules = _load_pipeline_yaml(root, "thresholds.yml", environment=environment)
    survivorship_rules = _load_pipeline_yaml(root, "survivorship_rules.yml", environment=environment)

    _validate_allowed_keys(
        normalization_rules,
        allowed_keys={"name_normalization", "date_normalization", "phone_normalization"},
        path=normalization_path,
        context="top-level config",
    )
    name_rules = _require_mapping(
        normalization_rules,
        "name_normalization",
        path=normalization_path,
        context="normalization_rules",
    )
    _validate_allowed_keys(
        name_rules,
        allowed_keys={"trim_whitespace", "remove_punctuation", "uppercase"},
        path=normalization_path,
        context="name_normalization",
    )
    date_rules = _require_mapping(
        normalization_rules,
        "date_normalization",
        path=normalization_path,
        context="normalization_rules",
    )
    _validate_allowed_keys(
        date_rules,
        allowed_keys={"accepted_formats", "output_format"},
        path=normalization_path,
        context="date_normalization",
    )
    phone_rules = _require_mapping(
        normalization_rules,
        "phone_normalization",
        path=normalization_path,
        context="normalization_rules",
    )
    _validate_allowed_keys(
        phone_rules,
        allowed_keys={"digits_only", "output_format", "default_country_code"},
        path=normalization_path,
        context="phone_normalization",
    )

    _validate_allowed_keys(
        blocking_rules,
        allowed_keys={"blocking_passes"},
        path=blocking_path,
        context="top-level config",
    )
    blocking_pass_items = blocking_rules.get("blocking_passes")
    if not isinstance(blocking_pass_items, list) or not blocking_pass_items:
        raise _config_error(blocking_path, "blocking_passes must be a non-empty list")

    blocking_passes: list[BlockingPassConfig] = []
    blocking_pass_names: set[str] = set()
    for index, item in enumerate(blocking_pass_items):
        if not isinstance(item, Mapping):
            raise _config_error(blocking_path, f"blocking_passes[{index}] must be a mapping")
        _validate_allowed_keys(
            item,
            allowed_keys={"name", "fields"},
            path=blocking_path,
            context=f"blocking_passes[{index}]",
        )
        name = _require_non_empty_string(
            item,
            "name",
            path=blocking_path,
            context=f"blocking_passes[{index}]",
        )
        if name in blocking_pass_names:
            raise _config_error(blocking_path, f"blocking pass name '{name}' is duplicated")
        blocking_pass_names.add(name)

        fields = _require_string_list(
            item,
            "fields",
            path=blocking_path,
            context=f"blocking_passes[{index}]",
        )
        unsupported_fields = sorted(set(fields) - SUPPORTED_BLOCKING_FIELDS)
        if unsupported_fields:
            raise _config_error(
                blocking_path,
                f"blocking_passes[{index}].fields contains unsupported values: "
                f"{', '.join(unsupported_fields)}",
            )
        blocking_passes.append(BlockingPassConfig(name=name, fields=fields))

    _validate_allowed_keys(
        matching_rules,
        allowed_keys={"weights"},
        path=matching_path,
        context="top-level config",
    )
    raw_weights = _require_mapping(
        matching_rules,
        "weights",
        path=matching_path,
        context="matching_rules",
    )
    _validate_allowed_keys(
        raw_weights,
        allowed_keys=set(SUPPORTED_WEIGHT_FIELDS),
        path=matching_path,
        context="weights",
    )
    missing_weight_fields = [field for field in SUPPORTED_WEIGHT_FIELDS if field not in raw_weights]
    if missing_weight_fields:
        raise _config_error(
            matching_path,
            f"weights is missing required fields: {', '.join(missing_weight_fields)}",
        )
    weights: dict[str, float] = {}
    for field_name in SUPPORTED_WEIGHT_FIELDS:
        weight = _require_float(raw_weights, field_name, path=matching_path, context="weights")
        if weight < 0.0:
            raise _config_error(matching_path, f"weights.{field_name} must be greater than or equal to 0")
        weights[field_name] = weight
    total_weight = round(sum(weights.values()), 10)
    if total_weight <= 0.0:
        raise _config_error(matching_path, "weights must sum to a value greater than 0")

    phone_output_format = _optional_non_empty_string(
        phone_rules,
        "output_format",
        path=normalization_path,
        context="phone_normalization",
        default="digits_only",
    )
    if phone_output_format not in SUPPORTED_PHONE_OUTPUT_FORMATS:
        raise _config_error(
            normalization_path,
            "phone_normalization.output_format must be one of: "
            + ", ".join(sorted(SUPPORTED_PHONE_OUTPUT_FORMATS)),
        )
    default_country_code = _optional_non_empty_string(
        phone_rules,
        "default_country_code",
        path=normalization_path,
        context="phone_normalization",
        default="1",
    )
    if not re.fullmatch(r"\d+", default_country_code):
        raise _config_error(
            normalization_path,
            "phone_normalization.default_country_code must contain digits only",
        )

    _validate_allowed_keys(
        thresholds_rules,
        allowed_keys={"thresholds"},
        path=thresholds_path,
        context="top-level config",
    )
    raw_thresholds = _require_mapping(
        thresholds_rules,
        "thresholds",
        path=thresholds_path,
        context="thresholds_rules",
    )
    _validate_allowed_keys(
        raw_thresholds,
        allowed_keys={"auto_merge", "manual_review_min", "no_match_max"},
        path=thresholds_path,
        context="thresholds",
    )
    auto_merge = _require_float(
        raw_thresholds,
        "auto_merge",
        path=thresholds_path,
        context="thresholds",
    )
    manual_review_min = _require_float(
        raw_thresholds,
        "manual_review_min",
        path=thresholds_path,
        context="thresholds",
    )
    no_match_max = _require_float(
        raw_thresholds,
        "no_match_max",
        path=thresholds_path,
        context="thresholds",
    )
    if no_match_max < 0.0 or manual_review_min < 0.0 or auto_merge < 0.0:
        raise _config_error(thresholds_path, "threshold values must be greater than or equal to 0")
    if no_match_max >= manual_review_min:
        raise _config_error(
            thresholds_path,
            "thresholds.no_match_max must be less than thresholds.manual_review_min",
        )
    if manual_review_min > auto_merge:
        raise _config_error(
            thresholds_path,
            "thresholds.manual_review_min must be less than or equal to thresholds.auto_merge",
        )
    if auto_merge > total_weight:
        raise _config_error(
            thresholds_path,
            "thresholds.auto_merge cannot exceed the total configured match weight",
        )

    _validate_allowed_keys(
        survivorship_rules,
        allowed_keys={"source_priority", "field_rules"},
        path=survivorship_path,
        context="top-level config",
    )
    source_priority = _require_string_list(
        survivorship_rules,
        "source_priority",
        path=survivorship_path,
        context="survivorship_rules",
    )
    if len(set(source_priority)) != len(source_priority):
        raise _config_error(survivorship_path, "source_priority contains duplicate source names")

    raw_field_rules = _require_mapping(
        survivorship_rules,
        "field_rules",
        path=survivorship_path,
        context="survivorship_rules",
    )
    _validate_allowed_keys(
        raw_field_rules,
        allowed_keys=set(SUPPORTED_SURVIVORSHIP_FIELDS),
        path=survivorship_path,
        context="field_rules",
    )
    missing_field_rules = [field for field in SUPPORTED_SURVIVORSHIP_FIELDS if field not in raw_field_rules]
    if missing_field_rules:
        raise _config_error(
            survivorship_path,
            f"field_rules is missing required fields: {', '.join(missing_field_rules)}",
        )

    field_rules: dict[str, str] = {}
    for field_name in SUPPORTED_SURVIVORSHIP_FIELDS:
        rule_config = raw_field_rules.get(field_name)
        if not isinstance(rule_config, Mapping):
            raise _config_error(
                survivorship_path,
                f"field_rules.{field_name} must be a mapping",
            )
        _validate_allowed_keys(
            rule_config,
            allowed_keys={"strategy"},
            path=survivorship_path,
            context=f"field_rules.{field_name}",
        )
        strategy = _require_non_empty_string(
            rule_config,
            "strategy",
            path=survivorship_path,
            context=f"field_rules.{field_name}",
        )
        if strategy not in SUPPORTED_SURVIVORSHIP_STRATEGIES:
            raise _config_error(
                survivorship_path,
                f"field_rules.{field_name}.strategy must be one of: "
                f"{', '.join(sorted(SUPPORTED_SURVIVORSHIP_STRATEGIES))}",
            )
        field_rules[field_name] = strategy

    return PipelineConfig(
        normalization=NormalizationConfig(
            name=NameNormalizationConfig(
                trim_whitespace=_require_bool(
                    name_rules,
                    "trim_whitespace",
                    path=normalization_path,
                    context="name_normalization",
                ),
                remove_punctuation=_require_bool(
                    name_rules,
                    "remove_punctuation",
                    path=normalization_path,
                    context="name_normalization",
                ),
                uppercase=_require_bool(
                    name_rules,
                    "uppercase",
                    path=normalization_path,
                    context="name_normalization",
                ),
            ),
            date=DateNormalizationConfig(
                accepted_formats=_require_string_list(
                    date_rules,
                    "accepted_formats",
                    path=normalization_path,
                    context="date_normalization",
                ),
                output_format=_require_non_empty_string(
                    date_rules,
                    "output_format",
                    path=normalization_path,
                    context="date_normalization",
                ),
            ),
            phone=PhoneNormalizationConfig(
                digits_only=_require_bool(
                    phone_rules,
                    "digits_only",
                    path=normalization_path,
                    context="phone_normalization",
                ),
                output_format=phone_output_format,
                default_country_code=default_country_code,
            ),
        ),
        matching=MatchingConfig(
            blocking_passes=tuple(blocking_passes),
            weights=weights,
            thresholds=ThresholdConfig(
                auto_merge=auto_merge,
                manual_review_min=manual_review_min,
                no_match_max=no_match_max,
            ),
        ),
        survivorship=SurvivorshipConfig(
            source_priority=source_priority,
            field_rules=field_rules,
        ),
    )


def load_export_job_configs(
    config_dir: Path | None = None,
    *,
    environment: str | None = None,
) -> dict[str, ExportJobConfig]:
    root = config_dir or default_config_dir()
    export_jobs_path = root / "export_jobs.yml"
    export_jobs_rules = _load_pipeline_yaml(root, "export_jobs.yml", environment=environment)

    _validate_allowed_keys(
        export_jobs_rules,
        allowed_keys={"export_jobs"},
        path=export_jobs_path,
        context="top-level config",
    )
    raw_jobs = export_jobs_rules.get("export_jobs")
    if not isinstance(raw_jobs, list) or not raw_jobs:
        raise _config_error(export_jobs_path, "export_jobs must be a non-empty list")

    resolved_jobs: dict[str, ExportJobConfig] = {}
    for index, raw_job in enumerate(raw_jobs):
        if not isinstance(raw_job, Mapping):
            raise _config_error(export_jobs_path, f"export_jobs[{index}] must be a mapping")
        _validate_allowed_keys(
            raw_job,
            allowed_keys={
                "name",
                "consumer",
                "description",
                "output_root",
                "contract_name",
                "contract_version",
                "format",
            },
            path=export_jobs_path,
            context=f"export_jobs[{index}]",
        )

        name = _require_non_empty_string(
            raw_job,
            "name",
            path=export_jobs_path,
            context=f"export_jobs[{index}]",
        )
        if name in resolved_jobs:
            raise _config_error(export_jobs_path, f"export job name {name!r} is duplicated")

        consumer = _require_non_empty_string(
            raw_job,
            "consumer",
            path=export_jobs_path,
            context=f"export_jobs[{index}]",
        )
        if consumer not in SUPPORTED_EXPORT_JOB_CONSUMERS:
            raise _config_error(
                export_jobs_path,
                f"export_jobs[{index}].consumer must be one of: "
                f"{', '.join(sorted(SUPPORTED_EXPORT_JOB_CONSUMERS))}",
            )

        output_root_value = _require_non_empty_string(
            raw_job,
            "output_root",
            path=export_jobs_path,
            context=f"export_jobs[{index}]",
        )
        output_root = Path(output_root_value)
        if not output_root.is_absolute():
            output_root = (root.parent / output_root).resolve()

        contract_name = _optional_non_empty_string(
            raw_job,
            "contract_name",
            path=export_jobs_path,
            context=f"export_jobs[{index}]",
            default=DELIVERY_CONTRACT_NAME,
        )
        if contract_name != DELIVERY_CONTRACT_NAME:
            raise _config_error(
                export_jobs_path,
                f"export_jobs[{index}].contract_name must be {DELIVERY_CONTRACT_NAME!r}",
            )

        contract_version = _optional_non_empty_string(
            raw_job,
            "contract_version",
            path=export_jobs_path,
            context=f"export_jobs[{index}]",
            default=DELIVERY_CONTRACT_VERSION,
        )
        if contract_version != DELIVERY_CONTRACT_VERSION:
            raise _config_error(
                export_jobs_path,
                f"export_jobs[{index}].contract_version must be {DELIVERY_CONTRACT_VERSION!r}",
            )

        export_format = _optional_non_empty_string(
            raw_job,
            "format",
            path=export_jobs_path,
            context=f"export_jobs[{index}]",
            default="csv_snapshot",
        )
        if export_format not in SUPPORTED_EXPORT_JOB_FORMATS:
            raise _config_error(
                export_jobs_path,
                f"export_jobs[{index}].format must be one of: "
                f"{', '.join(sorted(SUPPORTED_EXPORT_JOB_FORMATS))}",
            )

        resolved_jobs[name] = ExportJobConfig(
            name=name,
            consumer=consumer,
            description=_require_non_empty_string(
                raw_job,
                "description",
                path=export_jobs_path,
                context=f"export_jobs[{index}]",
            ),
            output_root=output_root,
            contract_name=contract_name,
            contract_version=contract_version,
            export_format=export_format,
        )

    return resolved_jobs


def load_benchmark_fixture_configs(
    config_dir: Path | None = None,
    *,
    environment: str | None = None,
) -> dict[str, BenchmarkFixtureConfig]:
    root = config_dir or default_config_dir()
    benchmark_path = root / "benchmark_fixtures.yml"
    benchmark_rules = _load_pipeline_yaml(root, "benchmark_fixtures.yml", environment=environment)

    _validate_allowed_keys(
        benchmark_rules,
        allowed_keys={"benchmark_fixtures"},
        path=benchmark_path,
        context="top-level config",
    )
    raw_fixtures = benchmark_rules.get("benchmark_fixtures")
    if not isinstance(raw_fixtures, list) or not raw_fixtures:
        raise _config_error(benchmark_path, "benchmark_fixtures must be a non-empty list")

    resolved_fixtures: dict[str, BenchmarkFixtureConfig] = {}
    for index, raw_fixture in enumerate(raw_fixtures):
        if not isinstance(raw_fixture, Mapping):
            raise _config_error(benchmark_path, f"benchmark_fixtures[{index}] must be a mapping")
        _validate_allowed_keys(
            raw_fixture,
            allowed_keys={
                "name",
                "description",
                "mode",
                "profile",
                "person_count",
                "duplicate_rate",
                "seed",
                "formats",
                "stream_batch_count",
                "stream_events_per_batch",
                "capacity_targets",
            },
            path=benchmark_path,
            context=f"benchmark_fixtures[{index}]",
        )

        name = _require_non_empty_string(
            raw_fixture,
            "name",
            path=benchmark_path,
            context=f"benchmark_fixtures[{index}]",
        )
        if name in resolved_fixtures:
            raise _config_error(benchmark_path, f"benchmark fixture name {name!r} is duplicated")

        mode = _optional_non_empty_string(
            raw_fixture,
            "mode",
            path=benchmark_path,
            context=f"benchmark_fixtures[{index}]",
            default="batch",
        )
        if mode not in SUPPORTED_BENCHMARK_MODES:
            raise _config_error(
                benchmark_path,
                f"benchmark_fixtures[{index}].mode must be one of: "
                f"{', '.join(sorted(SUPPORTED_BENCHMARK_MODES))}",
            )

        profile = _require_non_empty_string(
            raw_fixture,
            "profile",
            path=benchmark_path,
            context=f"benchmark_fixtures[{index}]",
        )
        if profile not in SUPPORTED_SYNTHETIC_PROFILES:
            raise _config_error(
                benchmark_path,
                f"benchmark_fixtures[{index}].profile must be one of: "
                f"{', '.join(sorted(SUPPORTED_SYNTHETIC_PROFILES))}",
            )

        person_count_value = raw_fixture.get("person_count")
        if isinstance(person_count_value, bool) or not isinstance(person_count_value, int) or person_count_value <= 0:
            raise _config_error(
                benchmark_path,
                f"benchmark_fixtures[{index}].person_count must be an integer greater than 0",
            )

        duplicate_rate = _require_float(
            raw_fixture,
            "duplicate_rate",
            path=benchmark_path,
            context=f"benchmark_fixtures[{index}]",
        )
        if duplicate_rate < 0.0 or duplicate_rate > 1.0:
            raise _config_error(
                benchmark_path,
                f"benchmark_fixtures[{index}].duplicate_rate must be between 0 and 1",
            )

        seed_value = raw_fixture.get("seed")
        if isinstance(seed_value, bool) or not isinstance(seed_value, int):
            raise _config_error(
                benchmark_path,
                f"benchmark_fixtures[{index}].seed must be an integer",
            )

        stream_batch_count = _require_positive_int_if_present(
            raw_fixture,
            "stream_batch_count",
            path=benchmark_path,
            context=f"benchmark_fixtures[{index}]",
            default=0,
        )
        stream_events_per_batch = _require_positive_int_if_present(
            raw_fixture,
            "stream_events_per_batch",
            path=benchmark_path,
            context=f"benchmark_fixtures[{index}]",
            default=0,
        )
        if mode == "event_stream":
            if stream_batch_count <= 0:
                raise _config_error(
                    benchmark_path,
                    f"benchmark_fixtures[{index}].stream_batch_count must be greater than 0 for event_stream fixtures",
                )
            if stream_events_per_batch <= 0:
                raise _config_error(
                    benchmark_path,
                    f"benchmark_fixtures[{index}].stream_events_per_batch must be greater than 0 for event_stream fixtures",
                )
        elif stream_batch_count or stream_events_per_batch:
            raise _config_error(
                benchmark_path,
                f"benchmark_fixtures[{index}] stream_batch_count and stream_events_per_batch require mode=event_stream",
            )

        formats = _require_string_list(
            raw_fixture,
            "formats",
            path=benchmark_path,
            context=f"benchmark_fixtures[{index}]",
        )
        unsupported_formats = sorted(set(formats) - SUPPORTED_BENCHMARK_FORMATS)
        if unsupported_formats:
            raise _config_error(
                benchmark_path,
                f"benchmark_fixtures[{index}].formats contains unsupported values: "
                f"{', '.join(unsupported_formats)}",
            )

        raw_targets = raw_fixture.get("capacity_targets", {})
        if not isinstance(raw_targets, Mapping):
            raise _config_error(
                benchmark_path,
                f"benchmark_fixtures[{index}].capacity_targets must be a mapping",
            )
        capacity_targets: dict[str, BenchmarkCapacityTargetConfig] = {}
        for deployment_name, target_value in raw_targets.items():
            if not isinstance(deployment_name, str) or not deployment_name.strip():
                raise _config_error(
                    benchmark_path,
                    f"benchmark_fixtures[{index}].capacity_targets contains an invalid deployment name",
                )
            if not isinstance(target_value, Mapping):
                raise _config_error(
                    benchmark_path,
                    f"benchmark_fixtures[{index}].capacity_targets.{deployment_name} must be a mapping",
                )
            _validate_allowed_keys(
                target_value,
                allowed_keys={
                    "runtime_environment",
                    "state_store_backend",
                    "max_total_duration_seconds",
                    "min_normalize_records_per_second",
                    "min_match_candidate_pairs_per_second",
                    "max_stream_batch_duration_seconds",
                    "max_p95_stream_batch_duration_seconds",
                    "min_stream_events_per_second",
                },
                path=benchmark_path,
                context=f"benchmark_fixtures[{index}].capacity_targets.{deployment_name}",
            )
            runtime_environment = None
            if "runtime_environment" in target_value:
                runtime_environment = _require_non_empty_string(
                    target_value,
                    "runtime_environment",
                    path=benchmark_path,
                    context=f"benchmark_fixtures[{index}].capacity_targets.{deployment_name}",
                )
            state_store_backend = _optional_non_empty_string(
                target_value,
                "state_store_backend",
                path=benchmark_path,
                context=f"benchmark_fixtures[{index}].capacity_targets.{deployment_name}",
                default="sqlite",
            )
            if state_store_backend not in SUPPORTED_BENCHMARK_STATE_STORE_BACKENDS:
                raise _config_error(
                    benchmark_path,
                    f"benchmark_fixtures[{index}].capacity_targets.{deployment_name}.state_store_backend "
                    f"must be one of: {', '.join(sorted(SUPPORTED_BENCHMARK_STATE_STORE_BACKENDS))}",
                )
            max_total_duration_seconds = _require_float(
                target_value,
                "max_total_duration_seconds",
                path=benchmark_path,
                context=f"benchmark_fixtures[{index}].capacity_targets.{deployment_name}",
            )
            min_normalize_records_per_second = _require_float(
                target_value,
                "min_normalize_records_per_second",
                path=benchmark_path,
                context=f"benchmark_fixtures[{index}].capacity_targets.{deployment_name}",
            )
            min_match_candidate_pairs_per_second = _require_float(
                target_value,
                "min_match_candidate_pairs_per_second",
                path=benchmark_path,
                context=f"benchmark_fixtures[{index}].capacity_targets.{deployment_name}",
            )
            max_stream_batch_duration_seconds = _optional_float_if_present(
                target_value,
                "max_stream_batch_duration_seconds",
                path=benchmark_path,
                context=f"benchmark_fixtures[{index}].capacity_targets.{deployment_name}",
            )
            max_p95_stream_batch_duration_seconds = _optional_float_if_present(
                target_value,
                "max_p95_stream_batch_duration_seconds",
                path=benchmark_path,
                context=f"benchmark_fixtures[{index}].capacity_targets.{deployment_name}",
            )
            min_stream_events_per_second = _optional_float_if_present(
                target_value,
                "min_stream_events_per_second",
                path=benchmark_path,
                context=f"benchmark_fixtures[{index}].capacity_targets.{deployment_name}",
            )
            if max_total_duration_seconds <= 0.0:
                raise _config_error(
                    benchmark_path,
                    f"benchmark_fixtures[{index}].capacity_targets.{deployment_name}.max_total_duration_seconds "
                    "must be greater than 0",
                )
            if min_normalize_records_per_second < 0.0 or min_match_candidate_pairs_per_second < 0.0:
                raise _config_error(
                    benchmark_path,
                    f"benchmark_fixtures[{index}].capacity_targets.{deployment_name} minimum throughput values "
                    "must be greater than or equal to 0",
                )
            stream_thresholds = {
                "max_stream_batch_duration_seconds": max_stream_batch_duration_seconds,
                "max_p95_stream_batch_duration_seconds": max_p95_stream_batch_duration_seconds,
                "min_stream_events_per_second": min_stream_events_per_second,
            }
            if mode != "event_stream" and any(value is not None for value in stream_thresholds.values()):
                raise _config_error(
                    benchmark_path,
                    f"benchmark_fixtures[{index}].capacity_targets.{deployment_name} stream thresholds "
                    "require mode=event_stream",
                )
            if max_stream_batch_duration_seconds is not None and max_stream_batch_duration_seconds <= 0.0:
                raise _config_error(
                    benchmark_path,
                    f"benchmark_fixtures[{index}].capacity_targets.{deployment_name}.max_stream_batch_duration_seconds "
                    "must be greater than 0",
                )
            if (
                max_p95_stream_batch_duration_seconds is not None
                and max_p95_stream_batch_duration_seconds <= 0.0
            ):
                raise _config_error(
                    benchmark_path,
                    f"benchmark_fixtures[{index}].capacity_targets.{deployment_name}.max_p95_stream_batch_duration_seconds "
                    "must be greater than 0",
                )
            if min_stream_events_per_second is not None and min_stream_events_per_second < 0.0:
                raise _config_error(
                    benchmark_path,
                    f"benchmark_fixtures[{index}].capacity_targets.{deployment_name}.min_stream_events_per_second "
                    "must be greater than or equal to 0",
                )
            normalized_deployment_name = deployment_name.strip()
            capacity_targets[normalized_deployment_name] = BenchmarkCapacityTargetConfig(
                deployment_name=normalized_deployment_name,
                runtime_environment=runtime_environment,
                state_store_backend=state_store_backend,
                max_total_duration_seconds=max_total_duration_seconds,
                min_normalize_records_per_second=min_normalize_records_per_second,
                min_match_candidate_pairs_per_second=min_match_candidate_pairs_per_second,
                max_stream_batch_duration_seconds=max_stream_batch_duration_seconds,
                max_p95_stream_batch_duration_seconds=max_p95_stream_batch_duration_seconds,
                min_stream_events_per_second=min_stream_events_per_second,
            )

        resolved_fixtures[name] = BenchmarkFixtureConfig(
            name=name,
            description=_require_non_empty_string(
                raw_fixture,
                "description",
                path=benchmark_path,
                context=f"benchmark_fixtures[{index}]",
            ),
            mode=mode,
            profile=profile,
            person_count=person_count_value,
            duplicate_rate=duplicate_rate,
            seed=seed_value,
            formats=formats,
            stream_batch_count=stream_batch_count,
            stream_events_per_batch=stream_events_per_batch,
            capacity_targets=capacity_targets,
        )

    return resolved_fixtures
