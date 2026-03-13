"""Runtime loading for pipeline YAML configuration files."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
import re

import yaml


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


def default_config_dir() -> Path:
    return Path(__file__).resolve().parents[2] / "config"


def _config_error(path: Path, message: str) -> ConfigValidationError:
    return ConfigValidationError(f"{path.name}: {message}")


def _load_yaml(path: Path) -> dict[str, object]:
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")

    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise _config_error(path, "configuration file must contain a mapping")
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


def load_pipeline_config(config_dir: Path | None = None) -> PipelineConfig:
    root = config_dir or default_config_dir()

    normalization_path = root / "normalization_rules.yml"
    blocking_path = root / "blocking_rules.yml"
    matching_path = root / "matching_rules.yml"
    thresholds_path = root / "thresholds.yml"
    survivorship_path = root / "survivorship_rules.yml"

    normalization_rules = _load_yaml(normalization_path)
    blocking_rules = _load_yaml(blocking_path)
    matching_rules = _load_yaml(matching_path)
    thresholds_rules = _load_yaml(thresholds_path)
    survivorship_rules = _load_yaml(survivorship_path)

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
