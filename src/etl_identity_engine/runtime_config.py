"""Runtime loading for pipeline YAML configuration files."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml


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


def _load_yaml(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")

    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Configuration file must contain a mapping: {path}")
    return data


def load_pipeline_config(config_dir: Path | None = None) -> PipelineConfig:
    root = config_dir or default_config_dir()

    normalization_rules = _load_yaml(root / "normalization_rules.yml")
    blocking_rules = _load_yaml(root / "blocking_rules.yml")
    matching_rules = _load_yaml(root / "matching_rules.yml")
    thresholds_rules = _load_yaml(root / "thresholds.yml")
    survivorship_rules = _load_yaml(root / "survivorship_rules.yml")

    name_rules = normalization_rules.get("name_normalization", {})
    date_rules = normalization_rules.get("date_normalization", {})
    phone_rules = normalization_rules.get("phone_normalization", {})

    blocking_passes = tuple(
        BlockingPassConfig(
            name=str(item.get("name", "")),
            fields=tuple(str(field) for field in item.get("fields", [])),
        )
        for item in blocking_rules.get("blocking_passes", [])
        if isinstance(item, dict)
    )

    field_rules = {
        str(field_name): str(rule_config.get("strategy", ""))
        for field_name, rule_config in survivorship_rules.get("field_rules", {}).items()
        if isinstance(rule_config, dict)
    }

    return PipelineConfig(
        normalization=NormalizationConfig(
            name=NameNormalizationConfig(
                trim_whitespace=bool(name_rules.get("trim_whitespace", True)),
                remove_punctuation=bool(name_rules.get("remove_punctuation", True)),
                uppercase=bool(name_rules.get("uppercase", True)),
            ),
            date=DateNormalizationConfig(
                accepted_formats=tuple(str(fmt) for fmt in date_rules.get("accepted_formats", [])),
                output_format=str(date_rules.get("output_format", "%Y-%m-%d")),
            ),
            phone=PhoneNormalizationConfig(
                digits_only=bool(phone_rules.get("digits_only", True)),
            ),
        ),
        matching=MatchingConfig(
            blocking_passes=blocking_passes,
            weights={
                str(field_name): float(weight)
                for field_name, weight in matching_rules.get("weights", {}).items()
            },
            thresholds=ThresholdConfig(
                auto_merge=float(thresholds_rules.get("thresholds", {}).get("auto_merge", 0.9)),
                manual_review_min=float(
                    thresholds_rules.get("thresholds", {}).get("manual_review_min", 0.6)
                ),
                no_match_max=float(thresholds_rules.get("thresholds", {}).get("no_match_max", 0.59)),
            ),
        ),
        survivorship=SurvivorshipConfig(
            source_priority=tuple(str(value) for value in survivorship_rules.get("source_priority", [])),
            field_rules=field_rules,
        ),
    )
