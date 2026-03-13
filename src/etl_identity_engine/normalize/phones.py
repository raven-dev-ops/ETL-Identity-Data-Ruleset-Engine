"""Phone normalization helpers."""

from __future__ import annotations

import re


SUPPORTED_PHONE_OUTPUT_FORMATS = frozenset({"digits_only", "e164"})


def _extract_phone_digits(value: str) -> str:
    text = re.sub(r"(?i)\b(?:EXT|EXTENSION|X)\b\.?\s*\d+\s*$", "", value.strip())
    return re.sub(r"\D+", "", text)


def _format_e164(digits: str, *, default_country_code: str) -> str:
    if not digits:
        return ""
    if len(digits) == 10:
        return f"+{default_country_code}{digits}"
    if len(digits) >= 11:
        return f"+{digits}"
    return digits


def normalize_phone(
    value: str,
    *,
    digits_only: bool = True,
    output_format: str = "digits_only",
    default_country_code: str = "1",
) -> str:
    if output_format not in SUPPORTED_PHONE_OUTPUT_FORMATS:
        raise ValueError(
            f"Unsupported phone output format: {output_format}. "
            f"Expected one of: {', '.join(sorted(SUPPORTED_PHONE_OUTPUT_FORMATS))}"
        )

    stripped_value = value.strip()
    digits = _extract_phone_digits(stripped_value)

    if output_format == "e164":
        return _format_e164(digits, default_country_code=default_country_code)
    if digits_only:
        return digits
    return stripped_value

