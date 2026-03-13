"""Name normalization helpers."""

from __future__ import annotations

import re


def normalize_name(
    value: str,
    *,
    trim_whitespace: bool = True,
    remove_punctuation: bool = True,
    uppercase: bool = True,
) -> str:
    text = value.strip() if trim_whitespace else value
    if "," in text:
        parts = [part.strip() for part in text.split(",") if part.strip()]
        if len(parts) == 2:
            text = f"{parts[1]} {parts[0]}"

    if remove_punctuation:
        text = re.sub(r"[^A-Za-z0-9 ]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text.upper() if uppercase else text

