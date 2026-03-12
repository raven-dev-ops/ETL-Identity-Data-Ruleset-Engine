"""Deterministic conflict recipe helpers for synthetic identity records."""

from __future__ import annotations

import random
from typing import Callable


Record = dict[str, str]
Recipe = Callable[[Record, random.Random], str]

_NICKNAME_MAP = {
    "JOHN": "JON",
    "JANE": "JANIE",
    "ALEX": "ALEC",
    "SAM": "SAMUEL",
    "RILEY": "RILEE",
    "MORGAN": "MORGYN",
    "CASEY": "KASEY",
    "TAYLOR": "TAYLER",
}

_ADDRESS_SUFFIX_MAP = {
    "STREET": "ST",
    "AVENUE": "AVE",
    "ROAD": "RD",
    "LANE": "LN",
    "DRIVE": "DR",
}


def recipe_nickname_variant(record: Record, _: random.Random) -> str:
    first = record.get("first_name", "").upper()
    if first in _NICKNAME_MAP:
        record["first_name"] = _NICKNAME_MAP[first]
        return "nickname_variant"
    return "name_case_variant"


def recipe_name_case_variant(record: Record, _: random.Random) -> str:
    record["first_name"] = record.get("first_name", "").title()
    record["last_name"] = record.get("last_name", "").title()
    return "name_case_variant"


def recipe_dob_transposition(record: Record, _: random.Random) -> str:
    dob = record.get("dob", "")
    parts = dob.split("-")
    if len(parts) != 3:
        return "dob_transposition"

    year, month, day = parts
    if len(month) == 2 and len(day) == 2:
        record["dob"] = f"{year}-{day}-{month}"
    return "dob_transposition"


def recipe_address_suffix_variant(record: Record, _: random.Random) -> str:
    tokens = record.get("address", "").upper().split(" ")
    normalized = [_ADDRESS_SUFFIX_MAP.get(token, token) for token in tokens]
    record["address"] = " ".join(normalized)
    return "address_suffix_variant"


def recipe_stale_address_shift(record: Record, rng: random.Random) -> str:
    address = record.get("address", "")
    tokens = address.split(" ")
    if not tokens:
        return "stale_address_shift"

    try:
        number = int(tokens[0])
        number += rng.randint(20, 200)
        tokens[0] = str(number)
        record["address"] = " ".join(tokens)
    except ValueError:
        pass
    return "stale_address_shift"


def recipe_phone_format_variant(record: Record, _: random.Random) -> str:
    digits = "".join(ch for ch in record.get("phone", "") if ch.isdigit())
    if len(digits) == 10:
        record["phone"] = f"({digits[0:3]}) {digits[3:6]}-{digits[6:10]}"
    else:
        record["phone"] = digits
    return "phone_format_variant"


_RECIPE_FUNCTIONS: tuple[Recipe, ...] = (
    recipe_nickname_variant,
    recipe_name_case_variant,
    recipe_dob_transposition,
    recipe_address_suffix_variant,
    recipe_stale_address_shift,
    recipe_phone_format_variant,
)


def apply_conflict_recipes(
    record: Record,
    rng: random.Random,
    min_recipes: int = 1,
    max_recipes: int = 3,
) -> list[str]:
    """Mutate record with deterministic conflict recipes and return applied recipe names."""
    if min_recipes < 1 or max_recipes < min_recipes:
        raise ValueError("invalid recipe bounds")

    recipe_count = rng.randint(min_recipes, min(max_recipes, len(_RECIPE_FUNCTIONS)))
    selected = rng.sample(_RECIPE_FUNCTIONS, recipe_count)

    applied: list[str] = []
    for recipe in selected:
        applied.append(recipe(record, rng))
    return applied

