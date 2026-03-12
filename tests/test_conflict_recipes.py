import random

from etl_identity_engine.generate.conflict_recipes import apply_conflict_recipes


def test_conflict_recipes_are_deterministic_with_seed() -> None:
    base_one = {
        "first_name": "JOHN",
        "last_name": "SMITH",
        "dob": "1985-03-12",
        "address": "123 MAIN STREET",
        "phone": "555-111-2222",
    }
    base_two = dict(base_one)

    rng1 = random.Random(42)
    rng2 = random.Random(42)

    conflicts_one = apply_conflict_recipes(base_one, rng1, min_recipes=2, max_recipes=2)
    conflicts_two = apply_conflict_recipes(base_two, rng2, min_recipes=2, max_recipes=2)

    assert conflicts_one == conflicts_two
    assert base_one == base_two

