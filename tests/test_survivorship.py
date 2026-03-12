from etl_identity_engine.survivorship.rules_engine import choose_value, merge_records


def test_choose_value_prefers_higher_source_priority() -> None:
    values = [
        {"value": "JOHN", "source_system": "source_a"},
        {"value": "JOHNATHAN", "source_system": "source_b"},
    ]
    chosen = choose_value(values, source_priority=["source_b", "source_a"])
    assert chosen == "JOHNATHAN"


def test_merge_records_returns_single_golden_record() -> None:
    records = [
        {
            "source_system": "source_a",
            "first_name": "JOHN",
            "last_name": "SMITH",
            "dob": "1985-03-12",
            "address": "123 MAIN ST",
            "phone": "5551234567",
        },
        {
            "source_system": "source_b",
            "first_name": "JOHNATHAN",
            "last_name": "SMITH",
            "dob": "1985-03-12",
            "address": "123 MAIN STREET",
            "phone": "5551234567",
        },
    ]
    golden = merge_records(records)
    assert golden["golden_id"] == "G-00001"
    assert golden["first_name"] == "JOHNATHAN"

