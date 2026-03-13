from etl_identity_engine.survivorship.rules_engine import (
    build_golden_records,
    choose_value,
    merge_records,
)


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
            "source_record_id": "A-1",
            "source_system": "source_a",
            "first_name": "JOHN",
            "last_name": "SMITH",
            "dob": "1985-03-12",
            "address": "123 MAIN ST",
            "phone": "5551234567",
        },
        {
            "source_record_id": "B-1",
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
    assert golden["first_name"] == "JOHN"
    assert golden["first_name_source_record_id"] == "A-1"
    assert golden["first_name_rule_name"] == "source_priority_then_non_null"
    assert golden["source_record_count"] == "2"


def test_choose_value_prefers_most_recent_timestamp_within_same_priority() -> None:
    values = [
        {
            "value": "123 OLD ST",
            "source_system": "source_a",
            "updated_at": "2024-01-10T10:00:00Z",
        },
        {
            "value": "456 NEW ST",
            "source_system": "source_a",
            "updated_at": "2025-01-10T10:00:00Z",
        },
    ]
    chosen = choose_value(values, source_priority=["source_a", "source_b"])
    assert chosen == "456 NEW ST"


def test_build_golden_records_groups_by_person_entity_id() -> None:
    records = [
        {
            "source_record_id": "A-1",
            "person_entity_id": "P-000001",
            "source_system": "source_a",
            "first_name": "JOHN",
            "last_name": "SMITH",
            "dob": "1985-03-12",
            "address": "123 MAIN ST",
            "phone": "5551234567",
        },
        {
            "source_record_id": "B-1",
            "person_entity_id": "P-000001",
            "source_system": "source_b",
            "first_name": "JON",
            "last_name": "SMITH",
            "dob": "1985-03-12",
            "address": "123 MAIN STREET",
            "phone": "5551234567",
        },
        {
            "source_record_id": "A-2",
            "person_entity_id": "P-000002",
            "source_system": "source_a",
            "first_name": "JANE",
            "last_name": "JONES",
            "dob": "1990-04-08",
            "address": "456 OAK AVE",
            "phone": "5557654321",
        },
    ]

    golden_records = build_golden_records(records)

    assert len(golden_records) == 2
    assert [row["golden_id"] for row in golden_records] == ["G-00001", "G-00002"]
    assert golden_records[0]["person_entity_id"] == "P-000001"
    assert golden_records[0]["source_record_count"] == "2"


def test_build_golden_records_prefers_cluster_id_when_present() -> None:
    records = [
        {
            "source_record_id": "A-1",
            "person_entity_id": "P-000001",
            "cluster_id": "C-00002",
            "source_system": "source_a",
            "first_name": "JOHN",
            "last_name": "SMITH",
            "dob": "1985-03-12",
            "address": "123 MAIN ST",
            "phone": "5551234567",
        },
        {
            "source_record_id": "A-2",
            "person_entity_id": "P-000002",
            "cluster_id": "C-00001",
            "source_system": "source_a",
            "first_name": "JANE",
            "last_name": "JONES",
            "dob": "1990-04-08",
            "address": "456 OAK AVE",
            "phone": "5557654321",
        },
    ]

    golden_records = build_golden_records(records)

    assert [row["cluster_id"] for row in golden_records] == ["C-00001", "C-00002"]

