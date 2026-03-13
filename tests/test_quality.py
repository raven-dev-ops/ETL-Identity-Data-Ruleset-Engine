from etl_identity_engine.quality.exceptions import build_run_summary, extract_exception_rows


def test_extract_exception_rows_detects_invalid_dob_and_malformed_phone() -> None:
    rows = [
        {
            "source_record_id": "A-1",
            "person_entity_id": "P-1",
            "source_system": "source_a",
            "first_name": "JOHN",
            "last_name": "SMITH",
            "dob": "1985-31-12",
            "phone": "555-12",
            "address": "123 MAIN ST",
            "canonical_name": "JOHN SMITH",
            "canonical_dob": "",
            "canonical_phone": "55512",
            "canonical_address": "123 MAIN STREET",
        }
    ]

    exception_rows = extract_exception_rows(rows)

    assert len(exception_rows["invalid_dobs"]) == 1
    assert len(exception_rows["malformed_phones"]) == 1
    assert exception_rows["invalid_dobs"][0]["reason_code"] == "invalid_dob"
    assert exception_rows["malformed_phones"][0]["reason_code"] == "malformed_phone"


def test_build_run_summary_includes_exception_and_decision_counts() -> None:
    rows = [
        {
            "source_record_id": "A-1",
            "person_entity_id": "P-1",
            "source_system": "source_a",
            "first_name": "JOHN",
            "last_name": "SMITH",
            "dob": "1985-03-12",
            "phone": "555-123-4567",
            "address": "123 MAIN ST",
            "canonical_name": "JOHN SMITH",
            "canonical_dob": "1985-03-12",
            "canonical_phone": "5551234567",
            "canonical_address": "123 MAIN STREET",
        }
    ]

    summary = build_run_summary(
        rows,
        candidate_pair_count=4,
        decision_counts={"auto_merge": 2, "manual_review": 1, "no_match": 1},
        cluster_count=1,
        golden_record_count=1,
        review_queue_count=1,
    )

    assert summary["candidate_pair_count"] == 4
    assert summary["decision_counts"] == {
        "auto_merge": 2,
        "manual_review": 1,
        "no_match": 1,
    }
    assert summary["exception_counts"] == {
        "invalid_dobs": 0,
        "malformed_phones": 0,
        "normalization_failures": 0,
    }
