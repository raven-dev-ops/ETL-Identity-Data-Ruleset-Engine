from etl_identity_engine.quality.exceptions import (
    build_run_report_markdown,
    build_run_summary,
    extract_exception_rows,
)


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
    assert summary["before_after_completeness"] == {
        "name": {"before": 1, "after": 1, "delta": 0},
        "dob": {"before": 1, "after": 1, "delta": 0},
        "phone": {"before": 1, "after": 1, "delta": 0},
    }
    assert summary["duplicate_reduction"] == {
        "before_record_count": 1,
        "after_record_count": 1,
        "records_consolidated": 0,
        "reduction_ratio": 0.0,
    }


def test_build_run_report_markdown_includes_before_after_and_duplicate_metrics() -> None:
    summary = {
        "total_records": 4,
        "candidate_pair_count": 3,
        "cluster_count": 2,
        "golden_record_count": 2,
        "review_queue_count": 1,
        "decision_counts": {"auto_merge": 1, "manual_review": 1, "no_match": 1},
        "completeness": {
            "raw_name_present": 4,
            "canonical_name_present": 4,
            "raw_dob_present": 4,
            "canonical_dob_present": 3,
            "raw_phone_present": 4,
            "canonical_phone_present": 3,
        },
        "before_after_completeness": {
            "name": {"before": 4, "after": 4, "delta": 0},
            "dob": {"before": 4, "after": 3, "delta": -1},
            "phone": {"before": 4, "after": 3, "delta": -1},
        },
        "duplicate_reduction": {
            "before_record_count": 4,
            "after_record_count": 2,
            "records_consolidated": 2,
            "reduction_ratio": 0.5,
        },
        "performance": {
            "total_duration_seconds": 1.234,
            "phase_metrics": {
                "normalize": {
                    "duration_seconds": 0.123,
                    "input_record_count": 4,
                    "output_record_count": 4,
                    "output_records_per_second": 32.52,
                    "candidate_pair_count": 0,
                    "candidate_pairs_per_second": 0.0,
                }
            },
        },
        "missing_field_counts": {"phone": 1},
        "exception_counts": {"invalid_dobs": 1, "malformed_phones": 1, "normalization_failures": 0},
    }

    report = build_run_report_markdown("data/normalized/normalized_person_records.csv", summary)

    assert "## Before/After Completeness" in report
    assert "- `dob`: before=`4`, after=`3`, delta=`-1`" in report
    assert "## Duplicate Reduction" in report
    assert "- `records_consolidated`: `2`" in report
    assert "- `reduction_ratio`: `0.5`" in report
    assert "## Performance" in report
    assert "- `total_duration_seconds`: `1.234`" in report
