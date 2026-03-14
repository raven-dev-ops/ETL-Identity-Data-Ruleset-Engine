"""Stable output contracts for pipeline artifacts."""

from __future__ import annotations

from pathlib import Path

from etl_identity_engine.generate.synth_generator import PERSON_HEADERS


NORMALIZED_HEADERS = PERSON_HEADERS + (
    "canonical_name",
    "canonical_dob",
    "canonical_address",
    "canonical_phone",
)

MATCH_SCORE_HEADERS = (
    "left_id",
    "right_id",
    "score",
    "decision",
    "matched_fields",
    "reason_trace",
)

BLOCKING_METRICS_HEADERS = (
    "pass_name",
    "fields",
    "raw_candidate_pair_count",
    "new_candidate_pair_count",
    "cumulative_candidate_pair_count",
    "overall_deduplicated_candidate_pair_count",
)

ENTITY_CLUSTER_HEADERS = (
    "cluster_id",
    "source_record_id",
    "source_system",
    "person_entity_id",
)

MANUAL_REVIEW_HEADERS = (
    "review_id",
    "left_id",
    "right_id",
    "score",
    "reason_codes",
    "top_contributing_match_signals",
    "queue_status",
)

GOLDEN_HEADERS = (
    "golden_id",
    "first_name",
    "first_name_source_record_id",
    "first_name_source_system",
    "first_name_rule_name",
    "last_name",
    "last_name_source_record_id",
    "last_name_source_system",
    "last_name_rule_name",
    "dob",
    "dob_source_record_id",
    "dob_source_system",
    "dob_rule_name",
    "address",
    "address_source_record_id",
    "address_source_system",
    "address_rule_name",
    "phone",
    "phone_source_record_id",
    "phone_source_system",
    "phone_rule_name",
    "person_entity_id",
    "cluster_id",
    "source_record_count",
)

CROSSWALK_HEADERS = (
    "source_record_id",
    "source_system",
    "person_entity_id",
    "cluster_id",
    "golden_id",
)

EXCEPTION_HEADERS = (
    "source_record_id",
    "person_entity_id",
    "source_system",
    "field_name",
    "raw_value",
    "canonical_value",
    "reason_code",
)

PIPELINE_CSV_ARTIFACT_HEADERS = {
    Path("data/normalized/normalized_person_records.csv"): NORMALIZED_HEADERS,
    Path("data/matches/candidate_scores.csv"): MATCH_SCORE_HEADERS,
    Path("data/matches/blocking_metrics.csv"): BLOCKING_METRICS_HEADERS,
    Path("data/matches/entity_clusters.csv"): ENTITY_CLUSTER_HEADERS,
    Path("data/golden/golden_person_records.csv"): GOLDEN_HEADERS,
    Path("data/golden/source_to_golden_crosswalk.csv"): CROSSWALK_HEADERS,
    Path("data/review_queue/manual_review_queue.csv"): MANUAL_REVIEW_HEADERS,
    Path("data/exceptions/invalid_dobs.csv"): EXCEPTION_HEADERS,
    Path("data/exceptions/malformed_phones.csv"): EXCEPTION_HEADERS,
    Path("data/exceptions/normalization_failures.csv"): EXCEPTION_HEADERS,
}

DELIVERY_CONTRACT_NAME = "golden_crosswalk_snapshot"
DELIVERY_CONTRACT_VERSION = "v1"
DELIVERY_ARTIFACT_HEADERS = {
    Path("golden_person_records.csv"): GOLDEN_HEADERS,
    Path("source_to_golden_crosswalk.csv"): CROSSWALK_HEADERS,
}
DELIVERY_MANIFEST_KEYS = (
    "contract_name",
    "contract_version",
    "snapshot_id",
    "published_at_utc",
    "run_id",
    "state_db",
    "source_run",
    "row_counts",
    "artifacts",
)
DELIVERY_CURRENT_POINTER_KEYS = (
    "contract_name",
    "contract_version",
    "snapshot_id",
    "run_id",
    "published_at_utc",
    "relative_snapshot_path",
    "relative_manifest_path",
)

PIPELINE_ARTIFACT_PATHS = tuple(
    list(PIPELINE_CSV_ARTIFACT_HEADERS)
    + [
        Path("data/exceptions/run_report.md"),
        Path("data/exceptions/run_summary.json"),
    ]
)

MATCH_DECISIONS = ("auto_merge", "manual_review", "no_match")
SUMMARY_EXCEPTION_KEYS = ("invalid_dobs", "malformed_phones", "normalization_failures")
SUMMARY_COMPLETENESS_KEYS = (
    "raw_name_present",
    "canonical_name_present",
    "raw_dob_present",
    "canonical_dob_present",
    "raw_phone_present",
    "canonical_phone_present",
)
SUMMARY_BEFORE_AFTER_FIELDS = ("name", "dob", "phone")
SUMMARY_BEFORE_AFTER_KEYS = ("before", "after", "delta")
SUMMARY_DUPLICATE_REDUCTION_KEYS = (
    "before_record_count",
    "after_record_count",
    "records_consolidated",
    "reduction_ratio",
)
SUMMARY_TOP_LEVEL_KEYS = (
    "total_records",
    "missing_field_counts",
    "exception_counts",
    "candidate_pair_count",
    "decision_counts",
    "cluster_count",
    "golden_record_count",
    "review_queue_count",
    "completeness",
    "before_after_completeness",
    "duplicate_reduction",
)
