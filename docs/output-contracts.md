# Output Contracts

This repository treats the pipeline artifacts below as stable contracts
for the current `0.1.x` prototype line. Contract tests validate the file
names, CSV headers, and summary-object shape before merge.

CSV artifacts always write a header row, even when a run produces zero
data rows.

## CSV Artifacts

| Artifact | Relative path | Required columns | Notes |
| --- | --- | --- | --- |
| Normalized records | `data/normalized/normalized_person_records.csv` | `source_record_id`, `person_entity_id`, `source_system`, `first_name`, `last_name`, `dob`, `address`, `city`, `state`, `postal_code`, `phone`, `updated_at`, `is_conflict_variant`, `conflict_types`, `canonical_name`, `canonical_dob`, `canonical_address`, `canonical_phone` | Canonical fields are appended to the supported synthetic-source columns. |
| Candidate scores | `data/matches/candidate_scores.csv` | `left_id`, `right_id`, `score`, `decision`, `matched_fields`, `reason_trace` | `score` is numeric. `decision` is one of `auto_merge`, `manual_review`, or `no_match`. |
| Blocking metrics | `data/matches/blocking_metrics.csv` | `pass_name`, `fields`, `raw_candidate_pair_count`, `new_candidate_pair_count`, `cumulative_candidate_pair_count`, `overall_deduplicated_candidate_pair_count` | Count fields are integers. `fields` is semicolon-delimited. |
| Entity clusters | `data/matches/entity_clusters.csv` | `cluster_id`, `source_record_id`, `source_system`, `person_entity_id` | One row per input source record. |
| Golden records | `data/golden/golden_person_records.csv` | `golden_id`, `first_name`, `first_name_source_record_id`, `first_name_source_system`, `first_name_rule_name`, `last_name`, `last_name_source_record_id`, `last_name_source_system`, `last_name_rule_name`, `dob`, `dob_source_record_id`, `dob_source_system`, `dob_rule_name`, `address`, `address_source_record_id`, `address_source_system`, `address_rule_name`, `phone`, `phone_source_record_id`, `phone_source_system`, `phone_rule_name`, `person_entity_id`, `cluster_id`, `source_record_count` | Includes field-level provenance for each surviving attribute. |
| Source-to-golden crosswalk | `data/golden/source_to_golden_crosswalk.csv` | `source_record_id`, `source_system`, `person_entity_id`, `cluster_id`, `golden_id` | Stable join surface for downstream consumers. |
| Manual review queue | `data/review_queue/manual_review_queue.csv` | `review_id`, `left_id`, `right_id`, `score`, `reason_codes`, `top_contributing_match_signals`, `queue_status` | `queue_status` currently defaults to `pending`, and the artifact is the supported CSV handoff for manual review in `0.1.x`. |
| Invalid DOB exceptions | `data/exceptions/invalid_dobs.csv` | `source_record_id`, `person_entity_id`, `source_system`, `field_name`, `raw_value`, `canonical_value`, `reason_code` | Exception CSV contract is shared across all exception outputs. |
| Malformed phone exceptions | `data/exceptions/malformed_phones.csv` | `source_record_id`, `person_entity_id`, `source_system`, `field_name`, `raw_value`, `canonical_value`, `reason_code` | `reason_code` is `malformed_phone` when populated. |
| Normalization failures | `data/exceptions/normalization_failures.csv` | `source_record_id`, `person_entity_id`, `source_system`, `field_name`, `raw_value`, `canonical_value`, `reason_code` | Used for canonical-name and canonical-address failures. |

## Summary Artifacts

### `data/exceptions/run_summary.json`

Required top-level keys:

- `total_records`
- `missing_field_counts`
- `exception_counts`
- `candidate_pair_count`
- `decision_counts`
- `cluster_count`
- `golden_record_count`
- `review_queue_count`
- `completeness`
- `before_after_completeness`
- `duplicate_reduction`

Nested contract notes:

- `missing_field_counts` is a `dict[str, int]` keyed only by fields with
  non-zero missing counts.
- `exception_counts` always includes `invalid_dobs`,
  `malformed_phones`, and `normalization_failures`.
- `decision_counts` always includes `auto_merge`, `manual_review`, and
  `no_match`.
- `completeness` always includes raw/canonical counts for name, DOB, and
  phone.
- `before_after_completeness` always includes `name`, `dob`, and
  `phone`, each with integer `before`, `after`, and `delta` values.
- `duplicate_reduction` always includes integer record counts plus a
  float `reduction_ratio`.

### `data/exceptions/run_report.md`

The markdown report begins with `# Pipeline Report` and includes summary
sections for completeness, before/after completeness, duplicate
reduction, missing-field counts, and exception counts.

## Downstream Delivery Contract

Persisted runs can now be published for downstream ETL consumers through
the versioned `golden_crosswalk_snapshot/v1` delivery contract.

That contract is separate from the prototype working-directory outputs:

- it publishes immutable snapshot directories
- it writes an atomic `current.json` pointer for consumers
- it includes `delivery_manifest.json` with row counts, headers, and
  `sha256` checksums for the published artifacts

The consumer-facing publication layout and versioning rules are
documented in [delivery-contracts.md](delivery-contracts.md).
