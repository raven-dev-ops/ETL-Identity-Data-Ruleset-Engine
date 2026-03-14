# Matching and Thresholds

The current runtime path uses external YAML config for blocking passes,
field weights, and threshold bands.

The supported matching strategy for the public `0.x` line remains a
deterministic, explainable heuristic scorer. ML-assisted scoring is
intentionally out of scope for the supported runtime surface.

## Blocking

Configured in `config/blocking_rules.yml`.

Current passes:

- `last_initial_plus_dob`
- `last_name_plus_birth_year`

Candidate generation is multi-pass and de-duplicates pairs that appear
in more than one blocking pass.

The runtime also emits per-pass blocking metrics in
`data/matches/blocking_metrics.csv`.

Current blocking metrics fields:

- `pass_name`
- `fields`
- `raw_candidate_pair_count`
- `new_candidate_pair_count`
- `cumulative_candidate_pair_count`
- `overall_deduplicated_candidate_pair_count`

## Scoring

Configured in `config/matching_rules.yml`.

Current weighted signals:

- `canonical_name`
- `canonical_dob`
- `canonical_phone`
- `canonical_address`

The scorer now also emits a derived partial-name signal when two records
share a surname and have either a matching first-name initial or a known
nickname-family equivalent.

The scorer also emits:

- `canonical_name_phonetic` when two records have soundalike first and
  last name tokens under a lightweight Soundex-style comparison even
  though they are not exact or nickname/initial matches
- `canonical_phone_partial` when two phone values share the same trailing
  10 digits but differ in formatting or country-code prefix shape
- `canonical_address_partial` when two addresses share the same house
  number and overlapping normalized street-core tokens even if unit or
  directional detail differs

The runtime now emits the following fields for each candidate pair in
`data/matches/candidate_scores.csv`:

- `left_id`
- `right_id`
- `score`
- `decision`
- `matched_fields`
- `reason_trace`

`matched_fields` lists the weighted signals that contributed to the
score. `reason_trace` includes those signals annotated with their
applied weights. Derived signals such as `canonical_name_partial` are
reported explicitly rather than being folded into exact-match labels.

Current derived heuristic-signal weight ratios:

- `canonical_name_partial`: `70%` of the configured name weight
- `canonical_name_phonetic`: `50%` of the configured name weight
- `canonical_phone_partial`: `80%` of the configured phone weight
- `canonical_address_partial`: `60%` of the configured address weight

## Thresholds

Configured in `config/thresholds.yml`.

Current decision bands:

- `auto_merge`: score greater than or equal to `auto_merge`
- `manual_review`: score between `manual_review_min` and
  `auto_merge`
- `no_match`: score less than or equal to `no_match_max`

The test suite now includes threshold-boundary fixtures that exercise:

- exact matches
- partial-name plus exact-DOB cases
- phonetic-name plus supporting-signal cases
- exact-name plus partial-address cases
- partial-phone country-code variants
- low-signal cases that must remain below `manual_review_min`

## Cluster Construction

The standalone `cluster` stage and `run-all` both convert `auto_merge`
links into deterministic connected components and assign stable
`cluster_id` values in `data/matches/entity_clusters.csv`.

Every source record receives a cluster assignment:

- auto-merged records share a cluster
- unresolved records remain singleton clusters

This keeps the runtime deterministic for fixed input and config while
preserving unresolved duplicates for manual review.

## Command Examples

Run the matching stage against the normalized artifact:

```bash
python -m etl_identity_engine.cli match \
  --input data/normalized/normalized_person_records.csv \
  --output data/matches/candidate_scores.csv
```

This command writes both `candidate_scores.csv` and
`blocking_metrics.csv` into `data/matches/`.

Build entity clusters from the normalized and matching artifacts:

```bash
python -m etl_identity_engine.cli cluster \
  --input data/normalized/normalized_person_records.csv \
  --matches data/matches/candidate_scores.csv \
  --output data/matches/entity_clusters.csv
```
