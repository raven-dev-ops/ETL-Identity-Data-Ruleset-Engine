# Matching and Thresholds

The current runtime path uses external YAML config for blocking passes,
field weights, and threshold bands.

## Blocking

Configured in `config/blocking_rules.yml`.

Current passes:

- `last_initial_plus_dob`
- `last_name_plus_birth_year`

Candidate generation is multi-pass and de-duplicates pairs that appear
in more than one blocking pass.

## Scoring

Configured in `config/matching_rules.yml`.

Current weighted exact-match signals:

- `canonical_name`
- `canonical_dob`
- `canonical_phone`
- `canonical_address`

The runtime now emits the following fields for each candidate pair in
`data/matches/candidate_scores.csv`:

- `left_id`
- `right_id`
- `score`
- `decision`
- `matched_fields`
- `reason_trace`

`matched_fields` lists the exact-match canonical fields that contributed
to the score. `reason_trace` includes the same signals annotated with
their configured weights.

## Thresholds

Configured in `config/thresholds.yml`.

Current decision bands:

- `auto_merge`: score greater than or equal to `auto_merge`
- `manual_review`: score between `manual_review_min` and
  `auto_merge`
- `no_match`: score less than or equal to `no_match_max`

## Cluster Construction

`run-all` converts `auto_merge` links into deterministic connected
components and assigns stable `cluster_id` values in
`data/matches/entity_clusters.csv`.

Every source record receives a cluster assignment:

- auto-merged records share a cluster
- unresolved records remain singleton clusters

This keeps the runtime deterministic for fixed input and config while
preserving unresolved duplicates for manual review.
