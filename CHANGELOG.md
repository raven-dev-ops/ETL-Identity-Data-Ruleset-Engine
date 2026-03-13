# Changelog

All notable changes to this project will be documented in this file.

## [0.1.0] - Pending

### Added

- Config-driven CLI stages for synthetic generation, normalization,
  matching, golden-record generation, reporting, and `run-all`
  orchestration.
- Deterministic synthetic multi-table source generation with conflict
  injection and CSV or Parquet output.
- Multi-pass blocking metrics, explainable weighted scoring, clustering,
  survivorship provenance, and source-to-golden crosswalk outputs.
- Runtime config validation, data-quality exception artifacts,
  before/after completeness metrics, and duplicate-reduction reporting.
- Stage docs, output-contract docs, release-process docs, and GitHub
  backlog automation.

### Changed

- CI now validates Linux and Windows paths and publishes coverage
  artifacts.
- Pipeline artifact schemas are treated as stable contracts and covered
  by dedicated contract tests.

### Known Limitations

- Matching is intentionally rules-based and does not include advanced
  phonetic or ML-assisted scoring.
- The manual review queue remains a file output rather than a managed
  workflow.

