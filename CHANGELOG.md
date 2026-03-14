# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

## [0.1.2] - 2026-03-13

### Added

- Lightweight phonetic-name scoring for explainable candidate matching.
- Deterministic release-sample bundle packaging tests, including a
  byte-stability check for fixed metadata.
- A cross-platform Python-native `scripts/run_checks.py` entrypoint for
  shell-free local validation.

### Changed

- Release-sample packaging now writes deterministic zip entry metadata
  and derives the manifest timestamp from `SOURCE_DATE_EPOCH` or the
  HEAD commit timestamp so rebuilds are reproducible for a fixed commit.
- Active-backlog sync now skips entries marked `Status: closed` by
  default, with explicit `--include-closed` support for historical
  re-syncs.
- Local `run_checks` wrappers now validate the active backlog dry-run
  and release-sample packaging in addition to lint and tests, and CI now
  uses `--include-closed` for the historical bootstrap-backlog dry-run.
- Local `run_checks` release-sample validation now uses temporary output
  directories so routine pre-push checks do not leave retained bundles
  under `dist/`.
- `run_checks` and `run_pipeline` shell wrappers now delegate to the
  Python entrypoints so local maintenance behavior stays aligned across
  platforms.
- CI now validates Python `3.12` compatibility on Linux and Windows and
  adds a macOS `3.12` compatibility job alongside the existing Linux and
  Windows Python `3.11` baseline jobs.
- Public scope documentation now treats the synthetic-only boundary,
  deterministic heuristic matching strategy, and CSV manual-review model
  as explicit supported boundaries rather than open product ambiguity.
- The PowerShell backlog wrapper now delegates to the Python backlog
  script so both entrypoints stay behaviorally aligned.

## [0.1.1] - 2026-03-13

### Added

- Standalone `cluster` and `review-queue` CLI stages so the file-based
  pipeline can be rerun step-by-step outside `run-all`.
- Direct IO-reader tests, release-bundle packaging tests, and local test
  bootstrap for the `src/` package layout.
- Cross-platform `scripts/package_release_sample.py` for generating the
  documented release sample zip with a `manifest.json`.

### Changed

- Standalone `golden` now requires real cluster assignments instead of
  inferring groups from synthetic `person_entity_id` values.
- Standalone `report` now reads the downstream match, cluster, golden,
  and review-queue artifacts so its summary matches `run-all`.
- Stage input reads now fail fast on missing files, and `normalize`
  falls back to Parquet discovery only when CSV inputs are absent.
- `run-all` now accepts `--formats` and can normalize directly from
  generated Parquet inputs when CSV output is not requested.
- Pipeline wrapper scripts now forward arbitrary `run-all` CLI
  arguments instead of hard-coding a subset of options.
- Release-process documentation now uses the packaged sample-bundle
  workflow and the post-release tracker snapshot now records epic `#44`
  accurately.

### Removed

- Unused `generate.schemas` and `quality.reporting` modules from the
  package surface.

## [0.1.0] - 2026-03-13

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
