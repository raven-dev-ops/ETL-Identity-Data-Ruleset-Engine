# Release Process

This document defines the maintainer checklist for tagged release lines
starting with `v0.1.0`, including later feature, patch, and hotfix
releases.

## Required Checks For `main`

These merge gates must be configured in GitHub branch protection or a
ruleset because they cannot be stored fully in git:

- Require pull requests before merging to `main`.
- Require at least one approval.
- Dismiss stale approvals when new commits are pushed.
- Require branches to be up to date before merge.
- Block force pushes and branch deletion on `main`.
- Require these status checks:
  - `CI / test-linux`
  - `CI / test-windows`
  - `CI / container-supply-chain`
  - `CI / release-hardening`
  - compatibility jobs for Python `3.12` and macOS should remain green
    even if branch protection keeps the baseline required-check list
    narrower
  - `Issue Metadata / verify-github-issue-metadata` when the change
    touches `.github/ISSUE_TEMPLATE/**`,
    `scripts/verify_github_issue_metadata.py`, or
    `.github/workflows/issue-metadata.yml`

`Lint` is currently a manual workflow and is not part of the required
check baseline.

## Release-Readiness Criteria

- `CI / test-linux` and `CI / test-windows` are green on the release
  commit.
- Compatibility jobs for Linux `3.12`, Windows `3.12`, and macOS `3.12`
  are green on the release commit.
- Coverage remains at or above `85%`.
- Artifact contract tests pass for the documented pipeline outputs.
- `README.md`, `CHANGELOG.md`, and the release notes reflect the current
  behavior.
- The backlog dry-run still parses the active planning backlog.
- A source distribution and wheel are both produced successfully from
  the release commit.
- The release hardening job produces a retained dependency inventory and
  artifact-hash summary for the built release artifacts.
- The release hardening dependency audit is green on the release
  commit.
- The container supply-chain job produces retained image attestation,
  SBOM-style inventory, provenance, and dependency-audit outputs for the
  built image artifact.
- The container dependency audit is green on the release commit.
- The installed `etl-identity-engine` console script resolves and shows
  the expected CLI help output from the release environment.
- A fresh packaged small-profile release sample bundle is produced
  successfully.

## Release Line Guidance

- `v0.1.0` established the initial public release line for the project.
- Feature-complete milestone cycles may advance the minor version for
  the pre-`1.0` line; patch and hotfix changes should advance the patch
  version from the latest published tag.
- Do not retag or silently replace an existing published release.
- Hotfixes follow the same process as patch releases: land the fix on a
  reviewed green commit, update `CHANGELOG.md`, then cut the next patch
  tag from that commit.
- If a defect is discovered after a tag is pushed, keep the existing tag
  immutable and queue the correction for the next patch release.

## Supported Boundaries And Constraints

- The public repository remains synthetic-only by design, but the
  runtime supports manifest-driven landed batches in controlled
  deployments outside the repository. Operational or sensitive data must
  not be committed back into the repo or release artifacts.
- Matching remains rules-based and intentionally explainable; it
  includes exact, heuristic partial, and lightweight phonetic-name
  scoring. ML-assisted scoring is intentionally out of scope for the
  supported public line.
- The supported manual-review model includes both the portable CSV queue
  artifact and the persisted SQLite review workflow documented in
  [review-workflow.md](review-workflow.md).
- Output contracts are stable for the documented CSV and summary
  artifacts only; synthetic generator outputs are not versioned as a
  release contract.
- CI validates a maintained support matrix of Python `3.11` baseline
  jobs on Linux and Windows plus Python `3.12` compatibility jobs on
  Linux, Windows, and macOS, including the documented release-bundle
  packaging path.
- The Python venv bootstrap does not install shell runtimes, but the
  repo now provides Python-native `scripts/run_checks.py` and
  `scripts/run_pipeline.py` entrypoints for shell-free local execution.

## Sample Output Set

Generate a release-candidate sample bundle with:

```bash
python scripts/package_release_sample.py --output-dir dist/release-samples --profile small --seed 42 --formats csv,parquet
```

The script writes a zip archive named like
`etl-identity-engine-vX.Y.Z-sample-small.zip`.

For a fixed clean commit, rerunning the script produces a byte-stable
zip archive. The manifest timestamp defaults to the HEAD commit
timestamp and the script also honors `SOURCE_DATE_EPOCH` when you need a
reproducible rebuild timestamp override.

That bundle should include at least:

- `data/normalized/normalized_person_records.csv`
- `data/matches/candidate_scores.csv`
- `data/matches/blocking_metrics.csv`
- `data/matches/entity_clusters.csv`
- `data/golden/golden_person_records.csv`
- `data/golden/source_to_golden_crosswalk.csv`
- `data/review_queue/manual_review_queue.csv`
- `data/exceptions/invalid_dobs.csv`
- `data/exceptions/malformed_phones.csv`
- `data/exceptions/normalization_failures.csv`
- `data/exceptions/run_summary.json`
- `data/exceptions/run_report.md`
- `manifest.json`

See [output-contracts.md](output-contracts.md) for the stable file
shapes.

## Release Hardening Outputs

Generate the retained dependency-inventory and audit outputs with:

```bash
python scripts/release_hardening_check.py --output-dir dist/release-hardening
```

That command writes:

- `artifacts/*.whl`
- `artifacts/*.tar.gz`
- `dependency_inventory.json`
- `dependency_audit.json`
- `release_hardening_summary.json`

The script clears stale files under the selected `artifacts/`
subdirectory before building, so rerunning it against the documented
output directory does not fail because of older wheel or sdist files.

The CI `release-hardening` job publishes the same directory as the
`release-hardening-inventory` artifact.

## Container Supply-Chain Outputs

Generate the retained image attestation and scan outputs with:

```bash
python scripts/container_supply_chain_check.py --output-dir dist/container-supply-chain --image-tag etl-identity-engine:release-hardening
```

That command writes:

- `container_requirements.txt`
- `container_sbom.json`
- `container_provenance.json`
- `container_dependency_audit.json`
- `container_attestation.json`
- `container_supply_chain_summary.json`

The current line uses an attestation bundle rather than registry signing
as the default release-path control. The attestation binds the built
image ID and tag to the emitted SBOM, provenance, and scan outputs.

The dependency audit gate is enforced from the installed Python packages
inside the built image, so a vulnerable image dependency fails the
script and blocks publication.

The CI `container-supply-chain` job publishes the same directory as the
`container-supply-chain` artifact.

## Release Checklist

- Confirm `pyproject.toml` version matches the intended tag.
- Update `CHANGELOG.md` with a new section for the intended release tag.
- Run the local validation path:
  - `./scripts/run_checks.ps1` on Windows
  - `./scripts/run_checks.sh` on systems that already provide `bash`
  - `python scripts/run_checks.py` on any platform when you want the
    shell-free equivalent path
- Those wrappers cover package-build verification, an installed
  `etl-identity-engine --help` smoke check, `ruff`, `pytest`, the
  active-backlog dry-run, and release-sample packaging.
- The wrapper build and packaging checks use temporary output
  directories; the
  explicit `package_release_sample.py --output-dir dist/release-samples`
  command remains the maintainer path when you want a retained bundle.
- Run the retained release-hardening command:
  - `python scripts/release_hardening_check.py --output-dir dist/release-hardening`
- Review `dist/release-hardening/release_hardening_summary.json`,
  `dependency_inventory.json`, and `dependency_audit.json`.
- Run the retained container supply-chain command:
  - `python scripts/container_supply_chain_check.py --output-dir dist/container-supply-chain --image-tag etl-identity-engine:release-hardening`
- Review `dist/container-supply-chain/container_supply_chain_summary.json`,
  `container_sbom.json`, `container_provenance.json`,
  `container_dependency_audit.json`, and `container_attestation.json`.
- Run the backlog dry-run:
  - `python scripts/create_github_backlog.py --repo "<OWNER/REPO>" --dry-run`
  - For historical backlog validation, use `--include-closed`.
- Build the packaged release sample:
  - `python scripts/package_release_sample.py --output-dir dist/release-samples --profile small --seed 42 --formats csv,parquet`
- Review the packaged output set against
  [output-contracts.md](output-contracts.md).
- Verify the required GitHub checks passed on the release commit.
- Draft release notes summarizing:
  - included features
  - patch or hotfix scope, if the release is not the initial tag in a line
  - known limitations
  - validation commands
  - sample output attachment location
- Create and push the annotated tag.
- Publish the GitHub release using the tag and changelog summary, then
  attach the packaged sample zip.

## Tag Procedure

From a clean checkout on the release commit:

```bash
git pull --ff-only
git tag -a vX.Y.Z -m "Release vX.Y.Z"
git push origin vX.Y.Z
```

Then create the GitHub release for `vX.Y.Z` and paste the matching
changelog entry into the release notes.
