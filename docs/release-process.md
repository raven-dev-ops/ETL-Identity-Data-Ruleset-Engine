# Release Process

This document defines the maintainer checklist for tagged release lines
starting with `v0.1.0`, including later patch and hotfix releases.

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
  - `Issue Metadata / verify-github-issue-metadata` when the change
    touches `.github/ISSUE_TEMPLATE/**`,
    `scripts/verify_github_issue_metadata.py`, or
    `.github/workflows/issue-metadata.yml`

`Lint` is currently a manual workflow and is not part of the required
check baseline.

## Release-Readiness Criteria

- `CI / test-linux` and `CI / test-windows` are green on the release
  commit.
- Coverage remains at or above `85%`.
- Artifact contract tests pass for the documented pipeline outputs.
- `README.md`, `CHANGELOG.md`, and the release notes reflect the current
  behavior.
- The backlog dry-run still parses the active planning backlog.
- A fresh packaged small-profile release sample bundle is produced
  successfully.

## Release Line Guidance

- `v0.1.0` established the initial public release line for the project.
- Later fixes should ship as new patch tags such as `v0.1.1` or
  `v0.1.2`; do not retag or silently replace an existing published
  release.
- Hotfixes follow the same process as patch releases: land the fix on a
  reviewed green commit, update `CHANGELOG.md`, then cut the next patch
  tag from that commit.
- If a defect is discovered after a tag is pushed, keep the existing tag
  immutable and queue the correction for the next patch release.

## Known Limitations

- Matching remains rules-based and intentionally lightweight; it does
  not include phonetic or ML-based scoring.
- The manual review queue is a CSV handoff, not a persistent workflow.
- Output contracts are stable for the documented CSV and summary
  artifacts only; synthetic generator outputs are not versioned as a
  release contract.
- CI currently targets Python `3.11` on Linux and Windows only.
- The Python venv bootstrap does not install shell runtimes; local
  Windows validation should use PowerShell, while Linux CI validates the
  bash path.

## Sample Output Set

Generate a release-candidate sample bundle with:

```bash
python scripts/package_release_sample.py --output-dir dist/release-samples --profile small --seed 42 --formats csv,parquet
```

The script writes a zip archive named like
`etl-identity-engine-vX.Y.Z-sample-small.zip`.

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

## Release Checklist

- Confirm `pyproject.toml` version matches the intended tag.
- Update `CHANGELOG.md` with a new section for the intended release tag.
- Run the local validation path:
  - `./scripts/run_checks.ps1` on Windows
  - `./scripts/run_checks.sh` on systems that already provide `bash`
- Run the backlog dry-run:
  - `python scripts/create_github_backlog.py --repo "<OWNER/REPO>" --dry-run`
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
