# Contributing

## Development Setup

1. Install Python 3.11+.
2. Create and bootstrap a virtual environment.
3. Run lint/tests from that environment.
4. Use the venv-provided `gh` only when you want to verify current GitHub-side metadata.

The venv provides Python packages and the optional venv-scoped `gh`
binary. It does not install shell runtimes such as `bash` or
PowerShell.

### Windows (PowerShell)

Use this path for local Windows validation.

```powershell
./scripts/bootstrap_venv.ps1
.\.venv\Scripts\Activate.ps1
./scripts/run_checks.ps1
```

### macOS / Linux (bash)

Use this path only on systems that already have `bash`.

```bash
chmod +x ./scripts/bootstrap_venv.sh ./scripts/run_checks.sh
./scripts/bootstrap_venv.sh
source .venv/bin/activate
./scripts/run_checks.sh
```

### Manual Equivalent

- `python -m venv .venv`
- `.venv/bin/python -m pip install -r requirements-dev.txt`
- `.venv/bin/python -m ruff check .`
- `.venv/bin/python -m pytest`
- `.venv/bin/gh --version`
- `.venv/bin/python scripts/verify_github_issue_metadata.py --repo raven-dev-ops/ETL-Identity-Data-Ruleset-Engine` (optional post-push)

On Windows, replace `.venv/bin/python` with `.venv\Scripts\python.exe` and `.venv/bin/gh` with `.venv\Scripts\gh.exe`.

## Branch and PR Expectations

- Keep changes scoped and documented.
- Add or update tests for behavior changes.
- Use the pull request template and include validation steps.

## Required Checks and Branch Protection

- Protect `main` with a GitHub branch-protection rule or ruleset.
- Require pull requests, at least one approval, and dismissal of stale approvals on new commits.
- Require branches to be up to date before merge.
- Block force pushes and branch deletion on `main`.
- Require these checks before merge:
  - `CI / test-linux`
  - `CI / test-windows`
  - `Issue Metadata / verify-github-issue-metadata` when issue-template or metadata-verification files change
- `Lint` is optional today because it is a manual workflow and not part of the merge gate baseline.
- The release-readiness coverage floor is `85%`, enforced in CI and referenced in [docs/release-process.md](docs/release-process.md).

## Issue Workflow

- Use the GitHub issue forms under `.github/ISSUE_TEMPLATE/` instead of opening blank issues.
- Pick the form that matches the work: `bug`, `feature`, `docs`, `chore`, or `epic`.
- Search existing issues and review `planning/post-v0.1.0-github-issues-backlog.md` before creating new work.
- Treat `planning/github-issues-backlog.md` as the closed bootstrap history for `M1` through `M6`, not the active source for new work.
- Keep issue reports limited to synthetic data, local repro steps, and project artifacts.
- Report security issues privately through the repository security advisory flow, not in public issues.
- `./scripts/run_checks.ps1` or `./scripts/run_checks.sh` is the authoritative pre-push local check path and uses the venv's Python tooling plus the host shell runtime for the platform-specific wrapper.
- `./scripts/run_checks.ps1 -IncludeRemoteGitHubChecks` or `./scripts/run_checks.sh --include-remote-github-checks` is an optional post-push deployed-state check that uses the venv's bundled Python and `gh`.
- Windows contributors should use the PowerShell scripts locally; the bash path is exercised in Linux CI and is not installed by the venv bootstrap.
- Your unpushed local issue-template files are validated by the local `pytest` issue-template tests instead of the remote metadata query.
- After issue-template changes land on `main`, the `Issue Metadata` GitHub Actions workflow verifies the default-branch issue metadata plus the pushed `.github/ISSUE_TEMPLATE/*.yml` files.

## GitHub Automation

- Authenticate once with the venv-scoped GitHub CLI: `gh auth login`.
- Use `python scripts/create_github_backlog.py --repo raven-dev-ops/ETL-Identity-Data-Ruleset-Engine --dry-run` to preview backlog creation from the active post-release backlog.
- Use `python scripts/create_github_backlog.py --repo raven-dev-ops/ETL-Identity-Data-Ruleset-Engine` to apply labels, milestones, epics, and issues from the active post-release backlog.
- Use `python scripts/create_github_backlog.py --repo raven-dev-ops/ETL-Identity-Data-Ruleset-Engine --backlog-path planning/github-issues-backlog.md --dry-run` only when re-syncing the completed bootstrap backlog.
- On Windows, `./scripts/create_github_backlog.ps1 -Repo raven-dev-ops/ETL-Identity-Data-Ruleset-Engine` remains available if you prefer PowerShell.

## Data Safety

- Do not commit real operational or personal data.
- This repository is synthetic-data only.
