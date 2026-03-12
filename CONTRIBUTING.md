# Contributing

## Development Setup

1. Install Python 3.11+.
2. Create and bootstrap a virtual environment.
3. Run lint/tests from that environment.
4. Use the venv-provided `gh` only when you want to verify current GitHub-side metadata.

### Windows (PowerShell)

```powershell
./scripts/bootstrap_venv.ps1
.\.venv\Scripts\Activate.ps1
./scripts/run_checks.ps1
```

### macOS / Linux (bash)

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

## Issue Workflow

- Use the GitHub issue forms under `.github/ISSUE_TEMPLATE/` instead of opening blank issues.
- Pick the form that matches the work: `bug`, `feature`, `docs`, `chore`, or `epic`.
- Search existing issues and review `planning/github-issues-backlog.md` before creating new work.
- Keep issue reports limited to synthetic data, local repro steps, and project artifacts.
- Report security issues privately through the repository security advisory flow, not in public issues.
- `./scripts/run_checks.ps1` or `./scripts/run_checks.sh` is the authoritative pre-push local check path and uses only the venv's bundled Python tooling.
- `./scripts/run_checks.ps1 -IncludeRemoteGitHubChecks` or `./scripts/run_checks.sh --include-remote-github-checks` is an optional post-push deployed-state check that uses the venv's bundled Python and `gh`.
- Your unpushed local issue-template files are validated by the local `pytest` issue-template tests instead of the remote metadata query.
- After issue-template changes land on `main`, the `Issue Metadata` GitHub Actions workflow verifies that GitHub recognized the default-branch forms and contact links.

## GitHub Automation

- Authenticate once with the venv-scoped GitHub CLI: `gh auth login`.
- Use `python scripts/create_github_backlog.py --repo raven-dev-ops/ETL-Identity-Data-Ruleset-Engine --dry-run` to preview backlog creation from `planning/github-issues-backlog.md`.
- Use `python scripts/create_github_backlog.py --repo raven-dev-ops/ETL-Identity-Data-Ruleset-Engine` to apply labels, milestones, epics, and issues.
- On Windows, `./scripts/create_github_backlog.ps1 -Repo raven-dev-ops/ETL-Identity-Data-Ruleset-Engine` remains available if you prefer PowerShell.

## Data Safety

- Do not commit real operational or personal data.
- This repository is synthetic-data only.
