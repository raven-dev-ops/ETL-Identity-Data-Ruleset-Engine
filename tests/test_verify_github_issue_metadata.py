from __future__ import annotations

import importlib.util
from pathlib import Path
import sys


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1] / "scripts" / "verify_github_issue_metadata.py"
)
SPEC = importlib.util.spec_from_file_location("verify_github_issue_metadata", SCRIPT_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def test_validate_accepts_expected_issue_metadata() -> None:
    parsed = MODULE.ParsedRepo(
        repo_url="https://github.com/raven-dev-ops/ETL-Identity-Data-Ruleset-Engine",
        default_branch="main",
        blank_issues_enabled=False,
        contact_links=[
            {
                "name": "Private security report",
                "about": "security",
                "url": "https://github.com/raven-dev-ops/ETL-Identity-Data-Ruleset-Engine/security/advisories/new",
            },
            {
                "name": "Contribution guide",
                "about": "contributing",
                "url": "https://github.com/raven-dev-ops/ETL-Identity-Data-Ruleset-Engine/blob/main/CONTRIBUTING.md",
            },
            {
                "name": "Project backlog",
                "about": "planning",
                "url": "https://github.com/raven-dev-ops/ETL-Identity-Data-Ruleset-Engine/blob/main/planning/active-github-issues-backlog.md",
            },
        ],
        issue_templates=[
            {"name": "Bug Report", "about": "", "title": "[Bug]: ", "filename": "bug.yml"},
            {"name": "Maintenance Task", "about": "", "title": "[Chore]: ", "filename": "chore.yml"},
            {"name": "Documentation", "about": "", "title": "[Docs]: ", "filename": "docs.yml"},
            {"name": "Epic", "about": "", "title": "[Epic]: ", "filename": "epic.yml"},
            {"name": "Feature Request", "about": "", "title": "[Feature]: ", "filename": "feature.yml"},
        ],
        template_files=["bug.yml", "chore.yml", "config.yml", "docs.yml", "epic.yml", "feature.yml"],
    )

    assert MODULE._validate(parsed) == []


def test_validate_reports_missing_remote_metadata() -> None:
    parsed = MODULE.ParsedRepo(
        repo_url="https://github.com/raven-dev-ops/ETL-Identity-Data-Ruleset-Engine",
        default_branch="main",
        blank_issues_enabled=True,
        contact_links=[],
        issue_templates=[],
        template_files=[],
    )

    errors = MODULE._validate(parsed)

    assert "blank issues are still enabled on the default branch" in errors
    assert "missing contact link: Private security report" in errors
    assert "missing contact link: Contribution guide" in errors
    assert "missing contact link: Project backlog" in errors
    assert any(error.startswith("missing pushed issue template files on default branch:") for error in errors)
