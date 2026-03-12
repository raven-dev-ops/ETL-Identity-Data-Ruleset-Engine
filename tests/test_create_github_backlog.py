from __future__ import annotations

import importlib.util
from pathlib import Path
import sys


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "create_github_backlog.py"
SPEC = importlib.util.spec_from_file_location("create_github_backlog", SCRIPT_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)

BACKLOG_PATH = Path(__file__).resolve().parents[1] / "planning" / "github-issues-backlog.md"


def test_parse_backlog_current_catalog() -> None:
    parsed = MODULE.parse_backlog(BACKLOG_PATH.read_text(encoding="utf-8"))

    assert parsed.milestones == ("M1", "M2", "M3", "M4", "M5", "M6")
    assert len(parsed.labels) == 16
    assert len(parsed.epics) == 6
    assert len(parsed.issues) == 32
    assert parsed.issues[0].title == "Bootstrap repository skeleton"
    assert parsed.issues[-1].title == "Prepare `v0.1.0` release checklist and tag plan"


def test_build_issue_body_uses_expected_sections() -> None:
    issue = MODULE.IssueItem(
        title="Example issue",
        milestone="M1",
        labels=("type:chore", "area:repo"),
        depends_on="#1",
        description_items=("first", "second"),
        acceptance_items=("done",),
    )

    body = MODULE.build_issue_body(issue)

    assert "## Milestone" in body
    assert "- ``M1``" in body
    assert "## Depends On" in body
    assert "- #1" in body
    assert "## Description" in body
    assert "- first" in body
    assert "## Acceptance Criteria" in body
    assert "- done" in body
